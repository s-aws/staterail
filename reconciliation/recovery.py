from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import (
    ErrorCategory,
    ErrorCode,
    EventType,
    ExchangeLookupStatus,
    ReconciliationIssue,
)
from core.errors import error_event_payload, exception_to_error_payload
from core.json_tools import JsonValue, normalize_json
from core.order_update_contract import OrderUpdateContractResult, validate_exchange_order_update
from exchanges.coinbase.advanced_trade_rest import CoinbaseOrderLookupResult
from projections.state import ReconciliationMismatchSnapshot, SourceOfTruthProjection


class OrderLookupClient(Protocol):
    def get_order(self, order_id: str) -> CoinbaseOrderLookupResult:
        ...


@dataclass(frozen=True)
class ReconciliationRecoveryResult:
    action_id: str
    reason: ReconciliationIssue
    lookup_status: ExchangeLookupStatus
    recovery_sequence: int
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


class ReconciliationRecovery:
    def __init__(
        self,
        core: AuditCore,
        *,
        order_lookup_client: OrderLookupClient,
        clock: Clock | None = None,
    ) -> None:
        self._core = core
        self._order_lookup_client = order_lookup_client
        self._clock = clock or SystemClock()

    def recover(self) -> tuple[ReconciliationRecoveryResult, ...]:
        projection = SourceOfTruthProjection.from_ledger(self._core.ledger)
        results: list[ReconciliationRecoveryResult] = []
        for mismatch in projection.reconciliation_mismatches.values():
            if projection.has_reconciliation_recovery(action_id=mismatch.action_id, reason=mismatch.reason):
                continue
            result = self._recover_mismatch(mismatch)
            results.append(result)
        return tuple(results)

    def _recover_mismatch(self, mismatch: ReconciliationMismatchSnapshot) -> ReconciliationRecoveryResult:
        client_order_id = _string_or_none(mismatch.payload.get("client_order_id"))
        exchange_order_id = _string_or_none(mismatch.payload.get("exchange_order_id"))
        if exchange_order_id is None:
            return self._emit_recovery(
                mismatch=mismatch,
                lookup_status=ExchangeLookupStatus.FAILED,
                client_order_id=client_order_id,
                exchange_order_id=None,
                error_code="exchange_order_id_required",
                error_message=(
                    "Cannot recover Coinbase order status without exchange_order_id; "
                    "client_order_id-only lookup is not supported by this recovery client"
                ),
            )

        try:
            lookup = self._order_lookup_client.get_order(exchange_order_id)
        except Exception as exc:
            self._core.emit(
                EventType.ERROR,
                exception_to_error_payload(
                    exc,
                    category=ErrorCategory.RECONCILIATION,
                    context={
                        "action_id": mismatch.action_id,
                        "client_order_id": client_order_id,
                        "exchange_order_id": exchange_order_id,
                        "reason": mismatch.reason.value,
                    },
                    error_code=ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                ),
            )
            return self._emit_recovery(
                mismatch=mismatch,
                lookup_status=ExchangeLookupStatus.FAILED,
                exchange_order_id=exchange_order_id,
                error_code="lookup_exception",
                error_message=str(exc),
            )

        recovery = self._emit_recovery(
            mismatch=mismatch,
            lookup_status=lookup.status,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            error_code=lookup.error_code,
            error_message=lookup.error_message,
            order_update=lookup.order_update,
            raw_response=lookup.raw_response,
            status_code=lookup.status_code,
        )
        if lookup.status == ExchangeLookupStatus.FAILED:
            self._core.emit(
                EventType.ERROR,
                error_event_payload(
                    category=ErrorCategory.RECONCILIATION,
                    context={
                        "action_id": mismatch.action_id,
                        "client_order_id": client_order_id,
                        "exchange_order_id": exchange_order_id,
                        "lookup_status": lookup.status.value,
                        "reason": mismatch.reason.value,
                        "status_code": lookup.status_code,
                    },
                    error_code=lookup.error_code or ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                    message=lookup.error_message or "Coinbase order lookup failed during reconciliation",
                ),
            )
        return recovery

    def _emit_recovery(
        self,
        *,
        mismatch: ReconciliationMismatchSnapshot,
        lookup_status: ExchangeLookupStatus,
        client_order_id: str | None,
        exchange_order_id: str | None,
        error_code: str | None,
        error_message: str | None,
        order_update: object | None = None,
        raw_response: object | None = None,
        status_code: int | None = None,
    ) -> ReconciliationRecoveryResult:
        payload: dict[str, JsonValue] = {
            "action_id": mismatch.action_id,
            "client_order_id": client_order_id,
            "error_code": error_code,
            "error_message": error_message,
            "exchange_order_id": exchange_order_id,
            "lookup_status": lookup_status.value,
            "mismatch_sequence": mismatch.sequence,
            "observed_at": self._clock.now(),
            "reason": mismatch.reason.value,
            "status_code": status_code,
        }
        if order_update is not None:
            normalized_order_update = normalize_json(order_update)
            order_update_payload = normalized_order_update if isinstance(normalized_order_update, dict) else {}
            if order_update_payload:
                contract = validate_exchange_order_update(order_update_payload)
                if contract.valid:
                    payload["order_update"] = order_update_payload
                else:
                    self._emit_order_update_contract_error(
                        contract=contract,
                        mismatch=mismatch,
                        order_update=order_update_payload,
                    )
        if raw_response is not None:
            payload["raw_response"] = raw_response

        record = self._core.emit(EventType.RECONCILIATION_RECOVERY, payload)
        return ReconciliationRecoveryResult(
            action_id=mismatch.action_id,
            client_order_id=client_order_id,
            error_code=error_code,
            error_message=error_message,
            exchange_order_id=exchange_order_id,
            lookup_status=lookup_status,
            reason=mismatch.reason,
            recovery_sequence=record.sequence,
        )

    def _emit_order_update_contract_error(
        self,
        *,
        contract: OrderUpdateContractResult,
        mismatch: ReconciliationMismatchSnapshot,
        order_update: Mapping[str, JsonValue],
    ) -> None:
        self._core.emit(
            EventType.ERROR,
            error_event_payload(
                category=ErrorCategory.RECONCILIATION,
                context={
                    "action_id": mismatch.action_id,
                    "invalid_fields": list(contract.invalid_fields),
                    "missing_fields": list(contract.missing_fields),
                    "raw_order_update": dict(order_update),
                    "reason": mismatch.reason.value,
                },
                error_code=ErrorCode.EXCHANGE_ORDER_UPDATE_INVALID,
                message="Coinbase recovery returned an order update that failed contract validation",
            ),
        )


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
