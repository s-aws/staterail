from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, ExchangeLookupStatus, ExecutionMode
from core.errors import error_event_payload
from core.json_tools import JsonValue
from exchanges.coinbase.advanced_trade_rest import CoinbaseFillsLookupResult
from projections.state import OrderSnapshot, SourceOfTruthProjection


class FillLookupClient(Protocol):
    def list_fills(
        self,
        *,
        order_ids: tuple[str, ...],
        cursor: str | None = None,
        limit: int = 100,
    ) -> CoinbaseFillsLookupResult:
        ...


@dataclass(frozen=True)
class FillReconciliationPolicy:
    execution_modes: tuple[ExecutionMode, ...] = (ExecutionMode.LIVE,)
    limit: int = 100
    max_pages_per_order: int = 10

    def __post_init__(self) -> None:
        if not self.execution_modes:
            raise ValueError("execution_modes must not be empty")
        for mode in self.execution_modes:
            if not isinstance(mode, ExecutionMode):
                raise TypeError("execution_modes must contain ExecutionMode values")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.max_pages_per_order <= 0:
            raise ValueError("max_pages_per_order must be positive")


@dataclass(frozen=True)
class FillReconciliationResult:
    exchange_order_id: str
    emitted_fill_ids: tuple[str, ...]
    lookup_status: ExchangeLookupStatus
    error_code: str | None = None
    error_message: str | None = None


class FillReconciliation:
    def __init__(
        self,
        core: AuditCore,
        *,
        fill_lookup_client: FillLookupClient,
        clock: Clock | None = None,
        policy: FillReconciliationPolicy | None = None,
    ) -> None:
        self._core = core
        self._fill_lookup_client = fill_lookup_client
        self._clock = clock or SystemClock()
        self._policy = policy or FillReconciliationPolicy()

    def reconcile(self) -> tuple[FillReconciliationResult, ...]:
        projection = SourceOfTruthProjection.from_ledger(self._core.ledger)
        emitted_fill_ids = set(projection.fills_by_id)
        results: list[FillReconciliationResult] = []
        for order in projection.orders_by_action_id.values():
            if not self._should_reconcile(order):
                continue
            result = self._reconcile_order(order, emitted_fill_ids)
            results.append(result)
        return tuple(results)

    def _should_reconcile(self, order: OrderSnapshot) -> bool:
        return order.execution_mode in self._policy.execution_modes and order.exchange_order_id is not None

    def _reconcile_order(
        self,
        order: OrderSnapshot,
        emitted_fill_ids: set[str],
    ) -> FillReconciliationResult:
        exchange_order_id = order.exchange_order_id
        if exchange_order_id is None:
            raise RuntimeError("Fill reconciliation received an order without exchange_order_id")

        emitted_for_order: list[str] = []
        cursor: str | None = None
        last_status = ExchangeLookupStatus.FOUND
        last_error_code: str | None = None
        last_error_message: str | None = None
        for _ in range(self._policy.max_pages_per_order):
            lookup = self._fill_lookup_client.list_fills(
                order_ids=(exchange_order_id,),
                cursor=cursor,
                limit=self._policy.limit,
            )
            last_status = lookup.status
            last_error_code = lookup.error_code
            last_error_message = lookup.error_message
            if lookup.status != ExchangeLookupStatus.FOUND:
                self._emit_lookup_error(order, lookup)
                break

            for fill in lookup.fills:
                fill_payload = dict(fill) if isinstance(fill, Mapping) else {}
                fill_id = _string_or_none(fill_payload.get("fill_id"))
                if fill_id is None:
                    self._emit_fill_payload_error(
                        order,
                        error_code=ErrorCode.FILL_PAYLOAD_INVALID,
                        message="Coinbase fill lookup returned a fill without fill_id",
                        context={
                            "field": "fill_id",
                            "raw_fill": fill_payload,
                        },
                    )
                    continue
                if fill_id in emitted_fill_ids:
                    continue
                fill_order_id = _string_or_none(fill_payload.get("order_id"))
                if fill_order_id is not None and fill_order_id != exchange_order_id:
                    self._emit_fill_payload_error(
                        order,
                        error_code=ErrorCode.FILL_ORDER_MISMATCH,
                        message="Coinbase fill lookup returned a fill for a different order_id",
                        context={
                            "expected_order_id": exchange_order_id,
                            "fill_id": fill_id,
                            "observed_order_id": fill_order_id,
                        },
                    )
                    continue
                payload = self._fill_payload(fill=fill_payload, order=order)
                self._core.emit(EventType.EXCHANGE_FILL, payload)
                emitted_fill_ids.add(fill_id)
                emitted_for_order.append(fill_id)

            if lookup.cursor is None:
                break
            cursor = lookup.cursor
        else:
            self._core.emit(
                EventType.ERROR,
                error_event_payload(
                    category=ErrorCategory.RECONCILIATION,
                    context={
                        "exchange_order_id": exchange_order_id,
                        "max_pages_per_order": self._policy.max_pages_per_order,
                    },
                    error_code=ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                    message="Coinbase fills pagination exceeded max_pages_per_order",
                ),
            )

        return FillReconciliationResult(
            emitted_fill_ids=tuple(emitted_for_order),
            error_code=last_error_code,
            error_message=last_error_message,
            exchange_order_id=exchange_order_id,
            lookup_status=last_status,
        )

    def _fill_payload(self, *, fill: object, order: OrderSnapshot) -> dict[str, JsonValue]:
        payload = dict(fill) if isinstance(fill, Mapping) else {}
        payload["action_id"] = order.action_id
        payload["client_order_id"] = order.client_order_id
        payload["observed_at"] = self._clock.now()
        payload["order_id"] = _string_or_none(payload.get("order_id")) or order.exchange_order_id
        payload["product_id"] = _string_or_none(payload.get("product_id")) or order.product_id
        if _string_or_none(payload.get("side")) is None and order.side is not None:
            payload["side"] = order.side.value.upper()
        return payload

    def _emit_lookup_error(self, order: OrderSnapshot, lookup: CoinbaseFillsLookupResult) -> None:
        self._core.emit(
            EventType.ERROR,
            error_event_payload(
                category=ErrorCategory.RECONCILIATION,
                context={
                    "action_id": order.action_id,
                    "exchange_order_id": order.exchange_order_id,
                    "lookup_status": lookup.status.value,
                    "status_code": lookup.status_code,
                },
                error_code=lookup.error_code or ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                message=lookup.error_message or "Coinbase fills lookup failed during reconciliation",
            ),
        )

    def _emit_fill_payload_error(
        self,
        order: OrderSnapshot,
        *,
        error_code: ErrorCode,
        message: str,
        context: Mapping[str, JsonValue],
    ) -> None:
        self._core.emit(
            EventType.ERROR,
            error_event_payload(
                category=ErrorCategory.RECONCILIATION,
                context={
                    "action_id": order.action_id,
                    "client_order_id": order.client_order_id,
                    "exchange_order_id": order.exchange_order_id,
                    **context,
                },
                error_code=error_code,
                message=message,
            ),
        )


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
