from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Protocol

from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, ExchangeLookupStatus, ProductVenue, ReconciliationIssue
from core.errors import error_event_payload
from core.json_tools import JsonValue
from exchanges.coinbase.advanced_trade_rest import CoinbaseAccountsLookupResult, CoinbasePositionsLookupResult
from projections.state import PositionSnapshot, SourceOfTruthProjection
from reconciliation.exchange_state_contract import (
    ExchangeStateSnapshotContractResult,
    validate_exchange_balance_snapshot,
    validate_exchange_position_snapshot,
)


class AccountLookupClient(Protocol):
    def list_accounts(
        self,
        *,
        cursor: str | None = None,
        limit: int = 250,
        retail_portfolio_id: str | None = None,
    ) -> CoinbaseAccountsLookupResult:
        ...


class PositionLookupClient(Protocol):
    def list_us_futures_positions(self) -> CoinbasePositionsLookupResult:
        ...

    def list_perpetual_positions(self, portfolio_uuid: str) -> CoinbasePositionsLookupResult:
        ...


@dataclass(frozen=True)
class ExchangeStateReconciliationPolicy:
    account_page_limit: int = 250
    max_account_pages: int = 10
    position_product_ids: tuple[str, ...] = ()
    position_size_tolerance: str = "0"
    retail_portfolio_id: str | None = None
    perpetual_portfolio_uuid: str | None = None

    def __post_init__(self) -> None:
        if self.account_page_limit <= 0:
            raise ValueError("account_page_limit must be positive")
        if self.max_account_pages <= 0:
            raise ValueError("max_account_pages must be positive")
        for product_id in self.position_product_ids:
            if not isinstance(product_id, str) or not product_id:
                raise TypeError("position_product_ids must contain non-empty strings")
        if len(self.position_product_ids) != len(set(self.position_product_ids)):
            raise ValueError("position_product_ids must be unique")
        if _decimal_or_none(self.position_size_tolerance) is None:
            raise ValueError("position_size_tolerance must be decimal-compatible")


@dataclass(frozen=True)
class ExchangeStateReconciliationResult:
    balance_snapshots: int
    drift_count: int
    error_count: int
    position_snapshots: int
    new_drift_record_count: int = 0


class ExchangeStateReconciliation:
    def __init__(
        self,
        core: AuditCore,
        *,
        account_lookup_client: AccountLookupClient | None = None,
        clock: Clock | None = None,
        policy: ExchangeStateReconciliationPolicy | None = None,
        position_lookup_client: PositionLookupClient | None = None,
    ) -> None:
        self._core = core
        self._account_lookup_client = account_lookup_client
        self._clock = clock or SystemClock()
        self._policy = policy or ExchangeStateReconciliationPolicy()
        self._position_lookup_client = position_lookup_client

    def reconcile(self) -> ExchangeStateReconciliationResult:
        projection = SourceOfTruthProjection.from_ledger(self._core.ledger)
        balance_snapshots, account_errors = self._snapshot_accounts()
        position_snapshots, actual_positions, position_errors = self._snapshot_positions()
        drift_count = 0
        new_drift_record_count = 0
        if self._position_lookup_client is not None and position_errors == 0:
            drift_count, new_drift_record_count = self._audit_position_drifts(projection, actual_positions)
        return ExchangeStateReconciliationResult(
            balance_snapshots=balance_snapshots,
            drift_count=drift_count,
            error_count=account_errors + position_errors,
            new_drift_record_count=new_drift_record_count,
            position_snapshots=position_snapshots,
        )

    def _snapshot_accounts(self) -> tuple[int, int]:
        if self._account_lookup_client is None:
            return 0, 0

        snapshots = 0
        errors = 0
        cursor: str | None = None
        for _ in range(self._policy.max_account_pages):
            lookup = self._account_lookup_client.list_accounts(
                cursor=cursor,
                limit=self._policy.account_page_limit,
                retail_portfolio_id=self._policy.retail_portfolio_id,
            )
            if lookup.status != ExchangeLookupStatus.FOUND:
                self._emit_lookup_error("accounts", lookup)
                return snapshots, errors + 1
            for account in lookup.accounts:
                payload = self._snapshot_payload(account)
                contract = validate_exchange_balance_snapshot(payload)
                if not contract.valid:
                    self._emit_snapshot_contract_error(
                        EventType.EXCHANGE_BALANCE_SNAPSHOT,
                        contract=contract,
                        payload=payload,
                    )
                    errors += 1
                    continue
                self._core.emit(EventType.EXCHANGE_BALANCE_SNAPSHOT, payload)
                snapshots += 1
            if not lookup.has_next:
                return snapshots, errors
            cursor = lookup.cursor
            if cursor is None:
                self._core.emit(
                    EventType.ERROR,
                    error_event_payload(
                        category=ErrorCategory.RECONCILIATION,
                        context={"lookup": "accounts"},
                        error_code=ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                        message="Coinbase accounts lookup indicated has_next without cursor",
                    ),
                )
                return snapshots, errors + 1

        self._core.emit(
            EventType.ERROR,
            error_event_payload(
                category=ErrorCategory.RECONCILIATION,
                context={
                    "lookup": "accounts",
                    "max_account_pages": self._policy.max_account_pages,
                },
                error_code=ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                message="Coinbase accounts pagination exceeded max_account_pages",
            ),
        )
        return snapshots, errors + 1

    def _snapshot_positions(self) -> tuple[int, dict[tuple[ProductVenue, str], dict[str, JsonValue]], int]:
        if self._position_lookup_client is None:
            return 0, {}, 0

        snapshots = 0
        errors = 0
        positions: dict[tuple[ProductVenue, str], dict[str, JsonValue]] = {}
        for venue, lookup in self._position_lookups():
            if lookup.status != ExchangeLookupStatus.FOUND:
                self._emit_lookup_error(f"{venue.value}_positions", lookup)
                errors += 1
                continue
            for position in lookup.positions:
                payload = self._snapshot_payload(position, venue=venue)
                contract = validate_exchange_position_snapshot(payload)
                if not contract.valid:
                    self._emit_snapshot_contract_error(
                        EventType.EXCHANGE_POSITION_SNAPSHOT,
                        contract=contract,
                        payload=payload,
                    )
                    errors += 1
                    continue
                product_id = _string_or_none(payload.get("product_id"))
                if product_id is not None:
                    positions[(venue, product_id)] = payload
                self._core.emit(EventType.EXCHANGE_POSITION_SNAPSHOT, payload)
                snapshots += 1
        return snapshots, positions, errors

    def _position_lookups(self) -> tuple[tuple[ProductVenue, CoinbasePositionsLookupResult], ...]:
        if self._position_lookup_client is None:
            return ()

        lookups = [(ProductVenue.FCM, self._position_lookup_client.list_us_futures_positions())]
        if self._policy.perpetual_portfolio_uuid is not None:
            lookups.append(
                (
                    ProductVenue.INTX,
                    self._position_lookup_client.list_perpetual_positions(self._policy.perpetual_portfolio_uuid),
                )
            )
        return tuple(lookups)

    def _audit_position_drifts(
        self,
        projection: SourceOfTruthProjection,
        actual_positions: dict[tuple[ProductVenue, str], dict[str, JsonValue]],
    ) -> tuple[int, int]:
        drift_count = 0
        new_drift_record_count = 0
        scoped_product_ids = set(self._policy.position_product_ids)
        bot_positions = {
            product_id: position
            for product_id, position in projection.positions_by_product_id.items()
            if _decimal_or_zero(position.net_size) != 0
            and (not scoped_product_ids or product_id in scoped_product_ids)
        }
        matched_products: set[str] = set()

        for (venue, product_id), actual in actual_positions.items():
            if scoped_product_ids and product_id not in scoped_product_ids:
                continue
            actual_size = _decimal_or_none(actual.get("net_size"))
            if actual_size is None:
                continue
            bot_position = bot_positions.get(product_id)
            if bot_position is None:
                if actual_size != 0:
                    drift_count += 1
                    new_drift_record_count += self._emit_drift(
                        exchange_size=actual_size,
                        issue=ReconciliationIssue.BOT_POSITION_MISSING,
                        product_id=product_id,
                        projection=projection,
                        venue=venue,
                    )
                continue

            matched_products.add(product_id)
            bot_size = _decimal_or_zero(bot_position.net_size)
            if abs(bot_size - actual_size) > _decimal_or_zero(self._policy.position_size_tolerance):
                drift_count += 1
                new_drift_record_count += self._emit_drift(
                    bot_size=bot_size,
                    exchange_size=actual_size,
                    issue=ReconciliationIssue.POSITION_SIZE_DRIFT,
                    product_id=product_id,
                    projection=projection,
                    venue=venue,
                )

        missing_products = set(bot_positions) - matched_products
        for product_id in sorted(missing_products):
            drift_count += 1
            new_drift_record_count += self._emit_drift(
                bot_size=_decimal_or_zero(bot_positions[product_id].net_size),
                issue=ReconciliationIssue.EXCHANGE_POSITION_MISSING,
                product_id=product_id,
                projection=projection,
                venue=None,
            )
        return drift_count, new_drift_record_count

    def _emit_drift(
        self,
        *,
        issue: ReconciliationIssue,
        product_id: str,
        projection: SourceOfTruthProjection,
        bot_size: Decimal | None = None,
        exchange_size: Decimal | None = None,
        venue: ProductVenue | None,
    ) -> int:
        drift_key = _drift_key(
            bot_size=bot_size,
            exchange_size=exchange_size,
            issue=issue,
            product_id=product_id,
            venue=venue,
        )
        if projection.has_reconciliation_drift(drift_key):
            return 0

        self._core.emit(
            EventType.RECONCILIATION_DRIFT,
            {
                "bot_net_size": _decimal_string(bot_size) if bot_size is not None else None,
                "drift_key": drift_key,
                "exchange_net_size": _decimal_string(exchange_size) if exchange_size is not None else None,
                "issue": issue.value,
                "observed_at": self._clock.now(),
                "product_id": product_id,
                "venue": venue.value if venue is not None else None,
            },
        )
        return 1

    def _snapshot_payload(self, payload: object, *, venue: ProductVenue | None = None) -> dict[str, JsonValue]:
        normalized = dict(payload) if isinstance(payload, dict) else {}
        normalized["observed_at"] = self._clock.now()
        if venue is not None:
            normalized["venue"] = venue.value
        return normalized

    def _emit_lookup_error(self, lookup_name: str, lookup: object) -> None:
        self._core.emit(
            EventType.ERROR,
            error_event_payload(
                category=ErrorCategory.RECONCILIATION,
                context={
                    "lookup": lookup_name,
                    "lookup_status": getattr(getattr(lookup, "status", None), "value", None),
                    "status_code": getattr(lookup, "status_code", None),
                },
                error_code=getattr(lookup, "error_code", None) or ErrorCode.RECONCILIATION_LOOKUP_FAILED,
                message=getattr(lookup, "error_message", None) or "Coinbase exchange-state lookup failed",
            ),
        )

    def _emit_snapshot_contract_error(
        self,
        event_type: EventType,
        *,
        contract: ExchangeStateSnapshotContractResult,
        payload: dict[str, JsonValue],
    ) -> None:
        self._core.emit(
            EventType.ERROR,
            error_event_payload(
                category=ErrorCategory.RECONCILIATION,
                context={
                    "event_type": event_type.value,
                    "invalid_fields": list(contract.invalid_fields),
                    "missing_fields": list(contract.missing_fields),
                    "raw_snapshot": payload,
                },
                error_code=ErrorCode.EXCHANGE_STATE_SNAPSHOT_INVALID,
                message=f"Coinbase exchange-state snapshot failed contract validation for {event_type.value}",
            ),
        )


def _drift_key(
    *,
    issue: ReconciliationIssue,
    product_id: str,
    bot_size: Decimal | None,
    exchange_size: Decimal | None,
    venue: ProductVenue | None,
) -> str:
    return ":".join(
        [
            issue.value,
            venue.value if venue is not None else "unknown_venue",
            product_id,
            _decimal_string(bot_size) if bot_size is not None else "missing_bot",
            _decimal_string(exchange_size) if exchange_size is not None else "missing_exchange",
        ]
    )


def _decimal_or_none(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_or_zero(value: object) -> Decimal:
    return _decimal_or_none(value) or Decimal("0")


def _decimal_string(value: Decimal | None) -> str:
    if value is None:
        return "0"
    return format(value.normalize(), "f")


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
