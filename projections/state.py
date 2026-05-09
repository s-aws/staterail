from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from audit.ledger import AuditLedger, AuditRecord
from core.enums import (
    ActionFailureReason,
    ActionRejectionReason,
    ActionStatus,
    ActionType,
    AnchorImmutabilityMode,
    AnchorStoreType,
    DigestAlgorithm,
    EventType,
    ExchangeOrderStatus,
    ExchangeLookupStatus,
    ExecutionMode,
    ExecutionStatus,
    FeedStatus,
    LedgerHealthStatus,
    ErrorCategory,
    ErrorCode,
    HttpMethod,
    MarginType,
    CoinbaseWebSocketChannel,
    OrderBookSide,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    ReconciliationIssue,
    ReadinessStatus,
    RuntimeComponent,
    RuntimeStopReason,
    RuntimeTask,
    StrategyEvaluationStatus,
    StrategySimulationStatus,
    TimeInForce,
    TriggerRelation,
)
from core.json_tools import JsonValue, normalize_json


@dataclass
class ActionSnapshot:
    action_id: str
    status: ActionStatus
    requested_sequence: int | None = None
    accepted_sequence: int | None = None
    execution_started_sequence: int | None = None
    failed_sequence: int | None = None
    failure_reason: ActionFailureReason | None = None
    rejected_sequence: int | None = None
    executed_sequence: int | None = None
    last_payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class DataMessageSnapshot:
    message_key: str
    source_id: str | None = None
    message_event_type: EventType | None = None
    received_sequences: list[int] = field(default_factory=list)
    accepted_sequence: int | None = None
    duplicate_sequences: list[int] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return self.accepted_sequence is not None


@dataclass
class MarketTickerSnapshot:
    product_id: str
    sequence: int
    message_key: str
    source_id: str | None = None
    ask_price: str | None = None
    ask_size: str | None = None
    bid_price: str | None = None
    bid_size: str | None = None
    exchange_sequence: int | None = None
    last_price: str | None = None
    observed_at: datetime | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    timestamp: str | None = None


@dataclass
class MarketOrderBookSnapshot:
    product_id: str
    sequence: int
    message_key: str
    source_id: str | None = None
    ask_levels: dict[str, str] = field(default_factory=dict)
    best_ask_price: str | None = None
    best_ask_size: str | None = None
    best_bid_price: str | None = None
    best_bid_size: str | None = None
    bid_levels: dict[str, str] = field(default_factory=dict)
    exchange_sequence: int | None = None
    observed_at: datetime | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    timestamp: str | None = None
    update_count: int = 0


@dataclass
class MarketTradeSnapshot:
    trade_id: str
    product_id: str
    sequence: int
    message_key: str
    source_id: str | None = None
    exchange_sequence: int | None = None
    observed_at: datetime | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    price: str | None = None
    side: OrderSide | None = None
    size: str | None = None
    trade_time: str | None = None


@dataclass
class FeedSourceSnapshot:
    source_id: str
    status: FeedStatus
    last_seen: str | None = None
    connected_count: int = 0
    disconnected_count: int = 0
    last_disconnect_reason: str | None = None
    last_reconnect_attempt: int | None = None
    last_reconnect_delay_seconds: float | None = None
    last_reconnect_scheduled_sequence: int | None = None
    reconnect_scheduled_count: int = 0


@dataclass
class FeedDegradationSnapshot:
    sequence: int
    connected_sources: tuple[str, ...] = ()
    disconnected_sources: tuple[str, ...] = ()
    live_count: int | None = None
    min_live_sources: int | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    stale_sources: tuple[str, ...] = ()


@dataclass
class OrderSnapshot:
    action_id: str
    lifecycle_status: OrderLifecycleStatus
    requested_sequence: int | None = None
    accepted_sequence: int | None = None
    execution_started_sequence: int | None = None
    executed_sequence: int | None = None
    terminal_sequence: int | None = None
    product_id: str | None = None
    side: OrderSide | None = None
    order_type: OrderType | None = None
    size: str | None = None
    limit_price: str | None = None
    leverage: str | None = None
    margin_type: MarginType | None = None
    post_only: bool | None = None
    reduce_only: bool | None = None
    time_in_force: TimeInForce | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    execution_mode: ExecutionMode | None = None
    execution_status: ExecutionStatus | None = None
    exchange_status: ExchangeOrderStatus | None = None
    cancel_action_ids: list[str] = field(default_factory=list)
    average_fill_price: str | None = None
    fill_ids: list[str] = field(default_factory=list)
    filled_size: str = "0"
    last_execution_result: dict[str, JsonValue] = field(default_factory=dict)
    last_exchange_update: dict[str, JsonValue] = field(default_factory=dict)
    total_fees: str = "0"


@dataclass
class LogicalOrderSnapshot:
    logical_order_id: str
    root_order_id: str
    lineage_relation: OrderLineageRelation
    product_id: str
    side: OrderSide
    size: str
    sequence: int
    child_order_ids: list[str] = field(default_factory=list)
    created_by_action_id: str | None = None
    limit_price: str | None = None
    parent_order_id: str | None = None
    placement_ids: list[str] = field(default_factory=list)
    payload: dict[str, JsonValue] = field(default_factory=dict)
    source_order_ids: tuple[str, ...] = ()


@dataclass
class OrderPlacementSnapshot:
    placement_id: str
    logical_order_id: str
    placement_kind: OrderPlacementKind
    placement_status: OrderPlacementStatus
    product_id: str
    side: OrderSide
    size: str
    sequence: int
    action_id: str | None = None
    exchange_order_id: str | None = None
    limit_price: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    release_of_action_id: str | None = None
    release_of_placement_id: str | None = None
    venue_client_order_id: str | None = None


@dataclass
class PassiveMarketMakingQuoteSnapshot:
    placement_id: str
    logical_order_id: str
    product_id: str
    side: OrderSide
    size: str
    sequence: int
    action_id: str | None = None
    ask_price: str | None = None
    bid_price: str | None = None
    half_spread_bps: str | None = None
    limit_price: str | None = None
    midpoint: str | None = None
    released: bool = False
    release_placement_id: str | None = None


@dataclass
class FillSnapshot:
    fill_id: str
    sequence: int
    order_id: str | None = None
    trade_id: str | None = None
    product_id: str | None = None
    side: OrderSide | None = None
    price: str | None = None
    size: str | None = None
    commission: str | None = None
    trade_time: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class PositionSnapshot:
    product_id: str
    net_size: str = "0"
    gross_buy_size: str = "0"
    gross_sell_size: str = "0"
    gross_buy_notional: str = "0"
    gross_sell_notional: str = "0"
    total_fees: str = "0"
    fill_count: int = 0


@dataclass
class ExchangeBalanceSnapshot:
    account_id: str
    currency: str | None
    sequence: int
    available: str | None = None
    hold: str | None = None
    venue: ProductVenue | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class ExchangePositionSnapshot:
    product_id: str
    venue: ProductVenue
    sequence: int
    net_size: str | None = None
    side: str | None = None
    average_entry_price: str | None = None
    current_price: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class ExchangeProductSnapshot:
    product_id: str
    product_type: ProductType
    product_venue: ProductVenue
    sequence: int
    cancel_only: bool | None = None
    is_disabled: bool | None = None
    limit_only: bool | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    post_only: bool | None = None
    trading_disabled: bool | None = None
    tradable_for_new_orders: bool | None = None
    view_only: bool | None = None


@dataclass
class ExchangeRequestRetrySnapshot:
    sequence: int
    method: HttpMethod
    attempt: int
    next_attempt: int
    max_attempts: int
    delay_seconds: float
    error_category: ErrorCategory | None = None
    error_code: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    status_code: int | None = None
    url: str | None = None


@dataclass
class ErrorSnapshot:
    sequence: int
    category: ErrorCategory | None = None
    code: ErrorCode | None = None
    exception_type: str | None = None
    message: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    retryable: bool | None = None


@dataclass
class ReconciliationMismatchSnapshot:
    action_id: str
    reason: ReconciliationIssue
    sequence: int
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class ReconciliationRecoverySnapshot:
    action_id: str
    reason: ReconciliationIssue
    lookup_status: ExchangeLookupStatus
    sequence: int
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class ReconciliationDriftSnapshot:
    drift_key: str
    issue: ReconciliationIssue
    product_id: str
    sequence: int
    venue: ProductVenue | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class RuntimeTaskSnapshot:
    task_id: RuntimeTask
    completed_count: int = 0
    last_completed_sequence: int | None = None
    last_error_sequence: int | None = None
    last_result: JsonValue = field(default_factory=dict)
    last_started_sequence: int | None = None
    started_count: int = 0


@dataclass
class SystemStartSnapshot:
    sequence: int
    component: RuntimeComponent | None = None
    startup_metadata: dict[str, JsonValue] = field(default_factory=dict)
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class SystemStopSnapshot:
    sequence: int
    component: RuntimeComponent | None = None
    completed_cycles: int | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    reason: RuntimeStopReason | None = None
    stopped_at: str | None = None


@dataclass
class TriggerSnapshot:
    sequence: int
    trigger_id: str
    matched_event_type: EventType | None = None
    matched_sequence: int | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    relation: TriggerRelation | None = None
    target_time: str | None = None


@dataclass
class StrategyEvaluationSnapshot:
    strategy_id: str
    status: StrategyEvaluationStatus
    started_sequence: int
    action_ids: list[str] = field(default_factory=list)
    as_of_sequence: int | None = None
    closed_sequence: int | None = None
    error_sequence: int | None = None
    intent_count: int = 0
    payload: dict[str, JsonValue] = field(default_factory=dict)
    submitted_action_count: int = 0


@dataclass
class LedgerAnchorSnapshot:
    sequence: int
    artifact_uri: str
    checkpoint_hash: str
    checkpoint_through_sequence: int
    immutability_mode: AnchorImmutabilityMode | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    retention_until: str | None = None
    store_metadata: dict[str, JsonValue] = field(default_factory=dict)
    store_type: AnchorStoreType | None = None
    version_id: str | None = None


@dataclass
class LedgerArchiveSnapshot:
    sequence: int
    artifact_uri: str
    record_count: int
    through_hash: str
    through_sequence: int
    immutability_mode: AnchorImmutabilityMode | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    retention_until: str | None = None
    store_metadata: dict[str, JsonValue] = field(default_factory=dict)
    store_type: AnchorStoreType | None = None
    version_id: str | None = None


@dataclass
class LedgerCheckpointSnapshot:
    sequence: int
    checkpoint_hash: str
    created_at: str | None = None
    digest_algorithm: DigestAlgorithm | None = None
    ledger_path: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    record_count: int | None = None
    records_digest: str | None = None
    through_hash: str | None = None
    through_sequence: int | None = None


@dataclass
class LivePreflightResultSnapshot:
    sequence: int
    status: ReadinessStatus | None = None
    completed_step_names: tuple[str, ...] = ()
    config_fingerprint: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class RuntimeHealthCheckResultSnapshot:
    sequence: int
    checked_health_status: LedgerHealthStatus | None = None
    checked_through_sequence: int | None = None
    attention_check_count: int | None = None
    attention_checks: tuple[str, ...] = ()
    payload: dict[str, JsonValue] = field(default_factory=dict)
    record_count: int | None = None


@dataclass
class StrategySimulationResultSnapshot:
    sequence: int
    status: StrategySimulationStatus | None = None
    accepted_action_count: int = 0
    config_fingerprint: str | None = None
    failed_count: int = 0
    rejected_action_count: int = 0
    strategy_ids: tuple[str, ...] = ()
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass
class LedgerHealthAcknowledgementSnapshot:
    sequence: int
    acknowledged_by: str | None = None
    acknowledged_health_status: LedgerHealthStatus | None = None
    acknowledged_through_hash: str | None = None
    acknowledged_through_sequence: int | None = None
    attention_check_count: int | None = None
    ledger_health_attention_digest: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)
    reason: str | None = None


class SourceOfTruthProjection:
    def __init__(
        self,
        *,
        max_market_trades_per_product: int | None = None,
        max_order_book_sample_depth_per_side: int | None = None,
        max_order_book_samples_per_product: int = 1,
        order_book_sample_product_ids: tuple[str, ...] = (),
    ) -> None:
        _validate_optional_positive_int(
            max_market_trades_per_product,
            "max_market_trades_per_product",
        )
        _validate_optional_positive_int(
            max_order_book_sample_depth_per_side,
            "max_order_book_sample_depth_per_side",
        )
        _validate_positive_int(
            max_order_book_samples_per_product,
            "max_order_book_samples_per_product",
        )
        _validate_string_scope(
            order_book_sample_product_ids,
            "order_book_sample_product_ids",
        )
        self.actions: dict[str, ActionSnapshot] = {}
        self.audit_anchors: list[LedgerAnchorSnapshot] = []
        self.audit_archives: list[LedgerArchiveSnapshot] = []
        self.audit_checkpoints: list[LedgerCheckpointSnapshot] = []
        self.live_preflight_results: list[LivePreflightResultSnapshot] = []
        self.ledger_health_acknowledgements: list[LedgerHealthAcknowledgementSnapshot] = []
        self.runtime_health_check_results: list[RuntimeHealthCheckResultSnapshot] = []
        self.strategy_simulation_results: list[StrategySimulationResultSnapshot] = []
        self.data_messages: dict[str, DataMessageSnapshot] = {}
        self.feed_degradations: list[FeedDegradationSnapshot] = []
        self.feed_sources: dict[str, FeedSourceSnapshot] = {}
        self.latest_tickers_by_product_id: dict[str, MarketTickerSnapshot] = {}
        self.order_books_by_product_id: dict[str, MarketOrderBookSnapshot] = {}
        self.order_book_samples_by_product_id: dict[str, list[MarketOrderBookSnapshot]] = {}
        self.order_book_sample_retention_dropped_by_product_id: dict[str, int] = {}
        self.order_book_sample_scope_skipped_by_product_id: dict[str, int] = {}
        self.max_order_book_sample_depth_per_side = max_order_book_sample_depth_per_side
        self.max_order_book_samples_per_product = max_order_book_samples_per_product
        self.order_book_sample_product_ids = order_book_sample_product_ids
        self.market_trades_by_id: dict[str, MarketTradeSnapshot] = {}
        self.market_trade_ids_by_product_id: dict[str, list[str]] = {}
        self.market_trade_retention_dropped_by_product_id: dict[str, int] = {}
        self.max_market_trades_per_product = max_market_trades_per_product
        self.orders_by_action_id: dict[str, OrderSnapshot] = {}
        self.orders_by_client_order_id: dict[str, OrderSnapshot] = {}
        self.orders_by_exchange_order_id: dict[str, OrderSnapshot] = {}
        self.logical_order_id_by_action_id: dict[str, str] = {}
        self.logical_orders_by_id: dict[str, LogicalOrderSnapshot] = {}
        self.placement_ids_by_action_id: dict[str, list[str]] = {}
        self.placements_by_id: dict[str, OrderPlacementSnapshot] = {}
        self.placements_by_logical_order_id: dict[str, list[OrderPlacementSnapshot]] = {}
        self.release_placement_id_by_staged_placement_id: dict[str, str] = {}
        self.fills_by_id: dict[str, FillSnapshot] = {}
        self.fills_by_order_id: dict[str, list[FillSnapshot]] = {}
        self.positions_by_product_id: dict[str, PositionSnapshot] = {}
        self.exchange_balances_by_account_id: dict[str, ExchangeBalanceSnapshot] = {}
        self.exchange_positions_by_venue_product: dict[tuple[ProductVenue, str], ExchangePositionSnapshot] = {}
        self.exchange_products_by_product_id: dict[str, ExchangeProductSnapshot] = {}
        self.exchange_product_snapshot_sequences: list[int] = []
        self.exchange_request_retries: list[ExchangeRequestRetrySnapshot] = []
        self.errors: list[ErrorSnapshot] = []
        self.reconciliation_drifts: dict[str, ReconciliationDriftSnapshot] = {}
        self.reconciliation_mismatches: dict[tuple[str, ReconciliationIssue], ReconciliationMismatchSnapshot] = {}
        self.reconciliation_recoveries: dict[tuple[str, ReconciliationIssue], ReconciliationRecoverySnapshot] = {}
        self.runtime_tasks: dict[RuntimeTask, RuntimeTaskSnapshot] = {}
        self.strategy_evaluations: list[StrategyEvaluationSnapshot] = []
        self.strategy_evaluations_by_started_sequence: dict[int, StrategyEvaluationSnapshot] = {}
        self.system_starts: list[SystemStartSnapshot] = []
        self.system_stops: list[SystemStopSnapshot] = []
        self.trigger_firings: list[TriggerSnapshot] = []
        self.record_occurred_at_by_sequence: dict[int, datetime] = {}
        self._received_data_payloads_by_sequence: dict[int, dict[str, JsonValue]] = {}
        self.error_sequences: list[int] = []
        self.feed_degraded_sequences: list[int] = []
        self.trigger_sequences: list[int] = []
        self.sequence_gap_sequences: list[int] = []
        self.out_of_order_sequences: list[int] = []
        self.last_sequence = 0
        self.last_record_hash: str | None = None

    @classmethod
    def from_ledger(
        cls,
        ledger: AuditLedger,
        *,
        max_market_trades_per_product: int | None = None,
        max_order_book_sample_depth_per_side: int | None = None,
        max_order_book_samples_per_product: int = 1,
        order_book_sample_product_ids: tuple[str, ...] = (),
    ) -> "SourceOfTruthProjection":
        projection = cls(
            max_market_trades_per_product=max_market_trades_per_product,
            max_order_book_sample_depth_per_side=max_order_book_sample_depth_per_side,
            max_order_book_samples_per_product=max_order_book_samples_per_product,
            order_book_sample_product_ids=order_book_sample_product_ids,
        )
        for record in ledger.iter_records():
            projection.apply(record)
        return projection

    @classmethod
    def from_records(
        cls,
        records: Iterable[AuditRecord],
        *,
        max_market_trades_per_product: int | None = None,
        max_order_book_sample_depth_per_side: int | None = None,
        max_order_book_samples_per_product: int = 1,
        order_book_sample_product_ids: tuple[str, ...] = (),
    ) -> "SourceOfTruthProjection":
        projection = cls(
            max_market_trades_per_product=max_market_trades_per_product,
            max_order_book_sample_depth_per_side=max_order_book_sample_depth_per_side,
            max_order_book_samples_per_product=max_order_book_samples_per_product,
            order_book_sample_product_ids=order_book_sample_product_ids,
        )
        for record in records:
            projection.apply(record)
        return projection

    def apply(self, record: AuditRecord) -> None:
        self.last_sequence = record.sequence
        self.last_record_hash = record.record_hash
        self.record_occurred_at_by_sequence[record.sequence] = record.occurred_at

        if record.event_type in _ACTION_EVENTS:
            self._apply_action(record)
        elif record.event_type == EventType.AUDIT_ANCHOR_PUBLISHED:
            self._apply_audit_anchor(record)
        elif record.event_type == EventType.AUDIT_LEDGER_ARCHIVED:
            self._apply_audit_archive(record)
        elif record.event_type == EventType.AUDIT_CHECKPOINT:
            self._apply_audit_checkpoint(record)
        elif record.event_type in {EventType.DATA_RECEIVED, EventType.DATA_ACCEPTED, EventType.DATA_DUPLICATE}:
            self._apply_data(record)
        elif record.event_type in _FEED_EVENTS:
            self._apply_feed(record)
        elif record.event_type == EventType.FEED_DEGRADED:
            self.feed_degraded_sequences.append(record.sequence)
            self._apply_feed_degraded(record)
        elif record.event_type == EventType.ERROR:
            self.error_sequences.append(record.sequence)
            self._apply_error(record)
            self._apply_runtime_task_error(record)
        elif record.event_type == EventType.EXCHANGE_BALANCE_SNAPSHOT:
            self._apply_exchange_balance_snapshot(record)
        elif record.event_type == EventType.EXCHANGE_FILL:
            self._apply_exchange_fill(record)
        elif record.event_type == EventType.EXCHANGE_POSITION_SNAPSHOT:
            self._apply_exchange_position_snapshot(record)
        elif record.event_type == EventType.EXCHANGE_PRODUCT_SNAPSHOT:
            self._apply_exchange_product_snapshot(record)
        elif record.event_type == EventType.EXCHANGE_REQUEST_RETRY:
            self._apply_exchange_request_retry(record)
        elif record.event_type == EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED:
            self._apply_ledger_health_acknowledgement(record)
        elif record.event_type == EventType.LIVE_PREFLIGHT_RESULT:
            self._apply_live_preflight_result(record)
        elif record.event_type == EventType.RUNTIME_HEALTH_CHECK_RESULT:
            self._apply_runtime_health_check_result(record)
        elif record.event_type == EventType.STRATEGY_SIMULATION_RESULT:
            self._apply_strategy_simulation_result(record)
        elif record.event_type == EventType.ORDER_LOGICAL_CREATED:
            self._apply_logical_order_created(record)
        elif record.event_type == EventType.ORDER_PLACEMENT_RECORDED:
            self._apply_order_placement_recorded(record)
        elif record.event_type == EventType.RECONCILIATION_DRIFT:
            self._apply_reconciliation_drift(record)
        elif record.event_type == EventType.RECONCILIATION_MISMATCH:
            self._apply_reconciliation_mismatch(record)
        elif record.event_type == EventType.RECONCILIATION_RECOVERY:
            self._apply_reconciliation_recovery(record)
        elif record.event_type in {EventType.RUNTIME_TASK_COMPLETED, EventType.RUNTIME_TASK_STARTED}:
            self._apply_runtime_task(record)
        elif record.event_type in {
            EventType.STRATEGY_EVALUATION_COMPLETED,
            EventType.STRATEGY_EVALUATION_FAILED,
            EventType.STRATEGY_EVALUATION_STARTED,
        }:
            self._apply_strategy_evaluation(record)
        elif record.event_type == EventType.SYSTEM_STARTED:
            self._apply_system_started(record)
        elif record.event_type == EventType.SYSTEM_STOPPED:
            self._apply_system_stopped(record)
        elif record.event_type == EventType.TRIGGER_FIRED:
            self.trigger_sequences.append(record.sequence)
            self._apply_trigger_fired(record)

    @property
    def accepted_data_count(self) -> int:
        return sum(1 for message in self.data_messages.values() if message.accepted)

    @property
    def duplicate_data_count(self) -> int:
        return sum(len(message.duplicate_sequences) for message in self.data_messages.values())

    @property
    def error_count(self) -> int:
        return len(self.error_sequences)

    @property
    def open_orders(self) -> tuple[OrderSnapshot, ...]:
        return tuple(
            order
            for order in self.orders_by_action_id.values()
            if order.lifecycle_status == OrderLifecycleStatus.OPEN
        )

    @property
    def staged_order_placements(self) -> tuple[OrderPlacementSnapshot, ...]:
        return self.placements_by_status(OrderPlacementStatus.STAGED)

    @property
    def unreleased_staged_order_placements(self) -> tuple[OrderPlacementSnapshot, ...]:
        return tuple(
            placement
            for placement in self.staged_order_placements
            if placement.placement_id not in self.release_placement_id_by_staged_placement_id
        )

    @property
    def passive_market_making_quotes(self) -> tuple[PassiveMarketMakingQuoteSnapshot, ...]:
        quotes: list[PassiveMarketMakingQuoteSnapshot] = []
        for placement in self.staged_order_placements:
            if placement.placement_kind != OrderPlacementKind.STAGED_RELEASE:
                continue
            metadata = _passive_market_making_metadata(placement.payload)
            if metadata is None:
                continue
            release_placement_id = self.release_placement_id_by_staged_placement_id.get(
                placement.placement_id
            )
            quotes.append(
                PassiveMarketMakingQuoteSnapshot(
                    action_id=placement.action_id,
                    ask_price=_string_or_none(metadata.get("ask_price")),
                    bid_price=_string_or_none(metadata.get("bid_price")),
                    half_spread_bps=_string_or_none(metadata.get("half_spread_bps")),
                    limit_price=placement.limit_price,
                    logical_order_id=placement.logical_order_id,
                    midpoint=_string_or_none(metadata.get("midpoint")),
                    placement_id=placement.placement_id,
                    product_id=placement.product_id,
                    release_placement_id=release_placement_id,
                    released=release_placement_id is not None,
                    sequence=placement.sequence,
                    side=placement.side,
                    size=placement.size,
                )
            )
        return tuple(quotes)

    @property
    def unreleased_passive_market_making_quotes(
        self,
    ) -> tuple[PassiveMarketMakingQuoteSnapshot, ...]:
        return tuple(quote for quote in self.passive_market_making_quotes if not quote.released)

    @property
    def released_staged_placement_ids(self) -> tuple[str, ...]:
        return tuple(self.release_placement_id_by_staged_placement_id)

    @property
    def reconciliation_mismatch_count(self) -> int:
        return len(self.reconciliation_mismatches)

    @property
    def reconciliation_recovery_count(self) -> int:
        return len(self.reconciliation_recoveries)

    @property
    def fill_count(self) -> int:
        return len(self.fills_by_id)

    @property
    def market_trade_count(self) -> int:
        return len(self.market_trades_by_id)

    @property
    def market_trade_retention_dropped_count(self) -> int:
        return sum(self.market_trade_retention_dropped_by_product_id.values())

    @property
    def order_book_sample_retention_dropped_count(self) -> int:
        return sum(self.order_book_sample_retention_dropped_by_product_id.values())

    @property
    def order_book_sample_scope_skipped_count(self) -> int:
        return sum(self.order_book_sample_scope_skipped_by_product_id.values())

    def latest_ticker(self, product_id: str) -> MarketTickerSnapshot | None:
        return self.latest_tickers_by_product_id.get(product_id)

    def order_book(self, product_id: str) -> MarketOrderBookSnapshot | None:
        return self.order_books_by_product_id.get(product_id)

    def order_book_samples_for_product(
        self,
        product_id: str,
    ) -> tuple[MarketOrderBookSnapshot, ...]:
        return tuple(self.order_book_samples_by_product_id.get(product_id, ()))

    def market_trades_for_product(self, product_id: str) -> tuple[MarketTradeSnapshot, ...]:
        return tuple(
            self.market_trades_by_id[trade_id]
            for trade_id in self.market_trade_ids_by_product_id.get(product_id, ())
            if trade_id in self.market_trades_by_id
        )

    @property
    def reconciliation_drift_count(self) -> int:
        return len(self.reconciliation_drifts)

    @property
    def audit_anchor_count(self) -> int:
        return len(self.audit_anchors)

    @property
    def audit_archive_count(self) -> int:
        return len(self.audit_archives)

    @property
    def audit_checkpoint_count(self) -> int:
        return len(self.audit_checkpoints)

    @property
    def exchange_product_count(self) -> int:
        return len(self.exchange_products_by_product_id)

    @property
    def exchange_product_snapshot_count(self) -> int:
        return len(self.exchange_product_snapshot_sequences)

    @property
    def feed_degraded_count(self) -> int:
        return len(self.feed_degraded_sequences)

    def has_reconciliation_mismatch(self, *, action_id: str, reason: ReconciliationIssue) -> bool:
        return (action_id, reason) in self.reconciliation_mismatches

    def has_reconciliation_recovery(self, *, action_id: str, reason: ReconciliationIssue) -> bool:
        return (action_id, reason) in self.reconciliation_recoveries

    def has_fill(self, fill_id: str) -> bool:
        return fill_id in self.fills_by_id

    def placements_by_status(self, status: OrderPlacementStatus) -> tuple[OrderPlacementSnapshot, ...]:
        if not isinstance(status, OrderPlacementStatus):
            raise TypeError("status must be an OrderPlacementStatus")
        return tuple(
            placement
            for placement in self.placements_by_id.values()
            if placement.placement_status == status
        )

    def placements_for_action(self, action_id: str) -> tuple[OrderPlacementSnapshot, ...]:
        placement_ids = self.placement_ids_by_action_id.get(action_id, [])
        return tuple(
            self.placements_by_id[placement_id]
            for placement_id in placement_ids
            if placement_id in self.placements_by_id
        )

    def placements_for_logical_order(self, logical_order_id: str) -> tuple[OrderPlacementSnapshot, ...]:
        return tuple(self.placements_by_logical_order_id.get(logical_order_id, ()))

    def latest_placement_for_logical_order(self, logical_order_id: str) -> OrderPlacementSnapshot | None:
        placements = self.placements_for_logical_order(logical_order_id)
        if not placements:
            return None
        return max(placements, key=lambda placement: placement.sequence)

    def release_placement_for_staged_placement(
        self,
        staged_placement_id: str,
    ) -> OrderPlacementSnapshot | None:
        release_placement_id = self.release_placement_id_by_staged_placement_id.get(
            staged_placement_id
        )
        if release_placement_id is None:
            return None
        return self.placements_by_id.get(release_placement_id)

    def has_reconciliation_drift(self, drift_key: str) -> bool:
        return drift_key in self.reconciliation_drifts

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "actions": self.actions,
            "audit_anchors": self.audit_anchors,
            "audit_archives": self.audit_archives,
            "audit_checkpoints": self.audit_checkpoints,
            "data_messages": self.data_messages,
            "errors": self.errors,
            "error_sequences": self.error_sequences,
            "exchange_balances": self.exchange_balances_by_account_id,
            "exchange_positions": {
                f"{venue.value}:{product_id}": snapshot
                for (venue, product_id), snapshot in self.exchange_positions_by_venue_product.items()
            },
            "exchange_product_snapshot_sequences": self.exchange_product_snapshot_sequences,
            "exchange_products": self.exchange_products_by_product_id,
            "exchange_request_retries": self.exchange_request_retries,
            "feed_degradations": self.feed_degradations,
            "feed_degraded_sequences": self.feed_degraded_sequences,
            "feed_sources": self.feed_sources,
            "fills": self.fills_by_id,
            "last_record_hash": self.last_record_hash,
            "last_sequence": self.last_sequence,
            "ledger_health_acknowledgements": self.ledger_health_acknowledgements,
            "live_preflight_results": self.live_preflight_results,
            "logical_order_id_by_action_id": self.logical_order_id_by_action_id,
            "logical_orders": self.logical_orders_by_id,
            "market_order_books": self.order_books_by_product_id,
            "market_order_book_sample_retention": {
                "dropped_by_product_id": self.order_book_sample_retention_dropped_by_product_id,
                "dropped_count": self.order_book_sample_retention_dropped_count,
                "max_order_book_sample_depth_per_side": self.max_order_book_sample_depth_per_side,
                "max_order_book_samples_per_product": self.max_order_book_samples_per_product,
                "order_book_sample_product_ids": list(self.order_book_sample_product_ids),
                "scope_skipped_by_product_id": self.order_book_sample_scope_skipped_by_product_id,
                "scope_skipped_count": self.order_book_sample_scope_skipped_count,
            },
            "market_order_book_samples_by_product_id": self.order_book_samples_by_product_id,
            "market_tickers": self.latest_tickers_by_product_id,
            "market_trade_retention": {
                "dropped_by_product_id": self.market_trade_retention_dropped_by_product_id,
                "dropped_count": self.market_trade_retention_dropped_count,
                "max_market_trades_per_product": self.max_market_trades_per_product,
            },
            "market_trade_ids_by_product_id": self.market_trade_ids_by_product_id,
            "market_trades": self.market_trades_by_id,
            "open_order_action_ids": [order.action_id for order in self.open_orders],
            "orders": self.orders_by_action_id,
            "out_of_order_sequences": self.out_of_order_sequences,
            "order_placements": self.placements_by_id,
            "passive_market_making_quotes": {
                quote.placement_id: quote for quote in self.passive_market_making_quotes
            },
            "placement_ids_by_action_id": self.placement_ids_by_action_id,
            "positions": self.positions_by_product_id,
            "release_placement_id_by_staged_placement_id": (
                self.release_placement_id_by_staged_placement_id
            ),
            "unreleased_passive_market_making_quote_ids": [
                quote.placement_id for quote in self.unreleased_passive_market_making_quotes
            ],
            "unreleased_staged_placement_ids": [
                placement.placement_id for placement in self.unreleased_staged_order_placements
            ],
            "reconciliation_drifts": self.reconciliation_drifts,
            "reconciliation_mismatches": {
                f"{action_id}:{reason.value}": snapshot
                for (action_id, reason), snapshot in self.reconciliation_mismatches.items()
            },
            "reconciliation_recoveries": {
                f"{action_id}:{reason.value}": snapshot
                for (action_id, reason), snapshot in self.reconciliation_recoveries.items()
            },
            "runtime_tasks": {task_id.value: snapshot for task_id, snapshot in self.runtime_tasks.items()},
            "runtime_health_check_results": self.runtime_health_check_results,
            "sequence_gap_sequences": self.sequence_gap_sequences,
            "strategy_evaluations": self.strategy_evaluations,
            "strategy_simulation_results": self.strategy_simulation_results,
            "system_starts": self.system_starts,
            "system_stops": self.system_stops,
            "trigger_firings": self.trigger_firings,
            "trigger_sequences": self.trigger_sequences,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Source-of-truth payload must normalize to an object")
        return normalized

    def _apply_runtime_task(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        task_id = _runtime_task_or_none(payload.get("task_id"))
        if task_id is None:
            self.error_sequences.append(record.sequence)
            return

        task = self.runtime_tasks.get(task_id)
        if task is None:
            task = RuntimeTaskSnapshot(task_id=task_id)
            self.runtime_tasks[task_id] = task

        if record.event_type == EventType.RUNTIME_TASK_STARTED:
            task.started_count += 1
            task.last_started_sequence = record.sequence
        elif record.event_type == EventType.RUNTIME_TASK_COMPLETED:
            task.completed_count += 1
            task.last_completed_sequence = record.sequence
            task.last_result = payload.get("result")

    def _apply_runtime_task_error(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        task_id = _runtime_task_or_none(payload.get("task_id"))
        if task_id is None:
            return

        task = self.runtime_tasks.get(task_id)
        if task is None:
            task = RuntimeTaskSnapshot(task_id=task_id)
            self.runtime_tasks[task_id] = task
        task.last_error_sequence = record.sequence

    def _apply_runtime_health_check_result(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.runtime_health_check_results.append(
            RuntimeHealthCheckResultSnapshot(
                attention_check_count=_int_or_none(payload.get("attention_check_count")),
                attention_checks=_string_tuple(payload.get("attention_checks")),
                checked_health_status=_ledger_health_status_or_none(
                    payload.get("checked_health_status")
                ),
                checked_through_sequence=_int_or_none(payload.get("checked_through_sequence")),
                payload=payload,
                record_count=_int_or_none(payload.get("record_count")),
                sequence=record.sequence,
            )
        )

    def _apply_strategy_evaluation(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        strategy_id = _string_or_none(payload.get("strategy_id"))
        if strategy_id is None:
            self.error_sequences.append(record.sequence)
            return

        if record.event_type == EventType.STRATEGY_EVALUATION_STARTED:
            snapshot = StrategyEvaluationSnapshot(
                as_of_sequence=_int_or_none(payload.get("as_of_sequence")),
                payload=payload,
                started_sequence=record.sequence,
                status=StrategyEvaluationStatus.STARTED,
                strategy_id=strategy_id,
            )
            self.strategy_evaluations.append(snapshot)
            self.strategy_evaluations_by_started_sequence[record.sequence] = snapshot
            return

        started_sequence = _int_or_none(payload.get("started_sequence"))
        snapshot = (
            self.strategy_evaluations_by_started_sequence.get(started_sequence)
            if started_sequence is not None
            else None
        )
        if snapshot is None:
            self.error_sequences.append(record.sequence)
            return

        snapshot.action_ids = _strategy_action_ids(payload.get("action_receipts"))
        snapshot.as_of_sequence = _int_or_none(payload.get("as_of_sequence")) or snapshot.as_of_sequence
        snapshot.closed_sequence = record.sequence
        snapshot.error_sequence = _int_or_none(payload.get("error_sequence"))
        snapshot.intent_count = _int_or_none(payload.get("intent_count")) or 0
        snapshot.payload = payload
        snapshot.status = _strategy_evaluation_status_or_none(payload.get("status")) or (
            StrategyEvaluationStatus.FAILED
            if record.event_type == EventType.STRATEGY_EVALUATION_FAILED
            else StrategyEvaluationStatus.COMPLETED
        )
        snapshot.submitted_action_count = _int_or_none(payload.get("submitted_action_count")) or len(snapshot.action_ids)

    def _apply_system_started(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.system_starts.append(
            SystemStartSnapshot(
                component=_runtime_component_or_none(payload.get("component")),
                payload=payload,
                sequence=record.sequence,
                startup_metadata=_payload_dict(payload.get("startup_metadata")),
            )
        )

    def _apply_system_stopped(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.system_stops.append(
            SystemStopSnapshot(
                completed_cycles=_int_or_none(payload.get("completed_cycles")),
                component=_runtime_component_or_none(payload.get("component")),
                payload=payload,
                reason=_runtime_stop_reason_or_none(payload.get("reason")),
                sequence=record.sequence,
                stopped_at=_string_or_none(payload.get("stopped_at")),
            )
        )

    def _apply_audit_anchor(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        artifact_uri = _string_or_none(payload.get("artifact_uri"))
        checkpoint_hash = _string_or_none(payload.get("checkpoint_hash"))
        checkpoint_through_sequence = _int_or_none(payload.get("checkpoint_through_sequence"))
        if artifact_uri is None or checkpoint_hash is None or checkpoint_through_sequence is None:
            self.error_sequences.append(record.sequence)
            return
        self.audit_anchors.append(
            LedgerAnchorSnapshot(
                artifact_uri=artifact_uri,
                checkpoint_hash=checkpoint_hash,
                checkpoint_through_sequence=checkpoint_through_sequence,
                immutability_mode=_anchor_immutability_mode_or_none(payload.get("immutability_mode")),
                payload=payload,
                retention_until=_string_or_none(payload.get("retention_until")),
                sequence=record.sequence,
                store_metadata=_payload_dict(payload.get("store_metadata")),
                store_type=_anchor_store_type_or_none(payload.get("store_type")),
                version_id=_string_or_none(payload.get("version_id")),
            )
        )

    def _apply_audit_archive(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        artifact_uri = _string_or_none(payload.get("artifact_uri"))
        record_count = _int_or_none(payload.get("record_count"))
        through_hash = _string_or_none(payload.get("through_hash"))
        through_sequence = _int_or_none(payload.get("through_sequence"))
        if artifact_uri is None or record_count is None or through_hash is None or through_sequence is None:
            self.error_sequences.append(record.sequence)
            return
        self.audit_archives.append(
            LedgerArchiveSnapshot(
                artifact_uri=artifact_uri,
                immutability_mode=_anchor_immutability_mode_or_none(payload.get("immutability_mode")),
                payload=payload,
                record_count=record_count,
                retention_until=_string_or_none(payload.get("retention_until")),
                sequence=record.sequence,
                store_metadata=_payload_dict(payload.get("store_metadata")),
                store_type=_anchor_store_type_or_none(payload.get("store_type")),
                through_hash=through_hash,
                through_sequence=through_sequence,
                version_id=_string_or_none(payload.get("version_id")),
            )
        )

    def _apply_audit_checkpoint(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        checkpoint_hash = _string_or_none(payload.get("checkpoint_hash"))
        if checkpoint_hash is None:
            self.error_sequences.append(record.sequence)
            return
        self.audit_checkpoints.append(
            LedgerCheckpointSnapshot(
                checkpoint_hash=checkpoint_hash,
                created_at=_string_or_none(payload.get("created_at")),
                digest_algorithm=_digest_algorithm_or_none(payload.get("digest_algorithm")),
                ledger_path=_string_or_none(payload.get("ledger_path")),
                payload=payload,
                record_count=_int_or_none(payload.get("record_count")),
                records_digest=_string_or_none(payload.get("records_digest")),
                sequence=record.sequence,
                through_hash=_string_or_none(payload.get("through_hash")),
                through_sequence=_int_or_none(payload.get("through_sequence")),
            )
        )

    def _apply_live_preflight_result(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.live_preflight_results.append(
            LivePreflightResultSnapshot(
                completed_step_names=_string_tuple(payload.get("completed_step_names")),
                config_fingerprint=_string_or_none(payload.get("config_fingerprint")),
                payload=payload,
                sequence=record.sequence,
                status=_readiness_status_or_none(payload.get("status")),
            )
        )

    def _apply_ledger_health_acknowledgement(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.ledger_health_acknowledgements.append(
            LedgerHealthAcknowledgementSnapshot(
                acknowledged_by=_string_or_none(payload.get("acknowledged_by")),
                acknowledged_health_status=_ledger_health_status_or_none(
                    payload.get("acknowledged_health_status")
                ),
                acknowledged_through_hash=_string_or_none(
                    payload.get("acknowledged_through_hash")
                ),
                acknowledged_through_sequence=_int_or_none(
                    payload.get("acknowledged_through_sequence")
                ),
                attention_check_count=_int_or_none(payload.get("attention_check_count")),
                ledger_health_attention_digest=_string_or_none(
                    payload.get("ledger_health_attention_digest")
                ),
                payload=payload,
                reason=_string_or_none(payload.get("reason")),
                sequence=record.sequence,
            )
        )

    def _apply_strategy_simulation_result(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.strategy_simulation_results.append(
            StrategySimulationResultSnapshot(
                accepted_action_count=_int_or_zero(payload.get("accepted_action_count")),
                config_fingerprint=_string_or_none(payload.get("config_fingerprint")),
                failed_count=_int_or_zero(payload.get("failed_count")),
                payload=payload,
                rejected_action_count=_int_or_zero(payload.get("rejected_action_count")),
                sequence=record.sequence,
                status=_strategy_simulation_status_or_none(payload.get("status")),
                strategy_ids=_string_tuple(payload.get("strategy_ids")),
            )
        )

    def _apply_trigger_fired(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        trigger_id = _string_or_none(payload.get("trigger_id"))
        if trigger_id is None:
            self.error_sequences.append(record.sequence)
            return

        self.trigger_firings.append(
            TriggerSnapshot(
                matched_event_type=_event_type_or_none(payload.get("matched_event_type")),
                matched_sequence=_int_or_none(payload.get("matched_sequence")),
                payload=payload,
                relation=_trigger_relation_or_none(payload.get("relation")),
                sequence=record.sequence,
                target_time=_string_or_none(payload.get("target_time")),
                trigger_id=trigger_id,
            )
        )

    def _apply_action(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        action_id = _action_id(payload)
        if action_id is None:
            self.error_sequences.append(record.sequence)
            return

        current = self.actions.get(action_id)
        if current is None:
            current = ActionSnapshot(action_id=action_id, status=ActionStatus.REQUESTED)
            self.actions[action_id] = current

        if _is_duplicate_action_attempt(record, current, payload):
            return

        current.last_payload = payload
        if record.event_type == EventType.ACTION_REQUESTED:
            current.status = ActionStatus.REQUESTED
            current.requested_sequence = record.sequence
            self._apply_order_requested(record, payload)
        elif record.event_type == EventType.ACTION_ACCEPTED:
            current.status = ActionStatus.ACCEPTED
            current.accepted_sequence = record.sequence
            self._apply_order_accepted(record, payload)
        elif record.event_type == EventType.ACTION_EXECUTION_STARTED:
            current.status = ActionStatus.ACCEPTED
            current.execution_started_sequence = record.sequence
            self._apply_order_execution_started(record, payload)
        elif record.event_type == EventType.ACTION_EXECUTION_FAILED:
            current.status = ActionStatus.FAILED
            current.failed_sequence = record.sequence
            current.failure_reason = _action_failure_reason_or_none(payload.get("failure_reason"))
            self._apply_order_execution_failed(record, payload)
        elif record.event_type == EventType.ACTION_REJECTED:
            current.status = ActionStatus.REJECTED
            current.rejected_sequence = record.sequence
            self._apply_order_rejected(record, payload)
        elif record.event_type == EventType.ACTION_EXECUTED:
            current.executed_sequence = record.sequence
            execution_result = _payload_dict(payload.get("execution_result"))
            execution_status = _execution_status_or_none(execution_result.get("status"))
            failure_reason = _action_failure_reason_for_execution_status(execution_status)
            if failure_reason is not None:
                current.status = ActionStatus.FAILED
                current.failed_sequence = record.sequence
                current.failure_reason = failure_reason
            else:
                current.status = ActionStatus.EXECUTED
            self._apply_order_executed(record, payload)

    def _apply_data(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        message_key = _string_or_none(payload.get("message_key"))
        if message_key is None:
            self.error_sequences.append(record.sequence)
            return

        current = self.data_messages.get(message_key)
        if current is None:
            current = DataMessageSnapshot(message_key=message_key)
            self.data_messages[message_key] = current

        source_id = _string_or_none(payload.get("source_id"))
        if source_id is not None:
            current.source_id = source_id

        message_event_type = _event_type_or_none(payload.get("message_event_type"))
        if message_event_type is not None:
            current.message_event_type = message_event_type

        if record.event_type == EventType.DATA_RECEIVED:
            if _should_record_unique_sequence_anomaly(current, message_event_type):
                self._record_sequence_anomaly(record, message_event_type)
            current.received_sequences.append(record.sequence)
            self._received_data_payloads_by_sequence[record.sequence] = payload
        elif record.event_type == EventType.DATA_ACCEPTED:
            if _should_record_unique_sequence_anomaly(current, message_event_type):
                self._record_sequence_anomaly(record, message_event_type)
            current.accepted_sequence = record.sequence
            self._apply_accepted_feed_payload(record, payload)
        elif record.event_type == EventType.DATA_DUPLICATE:
            current.duplicate_sequences.append(record.sequence)

    def _record_sequence_anomaly(
        self,
        record: AuditRecord,
        message_event_type: EventType | None,
    ) -> None:
        if message_event_type == EventType.DATA_SEQUENCE_GAP:
            self.sequence_gap_sequences.append(record.sequence)
        elif message_event_type == EventType.DATA_OUT_OF_ORDER:
            self.out_of_order_sequences.append(record.sequence)

    def _apply_feed(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        source_id = _string_or_none(payload.get("source_id"))
        if source_id is None:
            return

        current = self.feed_sources.get(source_id)
        if current is None:
            current = FeedSourceSnapshot(source_id=source_id, status=FeedStatus.DISCONNECTED)
            self.feed_sources[source_id] = current

        if record.event_type == EventType.FEED_CONNECTED:
            current.status = FeedStatus.CONNECTED
            current.connected_count += 1
        elif record.event_type == EventType.FEED_DISCONNECTED:
            current.status = FeedStatus.DISCONNECTED
            current.disconnected_count += 1
            current.last_disconnect_reason = _string_or_none(payload.get("reason"))
        elif record.event_type == EventType.FEED_HEARTBEAT:
            current.status = FeedStatus.CONNECTED
            current.last_seen = _string_or_none(payload.get("received_at"))
        elif record.event_type == EventType.FEED_RECONNECT_SCHEDULED:
            current.reconnect_scheduled_count += 1
            current.last_reconnect_attempt = _int_or_none(payload.get("attempt"))
            current.last_reconnect_delay_seconds = _float_or_none(payload.get("delay_seconds"))
            current.last_reconnect_scheduled_sequence = record.sequence

    def _apply_feed_degraded(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        self.feed_degradations.append(
            FeedDegradationSnapshot(
                connected_sources=_string_tuple(payload.get("connected_sources")),
                disconnected_sources=_string_tuple(payload.get("disconnected_sources")),
                live_count=_int_or_none(payload.get("live_count")),
                min_live_sources=_int_or_none(payload.get("min_live_sources")),
                payload=payload,
                sequence=record.sequence,
                stale_sources=_string_tuple(payload.get("stale_sources")),
            )
        )

    def _apply_error(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        error = _payload_dict(payload.get("error"))
        self.errors.append(
            ErrorSnapshot(
                category=_error_category_or_none(payload.get("error_category"))
                or _error_category_or_none(error.get("category")),
                code=_error_code_or_none(payload.get("error_code")) or _error_code_or_none(error.get("code")),
                exception_type=_string_or_none(payload.get("exception_type"))
                or _string_or_none(error.get("exception_type")),
                message=_string_or_none(payload.get("message")) or _string_or_none(error.get("message")),
                payload=payload,
                retryable=_first_bool(payload.get("retryable"), error.get("retryable")),
                sequence=record.sequence,
            )
        )

    def _apply_reconciliation_mismatch(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        action_id = _string_or_none(payload.get("action_id"))
        reason = _reconciliation_issue_or_none(payload.get("reason"))
        if action_id is None or reason is None:
            self.error_sequences.append(record.sequence)
            return

        self.reconciliation_mismatches[(action_id, reason)] = ReconciliationMismatchSnapshot(
            action_id=action_id,
            reason=reason,
            sequence=record.sequence,
            payload=payload,
        )

    def _apply_reconciliation_recovery(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        action_id = _string_or_none(payload.get("action_id"))
        reason = _reconciliation_issue_or_none(payload.get("reason"))
        lookup_status = _exchange_lookup_status_or_none(payload.get("lookup_status"))
        if action_id is None or reason is None or lookup_status is None:
            self.error_sequences.append(record.sequence)
            return

        self.reconciliation_recoveries[(action_id, reason)] = ReconciliationRecoverySnapshot(
            action_id=action_id,
            lookup_status=lookup_status,
            reason=reason,
            sequence=record.sequence,
            payload=payload,
        )

        order_update = _payload_dict(payload.get("order_update"))
        if order_update:
            self._apply_exchange_order_update(record, order_update)

    def _apply_reconciliation_drift(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        drift_key = _string_or_none(payload.get("drift_key"))
        issue = _reconciliation_issue_or_none(payload.get("issue"))
        product_id = _string_or_none(payload.get("product_id"))
        if drift_key is None or issue is None or product_id is None:
            self.error_sequences.append(record.sequence)
            return

        self.reconciliation_drifts[drift_key] = ReconciliationDriftSnapshot(
            drift_key=drift_key,
            issue=issue,
            payload=payload,
            product_id=product_id,
            sequence=record.sequence,
            venue=_product_venue_or_none(payload.get("venue")),
        )

    def _apply_exchange_balance_snapshot(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        account_id = _string_or_none(payload.get("account_id"))
        if account_id is None:
            self.error_sequences.append(record.sequence)
            return

        self.exchange_balances_by_account_id[account_id] = ExchangeBalanceSnapshot(
            account_id=account_id,
            available=_string_or_none(payload.get("available")),
            currency=_string_or_none(payload.get("currency")),
            hold=_string_or_none(payload.get("hold")),
            payload=payload,
            sequence=record.sequence,
            venue=_product_venue_or_none(payload.get("venue")),
        )

    def _apply_exchange_position_snapshot(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        product_id = _string_or_none(payload.get("product_id"))
        venue = _product_venue_or_none(payload.get("venue"))
        if product_id is None or venue is None:
            self.error_sequences.append(record.sequence)
            return

        self.exchange_positions_by_venue_product[(venue, product_id)] = ExchangePositionSnapshot(
            average_entry_price=_string_or_none(payload.get("average_entry_price")),
            current_price=_string_or_none(payload.get("current_price")),
            net_size=_string_or_none(payload.get("net_size")),
            payload=payload,
            product_id=product_id,
            sequence=record.sequence,
            side=_string_or_none(payload.get("side")),
            venue=venue,
        )

    def _apply_exchange_product_snapshot(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        products = _payload_dict_tuple(payload.get("products"))
        if products is None:
            self.error_sequences.append(record.sequence)
            return

        self.exchange_product_snapshot_sequences.append(record.sequence)
        for product in products:
            product_id = _string_or_none(product.get("product_id"))
            product_type = _product_type_or_none(product.get("product_type"))
            product_venue = _product_venue_or_none(product.get("product_venue")) or ProductVenue.UNKNOWN
            if product_id is None or product_type is None:
                self.error_sequences.append(record.sequence)
                continue
            self.exchange_products_by_product_id[product_id] = ExchangeProductSnapshot(
                cancel_only=_bool_or_none(product.get("cancel_only")),
                is_disabled=_bool_or_none(product.get("is_disabled")),
                limit_only=_bool_or_none(product.get("limit_only")),
                payload=product,
                post_only=_bool_or_none(product.get("post_only")),
                product_id=product_id,
                product_type=product_type,
                product_venue=product_venue,
                sequence=record.sequence,
                trading_disabled=_bool_or_none(product.get("trading_disabled")),
                tradable_for_new_orders=_bool_or_none(product.get("tradable_for_new_orders")),
                view_only=_bool_or_none(product.get("view_only")),
            )

    def _apply_exchange_request_retry(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        method = _http_method_or_none(payload.get("method"))
        attempt = _int_or_none(payload.get("attempt"))
        next_attempt = _int_or_none(payload.get("next_attempt"))
        max_attempts = _int_or_none(payload.get("max_attempts"))
        delay_seconds = _float_or_none(payload.get("delay_seconds"))
        if method is None or attempt is None or next_attempt is None or max_attempts is None or delay_seconds is None:
            self.error_sequences.append(record.sequence)
            return

        self.exchange_request_retries.append(
            ExchangeRequestRetrySnapshot(
                attempt=attempt,
                delay_seconds=delay_seconds,
                error_category=_error_category_or_none(payload.get("error_category")),
                error_code=_string_or_none(payload.get("error_code")),
                max_attempts=max_attempts,
                method=method,
                next_attempt=next_attempt,
                payload=payload,
                sequence=record.sequence,
                status_code=_int_or_none(payload.get("status_code")),
                url=_string_or_none(payload.get("url")),
            )
        )

    def _apply_logical_order_created(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        logical_order_id = _string_or_none(payload.get("logical_order_id"))
        root_order_id = _string_or_none(payload.get("root_order_id"))
        lineage_relation = _order_lineage_relation_or_none(payload.get("lineage_relation"))
        product_id = _string_or_none(payload.get("product_id"))
        side = _order_side_or_none(payload.get("side"))
        size = _string_or_none(payload.get("size"))
        if (
            logical_order_id is None
            or root_order_id is None
            or lineage_relation is None
            or product_id is None
            or side is None
            or size is None
        ):
            self.error_sequences.append(record.sequence)
            return

        if logical_order_id in self.logical_orders_by_id:
            return

        logical_order = LogicalOrderSnapshot(
            created_by_action_id=_string_or_none(payload.get("created_by_action_id")),
            lineage_relation=lineage_relation,
            limit_price=_string_or_none(payload.get("limit_price")),
            logical_order_id=logical_order_id,
            parent_order_id=_string_or_none(payload.get("parent_order_id")),
            payload=payload,
            product_id=product_id,
            root_order_id=root_order_id,
            sequence=record.sequence,
            side=side,
            size=size,
            source_order_ids=_string_tuple(payload.get("source_order_ids")),
        )
        self.logical_orders_by_id[logical_order_id] = logical_order
        if logical_order.created_by_action_id is not None:
            self.logical_order_id_by_action_id.setdefault(logical_order.created_by_action_id, logical_order_id)

        if logical_order.parent_order_id is not None:
            parent = self.logical_orders_by_id.get(logical_order.parent_order_id)
            if parent is not None and logical_order_id not in parent.child_order_ids:
                parent.child_order_ids.append(logical_order_id)

    def _apply_order_placement_recorded(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        placement_id = _string_or_none(payload.get("placement_id"))
        logical_order_id = _string_or_none(payload.get("logical_order_id"))
        placement_kind = _order_placement_kind_or_none(payload.get("placement_kind"))
        placement_status = _order_placement_status_or_none(payload.get("placement_status"))
        product_id = _string_or_none(payload.get("product_id"))
        side = _order_side_or_none(payload.get("side"))
        size = _string_or_none(payload.get("size"))
        if (
            placement_id is None
            or logical_order_id is None
            or placement_kind is None
            or placement_status is None
            or product_id is None
            or side is None
            or size is None
        ):
            self.error_sequences.append(record.sequence)
            return

        if placement_id in self.placements_by_id:
            return

        release_of_action_id, release_of_placement_id = _release_relationship_ids(payload)
        placement = OrderPlacementSnapshot(
            action_id=_string_or_none(payload.get("action_id")),
            exchange_order_id=_string_or_none(payload.get("exchange_order_id")),
            limit_price=_string_or_none(payload.get("limit_price")),
            logical_order_id=logical_order_id,
            payload=payload,
            placement_id=placement_id,
            placement_kind=placement_kind,
            placement_status=placement_status,
            product_id=product_id,
            release_of_action_id=release_of_action_id,
            release_of_placement_id=release_of_placement_id,
            sequence=record.sequence,
            side=side,
            size=size,
            venue_client_order_id=_string_or_none(payload.get("venue_client_order_id")),
        )
        self.placements_by_id[placement_id] = placement
        if placement.action_id is not None:
            self.placement_ids_by_action_id.setdefault(placement.action_id, []).append(placement_id)
            self.logical_order_id_by_action_id.setdefault(placement.action_id, logical_order_id)
        self.placements_by_logical_order_id.setdefault(logical_order_id, []).append(placement)
        logical_order = self.logical_orders_by_id.get(logical_order_id)
        if logical_order is not None and placement_id not in logical_order.placement_ids:
            logical_order.placement_ids.append(placement_id)
        if placement_kind == OrderPlacementKind.RELEASE and release_of_placement_id is not None:
            self.release_placement_id_by_staged_placement_id.setdefault(
                release_of_placement_id,
                placement_id,
            )

    def _apply_exchange_fill(self, record: AuditRecord) -> None:
        payload = _payload_dict(record.payload)
        fill_id = _string_or_none(payload.get("fill_id"))
        if fill_id is None:
            self.error_sequences.append(record.sequence)
            return
        if fill_id in self.fills_by_id:
            return

        order_id = _string_or_none(payload.get("order_id"))
        order = self.orders_by_exchange_order_id.get(order_id) if order_id is not None else None
        product_id = _string_or_none(payload.get("product_id")) or (order.product_id if order is not None else None)
        side = _coinbase_order_side_or_none(payload.get("side")) or (order.side if order is not None else None)
        fill = FillSnapshot(
            fill_id=fill_id,
            commission=_string_or_none(payload.get("commission")),
            order_id=order_id,
            payload=payload,
            price=_string_or_none(payload.get("price")),
            product_id=product_id,
            sequence=record.sequence,
            side=side,
            size=_string_or_none(payload.get("size")),
            trade_id=_string_or_none(payload.get("trade_id")),
            trade_time=_string_or_none(payload.get("trade_time")),
        )
        self.fills_by_id[fill_id] = fill
        if order_id is not None:
            self.fills_by_order_id.setdefault(order_id, []).append(fill)
        if order is not None:
            self._apply_fill_to_order(order, fill)
        self._apply_fill_to_position(fill)

    def _apply_order_requested(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        action_type = _action_type_or_none(payload.get("action_type"))
        if action_type != ActionType.PLACE_ORDER:
            return

        action_id = _string_or_none(payload.get("action_id"))
        order_payload = _payload_dict(payload.get("payload"))
        if action_id is None:
            return

        order = self.orders_by_action_id.get(action_id)
        if order is None:
            order = OrderSnapshot(action_id=action_id, lifecycle_status=OrderLifecycleStatus.REQUESTED)
            self.orders_by_action_id[action_id] = order

        order.lifecycle_status = OrderLifecycleStatus.REQUESTED
        order.requested_sequence = record.sequence
        order.product_id = _string_or_none(order_payload.get("product_id"))
        order.side = _order_side_or_none(order_payload.get("side"))
        order.order_type = _order_type_or_none(order_payload.get("order_type"))
        order.size = _string_or_none(order_payload.get("size"))
        order.limit_price = _string_or_none(order_payload.get("limit_price"))
        order.leverage = _string_or_none(order_payload.get("leverage"))
        order.margin_type = _margin_type_or_none(order_payload.get("margin_type"))
        order.post_only = _bool_or_none(order_payload.get("post_only"))
        order.reduce_only = _bool_or_none(order_payload.get("reduce_only"))
        order.time_in_force = _time_in_force_or_none(order_payload.get("time_in_force"))
        client_order_id = _string_or_none(payload.get("idempotency_key")) or action_id
        order.client_order_id = client_order_id

    def _apply_order_accepted(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        if _action_type_or_none(payload.get("action_type")) != ActionType.PLACE_ORDER:
            return
        order = self._order_by_action_payload(payload)
        if order is None:
            return
        order.lifecycle_status = OrderLifecycleStatus.ACCEPTED
        order.accepted_sequence = record.sequence
        self._index_order(order)

    def _apply_order_execution_started(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        if _action_type_or_none(payload.get("action_type")) != ActionType.PLACE_ORDER:
            return
        order = self._order_by_action_payload(payload)
        if order is not None:
            order.execution_started_sequence = record.sequence

    def _apply_order_execution_failed(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        if _action_type_or_none(payload.get("action_type")) != ActionType.PLACE_ORDER:
            return
        order = self._order_by_action_payload(payload)
        if order is None:
            return
        execution_started_sequence = _int_or_none(payload.get("execution_started_sequence"))
        if execution_started_sequence is not None:
            order.execution_started_sequence = execution_started_sequence
        execution_result = _payload_dict(payload.get("execution_result"))
        execution_status = _execution_status_or_none(execution_result.get("status"))
        if execution_status in {ExecutionStatus.FAILED, ExecutionStatus.REJECTED}:
            order.last_execution_result = dict(execution_result)
            order.execution_mode = _execution_mode_or_none(execution_result.get("mode"))
            order.execution_status = execution_status
            order.client_order_id = _string_or_none(execution_result.get("client_order_id")) or order.client_order_id
            order.exchange_order_id = _string_or_none(execution_result.get("exchange_order_id")) or order.exchange_order_id
            self._index_order(order)
            order.lifecycle_status = (
                OrderLifecycleStatus.REJECTED
                if execution_status == ExecutionStatus.REJECTED
                else OrderLifecycleStatus.FAILED
            )
            order.terminal_sequence = record.sequence
            return
        order.lifecycle_status = OrderLifecycleStatus.EXECUTION_UNKNOWN

    def _apply_order_rejected(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        if _action_type_or_none(payload.get("action_type")) != ActionType.PLACE_ORDER:
            return
        order = self._order_by_action_payload(payload)
        if order is None:
            return
        order.lifecycle_status = OrderLifecycleStatus.REJECTED
        order.terminal_sequence = record.sequence

    def _apply_order_executed(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        execution_result = _payload_dict(payload.get("execution_result"))
        action_type = _action_type_or_none(payload.get("action_type")) or _action_type_or_none(
            execution_result.get("action_type")
        )
        if action_type == ActionType.PLACE_ORDER:
            self._apply_place_order_execution(record, payload, execution_result)
        elif action_type == ActionType.CANCEL_ORDER:
            self._apply_cancel_order_execution(record, payload, execution_result)

    def _apply_place_order_execution(
        self,
        record: AuditRecord,
        payload: Mapping[str, JsonValue],
        execution_result: Mapping[str, JsonValue],
    ) -> None:
        order = self._order_by_action_payload(payload)
        if order is None:
            action_id = _string_or_none(payload.get("action_id"))
            if action_id is None:
                return
            order = OrderSnapshot(action_id=action_id, lifecycle_status=OrderLifecycleStatus.ACCEPTED)
            self.orders_by_action_id[action_id] = order

        order.executed_sequence = record.sequence
        execution_started_sequence = _int_or_none(payload.get("execution_started_sequence"))
        if execution_started_sequence is not None:
            order.execution_started_sequence = execution_started_sequence
        order.last_execution_result = dict(execution_result)
        order.execution_mode = _execution_mode_or_none(execution_result.get("mode"))
        order.execution_status = _execution_status_or_none(execution_result.get("status"))
        order.client_order_id = _string_or_none(execution_result.get("client_order_id")) or order.client_order_id
        order.exchange_order_id = _string_or_none(execution_result.get("exchange_order_id")) or order.exchange_order_id
        self._index_order(order)

        if order.execution_status == ExecutionStatus.ACCEPTED:
            order.lifecycle_status = OrderLifecycleStatus.OPEN
        elif order.execution_status == ExecutionStatus.REJECTED:
            order.lifecycle_status = OrderLifecycleStatus.REJECTED
            order.terminal_sequence = record.sequence
        elif order.execution_status == ExecutionStatus.FAILED:
            order.lifecycle_status = OrderLifecycleStatus.FAILED
            order.terminal_sequence = record.sequence

    def _apply_cancel_order_execution(
        self,
        record: AuditRecord,
        payload: Mapping[str, JsonValue],
        execution_result: Mapping[str, JsonValue],
    ) -> None:
        order = self._order_by_execution_identifiers(execution_result)
        cancel_action_id = _string_or_none(payload.get("action_id"))
        if order is None:
            return

        execution_status = _execution_status_or_none(execution_result.get("status"))
        if execution_status == ExecutionStatus.CANCELLED:
            order.lifecycle_status = OrderLifecycleStatus.CANCELLED
            order.terminal_sequence = record.sequence
        elif execution_status == ExecutionStatus.FAILED:
            order.lifecycle_status = OrderLifecycleStatus.FAILED
            order.terminal_sequence = record.sequence
        order.execution_status = execution_status
        order.execution_mode = _execution_mode_or_none(execution_result.get("mode")) or order.execution_mode
        order.last_execution_result = dict(execution_result)
        if cancel_action_id is not None:
            order.cancel_action_ids.append(cancel_action_id)

    def _order_by_action_payload(self, payload: Mapping[str, JsonValue]) -> OrderSnapshot | None:
        action_id = _string_or_none(payload.get("action_id"))
        if action_id is None:
            return None
        return self.orders_by_action_id.get(action_id)

    def _order_by_execution_identifiers(
        self,
        execution_result: Mapping[str, JsonValue],
    ) -> OrderSnapshot | None:
        exchange_order_id = _string_or_none(execution_result.get("exchange_order_id"))
        if exchange_order_id is not None and exchange_order_id in self.orders_by_exchange_order_id:
            return self.orders_by_exchange_order_id[exchange_order_id]

        client_order_id = _string_or_none(execution_result.get("client_order_id"))
        if client_order_id is not None and client_order_id in self.orders_by_client_order_id:
            return self.orders_by_client_order_id[client_order_id]
        return None

    def _index_order(self, order: OrderSnapshot) -> None:
        if order.client_order_id is not None:
            self.orders_by_client_order_id[order.client_order_id] = order
        if order.exchange_order_id is not None:
            self.orders_by_exchange_order_id[order.exchange_order_id] = order

    def _apply_accepted_feed_payload(self, record: AuditRecord, payload: Mapping[str, JsonValue]) -> None:
        received_sequence = _int_or_none(payload.get("received_sequence"))
        if received_sequence is None:
            return
        received_payload = self._received_data_payloads_by_sequence.get(received_sequence)
        if received_payload is None:
            return

        if _event_type_or_none(payload.get("message_event_type")) != EventType.EXCHANGE_ORDER_UPDATE:
            self._apply_accepted_market_data(record, payload, received_payload)
            return

        feed_payload = _payload_dict(received_payload.get("payload"))
        order_update = _payload_dict(feed_payload.get("order"))
        if not order_update:
            return

        self._apply_exchange_order_update(record, order_update)

    def _apply_accepted_market_data(
        self,
        record: AuditRecord,
        accepted_payload: Mapping[str, JsonValue],
        received_payload: Mapping[str, JsonValue],
    ) -> None:
        feed_payload = _payload_dict(received_payload.get("payload"))
        raw_payload = _payload_dict(feed_payload.get("raw")) or feed_payload
        channel = _string_or_none(feed_payload.get("channel")) or _string_or_none(raw_payload.get("channel"))
        if channel is None:
            return

        context = _MarketDataContext(
            exchange_sequence=_first_int(feed_payload, raw_payload, "sequence_num"),
            message_key=_string_or_none(accepted_payload.get("message_key")) or f"accepted-data-{record.sequence}",
            observed_at=record.occurred_at,
            sequence=record.sequence,
            source_id=_string_or_none(accepted_payload.get("source_id")),
            timestamp=_string_or_none(feed_payload.get("timestamp")) or _string_or_none(raw_payload.get("timestamp")),
        )
        if channel in {
            CoinbaseWebSocketChannel.TICKER.value,
            CoinbaseWebSocketChannel.TICKER_BATCH.value,
        }:
            self._apply_ticker_payloads(raw_payload, context)
        elif channel in {
            CoinbaseWebSocketChannel.LEVEL2.value,
            _COINBASE_L2_DATA_CHANNEL,
        }:
            self._apply_order_book_payloads(raw_payload, context)
        elif channel == CoinbaseWebSocketChannel.MARKET_TRADES.value:
            self._apply_market_trade_payloads(raw_payload, context)

    def _apply_ticker_payloads(
        self,
        raw_payload: Mapping[str, JsonValue],
        context: "_MarketDataContext",
    ) -> None:
        for ticker in _ticker_payloads(raw_payload):
            product_id = _string_or_none(ticker.get("product_id"))
            if product_id is None:
                continue
            self.latest_tickers_by_product_id[product_id] = MarketTickerSnapshot(
                ask_price=_first_string(ticker, "best_ask", "best_ask_price", "ask", "ask_price"),
                ask_size=_first_string(
                    ticker,
                    "best_ask_quantity",
                    "best_ask_size",
                    "ask_quantity",
                    "ask_size",
                ),
                bid_price=_first_string(ticker, "best_bid", "best_bid_price", "bid", "bid_price"),
                bid_size=_first_string(
                    ticker,
                    "best_bid_quantity",
                    "best_bid_size",
                    "bid_quantity",
                    "bid_size",
                ),
                exchange_sequence=context.exchange_sequence,
                last_price=_first_string(ticker, "price", "last_price"),
                message_key=context.message_key,
                observed_at=context.observed_at,
                payload=dict(ticker),
                product_id=product_id,
                sequence=context.sequence,
                source_id=context.source_id,
                timestamp=_first_string(ticker, "time", "timestamp") or context.timestamp,
            )

    def _apply_order_book_payloads(
        self,
        raw_payload: Mapping[str, JsonValue],
        context: "_MarketDataContext",
    ) -> None:
        reset_snapshot_products: set[str] = set()
        updated_products: set[str] = set()
        for event in _events(raw_payload.get("events")):
            event_product_id = _string_or_none(event.get("product_id"))
            event_type = _string_or_none(event.get("type"))
            for update in _updates(event.get("updates")):
                product_id = _string_or_none(update.get("product_id")) or event_product_id
                if product_id is None:
                    continue
                side = _order_book_side_or_none(update.get("side"))
                price = _first_string(update, "price_level", "price")
                quantity = _first_string(update, "new_quantity", "quantity", "size")
                if side is None or price is None or quantity is None:
                    continue

                book = self.order_books_by_product_id.get(product_id)
                if book is None:
                    book = MarketOrderBookSnapshot(
                        message_key=context.message_key,
                        observed_at=context.observed_at,
                        product_id=product_id,
                        sequence=context.sequence,
                    )
                    self.order_books_by_product_id[product_id] = book

                if event_type == "snapshot" and product_id not in reset_snapshot_products:
                    _reset_order_book_levels(book)
                    reset_snapshot_products.add(product_id)

                book.exchange_sequence = context.exchange_sequence
                book.message_key = context.message_key
                book.observed_at = context.observed_at
                book.payload = dict(event)
                book.sequence = context.sequence
                book.source_id = context.source_id
                book.timestamp = _first_string(update, "event_time", "time", "timestamp") or context.timestamp
                book.update_count += 1
                _update_order_book_level(book, side=side, price=price, quantity=quantity)
                updated_products.add(product_id)

        for product_id in sorted(updated_products):
            book = self.order_books_by_product_id.get(product_id)
            if book is not None:
                self._append_order_book_sample(product_id, book)

    def _append_order_book_sample(
        self,
        product_id: str,
        book: MarketOrderBookSnapshot,
    ) -> None:
        if (
            self.order_book_sample_product_ids
            and product_id not in self.order_book_sample_product_ids
        ):
            self.order_book_sample_scope_skipped_by_product_id[product_id] = (
                self.order_book_sample_scope_skipped_by_product_id.get(product_id, 0) + 1
            )
            return
        samples = self.order_book_samples_by_product_id.setdefault(product_id, [])
        samples.append(
            _copy_order_book_snapshot(
                book,
                max_depth_per_side=self.max_order_book_sample_depth_per_side,
            )
        )
        dropped_count = 0
        while len(samples) > self.max_order_book_samples_per_product:
            samples.pop(0)
            dropped_count += 1
        if dropped_count:
            self.order_book_sample_retention_dropped_by_product_id[product_id] = (
                self.order_book_sample_retention_dropped_by_product_id.get(product_id, 0)
                + dropped_count
            )

    def _apply_market_trade_payloads(
        self,
        raw_payload: Mapping[str, JsonValue],
        context: "_MarketDataContext",
    ) -> None:
        for trade_index, trade in enumerate(_trade_payloads(raw_payload)):
            product_id = _string_or_none(trade.get("product_id"))
            if product_id is None:
                continue
            trade_id = _string_or_none(trade.get("trade_id")) or f"{product_id}:{context.sequence}:{trade_index}"
            is_new_trade = trade_id not in self.market_trades_by_id
            self.market_trades_by_id[trade_id] = MarketTradeSnapshot(
                exchange_sequence=context.exchange_sequence,
                message_key=context.message_key,
                observed_at=context.observed_at,
                payload=dict(trade),
                price=_first_string(trade, "price"),
                product_id=product_id,
                sequence=context.sequence,
                side=_coinbase_order_side_or_none(trade.get("side")),
                size=_first_string(trade, "size"),
                source_id=context.source_id,
                trade_id=trade_id,
                trade_time=_first_string(trade, "time", "trade_time", "timestamp") or context.timestamp,
            )
            if is_new_trade:
                self.market_trade_ids_by_product_id.setdefault(product_id, []).append(trade_id)
                self._enforce_market_trade_retention(product_id)

    def _enforce_market_trade_retention(self, product_id: str) -> None:
        if self.max_market_trades_per_product is None:
            return
        trade_ids = self.market_trade_ids_by_product_id.get(product_id)
        if trade_ids is None:
            return
        dropped_count = 0
        while len(trade_ids) > self.max_market_trades_per_product:
            dropped_trade_id = trade_ids.pop(0)
            self.market_trades_by_id.pop(dropped_trade_id, None)
            dropped_count += 1
        if dropped_count:
            self.market_trade_retention_dropped_by_product_id[product_id] = (
                self.market_trade_retention_dropped_by_product_id.get(product_id, 0)
                + dropped_count
            )

    def _apply_exchange_order_update(self, record: AuditRecord, order_update: Mapping[str, JsonValue]) -> None:
        order = self._order_by_exchange_update(order_update)
        if order is None:
            action_id = (
                _string_or_none(order_update.get("client_order_id"))
                or _string_or_none(order_update.get("order_id"))
                or f"exchange-update-{record.sequence}"
            )
            order = OrderSnapshot(action_id=action_id, lifecycle_status=OrderLifecycleStatus.PENDING)
            self.orders_by_action_id[action_id] = order

        order.exchange_order_id = _string_or_none(order_update.get("order_id")) or order.exchange_order_id
        order.client_order_id = _string_or_none(order_update.get("client_order_id")) or order.client_order_id
        order.product_id = _string_or_none(order_update.get("product_id")) or order.product_id
        order.side = _coinbase_order_side_or_none(order_update.get("order_side")) or order.side
        order.order_type = _coinbase_order_type_or_none(order_update.get("order_type")) or order.order_type
        order.size = _string_or_none(order_update.get("leaves_quantity")) or order.size
        order.limit_price = _string_or_none(order_update.get("limit_price")) or order.limit_price
        order.exchange_status = _exchange_order_status_or_none(order_update.get("status"))
        order.last_exchange_update = dict(order_update)
        order.lifecycle_status = _lifecycle_from_exchange_status(order.exchange_status) or order.lifecycle_status
        if order.lifecycle_status in {
            OrderLifecycleStatus.CANCELLED,
            OrderLifecycleStatus.EXPIRED,
            OrderLifecycleStatus.FAILED,
            OrderLifecycleStatus.FILLED,
        }:
            order.terminal_sequence = record.sequence
        self._index_order(order)

    def _order_by_exchange_update(self, order_update: Mapping[str, JsonValue]) -> OrderSnapshot | None:
        order_id = _string_or_none(order_update.get("order_id"))
        if order_id is not None and order_id in self.orders_by_exchange_order_id:
            return self.orders_by_exchange_order_id[order_id]

        client_order_id = _string_or_none(order_update.get("client_order_id"))
        if client_order_id is not None and client_order_id in self.orders_by_client_order_id:
            return self.orders_by_client_order_id[client_order_id]
        return None

    def _apply_fill_to_order(self, order: OrderSnapshot, fill: FillSnapshot) -> None:
        if fill.fill_id in order.fill_ids:
            return
        size = _decimal_or_none(fill.size)
        price = _decimal_or_none(fill.price)
        commission = _decimal_or_zero(fill.commission)
        order.fill_ids.append(fill.fill_id)
        order.total_fees = _decimal_string(_decimal_or_zero(order.total_fees) + commission)
        if size is None:
            return

        previous_size = _decimal_or_zero(order.filled_size)
        next_size = previous_size + size
        order.filled_size = _decimal_string(next_size)
        if price is None or next_size == 0:
            return

        previous_average = _decimal_or_zero(order.average_fill_price)
        previous_notional = previous_average * previous_size
        next_average = (previous_notional + (price * size)) / next_size
        order.average_fill_price = _decimal_string(next_average)

    def _apply_fill_to_position(self, fill: FillSnapshot) -> None:
        if fill.product_id is None or fill.side is None:
            return
        size = _decimal_or_none(fill.size)
        price = _decimal_or_none(fill.price)
        if size is None or price is None:
            return

        position = self.positions_by_product_id.get(fill.product_id)
        if position is None:
            position = PositionSnapshot(product_id=fill.product_id)
            self.positions_by_product_id[fill.product_id] = position

        notional = size * price
        commission = _decimal_or_zero(fill.commission)
        if fill.side == OrderSide.BUY:
            position.net_size = _decimal_string(_decimal_or_zero(position.net_size) + size)
            position.gross_buy_size = _decimal_string(_decimal_or_zero(position.gross_buy_size) + size)
            position.gross_buy_notional = _decimal_string(
                _decimal_or_zero(position.gross_buy_notional) + notional
            )
        elif fill.side == OrderSide.SELL:
            position.net_size = _decimal_string(_decimal_or_zero(position.net_size) - size)
            position.gross_sell_size = _decimal_string(_decimal_or_zero(position.gross_sell_size) + size)
            position.gross_sell_notional = _decimal_string(
                _decimal_or_zero(position.gross_sell_notional) + notional
            )
        position.total_fees = _decimal_string(_decimal_or_zero(position.total_fees) + commission)
        position.fill_count += 1


@dataclass(frozen=True)
class _MarketDataContext:
    sequence: int
    message_key: str
    source_id: str | None
    exchange_sequence: int | None
    observed_at: datetime
    timestamp: str | None


_COINBASE_L2_DATA_CHANNEL = "l2_data"

_ACTION_EVENTS = {
    EventType.ACTION_ACCEPTED,
    EventType.ACTION_EXECUTION_FAILED,
    EventType.ACTION_EXECUTION_STARTED,
    EventType.ACTION_EXECUTED,
    EventType.ACTION_REJECTED,
    EventType.ACTION_REQUESTED,
}

_FEED_EVENTS = {
    EventType.FEED_CONNECTED,
    EventType.FEED_DISCONNECTED,
    EventType.FEED_HEARTBEAT,
    EventType.FEED_RECONNECT_SCHEDULED,
}


def _ticker_payloads(raw_payload: Mapping[str, JsonValue]) -> tuple[dict[str, JsonValue], ...]:
    tickers: list[dict[str, JsonValue]] = []
    for event in _events(raw_payload.get("events")):
        event_tickers = _payload_dict_tuple(event.get("tickers"))
        if event_tickers is not None:
            tickers.extend(event_tickers)
        elif _string_or_none(event.get("product_id")) is not None:
            tickers.append(event)
    return tuple(tickers)


def _trade_payloads(raw_payload: Mapping[str, JsonValue]) -> tuple[dict[str, JsonValue], ...]:
    trades: list[dict[str, JsonValue]] = []
    for event in _events(raw_payload.get("events")):
        event_trades = _payload_dict_tuple(event.get("trades"))
        if event_trades is not None:
            trades.extend(event_trades)
    return tuple(trades)


def _events(value: JsonValue) -> tuple[dict[str, JsonValue], ...]:
    values = _payload_dict_tuple(value)
    return values or ()


def _updates(value: JsonValue) -> tuple[dict[str, JsonValue], ...]:
    values = _payload_dict_tuple(value)
    return values or ()


def _first_string(payload: Mapping[str, JsonValue], *keys: str) -> str | None:
    for key in keys:
        value = _string_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _first_int(
    primary_payload: Mapping[str, JsonValue],
    fallback_payload: Mapping[str, JsonValue],
    key: str,
) -> int | None:
    primary_value = _int_or_none(primary_payload.get(key))
    if primary_value is not None:
        return primary_value
    return _int_or_none(fallback_payload.get(key))


def _update_order_book_level(
    book: MarketOrderBookSnapshot,
    *,
    side: OrderBookSide,
    price: str,
    quantity: str,
) -> None:
    levels = book.bid_levels if side == OrderBookSide.BID else book.ask_levels
    if _is_zero_quantity(quantity):
        levels.pop(price, None)
    else:
        levels[price] = quantity
    _sync_order_book_top(book)


def _reset_order_book_levels(book: MarketOrderBookSnapshot) -> None:
    book.ask_levels.clear()
    book.bid_levels.clear()
    _sync_order_book_top(book)


def _sync_order_book_top(book: MarketOrderBookSnapshot) -> None:
    best_bid_price = _best_price(book.bid_levels, side=OrderBookSide.BID)
    best_ask_price = _best_price(book.ask_levels, side=OrderBookSide.ASK)
    book.best_bid_price = best_bid_price
    book.best_bid_size = book.bid_levels.get(best_bid_price) if best_bid_price is not None else None
    book.best_ask_price = best_ask_price
    book.best_ask_size = book.ask_levels.get(best_ask_price) if best_ask_price is not None else None


def _copy_order_book_snapshot(
    book: MarketOrderBookSnapshot,
    *,
    max_depth_per_side: int | None,
) -> MarketOrderBookSnapshot:
    copied = MarketOrderBookSnapshot(
        ask_levels=_retained_order_book_levels(
            book.ask_levels,
            max_depth_per_side=max_depth_per_side,
            side=OrderBookSide.ASK,
        ),
        best_ask_price=book.best_ask_price,
        best_ask_size=book.best_ask_size,
        best_bid_price=book.best_bid_price,
        best_bid_size=book.best_bid_size,
        bid_levels=_retained_order_book_levels(
            book.bid_levels,
            max_depth_per_side=max_depth_per_side,
            side=OrderBookSide.BID,
        ),
        exchange_sequence=book.exchange_sequence,
        message_key=book.message_key,
        observed_at=book.observed_at,
        payload=dict(book.payload),
        product_id=book.product_id,
        sequence=book.sequence,
        source_id=book.source_id,
        timestamp=book.timestamp,
        update_count=book.update_count,
    )
    _sync_order_book_top(copied)
    return copied


def _retained_order_book_levels(
    levels: Mapping[str, str],
    *,
    max_depth_per_side: int | None,
    side: OrderBookSide,
) -> dict[str, str]:
    if max_depth_per_side is None or len(levels) <= max_depth_per_side:
        return dict(levels)
    parsed_levels = tuple(
        (price, quantity, parsed_price)
        for price, quantity in levels.items()
        if (parsed_price := _decimal_or_none(price)) is not None
    )
    sorted_levels = sorted(
        parsed_levels,
        key=lambda item: item[2],
        reverse=(side == OrderBookSide.BID),
    )
    retained_prices = {price for price, _quantity, _parsed_price in sorted_levels[:max_depth_per_side]}
    return {
        price: quantity
        for price, quantity in levels.items()
        if price in retained_prices
    }


def _best_price(levels: Mapping[str, str], *, side: OrderBookSide) -> str | None:
    parsed_levels = tuple(
        (price, parsed_price)
        for price in levels
        if (parsed_price := _decimal_or_none(price)) is not None
    )
    if not parsed_levels:
        return None
    if side == OrderBookSide.BID:
        return max(parsed_levels, key=lambda item: item[1])[0]
    return min(parsed_levels, key=lambda item: item[1])[0]


def _order_book_side_or_none(value: Any) -> OrderBookSide | None:
    if not isinstance(value, str):
        return None
    normalized = value.lower()
    if normalized in {"bid", "buy"}:
        return OrderBookSide.BID
    if normalized in {"ask", "offer", "sell"}:
        return OrderBookSide.ASK
    return None


def _is_zero_quantity(value: str) -> bool:
    quantity = _decimal_or_none(value)
    return quantity is not None and quantity == 0


def _payload_dict(payload: JsonValue) -> dict[str, JsonValue]:
    normalized = normalize_json(payload)
    if isinstance(normalized, dict):
        return normalized
    return {}


def _payload_dict_tuple(payload: JsonValue) -> tuple[dict[str, JsonValue], ...] | None:
    normalized = normalize_json(payload)
    if not isinstance(normalized, list):
        return None
    products: list[dict[str, JsonValue]] = []
    for item in normalized:
        if not isinstance(item, dict):
            return None
        products.append(item)
    return tuple(products)


def _release_relationship_ids(payload: dict[str, JsonValue]) -> tuple[str | None, str | None]:
    metadata = _payload_dict(payload.get("metadata"))
    staged_release = _payload_dict(metadata.get("staged_release"))
    return (
        _string_or_none(staged_release.get("release_of_action_id")),
        _string_or_none(staged_release.get("release_of_placement_id")),
    )


def _passive_market_making_metadata(payload: dict[str, JsonValue]) -> dict[str, JsonValue] | None:
    metadata = _payload_dict(payload.get("metadata"))
    passive_market_making = _payload_dict(metadata.get("passive_market_making"))
    return passive_market_making or None


def _strategy_action_ids(payload: JsonValue) -> list[str]:
    action_receipts = _payload_dict_tuple(payload)
    if action_receipts is None:
        return []
    return [
        action_id
        for receipt in action_receipts
        if (action_id := _string_or_none(receipt.get("action_id"))) is not None
    ]


def _action_id(payload: Mapping[str, Any]) -> str | None:
    for key in ("action_id", "client_order_id", "order_id"):
        value = _string_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _is_duplicate_action_attempt(
    record: AuditRecord,
    current: ActionSnapshot,
    payload: Mapping[str, Any],
) -> bool:
    if record.event_type == EventType.ACTION_REQUESTED and current.requested_sequence is not None:
        return True
    return (
        record.event_type == EventType.ACTION_REJECTED
        and payload.get("rejection_reason") == ActionRejectionReason.DUPLICATE_ACTION_ID.value
        and current.status != ActionStatus.REQUESTED
    )


def _event_type_or_none(value: Any) -> EventType | None:
    try:
        return EventType(value)
    except (TypeError, ValueError):
        return None


def _should_record_unique_sequence_anomaly(
    current: DataMessageSnapshot,
    message_event_type: EventType | None,
) -> bool:
    return (
        message_event_type in {EventType.DATA_SEQUENCE_GAP, EventType.DATA_OUT_OF_ORDER}
        and not current.received_sequences
        and current.accepted_sequence is None
    )


def _action_type_or_none(value: Any) -> ActionType | None:
    try:
        return ActionType(value)
    except (TypeError, ValueError):
        return None


def _action_failure_reason_or_none(value: Any) -> ActionFailureReason | None:
    try:
        return ActionFailureReason(value)
    except (TypeError, ValueError):
        return None


def _action_failure_reason_for_execution_status(
    value: ExecutionStatus | None,
) -> ActionFailureReason | None:
    if value == ExecutionStatus.REJECTED:
        return ActionFailureReason.EXECUTION_REJECTED
    if value == ExecutionStatus.FAILED:
        return ActionFailureReason.EXECUTION_FAILED
    return None


def _digest_algorithm_or_none(value: Any) -> DigestAlgorithm | None:
    try:
        return DigestAlgorithm(value)
    except (TypeError, ValueError):
        return None


def _execution_mode_or_none(value: Any) -> ExecutionMode | None:
    try:
        return ExecutionMode(value)
    except (TypeError, ValueError):
        return None


def _execution_status_or_none(value: Any) -> ExecutionStatus | None:
    try:
        return ExecutionStatus(value)
    except (TypeError, ValueError):
        return None


def _exchange_order_status_or_none(value: Any) -> ExchangeOrderStatus | None:
    try:
        return ExchangeOrderStatus(value)
    except (TypeError, ValueError):
        return None


def _exchange_lookup_status_or_none(value: Any) -> ExchangeLookupStatus | None:
    try:
        return ExchangeLookupStatus(value)
    except (TypeError, ValueError):
        return None


def _reconciliation_issue_or_none(value: Any) -> ReconciliationIssue | None:
    try:
        return ReconciliationIssue(value)
    except (TypeError, ValueError):
        return None


def _product_venue_or_none(value: Any) -> ProductVenue | None:
    try:
        return ProductVenue(value)
    except (TypeError, ValueError):
        return None


def _product_type_or_none(value: Any) -> ProductType | None:
    try:
        return ProductType(value)
    except (TypeError, ValueError):
        return None


def _runtime_task_or_none(value: Any) -> RuntimeTask | None:
    try:
        return RuntimeTask(value)
    except (TypeError, ValueError):
        return None


def _strategy_evaluation_status_or_none(value: Any) -> StrategyEvaluationStatus | None:
    try:
        return StrategyEvaluationStatus(value)
    except (TypeError, ValueError):
        return None


def _strategy_simulation_status_or_none(value: Any) -> StrategySimulationStatus | None:
    try:
        return StrategySimulationStatus(value)
    except (TypeError, ValueError):
        return None


def _runtime_component_or_none(value: Any) -> RuntimeComponent | None:
    try:
        return RuntimeComponent(value)
    except (TypeError, ValueError):
        return None


def _runtime_stop_reason_or_none(value: Any) -> RuntimeStopReason | None:
    try:
        return RuntimeStopReason(value)
    except (TypeError, ValueError):
        return None


def _readiness_status_or_none(value: Any) -> ReadinessStatus | None:
    try:
        return ReadinessStatus(value)
    except (TypeError, ValueError):
        return None


def _ledger_health_status_or_none(value: Any) -> LedgerHealthStatus | None:
    try:
        return LedgerHealthStatus(value)
    except (TypeError, ValueError):
        return None


def _trigger_relation_or_none(value: Any) -> TriggerRelation | None:
    try:
        return TriggerRelation(value)
    except (TypeError, ValueError):
        return None


def _error_category_or_none(value: Any) -> ErrorCategory | None:
    try:
        return ErrorCategory(value)
    except (TypeError, ValueError):
        return None


def _error_code_or_none(value: Any) -> ErrorCode | None:
    try:
        return ErrorCode(value)
    except (TypeError, ValueError):
        return None


def _http_method_or_none(value: Any) -> HttpMethod | None:
    try:
        return HttpMethod(value)
    except (TypeError, ValueError):
        return None


def _anchor_store_type_or_none(value: Any) -> AnchorStoreType | None:
    try:
        return AnchorStoreType(value)
    except (TypeError, ValueError):
        return None


def _anchor_immutability_mode_or_none(value: Any) -> AnchorImmutabilityMode | None:
    try:
        return AnchorImmutabilityMode(value)
    except (TypeError, ValueError):
        return None


def _lifecycle_from_exchange_status(status: ExchangeOrderStatus | None) -> OrderLifecycleStatus | None:
    if status == ExchangeOrderStatus.PENDING:
        return OrderLifecycleStatus.PENDING
    if status == ExchangeOrderStatus.OPEN:
        return OrderLifecycleStatus.OPEN
    if status == ExchangeOrderStatus.FILLED:
        return OrderLifecycleStatus.FILLED
    if status == ExchangeOrderStatus.CANCEL_QUEUED:
        return OrderLifecycleStatus.CANCEL_QUEUED
    if status == ExchangeOrderStatus.CANCELLED:
        return OrderLifecycleStatus.CANCELLED
    if status == ExchangeOrderStatus.EXPIRED:
        return OrderLifecycleStatus.EXPIRED
    if status == ExchangeOrderStatus.FAILED:
        return OrderLifecycleStatus.FAILED
    return None


def _order_lineage_relation_or_none(value: Any) -> OrderLineageRelation | None:
    try:
        return OrderLineageRelation(value)
    except (TypeError, ValueError):
        return None


def _order_placement_kind_or_none(value: Any) -> OrderPlacementKind | None:
    try:
        return OrderPlacementKind(value)
    except (TypeError, ValueError):
        return None


def _order_placement_status_or_none(value: Any) -> OrderPlacementStatus | None:
    try:
        return OrderPlacementStatus(value)
    except (TypeError, ValueError):
        return None


def _order_side_or_none(value: Any) -> OrderSide | None:
    try:
        return OrderSide(value)
    except (TypeError, ValueError):
        return None


def _order_type_or_none(value: Any) -> OrderType | None:
    try:
        return OrderType(value)
    except (TypeError, ValueError):
        return None


def _margin_type_or_none(value: Any) -> MarginType | None:
    try:
        return MarginType(value)
    except (TypeError, ValueError):
        return None


def _time_in_force_or_none(value: Any) -> TimeInForce | None:
    try:
        return TimeInForce(value)
    except (TypeError, ValueError):
        return None


def _coinbase_order_side_or_none(value: Any) -> OrderSide | None:
    if isinstance(value, str):
        return _order_side_or_none(value.lower())
    return None


def _coinbase_order_type_or_none(value: Any) -> OrderType | None:
    if not isinstance(value, str):
        return None
    if value == "LIMIT":
        return OrderType.LIMIT
    if value == "MARKET":
        return OrderType.MARKET
    return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        parsed = _bool_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None and parsed >= 0 else 0


def _validate_optional_positive_int(value: int | None, field_name: str) -> None:
    if value is None:
        return
    _validate_positive_int(value, field_name)


def _validate_positive_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer when provided")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _validate_string_scope(value: tuple[str, ...], field_name: str) -> None:
    if not isinstance(value, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    for item in value:
        if not isinstance(item, str) or not item:
            raise TypeError(f"{field_name} must contain non-empty strings")
    if len(value) != len(set(value)):
        raise ValueError(f"{field_name} must be unique")


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_or_zero(value: Any) -> Decimal:
    return _decimal_or_none(value) or Decimal("0")


def _decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value:
        return value
    return None


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Iterable) or isinstance(value, str | bytes | bytearray):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)
