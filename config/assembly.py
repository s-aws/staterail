from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from actions.gateway import ActionExecutor, ActionGateway
from actions.dry_run import DryRunExecutor
from actions.venue_guard import ProductVenueRestrictedExecutor
from audit.anchors import LedgerAnchorStore
from audit.archives import LedgerArchiveStore
from audit.tasks import AuditAnchorTask, AuditArchiveTask
from core.clock import Clock
from core.engine import AuditCore
from core.enums import (
    AnchorImmutabilityMode,
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    EventType,
    ExecutionMode,
    LedgerAnchorStoreProvider,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    RuntimeTask,
    TimeInForce,
    TriggerRelation,
    TriggerRuleType,
)
from exchanges.coinbase.advanced_trade_rest import (
    CoinbaseAdvancedTradeAccountLookupClient,
    CoinbaseAdvancedTradeFillLookupClient,
    CoinbaseAdvancedTradeOrderLookupClient,
    CoinbaseAdvancedTradePositionLookupClient,
    CoinbaseAdvancedTradeRestExecutor,
    CoinbaseRestConfig,
    CoinbaseRestRetryPolicy,
    CoinbaseRetryingHttpTransport,
    HttpTransport,
    RestRetrySleep,
    UrlLibHttpTransport,
)
from exchanges.coinbase.auth import TokenProvider
from exchanges.coinbase.advanced_trade_ws import CoinbaseAdvancedTradeFeedSource, CoinbaseWebSocketConfig, JwtFactory
from exchanges.coinbase.products import CoinbaseProductCatalogClient
from exchanges.coinbase.venues import COINBASE_LIVE_EXECUTION_PRODUCT_VENUES
from feeds import AsyncFeedSource, FeedSupervisor, ReconnectPolicy, RedundantFeedRouter
from products.catalog import ProductCatalog
from products.replay import product_catalog_from_projection
from products.tasks import ProductCatalogLookup, ProductCatalogRefreshTask
from projections.state import SourceOfTruthProjection
from reconciliation.fills import FillReconciliation, FillReconciliationPolicy
from reconciliation.positions import ExchangeStateReconciliation, ExchangeStateReconciliationPolicy
from reconciliation.recovery import ReconciliationRecovery
from reconciliation.watchdog import ReconciliationPolicy, ReconciliationWatchdog
from risk.gate import RiskGate, RiskPolicy
from runtime.orchestrator import RuntimeOrchestrator, ScheduledRuntimeTask, Sleep
from strategies import (
    OperatorPolicy,
    Strategy,
    StrategyEvaluationTask,
    StrategyInputRequirement,
    configured_strategies,
)
from triggers.rules import MessageTrigger, TimeTrigger, TriggerEngine


DEFAULT_COINBASE_REST_BASE_URL = "https://api.coinbase.com/api/v3/brokerage"
WebSocketSourceFactory = Callable[[CoinbaseWebSocketConfig], AsyncFeedSource]


@dataclass(frozen=True)
class TaskScheduleConfig:
    task_id: RuntimeTask
    interval: timedelta
    enabled: bool = True
    run_on_start: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, RuntimeTask):
            raise TypeError("task_id must be a RuntimeTask")
        if self.interval <= timedelta(0):
            raise ValueError("interval must be positive")
        if not isinstance(self.enabled, bool):
            raise TypeError("enabled must be a bool")
        if not isinstance(self.run_on_start, bool):
            raise TypeError("run_on_start must be a bool")


@dataclass(frozen=True)
class TimeTriggerConfig:
    trigger_id: str
    relation: TriggerRelation
    target_time: datetime
    tolerance: timedelta = timedelta(seconds=1)
    repeatable: bool = False
    rule_type: TriggerRuleType = TriggerRuleType.TIME

    def __post_init__(self) -> None:
        if not isinstance(self.rule_type, TriggerRuleType):
            raise TypeError("rule_type must be a TriggerRuleType")
        if self.rule_type != TriggerRuleType.TIME:
            raise ValueError("time trigger config rule_type must be time")
        if not self.trigger_id:
            raise ValueError("trigger_id is required")
        if not isinstance(self.relation, TriggerRelation):
            raise TypeError("relation must be a TriggerRelation")
        if not isinstance(self.target_time, datetime):
            raise TypeError("target_time must be a datetime")
        if not isinstance(self.tolerance, timedelta):
            raise TypeError("tolerance must be a datetime.timedelta")
        if self.tolerance <= timedelta(0):
            raise ValueError("tolerance must be positive")
        if not isinstance(self.repeatable, bool):
            raise TypeError("repeatable must be a bool")

    def to_rule(self) -> TimeTrigger:
        return TimeTrigger(
            trigger_id=self.trigger_id,
            relation=self.relation,
            target_time=self.target_time,
            tolerance=self.tolerance,
            repeatable=self.repeatable,
        )


@dataclass(frozen=True)
class MessageTriggerConfig:
    trigger_id: str
    relation: TriggerRelation
    event_type: EventType | None = None
    repeatable: bool = True
    rule_type: TriggerRuleType = TriggerRuleType.MESSAGE

    def __post_init__(self) -> None:
        if not isinstance(self.rule_type, TriggerRuleType):
            raise TypeError("rule_type must be a TriggerRuleType")
        if self.rule_type != TriggerRuleType.MESSAGE:
            raise ValueError("message trigger config rule_type must be message")
        if not self.trigger_id:
            raise ValueError("trigger_id is required")
        if not isinstance(self.relation, TriggerRelation):
            raise TypeError("relation must be a TriggerRelation")
        if self.event_type is not None and not isinstance(self.event_type, EventType):
            raise TypeError("event_type must be an EventType")
        if not isinstance(self.repeatable, bool):
            raise TypeError("repeatable must be a bool")

    def to_rule(self) -> MessageTrigger:
        return MessageTrigger(
            trigger_id=self.trigger_id,
            relation=self.relation,
            event_type=self.event_type,
            repeatable=self.repeatable,
        )


TriggerRuleConfig = TimeTriggerConfig | MessageTriggerConfig


@dataclass(frozen=True)
class CoinbaseRestApiConfig:
    base_url: str = DEFAULT_COINBASE_REST_BASE_URL
    execution_mode: ExecutionMode = ExecutionMode.DRY_RUN
    retry_policy: CoinbaseRestRetryPolicy = field(default_factory=CoinbaseRestRetryPolicy)
    retail_portfolio_id: str | None = None
    perpetual_portfolio_uuid: str | None = None

    def __post_init__(self) -> None:
        if not self.base_url:
            raise ValueError("base_url is required")
        if not isinstance(self.execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")
        if not isinstance(self.retry_policy, CoinbaseRestRetryPolicy):
            raise TypeError("retry_policy must be a CoinbaseRestRetryPolicy")

    def to_rest_config(self) -> CoinbaseRestConfig:
        return CoinbaseRestConfig(
            base_url=self.base_url,
            execution_mode=self.execution_mode,
            portfolio_id=self.retail_portfolio_id,
        )


@dataclass(frozen=True)
class CoinbaseWebSocketSourceConfig:
    source_id: str
    channels: tuple[CoinbaseWebSocketChannel, ...]
    endpoint: CoinbaseWebSocketEndpoint
    product_ids: tuple[str, ...] = ()
    include_heartbeats: bool = True

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id is required")
        if not self.channels:
            raise ValueError("channels must not be empty")
        for channel in self.channels:
            if not isinstance(channel, CoinbaseWebSocketChannel):
                raise TypeError("channels must contain CoinbaseWebSocketChannel values")
        if len(self.channels) != len(set(self.channels)):
            raise ValueError("channels must be unique")
        if not isinstance(self.endpoint, CoinbaseWebSocketEndpoint):
            raise TypeError("endpoint must be a CoinbaseWebSocketEndpoint")
        if not isinstance(self.include_heartbeats, bool):
            raise TypeError("include_heartbeats must be a bool")
        for product_id in self.product_ids:
            if not isinstance(product_id, str) or not product_id:
                raise TypeError("product_ids must contain non-empty strings")
        if len(self.product_ids) != len(set(self.product_ids)):
            raise ValueError("product_ids must be unique")
        if _channels_require_products(self.channels) and not self.product_ids:
            raise ValueError("product_ids are required for product-scoped websocket channels")
        if self.is_user_source():
            if len(self.channels) > 1:
                raise ValueError("user websocket channel must be configured separately")
            if self.endpoint != CoinbaseWebSocketEndpoint.USER_ORDER_DATA:
                raise ValueError("user websocket channel must use USER_ORDER_DATA endpoint")
        elif self.endpoint == CoinbaseWebSocketEndpoint.USER_ORDER_DATA:
            raise ValueError("USER_ORDER_DATA endpoint requires the user websocket channel")

    def to_websocket_config(self, *, jwt_factory: JwtFactory | None = None) -> CoinbaseWebSocketConfig:
        if self.requires_jwt_factory() and jwt_factory is None:
            raise ValueError("jwt_factory is required for Coinbase user websocket sources")
        return CoinbaseWebSocketConfig(
            source_id=self.source_id,
            product_ids=self.product_ids,
            channels=self.channels,
            endpoint=self.endpoint,
            include_heartbeats=self.include_heartbeats,
            jwt_factory=jwt_factory,
        )

    def is_user_source(self) -> bool:
        return CoinbaseWebSocketChannel.USER in self.channels

    def requires_jwt_factory(self) -> bool:
        return self.is_user_source()


@dataclass(frozen=True)
class ReconciliationRuntimeConfig:
    watchdog_policy: ReconciliationPolicy = field(default_factory=ReconciliationPolicy)
    fill_policy: FillReconciliationPolicy = field(default_factory=FillReconciliationPolicy)
    exchange_state_policy: ExchangeStateReconciliationPolicy = field(default_factory=ExchangeStateReconciliationPolicy)
    watchdog_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.WATCHDOG,
            interval=timedelta(seconds=5),
            enabled=True,
        )
    )
    order_recovery_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.ORDER_RECOVERY,
            interval=timedelta(seconds=30),
            enabled=False,
        )
    )
    fill_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.FILL_RECONCILIATION,
            interval=timedelta(seconds=30),
            enabled=False,
        )
    )
    exchange_state_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.EXCHANGE_STATE_RECONCILIATION,
            interval=timedelta(seconds=60),
            enabled=False,
        )
    )

    def __post_init__(self) -> None:
        _require_schedule(self.watchdog_schedule, RuntimeTask.WATCHDOG)
        _require_schedule(self.order_recovery_schedule, RuntimeTask.ORDER_RECOVERY)
        _require_schedule(self.fill_schedule, RuntimeTask.FILL_RECONCILIATION)
        _require_schedule(self.exchange_state_schedule, RuntimeTask.EXCHANGE_STATE_RECONCILIATION)

    def enabled_schedules(self) -> tuple[TaskScheduleConfig, ...]:
        return tuple(
            schedule
            for schedule in (
                self.watchdog_schedule,
                self.order_recovery_schedule,
                self.fill_schedule,
                self.exchange_state_schedule,
            )
            if schedule.enabled
        )

    def rest_backed_schedules(self) -> tuple[TaskScheduleConfig, ...]:
        return tuple(
            schedule
            for schedule in (
                self.order_recovery_schedule,
                self.fill_schedule,
                self.exchange_state_schedule,
            )
            if schedule.enabled
        )


@dataclass(frozen=True)
class FeedRuntimeConfig:
    min_live_sources: int = 1
    reconnect_policy: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    stale_after: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if not isinstance(self.min_live_sources, int) or isinstance(self.min_live_sources, bool):
            raise TypeError("min_live_sources must be an integer")
        if self.min_live_sources <= 0:
            raise ValueError("min_live_sources must be positive")
        if not isinstance(self.reconnect_policy, ReconnectPolicy):
            raise TypeError("reconnect_policy must be a ReconnectPolicy")
        if not isinstance(self.stale_after, timedelta):
            raise TypeError("stale_after must be a datetime.timedelta")
        if self.stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")


@dataclass(frozen=True)
class RiskPolicyConfig:
    allowed_products: tuple[str, ...] = ()
    allowed_order_types: tuple[OrderType, ...] = ()
    allowed_sides: tuple[OrderSide, ...] = ()
    allowed_time_in_force: tuple[TimeInForce, ...] = ()
    allowed_lineage_relations: tuple[OrderLineageRelation, ...] = ()
    allowed_placement_kinds: tuple[OrderPlacementKind, ...] = ()
    max_order_size: Decimal | None = None
    max_order_notional: Decimal | None = None
    max_daily_notional: Decimal | None = None
    max_open_orders: int | None = None
    max_leverage: Decimal | None = None
    max_visible_notional: Decimal | None = None
    max_order_replacements: int | None = None
    require_reduce_only: bool = False
    require_post_only: bool = False
    require_staged_release_above_visible_limit: bool = False
    kill_switch_enabled: bool = False

    def __post_init__(self) -> None:
        for product_id in self.allowed_products:
            if not isinstance(product_id, str) or not product_id:
                raise TypeError("allowed_products must contain non-empty strings")
        if len(self.allowed_products) != len(set(self.allowed_products)):
            raise ValueError("allowed_products must be unique")
        for order_type in self.allowed_order_types:
            if not isinstance(order_type, OrderType):
                raise TypeError("allowed_order_types must contain OrderType values")
        if len(self.allowed_order_types) != len(set(self.allowed_order_types)):
            raise ValueError("allowed_order_types must be unique")
        _require_unique_enum_values(self.allowed_sides, OrderSide, "allowed_sides")
        _require_unique_enum_values(
            self.allowed_time_in_force,
            TimeInForce,
            "allowed_time_in_force",
        )
        _require_unique_enum_values(
            self.allowed_lineage_relations,
            OrderLineageRelation,
            "allowed_lineage_relations",
        )
        _require_unique_enum_values(
            self.allowed_placement_kinds,
            OrderPlacementKind,
            "allowed_placement_kinds",
        )
        _require_positive_optional_decimal(self.max_order_size, "max_order_size")
        _require_positive_optional_decimal(self.max_order_notional, "max_order_notional")
        _require_positive_optional_decimal(self.max_daily_notional, "max_daily_notional")
        _require_positive_optional_int(self.max_open_orders, "max_open_orders")
        _require_positive_optional_decimal(self.max_leverage, "max_leverage")
        _require_positive_optional_decimal(self.max_visible_notional, "max_visible_notional")
        _require_positive_optional_int(self.max_order_replacements, "max_order_replacements")
        if not isinstance(self.require_reduce_only, bool):
            raise TypeError("require_reduce_only must be a bool")
        if not isinstance(self.require_post_only, bool):
            raise TypeError("require_post_only must be a bool")
        if not isinstance(self.require_staged_release_above_visible_limit, bool):
            raise TypeError("require_staged_release_above_visible_limit must be a bool")
        if not isinstance(self.kill_switch_enabled, bool):
            raise TypeError("kill_switch_enabled must be a bool")

    def to_policy(self, *, product_catalog: ProductCatalog | None = None) -> RiskPolicy:
        return RiskPolicy.from_values(
            allowed_lineage_relations=self.allowed_lineage_relations or None,
            allowed_order_types=self.allowed_order_types or None,
            allowed_placement_kinds=self.allowed_placement_kinds or None,
            allowed_products=self.allowed_products or None,
            allowed_sides=self.allowed_sides or None,
            allowed_time_in_force=self.allowed_time_in_force or None,
            kill_switch_enabled=self.kill_switch_enabled,
            max_daily_notional=self.max_daily_notional,
            max_leverage=self.max_leverage,
            max_open_orders=self.max_open_orders,
            max_order_notional=self.max_order_notional,
            max_order_replacements=self.max_order_replacements,
            max_order_size=self.max_order_size,
            max_visible_notional=self.max_visible_notional,
            product_catalog=product_catalog,
            require_post_only=self.require_post_only,
            require_reduce_only=self.require_reduce_only,
            require_staged_release_above_visible_limit=(
                self.require_staged_release_above_visible_limit
            ),
        )


@dataclass(frozen=True)
class ProductCatalogRuntimeConfig:
    schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
            interval=timedelta(hours=1),
            enabled=False,
        )
    )
    product_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_schedule(self.schedule, RuntimeTask.PRODUCT_CATALOG_REFRESH)
        for product_id in self.product_ids:
            if not isinstance(product_id, str) or not product_id:
                raise TypeError("product_ids must contain non-empty strings")
        if len(self.product_ids) != len(set(self.product_ids)):
            raise ValueError("product_ids must be unique")


@dataclass(frozen=True)
class StrategyRuntimeConfig:
    schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.STRATEGY_EVALUATION,
            interval=timedelta(seconds=1),
            enabled=False,
            run_on_start=False,
        )
    )
    strategy_ids: tuple[str, ...] = ()
    strategy_parameters: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    market_data_requirements: tuple[StrategyInputRequirement, ...] = ()
    max_market_trades_per_product: int | None = None
    max_order_book_sample_depth_per_side: int | None = None
    max_order_book_samples_per_product: int = 1
    allow_live_execution: bool = False
    operator_policy: OperatorPolicy | None = None

    def __post_init__(self) -> None:
        _require_schedule(self.schedule, RuntimeTask.STRATEGY_EVALUATION)
        for strategy_id in self.strategy_ids:
            if not isinstance(strategy_id, str) or not strategy_id:
                raise TypeError("strategy_ids must contain non-empty strings")
        if len(self.strategy_ids) != len(set(self.strategy_ids)):
            raise ValueError("strategy_ids must be unique")
        if not isinstance(self.strategy_parameters, Mapping):
            raise TypeError("strategy_parameters must be a mapping")
        for strategy_id, parameters in self.strategy_parameters.items():
            if not isinstance(strategy_id, str) or not strategy_id:
                raise TypeError("strategy_parameters keys must be non-empty strings")
            if strategy_id not in self.strategy_ids:
                raise ValueError(
                    f"strategy_parameters configured for unselected strategy_id: {strategy_id}"
                )
            if not isinstance(parameters, Mapping):
                raise TypeError("strategy_parameters values must be mappings")
        if not isinstance(self.market_data_requirements, tuple):
            raise TypeError("market_data_requirements must be a tuple")
        for requirement in self.market_data_requirements:
            if not isinstance(requirement, StrategyInputRequirement):
                raise TypeError(
                    "market_data_requirements must contain StrategyInputRequirement values"
                )
        if self.max_market_trades_per_product is not None:
            if (
                isinstance(self.max_market_trades_per_product, bool)
                or not isinstance(self.max_market_trades_per_product, int)
            ):
                raise TypeError("max_market_trades_per_product must be an integer when provided")
            if self.max_market_trades_per_product <= 0:
                raise ValueError("max_market_trades_per_product must be positive")
        if (
            self.max_order_book_sample_depth_per_side is not None
            and (
                isinstance(self.max_order_book_sample_depth_per_side, bool)
                or not isinstance(self.max_order_book_sample_depth_per_side, int)
            )
        ):
            raise TypeError(
                "max_order_book_sample_depth_per_side must be an integer when provided"
            )
        if (
            self.max_order_book_sample_depth_per_side is not None
            and self.max_order_book_sample_depth_per_side <= 0
        ):
            raise ValueError("max_order_book_sample_depth_per_side must be positive")
        if (
            isinstance(self.max_order_book_samples_per_product, bool)
            or not isinstance(self.max_order_book_samples_per_product, int)
        ):
            raise TypeError("max_order_book_samples_per_product must be an integer")
        if self.max_order_book_samples_per_product <= 0:
            raise ValueError("max_order_book_samples_per_product must be positive")
        if not isinstance(self.allow_live_execution, bool):
            raise TypeError("allow_live_execution must be a bool")
        if self.operator_policy is not None and not isinstance(self.operator_policy, OperatorPolicy):
            raise TypeError("operator_policy must be an OperatorPolicy")


@dataclass(frozen=True)
class AuditAnchorStoreConfig:
    provider: LedgerAnchorStoreProvider
    local_anchor_dir: Path | None = None
    s3_bucket: str | None = None
    s3_expected_bucket_owner: str | None = None
    s3_immutability_mode: AnchorImmutabilityMode | None = None
    s3_key_prefix: str = "audit-anchors"
    s3_retention_period: timedelta | None = None
    s3_verify_bucket_configuration: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.provider, LedgerAnchorStoreProvider):
            raise TypeError("provider must be a LedgerAnchorStoreProvider")
        if not isinstance(self.s3_key_prefix, str):
            raise TypeError("s3_key_prefix must be a string")
        if not isinstance(self.s3_verify_bucket_configuration, bool):
            raise TypeError("s3_verify_bucket_configuration must be a bool")

        if self.provider == LedgerAnchorStoreProvider.LOCAL_FILE:
            if not isinstance(self.local_anchor_dir, Path):
                raise TypeError("local_anchor_dir must be a pathlib.Path for local_file anchor stores")
            if any(
                value is not None
                for value in (
                    self.s3_bucket,
                    self.s3_expected_bucket_owner,
                    self.s3_immutability_mode,
                    self.s3_retention_period,
                )
            ):
                raise ValueError("S3 anchor fields cannot be set for local_file anchor stores")
            return

        if self.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
            if self.local_anchor_dir is not None:
                raise ValueError("local_anchor_dir cannot be set for aws_s3_object_lock anchor stores")
            if not isinstance(self.s3_bucket, str) or not self.s3_bucket.strip():
                raise ValueError("s3_bucket must be a non-empty string")
            if self.s3_expected_bucket_owner is not None and not self.s3_expected_bucket_owner.strip():
                raise ValueError("s3_expected_bucket_owner must be non-empty when provided")
            if not isinstance(self.s3_immutability_mode, AnchorImmutabilityMode):
                raise TypeError("s3_immutability_mode must be an AnchorImmutabilityMode")
            if not isinstance(self.s3_retention_period, timedelta):
                raise TypeError("s3_retention_period must be a datetime.timedelta")
            if self.s3_retention_period.total_seconds() <= 0:
                raise ValueError("s3_retention_period must be positive")


@dataclass(frozen=True)
class AuditArchiveStoreConfig:
    provider: LedgerAnchorStoreProvider
    s3_bucket: str | None = None
    s3_expected_bucket_owner: str | None = None
    s3_immutability_mode: AnchorImmutabilityMode | None = None
    s3_key_prefix: str = "audit-ledger-archives"
    s3_retention_period: timedelta | None = None
    s3_verify_bucket_configuration: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.provider, LedgerAnchorStoreProvider):
            raise TypeError("provider must be a LedgerAnchorStoreProvider")
        if self.provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
            raise ValueError("audit archive stores must use aws_s3_object_lock")
        if not isinstance(self.s3_key_prefix, str):
            raise TypeError("s3_key_prefix must be a string")
        if not isinstance(self.s3_verify_bucket_configuration, bool):
            raise TypeError("s3_verify_bucket_configuration must be a bool")
        if not isinstance(self.s3_bucket, str) or not self.s3_bucket.strip():
            raise ValueError("s3_bucket must be a non-empty string")
        if self.s3_expected_bucket_owner is not None and not self.s3_expected_bucket_owner.strip():
            raise ValueError("s3_expected_bucket_owner must be non-empty when provided")
        if not isinstance(self.s3_immutability_mode, AnchorImmutabilityMode):
            raise TypeError("s3_immutability_mode must be an AnchorImmutabilityMode")
        if not isinstance(self.s3_retention_period, timedelta):
            raise TypeError("s3_retention_period must be a datetime.timedelta")
        if self.s3_retention_period.total_seconds() <= 0:
            raise ValueError("s3_retention_period must be positive")


@dataclass(frozen=True)
class CoinbaseBotConfig:
    audit_anchor_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.AUDIT_ANCHOR,
            interval=timedelta(hours=24),
            enabled=False,
            run_on_start=False,
        )
    )
    audit_anchor_store: AuditAnchorStoreConfig | None = None
    audit_archive_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.AUDIT_ARCHIVE,
            interval=timedelta(hours=24),
            enabled=False,
            run_on_start=False,
        )
    )
    audit_archive_store: AuditArchiveStoreConfig | None = None
    feed: FeedRuntimeConfig = field(default_factory=FeedRuntimeConfig)
    feed_health_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.FEED_HEALTH,
            interval=timedelta(seconds=5),
            enabled=True,
            run_on_start=False,
        )
    )
    product_catalog: ProductCatalogRuntimeConfig = field(default_factory=ProductCatalogRuntimeConfig)
    risk: RiskPolicyConfig = field(default_factory=RiskPolicyConfig)
    rest: CoinbaseRestApiConfig = field(default_factory=CoinbaseRestApiConfig)
    reconciliation: ReconciliationRuntimeConfig = field(default_factory=ReconciliationRuntimeConfig)
    strategies: StrategyRuntimeConfig = field(default_factory=StrategyRuntimeConfig)
    trigger_polling_schedule: TaskScheduleConfig = field(
        default_factory=lambda: TaskScheduleConfig(
            task_id=RuntimeTask.TRIGGER_POLLING,
            interval=timedelta(seconds=1),
            enabled=False,
        )
    )
    trigger_rules: tuple[TriggerRuleConfig, ...] = ()
    websocket_sources: tuple[CoinbaseWebSocketSourceConfig, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.rest, CoinbaseRestApiConfig):
            raise TypeError("rest must be a CoinbaseRestApiConfig")
        if not isinstance(self.reconciliation, ReconciliationRuntimeConfig):
            raise TypeError("reconciliation must be a ReconciliationRuntimeConfig")
        if not isinstance(self.feed, FeedRuntimeConfig):
            raise TypeError("feed must be a FeedRuntimeConfig")
        if not isinstance(self.risk, RiskPolicyConfig):
            raise TypeError("risk must be a RiskPolicyConfig")
        if not isinstance(self.product_catalog, ProductCatalogRuntimeConfig):
            raise TypeError("product_catalog must be a ProductCatalogRuntimeConfig")
        if not isinstance(self.strategies, StrategyRuntimeConfig):
            raise TypeError("strategies must be a StrategyRuntimeConfig")
        _require_schedule(self.feed_health_schedule, RuntimeTask.FEED_HEALTH)
        if self.audit_anchor_store is not None and not isinstance(
            self.audit_anchor_store,
            AuditAnchorStoreConfig,
        ):
            raise TypeError("audit_anchor_store must be an AuditAnchorStoreConfig")
        if self.audit_archive_store is not None and not isinstance(
            self.audit_archive_store,
            AuditArchiveStoreConfig,
        ):
            raise TypeError("audit_archive_store must be an AuditArchiveStoreConfig")
        _require_schedule(self.audit_anchor_schedule, RuntimeTask.AUDIT_ANCHOR)
        _require_schedule(self.audit_archive_schedule, RuntimeTask.AUDIT_ARCHIVE)
        _require_schedule(self.trigger_polling_schedule, RuntimeTask.TRIGGER_POLLING)
        for rule in self.trigger_rules:
            if not isinstance(rule, (TimeTriggerConfig, MessageTriggerConfig)):
                raise TypeError("trigger_rules must contain trigger config values")
        trigger_ids = [rule.trigger_id for rule in self.trigger_rules]
        if len(trigger_ids) != len(set(trigger_ids)):
            raise ValueError("trigger_ids must be unique")
        for source in self.websocket_sources:
            if not isinstance(source, CoinbaseWebSocketSourceConfig):
                raise TypeError("websocket_sources must contain CoinbaseWebSocketSourceConfig values")
        source_ids = [source.source_id for source in self.websocket_sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("websocket source_ids must be unique")

    def enabled_schedules(self) -> tuple[TaskScheduleConfig, ...]:
        schedules = list(self.reconciliation.enabled_schedules())
        if self.audit_anchor_schedule.enabled:
            schedules.insert(0, self.audit_anchor_schedule)
        if self.audit_archive_schedule.enabled:
            insert_at = 1 if self.audit_anchor_schedule.enabled else 0
            schedules.insert(insert_at, self.audit_archive_schedule)
        if self.product_catalog.schedule.enabled:
            schedules.append(self.product_catalog.schedule)
        if self.feed_health_schedule.enabled and self.websocket_sources:
            schedules.append(self.feed_health_schedule)
        if self.strategies.schedule.enabled:
            schedules.append(self.strategies.schedule)
        if self.trigger_polling_schedule.enabled:
            schedules.insert(0, self.trigger_polling_schedule)
        return tuple(schedules)

    def rest_backed_schedules(self) -> tuple[TaskScheduleConfig, ...]:
        schedules = list(self.reconciliation.rest_backed_schedules())
        if self.product_catalog.schedule.enabled:
            schedules.append(self.product_catalog.schedule)
        return tuple(schedules)

    def live_rest_execution_enabled(self) -> bool:
        return self.rest.execution_mode == ExecutionMode.LIVE

    def token_provider_required(self) -> bool:
        return self.live_rest_execution_enabled() or bool(self.rest_backed_schedules())

    def jwt_factory_required(self) -> bool:
        return any(source.requires_jwt_factory() for source in self.websocket_sources)


def effective_risk_policy_config(config: CoinbaseBotConfig) -> RiskPolicyConfig:
    if not isinstance(config, CoinbaseBotConfig):
        raise TypeError("config must be a CoinbaseBotConfig")
    if config.strategies.operator_policy is None:
        return config.risk
    return _merge_risk_policy_configs(
        config.risk,
        config.strategies.operator_policy.to_risk_policy_config(),
    )


def effective_strategy_allow_live_execution(config: CoinbaseBotConfig) -> bool:
    if not isinstance(config, CoinbaseBotConfig):
        raise TypeError("config must be a CoinbaseBotConfig")
    if config.strategies.operator_policy is None:
        return config.strategies.allow_live_execution
    return config.strategies.allow_live_execution and config.strategies.operator_policy.scope.live_orders_allowed


def effective_strategy_market_data_requirements(
    config: CoinbaseBotConfig,
) -> tuple[StrategyInputRequirement, ...]:
    if not isinstance(config, CoinbaseBotConfig):
        raise TypeError("config must be a CoinbaseBotConfig")
    requirements = list(config.strategies.market_data_requirements)
    if config.strategies.operator_policy is not None:
        requirements.extend(config.strategies.operator_policy.strategy_input_requirements())
    return _merged_strategy_input_requirements(tuple(requirements))


@dataclass(frozen=True)
class CoinbaseRuntimeAssembly:
    account_lookup_client: CoinbaseAdvancedTradeAccountLookupClient | None
    action_gateway: ActionGateway
    exchange_state_reconciliation: ExchangeStateReconciliation | None
    audit_anchor_task: AuditAnchorTask | None
    audit_archive_task: AuditArchiveTask | None
    feed_router: RedundantFeedRouter | None
    feed_supervisor: FeedSupervisor | None
    fill_lookup_client: CoinbaseAdvancedTradeFillLookupClient | None
    fill_reconciliation: FillReconciliation | None
    order_lookup_client: CoinbaseAdvancedTradeOrderLookupClient | None
    orchestrator: RuntimeOrchestrator
    product_catalog: ProductCatalog | None
    product_catalog_client: ProductCatalogLookup | None
    product_catalog_refresh_task: ProductCatalogRefreshTask | None
    recovery: ReconciliationRecovery | None
    rest_config: CoinbaseRestConfig
    rest_executor: ActionExecutor
    strategies: tuple[Strategy, ...]
    strategy_task: StrategyEvaluationTask | None
    watchdog: ReconciliationWatchdog
    websocket_configs: tuple[CoinbaseWebSocketConfig, ...]
    websocket_feed_sources: tuple[AsyncFeedSource, ...]


def assemble_coinbase_runtime(
    *,
    config: CoinbaseBotConfig,
    core: AuditCore,
    jwt_factory: JwtFactory | None = None,
    rest_retry_sleep: RestRetrySleep | None = None,
    sleep: Sleep | None = None,
    startup_metadata: Mapping[str, Any] | None = None,
    token_provider: TokenProvider | None = None,
    transport: HttpTransport | None = None,
    websocket_source_factory: WebSocketSourceFactory | None = None,
    clock: Clock | None = None,
    audit_anchor_store: LedgerAnchorStore | None = None,
    audit_archive_store: LedgerArchiveStore | None = None,
    product_catalog: ProductCatalog | None = None,
    product_catalog_client: ProductCatalogLookup | None = None,
    risk_gate: RiskGate | None = None,
    strategies: tuple[Strategy, ...] = (),
) -> CoinbaseRuntimeAssembly:
    if not isinstance(config, CoinbaseBotConfig):
        raise TypeError("config must be a CoinbaseBotConfig")

    rest_backed_schedules = tuple(
        schedule
        for schedule in config.rest_backed_schedules()
        if schedule.task_id != RuntimeTask.PRODUCT_CATALOG_REFRESH or product_catalog_client is None
    )
    if rest_backed_schedules and token_provider is None:
        enabled_task_ids = ", ".join(schedule.task_id.value for schedule in rest_backed_schedules)
        raise ValueError(f"token_provider is required for enabled REST-backed schedules: {enabled_task_ids}")

    rest_config = config.rest.to_rest_config()
    http_transport = _http_transport(
        core=core,
        policy=config.rest.retry_policy,
        rest_retry_sleep=rest_retry_sleep,
        transport=transport,
    )
    resolved_product_catalog = _product_catalog(config=config, core=core, product_catalog=product_catalog)
    rest_executor: ActionExecutor
    if config.live_rest_execution_enabled():
        if token_provider is None:
            raise ValueError("token_provider is required for live REST execution")
        rest_executor = _live_rest_executor(
            CoinbaseAdvancedTradeRestExecutor(
                rest_config,
                token_provider=token_provider,
                transport=http_transport,
            ),
            product_catalog=resolved_product_catalog,
            rest_config=rest_config,
        )
    else:
        rest_executor = DryRunExecutor()
    effective_risk = effective_risk_policy_config(config)
    action_gateway = ActionGateway(
        core,
        risk_gate=risk_gate or RiskGate(effective_risk.to_policy(product_catalog=resolved_product_catalog)),
    )

    websocket_configs = tuple(
        source.to_websocket_config(jwt_factory=jwt_factory)
        for source in config.websocket_sources
    )
    websocket_feed_sources = _websocket_feed_sources(
        websocket_configs,
        websocket_source_factory=websocket_source_factory,
    )
    feed_router = _feed_router(
        core=core,
        bot_config=config,
        clock=clock,
        websocket_configs=websocket_configs,
    )
    feed_supervisor = (
        FeedSupervisor(
            core,
            feed_router,
            websocket_feed_sources,
            reconnect_policy=config.feed.reconnect_policy,
            sleep=sleep,
        )
        if feed_router is not None
        else None
    )

    order_lookup_client = _order_lookup_client(rest_config, token_provider, http_transport)
    fill_lookup_client = _fill_lookup_client(rest_config, token_provider, http_transport)
    account_lookup_client = _account_lookup_client(rest_config, token_provider, http_transport)
    position_lookup_client = _position_lookup_client(rest_config, token_provider, http_transport)
    product_catalog_client = product_catalog_client or (
        _product_catalog_client(rest_config, token_provider, http_transport)
        if config.product_catalog.schedule.enabled
        else None
    )

    watchdog = ReconciliationWatchdog(
        core,
        clock=clock,
        policy=config.reconciliation.watchdog_policy,
    )
    recovery = (
        ReconciliationRecovery(
            core,
            clock=clock,
            order_lookup_client=order_lookup_client,
        )
        if order_lookup_client is not None
        else None
    )
    fill_reconciliation = (
        FillReconciliation(
            core,
            clock=clock,
            fill_lookup_client=fill_lookup_client,
            policy=config.reconciliation.fill_policy,
        )
        if fill_lookup_client is not None
        else None
    )
    exchange_state_reconciliation = (
        ExchangeStateReconciliation(
            core,
            account_lookup_client=account_lookup_client,
            clock=clock,
            policy=_exchange_state_policy(config),
            position_lookup_client=position_lookup_client,
        )
        if account_lookup_client is not None or position_lookup_client is not None
        else None
    )
    audit_anchor_task = (
        AuditAnchorTask(core.ledger.path, audit_anchor_store, clock=clock)
        if audit_anchor_store is not None
        else None
    )
    audit_archive_task = (
        AuditArchiveTask(core.ledger.path, audit_archive_store, clock=clock)
        if audit_archive_store is not None
        else None
    )
    if config.product_catalog.schedule.enabled and product_catalog_client is None:
        raise ValueError("product catalog refresh requires a product catalog client")
    product_catalog_refresh_task = (
        ProductCatalogRefreshTask(
            core,
            clock=clock,
            lookup_client=product_catalog_client,
            product_catalog=resolved_product_catalog,
            product_ids=config.product_catalog.product_ids,
        )
        if config.product_catalog.schedule.enabled and resolved_product_catalog is not None
        else None
    )
    selected_strategies = (
        configured_strategies(
            config.strategies.strategy_ids,
            static_strategies=strategies,
            strategy_parameters=config.strategies.strategy_parameters,
        )
        if config.strategies.schedule.enabled
        else ()
    )
    strategy_task = (
        StrategyEvaluationTask(
            core,
            action_gateway=action_gateway,
            allow_live_execution=effective_strategy_allow_live_execution(config),
            clock=clock,
            execution_mode=config.rest.execution_mode,
            executor=rest_executor,
            market_data_requirements=effective_strategy_market_data_requirements(config),
            max_market_trades_per_product=config.strategies.max_market_trades_per_product,
            max_order_book_sample_depth_per_side=(
                config.strategies.max_order_book_sample_depth_per_side
            ),
            max_order_book_samples_per_product=config.strategies.max_order_book_samples_per_product,
            operator_policy=config.strategies.operator_policy,
            product_catalog=resolved_product_catalog,
            strategies=selected_strategies,
        )
        if config.strategies.schedule.enabled
        else None
    )

    scheduled_tasks = _scheduled_tasks(
        audit_anchor_task=audit_anchor_task,
        audit_archive_task=audit_archive_task,
        config=config,
        core=core,
        feed_router=feed_router,
        watchdog=watchdog,
        recovery=recovery,
        fill_reconciliation=fill_reconciliation,
        exchange_state_reconciliation=exchange_state_reconciliation,
        product_catalog_refresh_task=product_catalog_refresh_task,
        strategy_task=strategy_task,
    )
    orchestrator = RuntimeOrchestrator(
        core,
        scheduled_tasks,
        clock=clock,
        sleep=sleep,
        startup_metadata=startup_metadata,
    )
    return CoinbaseRuntimeAssembly(
        account_lookup_client=account_lookup_client,
        action_gateway=action_gateway,
        audit_anchor_task=audit_anchor_task,
        audit_archive_task=audit_archive_task,
        exchange_state_reconciliation=exchange_state_reconciliation,
        feed_router=feed_router,
        feed_supervisor=feed_supervisor,
        fill_lookup_client=fill_lookup_client,
        fill_reconciliation=fill_reconciliation,
        order_lookup_client=order_lookup_client,
        orchestrator=orchestrator,
        product_catalog=resolved_product_catalog,
        product_catalog_client=product_catalog_client,
        product_catalog_refresh_task=product_catalog_refresh_task,
        recovery=recovery,
        rest_config=rest_config,
        rest_executor=rest_executor,
        strategies=selected_strategies,
        strategy_task=strategy_task,
        watchdog=watchdog,
        websocket_configs=websocket_configs,
        websocket_feed_sources=websocket_feed_sources,
    )


def _scheduled_tasks(
    *,
    audit_anchor_task: AuditAnchorTask | None,
    audit_archive_task: AuditArchiveTask | None,
    config: CoinbaseBotConfig,
    core: AuditCore,
    feed_router: RedundantFeedRouter | None,
    watchdog: ReconciliationWatchdog,
    recovery: ReconciliationRecovery | None,
    fill_reconciliation: FillReconciliation | None,
    exchange_state_reconciliation: ExchangeStateReconciliation | None,
    product_catalog_refresh_task: ProductCatalogRefreshTask | None,
    strategy_task: StrategyEvaluationTask | None,
) -> tuple[ScheduledRuntimeTask, ...]:
    handlers: dict[RuntimeTask, Callable[[], object]] = {
        RuntimeTask.TRIGGER_POLLING: core.emit_due_time_triggers,
        RuntimeTask.WATCHDOG: watchdog.audit,
    }
    if audit_anchor_task is not None:
        handlers[RuntimeTask.AUDIT_ANCHOR] = audit_anchor_task.run
    if audit_archive_task is not None:
        handlers[RuntimeTask.AUDIT_ARCHIVE] = audit_archive_task.run
    if feed_router is not None:
        handlers[RuntimeTask.FEED_HEALTH] = feed_router.audit_health
    if recovery is not None:
        handlers[RuntimeTask.ORDER_RECOVERY] = recovery.recover
    if fill_reconciliation is not None:
        handlers[RuntimeTask.FILL_RECONCILIATION] = fill_reconciliation.reconcile
    if exchange_state_reconciliation is not None:
        handlers[RuntimeTask.EXCHANGE_STATE_RECONCILIATION] = exchange_state_reconciliation.reconcile
    if product_catalog_refresh_task is not None:
        handlers[RuntimeTask.PRODUCT_CATALOG_REFRESH] = product_catalog_refresh_task.refresh
    if strategy_task is not None:
        handlers[RuntimeTask.STRATEGY_EVALUATION] = strategy_task.run

    tasks: list[ScheduledRuntimeTask] = []
    for schedule in _enabled_schedules(config):
        handler = handlers.get(schedule.task_id)
        if handler is None:
            raise ValueError(f"enabled schedule has no assembled handler: {schedule.task_id.value}")
        tasks.append(
            ScheduledRuntimeTask(
                task_id=schedule.task_id,
                interval=schedule.interval,
                handler=handler,
                run_on_start=schedule.run_on_start,
            )
        )
    return tuple(tasks)


def _enabled_schedules(config: CoinbaseBotConfig) -> tuple[TaskScheduleConfig, ...]:
    return config.enabled_schedules()


def trigger_engine_from_config(
    config: CoinbaseBotConfig,
    *,
    clock: Clock | None = None,
) -> TriggerEngine | None:
    if not config.trigger_rules:
        return None
    engine = TriggerEngine(clock=clock)
    for rule_config in config.trigger_rules:
        engine.register(rule_config.to_rule())
    return engine


def _websocket_feed_sources(
    websocket_configs: tuple[CoinbaseWebSocketConfig, ...],
    *,
    websocket_source_factory: WebSocketSourceFactory | None,
) -> tuple[AsyncFeedSource, ...]:
    factory = websocket_source_factory or CoinbaseAdvancedTradeFeedSource
    feed_sources = tuple(factory(config) for config in websocket_configs)
    for config, source in zip(websocket_configs, feed_sources, strict=True):
        if source.source_id != config.source_id:
            raise ValueError(
                "websocket source factory returned mismatched source_id: "
                f"expected {config.source_id}, observed {source.source_id}"
            )
    return feed_sources


def _feed_router(
    *,
    bot_config: CoinbaseBotConfig,
    core: AuditCore,
    clock: Clock | None,
    websocket_configs: tuple[CoinbaseWebSocketConfig, ...],
) -> RedundantFeedRouter | None:
    if not websocket_configs:
        return None
    return RedundantFeedRouter.from_ledger(
        core,
        clock=clock,
        expected_source_ids=tuple(source_config.source_id for source_config in websocket_configs),
        min_live_sources=bot_config.feed.min_live_sources,
        stale_after=bot_config.feed.stale_after,
    )


def _exchange_state_policy(config: CoinbaseBotConfig) -> ExchangeStateReconciliationPolicy:
    policy = config.reconciliation.exchange_state_policy
    return ExchangeStateReconciliationPolicy(
        account_page_limit=policy.account_page_limit,
        max_account_pages=policy.max_account_pages,
        position_size_tolerance=policy.position_size_tolerance,
        position_product_ids=_exchange_state_position_product_ids(config),
        retail_portfolio_id=policy.retail_portfolio_id or config.rest.retail_portfolio_id,
        perpetual_portfolio_uuid=policy.perpetual_portfolio_uuid or config.rest.perpetual_portfolio_uuid,
    )


def _exchange_state_position_product_ids(config: CoinbaseBotConfig) -> tuple[str, ...]:
    explicit_product_ids = config.reconciliation.exchange_state_policy.position_product_ids
    if explicit_product_ids:
        return explicit_product_ids

    product_ids: list[str] = []
    for product_id in effective_risk_policy_config(config).allowed_products:
        if product_id not in product_ids:
            product_ids.append(product_id)
    for product_id in config.product_catalog.product_ids:
        if product_id not in product_ids:
            product_ids.append(product_id)
    for websocket_source in config.websocket_sources:
        for product_id in websocket_source.product_ids:
            if product_id not in product_ids:
                product_ids.append(product_id)
    return tuple(product_ids)


def _http_transport(
    *,
    core: AuditCore,
    policy: CoinbaseRestRetryPolicy,
    rest_retry_sleep: RestRetrySleep | None,
    transport: HttpTransport | None,
) -> HttpTransport:
    base_transport = transport or UrlLibHttpTransport()
    if policy.max_attempts <= 1:
        return base_transport
    return CoinbaseRetryingHttpTransport(
        base_transport,
        core=core,
        policy=policy,
        sleep=rest_retry_sleep,
    )


def _live_rest_executor(
    executor: CoinbaseAdvancedTradeRestExecutor,
    *,
    product_catalog: ProductCatalog | None,
    rest_config: CoinbaseRestConfig,
) -> ActionExecutor:
    if product_catalog is None:
        return executor
    return ProductVenueRestrictedExecutor(
        executor,
        allowed_venues=COINBASE_LIVE_EXECUTION_PRODUCT_VENUES,
        mode=rest_config.execution_mode,
        product_catalog=product_catalog,
    )


def _order_lookup_client(
    rest_config: CoinbaseRestConfig,
    token_provider: TokenProvider | None,
    transport: HttpTransport | None,
) -> CoinbaseAdvancedTradeOrderLookupClient | None:
    if token_provider is None:
        return None
    return CoinbaseAdvancedTradeOrderLookupClient(rest_config, token_provider=token_provider, transport=transport)


def _fill_lookup_client(
    rest_config: CoinbaseRestConfig,
    token_provider: TokenProvider | None,
    transport: HttpTransport | None,
) -> CoinbaseAdvancedTradeFillLookupClient | None:
    if token_provider is None:
        return None
    return CoinbaseAdvancedTradeFillLookupClient(rest_config, token_provider=token_provider, transport=transport)


def _account_lookup_client(
    rest_config: CoinbaseRestConfig,
    token_provider: TokenProvider | None,
    transport: HttpTransport | None,
) -> CoinbaseAdvancedTradeAccountLookupClient | None:
    if token_provider is None:
        return None
    return CoinbaseAdvancedTradeAccountLookupClient(rest_config, token_provider=token_provider, transport=transport)


def _position_lookup_client(
    rest_config: CoinbaseRestConfig,
    token_provider: TokenProvider | None,
    transport: HttpTransport | None,
) -> CoinbaseAdvancedTradePositionLookupClient | None:
    if token_provider is None:
        return None
    return CoinbaseAdvancedTradePositionLookupClient(rest_config, token_provider=token_provider, transport=transport)


def _product_catalog_client(
    rest_config: CoinbaseRestConfig,
    token_provider: TokenProvider | None,
    transport: HttpTransport | None,
) -> CoinbaseProductCatalogClient | None:
    if token_provider is None:
        return None
    return CoinbaseProductCatalogClient(rest_config, token_provider=token_provider, transport=transport)


def _product_catalog(
    *,
    config: CoinbaseBotConfig,
    core: AuditCore,
    product_catalog: ProductCatalog | None,
) -> ProductCatalog | None:
    if product_catalog is not None:
        return product_catalog
    if config.product_catalog.schedule.enabled:
        return product_catalog_from_projection(SourceOfTruthProjection.from_ledger(core.ledger))
    return None


def _merged_strategy_input_requirements(
    requirements: tuple[StrategyInputRequirement, ...],
) -> tuple[StrategyInputRequirement, ...]:
    merged: dict[tuple[str, Any], StrategyInputRequirement] = {}
    for requirement in requirements:
        key = (requirement.product_id, requirement.data_kind)
        existing = merged.get(key)
        if existing is None or requirement.max_age < existing.max_age:
            merged[key] = requirement
    return tuple(
        merged[key]
        for key in sorted(
            merged,
            key=lambda item: (item[0], item[1].value),
        )
    )


def _merge_risk_policy_configs(base: RiskPolicyConfig, overlay: RiskPolicyConfig) -> RiskPolicyConfig:
    return RiskPolicyConfig(
        allowed_lineage_relations=_intersect_or_select(
            base.allowed_lineage_relations,
            overlay.allowed_lineage_relations,
            "allowed_lineage_relations",
        ),
        allowed_order_types=_intersect_or_select(
            base.allowed_order_types,
            overlay.allowed_order_types,
            "allowed_order_types",
        ),
        allowed_placement_kinds=_intersect_or_select(
            base.allowed_placement_kinds,
            overlay.allowed_placement_kinds,
            "allowed_placement_kinds",
        ),
        allowed_products=_intersect_or_select(
            base.allowed_products,
            overlay.allowed_products,
            "allowed_products",
        ),
        allowed_sides=_intersect_or_select(
            base.allowed_sides,
            overlay.allowed_sides,
            "allowed_sides",
        ),
        allowed_time_in_force=_intersect_or_select(
            base.allowed_time_in_force,
            overlay.allowed_time_in_force,
            "allowed_time_in_force",
        ),
        kill_switch_enabled=base.kill_switch_enabled or overlay.kill_switch_enabled,
        max_daily_notional=_minimum_optional_decimal(
            base.max_daily_notional,
            overlay.max_daily_notional,
        ),
        max_leverage=_minimum_optional_decimal(base.max_leverage, overlay.max_leverage),
        max_open_orders=_minimum_optional_int(base.max_open_orders, overlay.max_open_orders),
        max_order_notional=_minimum_optional_decimal(
            base.max_order_notional,
            overlay.max_order_notional,
        ),
        max_order_replacements=_minimum_optional_int(
            base.max_order_replacements,
            overlay.max_order_replacements,
        ),
        max_order_size=_minimum_optional_decimal(base.max_order_size, overlay.max_order_size),
        max_visible_notional=_minimum_optional_decimal(
            base.max_visible_notional,
            overlay.max_visible_notional,
        ),
        require_post_only=base.require_post_only or overlay.require_post_only,
        require_reduce_only=base.require_reduce_only or overlay.require_reduce_only,
        require_staged_release_above_visible_limit=(
            base.require_staged_release_above_visible_limit
            or overlay.require_staged_release_above_visible_limit
        ),
    )


def _intersect_or_select(base: tuple[Any, ...], overlay: tuple[Any, ...], field_name: str) -> tuple[Any, ...]:
    if not base:
        return overlay
    if not overlay:
        return base
    selected = tuple(item for item in base if item in overlay)
    if not selected:
        raise ValueError(f"risk {field_name} and operator policy {field_name} do not overlap")
    return selected


def _minimum_optional_decimal(base: Decimal | None, overlay: Decimal | None) -> Decimal | None:
    if base is None:
        return overlay
    if overlay is None:
        return base
    return min(base, overlay)


def _minimum_optional_int(base: int | None, overlay: int | None) -> int | None:
    if base is None:
        return overlay
    if overlay is None:
        return base
    return min(base, overlay)


def _require_schedule(schedule: TaskScheduleConfig, task_id: RuntimeTask) -> None:
    if not isinstance(schedule, TaskScheduleConfig):
        raise TypeError(f"{task_id.value} schedule must be a TaskScheduleConfig")
    if schedule.task_id != task_id:
        raise ValueError(f"schedule task_id must be {task_id.value}")


def _require_positive_optional_decimal(value: Decimal | None, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _require_positive_optional_int(value: int | None, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _require_unique_enum_values(values: tuple[Any, ...], enum_type: type[Any], field_name: str) -> None:
    for value in values:
        if not isinstance(value, enum_type):
            raise TypeError(f"{field_name} must contain {enum_type.__name__} values")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must be unique")


def _channels_require_products(channels: tuple[CoinbaseWebSocketChannel, ...]) -> bool:
    for channel in channels:
        if channel not in {
            CoinbaseWebSocketChannel.FUTURES_BALANCE_SUMMARY,
            CoinbaseWebSocketChannel.HEARTBEATS,
        }:
            return True
    return False
