from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from app.bootstrap import CoinbaseApplicationConfig, DEFAULT_LEDGER_PATH
from config.assembly import (
    AuditAnchorStoreConfig,
    AuditArchiveStoreConfig,
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
    CoinbaseRestRetryPolicy,
    CoinbaseWebSocketSourceConfig,
    FeedRuntimeConfig,
    MessageTriggerConfig,
    ProductCatalogRuntimeConfig,
    ReconciliationRuntimeConfig,
    RiskPolicyConfig,
    StrategyRuntimeConfig,
    TaskScheduleConfig,
    TimeTriggerConfig,
)
from feeds import ReconnectPolicy
from core.enums import (
    AnchorImmutabilityMode,
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    EventType,
    ExecutionMode,
    LedgerAnchorStoreProvider,
    OrderType,
    RuntimeTask,
    MarketDataKind,
    TriggerRelation,
    TriggerRuleType,
)
from core.errors import ConfigError
from core.json_tools import normalize_json
from reconciliation.fills import FillReconciliationPolicy
from reconciliation.positions import ExchangeStateReconciliationPolicy
from reconciliation.watchdog import ReconciliationPolicy
from strategies import StrategyInputRequirement
from strategies.operator_policy import OperatorPolicy, load_operator_policy_from_json_file, operator_policy_from_mapping


ENV_PREFIX = "STATERAIL_"
LEGACY_ENV_PREFIX = "COINBASE_BOT_"
_KNOWN_ENV_SUFFIXES = frozenset(
    {
        "COINBASE_API_KEY_NAME",
        "COINBASE_API_PRIVATE_KEY",
        "COINBASE_API_PRIVATE_KEY_FILE",
        "ALLOW_LIVE_TRADING",
        "AUDIT_ANCHOR_ENABLED",
        "AUDIT_ANCHOR_INTERVAL_SECONDS",
        "AUDIT_ANCHOR_LOCAL_DIR",
        "AUDIT_ANCHOR_RUN_ON_START",
        "AUDIT_ANCHOR_S3_BUCKET",
        "AUDIT_ANCHOR_S3_EXPECTED_BUCKET_OWNER",
        "AUDIT_ANCHOR_S3_IMMUTABILITY_MODE",
        "AUDIT_ANCHOR_S3_KEY_PREFIX",
        "AUDIT_ANCHOR_S3_RETENTION_DAYS",
        "AUDIT_ANCHOR_S3_VERIFY_BUCKET_CONFIGURATION",
        "AUDIT_ANCHOR_STORE_PROVIDER",
        "AUDIT_ARCHIVE_ENABLED",
        "AUDIT_ARCHIVE_INTERVAL_SECONDS",
        "AUDIT_ARCHIVE_RUN_ON_START",
        "AUDIT_ARCHIVE_S3_BUCKET",
        "AUDIT_ARCHIVE_S3_EXPECTED_BUCKET_OWNER",
        "AUDIT_ARCHIVE_S3_IMMUTABILITY_MODE",
        "AUDIT_ARCHIVE_S3_KEY_PREFIX",
        "AUDIT_ARCHIVE_S3_RETENTION_DAYS",
        "AUDIT_ARCHIVE_S3_VERIFY_BUCKET_CONFIGURATION",
        "AUDIT_ARCHIVE_STORE_PROVIDER",
        "EXECUTION_MODE",
        "FEED_HEALTH_ENABLED",
        "FEED_HEALTH_INTERVAL_SECONDS",
        "FEED_HEALTH_RUN_ON_START",
        "FEED_MIN_LIVE_SOURCES",
        "FEED_RECONNECT_INITIAL_DELAY_SECONDS",
        "FEED_RECONNECT_MAX_DELAY_SECONDS",
        "FEED_RECONNECT_MULTIPLIER",
        "FEED_STALE_AFTER_SECONDS",
        "EXCHANGE_STATE_ACCOUNT_PAGE_LIMIT",
        "EXCHANGE_STATE_MAX_ACCOUNT_PAGES",
        "EXCHANGE_STATE_PERPETUAL_PORTFOLIO_UUID",
        "EXCHANGE_STATE_POSITION_PRODUCT_IDS",
        "EXCHANGE_STATE_POSITION_SIZE_TOLERANCE",
        "EXCHANGE_STATE_RECONCILIATION_ENABLED",
        "EXCHANGE_STATE_RECONCILIATION_INTERVAL_SECONDS",
        "EXCHANGE_STATE_RECONCILIATION_RUN_ON_START",
        "EXCHANGE_STATE_RETAIL_PORTFOLIO_ID",
        "FILL_RECONCILIATION_ENABLED",
        "FILL_RECONCILIATION_EXECUTION_MODES",
        "FILL_RECONCILIATION_INTERVAL_SECONDS",
        "FILL_RECONCILIATION_LIMIT",
        "FILL_RECONCILIATION_MAX_PAGES_PER_ORDER",
        "FILL_RECONCILIATION_RUN_ON_START",
        "LEDGER_PATH",
        "ORDER_RECOVERY_ENABLED",
        "ORDER_RECOVERY_INTERVAL_SECONDS",
        "ORDER_RECOVERY_RUN_ON_START",
        "PERPETUAL_PORTFOLIO_UUID",
        "PRODUCT_CATALOG_ENABLED",
        "PRODUCT_CATALOG_INTERVAL_SECONDS",
        "PRODUCT_CATALOG_PRODUCT_IDS",
        "PRODUCT_CATALOG_RUN_ON_START",
        "REST_BASE_URL",
        "REST_RETRY_INITIAL_DELAY_SECONDS",
        "REST_RETRY_MAX_ATTEMPTS",
        "REST_RETRY_MAX_DELAY_SECONDS",
        "REST_RETRY_MULTIPLIER",
        "RETAIL_PORTFOLIO_ID",
        "RISK_ALLOWED_ORDER_TYPES",
        "RISK_ALLOWED_PRODUCTS",
        "RISK_KILL_SWITCH_ENABLED",
        "RISK_MAX_DAILY_NOTIONAL",
        "RISK_MAX_LEVERAGE",
        "RISK_MAX_OPEN_ORDERS",
        "RISK_MAX_ORDER_NOTIONAL",
        "RISK_MAX_ORDER_SIZE",
        "RISK_REQUIRE_REDUCE_ONLY",
        "STRATEGIES_ALLOW_LIVE_EXECUTION",
        "STRATEGIES_ENABLED",
        "STRATEGIES_INTERVAL_SECONDS",
        "STRATEGIES_MARKET_DATA_REQUIREMENTS",
        "STRATEGIES_MAX_MARKET_TRADES_PER_PRODUCT",
        "STRATEGIES_MAX_ORDER_BOOK_SAMPLE_DEPTH_PER_SIDE",
        "STRATEGIES_MAX_ORDER_BOOK_SAMPLES_PER_PRODUCT",
        "STRATEGIES_OPERATOR_POLICY_FILE",
        "STRATEGIES_ORDER_BOOK_SAMPLE_PRODUCT_IDS",
        "STRATEGIES_PARAMETERS",
        "STRATEGIES_RUN_ON_START",
        "STRATEGY_IDS",
        "TRIGGER_POLLING_ENABLED",
        "TRIGGER_POLLING_INTERVAL_SECONDS",
        "TRIGGER_POLLING_RUN_ON_START",
        "TRIGGERS",
        "USER_CONFIRMATION_TIMEOUT_SECONDS",
        "WATCHDOG_ENABLED",
        "WATCHDOG_EXECUTION_MODES",
        "WATCHDOG_INTERVAL_SECONDS",
        "WATCHDOG_RUN_ON_START",
        "WEBSOCKET_SOURCES",
    }
)
_SCHEDULE_FIELDS = frozenset({"enabled", "interval_seconds", "run_on_start"})
_AUDIT_ANCHOR_FIELDS = _SCHEDULE_FIELDS | frozenset({"store"})
_AUDIT_ARCHIVE_FIELDS = _SCHEDULE_FIELDS | frozenset({"store"})
_AUDIT_ANCHOR_STORE_FIELDS = frozenset(
    {
        "anchor_dir",
        "bucket",
        "expected_bucket_owner",
        "immutability_mode",
        "key_prefix",
        "provider",
        "retention_days",
        "verify_bucket_configuration",
    }
)
_AUDIT_ARCHIVE_STORE_FIELDS = frozenset(
    {
        "bucket",
        "expected_bucket_owner",
        "immutability_mode",
        "key_prefix",
        "provider",
        "retention_days",
        "verify_bucket_configuration",
    }
)
_FEED_FIELDS = frozenset({"health", "min_live_sources", "reconnect", "stale_after_seconds"})
_FEED_RECONNECT_FIELDS = frozenset({"initial_delay_seconds", "max_delay_seconds", "multiplier"})
_RISK_FIELDS = frozenset(
    {
        "allowed_order_types",
        "allowed_products",
        "kill_switch_enabled",
        "max_daily_notional",
        "max_leverage",
        "max_open_orders",
        "max_order_notional",
        "max_order_size",
        "require_reduce_only",
    }
)
_PRODUCT_CATALOG_FIELDS = _SCHEDULE_FIELDS | frozenset({"product_ids"})
_STRATEGIES_FIELDS = _SCHEDULE_FIELDS | frozenset(
    {
        "allow_live_execution",
        "market_data_requirements",
        "max_market_trades_per_product",
        "max_order_book_sample_depth_per_side",
        "max_order_book_samples_per_product",
        "operator_policy",
        "operator_policy_file",
        "order_book_sample_product_ids",
        "strategy_parameters",
        "strategy_ids",
    }
)
_WATCHDOG_FIELDS = _SCHEDULE_FIELDS | frozenset({"execution_modes", "user_confirmation_timeout_seconds"})
_ORDER_RECOVERY_FIELDS = _SCHEDULE_FIELDS
_FILL_FIELDS = _SCHEDULE_FIELDS | frozenset({"execution_modes", "limit", "max_pages_per_order"})
_EXCHANGE_STATE_FIELDS = _SCHEDULE_FIELDS | frozenset(
    {
        "account_page_limit",
        "max_account_pages",
        "perpetual_portfolio_uuid",
        "position_product_ids",
        "position_size_tolerance",
        "retail_portfolio_id",
    }
)


def has_coinbase_application_env(
    env: Mapping[str, str] | None = None,
    *,
    prefix: str = ENV_PREFIX,
) -> bool:
    environment = os.environ if env is None else env
    return any(key.startswith(prefix) or key.startswith(LEGACY_ENV_PREFIX) for key in environment)


def load_coinbase_application_config_from_env(
    env: Mapping[str, str] | None = None,
    *,
    prefix: str = ENV_PREFIX,
) -> CoinbaseApplicationConfig:
    environment = os.environ if env is None else env
    return load_coinbase_application_config_from_mapping(_env_to_raw(environment, prefix=prefix))


def load_coinbase_application_config_from_json_file(path: str | Path) -> CoinbaseApplicationConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        try:
            raw = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ConfigError(
                f"Invalid JSON config file: {config_path}",
                context={"config_path": str(config_path)},
            ) from exc
    return load_coinbase_application_config_from_mapping(raw)


def load_coinbase_application_config_from_mapping(raw: Mapping[str, Any]) -> CoinbaseApplicationConfig:
    if not isinstance(raw, Mapping):
        raise TypeError("application config must be a mapping")
    _reject_unknown(raw, {"bot", "ledger_path"}, "application")

    bot_raw = _mapping(raw.get("bot", {}), "bot")
    return CoinbaseApplicationConfig(
        ledger_path=Path(_string(raw.get("ledger_path", str(DEFAULT_LEDGER_PATH)), "ledger_path")),
        bot=_bot_config(bot_raw),
    )


def _bot_config(raw: Mapping[str, Any]) -> CoinbaseBotConfig:
    _reject_unknown(
        raw,
        {
            "audit_anchor",
            "audit_archive",
            "feed",
            "product_catalog",
            "reconciliation",
            "rest",
            "risk",
            "strategies",
            "trigger_polling",
            "triggers",
            "websocket_sources",
        },
        "bot",
    )
    audit_anchor_raw = _mapping(raw.get("audit_anchor", {}), "bot.audit_anchor")
    audit_archive_raw = _mapping(raw.get("audit_archive", {}), "bot.audit_archive")
    feed_raw = _mapping(raw.get("feed", {}), "bot.feed")
    product_catalog_raw = _mapping(raw.get("product_catalog", {}), "bot.product_catalog")
    websocket_sources_raw = raw.get("websocket_sources", ())
    defaults = CoinbaseBotConfig()
    return CoinbaseBotConfig(
        audit_anchor_schedule=_schedule(
            audit_anchor_raw,
            allowed_fields=_AUDIT_ANCHOR_FIELDS,
            field_name="bot.audit_anchor",
            task_id=RuntimeTask.AUDIT_ANCHOR,
            default_interval_seconds=defaults.audit_anchor_schedule.interval.total_seconds(),
            default_enabled=defaults.audit_anchor_schedule.enabled,
            default_run_on_start=defaults.audit_anchor_schedule.run_on_start,
        ),
        audit_anchor_store=_audit_anchor_store_config(audit_anchor_raw.get("store")),
        audit_archive_schedule=_schedule(
            audit_archive_raw,
            allowed_fields=_AUDIT_ARCHIVE_FIELDS,
            field_name="bot.audit_archive",
            task_id=RuntimeTask.AUDIT_ARCHIVE,
            default_interval_seconds=defaults.audit_archive_schedule.interval.total_seconds(),
            default_enabled=defaults.audit_archive_schedule.enabled,
            default_run_on_start=defaults.audit_archive_schedule.run_on_start,
        ),
        audit_archive_store=_audit_archive_store_config(audit_archive_raw.get("store")),
        feed=_feed_config(feed_raw),
        feed_health_schedule=_schedule(
            _mapping(feed_raw.get("health", {}), "bot.feed.health"),
            allowed_fields=_SCHEDULE_FIELDS,
            field_name="bot.feed.health",
            task_id=RuntimeTask.FEED_HEALTH,
            default_interval_seconds=defaults.feed_health_schedule.interval.total_seconds(),
            default_enabled=defaults.feed_health_schedule.enabled,
            default_run_on_start=defaults.feed_health_schedule.run_on_start,
        ),
        product_catalog=_product_catalog_config(product_catalog_raw),
        rest=_rest_config(_mapping(raw.get("rest", {}), "bot.rest")),
        risk=_risk_config(_mapping(raw.get("risk", {}), "bot.risk")),
        reconciliation=_reconciliation_config(_mapping(raw.get("reconciliation", {}), "bot.reconciliation")),
        strategies=_strategies_config(_mapping(raw.get("strategies", {}), "bot.strategies")),
        trigger_polling_schedule=_schedule(
            _mapping(raw.get("trigger_polling", {}), "bot.trigger_polling"),
            allowed_fields=_SCHEDULE_FIELDS,
            field_name="bot.trigger_polling",
            task_id=RuntimeTask.TRIGGER_POLLING,
            default_interval_seconds=defaults.trigger_polling_schedule.interval.total_seconds(),
            default_enabled=defaults.trigger_polling_schedule.enabled,
            default_run_on_start=defaults.trigger_polling_schedule.run_on_start,
        ),
        trigger_rules=tuple(
            _trigger_rule_config(_mapping(rule, "bot.triggers[]"))
            for rule in _sequence(raw.get("triggers", ()), "bot.triggers")
        ),
        websocket_sources=tuple(
            _websocket_source_config(_mapping(source, "bot.websocket_sources[]"))
            for source in _sequence(websocket_sources_raw, "bot.websocket_sources")
        ),
    )


def _audit_anchor_store_config(value: Any) -> AuditAnchorStoreConfig | None:
    if value is None:
        return None

    raw = _mapping(value, "bot.audit_anchor.store")
    _reject_unknown(raw, set(_AUDIT_ANCHOR_STORE_FIELDS), "bot.audit_anchor.store")
    provider = _enum(raw.get("provider"), LedgerAnchorStoreProvider, "bot.audit_anchor.store.provider")

    if provider == LedgerAnchorStoreProvider.LOCAL_FILE:
        _reject_unknown(raw, {"anchor_dir", "provider"}, "bot.audit_anchor.store")
        return AuditAnchorStoreConfig(
            provider=provider,
            local_anchor_dir=Path(_string(raw.get("anchor_dir"), "bot.audit_anchor.store.anchor_dir")),
        )

    if provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        _reject_unknown(
            raw,
            {
                "bucket",
                "expected_bucket_owner",
                "immutability_mode",
                "key_prefix",
                "provider",
                "retention_days",
                "verify_bucket_configuration",
            },
            "bot.audit_anchor.store",
        )
        return AuditAnchorStoreConfig(
            provider=provider,
            s3_bucket=_string(raw.get("bucket"), "bot.audit_anchor.store.bucket"),
            s3_expected_bucket_owner=_optional_string(
                raw.get("expected_bucket_owner"),
                "bot.audit_anchor.store.expected_bucket_owner",
            ),
            s3_immutability_mode=_enum(
                raw.get("immutability_mode"),
                AnchorImmutabilityMode,
                "bot.audit_anchor.store.immutability_mode",
            ),
            s3_key_prefix=_string(
                raw.get("key_prefix", "audit-anchors"),
                "bot.audit_anchor.store.key_prefix",
            ),
            s3_retention_period=timedelta(
                days=_positive_int(raw.get("retention_days"), "bot.audit_anchor.store.retention_days")
            ),
            s3_verify_bucket_configuration=_bool(
                raw.get("verify_bucket_configuration", True),
                "bot.audit_anchor.store.verify_bucket_configuration",
            ),
        )

    raise ConfigError(f"unsupported audit anchor store provider: {provider.value}")


def _audit_archive_store_config(value: Any) -> AuditArchiveStoreConfig | None:
    if value is None:
        return None

    raw = _mapping(value, "bot.audit_archive.store")
    _reject_unknown(raw, set(_AUDIT_ARCHIVE_STORE_FIELDS), "bot.audit_archive.store")
    provider = _enum(raw.get("provider"), LedgerAnchorStoreProvider, "bot.audit_archive.store.provider")
    if provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        raise ConfigError("bot.audit_archive.store.provider must be aws_s3_object_lock")

    return AuditArchiveStoreConfig(
        provider=provider,
        s3_bucket=_string(raw.get("bucket"), "bot.audit_archive.store.bucket"),
        s3_expected_bucket_owner=_optional_string(
            raw.get("expected_bucket_owner"),
            "bot.audit_archive.store.expected_bucket_owner",
        ),
        s3_immutability_mode=_enum(
            raw.get("immutability_mode"),
            AnchorImmutabilityMode,
            "bot.audit_archive.store.immutability_mode",
        ),
        s3_key_prefix=_string(
            raw.get("key_prefix", "audit-ledger-archives"),
            "bot.audit_archive.store.key_prefix",
        ),
        s3_retention_period=timedelta(
            days=_positive_int(raw.get("retention_days"), "bot.audit_archive.store.retention_days")
        ),
        s3_verify_bucket_configuration=_bool(
            raw.get("verify_bucket_configuration", True),
            "bot.audit_archive.store.verify_bucket_configuration",
        ),
    )


def _feed_config(raw: Mapping[str, Any]) -> FeedRuntimeConfig:
    _reject_unknown(raw, set(_FEED_FIELDS), "bot.feed")
    defaults = FeedRuntimeConfig()
    reconnect_raw = _mapping(raw.get("reconnect", {}), "bot.feed.reconnect")
    _reject_unknown(reconnect_raw, set(_FEED_RECONNECT_FIELDS), "bot.feed.reconnect")
    return FeedRuntimeConfig(
        min_live_sources=_positive_int(
            raw.get("min_live_sources", defaults.min_live_sources),
            "bot.feed.min_live_sources",
        ),
        reconnect_policy=ReconnectPolicy(
            initial_delay_seconds=_non_negative_float(
                reconnect_raw.get(
                    "initial_delay_seconds",
                    defaults.reconnect_policy.initial_delay_seconds,
                ),
                "bot.feed.reconnect.initial_delay_seconds",
            ),
            max_delay_seconds=_non_negative_float(
                reconnect_raw.get(
                    "max_delay_seconds",
                    defaults.reconnect_policy.max_delay_seconds,
                ),
                "bot.feed.reconnect.max_delay_seconds",
            ),
            multiplier=_minimum_float(
                reconnect_raw.get("multiplier", defaults.reconnect_policy.multiplier),
                1,
                "bot.feed.reconnect.multiplier",
            ),
        ),
        stale_after=timedelta(
            seconds=_positive_float(
                raw.get("stale_after_seconds", defaults.stale_after.total_seconds()),
                "bot.feed.stale_after_seconds",
            )
        ),
    )


def _rest_config(raw: Mapping[str, Any]) -> CoinbaseRestApiConfig:
    _reject_unknown(
        raw,
        {"base_url", "execution_mode", "perpetual_portfolio_uuid", "retail_portfolio_id", "retry"},
        "bot.rest",
    )
    defaults = CoinbaseRestApiConfig()
    retry_raw = _mapping(raw.get("retry", {}), "bot.rest.retry")
    return CoinbaseRestApiConfig(
        base_url=_string(raw.get("base_url", defaults.base_url), "bot.rest.base_url"),
        execution_mode=_enum(raw.get("execution_mode", defaults.execution_mode), ExecutionMode, "bot.rest.execution_mode"),
        retail_portfolio_id=_optional_string(raw.get("retail_portfolio_id"), "bot.rest.retail_portfolio_id"),
        perpetual_portfolio_uuid=_optional_string(
            raw.get("perpetual_portfolio_uuid"),
            "bot.rest.perpetual_portfolio_uuid",
        ),
        retry_policy=_rest_retry_policy(retry_raw, defaults.retry_policy),
    )


def _rest_retry_policy(raw: Mapping[str, Any], defaults: CoinbaseRestRetryPolicy) -> CoinbaseRestRetryPolicy:
    _reject_unknown(
        raw,
        {"initial_delay_seconds", "max_attempts", "max_delay_seconds", "multiplier"},
        "bot.rest.retry",
    )
    return CoinbaseRestRetryPolicy(
        initial_delay_seconds=_non_negative_float(
            raw.get("initial_delay_seconds", defaults.initial_delay_seconds),
            "bot.rest.retry.initial_delay_seconds",
        ),
        max_attempts=_positive_int(raw.get("max_attempts", defaults.max_attempts), "bot.rest.retry.max_attempts"),
        max_delay_seconds=_non_negative_float(
            raw.get("max_delay_seconds", defaults.max_delay_seconds),
            "bot.rest.retry.max_delay_seconds",
        ),
        multiplier=_minimum_float(raw.get("multiplier", defaults.multiplier), 1, "bot.rest.retry.multiplier"),
    )


def _risk_config(raw: Mapping[str, Any]) -> RiskPolicyConfig:
    _reject_unknown(raw, set(_RISK_FIELDS), "bot.risk")
    return RiskPolicyConfig(
        allowed_order_types=tuple(
            _enum(order_type, OrderType, "bot.risk.allowed_order_types[]")
            for order_type in _sequence(raw.get("allowed_order_types", ()), "bot.risk.allowed_order_types")
        ),
        allowed_products=tuple(
            _string(product_id, "bot.risk.allowed_products[]")
            for product_id in _sequence(raw.get("allowed_products", ()), "bot.risk.allowed_products")
        ),
        kill_switch_enabled=_bool(raw.get("kill_switch_enabled", False), "bot.risk.kill_switch_enabled"),
        max_daily_notional=_optional_decimal(raw.get("max_daily_notional"), "bot.risk.max_daily_notional"),
        max_leverage=_optional_decimal(raw.get("max_leverage"), "bot.risk.max_leverage"),
        max_open_orders=_optional_int(raw.get("max_open_orders"), "bot.risk.max_open_orders"),
        max_order_notional=_optional_decimal(raw.get("max_order_notional"), "bot.risk.max_order_notional"),
        max_order_size=_optional_decimal(raw.get("max_order_size"), "bot.risk.max_order_size"),
        require_reduce_only=_bool(raw.get("require_reduce_only", False), "bot.risk.require_reduce_only"),
    )


def _product_catalog_config(raw: Mapping[str, Any]) -> ProductCatalogRuntimeConfig:
    _reject_unknown(raw, set(_PRODUCT_CATALOG_FIELDS), "bot.product_catalog")
    defaults = ProductCatalogRuntimeConfig()
    return ProductCatalogRuntimeConfig(
        schedule=_schedule(
            raw,
            allowed_fields=_PRODUCT_CATALOG_FIELDS,
            field_name="bot.product_catalog",
            task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
            default_interval_seconds=defaults.schedule.interval.total_seconds(),
            default_enabled=defaults.schedule.enabled,
            default_run_on_start=defaults.schedule.run_on_start,
        ),
        product_ids=tuple(
            _string(product_id, "bot.product_catalog.product_ids[]")
            for product_id in _sequence(raw.get("product_ids", ()), "bot.product_catalog.product_ids")
        ),
    )


def _strategies_config(raw: Mapping[str, Any]) -> StrategyRuntimeConfig:
    _reject_unknown(raw, set(_STRATEGIES_FIELDS), "bot.strategies")
    defaults = StrategyRuntimeConfig()
    return StrategyRuntimeConfig(
        allow_live_execution=_bool(
            raw.get("allow_live_execution", defaults.allow_live_execution),
            "bot.strategies.allow_live_execution",
        ),
        schedule=_schedule(
            raw,
            allowed_fields=_STRATEGIES_FIELDS,
            field_name="bot.strategies",
            task_id=RuntimeTask.STRATEGY_EVALUATION,
            default_interval_seconds=defaults.schedule.interval.total_seconds(),
            default_enabled=defaults.schedule.enabled,
            default_run_on_start=defaults.schedule.run_on_start,
        ),
        market_data_requirements=tuple(
            _strategy_input_requirement_config(
                _mapping(requirement, "bot.strategies.market_data_requirements[]")
            )
            for requirement in _sequence(
                raw.get("market_data_requirements", ()),
                "bot.strategies.market_data_requirements",
            )
        ),
        max_market_trades_per_product=(
            None
            if raw.get("max_market_trades_per_product") is None
            else _positive_int(
                raw.get("max_market_trades_per_product"),
                "bot.strategies.max_market_trades_per_product",
            )
        ),
        max_order_book_sample_depth_per_side=(
            None
            if raw.get("max_order_book_sample_depth_per_side") is None
            else _positive_int(
                raw.get("max_order_book_sample_depth_per_side"),
                "bot.strategies.max_order_book_sample_depth_per_side",
            )
        ),
        max_order_book_samples_per_product=(
            1
            if raw.get("max_order_book_samples_per_product") is None
            else _positive_int(
                raw.get("max_order_book_samples_per_product"),
                "bot.strategies.max_order_book_samples_per_product",
            )
        ),
        order_book_sample_product_ids=tuple(
            _string(product_id, "bot.strategies.order_book_sample_product_ids[]")
            for product_id in _sequence(
                raw.get("order_book_sample_product_ids", ()),
                "bot.strategies.order_book_sample_product_ids",
            )
        ),
        operator_policy=_operator_policy_config(raw),
        strategy_parameters=_strategy_parameters_config(
            raw.get("strategy_parameters", {}),
        ),
        strategy_ids=tuple(
            _string(strategy_id, "bot.strategies.strategy_ids[]")
            for strategy_id in _sequence(raw.get("strategy_ids", ()), "bot.strategies.strategy_ids")
        ),
    )


def _operator_policy_config(raw: Mapping[str, Any]) -> OperatorPolicy | None:
    operator_policy = raw.get("operator_policy")
    operator_policy_file = raw.get("operator_policy_file")
    if operator_policy is not None and operator_policy_file is not None:
        raise ValueError("bot.strategies cannot configure both operator_policy and operator_policy_file")
    if operator_policy is not None:
        return operator_policy_from_mapping(
            _mapping(operator_policy, "bot.strategies.operator_policy")
        )
    if operator_policy_file is not None:
        return load_operator_policy_from_json_file(
            Path(_string(operator_policy_file, "bot.strategies.operator_policy_file"))
        )
    return None


def _strategy_input_requirement_config(raw: Mapping[str, Any]) -> StrategyInputRequirement:
    _reject_unknown(
        raw,
        {"data_kind", "max_age_seconds", "product_id"},
        "bot.strategies.market_data_requirements[]",
    )
    return StrategyInputRequirement(
        data_kind=_enum(
            raw.get("data_kind"),
            MarketDataKind,
            "bot.strategies.market_data_requirements[].data_kind",
        ),
        max_age=timedelta(
            seconds=_positive_float(
                raw.get("max_age_seconds"),
                "bot.strategies.market_data_requirements[].max_age_seconds",
            )
        ),
        product_id=_string(
            raw.get("product_id"),
            "bot.strategies.market_data_requirements[].product_id",
        ),
    )


def _strategy_parameters_config(raw: Any) -> dict[str, dict[str, Any]]:
    parameters = _mapping(raw, "bot.strategies.strategy_parameters")
    parsed: dict[str, dict[str, Any]] = {}
    for strategy_id, strategy_parameters in parameters.items():
        parsed_strategy_id = _string(
            strategy_id,
            "bot.strategies.strategy_parameters[]",
        )
        normalized = normalize_json(
            _mapping(
                strategy_parameters,
                f"bot.strategies.strategy_parameters[{parsed_strategy_id}]",
            )
        )
        if not isinstance(normalized, dict):
            raise TypeError(
                f"bot.strategies.strategy_parameters[{parsed_strategy_id}] must normalize to an object"
            )
        parsed[parsed_strategy_id] = normalized
    return parsed


def _reconciliation_config(raw: Mapping[str, Any]) -> ReconciliationRuntimeConfig:
    _reject_unknown(raw, {"exchange_state", "fills", "order_recovery", "watchdog"}, "bot.reconciliation")

    watchdog_raw = _mapping(raw.get("watchdog", {}), "bot.reconciliation.watchdog")
    fills_raw = _mapping(raw.get("fills", {}), "bot.reconciliation.fills")
    exchange_state_raw = _mapping(raw.get("exchange_state", {}), "bot.reconciliation.exchange_state")
    order_recovery_raw = _mapping(raw.get("order_recovery", {}), "bot.reconciliation.order_recovery")
    defaults = ReconciliationRuntimeConfig()

    return ReconciliationRuntimeConfig(
        watchdog_policy=ReconciliationPolicy(
            user_confirmation_timeout=timedelta(
                seconds=_positive_float(
                    watchdog_raw.get(
                        "user_confirmation_timeout_seconds",
                        defaults.watchdog_policy.user_confirmation_timeout.total_seconds(),
                    ),
                    "bot.reconciliation.watchdog.user_confirmation_timeout_seconds",
                )
            ),
            execution_modes=_execution_modes(
                watchdog_raw.get("execution_modes"),
                defaults.watchdog_policy.execution_modes,
                "bot.reconciliation.watchdog.execution_modes",
            ),
        ),
        fill_policy=FillReconciliationPolicy(
            execution_modes=_execution_modes(
                fills_raw.get("execution_modes"),
                defaults.fill_policy.execution_modes,
                "bot.reconciliation.fills.execution_modes",
            ),
            limit=_positive_int(
                fills_raw.get("limit", defaults.fill_policy.limit),
                "bot.reconciliation.fills.limit",
            ),
            max_pages_per_order=_positive_int(
                fills_raw.get("max_pages_per_order", defaults.fill_policy.max_pages_per_order),
                "bot.reconciliation.fills.max_pages_per_order",
            ),
        ),
        exchange_state_policy=ExchangeStateReconciliationPolicy(
            account_page_limit=_positive_int(
                exchange_state_raw.get(
                    "account_page_limit",
                    defaults.exchange_state_policy.account_page_limit,
                ),
                "bot.reconciliation.exchange_state.account_page_limit",
            ),
            max_account_pages=_positive_int(
                exchange_state_raw.get("max_account_pages", defaults.exchange_state_policy.max_account_pages),
                "bot.reconciliation.exchange_state.max_account_pages",
            ),
            position_size_tolerance=_string(
                exchange_state_raw.get(
                    "position_size_tolerance",
                    defaults.exchange_state_policy.position_size_tolerance,
                ),
                "bot.reconciliation.exchange_state.position_size_tolerance",
            ),
            position_product_ids=tuple(
                _string(product_id, "bot.reconciliation.exchange_state.position_product_ids[]")
                for product_id in _sequence(
                    exchange_state_raw.get(
                        "position_product_ids",
                        defaults.exchange_state_policy.position_product_ids,
                    ),
                    "bot.reconciliation.exchange_state.position_product_ids",
                )
            ),
            retail_portfolio_id=_optional_string(
                exchange_state_raw.get("retail_portfolio_id", defaults.exchange_state_policy.retail_portfolio_id),
                "bot.reconciliation.exchange_state.retail_portfolio_id",
            ),
            perpetual_portfolio_uuid=_optional_string(
                exchange_state_raw.get(
                    "perpetual_portfolio_uuid",
                    defaults.exchange_state_policy.perpetual_portfolio_uuid,
                ),
                "bot.reconciliation.exchange_state.perpetual_portfolio_uuid",
            ),
        ),
        watchdog_schedule=_schedule(
            watchdog_raw,
            allowed_fields=_WATCHDOG_FIELDS,
            field_name="bot.reconciliation.watchdog",
            task_id=RuntimeTask.WATCHDOG,
            default_interval_seconds=defaults.watchdog_schedule.interval.total_seconds(),
            default_enabled=defaults.watchdog_schedule.enabled,
            default_run_on_start=defaults.watchdog_schedule.run_on_start,
        ),
        order_recovery_schedule=_schedule(
            order_recovery_raw,
            allowed_fields=_ORDER_RECOVERY_FIELDS,
            field_name="bot.reconciliation.order_recovery",
            task_id=RuntimeTask.ORDER_RECOVERY,
            default_interval_seconds=defaults.order_recovery_schedule.interval.total_seconds(),
            default_enabled=defaults.order_recovery_schedule.enabled,
            default_run_on_start=defaults.order_recovery_schedule.run_on_start,
        ),
        fill_schedule=_schedule(
            fills_raw,
            allowed_fields=_FILL_FIELDS,
            field_name="bot.reconciliation.fills",
            task_id=RuntimeTask.FILL_RECONCILIATION,
            default_interval_seconds=defaults.fill_schedule.interval.total_seconds(),
            default_enabled=defaults.fill_schedule.enabled,
            default_run_on_start=defaults.fill_schedule.run_on_start,
        ),
        exchange_state_schedule=_schedule(
            exchange_state_raw,
            allowed_fields=_EXCHANGE_STATE_FIELDS,
            field_name="bot.reconciliation.exchange_state",
            task_id=RuntimeTask.EXCHANGE_STATE_RECONCILIATION,
            default_interval_seconds=defaults.exchange_state_schedule.interval.total_seconds(),
            default_enabled=defaults.exchange_state_schedule.enabled,
            default_run_on_start=defaults.exchange_state_schedule.run_on_start,
        ),
    )


def _schedule(
    raw: Mapping[str, Any],
    *,
    allowed_fields: frozenset[str],
    default_enabled: bool,
    default_interval_seconds: float,
    default_run_on_start: bool,
    field_name: str,
    task_id: RuntimeTask,
) -> TaskScheduleConfig:
    _reject_unknown(raw, set(allowed_fields), field_name)
    return TaskScheduleConfig(
        task_id=task_id,
        interval=timedelta(
            seconds=_positive_float(
                raw.get("interval_seconds", default_interval_seconds),
                f"{field_name}.interval_seconds",
            )
        ),
        enabled=_bool(raw.get("enabled", default_enabled), f"{field_name}.enabled"),
        run_on_start=_bool(raw.get("run_on_start", default_run_on_start), f"{field_name}.run_on_start"),
    )


def _trigger_rule_config(raw: Mapping[str, Any]) -> TimeTriggerConfig | MessageTriggerConfig:
    _reject_unknown(
        raw,
        {
            "event_type",
            "relation",
            "repeatable",
            "target_time",
            "tolerance_seconds",
            "trigger_id",
            "type",
        },
        "bot.triggers[]",
    )
    rule_type = _enum(raw.get("type"), TriggerRuleType, "bot.triggers[].type")

    if rule_type == TriggerRuleType.TIME:
        _reject_unknown(
            raw,
            {"relation", "repeatable", "target_time", "tolerance_seconds", "trigger_id", "type"},
            "bot.triggers[]",
        )
        return TimeTriggerConfig(
            trigger_id=_string(raw.get("trigger_id"), "bot.triggers[].trigger_id"),
            relation=_enum(raw.get("relation"), TriggerRelation, "bot.triggers[].relation"),
            target_time=_datetime(raw.get("target_time"), "bot.triggers[].target_time"),
            tolerance=timedelta(
                seconds=_positive_float(raw.get("tolerance_seconds", 1), "bot.triggers[].tolerance_seconds")
            ),
            repeatable=_bool(raw.get("repeatable", False), "bot.triggers[].repeatable"),
        )

    if rule_type == TriggerRuleType.MESSAGE:
        _reject_unknown(
            raw,
            {"event_type", "relation", "repeatable", "trigger_id", "type"},
            "bot.triggers[]",
        )
        event_type = raw.get("event_type")
        return MessageTriggerConfig(
            trigger_id=_string(raw.get("trigger_id"), "bot.triggers[].trigger_id"),
            relation=_enum(raw.get("relation"), TriggerRelation, "bot.triggers[].relation"),
            event_type=(
                None
                if event_type is None
                else _enum(event_type, EventType, "bot.triggers[].event_type")
            ),
            repeatable=_bool(raw.get("repeatable", True), "bot.triggers[].repeatable"),
        )

    raise ConfigError(f"unsupported trigger type: {rule_type.value}")


def _websocket_source_config(raw: Mapping[str, Any]) -> CoinbaseWebSocketSourceConfig:
    _reject_unknown(
        raw,
        {"channels", "endpoint", "include_heartbeats", "product_ids", "source_id"},
        "bot.websocket_sources[]",
    )
    return CoinbaseWebSocketSourceConfig(
        source_id=_string(raw.get("source_id"), "bot.websocket_sources[].source_id"),
        channels=tuple(
            _enum(channel, CoinbaseWebSocketChannel, "bot.websocket_sources[].channels[]")
            for channel in _sequence(raw.get("channels"), "bot.websocket_sources[].channels")
        ),
        endpoint=_enum(raw.get("endpoint"), CoinbaseWebSocketEndpoint, "bot.websocket_sources[].endpoint"),
        product_ids=tuple(
            _string(product_id, "bot.websocket_sources[].product_ids[]")
            for product_id in _sequence(raw.get("product_ids", ()), "bot.websocket_sources[].product_ids")
        ),
        include_heartbeats=_bool(raw.get("include_heartbeats", True), "bot.websocket_sources[].include_heartbeats"),
    )


def _env_to_raw(environment: Mapping[str, str], *, prefix: str) -> dict[str, Any]:
    _reject_legacy_env(environment)
    _reject_unknown_env(environment, prefix=prefix)
    raw: dict[str, Any] = {"bot": {"reconciliation": {}, "rest": {}}}
    bot = raw["bot"]
    rest = bot["rest"]
    reconciliation = bot["reconciliation"]

    _set_if_present(raw, "ledger_path", environment.get(f"{prefix}LEDGER_PATH"))
    _set_if_present(rest, "base_url", environment.get(f"{prefix}REST_BASE_URL"))
    _set_if_present(rest, "execution_mode", environment.get(f"{prefix}EXECUTION_MODE"))
    _set_if_present(rest, "retail_portfolio_id", environment.get(f"{prefix}RETAIL_PORTFOLIO_ID"))
    _set_if_present(rest, "perpetual_portfolio_uuid", environment.get(f"{prefix}PERPETUAL_PORTFOLIO_UUID"))
    risk = bot.setdefault("risk", {})
    _set_csv_if_present(risk, "allowed_products", environment.get(f"{prefix}RISK_ALLOWED_PRODUCTS"))
    _set_csv_if_present(risk, "allowed_order_types", environment.get(f"{prefix}RISK_ALLOWED_ORDER_TYPES"))
    _set_if_present(risk, "max_order_size", environment.get(f"{prefix}RISK_MAX_ORDER_SIZE"))
    _set_if_present(risk, "max_order_notional", environment.get(f"{prefix}RISK_MAX_ORDER_NOTIONAL"))
    _set_if_present(risk, "max_daily_notional", environment.get(f"{prefix}RISK_MAX_DAILY_NOTIONAL"))
    _set_if_present(risk, "max_open_orders", environment.get(f"{prefix}RISK_MAX_OPEN_ORDERS"))
    _set_if_present(risk, "max_leverage", environment.get(f"{prefix}RISK_MAX_LEVERAGE"))
    _set_if_present(risk, "require_reduce_only", environment.get(f"{prefix}RISK_REQUIRE_REDUCE_ONLY"))
    _set_if_present(risk, "kill_switch_enabled", environment.get(f"{prefix}RISK_KILL_SWITCH_ENABLED"))
    product_catalog = bot.setdefault("product_catalog", {})
    _env_schedule(bot, "product_catalog", environment, prefix=prefix, env_name="PRODUCT_CATALOG")
    _set_csv_if_present(
        product_catalog,
        "product_ids",
        environment.get(f"{prefix}PRODUCT_CATALOG_PRODUCT_IDS"),
    )
    strategies = bot.setdefault("strategies", {})
    _env_schedule(bot, "strategies", environment, prefix=prefix, env_name="STRATEGIES")
    _set_if_present(
        strategies,
        "allow_live_execution",
        environment.get(f"{prefix}STRATEGIES_ALLOW_LIVE_EXECUTION"),
    )
    _set_csv_if_present(strategies, "strategy_ids", environment.get(f"{prefix}STRATEGY_IDS"))
    _set_if_present(
        strategies,
        "max_market_trades_per_product",
        environment.get(f"{prefix}STRATEGIES_MAX_MARKET_TRADES_PER_PRODUCT"),
    )
    _set_if_present(
        strategies,
        "max_order_book_sample_depth_per_side",
        environment.get(f"{prefix}STRATEGIES_MAX_ORDER_BOOK_SAMPLE_DEPTH_PER_SIDE"),
    )
    _set_if_present(
        strategies,
        "max_order_book_samples_per_product",
        environment.get(f"{prefix}STRATEGIES_MAX_ORDER_BOOK_SAMPLES_PER_PRODUCT"),
    )
    _set_csv_if_present(
        strategies,
        "order_book_sample_product_ids",
        environment.get(f"{prefix}STRATEGIES_ORDER_BOOK_SAMPLE_PRODUCT_IDS"),
    )
    _set_if_present(
        strategies,
        "operator_policy_file",
        environment.get(f"{prefix}STRATEGIES_OPERATOR_POLICY_FILE"),
    )
    strategy_requirements = environment.get(f"{prefix}STRATEGIES_MARKET_DATA_REQUIREMENTS")
    if strategy_requirements is not None:
        try:
            strategies["market_data_requirements"] = json.loads(strategy_requirements)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{prefix}STRATEGIES_MARKET_DATA_REQUIREMENTS must be valid JSON") from exc
    strategy_parameters = environment.get(f"{prefix}STRATEGIES_PARAMETERS")
    if strategy_parameters is not None:
        try:
            strategies["strategy_parameters"] = json.loads(strategy_parameters)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{prefix}STRATEGIES_PARAMETERS must be valid JSON") from exc
    feed = bot.setdefault("feed", {})
    _set_if_present(feed, "min_live_sources", environment.get(f"{prefix}FEED_MIN_LIVE_SOURCES"))
    _set_if_present(feed, "stale_after_seconds", environment.get(f"{prefix}FEED_STALE_AFTER_SECONDS"))
    feed_reconnect = feed.setdefault("reconnect", {})
    _set_if_present(
        feed_reconnect,
        "initial_delay_seconds",
        environment.get(f"{prefix}FEED_RECONNECT_INITIAL_DELAY_SECONDS"),
    )
    _set_if_present(
        feed_reconnect,
        "max_delay_seconds",
        environment.get(f"{prefix}FEED_RECONNECT_MAX_DELAY_SECONDS"),
    )
    _set_if_present(feed_reconnect, "multiplier", environment.get(f"{prefix}FEED_RECONNECT_MULTIPLIER"))
    _env_schedule(feed, "health", environment, prefix=prefix, env_name="FEED_HEALTH")
    retry = rest.setdefault("retry", {})
    _set_if_present(retry, "max_attempts", environment.get(f"{prefix}REST_RETRY_MAX_ATTEMPTS"))
    _set_if_present(
        retry,
        "initial_delay_seconds",
        environment.get(f"{prefix}REST_RETRY_INITIAL_DELAY_SECONDS"),
    )
    _set_if_present(retry, "max_delay_seconds", environment.get(f"{prefix}REST_RETRY_MAX_DELAY_SECONDS"))
    _set_if_present(retry, "multiplier", environment.get(f"{prefix}REST_RETRY_MULTIPLIER"))

    _env_schedule(reconciliation, "watchdog", environment, prefix=prefix, env_name="WATCHDOG")
    _set_if_present(
        reconciliation.setdefault("watchdog", {}),
        "user_confirmation_timeout_seconds",
        environment.get(f"{prefix}USER_CONFIRMATION_TIMEOUT_SECONDS"),
    )
    _set_csv_if_present(
        reconciliation.setdefault("watchdog", {}),
        "execution_modes",
        environment.get(f"{prefix}WATCHDOG_EXECUTION_MODES"),
    )

    _env_schedule(reconciliation, "order_recovery", environment, prefix=prefix, env_name="ORDER_RECOVERY")
    _env_schedule(reconciliation, "fills", environment, prefix=prefix, env_name="FILL_RECONCILIATION")
    _set_csv_if_present(
        reconciliation.setdefault("fills", {}),
        "execution_modes",
        environment.get(f"{prefix}FILL_RECONCILIATION_EXECUTION_MODES"),
    )
    _set_if_present(reconciliation.setdefault("fills", {}), "limit", environment.get(f"{prefix}FILL_RECONCILIATION_LIMIT"))
    _set_if_present(
        reconciliation.setdefault("fills", {}),
        "max_pages_per_order",
        environment.get(f"{prefix}FILL_RECONCILIATION_MAX_PAGES_PER_ORDER"),
    )

    _env_schedule(
        reconciliation,
        "exchange_state",
        environment,
        prefix=prefix,
        env_name="EXCHANGE_STATE_RECONCILIATION",
    )
    exchange_state = reconciliation.setdefault("exchange_state", {})
    _set_if_present(exchange_state, "account_page_limit", environment.get(f"{prefix}EXCHANGE_STATE_ACCOUNT_PAGE_LIMIT"))
    _set_if_present(exchange_state, "max_account_pages", environment.get(f"{prefix}EXCHANGE_STATE_MAX_ACCOUNT_PAGES"))
    _set_if_present(
        exchange_state,
        "position_size_tolerance",
        environment.get(f"{prefix}EXCHANGE_STATE_POSITION_SIZE_TOLERANCE"),
    )
    _set_csv_if_present(
        exchange_state,
        "position_product_ids",
        environment.get(f"{prefix}EXCHANGE_STATE_POSITION_PRODUCT_IDS"),
    )
    _set_if_present(
        exchange_state,
        "retail_portfolio_id",
        environment.get(f"{prefix}EXCHANGE_STATE_RETAIL_PORTFOLIO_ID"),
    )
    _set_if_present(
        exchange_state,
        "perpetual_portfolio_uuid",
        environment.get(f"{prefix}EXCHANGE_STATE_PERPETUAL_PORTFOLIO_UUID"),
    )

    websocket_sources = environment.get(f"{prefix}WEBSOCKET_SOURCES")
    if websocket_sources is not None:
        try:
            bot["websocket_sources"] = json.loads(websocket_sources)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{prefix}WEBSOCKET_SOURCES must be valid JSON") from exc

    triggers = environment.get(f"{prefix}TRIGGERS")
    if triggers is not None:
        try:
            bot["triggers"] = json.loads(triggers)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{prefix}TRIGGERS must be valid JSON") from exc

    _env_schedule(bot, "audit_anchor", environment, prefix=prefix, env_name="AUDIT_ANCHOR")
    _env_audit_anchor_store(bot, environment, prefix=prefix)
    _env_schedule(bot, "audit_archive", environment, prefix=prefix, env_name="AUDIT_ARCHIVE")
    _env_audit_archive_store(bot, environment, prefix=prefix)
    _env_schedule(bot, "trigger_polling", environment, prefix=prefix, env_name="TRIGGER_POLLING")

    return _prune_empty(raw)


def _env_audit_anchor_store(bot: dict[str, Any], environment: Mapping[str, str], *, prefix: str) -> None:
    store: dict[str, Any] = {}
    _set_if_present(store, "provider", environment.get(f"{prefix}AUDIT_ANCHOR_STORE_PROVIDER"))
    _set_if_present(store, "anchor_dir", environment.get(f"{prefix}AUDIT_ANCHOR_LOCAL_DIR"))
    _set_if_present(store, "bucket", environment.get(f"{prefix}AUDIT_ANCHOR_S3_BUCKET"))
    _set_if_present(
        store,
        "expected_bucket_owner",
        environment.get(f"{prefix}AUDIT_ANCHOR_S3_EXPECTED_BUCKET_OWNER"),
    )
    _set_if_present(
        store,
        "immutability_mode",
        environment.get(f"{prefix}AUDIT_ANCHOR_S3_IMMUTABILITY_MODE"),
    )
    _set_if_present(store, "key_prefix", environment.get(f"{prefix}AUDIT_ANCHOR_S3_KEY_PREFIX"))
    _set_if_present(store, "retention_days", environment.get(f"{prefix}AUDIT_ANCHOR_S3_RETENTION_DAYS"))
    _set_if_present(
        store,
        "verify_bucket_configuration",
        environment.get(f"{prefix}AUDIT_ANCHOR_S3_VERIFY_BUCKET_CONFIGURATION"),
    )
    if store:
        bot.setdefault("audit_anchor", {})["store"] = store


def _env_audit_archive_store(bot: dict[str, Any], environment: Mapping[str, str], *, prefix: str) -> None:
    store: dict[str, Any] = {}
    _set_if_present(store, "provider", environment.get(f"{prefix}AUDIT_ARCHIVE_STORE_PROVIDER"))
    _set_if_present(store, "bucket", environment.get(f"{prefix}AUDIT_ARCHIVE_S3_BUCKET"))
    _set_if_present(
        store,
        "expected_bucket_owner",
        environment.get(f"{prefix}AUDIT_ARCHIVE_S3_EXPECTED_BUCKET_OWNER"),
    )
    _set_if_present(
        store,
        "immutability_mode",
        environment.get(f"{prefix}AUDIT_ARCHIVE_S3_IMMUTABILITY_MODE"),
    )
    _set_if_present(store, "key_prefix", environment.get(f"{prefix}AUDIT_ARCHIVE_S3_KEY_PREFIX"))
    _set_if_present(store, "retention_days", environment.get(f"{prefix}AUDIT_ARCHIVE_S3_RETENTION_DAYS"))
    _set_if_present(
        store,
        "verify_bucket_configuration",
        environment.get(f"{prefix}AUDIT_ARCHIVE_S3_VERIFY_BUCKET_CONFIGURATION"),
    )
    if store:
        bot.setdefault("audit_archive", {})["store"] = store


def _env_schedule(
    reconciliation: dict[str, Any],
    key: str,
    environment: Mapping[str, str],
    *,
    env_name: str,
    prefix: str,
) -> None:
    schedule = reconciliation.setdefault(key, {})
    _set_if_present(schedule, "enabled", environment.get(f"{prefix}{env_name}_ENABLED"))
    _set_if_present(schedule, "interval_seconds", environment.get(f"{prefix}{env_name}_INTERVAL_SECONDS"))
    _set_if_present(schedule, "run_on_start", environment.get(f"{prefix}{env_name}_RUN_ON_START"))


def _execution_modes(value: Any, default: tuple[ExecutionMode, ...], field_name: str) -> tuple[ExecutionMode, ...]:
    if value is None:
        return default
    return tuple(_enum(item, ExecutionMode, f"{field_name}[]") for item in _sequence(value, field_name))


def _enum(value: Any, enum_type: type[Any], field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a {enum_type.__name__}")
    for member in enum_type:
        if value == member.value or value == member.name:
            return member
        if value.lower() == member.name.lower():
            return member
    raise ConfigError(f"{field_name} is not a valid {enum_type.__name__}: {value}")


def _bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise TypeError(f"{field_name} must be a bool")


def _positive_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} must be numeric") from exc
    if number <= 0:
        raise ConfigError(f"{field_name} must be positive")
    return number


def _non_negative_float(value: Any, field_name: str) -> float:
    number = _numeric_float(value, field_name)
    if number < 0:
        raise ConfigError(f"{field_name} must be non-negative")
    return number


def _minimum_float(value: Any, minimum: float, field_name: str) -> float:
    number = _numeric_float(value, field_name)
    if number < minimum:
        raise ConfigError(f"{field_name} must be at least {minimum:g}")
    return number


def _numeric_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be numeric")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} must be numeric") from exc


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str):
        try:
            number = int(value)
        except ValueError as exc:
            raise TypeError(f"{field_name} must be an integer") from exc
        if str(number) != value.strip():
            raise TypeError(f"{field_name} must be an integer")
    else:
        raise TypeError(f"{field_name} must be an integer")
    if number <= 0:
        raise ConfigError(f"{field_name} must be positive")
    return number


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise TypeError(f"{field_name} must be a mapping")


def _sequence(value: Any, field_name: str) -> Sequence[Any]:
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if isinstance(value, Sequence):
        return value
    raise TypeError(f"{field_name} must be a sequence")


def _string(value: Any, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise TypeError(f"{field_name} must be a non-empty string")


def _datetime(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ConfigError(f"{field_name} must be an ISO-8601 datetime") from exc
    raise TypeError(f"{field_name} must be an ISO-8601 datetime")


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _string(value, field_name)


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, field_name)


def _optional_decimal(value: Any, field_name: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be a decimal")
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TypeError(f"{field_name} must be a decimal") from exc
    if not decimal.is_finite():
        raise ConfigError(f"{field_name} must be finite")
    return decimal


def _reject_unknown(raw: Mapping[str, Any], allowed: set[str], field_name: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"{field_name} has unknown field(s): {', '.join(unknown)}")


def _reject_unknown_env(environment: Mapping[str, str], *, prefix: str) -> None:
    unknown = sorted(
        key
        for key in environment
        if key.startswith(prefix) and key[len(prefix):] not in _KNOWN_ENV_SUFFIXES
    )
    if unknown:
        raise ConfigError(f"unknown StateRail env var(s): {', '.join(unknown)}")


def _reject_legacy_env(environment: Mapping[str, str]) -> None:
    legacy = sorted(key for key in environment if key.startswith(LEGACY_ENV_PREFIX))
    if legacy:
        raise ConfigError(
            f"{LEGACY_ENV_PREFIX} environment variables were renamed to {ENV_PREFIX}: "
            f"{', '.join(legacy)}"
        )


def _set_if_present(target: dict[str, Any], key: str, value: str | None) -> None:
    if value is not None:
        target[key] = value


def _set_csv_if_present(target: dict[str, Any], key: str, value: str | None) -> None:
    if value is not None:
        target[key] = tuple(item.strip() for item in value.split(",") if item.strip())


def _prune_empty(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: pruned for key, item in value.items() if (pruned := _prune_empty(item)) != {}}
    return value
