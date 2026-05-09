from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from config.assembly import (
    AuditAnchorStoreConfig,
    AuditArchiveStoreConfig,
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
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
from core.enums import LedgerAnchorStoreProvider
from core.json_tools import JsonValue, canonical_json

if TYPE_CHECKING:
    from app.bootstrap import CoinbaseApplicationConfig


APPLICATION_CONFIG_SCHEMA_VERSION = 11
CONFIG_FINGERPRINT_ALGORITHM = "sha256"


def application_config_startup_metadata(config: CoinbaseApplicationConfig) -> dict[str, JsonValue]:
    snapshot = application_config_snapshot(config)
    return {
        "application_config": {
            "fingerprint": application_config_fingerprint(config),
            "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
            "schema_version": APPLICATION_CONFIG_SCHEMA_VERSION,
            "snapshot": snapshot,
        }
    }


def application_config_fingerprint(config: CoinbaseApplicationConfig) -> str:
    return _sha256(canonical_json(application_config_snapshot(config)))


def application_config_snapshot(config: CoinbaseApplicationConfig) -> dict[str, JsonValue]:
    return {
        "bot": _bot_snapshot(config.bot),
        "ledger_path": config.ledger_path.as_posix(),
        "schema_version": APPLICATION_CONFIG_SCHEMA_VERSION,
    }


def _bot_snapshot(config: CoinbaseBotConfig) -> dict[str, JsonValue]:
    return {
        "audit_anchor": {
            "schedule": _schedule_snapshot(config.audit_anchor_schedule),
            "store": _audit_anchor_store_snapshot(config.audit_anchor_store),
        },
        "audit_archive": {
            "schedule": _schedule_snapshot(config.audit_archive_schedule),
            "store": _audit_archive_store_snapshot(config.audit_archive_store),
        },
        "feed": _feed_snapshot(config.feed, health_schedule=config.feed_health_schedule),
        "product_catalog": _product_catalog_snapshot(config.product_catalog),
        "reconciliation": _reconciliation_snapshot(config.reconciliation),
        "rest": _rest_snapshot(config.rest),
        "risk": _risk_snapshot(config.risk),
        "strategies": _strategies_snapshot(config.strategies),
        "trigger_polling": _schedule_snapshot(config.trigger_polling_schedule),
        "triggers": [_trigger_rule_snapshot(rule) for rule in config.trigger_rules],
        "websocket_sources": [_websocket_source_snapshot(source) for source in config.websocket_sources],
    }


def _audit_anchor_store_snapshot(config: AuditAnchorStoreConfig | None) -> dict[str, JsonValue] | None:
    if config is None:
        return None

    if config.provider == LedgerAnchorStoreProvider.LOCAL_FILE:
        return {
            "anchor_dir": config.local_anchor_dir.as_posix() if config.local_anchor_dir is not None else None,
            "provider": config.provider.value,
        }

    if config.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        return {
            "bucket": config.s3_bucket,
            "expected_bucket_owner": config.s3_expected_bucket_owner,
            "immutability_mode": (
                config.s3_immutability_mode.value if config.s3_immutability_mode is not None else None
            ),
            "key_prefix": config.s3_key_prefix,
            "provider": config.provider.value,
            "retention_seconds": (
                config.s3_retention_period.total_seconds() if config.s3_retention_period is not None else None
            ),
            "verify_bucket_configuration": config.s3_verify_bucket_configuration,
        }

    raise ValueError(f"unsupported audit anchor store provider: {config.provider.value}")


def _audit_archive_store_snapshot(config: AuditArchiveStoreConfig | None) -> dict[str, JsonValue] | None:
    if config is None:
        return None

    if config.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        return {
            "bucket": config.s3_bucket,
            "expected_bucket_owner": config.s3_expected_bucket_owner,
            "immutability_mode": (
                config.s3_immutability_mode.value if config.s3_immutability_mode is not None else None
            ),
            "key_prefix": config.s3_key_prefix,
            "provider": config.provider.value,
            "retention_seconds": (
                config.s3_retention_period.total_seconds() if config.s3_retention_period is not None else None
            ),
            "verify_bucket_configuration": config.s3_verify_bucket_configuration,
        }

    raise ValueError(f"unsupported audit archive store provider: {config.provider.value}")


def _feed_snapshot(
    config: FeedRuntimeConfig,
    *,
    health_schedule: TaskScheduleConfig,
) -> dict[str, JsonValue]:
    return {
        "health_schedule": _schedule_snapshot(health_schedule),
        "min_live_sources": config.min_live_sources,
        "reconnect_policy": {
            "initial_delay_seconds": config.reconnect_policy.initial_delay_seconds,
            "max_delay_seconds": config.reconnect_policy.max_delay_seconds,
            "multiplier": config.reconnect_policy.multiplier,
        },
        "stale_after_seconds": config.stale_after.total_seconds(),
    }


def _rest_snapshot(config: CoinbaseRestApiConfig) -> dict[str, JsonValue]:
    return {
        "base_url": config.base_url,
        "execution_mode": config.execution_mode.value,
        "perpetual_portfolio_uuid": config.perpetual_portfolio_uuid,
        "retry_policy": {
            "initial_delay_seconds": config.retry_policy.initial_delay_seconds,
            "max_attempts": config.retry_policy.max_attempts,
            "max_delay_seconds": config.retry_policy.max_delay_seconds,
            "multiplier": config.retry_policy.multiplier,
        },
        "retail_portfolio_id": config.retail_portfolio_id,
    }


def _risk_snapshot(config: RiskPolicyConfig) -> dict[str, JsonValue]:
    return {
        "allowed_lineage_relations": [relation.value for relation in config.allowed_lineage_relations],
        "allowed_order_types": [order_type.value for order_type in config.allowed_order_types],
        "allowed_placement_kinds": [kind.value for kind in config.allowed_placement_kinds],
        "allowed_products": list(config.allowed_products),
        "allowed_sides": [side.value for side in config.allowed_sides],
        "allowed_time_in_force": [
            time_in_force.value for time_in_force in config.allowed_time_in_force
        ],
        "kill_switch_enabled": config.kill_switch_enabled,
        "max_daily_notional": (
            str(config.max_daily_notional) if config.max_daily_notional is not None else None
        ),
        "max_leverage": str(config.max_leverage) if config.max_leverage is not None else None,
        "max_open_orders": config.max_open_orders,
        "max_order_notional": (
            str(config.max_order_notional) if config.max_order_notional is not None else None
        ),
        "max_order_replacements": config.max_order_replacements,
        "max_order_size": str(config.max_order_size) if config.max_order_size is not None else None,
        "max_visible_notional": (
            str(config.max_visible_notional) if config.max_visible_notional is not None else None
        ),
        "require_post_only": config.require_post_only,
        "require_reduce_only": config.require_reduce_only,
        "require_staged_release_above_visible_limit": (
            config.require_staged_release_above_visible_limit
        ),
    }


def _product_catalog_snapshot(config: ProductCatalogRuntimeConfig) -> dict[str, JsonValue]:
    return {
        "product_ids": list(config.product_ids),
        "schedule": _schedule_snapshot(config.schedule),
    }


def _strategies_snapshot(config: StrategyRuntimeConfig) -> dict[str, JsonValue]:
    return {
        "allow_live_execution": config.allow_live_execution,
        "market_data_requirements": [
            {
                "data_kind": requirement.data_kind.value,
                "max_age_seconds": requirement.max_age.total_seconds(),
                "product_id": requirement.product_id,
            }
            for requirement in config.market_data_requirements
        ],
        "operator_policy": (
            config.operator_policy.to_payload() if config.operator_policy is not None else None
        ),
        "schedule": _schedule_snapshot(config.schedule),
        "strategy_parameters": {
            strategy_id: config.strategy_parameters[strategy_id]
            for strategy_id in sorted(config.strategy_parameters)
        },
        "strategy_ids": list(config.strategy_ids),
    }


def _reconciliation_snapshot(config: ReconciliationRuntimeConfig) -> dict[str, JsonValue]:
    return {
        "exchange_state": {
            "policy": {
                "account_page_limit": config.exchange_state_policy.account_page_limit,
                "max_account_pages": config.exchange_state_policy.max_account_pages,
                "perpetual_portfolio_uuid": config.exchange_state_policy.perpetual_portfolio_uuid,
                "position_product_ids": list(config.exchange_state_policy.position_product_ids),
                "position_size_tolerance": config.exchange_state_policy.position_size_tolerance,
                "retail_portfolio_id": config.exchange_state_policy.retail_portfolio_id,
            },
            "schedule": _schedule_snapshot(config.exchange_state_schedule),
        },
        "fills": {
            "policy": {
                "execution_modes": [mode.value for mode in config.fill_policy.execution_modes],
                "limit": config.fill_policy.limit,
                "max_pages_per_order": config.fill_policy.max_pages_per_order,
            },
            "schedule": _schedule_snapshot(config.fill_schedule),
        },
        "order_recovery": {
            "schedule": _schedule_snapshot(config.order_recovery_schedule),
        },
        "watchdog": {
            "policy": {
                "execution_modes": [mode.value for mode in config.watchdog_policy.execution_modes],
                "user_confirmation_timeout_seconds": config.watchdog_policy.user_confirmation_timeout.total_seconds(),
            },
            "schedule": _schedule_snapshot(config.watchdog_schedule),
        },
    }


def _schedule_snapshot(config: TaskScheduleConfig) -> dict[str, JsonValue]:
    return {
        "enabled": config.enabled,
        "interval_seconds": config.interval.total_seconds(),
        "run_on_start": config.run_on_start,
        "task_id": config.task_id.value,
    }


def _trigger_rule_snapshot(rule: TimeTriggerConfig | MessageTriggerConfig) -> dict[str, JsonValue]:
    if isinstance(rule, TimeTriggerConfig):
        return {
            "relation": rule.relation.value,
            "repeatable": rule.repeatable,
            "target_time": rule.target_time,
            "tolerance_seconds": rule.tolerance.total_seconds(),
            "trigger_id": rule.trigger_id,
            "type": rule.rule_type.value,
        }
    if isinstance(rule, MessageTriggerConfig):
        return {
            "event_type": rule.event_type.value if rule.event_type is not None else None,
            "relation": rule.relation.value,
            "repeatable": rule.repeatable,
            "trigger_id": rule.trigger_id,
            "type": rule.rule_type.value,
        }
    raise TypeError("unsupported trigger rule config")


def _websocket_source_snapshot(config: CoinbaseWebSocketSourceConfig) -> dict[str, JsonValue]:
    return {
        "channels": [channel.value for channel in config.channels],
        "endpoint": config.endpoint.value,
        "include_heartbeats": config.include_heartbeats,
        "product_ids": list(config.product_ids),
        "source_id": config.source_id,
    }


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
