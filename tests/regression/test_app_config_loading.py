from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.bootstrap import CoinbaseApplicationConfig
from app.config_loading import (
    has_coinbase_application_env,
    load_coinbase_application_config_from_env,
    load_coinbase_application_config_from_json_file,
    load_coinbase_application_config_from_mapping,
)
from core.enums import (
    AnchorImmutabilityMode,
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    EventType,
    ExecutionMode,
    LedgerAnchorStoreProvider,
    MarginType,
    MarketDataKind,
    OrderType,
    RuntimeTask,
    TriggerRelation,
    TriggerRuleType,
)
from core.errors import ConfigError
from strategies import (
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
)


def test_load_application_config_from_mapping_converts_raw_values_to_typed_config(workspace_tmp_path):
    config = load_coinbase_application_config_from_mapping(
        {
            "ledger_path": str(workspace_tmp_path / "audit.jsonl"),
            "bot": {
                "rest": {
                    "execution_mode": "LIVE",
                    "retail_portfolio_id": "portfolio-1",
                    "perpetual_portfolio_uuid": "perp-portfolio-1",
                    "retry": {
                        "initial_delay_seconds": 0.1,
                        "max_attempts": 3,
                        "max_delay_seconds": 1,
                        "multiplier": 2,
                    },
                },
                "feed": {
                    "health": {
                        "enabled": True,
                        "interval_seconds": 3,
                        "run_on_start": False,
                    },
                    "min_live_sources": 2,
                    "reconnect": {
                        "initial_delay_seconds": 0.25,
                        "max_delay_seconds": 5,
                        "multiplier": 1.5,
                    },
                    "stale_after_seconds": 15,
                },
                "risk": {
                    "allowed_order_types": ["limit", "market"],
                    "allowed_products": ["BTC-USD", "ETH-USD"],
                    "kill_switch_enabled": False,
                    "max_leverage": "3",
                    "max_order_notional": "1000.50",
                    "max_order_size": "2.5",
                    "require_reduce_only": True,
                },
                "product_catalog": {
                    "enabled": True,
                    "interval_seconds": 3600,
                    "product_ids": ["BTC-USD", "BTC-PERP-INTX"],
                    "run_on_start": True,
                },
                "strategies": {
                    "allow_live_execution": True,
                    "enabled": True,
                    "interval_seconds": 5,
                    "market_data_requirements": [
                        {
                            "data_kind": "ticker",
                            "max_age_seconds": 3,
                            "product_id": "BTC-USD",
                        }
                    ],
                    "max_market_trades_per_product": 500,
                    "max_order_book_sample_depth_per_side": 10,
                    "max_order_book_samples_per_product": 25,
                    "operator_policy_file": "docs/examples/operator-policy.conservative-cfm-v0.json",
                    "run_on_start": False,
                    "strategy_ids": ["noop"],
                },
                "audit_anchor": {
                    "enabled": True,
                    "interval_seconds": 86400,
                    "run_on_start": False,
                    "store": {
                        "bucket": "audit-bucket",
                        "expected_bucket_owner": "123456789012",
                        "immutability_mode": "compliance",
                        "key_prefix": "staterail/anchors",
                        "provider": "aws_s3_object_lock",
                        "retention_days": 2555,
                        "verify_bucket_configuration": True,
                    },
                },
                "audit_archive": {
                    "enabled": True,
                    "interval_seconds": 43200,
                    "run_on_start": False,
                    "store": {
                        "bucket": "archive-bucket",
                        "expected_bucket_owner": "123456789012",
                        "immutability_mode": "governance",
                        "key_prefix": "staterail/ledger-archives",
                        "provider": "aws_s3_object_lock",
                        "retention_days": 3650,
                        "verify_bucket_configuration": True,
                    },
                },
                "reconciliation": {
                    "watchdog": {
                        "enabled": True,
                        "execution_modes": ["live"],
                        "interval_seconds": 7,
                        "run_on_start": False,
                        "user_confirmation_timeout_seconds": 45,
                    },
                    "fills": {
                        "enabled": True,
                        "execution_modes": ["LIVE"],
                        "interval_seconds": 11,
                        "limit": 50,
                        "max_pages_per_order": 3,
                    },
                    "exchange_state": {
                        "account_page_limit": 25,
                        "enabled": True,
                        "interval_seconds": 13,
                        "max_account_pages": 2,
                        "position_product_ids": ["BTC-USD"],
                        "position_size_tolerance": "0.001",
                    },
                },
                "trigger_polling": {
                    "enabled": True,
                    "interval_seconds": 2,
                    "run_on_start": False,
                },
                "triggers": [
                    {
                        "relation": "after",
                        "repeatable": False,
                        "target_time": "2026-01-01T12:00:00+00:00",
                        "tolerance_seconds": 2,
                        "trigger_id": "after-noon",
                        "type": "time",
                    },
                    {
                        "event_type": "data.accepted",
                        "relation": "on",
                        "repeatable": True,
                        "trigger_id": "on-data",
                        "type": "message",
                    },
                ],
                "websocket_sources": [
                    {
                        "channels": ["level2"],
                        "endpoint": "MARKET_DATA",
                        "product_ids": ["BTC-USD"],
                        "source_id": "coinbase-market-primary",
                    }
                ],
            },
        }
    )

    assert config.ledger_path == workspace_tmp_path / "audit.jsonl"
    assert config.bot.rest.execution_mode == ExecutionMode.LIVE
    assert config.bot.rest.retry_policy.max_attempts == 3
    assert config.bot.audit_anchor_schedule.enabled is True
    assert config.bot.audit_anchor_schedule.interval == timedelta(days=1)
    assert config.bot.audit_anchor_schedule.run_on_start is False
    assert config.bot.audit_anchor_schedule.task_id == RuntimeTask.AUDIT_ANCHOR
    assert config.bot.audit_anchor_store is not None
    assert config.bot.audit_anchor_store.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK
    assert config.bot.audit_anchor_store.s3_bucket == "audit-bucket"
    assert config.bot.audit_anchor_store.s3_expected_bucket_owner == "123456789012"
    assert config.bot.audit_anchor_store.s3_immutability_mode == AnchorImmutabilityMode.COMPLIANCE
    assert config.bot.audit_anchor_store.s3_key_prefix == "staterail/anchors"
    assert config.bot.audit_anchor_store.s3_retention_period == timedelta(days=2555)
    assert config.bot.audit_anchor_store.s3_verify_bucket_configuration is True
    assert config.bot.audit_archive_schedule.enabled is True
    assert config.bot.audit_archive_schedule.interval == timedelta(hours=12)
    assert config.bot.audit_archive_schedule.run_on_start is False
    assert config.bot.audit_archive_schedule.task_id == RuntimeTask.AUDIT_ARCHIVE
    assert config.bot.audit_archive_store is not None
    assert config.bot.audit_archive_store.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK
    assert config.bot.audit_archive_store.s3_bucket == "archive-bucket"
    assert config.bot.audit_archive_store.s3_expected_bucket_owner == "123456789012"
    assert config.bot.audit_archive_store.s3_immutability_mode == AnchorImmutabilityMode.GOVERNANCE
    assert config.bot.audit_archive_store.s3_key_prefix == "staterail/ledger-archives"
    assert config.bot.audit_archive_store.s3_retention_period == timedelta(days=3650)
    assert config.bot.audit_archive_store.s3_verify_bucket_configuration is True
    assert config.bot.feed.min_live_sources == 2
    assert config.bot.feed.reconnect_policy.initial_delay_seconds == 0.25
    assert config.bot.feed.reconnect_policy.max_delay_seconds == 5
    assert config.bot.feed.reconnect_policy.multiplier == 1.5
    assert config.bot.feed.stale_after == timedelta(seconds=15)
    assert config.bot.feed_health_schedule.enabled is True
    assert config.bot.feed_health_schedule.interval == timedelta(seconds=3)
    assert config.bot.feed_health_schedule.run_on_start is False
    assert config.bot.feed_health_schedule.task_id == RuntimeTask.FEED_HEALTH
    assert config.bot.risk.allowed_order_types == (OrderType.LIMIT, OrderType.MARKET)
    assert config.bot.risk.allowed_products == ("BTC-USD", "ETH-USD")
    assert config.bot.risk.kill_switch_enabled is False
    assert config.bot.risk.max_leverage == Decimal("3")
    assert config.bot.risk.max_order_notional == Decimal("1000.50")
    assert config.bot.risk.max_order_size == Decimal("2.5")
    assert config.bot.risk.require_reduce_only is True
    assert config.bot.product_catalog.schedule.enabled is True
    assert config.bot.product_catalog.schedule.interval == timedelta(hours=1)
    assert config.bot.product_catalog.schedule.run_on_start is True
    assert config.bot.product_catalog.schedule.task_id == RuntimeTask.PRODUCT_CATALOG_REFRESH
    assert config.bot.product_catalog.product_ids == ("BTC-USD", "BTC-PERP-INTX")
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.schedule.interval == timedelta(seconds=5)
    assert config.bot.strategies.schedule.run_on_start is False
    assert config.bot.strategies.schedule.task_id == RuntimeTask.STRATEGY_EVALUATION
    assert config.bot.strategies.strategy_ids == ("noop",)
    assert len(config.bot.strategies.market_data_requirements) == 1
    assert config.bot.strategies.market_data_requirements[0].data_kind == MarketDataKind.TICKER
    assert config.bot.strategies.market_data_requirements[0].max_age == timedelta(seconds=3)
    assert config.bot.strategies.market_data_requirements[0].product_id == "BTC-USD"
    assert config.bot.strategies.max_market_trades_per_product == 500
    assert config.bot.strategies.max_order_book_sample_depth_per_side == 10
    assert config.bot.strategies.max_order_book_samples_per_product == 25
    assert config.bot.strategies.operator_policy is not None
    assert config.bot.strategies.operator_policy.policy_name == "conservative_cfm_policy_v0"
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.rest.retry_policy.initial_delay_seconds == 0.1
    assert config.bot.reconciliation.watchdog_schedule.interval == timedelta(seconds=7)
    assert config.bot.reconciliation.watchdog_schedule.run_on_start is False
    assert config.bot.reconciliation.watchdog_policy.user_confirmation_timeout == timedelta(seconds=45)
    assert config.bot.reconciliation.fill_policy.limit == 50
    assert config.bot.reconciliation.exchange_state_policy.account_page_limit == 25
    assert config.bot.reconciliation.exchange_state_policy.position_product_ids == ("BTC-USD",)
    assert config.bot.trigger_polling_schedule.enabled is True
    assert config.bot.trigger_polling_schedule.interval == timedelta(seconds=2)
    assert config.bot.trigger_polling_schedule.run_on_start is False
    assert config.bot.trigger_rules[0].rule_type == TriggerRuleType.TIME
    assert config.bot.trigger_rules[0].trigger_id == "after-noon"
    assert config.bot.trigger_rules[0].relation == TriggerRelation.AFTER
    assert config.bot.trigger_rules[0].tolerance == timedelta(seconds=2)
    assert config.bot.trigger_rules[1].rule_type == TriggerRuleType.MESSAGE
    assert config.bot.trigger_rules[1].event_type == EventType.DATA_ACCEPTED
    assert config.bot.websocket_sources[0].channels == (CoinbaseWebSocketChannel.LEVEL2,)
    assert config.bot.websocket_sources[0].endpoint == CoinbaseWebSocketEndpoint.MARKET_DATA


def test_load_application_config_from_json_file(workspace_tmp_path):
    config_path = workspace_tmp_path / "bot-config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": str(workspace_tmp_path / "audit.jsonl"),
                "bot": {
                    "rest": {
                        "execution_mode": "dry_run",
                    },
                    "reconciliation": {
                        "watchdog": {
                            "interval_seconds": 9,
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    config = CoinbaseApplicationConfig.from_json_file(config_path)

    assert config.ledger_path == workspace_tmp_path / "audit.jsonl"
    assert config.bot.rest.execution_mode == ExecutionMode.DRY_RUN
    assert config.bot.reconciliation.watchdog_schedule.interval == timedelta(seconds=9)


@pytest.mark.parametrize(
    ("config_path", "execution_mode"),
    (
        ("docs/examples/config.dry-run.json", ExecutionMode.DRY_RUN),
        ("docs/examples/config.staged-release-manager.dry-run.json", ExecutionMode.DRY_RUN),
        ("docs/examples/config.anchor-repricing-manager.dry-run.json", ExecutionMode.DRY_RUN),
        ("docs/examples/config.passive-market-making.dry-run.json", ExecutionMode.DRY_RUN),
        ("docs/examples/config.cfm-live.json", ExecutionMode.LIVE),
        ("docs/examples/config.cfm-policy-probe.json", ExecutionMode.LIVE),
        ("docs/examples/config.cfm-passive-market-making.json", ExecutionMode.LIVE),
        ("docs/examples/config.cfm-passive-market-making-release.json", ExecutionMode.LIVE),
    ),
)
def test_checked_in_example_configs_parse(config_path, execution_mode):
    config = load_coinbase_application_config_from_json_file(Path(config_path))

    assert config.bot.rest.execution_mode == execution_mode


def test_env_example_contains_valid_safe_dry_run_defaults():
    env = _env_example_values(Path(".env.example"))

    config = load_coinbase_application_config_from_env(env)

    assert config.bot.rest.execution_mode == ExecutionMode.DRY_RUN
    assert config.bot.risk.allowed_products == ("BTC-USD",)
    assert config.bot.risk.allowed_order_types == (OrderType.LIMIT,)
    assert config.bot.product_catalog.schedule.enabled is False
    assert "COINBASE_API_SECRET" not in env
    assert "STATERAIL_COINBASE_API_PRIVATE_KEY" not in env
    assert env.get("STATERAIL_ALLOW_LIVE_TRADING") != "true"


def test_cfm_live_example_enables_required_live_safety_schedules():
    config = load_coinbase_application_config_from_json_file(Path("docs/examples/config.cfm-live.json"))

    assert config.bot.rest.perpetual_portfolio_uuid is None
    assert config.bot.product_catalog.schedule.enabled is True
    assert config.bot.product_catalog.product_ids == ("REPLACE_WITH_CFM_PRODUCT_IDS",)
    assert config.bot.feed_health_schedule.enabled is True
    assert len(config.bot.websocket_sources) == 4
    assert sum(1 for source in config.bot.websocket_sources if source.endpoint == CoinbaseWebSocketEndpoint.MARKET_DATA) == 2
    assert sum(
        1 for source in config.bot.websocket_sources if source.endpoint == CoinbaseWebSocketEndpoint.USER_ORDER_DATA
    ) == 2
    assert config.bot.reconciliation.order_recovery_schedule.enabled is True
    assert config.bot.reconciliation.fill_schedule.enabled is True
    assert config.bot.reconciliation.exchange_state_schedule.enabled is True
    assert config.bot.reconciliation.exchange_state_policy.position_product_ids == ("REPLACE_WITH_CFM_PRODUCT_IDS",)
    assert config.bot.reconciliation.exchange_state_policy.perpetual_portfolio_uuid is None
    assert config.bot.audit_anchor_schedule.enabled is False
    assert config.bot.audit_anchor_store is None
    assert config.bot.audit_archive_schedule.enabled is False
    assert config.bot.audit_archive_store is None


def test_passive_market_making_dry_run_example_selects_staged_hidden_strategy():
    config = load_coinbase_application_config_from_json_file(
        Path("docs/examples/config.passive-market-making.dry-run.json")
    )
    policy = config.bot.strategies.operator_policy
    assert policy is not None

    assert config.bot.rest.execution_mode == ExecutionMode.DRY_RUN
    assert config.bot.strategies.schedule.enabled is False
    assert config.bot.strategies.allow_live_execution is False
    assert config.bot.strategies.strategy_ids == (PASSIVE_MARKET_MAKING_STRATEGY_ID,)
    assert config.bot.strategies.strategy_parameters == {
        PASSIVE_MARKET_MAKING_STRATEGY_ID: {
            "half_spread_bps": "50",
            "max_products_per_evaluation": 1,
            "max_staged_release_count_per_side": 1,
            "target_notional_usd": "5",
        }
    }
    assert config.bot.product_catalog.product_ids == ("SHB-26JUN26-CDE",)
    assert config.bot.risk.max_order_size == Decimal("1000")
    assert policy.policy_name == "passive_market_making_dry_run_policy_v0"
    assert policy.scope.live_orders_allowed is False
    assert policy.order_behavior.default_order_type == OrderType.LIMIT
    assert policy.order_behavior.default_leverage == Decimal("1")
    assert policy.order_behavior.default_margin_type == MarginType.CROSS
    assert policy.order_behavior.post_only is True
    assert policy.staged_or_hidden_release.allow_release is False
    assert policy.staged_or_hidden_release.enabled is True
    assert policy.risk_limits.reduce_only_first is False


def test_anchor_repricing_manager_dry_run_example_selects_safe_cancel_replace_strategy():
    config = load_coinbase_application_config_from_json_file(
        Path("docs/examples/config.anchor-repricing-manager.dry-run.json")
    )
    policy = config.bot.strategies.operator_policy
    assert policy is not None
    assert policy.anchor_repricing is not None

    assert config.bot.rest.execution_mode == ExecutionMode.DRY_RUN
    assert config.bot.strategies.schedule.enabled is False
    assert config.bot.strategies.allow_live_execution is False
    assert config.bot.strategies.strategy_ids == (ANCHOR_REPRICING_MANAGER_STRATEGY_ID,)
    assert config.bot.product_catalog.product_ids == ("SHB-26JUN26-CDE",)
    assert policy.policy_name == "anchor_repricing_manager_dry_run_policy_v0"
    assert policy.scope.live_orders_allowed is False
    assert policy.anchor_repricing.enabled is True
    assert policy.moves.cancel_replace_when_amend_not_supported is True
    assert policy.risk_limits.kill_switch_enabled is False


def test_cfm_passive_market_making_template_enables_staged_hidden_strategy():
    config = load_coinbase_application_config_from_json_file(
        Path("docs/examples/config.cfm-passive-market-making.json")
    )
    policy = config.bot.strategies.operator_policy
    assert policy is not None

    assert config.bot.rest.execution_mode == ExecutionMode.LIVE
    assert config.bot.product_catalog.schedule.enabled is True
    assert config.bot.product_catalog.product_ids == ("REPLACE_WITH_CFM_PRODUCT_IDS",)
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.strategies.strategy_ids == (PASSIVE_MARKET_MAKING_STRATEGY_ID,)
    assert config.bot.strategies.strategy_parameters == {
        PASSIVE_MARKET_MAKING_STRATEGY_ID: {
            "half_spread_bps": "50",
            "max_products_per_evaluation": 2,
            "max_staged_release_count_per_side": 1,
            "target_notional_usd": "5",
        }
    }
    assert policy.policy_name == "passive_market_making_cfm_policy_v0"
    assert policy.scope.live_orders_allowed is True
    assert policy.scope.products == ("REPLACE_WITH_CFM_PRODUCT_IDS",)
    assert policy.risk_limits.kill_switch_enabled is False
    assert policy.risk_limits.reduce_only_first is False
    assert config.bot.risk.max_order_size == Decimal("1000")
    assert policy.market_data_requirements.max_order_book_age == timedelta(seconds=60)
    assert policy.order_behavior.default_order_type == OrderType.LIMIT
    assert policy.order_behavior.default_leverage == Decimal("1")
    assert policy.order_behavior.default_margin_type == MarginType.CROSS
    assert policy.order_behavior.post_only is True
    assert policy.staged_or_hidden_release.allow_release is False
    assert policy.staged_or_hidden_release.enabled is True


def test_cfm_passive_market_making_release_template_enables_single_release_manager():
    config = load_coinbase_application_config_from_json_file(
        Path("docs/examples/config.cfm-passive-market-making-release.json")
    )
    policy = config.bot.strategies.operator_policy
    assert policy is not None

    assert config.bot.rest.execution_mode == ExecutionMode.LIVE
    assert config.bot.product_catalog.schedule.enabled is True
    assert config.bot.product_catalog.product_ids == ("REPLACE_WITH_CFM_PRODUCT_IDS",)
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.strategies.strategy_ids == (STAGED_RELEASE_MANAGER_STRATEGY_ID,)
    assert config.bot.strategies.strategy_parameters == {
        STAGED_RELEASE_MANAGER_STRATEGY_ID: {
            "allow_live_overlap": False,
            "max_releases_per_evaluation": 1,
        }
    }
    assert policy.policy_name == "passive_market_making_release_cfm_policy_v0"
    assert policy.scope.live_orders_allowed is True
    assert policy.scope.products == ("REPLACE_WITH_CFM_PRODUCT_IDS",)
    assert policy.risk_limits.kill_switch_enabled is False
    assert policy.risk_limits.reduce_only_first is False
    assert config.bot.risk.max_order_notional == Decimal("200")
    assert policy.risk_limits.max_order_notional_usd == Decimal("200")
    assert policy.risk_limits.max_daily_notional_usd == Decimal("400")
    assert policy.risk_limits.max_open_orders == 4
    assert policy.market_data_requirements.require_order_book is True
    assert policy.market_data_requirements.max_order_book_age == timedelta(seconds=60)
    assert policy.order_behavior.default_order_type == OrderType.LIMIT
    assert policy.order_behavior.default_leverage == Decimal("1")
    assert policy.order_behavior.default_margin_type == MarginType.CROSS
    assert policy.order_behavior.post_only is True
    assert policy.staged_or_hidden_release.allow_release is True
    assert policy.staged_or_hidden_release.release_only_when_conditions_match is True
    assert policy.staged_or_hidden_release.max_visible_notional_usd == Decimal("200")


def test_load_application_config_from_env_converts_strings_to_typed_config(workspace_tmp_path):
    env = {
        "STATERAIL_LEDGER_PATH": str(workspace_tmp_path / "audit.jsonl"),
        "STATERAIL_EXECUTION_MODE": "LIVE",
        "STATERAIL_FEED_MIN_LIVE_SOURCES": "2",
        "STATERAIL_FEED_RECONNECT_INITIAL_DELAY_SECONDS": "0",
        "STATERAIL_FEED_RECONNECT_MAX_DELAY_SECONDS": "3",
        "STATERAIL_FEED_RECONNECT_MULTIPLIER": "2",
        "STATERAIL_FEED_STALE_AFTER_SECONDS": "20",
        "STATERAIL_FEED_HEALTH_ENABLED": "true",
        "STATERAIL_FEED_HEALTH_INTERVAL_SECONDS": "4",
        "STATERAIL_FEED_HEALTH_RUN_ON_START": "false",
        "STATERAIL_REST_RETRY_INITIAL_DELAY_SECONDS": "0",
        "STATERAIL_REST_RETRY_MAX_ATTEMPTS": "2",
        "STATERAIL_REST_RETRY_MAX_DELAY_SECONDS": "1",
        "STATERAIL_REST_RETRY_MULTIPLIER": "3",
        "STATERAIL_RETAIL_PORTFOLIO_ID": "portfolio-1",
        "STATERAIL_RISK_ALLOWED_PRODUCTS": "BTC-USD,ETH-USD",
        "STATERAIL_RISK_ALLOWED_ORDER_TYPES": "limit,market",
        "STATERAIL_RISK_KILL_SWITCH_ENABLED": "true",
        "STATERAIL_RISK_MAX_LEVERAGE": "2",
        "STATERAIL_RISK_MAX_ORDER_NOTIONAL": "500.25",
        "STATERAIL_RISK_MAX_ORDER_SIZE": "1.5",
        "STATERAIL_RISK_REQUIRE_REDUCE_ONLY": "false",
        "STATERAIL_PRODUCT_CATALOG_ENABLED": "true",
        "STATERAIL_PRODUCT_CATALOG_INTERVAL_SECONDS": "1800",
        "STATERAIL_PRODUCT_CATALOG_PRODUCT_IDS": "BTC-USD,BTC-PERP-INTX",
        "STATERAIL_PRODUCT_CATALOG_RUN_ON_START": "false",
        "STATERAIL_STRATEGIES_ENABLED": "true",
        "STATERAIL_STRATEGIES_ALLOW_LIVE_EXECUTION": "true",
        "STATERAIL_STRATEGIES_INTERVAL_SECONDS": "5",
        "STATERAIL_STRATEGIES_MARKET_DATA_REQUIREMENTS": json.dumps(
            [
                {
                    "data_kind": "order_book",
                    "max_age_seconds": 2,
                    "product_id": "BTC-USD",
                }
            ]
        ),
        "STATERAIL_STRATEGIES_MAX_MARKET_TRADES_PER_PRODUCT": "250",
        "STATERAIL_STRATEGIES_MAX_ORDER_BOOK_SAMPLE_DEPTH_PER_SIDE": "5",
        "STATERAIL_STRATEGIES_MAX_ORDER_BOOK_SAMPLES_PER_PRODUCT": "20",
        "STATERAIL_STRATEGIES_OPERATOR_POLICY_FILE": "docs/examples/operator-policy.conservative-cfm-v0.json",
        "STATERAIL_STRATEGIES_RUN_ON_START": "false",
        "STATERAIL_STRATEGY_IDS": "noop",
        "STATERAIL_AUDIT_ANCHOR_ENABLED": "true",
        "STATERAIL_AUDIT_ANCHOR_INTERVAL_SECONDS": "86400",
        "STATERAIL_AUDIT_ANCHOR_LOCAL_DIR": str(workspace_tmp_path / "anchors"),
        "STATERAIL_AUDIT_ANCHOR_RUN_ON_START": "false",
        "STATERAIL_AUDIT_ANCHOR_STORE_PROVIDER": "local_file",
        "STATERAIL_AUDIT_ARCHIVE_ENABLED": "true",
        "STATERAIL_AUDIT_ARCHIVE_INTERVAL_SECONDS": "43200",
        "STATERAIL_AUDIT_ARCHIVE_RUN_ON_START": "false",
        "STATERAIL_AUDIT_ARCHIVE_STORE_PROVIDER": "aws_s3_object_lock",
        "STATERAIL_AUDIT_ARCHIVE_S3_BUCKET": "archive-bucket",
        "STATERAIL_AUDIT_ARCHIVE_S3_EXPECTED_BUCKET_OWNER": "123456789012",
        "STATERAIL_AUDIT_ARCHIVE_S3_IMMUTABILITY_MODE": "compliance",
        "STATERAIL_AUDIT_ARCHIVE_S3_KEY_PREFIX": "staterail/ledger-archives",
        "STATERAIL_AUDIT_ARCHIVE_S3_RETENTION_DAYS": "3650",
        "STATERAIL_AUDIT_ARCHIVE_S3_VERIFY_BUCKET_CONFIGURATION": "true",
        "STATERAIL_TRIGGER_POLLING_ENABLED": "true",
        "STATERAIL_TRIGGER_POLLING_INTERVAL_SECONDS": "2",
        "STATERAIL_TRIGGER_POLLING_RUN_ON_START": "false",
        "STATERAIL_TRIGGERS": json.dumps(
            [
                {
                    "event_type": "error",
                    "relation": "after",
                    "repeatable": True,
                    "trigger_id": "after-error",
                    "type": "message",
                }
            ]
        ),
        "STATERAIL_WATCHDOG_INTERVAL_SECONDS": "6",
        "STATERAIL_WATCHDOG_RUN_ON_START": "false",
        "STATERAIL_USER_CONFIRMATION_TIMEOUT_SECONDS": "31",
        "STATERAIL_EXCHANGE_STATE_POSITION_PRODUCT_IDS": "BTC-USD",
        "STATERAIL_FILL_RECONCILIATION_ENABLED": "true",
        "STATERAIL_FILL_RECONCILIATION_INTERVAL_SECONDS": "17",
        "STATERAIL_FILL_RECONCILIATION_LIMIT": "75",
        "STATERAIL_WEBSOCKET_SOURCES": json.dumps(
            [
                {
                    "channels": ["USER"],
                    "endpoint": "USER_ORDER_DATA",
                    "product_ids": ["BTC-USD"],
                    "source_id": "coinbase-user-primary",
                }
            ]
        ),
    }

    config = load_coinbase_application_config_from_env(env)

    assert has_coinbase_application_env(env) is True
    assert config.ledger_path == workspace_tmp_path / "audit.jsonl"
    assert config.bot.rest.execution_mode == ExecutionMode.LIVE
    assert config.bot.rest.retry_policy.max_attempts == 2
    assert config.bot.rest.retry_policy.initial_delay_seconds == 0
    assert config.bot.rest.retry_policy.max_delay_seconds == 1
    assert config.bot.rest.retry_policy.multiplier == 3
    assert config.bot.rest.retail_portfolio_id == "portfolio-1"
    assert config.bot.risk.allowed_products == ("BTC-USD", "ETH-USD")
    assert config.bot.risk.allowed_order_types == (OrderType.LIMIT, OrderType.MARKET)
    assert config.bot.risk.kill_switch_enabled is True
    assert config.bot.risk.max_leverage == Decimal("2")
    assert config.bot.risk.max_order_notional == Decimal("500.25")
    assert config.bot.risk.max_order_size == Decimal("1.5")
    assert config.bot.risk.require_reduce_only is False
    assert config.bot.product_catalog.schedule.enabled is True
    assert config.bot.product_catalog.schedule.interval == timedelta(minutes=30)
    assert config.bot.product_catalog.schedule.run_on_start is False
    assert config.bot.product_catalog.product_ids == ("BTC-USD", "BTC-PERP-INTX")
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.schedule.interval == timedelta(seconds=5)
    assert config.bot.strategies.schedule.run_on_start is False
    assert config.bot.strategies.strategy_ids == ("noop",)
    assert config.bot.strategies.market_data_requirements[0].data_kind == MarketDataKind.ORDER_BOOK
    assert config.bot.strategies.market_data_requirements[0].max_age == timedelta(seconds=2)
    assert config.bot.strategies.market_data_requirements[0].product_id == "BTC-USD"
    assert config.bot.strategies.max_market_trades_per_product == 250
    assert config.bot.strategies.max_order_book_sample_depth_per_side == 5
    assert config.bot.strategies.max_order_book_samples_per_product == 20
    assert config.bot.strategies.operator_policy is not None
    assert config.bot.strategies.operator_policy.policy_name == "conservative_cfm_policy_v0"
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.audit_anchor_schedule.enabled is True
    assert config.bot.audit_anchor_schedule.interval == timedelta(days=1)
    assert config.bot.audit_anchor_schedule.run_on_start is False
    assert config.bot.audit_anchor_store is not None
    assert config.bot.audit_anchor_store.provider == LedgerAnchorStoreProvider.LOCAL_FILE
    assert config.bot.audit_anchor_store.local_anchor_dir == workspace_tmp_path / "anchors"
    assert config.bot.audit_archive_schedule.enabled is True
    assert config.bot.audit_archive_schedule.interval == timedelta(hours=12)
    assert config.bot.audit_archive_schedule.run_on_start is False
    assert config.bot.audit_archive_store is not None
    assert config.bot.audit_archive_store.s3_bucket == "archive-bucket"
    assert config.bot.audit_archive_store.s3_expected_bucket_owner == "123456789012"
    assert config.bot.audit_archive_store.s3_immutability_mode == AnchorImmutabilityMode.COMPLIANCE
    assert config.bot.audit_archive_store.s3_key_prefix == "staterail/ledger-archives"
    assert config.bot.audit_archive_store.s3_retention_period == timedelta(days=3650)
    assert config.bot.feed.min_live_sources == 2
    assert config.bot.feed.reconnect_policy.initial_delay_seconds == 0
    assert config.bot.feed.reconnect_policy.max_delay_seconds == 3
    assert config.bot.feed.reconnect_policy.multiplier == 2
    assert config.bot.feed.stale_after == timedelta(seconds=20)
    assert config.bot.feed_health_schedule.enabled is True
    assert config.bot.feed_health_schedule.interval == timedelta(seconds=4)
    assert config.bot.feed_health_schedule.run_on_start is False
    assert config.bot.reconciliation.watchdog_schedule.interval == timedelta(seconds=6)
    assert config.bot.reconciliation.watchdog_schedule.run_on_start is False
    assert config.bot.reconciliation.watchdog_policy.user_confirmation_timeout == timedelta(seconds=31)
    assert config.bot.reconciliation.fill_schedule.enabled is True
    assert config.bot.reconciliation.fill_policy.limit == 75
    assert config.bot.reconciliation.exchange_state_policy.position_product_ids == ("BTC-USD",)
    assert config.bot.trigger_polling_schedule.enabled is True
    assert config.bot.trigger_polling_schedule.interval == timedelta(seconds=2)
    assert config.bot.trigger_polling_schedule.run_on_start is False
    assert config.bot.trigger_rules[0].trigger_id == "after-error"
    assert config.bot.trigger_rules[0].event_type == EventType.ERROR
    assert config.bot.websocket_sources[0].channels == (CoinbaseWebSocketChannel.USER,)


def test_load_application_config_from_empty_env_uses_defaults():
    config = CoinbaseApplicationConfig.from_env({})

    assert config.bot.rest.execution_mode == ExecutionMode.DRY_RUN


def test_loader_rejects_unknown_fields():
    with pytest.raises(ConfigError, match="unknown"):
        load_coinbase_application_config_from_mapping({"bot": {"rest": {"mode": "live"}}})


def test_loader_rejects_task_fields_that_would_be_ignored():
    with pytest.raises(ValueError, match="order_recovery"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"reconciliation": {"order_recovery": {"limit": 5}}}}
        )


def test_loader_rejects_invalid_enums_and_intervals():
    with pytest.raises(ValueError, match="ExecutionMode"):
        load_coinbase_application_config_from_mapping({"bot": {"rest": {"execution_mode": "paper"}}})

    with pytest.raises(ValueError, match="positive"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"reconciliation": {"watchdog": {"interval_seconds": 0}}}}
        )

    with pytest.raises(TypeError, match="integer"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"reconciliation": {"fills": {"limit": 1.5}}}}
        )

    with pytest.raises(ValueError, match="non-negative"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"rest": {"retry": {"initial_delay_seconds": -1}}}}
        )

    with pytest.raises(ValueError, match="at least 1"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"feed": {"reconnect": {"multiplier": 0.5}}}}
        )

    with pytest.raises(ConfigError, match="unknown"):
        load_coinbase_application_config_from_mapping(
            {
                "bot": {
                    "audit_anchor": {
                        "store": {
                            "bucket": "audit-bucket",
                            "provider": "local_file",
                        }
                    }
                }
            }
        )

    with pytest.raises(ValueError, match="OrderType"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"risk": {"allowed_order_types": ["stop"]}}}
        )

    with pytest.raises(ValueError, match="max_order_size"):
        load_coinbase_application_config_from_mapping(
            {"bot": {"risk": {"max_order_size": "0"}}}
        )


def test_env_loader_rejects_invalid_websocket_sources_json():
    with pytest.raises(ValueError, match="WEBSOCKET_SOURCES"):
        load_coinbase_application_config_from_env({"STATERAIL_WEBSOCKET_SOURCES": "not-json"})


def test_env_loader_rejects_invalid_triggers_json():
    with pytest.raises(ValueError, match="TRIGGERS"):
        load_coinbase_application_config_from_env({"STATERAIL_TRIGGERS": "not-json"})


def test_env_loader_rejects_invalid_strategy_market_data_requirements_json():
    with pytest.raises(ValueError, match="STRATEGIES_MARKET_DATA_REQUIREMENTS"):
        load_coinbase_application_config_from_env(
            {"STATERAIL_STRATEGIES_MARKET_DATA_REQUIREMENTS": "not-json"}
        )


def test_env_loader_rejects_invalid_strategy_parameters_json():
    with pytest.raises(ValueError, match="STRATEGIES_PARAMETERS"):
        load_coinbase_application_config_from_env(
            {"STATERAIL_STRATEGIES_PARAMETERS": "not-json"}
        )


def test_env_loader_rejects_unknown_coinbase_env_vars():
    with pytest.raises(ConfigError, match="STATERAIL_NOT_REAL"):
        load_coinbase_application_config_from_env({"STATERAIL_NOT_REAL": "1"})


def test_env_loader_rejects_legacy_coinbase_bot_env_vars():
    with pytest.raises(ConfigError, match="renamed to STATERAIL_"):
        load_coinbase_application_config_from_env({"COINBASE_BOT_LEDGER_PATH": "data/audit.jsonl"})

    assert has_coinbase_application_env({"COINBASE_BOT_LEDGER_PATH": "data/audit.jsonl"}) is True


def test_has_coinbase_application_env_is_false_without_known_prefix():
    assert has_coinbase_application_env({"OTHER_LEDGER_PATH": "audit.jsonl"}) is False


def _env_example_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, value = stripped.partition("=")
        if not separator:
            raise ValueError(f"Invalid env example line: {line}")
        values[key] = value
    return values
