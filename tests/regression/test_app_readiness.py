from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from app.bootstrap import CoinbaseApplicationConfig, build_coinbase_application, default_coinbase_application_config
from app.config_fingerprint import application_config_fingerprint
from app.config_loading import load_coinbase_application_config_from_json_file
from app.credentials import COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from app.readiness import readiness_report
from audit.s3_object_lock import (
    S3ObjectLockAnchorConfig,
    S3ObjectLockLedgerArchiveStore,
    S3ObjectLockLedgerAnchorStore,
)
from config.assembly import (
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
    CoinbaseWebSocketSourceConfig,
    FeedRuntimeConfig,
    ProductCatalogRuntimeConfig,
    StrategyRuntimeConfig,
)
from config.assembly import ReconciliationRuntimeConfig, RiskPolicyConfig, TaskScheduleConfig
from core.enums import (
    AnchorImmutabilityMode,
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    ExecutionMode,
    LedgerAnchorStoreProvider,
    OperatorPolicyPermission,
    OrderType,
    ReadinessCheckName,
    ReadinessRequirement,
    ReadinessStatus,
    RiskControl,
    RuntimeTask,
)
from strategies import (
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
    load_operator_policy_from_json_file,
)


class ReadinessFakeS3ObjectLockClient:
    def __init__(self, *, versioning_status: str = "Enabled", object_lock_enabled: str = "Enabled") -> None:
        self.get_bucket_versioning_calls: list[dict[str, Any]] = []
        self.get_object_lock_configuration_calls: list[dict[str, Any]] = []
        self.put_object_calls: list[dict[str, Any]] = []
        self._versioning_status = versioning_status
        self._object_lock_enabled = object_lock_enabled

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        self.get_bucket_versioning_calls.append(kwargs)
        return {"Status": self._versioning_status}

    def get_object_lock_configuration(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_lock_configuration_calls.append(kwargs)
        return {"ObjectLockConfiguration": {"ObjectLockEnabled": self._object_lock_enabled}}


def test_readiness_reports_default_dry_run_config_as_ready_without_creating_ledger(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = default_coinbase_application_config(ledger_path=ledger_path)

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    payload = report.to_payload()

    assert report.status == ReadinessStatus.OK
    assert payload["config_fingerprint"] == application_config_fingerprint(config)
    assert checks[ReadinessCheckName.CONFIG_FINGERPRINT].status == ReadinessStatus.OK
    assert checks[ReadinessCheckName.CONFIG_FINGERPRINT].details["ledger_checked"] is False
    assert checks[ReadinessCheckName.CONFIG_FINGERPRINT].details["ledger_exists"] is False
    assert checks[ReadinessCheckName.CONFIG_FINGERPRINT].details["ledger_config_fingerprint_matches"] is None
    assert checks[ReadinessCheckName.CREDENTIALS].status == ReadinessStatus.OK
    assert checks[ReadinessCheckName.LEDGER_PATH].status == ReadinessStatus.OK
    assert checks[ReadinessCheckName.LEDGER_PATH].details["lock_exists"] is False
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["enabled_count"] == 1
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].details["source_count"] == 0
    assert not ledger_path.exists()


def test_readiness_reports_matching_latest_ledger_startup_config_fingerprint(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = default_coinbase_application_config(ledger_path=ledger_path)
    application = build_coinbase_application(config)

    asyncio.run(application.run(max_cycles=1))
    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    config_check = checks[ReadinessCheckName.CONFIG_FINGERPRINT]

    assert report.status == ReadinessStatus.OK
    assert config_check.status == ReadinessStatus.OK
    assert config_check.details["ledger_checked"] is True
    assert config_check.details["ledger_record_count"] == 4
    assert config_check.details["ledger_config_fingerprint_matches"] is True
    assert config_check.details["latest_ledger_config_fingerprint"] == application_config_fingerprint(config)
    assert config_check.details["latest_ledger_start_sequence"] == 1


def test_readiness_reports_config_drift_from_latest_ledger_startup(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    original_config = default_coinbase_application_config(ledger_path=ledger_path)
    application = build_coinbase_application(original_config)

    asyncio.run(application.run(max_cycles=1))
    changed_config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(feed=FeedRuntimeConfig(stale_after=timedelta(seconds=45))),
    )
    report = readiness_report(changed_config)
    checks = {check.name: check for check in report.checks}
    config_check = checks[ReadinessCheckName.CONFIG_FINGERPRINT]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert config_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert config_check.details["fingerprint"] == application_config_fingerprint(changed_config)
    assert config_check.details["latest_ledger_config_fingerprint"] == application_config_fingerprint(
        original_config
    )
    assert config_check.details["ledger_config_fingerprint_matches"] is False


def test_readiness_can_allow_reviewed_config_fingerprint_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    original_config = default_coinbase_application_config(ledger_path=ledger_path)
    application = build_coinbase_application(original_config)

    asyncio.run(application.run(max_cycles=1))
    changed_config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(feed=FeedRuntimeConfig(stale_after=timedelta(seconds=45))),
    )
    report = readiness_report(
        changed_config,
        allow_config_fingerprint_mismatch=True,
    )
    checks = {check.name: check for check in report.checks}
    config_check = checks[ReadinessCheckName.CONFIG_FINGERPRINT]

    assert report.status == ReadinessStatus.OK
    assert config_check.status == ReadinessStatus.OK
    assert config_check.count == 0
    assert config_check.details["fingerprint"] == application_config_fingerprint(changed_config)
    assert config_check.details["latest_ledger_config_fingerprint"] == application_config_fingerprint(
        original_config
    )
    assert config_check.details["ledger_config_fingerprint_matches"] is False
    assert config_check.details["ledger_config_fingerprint_mismatch_allowed"] is True


def test_readiness_reports_existing_ledger_lock_as_attention(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    lock_path = ledger_path.with_name(f"{ledger_path.name}.lock")
    lock_path.write_text("other-process\n", encoding="utf-8")
    config = default_coinbase_application_config(ledger_path=ledger_path)

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.LEDGER_PATH].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.LEDGER_PATH].details["lock_exists"] is True
    assert checks[ReadinessCheckName.LEDGER_PATH].details["lock_path"] == lock_path.as_posix()


def test_readiness_reports_live_config_missing_injected_credentials_and_websocket_redundancy(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
            websocket_sources=(
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-user-primary",
                    channels=(CoinbaseWebSocketChannel.USER,),
                    endpoint=CoinbaseWebSocketEndpoint.USER_ORDER_DATA,
                    product_ids=("BTC-USD",),
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.CREDENTIALS].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.CREDENTIALS].details["missing_requirements"] == [
        ReadinessRequirement.TOKEN_PROVIDER.value,
        ReadinessRequirement.JWT_FACTORY.value,
    ]
    assert checks[ReadinessCheckName.RISK_POLICY].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RISK_POLICY].details["unguarded_live_execution"] is True
    assert checks[ReadinessCheckName.RISK_POLICY].details["live_execution_without_product_catalog"] is True
    assert checks[ReadinessCheckName.RISK_POLICY].details["missing_requirements"] == [
        ReadinessRequirement.RISK_POLICY.value,
        ReadinessRequirement.PRODUCT_CATALOG.value,
    ]
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].details["user_source_count"] == 1
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].details["single_source_scope_count"] == 1


def test_readiness_reports_unreplaced_config_placeholders(workspace_tmp_path):
    config = load_coinbase_application_config_from_json_file("docs/examples/config.cfm-live.json")
    config = CoinbaseApplicationConfig(ledger_path=workspace_tmp_path / "audit.jsonl", bot=config.bot)

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    placeholder_check = checks[ReadinessCheckName.CONFIG_PLACEHOLDERS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert placeholder_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert placeholder_check.count > 0
    assert "$.bot.rest.perpetual_portfolio_uuid" not in placeholder_check.details["placeholder_paths"]
    assert "$.bot.reconciliation.exchange_state.policy.perpetual_portfolio_uuid" not in placeholder_check.details[
        "placeholder_paths"
    ]
    assert "$.bot.risk.allowed_products[0]" in placeholder_check.details["placeholder_paths"]


def test_readiness_accepts_live_config_with_configured_risk_controls(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
            risk=RiskPolicyConfig(
                allowed_order_types=(OrderType.LIMIT,),
                allowed_products=("BTC-USD",),
            ),
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("BTC-USD",),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    risk_check = checks[ReadinessCheckName.RISK_POLICY]

    assert risk_check.status == ReadinessStatus.OK
    assert checks[ReadinessCheckName.CONFIG_PLACEHOLDERS].status == ReadinessStatus.OK
    assert risk_check.details["unguarded_live_execution"] is False
    assert risk_check.details["live_execution_without_product_catalog"] is False
    assert risk_check.details["product_catalog_refresh_enabled"] is True
    assert risk_check.details["configured_controls"] == [
        RiskControl.ALLOWED_PRODUCTS.value,
        RiskControl.ALLOWED_ORDER_TYPES.value,
    ]


def test_readiness_reports_live_strategy_schedule_without_strategy_allowance(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
            risk=RiskPolicyConfig(
                allowed_order_types=(OrderType.LIMIT,),
                allowed_products=("BTC-USD",),
            ),
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("BTC-USD",),
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=("noop",),
            ),
        ),
    )

    report = readiness_report(config, live_trading_approved=True, token_provider_configured=True)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.STRATEGY_LIVE_APPROVAL.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["strategy_allow_live_execution"] is False


def test_readiness_reports_unresolved_strategy_ids(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=("missing-strategy",),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.STRATEGY_RESOLUTION.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["strategy_unresolved_ids"] == ["missing-strategy"]
    assert "noop" in runtime_check.details["strategy_available_ids"]


def test_readiness_reports_unsupported_strategy_parameters(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=("noop",),
                strategy_parameters={"noop": {"target_notional_usd": "5"}},
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.STRATEGY_PARAMETERS.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["strategy_parameter_ids"] == ["noop"]
    assert "not supported" in runtime_check.details["strategy_parameter_error"]


def test_readiness_accepts_registered_strategy_ids(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=("registered",),
            ),
        ),
    )

    report = readiness_report(config, available_strategy_ids=("registered",))
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.OK
    assert runtime_check.status == ReadinessStatus.OK
    assert runtime_check.details["strategy_unresolved_ids"] == []
    assert "registered" in runtime_check.details["strategy_available_ids"]


def test_readiness_accepts_builtin_staged_release_manager_strategy_id(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert runtime_check.status == ReadinessStatus.OK
    assert runtime_check.details["strategy_unresolved_ids"] == []
    assert STAGED_RELEASE_MANAGER_STRATEGY_ID in runtime_check.details["strategy_available_ids"]
    assert runtime_check.details["staged_release_manager_selected"] is True
    assert runtime_check.details["staged_release_manager_operator_policy_configured"] is True
    assert runtime_check.details["staged_release_manager_release_enabled"] is True


def test_readiness_reports_staged_release_manager_without_operator_policy(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(STAGED_RELEASE_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.OPERATOR_POLICY.value in runtime_check.details["missing_requirements"]
    assert ReadinessRequirement.STRATEGY_RESOLUTION.value not in runtime_check.details["missing_requirements"]
    assert runtime_check.details["staged_release_manager_selected"] is True
    assert runtime_check.details["staged_release_manager_operator_policy_configured"] is False
    assert runtime_check.details["staged_release_manager_release_enabled"] is None


def test_readiness_reports_staged_release_manager_with_disabled_staged_release_policy(
    workspace_tmp_path,
):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    policy = base_config.bot.strategies.operator_policy
    assert policy is not None
    disabled_policy = replace(
        policy,
        staged_or_hidden_release=replace(policy.staged_or_hidden_release, enabled=False),
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                operator_policy=disabled_policy,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.STAGED_RELEASE_POLICY.value in runtime_check.details["missing_requirements"]
    assert ReadinessRequirement.STRATEGY_RESOLUTION.value not in runtime_check.details["missing_requirements"]
    assert runtime_check.details["staged_release_manager_selected"] is True
    assert runtime_check.details["staged_release_manager_operator_policy_configured"] is True
    assert runtime_check.details["staged_release_manager_release_enabled"] is False


def test_readiness_reports_staged_release_manager_with_release_blocked_by_policy(
    workspace_tmp_path,
):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    policy = base_config.bot.strategies.operator_policy
    assert policy is not None
    release_blocked_policy = replace(
        policy,
        staged_or_hidden_release=replace(policy.staged_or_hidden_release, allow_release=False),
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                operator_policy=release_blocked_policy,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.STAGED_RELEASE_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["staged_release_manager_release_enabled"] is False


def test_readiness_reports_staged_release_manager_condition_matching_without_order_book(
    workspace_tmp_path,
):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    policy = base_config.bot.strategies.operator_policy
    assert policy is not None
    missing_order_book_policy = replace(
        policy,
        market_data_requirements=replace(
            policy.market_data_requirements,
            require_order_book=False,
        ),
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                operator_policy=missing_order_book_policy,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.STAGED_RELEASE_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["staged_release_manager_release_enabled"] is True
    assert runtime_check.details["staged_release_manager_release_conditions_match_enabled"] is True
    assert runtime_check.details["staged_release_manager_order_book_required"] is False


def test_readiness_reports_followup_manager_without_operator_policy(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.OPERATOR_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["followup_on_fill_manager_selected"] is True
    assert runtime_check.details["followup_on_fill_manager_operator_policy_configured"] is False


def test_readiness_reports_followup_manager_without_product_catalog(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.PRODUCT_CATALOG.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["followup_on_fill_manager_selected"] is True
    assert runtime_check.details["followup_on_fill_manager_followup_enabled"] is True
    assert runtime_check.details["followup_on_fill_manager_product_catalog_enabled"] is False


def test_readiness_reports_followup_manager_with_disabled_followup_policy(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=replace(
                base_config.bot.strategies,
                operator_policy=replace(
                    base_config.bot.strategies.operator_policy,
                    partial_fills=replace(
                        base_config.bot.strategies.operator_policy.partial_fills,
                        followup_enabled=False,
                    ),
                ),
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.FOLLOWUP_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["followup_on_fill_manager_followup_enabled"] is False


def test_readiness_accepts_followup_manager_with_policy_and_product_catalog(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert runtime_check.status == ReadinessStatus.OK
    assert runtime_check.details["followup_on_fill_manager_selected"] is True
    assert runtime_check.details["followup_on_fill_manager_product_catalog_enabled"] is True
    assert FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID in runtime_check.details["strategy_available_ids"]


def test_readiness_reports_consolidation_manager_without_operator_policy(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(CONSOLIDATION_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.OPERATOR_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["consolidation_manager_selected"] is True
    assert runtime_check.details["consolidation_manager_operator_policy_configured"] is False


def test_readiness_reports_consolidation_manager_without_product_catalog(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(CONSOLIDATION_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.PRODUCT_CATALOG.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["consolidation_manager_selected"] is True
    assert runtime_check.details["consolidation_manager_merge_enabled"] is True
    assert runtime_check.details["consolidation_manager_product_catalog_enabled"] is False


def test_readiness_reports_consolidation_manager_with_disabled_merge_policy(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    policy = base_config.bot.strategies.operator_policy
    assert policy is not None
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=replace(
                base_config.bot.strategies,
                operator_policy=replace(
                    policy,
                    lineage=replace(
                        policy.lineage,
                        merge_orders=OperatorPolicyPermission.DISABLED,
                    ),
                ),
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(CONSOLIDATION_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.CONSOLIDATION_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["consolidation_manager_merge_enabled"] is False


def test_readiness_accepts_consolidation_manager_with_policy_and_product_catalog(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(CONSOLIDATION_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert runtime_check.status == ReadinessStatus.OK
    assert runtime_check.details["consolidation_manager_selected"] is True
    assert runtime_check.details["consolidation_manager_merge_enabled"] is True
    assert runtime_check.details["consolidation_manager_product_catalog_enabled"] is True
    assert CONSOLIDATION_MANAGER_STRATEGY_ID in runtime_check.details["strategy_available_ids"]


def test_readiness_reports_anchor_repricing_manager_without_product_catalog(workspace_tmp_path):
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json")
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            strategies=StrategyRuntimeConfig(
                operator_policy=policy,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(ANCHOR_REPRICING_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.PRODUCT_CATALOG.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["anchor_repricing_manager_selected"] is True
    assert runtime_check.details["anchor_repricing_manager_product_catalog_enabled"] is False


def test_readiness_reports_anchor_repricing_manager_with_unsafe_policy(workspace_tmp_path):
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json")
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=StrategyRuntimeConfig(
                operator_policy=replace(
                    policy,
                    moves=replace(policy.moves, cancel_replace_when_amend_not_supported=False),
                ),
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(ANCHOR_REPRICING_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.ANCHOR_REPRICING_POLICY.value in runtime_check.details[
        "missing_requirements"
    ]
    assert runtime_check.details["anchor_repricing_manager_cancel_replace_enabled"] is False


def test_readiness_accepts_anchor_repricing_manager_with_policy_and_product_catalog(workspace_tmp_path):
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json")
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=StrategyRuntimeConfig(
                operator_policy=policy,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(ANCHOR_REPRICING_MANAGER_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert runtime_check.status == ReadinessStatus.OK
    assert runtime_check.details["anchor_repricing_manager_anchor_enabled"] is True
    assert runtime_check.details["anchor_repricing_manager_cancel_replace_enabled"] is True
    assert runtime_check.details["anchor_repricing_manager_move_enabled"] is True
    assert runtime_check.details["anchor_repricing_manager_product_catalog_enabled"] is True
    assert ANCHOR_REPRICING_MANAGER_STRATEGY_ID in runtime_check.details["strategy_available_ids"]


def test_readiness_reports_passive_market_making_without_operator_policy(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
            strategies=StrategyRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.OPERATOR_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["passive_market_making_selected"] is True
    assert runtime_check.details["passive_market_making_operator_policy_configured"] is False


def test_readiness_reports_passive_market_making_without_product_catalog(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.PRODUCT_CATALOG.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["passive_market_making_selected"] is True
    assert runtime_check.details["passive_market_making_product_catalog_enabled"] is False
    assert runtime_check.details["passive_market_making_staged_release_enabled"] is True


def test_readiness_reports_passive_market_making_with_unsafe_policy(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    policy = base_config.bot.strategies.operator_policy
    assert policy is not None
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=replace(
                base_config.bot.strategies,
                operator_policy=replace(
                    policy,
                    order_behavior=replace(policy.order_behavior, post_only=False),
                ),
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert runtime_check.status == ReadinessStatus.ATTENTION_REQUIRED
    assert ReadinessRequirement.PASSIVE_MARKET_MAKING_POLICY.value in runtime_check.details["missing_requirements"]
    assert runtime_check.details["passive_market_making_post_only"] is False


def test_readiness_accepts_passive_market_making_with_policy_and_product_catalog(workspace_tmp_path):
    base_config = load_coinbase_application_config_from_json_file(
        "docs/examples/config.staged-release-manager.dry-run.json"
    )
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=replace(
            base_config.bot,
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=("SHB-26JUN26-CDE",),
            ),
            strategies=replace(
                base_config.bot.strategies,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                ),
                strategy_ids=(PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}
    runtime_check = checks[ReadinessCheckName.RUNTIME_TASKS]

    assert runtime_check.status == ReadinessStatus.OK
    assert runtime_check.details["passive_market_making_selected"] is True
    assert runtime_check.details["passive_market_making_order_book_required"] is True
    assert runtime_check.details["passive_market_making_order_type_limit"] is True
    assert runtime_check.details["passive_market_making_post_only"] is True
    assert runtime_check.details["passive_market_making_product_catalog_enabled"] is True
    assert runtime_check.details["passive_market_making_staged_release_enabled"] is True
    assert PASSIVE_MARKET_MAKING_STRATEGY_ID in runtime_check.details["strategy_available_ids"]


def test_readiness_reports_impossible_min_live_feed_configuration(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            feed=FeedRuntimeConfig(min_live_sources=2),
            websocket_sources=(
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-market-primary",
                    channels=(CoinbaseWebSocketChannel.LEVEL2,),
                    endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                    product_ids=("BTC-USD",),
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].details["configured_min_live_sources"] == 2
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES].details["impossible_min_live_sources"] is True


def test_readiness_reports_websocket_config_without_feed_health_task(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            feed_health_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.FEED_HEALTH,
                interval=timedelta(seconds=5),
                enabled=False,
            ),
            websocket_sources=(
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-market-primary",
                    channels=(CoinbaseWebSocketChannel.LEVEL2,),
                    endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                    product_ids=("BTC-USD",),
                ),
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-market-secondary",
                    channels=(CoinbaseWebSocketChannel.LEVEL2,),
                    endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                    product_ids=("BTC-USD",),
                ),
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RUNTIME_TASKS].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["feed_health_required"] is True
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["feed_health_enabled"] is False
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["missing_requirements"] == [
        ReadinessRequirement.FEED_HEALTH_TASK.value
    ]


def test_readiness_reports_enabled_audit_anchor_missing_store(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            audit_anchor_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.AUDIT_ANCHOR,
                interval=timedelta(hours=24),
                enabled=True,
            ),
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RUNTIME_TASKS].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["audit_anchor_store_required"] is True
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["audit_anchor_store_configured"] is False
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["missing_requirements"] == [
        ReadinessRequirement.AUDIT_ANCHOR_STORE.value
    ]


def test_readiness_reports_enabled_audit_archive_missing_store(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            audit_archive_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.AUDIT_ARCHIVE,
                interval=timedelta(hours=24),
                enabled=True,
            ),
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
        ),
    )

    report = readiness_report(config)
    checks = {check.name: check for check in report.checks}

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RUNTIME_TASKS].status == ReadinessStatus.ATTENTION_REQUIRED
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["audit_archive_store_required"] is True
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["audit_archive_store_configured"] is False
    assert checks[ReadinessCheckName.RUNTIME_TASKS].details["missing_requirements"] == [
        ReadinessRequirement.AUDIT_ARCHIVE_STORE.value
    ]


def test_cli_readiness_reports_config_without_running_runtime_or_creating_ledger(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-readiness-audit.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["ledger_path"] == ledger_path.as_posix()
    assert payload["read_only"] is True
    assert not ledger_path.exists()
    assert "completed_cycles=" not in output


def test_cli_readiness_can_fail_on_attention(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-readiness-attention-audit.jsonl"
    ledger_path.with_name(f"{ledger_path.name}.lock").write_text("locked", encoding="utf-8")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
                readiness_fail_on_attention=True,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["read_only"] is True
    assert not ledger_path.exists()
    assert "completed_cycles=" not in output


def test_cli_readiness_preflights_s3_anchor_without_writing(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-s3-readiness-audit.jsonl"
    client = ReadinessFakeS3ObjectLockClient()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_anchor_dir=None,
                ledger_anchor_s3_bucket="audit-bucket",
                ledger_anchor_s3_expected_bucket_owner="123456789012",
                ledger_anchor_s3_mode=AnchorImmutabilityMode.COMPLIANCE.value,
                ledger_anchor_s3_prefix="staterail/anchors",
                ledger_anchor_s3_retention_days=2555,
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
            ),
            s3_anchor_store_factory=lambda config: S3ObjectLockLedgerAnchorStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    anchor_check = checks[ReadinessCheckName.ANCHOR_STORE.value]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert anchor_check["status"] == ReadinessStatus.OK.value
    assert anchor_check["details"]["bucket"] == "audit-bucket"
    assert anchor_check["details"]["bucket_configuration_verified"] is True
    assert anchor_check["details"]["provider"] == "aws_s3_object_lock"
    assert anchor_check["details"]["write_attempted"] is False
    assert client.get_bucket_versioning_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.get_object_lock_configuration_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.put_object_calls == []
    assert not ledger_path.exists()


def test_cli_readiness_preflights_configured_s3_anchor_store_without_writing(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "configured-s3-readiness-audit.jsonl"
    config_path = workspace_tmp_path / "bot-config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": str(ledger_path),
                "bot": {
                    "audit_anchor": {
                        "enabled": True,
                        "store": {
                            "bucket": "audit-bucket",
                            "expected_bucket_owner": "123456789012",
                            "immutability_mode": AnchorImmutabilityMode.COMPLIANCE.value,
                            "key_prefix": "staterail/anchors",
                            "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
                            "retention_days": 2555,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    client = ReadinessFakeS3ObjectLockClient()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                max_cycles=99,
                readiness=True,
            ),
            s3_anchor_store_factory=lambda config: S3ObjectLockLedgerAnchorStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    anchor_check = checks[ReadinessCheckName.ANCHOR_STORE.value]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert anchor_check["status"] == ReadinessStatus.OK.value
    assert anchor_check["details"]["provider"] == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value
    assert anchor_check["details"]["bucket_configuration_verified"] is True
    assert client.put_object_calls == []
    assert not ledger_path.exists()


def test_cli_readiness_preflights_s3_archive_without_writing(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-s3-archive-readiness-audit.jsonl"
    client = ReadinessFakeS3ObjectLockClient()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_archive_s3_bucket="archive-bucket",
                ledger_archive_s3_expected_bucket_owner="123456789012",
                ledger_archive_s3_mode=AnchorImmutabilityMode.COMPLIANCE.value,
                ledger_archive_s3_prefix="staterail/ledger-archives",
                ledger_archive_s3_retention_days=2555,
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
            ),
            s3_archive_store_factory=lambda config: S3ObjectLockLedgerArchiveStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    archive_check = checks[ReadinessCheckName.ARCHIVE_STORE.value]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert archive_check["status"] == ReadinessStatus.OK.value
    assert archive_check["details"]["bucket"] == "archive-bucket"
    assert archive_check["details"]["bucket_configuration_verified"] is True
    assert archive_check["details"]["provider"] == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value
    assert archive_check["details"]["write_attempted"] is False
    assert client.get_bucket_versioning_calls == [
        {"Bucket": "archive-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.get_object_lock_configuration_calls == [
        {"Bucket": "archive-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.put_object_calls == []
    assert not ledger_path.exists()


def test_cli_readiness_preflights_configured_s3_archive_store_without_writing(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "configured-s3-archive-readiness-audit.jsonl"
    config_path = workspace_tmp_path / "bot-config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": str(ledger_path),
                "bot": {
                    "audit_archive": {
                        "enabled": True,
                        "store": {
                            "bucket": "archive-bucket",
                            "expected_bucket_owner": "123456789012",
                            "immutability_mode": AnchorImmutabilityMode.COMPLIANCE.value,
                            "key_prefix": "staterail/ledger-archives",
                            "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
                            "retention_days": 2555,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    client = ReadinessFakeS3ObjectLockClient()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                max_cycles=99,
                readiness=True,
            ),
            s3_archive_store_factory=lambda config: S3ObjectLockLedgerArchiveStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    archive_check = checks[ReadinessCheckName.ARCHIVE_STORE.value]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert archive_check["status"] == ReadinessStatus.OK.value
    assert archive_check["details"]["provider"] == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value
    assert archive_check["details"]["bucket_configuration_verified"] is True
    assert client.put_object_calls == []
    assert not ledger_path.exists()


def test_cli_readiness_reports_s3_anchor_attention_for_bucket_misconfiguration(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-s3-readiness-misconfigured-audit.jsonl"
    client = ReadinessFakeS3ObjectLockClient(versioning_status="Suspended")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_anchor_dir=None,
                ledger_anchor_s3_bucket="audit-bucket",
                ledger_anchor_s3_expected_bucket_owner=None,
                ledger_anchor_s3_mode=AnchorImmutabilityMode.GOVERNANCE.value,
                ledger_anchor_s3_prefix="audit-anchors",
                ledger_anchor_s3_retention_days=1,
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
            ),
            s3_anchor_store_factory=lambda config: S3ObjectLockLedgerAnchorStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    anchor_check = checks[ReadinessCheckName.ANCHOR_STORE.value]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert anchor_check["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert anchor_check["details"]["bucket_configuration_verified"] is False
    assert "versioning" in anchor_check["details"]["message"]
    assert client.put_object_calls == []
    assert not ledger_path.exists()


def test_cli_readiness_reports_s3_archive_attention_for_bucket_misconfiguration(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-s3-archive-readiness-misconfigured-audit.jsonl"
    client = ReadinessFakeS3ObjectLockClient(object_lock_enabled="Disabled")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_archive_s3_bucket="archive-bucket",
                ledger_archive_s3_expected_bucket_owner=None,
                ledger_archive_s3_mode=AnchorImmutabilityMode.GOVERNANCE.value,
                ledger_archive_s3_prefix="audit-ledger-archives",
                ledger_archive_s3_retention_days=1,
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
            ),
            s3_archive_store_factory=lambda config: S3ObjectLockLedgerArchiveStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    archive_check = checks[ReadinessCheckName.ARCHIVE_STORE.value]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert archive_check["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert archive_check["details"]["bucket_configuration_verified"] is False
    assert "Object Lock" in archive_check["details"]["message"]
    assert client.put_object_calls == []
    assert not ledger_path.exists()


def _clear_coinbase_env(monkeypatch) -> None:
    for key in list(os.environ):
        if (
            key.startswith("STATERAIL_")
            or key.startswith("COINBASE_BOT_")
            or key in {COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV}
        ):
            monkeypatch.delenv(key, raising=False)
