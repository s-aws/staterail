from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import timedelta

import pytest

from app.bootstrap import CoinbaseApplicationConfig, default_coinbase_application_config
from app.ledger_health import ledger_health_payload
from app.ledger_health_acknowledgement import acknowledge_ledger_health
from app.live_preflight_gate import record_live_no_order_preflight_result
from app.live_runtime_gate import enforce_live_runtime_gate, live_runtime_gate_payload
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from app.strategy_simulation import strategy_simulation_payload
from app.strategy_simulation_gate import record_strategy_simulation_result
from core.engine import AuditCore
from audit.ledger import AuditLedger
from config.assembly import (
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
    ProductCatalogRuntimeConfig,
    RiskPolicyConfig,
    StrategyRuntimeConfig,
    TaskScheduleConfig,
)
from core.enums import (
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    LedgerHealthAcknowledgementIssue,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    OrderType,
    PreflightStep,
    ReadinessCheckName,
    ReadinessStatus,
    RuntimeTask,
    StrategySimulationGateIssue,
)
from core.errors import ConfigError, error_event_payload


def test_live_runtime_gate_is_noop_for_dry_run_without_creating_ledger(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "dry-run-gate.jsonl"
    config = default_coinbase_application_config(ledger_path=ledger_path)

    payload = live_runtime_gate_payload(config, approved=False)

    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["runtime_would_start"] is True
    assert payload["live_rest_execution"] is False
    assert not ledger_path.exists()


def test_live_runtime_gate_reports_missing_live_evidence(workspace_tmp_path):
    config = _live_config(workspace_tmp_path / "missing-live-gate.jsonl")

    payload = live_runtime_gate_payload(config, approved=False)
    checks = {check["name"]: check for check in payload["checks"]}

    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["runtime_would_start"] is False
    assert checks[ReadinessCheckName.LIVE_RUNTIME_SAFETY.value]["status"] == (
        ReadinessStatus.ATTENTION_REQUIRED.value
    )
    assert checks[ReadinessCheckName.LIVE_NO_ORDER_PREFLIGHT.value]["status"] == (
        ReadinessStatus.ATTENTION_REQUIRED.value
    )
    assert checks[ReadinessCheckName.STRATEGY_SIMULATION.value]["status"] == (
        ReadinessStatus.OK.value
    )


def test_live_runtime_gate_accepts_clean_preflight_without_strategies(workspace_tmp_path):
    config = _live_config(workspace_tmp_path / "clean-live-gate.jsonl")
    record_live_no_order_preflight_result(config, _clean_live_preflight_payload())

    payload = live_runtime_gate_payload(config, approved=True)
    checks = {check["name"]: check for check in payload["checks"]}

    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["runtime_would_start"] is True
    assert checks[ReadinessCheckName.LIVE_NO_ORDER_PREFLIGHT.value]["details"]["matching_result"][
        "status"
    ] == ReadinessStatus.OK.value
    assert checks[ReadinessCheckName.STRATEGY_SIMULATION.value]["details"]["required"] is False


def test_live_runtime_gate_blocks_ledger_health_attention(workspace_tmp_path):
    config = _live_config(workspace_tmp_path / "ledger-health-live-gate.jsonl")
    record_live_no_order_preflight_result(config, _clean_live_preflight_payload())
    AuditCore(AuditLedger(config.ledger_path)).emit(
        EventType.ERROR,
        error_event_payload(
            category=ErrorCategory.UNEXPECTED,
            error_code=ErrorCode.UNEXPECTED_EXCEPTION,
            message="operator review marker",
        ),
    )

    payload = live_runtime_gate_payload(config, approved=True)
    checks = {check["name"]: check for check in payload["checks"]}
    ledger_check = checks[ReadinessCheckName.LEDGER_HEALTH.value]

    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["runtime_would_start"] is False
    assert ledger_check["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert ledger_check["details"]["health_status"] == (
        LedgerHealthStatus.ATTENTION_REQUIRED.value
    )
    assert ledger_check["details"]["attention_checks"] == [
        {
            "count": 1,
            "name": LedgerHealthCheckName.ERROR_EVENTS.value,
            "status": LedgerHealthStatus.ATTENTION_REQUIRED.value,
        }
    ]
    with pytest.raises(ConfigError, match=ReadinessCheckName.LEDGER_HEALTH.value):
        enforce_live_runtime_gate(config, approved=True)


def test_live_runtime_gate_accepts_matching_ledger_health_acknowledgement(workspace_tmp_path):
    config = _live_config(workspace_tmp_path / "acknowledged-ledger-health-live-gate.jsonl")
    record_live_no_order_preflight_result(config, _clean_live_preflight_payload())
    core = AuditCore(AuditLedger(config.ledger_path))
    core.emit(
        EventType.ERROR,
        error_event_payload(
            category=ErrorCategory.UNEXPECTED,
            error_code=ErrorCode.UNEXPECTED_EXCEPTION,
            message="operator review marker",
        ),
    )
    acknowledgement_payload = acknowledge_ledger_health(
        config.ledger_path,
        acknowledged_by="operator-1",
        reason="reviewed preflight marker before live startup",
    )

    payload = live_runtime_gate_payload(config, approved=True)
    checks = {check["name"]: check for check in payload["checks"]}
    ledger_check = checks[ReadinessCheckName.LEDGER_HEALTH.value]

    assert acknowledgement_payload["acknowledgement_status"]["acknowledged"] is True
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["runtime_would_start"] is True
    assert ledger_check["status"] == ReadinessStatus.OK.value
    assert ledger_check["details"]["health_status"] == (
        LedgerHealthStatus.ATTENTION_REQUIRED.value
    )
    assert ledger_check["details"]["acknowledgement"]["acknowledged"] is True
    enforce_live_runtime_gate(config, approved=True)

    core.emit(EventType.ACTION_REQUESTED, {"action_id": "post-ack-action"})
    stale_payload = live_runtime_gate_payload(config, approved=True)
    stale_checks = {check["name"]: check for check in stale_payload["checks"]}
    stale_ack = stale_checks[ReadinessCheckName.LEDGER_HEALTH.value]["details"][
        "acknowledgement"
    ]

    assert stale_payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert stale_ack["acknowledged"] is False
    assert (
        LedgerHealthAcknowledgementIssue.STALE_AFTER_ACKNOWLEDGEMENT.value
        in stale_ack["latest_issues"]
    )


def test_live_runtime_gate_requires_strategy_qualification_when_strategies_are_enabled(
    workspace_tmp_path,
):
    config = _live_config(
        workspace_tmp_path / "strategy-live-gate.jsonl",
        strategies_enabled=True,
    )
    record_live_no_order_preflight_result(config, _clean_live_preflight_payload())

    missing_payload = live_runtime_gate_payload(config, approved=True)
    missing_checks = {check["name"]: check for check in missing_payload["checks"]}

    assert missing_payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert StrategySimulationGateIssue.MISSING.value in missing_checks[
        ReadinessCheckName.STRATEGY_SIMULATION.value
    ]["details"]["attention_reasons"]

    simulation = strategy_simulation_payload(config)
    record_strategy_simulation_result(config, simulation)
    clean_payload = live_runtime_gate_payload(config, approved=True)

    assert clean_payload["status"] == ReadinessStatus.OK.value
    assert clean_payload["runtime_would_start"] is True


def test_cli_live_runtime_gate_can_fail_on_attention_without_runtime_writes(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_bot_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-live-gate.jsonl"
    monkeypatch.setenv("STATERAIL_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=None,
                live_runtime_gate=True,
                live_runtime_gate_fail_on_attention=True,
                max_cycles=99,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert records == ()


def test_cli_ledger_health_acknowledgement_appends_review_record(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_bot_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-ledger-health-acknowledgement.jsonl"
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.ERROR,
        error_event_payload(
            category=ErrorCategory.UNEXPECTED,
            error_code=ErrorCode.UNEXPECTED_EXCEPTION,
            message="operator review marker",
        ),
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_health_acknowledge=True,
                ledger_health_acknowledged_by="operator-1",
                ledger_health_acknowledgement_reason="reviewed before live startup",
                ledger_path=str(ledger_path),
                max_cycles=99,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()

    assert exit_code == 0
    assert records[-1].event_type == EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED
    assert payload["acknowledgement_status"]["acknowledged"] is True
    assert payload["acknowledgement"]["acknowledged_by"] == "operator-1"


def test_ledger_health_reports_malformed_ledger_health_acknowledgement(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "malformed-ledger-health-acknowledgement.jsonl"
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED,
        {
            "acknowledged_by": "",
            "acknowledged_health_status": LedgerHealthStatus.OK.value,
            "acknowledged_through_hash": "not-a-sha256",
            "acknowledged_through_sequence": 0,
            "attention_check_count": 2,
            "attention_checks": [
                {
                    "count": -1,
                    "name": "not_a_health_check",
                    "status": "not_a_health_status",
                }
            ],
            "ledger_health_attention_digest": "not-a-sha256",
            "ledger_path": "",
            "reason": "",
            "schema_version": 999,
        },
    )

    payload = ledger_health_payload(ledger_path)
    checks = {check["name"]: check for check in payload["checks"]}
    acknowledgement_check = checks[
        LedgerHealthCheckName.LEDGER_HEALTH_ACKNOWLEDGEMENT_CONTRACT.value
    ]

    assert payload["status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert acknowledgement_check["status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert acknowledgement_check["count"] == 1
    assert acknowledgement_check["details"]["acknowledgement_count"] == 1
    assert acknowledgement_check["details"]["anomalies"][0]["invalid_fields"] == [
        "acknowledged_by",
        "acknowledged_health_status",
        "acknowledged_through_hash",
        "acknowledged_through_sequence",
        "attention_check_count",
        "attention_checks",
        "attention_checks.count",
        "attention_checks.name",
        "attention_checks.status",
        "ledger_health_attention_digest",
        "ledger_path",
        "reason",
        "schema_version",
    ]


def _live_config(
    ledger_path,
    *,
    strategies_enabled: bool = False,
) -> CoinbaseApplicationConfig:
    return CoinbaseApplicationConfig(
        ledger_path=ledger_path,
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
                    run_on_start=False,
                ),
                product_ids=("BTC-USD",),
            ),
            strategies=StrategyRuntimeConfig(
                allow_live_execution=strategies_enabled,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=strategies_enabled,
                    run_on_start=False,
                ),
                strategy_ids=("noop",) if strategies_enabled else (),
            ),
        ),
    )


def _clean_live_preflight_payload() -> dict[str, object]:
    return {
        "completed_step_names": [
            PreflightStep.READINESS.value,
            PreflightStep.PRODUCT_CATALOG_SMOKE.value,
            PreflightStep.FEED_SMOKE.value,
            PreflightStep.EXCHANGE_STATE_SMOKE.value,
        ],
        "order_endpoint_called": False,
        "runtime_tasks_started": False,
        "skipped_step_names": [],
        "status": ReadinessStatus.OK.value,
        "steps": [
            {"name": PreflightStep.READINESS.value, "status": ReadinessStatus.OK.value},
            {"name": PreflightStep.PRODUCT_CATALOG_SMOKE.value, "status": ReadinessStatus.OK.value},
            {"name": PreflightStep.FEED_SMOKE.value, "status": ReadinessStatus.OK.value},
            {"name": PreflightStep.EXCHANGE_STATE_SMOKE.value, "status": ReadinessStatus.OK.value},
        ],
        "stopped_after_step": None,
        "strategy_tasks_started": False,
        "writes_ledger": True,
    }


def _clear_coinbase_bot_env(monkeypatch) -> None:
    for key in list(os.environ):
        if key.startswith("STATERAIL_") or key.startswith("COINBASE_BOT_"):
            monkeypatch.delenv(key, raising=False)
