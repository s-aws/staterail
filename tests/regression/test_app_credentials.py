from __future__ import annotations

import argparse
import asyncio
import json
import os

import pytest

from app.config_loading import load_coinbase_application_config_from_env
from app.credentials import (
    COINBASE_API_KEY_NAME_ENV,
    COINBASE_API_PRIVATE_KEY_ENV,
    COINBASE_API_PRIVATE_KEY_FILE_ENV,
    COINBASE_SDK_API_KEY_ENV,
    COINBASE_SDK_API_SECRET_ENV,
    LEGACY_COINBASE_API_KEY_NAME_ENV,
    LEGACY_COINBASE_API_PRIVATE_KEY_ENV,
    has_coinbase_credentials_env,
    load_coinbase_jwt_credentials_from_env,
    load_coinbase_runtime_credentials_from_env,
)
from app.live_preflight_gate import record_live_no_order_preflight_result
from app.live_safety import LIVE_TRADING_APPROVAL_ENV
from app.main import run_from_args
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ErrorCategory,
    EventType,
    HttpMethod,
    PreflightStep,
    ReadinessCheckName,
    ReadinessRequirement,
    ReadinessStatus,
)
from core.errors import ConfigError
from exchanges.coinbase.auth import rest_auth_request


def test_coinbase_credentials_env_builds_rest_and_websocket_jwt_providers():
    rest_calls: list[tuple[str, str, str]] = []
    ws_calls: list[tuple[str, str]] = []
    env = {
        COINBASE_API_KEY_NAME_ENV: "organizations/org/apiKeys/key",
        COINBASE_API_PRIVATE_KEY_ENV: "-----BEGIN EC PRIVATE KEY-----\\nkey\\n-----END EC PRIVATE KEY-----\\n",
    }

    providers = load_coinbase_runtime_credentials_from_env(
        env,
        rest_jwt_builder=lambda jwt_uri, key_name, private_key: rest_calls.append(
            (jwt_uri, key_name, private_key)
        )
        or "rest-jwt",
        ws_jwt_builder=lambda key_name, private_key: ws_calls.append((key_name, private_key)) or "ws-jwt",
    )
    request = rest_auth_request(
        HttpMethod.GET,
        "https://api.coinbase.com/api/v3/brokerage/accounts?limit=1",
    )

    assert has_coinbase_credentials_env(env) is True
    assert providers.token_provider is not None
    assert providers.jwt_factory is not None
    assert providers.token_provider(request) == "rest-jwt"
    assert providers.jwt_factory({}) == "ws-jwt"
    assert rest_calls == [
        (
            "GET api.coinbase.com/api/v3/brokerage/accounts",
            "organizations/org/apiKeys/key",
            "-----BEGIN EC PRIVATE KEY-----\nkey\n-----END EC PRIVATE KEY-----\n",
        )
    ]
    assert ws_calls == [
        (
            "organizations/org/apiKeys/key",
            "-----BEGIN EC PRIVATE KEY-----\nkey\n-----END EC PRIVATE KEY-----\n",
        )
    ]


def test_coinbase_credentials_env_accepts_coinbase_sdk_names():
    credentials = load_coinbase_jwt_credentials_from_env(
        {
            COINBASE_SDK_API_KEY_ENV: "organizations/org/apiKeys/key",
            COINBASE_SDK_API_SECRET_ENV: "-----BEGIN EC PRIVATE KEY-----\nkey\n-----END EC PRIVATE KEY-----\n",
        }
    )

    assert has_coinbase_credentials_env({COINBASE_SDK_API_KEY_ENV: "organizations/org/apiKeys/key"}) is True
    assert credentials is not None
    assert credentials.api_key_name == "organizations/org/apiKeys/key"
    assert credentials.private_key == "-----BEGIN EC PRIVATE KEY-----\nkey\n-----END EC PRIVATE KEY-----\n"


def test_coinbase_credentials_env_can_read_private_key_file(workspace_tmp_path):
    key_path = workspace_tmp_path / "coinbase-key.pem"
    key_path.write_text("private-key", encoding="utf-8")

    credentials = load_coinbase_jwt_credentials_from_env(
        {
            COINBASE_API_KEY_NAME_ENV: "organizations/org/apiKeys/key",
            COINBASE_API_PRIVATE_KEY_FILE_ENV: str(key_path),
        }
    )

    assert credentials is not None
    assert credentials.private_key == "private-key"


def test_coinbase_credentials_env_rejects_partial_or_ambiguous_credentials():
    with pytest.raises(ConfigError, match=COINBASE_API_KEY_NAME_ENV):
        load_coinbase_jwt_credentials_from_env({COINBASE_API_PRIVATE_KEY_ENV: "private-key"})

    with pytest.raises(ConfigError, match="cannot both be set"):
        load_coinbase_jwt_credentials_from_env(
            {
                COINBASE_API_KEY_NAME_ENV: "organizations/org/apiKeys/key",
                COINBASE_API_PRIVATE_KEY_ENV: "private-key",
                COINBASE_API_PRIVATE_KEY_FILE_ENV: "private-key.pem",
            }
        )

    with pytest.raises(ConfigError, match="Conflicting Coinbase API key"):
        load_coinbase_jwt_credentials_from_env(
            {
                COINBASE_SDK_API_KEY_ENV: "organizations/org/apiKeys/key-a",
                COINBASE_API_KEY_NAME_ENV: "organizations/org/apiKeys/key-b",
                COINBASE_API_PRIVATE_KEY_ENV: "private-key",
            }
        )

    with pytest.raises(ConfigError, match="Conflicting Coinbase private key"):
        load_coinbase_jwt_credentials_from_env(
            {
                COINBASE_API_KEY_NAME_ENV: "organizations/org/apiKeys/key",
                COINBASE_SDK_API_SECRET_ENV: "private-key-a",
                COINBASE_API_PRIVATE_KEY_ENV: "private-key-b",
            }
        )


def test_coinbase_credentials_env_rejects_legacy_project_names():
    env = {
        LEGACY_COINBASE_API_KEY_NAME_ENV: "organizations/org/apiKeys/key",
        LEGACY_COINBASE_API_PRIVATE_KEY_ENV: "private-key",
    }

    assert has_coinbase_credentials_env(env) is True
    with pytest.raises(ConfigError, match="renamed to STATERAIL_COINBASE_"):
        load_coinbase_jwt_credentials_from_env(env)


def test_cli_readiness_detects_env_credentials_without_signing_tokens(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    ledger_path = workspace_tmp_path / "credential-readiness.jsonl"

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
    payload = json.loads(capsys.readouterr().out)
    checks = {check["name"]: check for check in payload["checks"]}

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert checks[ReadinessCheckName.CREDENTIALS.value]["status"] == ReadinessStatus.OK.value
    assert checks[ReadinessCheckName.CREDENTIALS.value]["details"]["token_provider_configured"] is True
    assert checks[ReadinessCheckName.LIVE_TRADING_APPROVAL.value]["status"] == (
        ReadinessStatus.ATTENTION_REQUIRED.value
    )
    assert not ledger_path.exists()


def test_cli_live_runtime_requires_explicit_operator_approval(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")

    ledger_path = workspace_tmp_path / "live-blocked.jsonl"

    with pytest.raises(ConfigError, match=LIVE_TRADING_APPROVAL_ENV):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_path=str(ledger_path),
                    max_cycles=1,
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.CONFIG.value
    assert records[0].payload["stage"] == "runtime_preflight"


def test_cli_live_runtime_requires_risk_controls_and_product_catalog(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")

    ledger_path = workspace_tmp_path / "live-missing-safety.jsonl"

    with pytest.raises(ConfigError, match=ReadinessRequirement.RISK_POLICY.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_path=str(ledger_path),
                    max_cycles=1,
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.CONFIG.value
    assert ReadinessRequirement.RISK_POLICY.value in records[0].payload["message"]
    assert ReadinessRequirement.PRODUCT_CATALOG.value in records[0].payload["message"]


def test_cli_live_runtime_requires_explicit_strategy_live_allowance(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_PRODUCTS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_ORDER_TYPES", "limit")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_ENABLED", "true")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_PRODUCT_IDS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_STRATEGIES_ENABLED", "true")
    monkeypatch.setenv("STATERAIL_STRATEGY_IDS", "noop")

    ledger_path = workspace_tmp_path / "live-strategy-blocked.jsonl"

    with pytest.raises(ConfigError, match=ReadinessRequirement.STRATEGY_LIVE_APPROVAL.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_path=str(ledger_path),
                    max_cycles=1,
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.CONFIG.value
    assert ReadinessRequirement.STRATEGY_LIVE_APPROVAL.value in records[0].payload["message"]


def test_cli_live_runtime_requires_live_no_order_preflight(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "live-missing-preflight.jsonl"
    monkeypatch.setenv("STATERAIL_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_PRODUCTS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_ORDER_TYPES", "limit")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_ENABLED", "true")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_PRODUCT_IDS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_RUN_ON_START", "false")

    with pytest.raises(ConfigError, match=ReadinessRequirement.LIVE_NO_ORDER_PREFLIGHT.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_path=None,
                    max_cycles=1,
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.CONFIG.value
    assert ReadinessRequirement.LIVE_NO_ORDER_PREFLIGHT.value in records[0].payload["message"]


def test_cli_live_runtime_requires_strategy_simulation_when_strategies_enabled(
    workspace_tmp_path,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "live-missing-strategy-simulation.jsonl"
    monkeypatch.setenv("STATERAIL_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_PRODUCTS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_ORDER_TYPES", "limit")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_ENABLED", "true")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_PRODUCT_IDS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_RUN_ON_START", "false")
    monkeypatch.setenv("STATERAIL_STRATEGIES_ENABLED", "true")
    monkeypatch.setenv("STATERAIL_STRATEGIES_RUN_ON_START", "false")
    monkeypatch.setenv("STATERAIL_STRATEGY_IDS", "noop")
    monkeypatch.setenv("STATERAIL_STRATEGIES_ALLOW_LIVE_EXECUTION", "true")
    config = load_coinbase_application_config_from_env()
    record_live_no_order_preflight_result(config, _clean_live_preflight_payload())

    with pytest.raises(ConfigError, match=ReadinessRequirement.STRATEGY_SIMULATION.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_path=None,
                    max_cycles=1,
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_category"] == ErrorCategory.CONFIG.value
    assert ReadinessRequirement.STRATEGY_SIMULATION.value in records[-1].payload["message"]


def test_cli_live_runtime_blocks_unreplaced_config_placeholders(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    ledger_path = workspace_tmp_path / "live-placeholder-blocked.jsonl"

    with pytest.raises(ConfigError, match=ReadinessRequirement.CONFIG_PLACEHOLDERS.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file="docs/examples/config.cfm-live.json",
                    ledger_path=str(ledger_path),
                    max_cycles=1,
                )
            )
        )

    records = AuditLedger(ledger_path).iter_records()
    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.CONFIG.value
    assert ReadinessRequirement.CONFIG_PLACEHOLDERS.value in records[0].payload["message"]


def test_cli_live_runtime_runs_when_explicitly_approved(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "live-approved.jsonl"
    monkeypatch.setenv("STATERAIL_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("STATERAIL_EXECUTION_MODE", "live")
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_PRODUCTS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_RISK_ALLOWED_ORDER_TYPES", "limit")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_ENABLED", "true")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_PRODUCT_IDS", "BTC-USD")
    monkeypatch.setenv("STATERAIL_PRODUCT_CATALOG_RUN_ON_START", "false")
    config = load_coinbase_application_config_from_env()
    record_live_no_order_preflight_result(config, _clean_live_preflight_payload())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=None,
                max_cycles=1,
            )
        )
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert ledger_path.exists()
    assert "completed_cycles=1" in output


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


def test_ledger_read_only_cli_does_not_load_coinbase_credentials(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "summary.jsonl"
    AuditCore(AuditLedger(ledger_path)).emit(EventType.SYSTEM_STARTED)
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key-without-key-name")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                ledger_summary=True,
                max_cycles=99,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["verified"] is True
    assert payload["record_count"] == 1


def _clear_coinbase_env(monkeypatch) -> None:
    for key in list(os.environ):
        if (
            key.startswith("STATERAIL_")
            or key.startswith("COINBASE_BOT_")
            or key in {COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV}
        ):
            monkeypatch.delenv(key, raising=False)
