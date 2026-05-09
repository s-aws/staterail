from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from app.bootstrap import CoinbaseApplicationConfig
from app.credentials import (
    COINBASE_SDK_API_KEY_ENV,
    COINBASE_SDK_API_SECRET_ENV,
    CoinbaseRuntimeCredentialProviders,
)
from app.exchange_state_smoke import exchange_state_smoke_payload
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from audit.ledger import AuditLedger
from config.assembly import CoinbaseBotConfig, CoinbaseRestApiConfig
from core.enums import EventType, ExecutionMode, ProductVenue, ReadinessStatus, RuntimeComponent
from core.errors import ConfigError
from exchanges.coinbase.advanced_trade_rest import HttpResponse
from exchanges.coinbase.auth import static_token_provider


class FakeTransport:
    def __init__(self, get_responses: list[HttpResponse]) -> None:
        self._get_responses = get_responses
        self.gets: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        self.gets.append({"headers": dict(headers), "query_params": query_params, "url": url})
        return self._get_responses.pop(0)

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        self.posts.append({"headers": dict(headers), "json_body": dict(json_body), "url": url})
        raise AssertionError("exchange-state smoke must not call POST endpoints")


def test_exchange_state_smoke_snapshots_state_without_runtime_or_order_tasks(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "exchange-state-smoke.jsonl"
    transport = FakeTransport(
        [
            _accounts_response(),
            _cfm_positions_response(number_of_contracts="0"),
        ]
    )

    payload = exchange_state_smoke_payload(
        _config(ledger_path),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )
    records = AuditLedger(ledger_path).iter_records()

    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["result"] == {
        "balance_snapshots": 1,
        "drift_count": 0,
        "error_count": 0,
        "new_drift_record_count": 0,
        "position_snapshots": 1,
    }
    assert payload["order_endpoint_called"] is False
    assert payload["runtime_tasks_started"] is False
    assert payload["strategy_tasks_started"] is False
    assert payload["websocket_started"] is False
    assert transport.posts == []
    assert [request["url"] for request in transport.gets] == [
        "https://api.coinbase.com/api/v3/brokerage/accounts",
        "https://api.coinbase.com/api/v3/brokerage/cfm/positions",
    ]
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.EXCHANGE_BALANCE_SNAPSHOT,
        EventType.EXCHANGE_POSITION_SNAPSHOT,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[0].payload["component"] == RuntimeComponent.EXCHANGE_STATE_SMOKE.value
    assert records[-1].payload["component"] == RuntimeComponent.EXCHANGE_STATE_SMOKE.value


def test_cli_exchange_state_smoke_can_fail_on_attention(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "exchange-state-smoke-attention.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "app.main.load_coinbase_runtime_credentials_from_env",
        lambda: CoinbaseRuntimeCredentialProviders(
            token_provider=static_token_provider("test-token")
        ),
    )
    transport = FakeTransport(
        [
            _accounts_response(),
            _cfm_positions_response(number_of_contracts="2"),
        ]
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                exchange_state_smoke=True,
                exchange_state_smoke_fail_on_attention=True,
                ledger_path=None,
                max_cycles=99,
            ),
            transport=transport,
        )
    )
    payload = json.loads(capsys.readouterr().out)
    event_types = [record.event_type for record in AuditLedger(ledger_path).iter_records()]

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["attention_reasons"] == ["position_drift"]
    assert payload["result"]["drift_count"] == 1
    assert payload["result"]["new_drift_record_count"] == 1
    assert EventType.RECONCILIATION_DRIFT in event_types
    assert transport.posts == []


def test_exchange_state_smoke_rejects_unresolved_placeholders_before_rest_calls(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "exchange-state-smoke-placeholder.jsonl"
    transport = FakeTransport([_accounts_response(), _cfm_positions_response(number_of_contracts="0")])
    config = _config(ledger_path, retail_portfolio_id="REPLACE_WITH_RETAIL_PORTFOLIO_ID")

    with pytest.raises(ConfigError, match="REPLACE_WITH_"):
        exchange_state_smoke_payload(
            config,
            token_provider=static_token_provider("test-token"),
            transport=transport,
        )

    records = AuditLedger(ledger_path).iter_records()

    assert transport.gets == []
    assert transport.posts == []
    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["stage"] == "exchange_state_smoke"
    assert records[0].payload["placeholder_paths"] == ["$.bot.rest.retail_portfolio_id"]


def _config(
    ledger_path: Path,
    *,
    retail_portfolio_id: str | None = None,
) -> CoinbaseApplicationConfig:
    return CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(
                execution_mode=ExecutionMode.LIVE,
                retail_portfolio_id=retail_portfolio_id,
            ),
        ),
    )


def _accounts_response() -> HttpResponse:
    return HttpResponse(
        status_code=200,
        body={
            "accounts": [
                {
                    "available_balance": {"currency": "USDC", "value": "10"},
                    "hold": {"currency": "USDC", "value": "0"},
                    "ready": True,
                    "type": "ACCOUNT_TYPE_CRYPTO",
                    "uuid": "account-1",
                }
            ],
            "has_next": False,
        },
    )


def _cfm_positions_response(*, number_of_contracts: str) -> HttpResponse:
    return HttpResponse(
        status_code=200,
        body={
            "positions": [
                {
                    "avg_entry_price": "100000",
                    "current_price": "100100",
                    "number_of_contracts": number_of_contracts,
                    "product_id": "BIT-29MAY26-CDE",
                    "side": "LONG",
                }
            ]
        },
    )


def _clear_coinbase_env(monkeypatch) -> None:
    for key in list(os.environ):
        if (
            key.startswith("STATERAIL_")
            or key.startswith("COINBASE_BOT_")
            or key in {COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV}
        ):
            monkeypatch.delenv(key, raising=False)
