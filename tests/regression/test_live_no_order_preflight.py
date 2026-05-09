from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import AsyncIterator, Mapping
from typing import Any

from app.credentials import (
    COINBASE_SDK_API_KEY_ENV,
    COINBASE_SDK_API_SECRET_ENV,
    CoinbaseRuntimeCredentialProviders,
)
from app.config_loading import load_coinbase_application_config_from_json_file
from app.ledger_health import ledger_health_payload
from app.ledger_summary import ledger_summary_payload
from app.live_safety import LIVE_TRADING_APPROVAL_ENV
from app.live_preflight_gate import live_no_order_preflight_gate_payload
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    EventType,
    ExecutionMode,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    PreflightStep,
    ProductType,
    ProductVenue,
    ReadinessStatus,
)
from exchanges.coinbase.advanced_trade_rest import HttpResponse
from exchanges.coinbase.auth import static_token_provider
from feeds.router import FeedMessage
from products.catalog import ProductCatalog


class FakeProductCatalogClient:
    def __init__(self, catalog: ProductCatalog) -> None:
        self._catalog = catalog
        self.calls: list[dict[str, object]] = []

    def list_products(
        self,
        *,
        product_ids: tuple[str, ...] = (),
        get_tradability_status: bool = True,
    ) -> ProductCatalog:
        self.calls.append(
            {
                "get_tradability_status": get_tradability_status,
                "product_ids": product_ids,
            }
        )
        return self._catalog


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
        raise AssertionError("live no-order preflight must not call POST endpoints")


class WaitingFeedSource:
    def __init__(self, source_id: str, messages: tuple[FeedMessage, ...]) -> None:
        self._source_id = source_id
        self._messages = messages

    @property
    def source_id(self) -> str:
        return self._source_id

    async def stream(self) -> AsyncIterator[FeedMessage]:
        for message in self._messages:
            await asyncio.sleep(0)
            yield message
        await asyncio.Event().wait()


def test_cli_live_no_order_preflight_runs_checks_in_order_without_orders(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setattr(
        "app.main.load_coinbase_runtime_credentials_from_env",
        lambda: CoinbaseRuntimeCredentialProviders(
            jwt_factory=lambda _payload: "test-jwt",
            token_provider=static_token_provider("test-token")
        ),
    )
    ledger_path = workspace_tmp_path / "live-no-order-preflight.jsonl"
    config_path = workspace_tmp_path / "config.json"
    product_ids = ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "feed": {"min_live_sources": 2, "stale_after_seconds": 30},
                    "websocket_sources": [
                        {
                            "channels": [CoinbaseWebSocketChannel.LEVEL2.value],
                            "endpoint": CoinbaseWebSocketEndpoint.MARKET_DATA.value,
                            "product_ids": list(product_ids),
                            "source_id": "coinbase-cfm-market-primary",
                        },
                        {
                            "channels": [CoinbaseWebSocketChannel.LEVEL2.value],
                            "endpoint": CoinbaseWebSocketEndpoint.MARKET_DATA.value,
                            "product_ids": list(product_ids),
                            "source_id": "coinbase-cfm-market-secondary",
                            },
                            {
                                "channels": [CoinbaseWebSocketChannel.USER.value],
                                "endpoint": CoinbaseWebSocketEndpoint.USER_ORDER_DATA.value,
                                "product_ids": list(product_ids),
                                "source_id": "coinbase-user-primary",
                            },
                            {
                                "channels": [CoinbaseWebSocketChannel.USER.value],
                                "endpoint": CoinbaseWebSocketEndpoint.USER_ORDER_DATA.value,
                                "product_ids": list(product_ids),
                                "source_id": "coinbase-user-secondary",
                            },
                        ],
                    "product_catalog": {
                        "enabled": True,
                        "product_ids": list(product_ids),
                    },
                    "reconciliation": {
                        "exchange_state": {
                            "position_product_ids": list(product_ids),
                        }
                    },
                    "risk": {
                        "allowed_order_types": ["limit"],
                        "allowed_products": list(product_ids),
                        "max_order_size": "1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    product_catalog_client = FakeProductCatalogClient(
        ProductCatalog.from_coinbase_payloads(
            [_product_payload(product_id) for product_id in product_ids]
        )
    )
    transport = FakeTransport(
        [
            _accounts_response(),
            _cfm_positions_response(product_id=product_ids[0], number_of_contracts="0"),
        ]
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                live_no_order_preflight=True,
                live_no_order_preflight_fail_on_attention=True,
                live_no_order_preflight_feed_seconds=0.01,
                max_cycles=99,
            ),
            product_catalog_client=product_catalog_client,
            transport=transport,
            websocket_source_factory=lambda source_config: WaitingFeedSource(
                source_config.source_id,
                (
                    FeedMessage(
                        source_config.source_id,
                        "SHB-26JUN26-CDE:level2:1",
                        EventType.DATA_RECEIVED,
                        {"sequence": 1},
                    ),
                ),
            ),
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()
    event_types = [record.event_type for record in records]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["completed_step_names"] == [
        PreflightStep.READINESS.value,
        PreflightStep.PRODUCT_CATALOG_SMOKE.value,
        PreflightStep.FEED_SMOKE.value,
        PreflightStep.EXCHANGE_STATE_SMOKE.value,
    ]
    assert payload["skipped_step_names"] == []
    assert payload["order_endpoint_called"] is False
    assert payload["runtime_tasks_started"] is False
    assert payload["strategy_tasks_started"] is False
    assert payload["preflight_result_sequence"] == records[-1].sequence
    assert [step["status"] for step in payload["steps"]] == [ReadinessStatus.OK.value] * 4
    assert product_catalog_client.calls == [
        {"get_tradability_status": True, "product_ids": product_ids}
    ]
    assert transport.posts == []
    assert [request["url"] for request in transport.gets] == [
        "https://api.coinbase.com/api/v3/brokerage/accounts",
        "https://api.coinbase.com/api/v3/brokerage/cfm/positions",
    ]
    assert EventType.EXCHANGE_PRODUCT_SNAPSHOT in event_types
    assert EventType.EXCHANGE_BALANCE_SNAPSHOT in event_types
    assert EventType.EXCHANGE_POSITION_SNAPSHOT in event_types
    assert EventType.RUNTIME_TASK_STARTED not in event_types
    assert EventType.STRATEGY_EVALUATION_STARTED not in event_types
    assert EventType.ACTION_REQUESTED not in event_types
    assert EventType.ACTION_EXECUTION_STARTED not in event_types
    assert records[-1].event_type == EventType.LIVE_PREFLIGHT_RESULT

    gate_payload = live_no_order_preflight_gate_payload(
        load_coinbase_application_config_from_json_file(config_path)
    )
    assert gate_payload["status"] == ReadinessStatus.OK.value
    assert gate_payload["matching_result"]["sequence"] == records[-1].sequence
    health = ledger_health_payload(ledger_path)
    checks = {check["name"]: check for check in health["checks"]}
    assert checks[LedgerHealthCheckName.LIVE_PREFLIGHT_CONTRACT.value]["status"] == (
        LedgerHealthStatus.OK.value
    )
    summary = ledger_summary_payload(ledger_path)
    assert summary["live_preflight_result_count"] == 1
    assert summary["latest_live_preflight_sequence"] == records[-1].sequence


def test_cli_live_no_order_preflight_stops_on_readiness_attention_without_writes(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "live-no-order-preflight-attention.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "product_catalog": {
                        "enabled": True,
                        "product_ids": ["SHB-26JUN26-CDE"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    product_catalog_client = FakeProductCatalogClient(ProductCatalog())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                live_no_order_preflight=True,
                live_no_order_preflight_fail_on_attention=True,
                live_no_order_preflight_feed_seconds=0.01,
                max_cycles=99,
            ),
            product_catalog_client=product_catalog_client,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["preflight_result_sequence"] is None
    assert payload["completed_step_names"] == [PreflightStep.READINESS.value]
    assert payload["skipped_step_names"] == [
        PreflightStep.PRODUCT_CATALOG_SMOKE.value,
        PreflightStep.FEED_SMOKE.value,
        PreflightStep.EXCHANGE_STATE_SMOKE.value,
    ]
    assert payload["stopped_after_step"] == PreflightStep.READINESS.value
    assert payload["writes_ledger"] is False
    assert product_catalog_client.calls == []
    assert not ledger_path.exists()


def test_cli_live_no_order_preflight_can_allow_reviewed_config_fingerprint_mismatch(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setattr(
        "app.main.load_coinbase_runtime_credentials_from_env",
        lambda: CoinbaseRuntimeCredentialProviders(
            jwt_factory=lambda _payload: "test-jwt",
            token_provider=static_token_provider("test-token"),
        ),
    )
    ledger_path = workspace_tmp_path / "live-no-order-preflight-config-transition.jsonl"
    original_config_path = workspace_tmp_path / "original-config.json"
    product_ids = ("SHB-26JUN26-CDE",)
    original_config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                    "risk": {
                        "allowed_order_types": ["limit"],
                        "allowed_products": list(product_ids),
                        "max_order_size": "1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(original_config_path),
                ledger_path=None,
                max_cycles=1,
            )
        )
    )
    capsys.readouterr()

    release_config_path = workspace_tmp_path / "release-config.json"
    release_config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "feed": {"min_live_sources": 2, "stale_after_seconds": 30},
                    "websocket_sources": [
                        {
                            "channels": [CoinbaseWebSocketChannel.LEVEL2.value],
                            "endpoint": CoinbaseWebSocketEndpoint.MARKET_DATA.value,
                            "product_ids": list(product_ids),
                            "source_id": "coinbase-cfm-market-primary",
                        },
                        {
                            "channels": [CoinbaseWebSocketChannel.LEVEL2.value],
                            "endpoint": CoinbaseWebSocketEndpoint.MARKET_DATA.value,
                            "product_ids": list(product_ids),
                            "source_id": "coinbase-cfm-market-secondary",
                        },
                        {
                            "channels": [CoinbaseWebSocketChannel.USER.value],
                            "endpoint": CoinbaseWebSocketEndpoint.USER_ORDER_DATA.value,
                            "product_ids": list(product_ids),
                            "source_id": "coinbase-user-primary",
                        },
                        {
                            "channels": [CoinbaseWebSocketChannel.USER.value],
                            "endpoint": CoinbaseWebSocketEndpoint.USER_ORDER_DATA.value,
                            "product_ids": list(product_ids),
                            "source_id": "coinbase-user-secondary",
                        },
                    ],
                    "product_catalog": {
                        "enabled": True,
                        "product_ids": list(product_ids),
                    },
                    "risk": {
                        "allowed_order_types": ["limit"],
                        "allowed_products": list(product_ids),
                        "max_order_size": "1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    product_catalog_client = FakeProductCatalogClient(ProductCatalog())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(release_config_path),
                ledger_path=None,
                live_no_order_preflight=True,
                live_no_order_preflight_fail_on_attention=True,
                live_no_order_preflight_feed_seconds=0.01,
                max_cycles=99,
                readiness_allow_config_fingerprint_mismatch=True,
            ),
            product_catalog_client=product_catalog_client,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    readiness_step = payload["steps"][0]
    config_check = next(
        check
        for check in readiness_step["payload"]["checks"]
        if check["name"] == "config_fingerprint"
    )
    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert readiness_step["status"] == ReadinessStatus.OK.value
    assert config_check["status"] == ReadinessStatus.OK.value
    assert config_check["details"]["ledger_config_fingerprint_matches"] is False
    assert config_check["details"]["ledger_config_fingerprint_mismatch_allowed"] is True
    assert payload["completed_step_names"] == [
        PreflightStep.READINESS.value,
        PreflightStep.PRODUCT_CATALOG_SMOKE.value,
    ]
    assert payload["stopped_after_step"] == PreflightStep.PRODUCT_CATALOG_SMOKE.value
    assert product_catalog_client.calls == [
        {"get_tradability_status": True, "product_ids": product_ids}
    ]


def test_ledger_health_reports_malformed_live_preflight_result(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "malformed-live-preflight.jsonl"
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.LIVE_PREFLIGHT_RESULT,
        {
            "completed_step_names": [PreflightStep.READINESS.value],
            "config_fingerprint": "",
            "fingerprint_algorithm": "sha512",
            "order_endpoint_called": True,
            "runtime_tasks_started": False,
            "schema_version": 99,
            "skipped_step_names": [],
            "status": "bad-status",
            "step_statuses": [{"name": PreflightStep.READINESS.value, "status": "bad-status"}],
            "strategy_tasks_started": False,
        },
    )

    payload = ledger_health_payload(ledger_path)
    checks = {check["name"]: check for check in payload["checks"]}
    check = checks[LedgerHealthCheckName.LIVE_PREFLIGHT_CONTRACT.value]

    assert check["status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert check["count"] == 1
    assert check["details"]["anomalies"][0]["sequence"] == 1


def _product_payload(product_id: str) -> dict[str, object]:
    return {
        "product_id": product_id,
        "product_type": ProductType.FUTURE.value,
        "product_venue": ProductVenue.FCM.value,
        "trading_disabled": False,
    }


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


def _cfm_positions_response(*, product_id: str, number_of_contracts: str) -> HttpResponse:
    return HttpResponse(
        status_code=200,
        body={
            "positions": [
                {
                    "avg_entry_price": "100000",
                    "current_price": "100100",
                    "number_of_contracts": number_of_contracts,
                    "product_id": product_id,
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
