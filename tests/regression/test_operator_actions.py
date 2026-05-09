from __future__ import annotations

import argparse
import asyncio
import json

import pytest

from actions.gateway import PlaceOrderIntent
from app.bootstrap import build_coinbase_application
from app.config_loading import load_coinbase_application_config_from_json_file
from app.credentials import COINBASE_API_PRIVATE_KEY_ENV, CoinbaseRuntimeCredentialProviders
from app.live_safety import LIVE_TRADING_APPROVAL_ENV
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionRejectionReason,
    ActionStatus,
    ActionType,
    EventType,
    ExecutionMode,
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    ReadinessRequirement,
    ReadinessStatus,
    TimeInForce,
)
from core.errors import ConfigError
from exchanges.coinbase.advanced_trade_rest import HttpResponse
from exchanges.coinbase.auth import static_token_provider
from projections.state import SourceOfTruthProjection


def test_cli_operator_place_order_routes_through_gateway_and_executor(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-place.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD"],
                        "max_order_size": "2",
                        "max_order_notional": "1000",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_id="operator-1",
                operator_place_action_id="operator-place-1",
                operator_place_client_order_id="operator-client-1",
                operator_place_leverage=None,
                operator_place_limit_price="100",
                operator_place_margin_type=None,
                operator_place_order=True,
                operator_place_order_type=OrderType.LIMIT.value,
                operator_place_post_only=True,
                operator_place_product_id="BTC-USD",
                operator_place_reason="operator dry-run canary",
                operator_place_reduce_only=False,
                operator_place_side=OrderSide.BUY.value,
                operator_place_size="1",
                operator_place_time_in_force=TimeInForce.GOOD_UNTIL_CANCELLED.value,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()
    projection = SourceOfTruthProjection.from_records(records)
    order = projection.orders_by_action_id["operator-place-1"]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["submitted"] is True
    assert payload["writes_ledger"] is True
    assert payload["runtime_tasks_started"] is False
    assert payload["websocket_started"] is False
    assert payload["receipt"]["action_id"] == "operator-place-1"
    assert payload["receipt"]["action_type"] == ActionType.PLACE_ORDER.value
    assert payload["receipt"]["status"] == ActionStatus.EXECUTED.value
    assert payload["logical_order_id"] == "operator-place-1"
    assert payload["client_order_id"] == "operator-client-1"
    assert payload["exchange_order_id"] == order.exchange_order_id
    assert payload["order"]["action_id"] == "operator-place-1"
    assert payload["order"]["lifecycle_status"] == OrderLifecycleStatus.OPEN.value
    assert order.client_order_id == "operator-client-1"
    assert order.exchange_order_id is not None
    assert EventType.ACTION_REQUESTED in [record.event_type for record in records]
    assert EventType.ACTION_EXECUTED in [record.event_type for record in records]


def test_cli_operator_place_order_reports_risk_rejection(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-place-risk-rejected.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD"],
                        "max_order_size": "0.5",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_id="operator-1",
                operator_place_action_id="operator-place-rejected",
                operator_place_client_order_id="operator-client-rejected",
                operator_place_leverage=None,
                operator_place_limit_price="100",
                operator_place_margin_type=None,
                operator_place_order=True,
                operator_place_order_type=OrderType.LIMIT.value,
                operator_place_post_only=True,
                operator_place_product_id="BTC-USD",
                operator_place_reason="operator dry-run rejection test",
                operator_place_reduce_only=False,
                operator_place_side=OrderSide.BUY.value,
                operator_place_size="1",
                operator_place_time_in_force=TimeInForce.GOOD_UNTIL_CANCELLED.value,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["submitted"] is True
    assert payload["writes_ledger"] is True
    assert payload["exchange_order_id"] is None
    assert payload["logical_order_id"] is None
    assert payload["receipt"]["status"] == ActionStatus.REJECTED.value
    assert payload["receipt"]["rejection_reason"] == ActionRejectionReason.RISK_CHECK_FAILED.value
    assert payload["order"]["lifecycle_status"] == OrderLifecycleStatus.REJECTED.value
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_REJECTED,
    ]


def test_cli_operator_place_order_rejects_malformed_input_without_writing(
    workspace_tmp_path,
):
    ledger_path = workspace_tmp_path / "operator-place-malformed.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {"rest": {"execution_mode": ExecutionMode.DRY_RUN.value}},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="--operator-place-reason"):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=str(config_path),
                    ledger_path=None,
                    operator_id="operator-1",
                    operator_place_action_id="operator-place-malformed",
                    operator_place_client_order_id="operator-client-malformed",
                    operator_place_leverage=None,
                    operator_place_limit_price="100",
                    operator_place_margin_type=None,
                    operator_place_order=True,
                    operator_place_order_type=OrderType.LIMIT.value,
                    operator_place_post_only=True,
                    operator_place_product_id="BTC-USD",
                    operator_place_reason=None,
                    operator_place_reduce_only=False,
                    operator_place_side=OrderSide.BUY.value,
                    operator_place_size="1",
                    operator_place_time_in_force=TimeInForce.GOOD_UNTIL_CANCELLED.value,
                )
            )
        )

    assert not ledger_path.exists()


def test_cli_operator_place_order_uses_mocked_live_executor_path(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")
    monkeypatch.setattr(
        "app.main.load_coinbase_runtime_credentials_from_env",
        lambda: CoinbaseRuntimeCredentialProviders(
            token_provider=static_token_provider("test-token"),
        ),
    )
    ledger_path = workspace_tmp_path / "operator-place-live.jsonl"
    _write_product_snapshot(ledger_path, product_id="BTC-USD")
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD"],
                        "max_order_size": "2",
                        "max_order_notional": "1000",
                    },
                    "product_catalog": {
                        "enabled": True,
                        "product_ids": ["BTC-USD"],
                        "run_on_start": False,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    transport = _SuccessfulOrderTransport(exchange_order_id="exchange-live-1")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_id="operator-1",
                operator_place_action_id="operator-place-live",
                operator_place_client_order_id="operator-client-live",
                operator_place_leverage="1",
                operator_place_limit_price="100",
                operator_place_margin_type=None,
                operator_place_order=True,
                operator_place_order_type=OrderType.LIMIT.value,
                operator_place_post_only=True,
                operator_place_product_id="BTC-USD",
                operator_place_reason="operator live canary test",
                operator_place_reduce_only=False,
                operator_place_side=OrderSide.BUY.value,
                operator_place_size="1",
                operator_place_time_in_force=TimeInForce.GOOD_UNTIL_CANCELLED.value,
            ),
            transport=transport,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["receipt"]["status"] == ActionStatus.EXECUTED.value
    assert payload["client_order_id"] == "operator-client-live"
    assert payload["exchange_order_id"] == "exchange-live-1"
    assert len(transport.posts) == 1
    assert transport.posts[0]["json_body"]["client_order_id"] == "operator-client-live"
    assert transport.posts[0]["json_body"]["leverage"] == "1"
    assert transport.posts[0]["json_body"]["product_id"] == "BTC-USD"


def test_cli_operator_cancel_order_routes_through_gateway_and_executor(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-cancel.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD"],
                        "max_order_size": "1",
                        "max_order_notional": "1000",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    config = load_coinbase_application_config_from_json_file(config_path)
    application = build_coinbase_application(config)
    application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="place-1",
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
        ).to_command()
    )
    projection = SourceOfTruthProjection.from_ledger(application.ledger)
    exchange_order_id = projection.orders_by_action_id["place-1"].exchange_order_id
    assert exchange_order_id is not None

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_cancel_action_id="cancel-operator-1",
                operator_cancel_allow_untracked=False,
                operator_cancel_client_order_id=None,
                operator_cancel_exchange_order_id=exchange_order_id,
                operator_cancel_order=True,
                operator_cancel_reason="operator regression cleanup",
                operator_id="operator-1",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()
    updated = SourceOfTruthProjection.from_records(records)
    cancelled_order = updated.orders_by_action_id["place-1"]

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["submitted"] is True
    assert payload["writes_ledger"] is True
    assert payload["receipt"]["action_id"] == "cancel-operator-1"
    assert payload["receipt"]["status"] == ActionStatus.EXECUTED.value
    assert payload["matched_order"]["action_id"] == "place-1"
    assert cancelled_order.lifecycle_status == OrderLifecycleStatus.CANCELLED
    assert cancelled_order.cancel_action_ids == ["cancel-operator-1"]
    assert EventType.ACTION_REQUESTED in [record.event_type for record in records]
    assert EventType.ACTION_EXECUTED in [record.event_type for record in records]


def test_cli_operator_open_orders_lists_tracked_open_orders_without_writing(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-open-orders.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD", "ETH-USD"],
                        "max_order_size": "2",
                        "max_order_notional": "10000",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    config = load_coinbase_application_config_from_json_file(config_path)
    application = build_coinbase_application(config)
    application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="place-btc-1",
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
        ).to_command()
    )
    application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="place-eth-1",
            limit_price="1000",
            order_type=OrderType.LIMIT,
            product_id="ETH-USD",
            side=OrderSide.SELL,
            size="1",
        ).to_command()
    )
    before_records = AuditLedger(ledger_path).iter_records()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_open_orders=True,
                operator_open_orders_product_id="BTC-USD",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    after_records = AuditLedger(ledger_path).iter_records()

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["writes_ledger"] is False
    assert payload["open_order_count"] == 1
    assert payload["open_orders"][0]["action_id"] == "place-btc-1"
    assert payload["open_orders"][0]["exchange_order_id"] is not None
    assert payload["open_orders"][0]["product_id"] == "BTC-USD"
    assert after_records == before_records


def test_cli_operator_cancel_all_open_orders_routes_each_cancel_through_gateway(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-cancel-all.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD", "ETH-USD"],
                        "max_order_size": "2",
                        "max_order_notional": "10000",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    config = load_coinbase_application_config_from_json_file(config_path)
    application = build_coinbase_application(config)
    application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="place-btc-1",
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
        ).to_command()
    )
    application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="place-btc-2",
            limit_price="101",
            order_type=OrderType.LIMIT,
            product_id="BTC-USD",
            side=OrderSide.SELL,
            size="1",
        ).to_command()
    )
    application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="place-eth-1",
            limit_price="1000",
            order_type=OrderType.LIMIT,
            product_id="ETH-USD",
            side=OrderSide.BUY,
            size="1",
        ).to_command()
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_cancel_action_id=None,
                operator_cancel_action_id_prefix="cancel-all-regression",
                operator_cancel_allow_untracked=False,
                operator_cancel_all_open_orders=True,
                operator_cancel_client_order_id=None,
                operator_cancel_exchange_order_id=None,
                operator_cancel_order=False,
                operator_cancel_product_id="BTC-USD",
                operator_cancel_reason="operator regression cleanup",
                operator_id="operator-1",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    updated = SourceOfTruthProjection.from_ledger(AuditLedger(ledger_path))

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["matched_open_order_count"] == 2
    assert payload["submitted_count"] == 2
    assert payload["failed_count"] == 0
    assert payload["remaining_open_order_count"] == 0
    assert payload["writes_ledger"] is True
    assert [result["action_id"] for result in payload["cancel_results"]] == [
        "cancel-all-regression-0001",
        "cancel-all-regression-0002",
    ]
    assert updated.orders_by_action_id["place-btc-1"].lifecycle_status == OrderLifecycleStatus.CANCELLED
    assert updated.orders_by_action_id["place-btc-2"].lifecycle_status == OrderLifecycleStatus.CANCELLED
    assert updated.orders_by_action_id["place-eth-1"].lifecycle_status == OrderLifecycleStatus.OPEN


def test_cli_operator_cancel_all_open_orders_noops_when_none_are_open(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-cancel-all-empty.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_cancel_action_id=None,
                operator_cancel_action_id_prefix="cancel-all-empty",
                operator_cancel_allow_untracked=False,
                operator_cancel_all_open_orders=True,
                operator_cancel_client_order_id=None,
                operator_cancel_exchange_order_id=None,
                operator_cancel_order=False,
                operator_cancel_product_id=None,
                operator_cancel_reason="operator regression cleanup",
                operator_id="operator-1",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["matched_open_order_count"] == 0
    assert payload["submitted_count"] == 0
    assert payload["writes_ledger"] is False
    assert not ledger_path.exists()


def test_cli_operator_cancel_order_blocks_untracked_order_without_writing(
    workspace_tmp_path,
    capsys,
):
    ledger_path = workspace_tmp_path / "operator-cancel-untracked.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.DRY_RUN.value},
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                operator_cancel_action_id="cancel-operator-1",
                operator_cancel_allow_untracked=False,
                operator_cancel_client_order_id=None,
                operator_cancel_exchange_order_id="missing-exchange-order",
                operator_cancel_order=True,
                operator_cancel_reason="operator regression cleanup",
                operator_id="operator-1",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["submitted"] is False
    assert payload["writes_ledger"] is False
    assert payload["matched_order"] is None
    assert not ledger_path.exists()


def test_cli_operator_cancel_live_requires_operator_approval(workspace_tmp_path, monkeypatch):
    monkeypatch.delenv(LIVE_TRADING_APPROVAL_ENV, raising=False)
    monkeypatch.setenv("COINBASE_API_KEY", "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    ledger_path = workspace_tmp_path / "operator-cancel-live-no-approval.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD"],
                        "max_order_size": "1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match=ReadinessRequirement.LIVE_TRADING_APPROVAL.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=str(config_path),
                    ledger_path=None,
                    operator_cancel_action_id="cancel-operator-1",
                    operator_cancel_allow_untracked=False,
                    operator_cancel_client_order_id=None,
                    operator_cancel_exchange_order_id="exchange-order-1",
                    operator_cancel_order=True,
                    operator_cancel_reason="operator regression cleanup",
                    operator_id="operator-1",
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert ReadinessRequirement.LIVE_TRADING_APPROVAL.value in records[0].payload["message"]


def test_cli_operator_cancel_all_live_requires_operator_approval(workspace_tmp_path, monkeypatch):
    monkeypatch.delenv(LIVE_TRADING_APPROVAL_ENV, raising=False)
    monkeypatch.setenv("COINBASE_API_KEY", "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    ledger_path = workspace_tmp_path / "operator-cancel-all-live-no-approval.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "risk": {
                        "allowed_order_types": [OrderType.LIMIT.value],
                        "allowed_products": ["BTC-USD"],
                        "max_order_size": "1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match=ReadinessRequirement.LIVE_TRADING_APPROVAL.value):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=str(config_path),
                    ledger_path=None,
                    operator_cancel_action_id=None,
                    operator_cancel_action_id_prefix="cancel-all-operator",
                    operator_cancel_allow_untracked=False,
                    operator_cancel_all_open_orders=True,
                    operator_cancel_client_order_id=None,
                    operator_cancel_exchange_order_id=None,
                    operator_cancel_order=False,
                    operator_cancel_product_id=None,
                    operator_cancel_reason="operator regression cleanup",
                    operator_id="operator-1",
                )
            )
        )
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert ReadinessRequirement.LIVE_TRADING_APPROVAL.value in records[0].payload["message"]


class _SuccessfulOrderTransport:
    def __init__(self, *, exchange_order_id: str) -> None:
        self._exchange_order_id = exchange_order_id
        self.posts: list[dict[str, object]] = []

    def get(self, url, *, headers, query_params=None):
        raise AssertionError(f"unexpected GET request: {url}")

    def post(self, url, *, headers, json_body):
        self.posts.append(
            {
                "headers": dict(headers),
                "json_body": dict(json_body),
                "url": url,
            }
        )
        return HttpResponse(
            status_code=200,
            body={
                "success": True,
                "success_response": {
                    "client_order_id": json_body["client_order_id"],
                    "order_id": self._exchange_order_id,
                },
            },
        )


def _write_product_snapshot(ledger_path, *, product_id: str) -> None:
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "configured_product_ids": [product_id],
            "product_count": 1,
            "product_ids": [product_id],
            "products": [
                {
                    "base_increment": "0.00000001",
                    "base_max_size": "100",
                    "base_min_size": "0.00000001",
                    "cancel_only": False,
                    "contract_size": None,
                    "is_disabled": False,
                    "limit_only": False,
                    "post_only": False,
                    "price_increment": "0.01",
                    "product_id": product_id,
                    "product_type": ProductType.SPOT.value,
                    "product_venue": ProductVenue.CBE.value,
                    "quote_increment": "0.01",
                    "quote_max_size": "1000000",
                    "quote_min_size": "1",
                    "raw": {
                        "base_increment": "0.00000001",
                        "price_increment": "0.01",
                        "product_id": product_id,
                        "product_type": ProductType.SPOT.value,
                        "product_venue": ProductVenue.CBE.value,
                    },
                    "trading_disabled": False,
                    "tradable_for_new_orders": True,
                    "view_only": False,
                }
            ],
            "refreshed_at": "2026-01-02T00:00:00+00:00",
        },
    )
