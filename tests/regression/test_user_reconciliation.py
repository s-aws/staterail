from __future__ import annotations

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionCommand, ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ErrorCode,
    EventType,
    ExchangeOrderStatus,
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
)
from exchanges.coinbase.advanced_trade_ws import CoinbaseMessageNormalizer
from feeds.router import RedundantFeedRouter
from projections.state import SourceOfTruthProjection


def test_projection_reconciles_accepted_user_order_update_to_existing_order(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    router = RedundantFeedRouter(core)

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), DryRunExecutor())
    normalizer = CoinbaseMessageNormalizer()
    for message in normalizer.normalize(
        "coinbase-primary",
        _user_message(sequence=40, status="FILLED", order_id="exchange-1", client_order_id="client-1"),
    ):
        router.ingest(message)

    projection = SourceOfTruthProjection.from_ledger(ledger)
    order = projection.orders_by_client_order_id["client-1"]
    assert order.lifecycle_status == OrderLifecycleStatus.FILLED
    assert order.exchange_status == ExchangeOrderStatus.FILLED
    assert order.exchange_order_id == "exchange-1"
    assert order.terminal_sequence == 8
    assert order.last_exchange_update["status"] == "FILLED"
    assert projection.open_orders == ()


def test_projection_does_not_apply_duplicate_user_updates_twice(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    router = RedundantFeedRouter(core)

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), DryRunExecutor())
    raw = _user_message(sequence=40, status="CANCELLED", order_id="exchange-1", client_order_id="client-1")
    primary_message = CoinbaseMessageNormalizer().normalize("coinbase-primary", raw)[0]
    secondary_message = CoinbaseMessageNormalizer().normalize("coinbase-secondary", raw)[0]

    assert router.ingest(primary_message) is True
    assert router.ingest(secondary_message) is False

    projection = SourceOfTruthProjection.from_ledger(ledger)
    order = projection.orders_by_client_order_id["client-1"]
    assert order.lifecycle_status == OrderLifecycleStatus.CANCELLED
    assert order.terminal_sequence == 8
    assert [record.event_type for record in ledger.iter_records()][-2:] == [
        EventType.DATA_RECEIVED,
        EventType.DATA_DUPLICATE,
    ]


def test_projection_creates_snapshot_order_for_user_channel_order_not_created_by_bot(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)

    message = CoinbaseMessageNormalizer().normalize(
        "coinbase-primary",
        _user_message(sequence=1, status="OPEN", order_id="exchange-unknown", client_order_id="client-unknown"),
    )[0]
    router.ingest(message)

    projection = SourceOfTruthProjection.from_ledger(ledger)
    order = projection.orders_by_exchange_order_id["exchange-unknown"]
    assert order.action_id == "client-unknown"
    assert order.lifecycle_status == OrderLifecycleStatus.OPEN
    assert order.product_id == "BTC-USD"
    assert order.side == OrderSide.BUY
    assert order.order_type == OrderType.LIMIT


def test_router_rejects_invalid_user_order_update_before_acceptance(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)

    message = CoinbaseMessageNormalizer().normalize(
        "coinbase-primary",
        {
            "channel": "user",
            "events": [
                {
                    "orders": [
                        {
                            "order_id": "exchange-1",
                        }
                    ],
                    "type": "update",
                }
            ],
            "sequence_num": 1,
            "timestamp": "2026-01-01T00:00:00Z",
        },
    )[0]

    assert router.ingest(message) is False
    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_code"] == ErrorCode.EXCHANGE_ORDER_UPDATE_INVALID.value
    assert set(records[-1].payload["error"]["context"]["missing_fields"]) == {"product_id", "status"}
    assert not any(record.event_type == EventType.DATA_ACCEPTED for record in records)
    assert projection.orders_by_action_id == {}


def _place_order(action_id: str, *, idempotency_key: str) -> ActionCommand:
    return PlaceOrderIntent(
        action_id=action_id,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="100000",
        idempotency_key=idempotency_key,
    ).to_command()


def _user_message(
    *,
    sequence: int,
    status: str,
    order_id: str,
    client_order_id: str,
) -> dict[str, object]:
    return {
        "channel": "user",
        "events": [
            {
                "orders": [
                    {
                        "avg_price": "100000",
                        "client_order_id": client_order_id,
                        "cumulative_quantity": "0.01",
                        "leaves_quantity": "0",
                        "limit_price": "100000",
                        "order_id": order_id,
                        "order_side": "BUY",
                        "order_type": "LIMIT",
                        "product_id": "BTC-USD",
                        "status": status,
                    }
                ],
                "type": "update",
            }
        ],
        "sequence_num": sequence,
        "timestamp": "2026-01-01T00:00:00Z",
    }
