from __future__ import annotations

from datetime import datetime, timezone

from actions.execution import ExecutionResult
from actions.gateway import ActionCommand, ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ErrorCode,
    EventType,
    ExchangeLookupStatus,
    ExecutionMode,
    ExecutionStatus,
    OrderSide,
    OrderType,
)
from exchanges.coinbase.advanced_trade_rest import CoinbaseFillsLookupResult
from projections.state import SourceOfTruthProjection
from reconciliation.fills import FillReconciliation


class FixedTestClock:
    def now(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=timezone.utc)


class LiveAcceptedExecutor:
    def __init__(self, *, exchange_order_id: str = "exchange-1") -> None:
        self._exchange_order_id = exchange_order_id

    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            client_order_id=command.idempotency_key,
            exchange_order_id=self._exchange_order_id,
            raw_response={"accepted": True},
        )


class DryRunAcceptedExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.DRY_RUN,
            client_order_id=command.idempotency_key,
            exchange_order_id="dry-run-1",
            raw_response={"accepted": True},
        )


class FakeFillLookupClient:
    def __init__(self, results: list[CoinbaseFillsLookupResult]) -> None:
        self._results = results
        self.requests: list[dict[str, object]] = []

    def list_fills(
        self,
        *,
        order_ids: tuple[str, ...],
        cursor: str | None = None,
        limit: int = 100,
    ) -> CoinbaseFillsLookupResult:
        self.requests.append({"cursor": cursor, "limit": limit, "order_ids": order_ids})
        return self._results.pop(0)


def test_fill_reconciliation_emits_fills_and_replays_position(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeFillLookupClient(
        [
            CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                fills=(
                    {
                        "commission": "1.25",
                        "fill_id": "fill-1",
                        "order_id": "exchange-1",
                        "price": "100000",
                        "product_id": "BTC-USD",
                        "side": "BUY",
                        "size": "0.01",
                        "trade_id": "trade-1",
                        "trade_time": "2026-01-01T00:00:00Z",
                    },
                ),
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    results = FillReconciliation(core, clock=FixedTestClock(), fill_lookup_client=lookup_client).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    order = projection.orders_by_action_id["place-1"]
    position = projection.positions_by_product_id["BTC-USD"]

    assert results[0].emitted_fill_ids == ("fill-1",)
    assert ledger.iter_records()[-1].event_type == EventType.EXCHANGE_FILL
    assert projection.fills_by_id["fill-1"].side == OrderSide.BUY
    assert order.fill_ids == ["fill-1"]
    assert order.filled_size == "0.01"
    assert order.average_fill_price == "100000"
    assert order.total_fees == "1.25"
    assert position.net_size == "0.01"
    assert position.gross_buy_notional == "1000"
    assert position.total_fees == "1.25"
    assert position.fill_count == 1


def test_fill_reconciliation_suppresses_duplicate_fill_events_after_restart(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    gateway = ActionGateway(core)

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    FillReconciliation(
        core,
        clock=FixedTestClock(),
        fill_lookup_client=FakeFillLookupClient([_fill_lookup_result("fill-1")]),
    ).reconcile()

    restarted_core = AuditCore(AuditLedger(ledger.path, clock=FixedTestClock()))
    duplicate_lookup_client = FakeFillLookupClient([_fill_lookup_result("fill-1")])
    results = FillReconciliation(
        restarted_core,
        clock=FixedTestClock(),
        fill_lookup_client=duplicate_lookup_client,
    ).reconcile()

    assert results[0].emitted_fill_ids == ()
    assert [
        record.event_type for record in ledger.iter_records() if record.event_type == EventType.EXCHANGE_FILL
    ] == [EventType.EXCHANGE_FILL]


def test_fill_reconciliation_logs_failed_lookup(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeFillLookupClient(
        [
            CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=500,
                raw_response={"message": "server error"},
                error_code="http_500",
                error_message="server error",
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    results = FillReconciliation(core, clock=FixedTestClock(), fill_lookup_client=lookup_client).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert results[0].lookup_status == ExchangeLookupStatus.FAILED
    assert ledger.iter_records()[-1].event_type == EventType.ERROR
    assert projection.error_count == 1


def test_fill_reconciliation_logs_and_skips_fill_without_fill_id(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeFillLookupClient(
        [
            CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                fills=(
                    {
                        "order_id": "exchange-1",
                        "price": "100000",
                        "product_id": "BTC-USD",
                        "side": "BUY",
                        "size": "0.01",
                    },
                ),
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    results = FillReconciliation(core, clock=FixedTestClock(), fill_lookup_client=lookup_client).reconcile()
    records = ledger.iter_records()

    assert results[0].emitted_fill_ids == ()
    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_code"] == ErrorCode.FILL_PAYLOAD_INVALID.value
    assert records[-1].payload["error"]["context"]["field"] == "fill_id"
    assert not any(record.event_type == EventType.EXCHANGE_FILL for record in records)


def test_fill_reconciliation_logs_and_skips_fill_for_different_order_id(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeFillLookupClient(
        [
            CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                fills=(
                    {
                        "fill_id": "fill-1",
                        "order_id": "different-exchange-1",
                        "price": "100000",
                        "product_id": "BTC-USD",
                        "side": "BUY",
                        "size": "0.01",
                    },
                ),
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    results = FillReconciliation(core, clock=FixedTestClock(), fill_lookup_client=lookup_client).reconcile()
    records = ledger.iter_records()

    assert results[0].emitted_fill_ids == ()
    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_code"] == ErrorCode.FILL_ORDER_MISMATCH.value
    assert records[-1].payload["error"]["context"]["expected_order_id"] == "exchange-1"
    assert records[-1].payload["error"]["context"]["observed_order_id"] == "different-exchange-1"
    assert not any(record.event_type == EventType.EXCHANGE_FILL for record in records)


def test_fill_reconciliation_ignores_dry_run_orders(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeFillLookupClient([])

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), DryRunAcceptedExecutor())

    assert FillReconciliation(core, clock=FixedTestClock(), fill_lookup_client=lookup_client).reconcile() == ()
    assert lookup_client.requests == []


def _fill_lookup_result(fill_id: str) -> CoinbaseFillsLookupResult:
    return CoinbaseFillsLookupResult(
        status=ExchangeLookupStatus.FOUND,
        status_code=200,
        fills=(
            {
                "commission": "1.25",
                "fill_id": fill_id,
                "order_id": "exchange-1",
                "price": "100000",
                "product_id": "BTC-USD",
                "side": "BUY",
                "size": "0.01",
            },
        ),
    )


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
