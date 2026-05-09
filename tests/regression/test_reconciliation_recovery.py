from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
    ReconciliationIssue,
)
from exchanges.coinbase.advanced_trade_rest import CoinbaseOrderLookupResult
from projections.state import SourceOfTruthProjection
from reconciliation.recovery import ReconciliationRecovery
from reconciliation.watchdog import ReconciliationPolicy, ReconciliationWatchdog


class MutableClock:
    def __init__(self, current_time: datetime) -> None:
        self.current_time = current_time

    def now(self) -> datetime:
        return self.current_time

    def advance(self, delta: timedelta) -> None:
        self.current_time += delta


class LiveAcceptedExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            client_order_id=command.idempotency_key,
            exchange_order_id="exchange-1",
            raw_response={"accepted": True},
        )


class RaisingExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        raise RuntimeError(f"lost execution result for {command.action_id}")


class FakeOrderLookupClient:
    def __init__(self, results: list[CoinbaseOrderLookupResult]) -> None:
        self._results = results
        self.order_ids: list[str] = []

    def get_order(self, order_id: str) -> CoinbaseOrderLookupResult:
        self.order_ids.append(order_id)
        return self._results.pop(0)


def test_recovery_queries_rest_for_mismatch_and_applies_order_update(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeOrderLookupClient(
        [
            CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                order_update={
                    "client_order_id": "client-1",
                    "limit_price": "100000",
                    "order_id": "exchange-1",
                    "order_side": "BUY",
                    "order_type": "LIMIT",
                    "product_id": "BTC-USD",
                    "raw_rest_order": {"status": "OPEN"},
                    "status": "OPEN",
                },
                raw_response={"order": {"order_id": "exchange-1"}},
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    clock.advance(timedelta(seconds=31))
    ReconciliationWatchdog(
        core,
        clock=clock,
        policy=ReconciliationPolicy(user_confirmation_timeout=timedelta(seconds=30)),
    ).audit()

    results = ReconciliationRecovery(core, clock=clock, order_lookup_client=lookup_client).recover()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    order = projection.orders_by_action_id["place-1"]
    recovery = projection.reconciliation_recoveries[
        ("place-1", ReconciliationIssue.MISSING_USER_CONFIRMATION)
    ]

    assert lookup_client.order_ids == ["exchange-1"]
    assert len(results) == 1
    assert results[0].lookup_status == ExchangeLookupStatus.FOUND
    assert ledger.iter_records()[-1].event_type == EventType.RECONCILIATION_RECOVERY
    assert recovery.lookup_status == ExchangeLookupStatus.FOUND
    assert order.lifecycle_status == OrderLifecycleStatus.OPEN
    assert order.last_exchange_update["status"] == "OPEN"
    assert projection.reconciliation_recovery_count == 1


def test_recovery_audits_missing_execution_result_without_exchange_order_id(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeOrderLookupClient([])

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), RaisingExecutor())
    clock.advance(timedelta(seconds=31))
    ReconciliationWatchdog(core, clock=clock).audit()

    results = ReconciliationRecovery(core, clock=clock, order_lookup_client=lookup_client).recover()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    recovery = projection.reconciliation_recoveries[
        ("place-1", ReconciliationIssue.MISSING_EXECUTION_RESULT)
    ]

    assert lookup_client.order_ids == []
    assert len(results) == 1
    assert results[0].client_order_id == "client-1"
    assert results[0].lookup_status == ExchangeLookupStatus.FAILED
    assert results[0].error_code == "exchange_order_id_required"
    assert recovery.payload["client_order_id"] == "client-1"
    assert recovery.payload["exchange_order_id"] is None
    assert projection.reconciliation_recovery_count == 1


def test_recovery_does_not_query_same_mismatch_after_restart(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeOrderLookupClient(
        [
            CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.NOT_FOUND,
                status_code=404,
                raw_response={"message": "not found"},
                error_code="order_not_found",
                error_message="Coinbase order was not found",
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    clock.advance(timedelta(seconds=31))
    ReconciliationWatchdog(core, clock=clock).audit()
    assert len(ReconciliationRecovery(core, clock=clock, order_lookup_client=lookup_client).recover()) == 1

    restarted_core = AuditCore(AuditLedger(ledger.path, clock=clock))
    restarted_lookup_client = FakeOrderLookupClient([])

    assert ReconciliationRecovery(
        restarted_core,
        clock=clock,
        order_lookup_client=restarted_lookup_client,
    ).recover() == ()
    assert restarted_lookup_client.order_ids == []


def test_recovery_logs_failed_lookup_as_error(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeOrderLookupClient(
        [
            CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=500,
                raw_response={"message": "server error"},
                error_code="http_500",
                error_message="server error",
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    clock.advance(timedelta(seconds=31))
    ReconciliationWatchdog(core, clock=clock).audit()

    results = ReconciliationRecovery(core, clock=clock, order_lookup_client=lookup_client).recover()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert results[0].lookup_status == ExchangeLookupStatus.FAILED
    assert [record.event_type for record in ledger.iter_records()][-2:] == [
        EventType.RECONCILIATION_RECOVERY,
        EventType.ERROR,
    ]
    assert projection.error_count == 1


def test_recovery_logs_and_skips_invalid_order_update(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    lookup_client = FakeOrderLookupClient(
        [
            CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                order_update={
                    "client_order_id": "client-1",
                    "order_id": "exchange-1",
                },
                raw_response={"order": {"order_id": "exchange-1"}},
            )
        ]
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), LiveAcceptedExecutor())
    clock.advance(timedelta(seconds=31))
    ReconciliationWatchdog(core, clock=clock).audit()

    results = ReconciliationRecovery(core, clock=clock, order_lookup_client=lookup_client).recover()
    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    recovery = projection.reconciliation_recoveries[
        ("place-1", ReconciliationIssue.MISSING_USER_CONFIRMATION)
    ]

    assert results[0].lookup_status == ExchangeLookupStatus.FOUND
    assert [record.event_type for record in records][-2:] == [
        EventType.ERROR,
        EventType.RECONCILIATION_RECOVERY,
    ]
    assert records[-2].payload["error_code"] == ErrorCode.EXCHANGE_ORDER_UPDATE_INVALID.value
    assert set(records[-2].payload["error"]["context"]["missing_fields"]) == {"product_id", "status"}
    assert "order_update" not in recovery.payload
    assert projection.orders_by_action_id["place-1"].last_exchange_update == {}


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
