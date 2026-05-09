from __future__ import annotations

from datetime import datetime, timedelta, timezone

from actions.execution import ExecutionResult
from actions.gateway import ActionCommand, ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionType,
    EventType,
    ExecutionMode,
    ExecutionStatus,
    OrderSide,
    OrderType,
    ReconciliationIssue,
)
from exchanges.coinbase.advanced_trade_ws import CoinbaseMessageNormalizer
from feeds.router import RedundantFeedRouter
from projections.state import SourceOfTruthProjection
from reconciliation.watchdog import ReconciliationPolicy, ReconciliationWatchdog


class MutableClock:
    def __init__(self, current_time: datetime) -> None:
        self.current_time = current_time

    def now(self) -> datetime:
        return self.current_time

    def advance(self, delta: timedelta) -> None:
        self.current_time += delta


class LiveAcceptedExecutor:
    def __init__(self, *, client_order_id: str, exchange_order_id: str) -> None:
        self._client_order_id = client_order_id
        self._exchange_order_id = exchange_order_id

    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            client_order_id=self._client_order_id,
            exchange_order_id=self._exchange_order_id,
            raw_response={"accepted": True},
        )


class RaisingExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        raise RuntimeError(f"lost execution result for {command.action_id}")


def test_watchdog_audits_live_order_missing_user_confirmation_after_timeout(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    watchdog = ReconciliationWatchdog(
        core,
        clock=clock,
        policy=ReconciliationPolicy(user_confirmation_timeout=timedelta(seconds=30)),
    )

    gateway.submit_and_execute(
        _place_order("place-1", idempotency_key="client-1"),
        LiveAcceptedExecutor(client_order_id="client-1", exchange_order_id="exchange-1"),
    )
    clock.advance(timedelta(seconds=29))
    assert watchdog.audit() == ()

    clock.advance(timedelta(seconds=2))
    findings = watchdog.audit()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    mismatch = projection.reconciliation_mismatches[
        ("place-1", ReconciliationIssue.MISSING_USER_CONFIRMATION)
    ]

    assert len(findings) == 1
    assert ledger.iter_records()[-1].event_type == EventType.RECONCILIATION_MISMATCH
    assert mismatch.sequence == 7
    assert mismatch.payload["client_order_id"] == "client-1"
    assert mismatch.payload["exchange_order_id"] == "exchange-1"
    assert mismatch.payload["elapsed_seconds"] == 31.0
    assert projection.reconciliation_mismatch_count == 1


def test_watchdog_audits_missing_execution_result_after_timeout(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    watchdog = ReconciliationWatchdog(
        core,
        clock=clock,
        policy=ReconciliationPolicy(execution_result_timeout=timedelta(seconds=30)),
    )

    gateway.submit_and_execute(_place_order("place-1", idempotency_key="client-1"), RaisingExecutor())
    clock.advance(timedelta(seconds=29))
    assert watchdog.audit() == ()

    clock.advance(timedelta(seconds=2))
    findings = watchdog.audit()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    mismatch = projection.reconciliation_mismatches[
        ("place-1", ReconciliationIssue.MISSING_EXECUTION_RESULT)
    ]

    assert len(findings) == 1
    assert findings[0].reason == ReconciliationIssue.MISSING_EXECUTION_RESULT
    assert mismatch.sequence == 7
    assert mismatch.payload["client_order_id"] == "client-1"
    assert mismatch.payload["execution_started_sequence"] == 4
    assert "executed_sequence" not in mismatch.payload
    assert projection.reconciliation_mismatch_count == 1


def test_watchdog_does_not_emit_duplicate_mismatches_after_restart(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)

    gateway.submit_and_execute(
        _place_order("place-1", idempotency_key="client-1"),
        LiveAcceptedExecutor(client_order_id="client-1", exchange_order_id="exchange-1"),
    )
    clock.advance(timedelta(seconds=31))

    first_watchdog = ReconciliationWatchdog(core, clock=clock)
    assert len(first_watchdog.audit()) == 1

    restarted_core = AuditCore(AuditLedger(ledger.path, clock=clock))
    restarted_watchdog = ReconciliationWatchdog(restarted_core, clock=clock)
    clock.advance(timedelta(seconds=31))

    assert restarted_watchdog.audit() == ()
    assert [
        record.event_type for record in ledger.iter_records() if record.event_type == EventType.RECONCILIATION_MISMATCH
    ] == [EventType.RECONCILIATION_MISMATCH]


def test_watchdog_does_not_flag_order_confirmed_by_user_channel(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    router = RedundantFeedRouter(core, clock=clock)

    gateway.submit_and_execute(
        _place_order("place-1", idempotency_key="client-1"),
        LiveAcceptedExecutor(client_order_id="client-1", exchange_order_id="exchange-1"),
    )
    clock.advance(timedelta(seconds=5))
    for message in CoinbaseMessageNormalizer().normalize(
        "coinbase-primary",
        _user_message(sequence=1, status="OPEN", order_id="exchange-1", client_order_id="client-1"),
    ):
        router.ingest(message)

    clock.advance(timedelta(seconds=60))
    watchdog = ReconciliationWatchdog(core, clock=clock)
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.orders_by_action_id["place-1"].last_exchange_update["status"] == "OPEN"
    assert watchdog.audit() == ()
    assert EventType.RECONCILIATION_MISMATCH not in [record.event_type for record in ledger.iter_records()]


def test_watchdog_ignores_dry_run_orders(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": "place-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "execution_result": {
                "action_id": "place-1",
                "action_type": ActionType.PLACE_ORDER.value,
                "client_order_id": "client-1",
                "error_code": None,
                "error_message": None,
                "exchange_order_id": "dry-run-1",
                "mode": ExecutionMode.DRY_RUN.value,
                "raw_response": {"simulated": True},
                "status": ExecutionStatus.ACCEPTED.value,
            },
        },
    )
    clock.advance(timedelta(seconds=60))

    watchdog = ReconciliationWatchdog(core, clock=clock)

    assert watchdog.audit() == ()
    assert EventType.RECONCILIATION_MISMATCH not in [record.event_type for record in ledger.iter_records()]


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
                        "leaves_quantity": "0.01",
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
        "timestamp": "2026-01-01T00:00:05Z",
    }
