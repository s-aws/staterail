from __future__ import annotations

from actions.dry_run import DryRunExecutor
from actions.execution import ExecutionResult
from actions.gateway import ActionCommand, ActionGateway, CancelOrderIntent, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ActionType,
    EventType,
    ExecutionMode,
    ExecutionStatus,
    MarginType,
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
)
from projections.state import SourceOfTruthProjection


class FailedPlaceOrderExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.FAILED,
            mode=ExecutionMode.DRY_RUN,
            error_code="dry_run_failure",
            error_message="simulated placement failure",
            raw_response={"simulated": True},
        )


def test_projection_rebuilds_open_order_from_dry_run_execution(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("place-1"), DryRunExecutor())
    projection = SourceOfTruthProjection.from_ledger(AuditLedger(ledger.path))

    order = projection.orders_by_action_id["place-1"]
    assert receipt.status == ActionStatus.EXECUTED
    assert order.lifecycle_status == OrderLifecycleStatus.OPEN
    assert order.requested_sequence == 1
    assert order.accepted_sequence == 2
    assert order.execution_started_sequence == 4
    assert order.executed_sequence == 5
    assert order.product_id == "BTC-PERP-INTX"
    assert order.side == OrderSide.BUY
    assert order.order_type == OrderType.LIMIT
    assert order.margin_type == MarginType.CROSS
    assert order.exchange_order_id in projection.orders_by_exchange_order_id
    assert projection.logical_orders_by_id["place-1"].created_by_action_id == "place-1"
    assert projection.placements_by_id["place-1"].exchange_order_id == order.exchange_order_id
    assert projection.open_orders == (order,)


def test_projection_links_cancel_execution_to_original_order(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    gateway.submit_and_execute(_place_order("place-1"), DryRunExecutor())
    first_projection = SourceOfTruthProjection.from_ledger(ledger)
    exchange_order_id = first_projection.orders_by_action_id["place-1"].exchange_order_id
    assert exchange_order_id is not None

    gateway.submit_and_execute(
        CancelOrderIntent(action_id="cancel-1", exchange_order_id=exchange_order_id).to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(AuditLedger(ledger.path))

    order = projection.orders_by_action_id["place-1"]
    assert order.lifecycle_status == OrderLifecycleStatus.CANCELLED
    assert order.cancel_action_ids == ["cancel-1"]
    assert order.terminal_sequence == 10
    assert projection.open_orders == ()


def test_projection_indexes_order_by_client_order_id_from_idempotency_key(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    gateway.submit_and_execute(
        _place_order("place-1", idempotency_key="client-order-1"),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)

    order = projection.orders_by_client_order_id["client-order-1"]
    assert order.action_id == "place-1"
    assert order.client_order_id == "client-order-1"


def test_projection_marks_failed_execution_as_terminal_order_failure(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("place-1"), FailedPlaceOrderExecutor())
    projection = SourceOfTruthProjection.from_ledger(ledger)

    order = projection.orders_by_action_id["place-1"]
    assert receipt.status == ActionStatus.FAILED
    assert order.lifecycle_status == OrderLifecycleStatus.FAILED
    assert order.execution_status == ExecutionStatus.FAILED
    assert order.terminal_sequence == 5
    assert order.last_execution_result["error_message"] == "simulated placement failure"
    assert [record.event_type for record in ledger.iter_records()] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTION_FAILED,
    ]


def test_projection_marks_rejected_place_order_as_terminal(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))
    command = ActionCommand(
        action_id="place-1",
        action_type=ActionType.PLACE_ORDER,
        payload={"product_id": "BTC-PERP-INTX"},
    )

    gateway.submit(command)
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert [record.event_type for record in ledger.iter_records()] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_REJECTED,
    ]
    order = projection.orders_by_action_id["place-1"]
    assert order.lifecycle_status == OrderLifecycleStatus.REJECTED
    assert order.terminal_sequence == 2


def _place_order(action_id: str, *, idempotency_key: str | None = None) -> ActionCommand:
    return PlaceOrderIntent(
        action_id=action_id,
        product_id="BTC-PERP-INTX",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="100000",
        margin_type=MarginType.CROSS,
        idempotency_key=idempotency_key,
    ).to_command()
