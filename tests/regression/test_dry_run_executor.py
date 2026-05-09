from __future__ import annotations

import pytest

from actions.dry_run import DryRunExecutor
from actions.execution import ExecutionResult
from actions.gateway import ActionCommand, ActionGateway, CancelOrderIntent, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ActionType,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    ExecutionStatus,
    OrderSide,
    OrderType,
)
from projections.state import SourceOfTruthProjection


def test_dry_run_executor_places_order_with_deterministic_exchange_order_id(workspace_tmp_path):
    command = _place_order("action-1")
    first = DryRunExecutor().execute(command)
    second = DryRunExecutor().execute(command)

    assert first.exchange_order_id == second.exchange_order_id
    assert first.status == ExecutionStatus.ACCEPTED
    assert first.mode == ExecutionMode.DRY_RUN
    assert first.client_order_id == "action-1"


def test_gateway_records_dry_run_execution_result(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), DryRunExecutor())

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    executed_record = next(record for record in records if record.event_type == EventType.ACTION_EXECUTED)
    execution_result = executed_record.payload["execution_result"]
    assert receipt.status == ActionStatus.EXECUTED
    assert projection.actions["action-1"].status == ActionStatus.EXECUTED
    assert projection.logical_orders_by_id["action-1"].logical_order_id == "action-1"
    assert projection.placements_by_id["action-1"].logical_order_id == "action-1"
    assert execution_result["mode"] == ExecutionMode.DRY_RUN.value
    assert execution_result["status"] == ExecutionStatus.ACCEPTED.value
    assert execution_result["raw_response"]["simulated"] is True


def test_dry_run_executor_cancels_order_without_live_exchange_call(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))
    command = CancelOrderIntent(action_id="cancel-1", exchange_order_id="dry-run-order-1").to_command()

    receipt = gateway.submit_and_execute(command, DryRunExecutor())

    execution_result = ledger.iter_records()[-1].payload["execution_result"]
    assert receipt.status == ActionStatus.EXECUTED
    assert execution_result["status"] == ExecutionStatus.CANCELLED.value
    assert execution_result["exchange_order_id"] == "dry-run-order-1"
    assert [record.event_type for record in ledger.iter_records()] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTED,
    ]


def test_execution_result_requires_typed_error_category():
    result = ExecutionResult(
        action_id="action-1",
        action_type=ActionType.PLACE_ORDER,
        status=ExecutionStatus.FAILED,
        mode=ExecutionMode.LIVE,
        error_category=ErrorCategory.EXCHANGE_TRANSPORT,
        error_code=ErrorCode.EXCHANGE_TRANSPORT_FAILED,
        error_message="server error",
    )

    assert result.to_payload()["error_category"] == ErrorCategory.EXCHANGE_TRANSPORT.value
    assert result.to_payload()["error_code"] == ErrorCode.EXCHANGE_TRANSPORT_FAILED.value

    with pytest.raises(TypeError, match="ErrorCategory"):
        ExecutionResult(
            action_id="action-2",
            action_type=ActionType.PLACE_ORDER,
            status=ExecutionStatus.FAILED,
            mode=ExecutionMode.LIVE,
            error_category="exchange_transport",
            error_code="http_500",
            error_message="server error",
        )

    with pytest.raises(TypeError, match="error_code"):
        ExecutionResult(
            action_id="action-3",
            action_type=ActionType.PLACE_ORDER,
            status=ExecutionStatus.FAILED,
            mode=ExecutionMode.LIVE,
            error_category=ErrorCategory.EXCHANGE_TRANSPORT,
            error_code="",
            error_message="server error",
        )


def _place_order(action_id: str) -> ActionCommand:
    return PlaceOrderIntent(
        action_id=action_id,
        product_id="BTC-PERP-INTX",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="100000",
    ).to_command()
