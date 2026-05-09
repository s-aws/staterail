from __future__ import annotations

from decimal import Decimal

import pytest

from actions.execution import ExecutionResult
from actions.gateway import ActionCommand, ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionFailureReason,
    ActionRejectionReason,
    ActionStatus,
    ActionType,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    ExecutionStatus,
    MarginType,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OrderSizingDecisionStatus,
    OrderType,
)
from orders.sizing import OrderSizingDecision
from projections.state import SourceOfTruthProjection
from risk.gate import RiskGate, RiskPolicy


class SuccessfulExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            exchange_order_id=f"exchange-{command.action_id}",
            raw_response={"accepted": True},
        )


class RejectedExecutionExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=ExecutionMode.LIVE,
            client_order_id=command.idempotency_key or command.action_id,
            error_code="venue_position_limit",
            error_message="venue rejected order before placement",
            raw_response={"success": False, "preview_failure_reason": "POSITION_LIMIT"},
        )


class FixedExchangeOrderIdExecutor:
    def __init__(self, exchange_order_id: str) -> None:
        self._exchange_order_id = exchange_order_id

    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            client_order_id=command.idempotency_key or command.action_id,
            exchange_order_id=self._exchange_order_id,
            raw_response={"accepted": True},
        )


class FailingExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        raise RuntimeError(f"cannot execute {command.action_id}")


class InvalidExecutor:
    def execute(self, command: ActionCommand) -> dict[str, str]:
        return {"exchange_order_id": f"exchange-{command.action_id}"}


class MismatchedActionIdExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=f"other-{command.action_id}",
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            exchange_order_id=f"exchange-{command.action_id}",
            raw_response={"accepted": True},
        )


class MismatchedActionTypeExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=ActionType.CANCEL_ORDER,
            status=ExecutionStatus.CANCELLED,
            mode=ExecutionMode.LIVE,
            exchange_order_id=f"exchange-{command.action_id}",
            raw_response={"cancelled": True},
        )


class MismatchedClientOrderIdExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            client_order_id="unexpected-client-order-id",
            exchange_order_id=f"exchange-{command.action_id}",
            raw_response={"accepted": True},
        )


class InvalidExecutionStatusExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.CANCELLED,
            mode=ExecutionMode.LIVE,
            client_order_id=command.idempotency_key or command.action_id,
            exchange_order_id=f"exchange-{command.action_id}",
            raw_response={"cancelled": True},
        )


class MissingVenueOrderIdentifierExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.LIVE,
            raw_response={"accepted": True},
        )


def test_action_gateway_audits_request_before_acceptance(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit(_place_order("action-1"))

    assert receipt.status == ActionStatus.ACCEPTED
    assert [record.event_type for record in ledger.iter_records()] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
    ]


def test_action_gateway_rejects_invalid_command_after_auditing_request(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))
    command = ActionCommand(
        action_id="bad-action",
        action_type=ActionType.PLACE_ORDER,
        payload={"product_id": "BTC-PERP-INTX"},
    )

    receipt = gateway.submit(command)

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.REJECTED
    assert receipt.rejection_reason == ActionRejectionReason.VALIDATION_FAILED
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_REJECTED,
    ]
    assert "side is required" in records[-1].payload["validation_errors"]


def test_action_gateway_rejects_non_positive_size_before_logical_order(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))
    command = PlaceOrderIntent(
        action_id="action-1",
        product_id="BTC-PERP-INTX",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0",
        limit_price="100000",
    ).to_command()

    receipt = gateway.submit(command)

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.REJECTED
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_REJECTED,
    ]
    assert "size must be a positive decimal string" in records[-1].payload["validation_errors"]


def test_action_gateway_duplicate_action_id_does_not_rewrite_original_projection(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    first = gateway.submit(_place_order("action-1"))
    second = gateway.submit(_place_order("action-1"))
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert first.status == ActionStatus.ACCEPTED
    assert second.status == ActionStatus.REJECTED
    assert second.rejection_reason == ActionRejectionReason.DUPLICATE_ACTION_ID
    assert projection.actions["action-1"].status == ActionStatus.ACCEPTED


def test_action_gateway_rejects_duplicate_client_order_id_without_rewriting_index(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    first = gateway.submit(_place_order("action-1", idempotency_key="client-1"))
    second = gateway.submit(_place_order("action-2", idempotency_key="client-1"))
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert first.status == ActionStatus.ACCEPTED
    assert second.status == ActionStatus.REJECTED
    assert second.rejection_reason == ActionRejectionReason.DUPLICATE_ORDER_IDENTITY
    assert projection.orders_by_client_order_id["client-1"].action_id == "action-1"
    assert projection.orders_by_action_id["action-2"].lifecycle_status == OrderLifecycleStatus.REJECTED


def test_action_gateway_preview_reuses_validation_without_appending(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    gateway = ActionGateway(core)

    preview = gateway.preview(_place_order("action-1"))

    assert preview.status == ActionStatus.ACCEPTED
    assert preview.to_payload()["status"] == ActionStatus.ACCEPTED.value
    assert ledger.iter_records() == ()


def test_action_gateway_preview_reports_risk_rejection_without_appending(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    gateway = ActionGateway(
        core,
        risk_gate=RiskGate(
            RiskPolicy.from_values(
                allowed_products=("BTC-USD",),
                kill_switch_enabled=True,
            )
        ),
    )

    preview = gateway.preview(_place_order("action-1"))

    assert preview.status == ActionStatus.REJECTED
    assert preview.rejection_reason == ActionRejectionReason.RISK_CHECK_FAILED
    assert preview.risk_evaluation is not None
    assert "kill switch is enabled" in preview.validation_errors
    assert ledger.iter_records() == ()


def test_action_gateway_executes_accepted_action_and_records_result(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), SuccessfulExecutor())

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    executed_record = next(record for record in records if record.event_type == EventType.ACTION_EXECUTED)
    execution_result = executed_record.payload["execution_result"]
    assert receipt.status == ActionStatus.EXECUTED
    assert projection.actions["action-1"].status == ActionStatus.EXECUTED
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTED,
        EventType.ORDER_PLACEMENT_RECORDED,
    ]
    assert projection.actions["action-1"].execution_started_sequence == 4
    assert executed_record.payload["execution_started_sequence"] == 4
    assert projection.logical_orders_by_id["action-1"].created_by_action_id == "action-1"
    assert projection.placements_by_id["action-1"].exchange_order_id == "exchange-action-1"
    assert execution_result["status"] == ExecutionStatus.ACCEPTED.value
    assert execution_result["mode"] == ExecutionMode.LIVE.value


def test_action_gateway_marks_executor_reject_result_as_failed_execution(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), RejectedExecutionExecutor())

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    failed_record = records[-1]
    execution_result = failed_record.payload["execution_result"]
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTION_REJECTED
    assert projection.actions["action-1"].status == ActionStatus.FAILED
    assert projection.actions["action-1"].failure_reason == ActionFailureReason.EXECUTION_REJECTED
    assert projection.orders_by_action_id["action-1"].lifecycle_status == OrderLifecycleStatus.REJECTED
    assert projection.orders_by_action_id["action-1"].last_execution_result["status"] == (
        ExecutionStatus.REJECTED.value
    )
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert failed_record.payload["failure_reason"] == ActionFailureReason.EXECUTION_REJECTED.value
    assert failed_record.payload["error_sequence"] is None
    assert execution_result["status"] == ExecutionStatus.REJECTED.value
    assert execution_result["error_message"] == "venue rejected order before placement"
    assert not any(record.event_type == EventType.ACTION_EXECUTED for record in records)
    assert not any(record.event_type == EventType.ORDER_PLACEMENT_RECORDED for record in records)


def test_action_gateway_fails_duplicate_exchange_order_id_before_recording_execution(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    first = gateway.submit_and_execute(_place_order("action-1"), FixedExchangeOrderIdExecutor("exchange-1"))
    second = gateway.submit_and_execute(_place_order("action-2"), FixedExchangeOrderIdExecutor("exchange-1"))

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    executed_action_ids = [
        record.payload["action_id"]
        for record in records
        if record.event_type == EventType.ACTION_EXECUTED
    ]

    assert first.status == ActionStatus.EXECUTED
    assert second.status == ActionStatus.FAILED
    assert second.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert executed_action_ids == ["action-1"]
    assert projection.orders_by_exchange_order_id["exchange-1"].action_id == "action-1"
    assert records[-2].event_type == EventType.ERROR
    assert records[-2].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert records[-2].payload["error"]["context"]["identifier_type"] == "exchange_order_id"
    assert records[-2].payload["error"]["context"]["identifier"] == "exchange-1"


def test_action_gateway_logs_executor_errors_and_marks_execution_failed(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), FailingExecutor())

    projection = SourceOfTruthProjection.from_ledger(ledger)
    event_types = [record.event_type for record in ledger.iter_records()]
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert projection.actions["action-1"].status == ActionStatus.FAILED
    assert projection.actions["action-1"].failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert projection.orders_by_action_id["action-1"].lifecycle_status == OrderLifecycleStatus.EXECUTION_UNKNOWN
    assert event_types == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ERROR,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert projection.error_count == 1


def test_action_gateway_fails_non_contract_executor_results(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), InvalidExecutor())

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert records[3].event_type == EventType.ACTION_EXECUTION_STARTED
    assert records[4].event_type == EventType.ERROR
    assert records[5].event_type == EventType.ACTION_EXECUTION_FAILED
    assert records[5].payload["failure_reason"] == ActionFailureReason.EXECUTOR_ERROR.value
    assert records[5].payload["execution_started_sequence"] == 4
    assert records[5].payload["error_sequence"] == 5
    assert records[4].payload["exception_type"] == "TypeError"
    assert records[4].payload["error_category"] == ErrorCategory.ACTION_EXECUTOR.value
    assert records[4].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_FAILED.value
    assert records[4].payload["error"]["context"]["action_id"] == "action-1"
    assert records[4].payload["error"]["context"]["execution_started_sequence"] == 4
    assert "ExecutionResult" in records[4].payload["message"]


def test_action_gateway_fails_mismatched_executor_results_before_recording_execution(
    workspace_tmp_path,
):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), MismatchedActionIdExecutor())

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ERROR,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert projection.actions["action-1"].status == ActionStatus.FAILED
    assert records[4].payload["exception_type"] == "ActionExecutorContractError"
    assert records[4].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert records[4].payload["error"]["context"]["expected_action_id"] == "action-1"
    assert records[4].payload["error"]["context"]["observed_action_id"] == "other-action-1"
    assert "mismatched action_id" in records[4].payload["message"]
    assert not any(record.event_type == EventType.ACTION_EXECUTED for record in records)


def test_action_gateway_fails_mismatched_executor_action_type_before_recording_execution(
    workspace_tmp_path,
):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), MismatchedActionTypeExecutor())

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ERROR,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert records[4].payload["exception_type"] == "ActionExecutorContractError"
    assert records[4].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert records[4].payload["error"]["context"]["expected_action_type"] == ActionType.PLACE_ORDER.value
    assert records[4].payload["error"]["context"]["observed_action_type"] == ActionType.CANCEL_ORDER.value
    assert "mismatched action_type" in records[4].payload["message"]
    assert not any(record.event_type == EventType.ACTION_EXECUTED for record in records)


def test_action_gateway_fails_mismatched_executor_client_order_id_before_recording_execution(
    workspace_tmp_path,
):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(
        _place_order("action-1", idempotency_key="expected-client-order-id"),
        MismatchedClientOrderIdExecutor(),
    )

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ERROR,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert records[4].payload["exception_type"] == "ActionExecutorContractError"
    assert records[4].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert records[4].payload["error"]["context"]["expected_client_order_id"] == "expected-client-order-id"
    assert records[4].payload["error"]["context"]["observed_client_order_id"] == "unexpected-client-order-id"
    assert "mismatched client_order_id" in records[4].payload["message"]
    assert not any(record.event_type == EventType.ACTION_EXECUTED for record in records)


def test_action_gateway_fails_invalid_execution_status_before_recording_execution(
    workspace_tmp_path,
):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), InvalidExecutionStatusExecutor())

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ERROR,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert records[4].payload["exception_type"] == "ActionExecutorContractError"
    assert records[4].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert records[4].payload["error"]["context"]["allowed_statuses"] == [
        ExecutionStatus.ACCEPTED.value,
        ExecutionStatus.REJECTED.value,
        ExecutionStatus.FAILED.value,
    ]
    assert records[4].payload["error"]["context"]["observed_status"] == ExecutionStatus.CANCELLED.value
    assert "invalid status" in records[4].payload["message"]
    assert not any(record.event_type == EventType.ACTION_EXECUTED for record in records)


def test_action_gateway_fails_accepted_place_order_without_venue_identifier_before_recording_placement(
    workspace_tmp_path,
):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(_place_order("action-1"), MissingVenueOrderIdentifierExecutor())

    records = ledger.iter_records()
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTOR_ERROR
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ERROR,
        EventType.ACTION_EXECUTION_FAILED,
    ]
    assert records[4].payload["exception_type"] == "ActionExecutorContractError"
    assert records[4].payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert "without a venue order identifier" in records[4].payload["message"]
    assert not any(record.event_type == EventType.ACTION_EXECUTED for record in records)
    assert not any(record.event_type == EventType.ORDER_PLACEMENT_RECORDED for record in records)


def test_action_gateway_records_followup_lineage_from_place_order_payload(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    gateway.submit_and_execute(_place_order("parent-order"), SuccessfulExecutor())
    receipt = gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="child-action",
            logical_order_id="child-order",
            lineage_relation=OrderLineageRelation.FOLLOWUP_AFTER_FILL,
            root_order_id="parent-order",
            parent_order_id="parent-order",
            source_order_ids=("parent-order",),
            product_id="BTC-PERP-INTX",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="101000",
            margin_type=MarginType.CROSS,
        ).to_command(),
        SuccessfulExecutor(),
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    child = projection.logical_orders_by_id["child-order"]
    assert receipt.status == ActionStatus.EXECUTED
    assert child.lineage_relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL
    assert child.root_order_id == "parent-order"
    assert child.parent_order_id == "parent-order"
    assert child.source_order_ids == ("parent-order",)
    assert projection.logical_orders_by_id["parent-order"].child_order_ids == ["child-order"]
    assert projection.placements_by_id["child-action"].logical_order_id == "child-order"


def test_action_gateway_records_cancel_replace_placement_for_existing_logical_order(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    gateway.submit_and_execute(_place_order("logical-order"), SuccessfulExecutor())
    receipt = gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="move-action",
            logical_order_id="logical-order",
            placement_kind=OrderPlacementKind.CANCEL_REPLACE,
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="100500",
            margin_type=MarginType.CROSS,
        ).to_command(),
        SuccessfulExecutor(),
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    logical_order = projection.logical_orders_by_id["logical-order"]
    assert receipt.status == ActionStatus.EXECUTED
    assert logical_order.placement_ids == ["logical-order", "move-action"]
    assert projection.placements_by_id["move-action"].logical_order_id == "logical-order"
    assert projection.placements_by_id["move-action"].placement_kind == OrderPlacementKind.CANCEL_REPLACE


def test_action_gateway_records_staged_release_without_execution(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))
    intent = PlaceOrderIntent(
        action_id="stage-action",
        logical_order_id="stage-logical",
        product_id="BTC-PERP-INTX",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="100500",
        margin_type=MarginType.CROSS,
    )

    staged_intent = intent.as_staged_release()
    receipt = gateway.submit(staged_intent.to_command())

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    placement = projection.placements_by_id["stage-action"]
    assert intent.placement_kind is None
    assert staged_intent.placement_kind == OrderPlacementKind.STAGED_RELEASE
    assert receipt.status == ActionStatus.ACCEPTED
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ORDER_PLACEMENT_RECORDED,
    ]
    assert placement.logical_order_id == "stage-logical"
    assert placement.placement_kind == OrderPlacementKind.STAGED_RELEASE
    assert placement.placement_status == OrderPlacementStatus.STAGED
    assert placement.exchange_order_id is None
    assert placement.venue_client_order_id is None
    assert projection.logical_orders_by_id["stage-logical"].placement_ids == ["stage-action"]


def test_action_gateway_does_not_execute_staged_release(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="stage-action",
            logical_order_id="stage-logical",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="100500",
            margin_type=MarginType.CROSS,
        ).to_command(),
        FailingExecutor(),
    )

    assert receipt.status == ActionStatus.ACCEPTED
    assert [record.event_type for record in ledger.iter_records()] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ORDER_PLACEMENT_RECORDED,
    ]


def test_action_gateway_executes_release_for_existing_staged_logical_order(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    staged = gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="stage-action",
            idempotency_key="stage-client-order",
            logical_order_id="stage-logical",
            margin_type=MarginType.CROSS,
            metadata={"release_plan": "entry"},
            order_type=OrderType.LIMIT,
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            size="0.01",
            limit_price="100500",
        ).to_command(),
        FailingExecutor(),
    )
    released = gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="release-action",
            idempotency_key="release-client-order",
            logical_order_id="stage-logical",
            margin_type=MarginType.CROSS,
            metadata={
                "staged_release": {
                    "release_of_action_id": "stage-action",
                    "release_of_placement_id": "stage-action",
                },
            },
            order_type=OrderType.LIMIT,
            placement_kind=OrderPlacementKind.RELEASE,
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            size="0.01",
            limit_price="100500",
        ).to_command(),
        SuccessfulExecutor(),
    )

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    placements = projection.placements_for_logical_order("stage-logical")
    release_placement = projection.placements_by_id["release-action"]

    assert staged.status == ActionStatus.ACCEPTED
    assert released.status == ActionStatus.EXECUTED
    assert [placement.placement_kind for placement in placements] == [
        OrderPlacementKind.STAGED_RELEASE,
        OrderPlacementKind.RELEASE,
    ]
    assert release_placement.placement_status == OrderPlacementStatus.ACCEPTED
    assert release_placement.exchange_order_id == "exchange-release-action"
    assert release_placement.payload["metadata"]["staged_release"]["release_of_placement_id"] == "stage-action"
    assert projection.orders_by_action_id["release-action"].lifecycle_status == OrderLifecycleStatus.OPEN
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ORDER_PLACEMENT_RECORDED,
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTED,
        EventType.ORDER_PLACEMENT_RECORDED,
    ]


def test_action_gateway_rejects_release_without_existing_logical_order(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit(
        PlaceOrderIntent(
            action_id="release-action",
            logical_order_id="missing-logical",
            margin_type=MarginType.CROSS,
            order_type=OrderType.LIMIT,
            placement_kind=OrderPlacementKind.RELEASE,
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            size="0.01",
            limit_price="100500",
        ).to_command()
    )

    assert receipt.status == ActionStatus.REJECTED
    assert any(
        "release placement_kind require an existing logical_order_id" in error
        for error in ledger.iter_records()[-1].payload["validation_errors"]
    )


def test_action_gateway_rejects_ambiguous_existing_logical_order_placement(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    gateway.submit_and_execute(_place_order("logical-order"), SuccessfulExecutor())
    receipt = gateway.submit(
        PlaceOrderIntent(
            action_id="ambiguous-action",
            logical_order_id="logical-order",
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="100500",
            margin_type=MarginType.CROSS,
        ).to_command()
    )

    assert receipt.status == ActionStatus.REJECTED
    assert any(
        "existing logical_order_id requires" in error
        for error in ledger.iter_records()[-1].payload["validation_errors"]
    )


def test_place_order_intent_uses_accepted_sizing_decision():
    intent = PlaceOrderIntent(
        action_id="sized-action",
        product_id="BTC-PERP-INTX",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="100000",
        margin_type=MarginType.CROSS,
    )
    decision = OrderSizingDecision(
        status=OrderSizingDecisionStatus.ACCEPTED,
        lineage_relation=OrderLineageRelation.ROOT,
        product_id="BTC-PERP-INTX",
        requested_sizes=(Decimal("0.02"),),
        output_sizes=(Decimal("0.02"),),
        limit_price=Decimal("100000.0"),
    )

    sized_intent = intent.with_sizing_decision(decision)
    command_payload = sized_intent.to_command().to_payload()["payload"]

    assert intent.size == "0.01"
    assert sized_intent.size == "0.02"
    assert sized_intent.limit_price == "100000.0"
    assert command_payload["size"] == "0.02"
    assert command_payload["limit_price"] == "100000.0"


def test_place_order_intent_requires_enums():
    with pytest.raises(TypeError):
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BTC-PERP-INTX",
            side="buy",
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="100000",
        )
    with pytest.raises(TypeError):
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BIT-29MAY26-CDE",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="100000",
            margin_type="cross",
        )
    with pytest.raises(TypeError):
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BIT-29MAY26-CDE",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="100000",
            lineage_relation="followup_after_fill",
        )
    with pytest.raises(TypeError):
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BIT-29MAY26-CDE",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="100000",
            placement_kind="cancel_replace",
        )
    with pytest.raises(TypeError):
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BIT-29MAY26-CDE",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="100000",
            source_order_ids=["source-1"],
        )
    with pytest.raises(ValueError):
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BIT-29MAY26-CDE",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="100000",
            logical_order_id="",
        )


def test_action_gateway_rejects_invalid_margin_type_payload(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit(
        ActionCommand(
            action_id="action-1",
            action_type=ActionType.PLACE_ORDER,
            payload={
                "limit_price": "100000",
                "margin_type": "portfolio",
                "order_type": OrderType.LIMIT.value,
                "post_only": "yes",
                "product_id": "BIT-29MAY26-CDE",
                "side": OrderSide.BUY.value,
                "size": "1",
                "time_in_force": "good_until_cancelled",
            },
        )
    )

    assert receipt.status == ActionStatus.REJECTED
    assert "margin_type is invalid" in ledger.iter_records()[-1].payload["validation_errors"]
    assert "post_only must be a bool" in ledger.iter_records()[-1].payload["validation_errors"]


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
