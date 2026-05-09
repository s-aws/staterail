from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
from enum import Enum
from threading import RLock
from typing import Any, Protocol

from actions.execution import ExecutionResult, valid_execution_statuses_for_action_type
from core.engine import AuditCore
from core.enums import (
    ActionFailureReason,
    ActionRejectionReason,
    ActionStatus,
    ActionType,
    ErrorCategory,
    ExecutionStatus,
    EventType,
    MarginType,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OrderType,
    RiskCheckStatus,
    TimeInForce,
)
from core.errors import ActionExecutorContractError, exception_to_error_payload
from core.json_tools import JsonValue, normalize_json
from orders.lineage import LogicalOrderRecord, OrderPlacementRecord
from orders.sizing import OrderSizingDecision
from projections.state import SourceOfTruthProjection
from risk.gate import RiskEvaluation, RiskGate


class ActionExecutor(Protocol):
    def execute(self, command: "ActionCommand") -> ExecutionResult:
        ...


@dataclass(frozen=True)
class ActionCommand:
    action_id: str
    action_type: ActionType
    payload: Mapping[str, Any]
    requested_by: str = "strategy"
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        if not self.action_id:
            raise ValueError("action_id is required")
        if not isinstance(self.action_type, ActionType):
            raise TypeError("action_type must be an ActionType")
        if not self.requested_by:
            raise ValueError("requested_by is required")

    def to_payload(self) -> dict[str, JsonValue]:
        payload = normalize_json(self.payload)
        if not isinstance(payload, dict):
            raise TypeError("Action payload must normalize to a JSON object")
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.value,
            "idempotency_key": self.idempotency_key,
            "payload": payload,
            "requested_by": self.requested_by,
        }


@dataclass(frozen=True)
class PlaceOrderIntent:
    action_id: str
    product_id: str
    side: OrderSide
    order_type: OrderType
    size: str
    limit_price: str | None = None
    leverage: str | None = None
    margin_type: MarginType | None = None
    time_in_force: TimeInForce = TimeInForce.GOOD_UNTIL_CANCELLED
    post_only: bool = False
    reduce_only: bool = False
    logical_order_id: str | None = None
    lineage_relation: OrderLineageRelation | None = None
    root_order_id: str | None = None
    parent_order_id: str | None = None
    source_order_ids: tuple[str, ...] = ()
    placement_kind: OrderPlacementKind | None = None
    requested_by: str = "strategy"
    idempotency_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.product_id:
            raise ValueError("product_id is required")
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be an OrderSide")
        if not isinstance(self.order_type, OrderType):
            raise TypeError("order_type must be an OrderType")
        if not isinstance(self.time_in_force, TimeInForce):
            raise TypeError("time_in_force must be a TimeInForce")
        if not isinstance(self.post_only, bool):
            raise TypeError("post_only must be a bool")
        if not isinstance(self.reduce_only, bool):
            raise TypeError("reduce_only must be a bool")
        if not self.size:
            raise ValueError("size is required")
        if self.order_type == OrderType.LIMIT and not self.limit_price:
            raise ValueError("limit_price is required for limit orders")
        if self.margin_type is not None and not isinstance(self.margin_type, MarginType):
            raise TypeError("margin_type must be a MarginType")
        if self.lineage_relation is not None and not isinstance(self.lineage_relation, OrderLineageRelation):
            raise TypeError("lineage_relation must be an OrderLineageRelation")
        if self.placement_kind is not None and not isinstance(self.placement_kind, OrderPlacementKind):
            raise TypeError("placement_kind must be an OrderPlacementKind")
        if self.logical_order_id is not None and not self.logical_order_id:
            raise ValueError("logical_order_id cannot be empty")
        if self.root_order_id is not None and not self.root_order_id:
            raise ValueError("root_order_id cannot be empty")
        if self.parent_order_id is not None and not self.parent_order_id:
            raise ValueError("parent_order_id cannot be empty")
        if not isinstance(self.source_order_ids, tuple):
            raise TypeError("source_order_ids must be a tuple")
        if any(not isinstance(source_order_id, str) or not source_order_id for source_order_id in self.source_order_ids):
            raise ValueError("source_order_ids must contain non-empty strings")
        metadata = normalize_json(self.metadata)
        if not isinstance(metadata, dict):
            raise TypeError("metadata must normalize to a JSON object")
        object.__setattr__(self, "metadata", metadata)

    def as_staged_release(self) -> "PlaceOrderIntent":
        return replace(self, placement_kind=OrderPlacementKind.STAGED_RELEASE)

    def with_sizing_decision(self, decision: OrderSizingDecision) -> "PlaceOrderIntent":
        if not isinstance(decision, OrderSizingDecision):
            raise TypeError("decision must be an OrderSizingDecision")
        if decision.product_id != self.product_id:
            raise ValueError("sizing decision product_id must match intent product_id")
        limit_price = self.limit_price
        decision_limit_price = str(decision.limit_price) if decision.limit_price is not None else None
        if decision_limit_price is not None:
            if limit_price is not None and not _decimal_strings_equal(limit_price, decision_limit_price):
                raise ValueError("sizing decision limit_price must match intent limit_price")
            limit_price = decision_limit_price
        return replace(self, size=decision.single_output_size(), limit_price=limit_price)

    def to_command(self) -> ActionCommand:
        payload = {
            "limit_price": self.limit_price,
            "leverage": self.leverage,
            "margin_type": self.margin_type.value if self.margin_type is not None else None,
            "order_type": self.order_type.value,
            "post_only": self.post_only,
            "product_id": self.product_id,
            "reduce_only": self.reduce_only,
            "side": self.side.value,
            "size": self.size,
            "time_in_force": self.time_in_force.value,
        }
        if self.logical_order_id is not None:
            payload["logical_order_id"] = self.logical_order_id
        if self.lineage_relation is not None:
            payload["lineage_relation"] = self.lineage_relation.value
        if self.root_order_id is not None:
            payload["root_order_id"] = self.root_order_id
        if self.parent_order_id is not None:
            payload["parent_order_id"] = self.parent_order_id
        if self.source_order_ids:
            payload["source_order_ids"] = list(self.source_order_ids)
        if self.placement_kind is not None:
            payload["placement_kind"] = self.placement_kind.value
        if self.metadata:
            payload["metadata"] = self.metadata
        return ActionCommand(
            action_id=self.action_id,
            action_type=ActionType.PLACE_ORDER,
            payload=payload,
            requested_by=self.requested_by,
            idempotency_key=self.idempotency_key,
        )


@dataclass(frozen=True)
class CancelOrderIntent:
    action_id: str
    exchange_order_id: str | None = None
    client_order_id: str | None = None
    requested_by: str = "strategy"
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        if not self.exchange_order_id and not self.client_order_id:
            raise ValueError("exchange_order_id or client_order_id is required")

    def to_command(self) -> ActionCommand:
        return ActionCommand(
            action_id=self.action_id,
            action_type=ActionType.CANCEL_ORDER,
            payload={
                "client_order_id": self.client_order_id,
                "exchange_order_id": self.exchange_order_id,
            },
            requested_by=self.requested_by,
            idempotency_key=self.idempotency_key,
        )


@dataclass(frozen=True)
class ActionReceipt:
    action_id: str
    action_type: ActionType
    status: ActionStatus
    requested_sequence: int
    decision_sequence: int
    failure_reason: ActionFailureReason | None = None
    rejection_reason: ActionRejectionReason | None = None
    message: str | None = None


@dataclass(frozen=True)
class ActionPreview:
    action_id: str
    action_type: ActionType
    status: ActionStatus
    rejection_reason: ActionRejectionReason | None = None
    validation_errors: tuple[str, ...] = ()
    risk_evaluation: RiskEvaluation | None = None

    def __post_init__(self) -> None:
        if not self.action_id:
            raise ValueError("action_id is required")
        if not isinstance(self.action_type, ActionType):
            raise TypeError("action_type must be an ActionType")
        if not isinstance(self.status, ActionStatus):
            raise TypeError("status must be an ActionStatus")
        if self.rejection_reason is not None and not isinstance(self.rejection_reason, ActionRejectionReason):
            raise TypeError("rejection_reason must be an ActionRejectionReason")
        if not isinstance(self.validation_errors, tuple):
            raise TypeError("validation_errors must be a tuple")
        if self.risk_evaluation is not None and not isinstance(self.risk_evaluation, RiskEvaluation):
            raise TypeError("risk_evaluation must be a RiskEvaluation")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.value,
            "rejection_reason": self.rejection_reason.value if self.rejection_reason is not None else None,
            "risk_evaluation": self.risk_evaluation.to_payload() if self.risk_evaluation is not None else None,
            "status": self.status.value,
            "validation_errors": list(self.validation_errors),
        }


class ActionGateway:
    def __init__(self, core: AuditCore, risk_gate: RiskGate | None = None) -> None:
        self._core = core
        self._risk_gate = risk_gate
        self._lock = RLock()

    def preview(
        self,
        command: ActionCommand,
        *,
        projection: SourceOfTruthProjection | None = None,
    ) -> ActionPreview:
        if not isinstance(command, ActionCommand):
            raise TypeError("command must be an ActionCommand")

        with self._lock:
            resolved_projection = projection or SourceOfTruthProjection.from_ledger(self._core.ledger)
            rejection_reason, validation_errors = self._validate(command, resolved_projection)
            if rejection_reason is not None:
                return ActionPreview(
                    action_id=command.action_id,
                    action_type=command.action_type,
                    rejection_reason=rejection_reason,
                    status=ActionStatus.REJECTED,
                    validation_errors=tuple(validation_errors),
                )

            risk_evaluation = self._evaluate_risk(command, resolved_projection)
            if not risk_evaluation.passed:
                return ActionPreview(
                    action_id=command.action_id,
                    action_type=command.action_type,
                    rejection_reason=ActionRejectionReason.RISK_CHECK_FAILED,
                    risk_evaluation=risk_evaluation,
                    status=ActionStatus.REJECTED,
                    validation_errors=tuple(risk_evaluation.failure_messages()),
                )

            return ActionPreview(
                action_id=command.action_id,
                action_type=command.action_type,
                risk_evaluation=risk_evaluation,
                status=ActionStatus.ACCEPTED,
            )

    def submit(self, command: ActionCommand) -> ActionReceipt:
        if not isinstance(command, ActionCommand):
            raise TypeError("command must be an ActionCommand")

        with self._lock:
            projection = SourceOfTruthProjection.from_ledger(self._core.ledger)
            request_record = self._core.emit(EventType.ACTION_REQUESTED, command.to_payload())
            rejection_reason, validation_errors = self._validate(command, projection)

            if rejection_reason is not None:
                return self._reject(
                    command=command,
                    requested_sequence=request_record.sequence,
                    rejection_reason=rejection_reason,
                    validation_errors=validation_errors,
                )

            risk_evaluation = self._evaluate_risk(command, projection)
            if not risk_evaluation.passed:
                return self._reject(
                    command=command,
                    requested_sequence=request_record.sequence,
                    rejection_reason=ActionRejectionReason.RISK_CHECK_FAILED,
                    validation_errors=risk_evaluation.failure_messages(),
                    risk_evaluation=risk_evaluation,
                )

            accepted_record = self._core.emit(
                EventType.ACTION_ACCEPTED,
                {
                    "action_id": command.action_id,
                    "action_type": command.action_type.value,
                    "requested_sequence": request_record.sequence,
                    "risk_evaluation": risk_evaluation.to_payload(),
                },
            )
            self._emit_logical_order_created(command, projection)
            self._emit_staged_order_placement_recorded(command)
            return ActionReceipt(
                action_id=command.action_id,
                action_type=command.action_type,
                status=ActionStatus.ACCEPTED,
                requested_sequence=request_record.sequence,
                decision_sequence=accepted_record.sequence,
            )

    def submit_and_execute(self, command: ActionCommand, executor: ActionExecutor) -> ActionReceipt:
        receipt = self.submit(command)
        if receipt.status != ActionStatus.ACCEPTED:
            return receipt
        if _is_staged_release(command):
            return receipt

        execution_started_record = self._core.emit(
            EventType.ACTION_EXECUTION_STARTED,
            {
                "accepted_sequence": receipt.decision_sequence,
                "action_id": command.action_id,
                "action_type": command.action_type.value,
                "requested_sequence": receipt.requested_sequence,
            },
        )
        try:
            execution_result = executor.execute(command)
            if not isinstance(execution_result, ExecutionResult):
                raise TypeError("executor must return an ExecutionResult")
            projection = SourceOfTruthProjection.from_ledger(self._core.ledger)
            _validate_execution_result(command, execution_result, projection)
            failure_reason = _execution_failure_reason(execution_result)
            if failure_reason is not None:
                return self._fail_execution(
                    command=command,
                    accepted_sequence=receipt.decision_sequence,
                    error_sequence=None,
                    execution_result=execution_result,
                    execution_started_sequence=execution_started_record.sequence,
                    failure_reason=failure_reason,
                    message=_execution_failure_message(execution_result),
                    requested_sequence=receipt.requested_sequence,
                )
            executed_record = self._core.emit(
                EventType.ACTION_EXECUTED,
                {
                    "action_id": command.action_id,
                    "action_type": command.action_type.value,
                    "execution_started_sequence": execution_started_record.sequence,
                    "execution_result": execution_result.to_payload(),
                    "requested_sequence": receipt.requested_sequence,
                },
            )
            self._emit_order_placement_recorded(command, execution_result)
            return ActionReceipt(
                action_id=command.action_id,
                action_type=command.action_type,
                status=ActionStatus.EXECUTED,
                requested_sequence=receipt.requested_sequence,
                decision_sequence=executed_record.sequence,
            )
        except Exception as exc:
            error_record = self._core.emit(
                EventType.ERROR,
                exception_to_error_payload(
                    exc,
                    category=ErrorCategory.ACTION_EXECUTOR,
                    context={
                        "action_id": command.action_id,
                        "action_type": command.action_type.value,
                        "execution_started_sequence": execution_started_record.sequence,
                    },
                ),
            )
            return self._fail_execution(
                command=command,
                accepted_sequence=receipt.decision_sequence,
                error_sequence=error_record.sequence,
                execution_started_sequence=execution_started_record.sequence,
                failure_reason=ActionFailureReason.EXECUTOR_ERROR,
                message=f"executor failed at sequence {error_record.sequence}: {exc}",
                requested_sequence=receipt.requested_sequence,
            )

    def _fail_execution(
        self,
        *,
        command: ActionCommand,
        accepted_sequence: int,
        error_sequence: int | None,
        execution_started_sequence: int,
        failure_reason: ActionFailureReason,
        message: str,
        requested_sequence: int,
        execution_result: ExecutionResult | None = None,
    ) -> ActionReceipt:
        payload: dict[str, JsonValue] = {
            "accepted_sequence": accepted_sequence,
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "error_sequence": error_sequence,
            "execution_started_sequence": execution_started_sequence,
            "failure_reason": failure_reason.value,
            "message": message,
            "requested_sequence": requested_sequence,
        }
        if execution_result is not None:
            payload["execution_result"] = execution_result.to_payload()
        failed_record = self._core.emit(
            EventType.ACTION_EXECUTION_FAILED,
            payload,
        )
        return ActionReceipt(
            action_id=command.action_id,
            action_type=command.action_type,
            decision_sequence=failed_record.sequence,
            failure_reason=failure_reason,
            message=message,
            requested_sequence=requested_sequence,
            status=ActionStatus.FAILED,
        )

    def _reject(
        self,
        *,
        command: ActionCommand,
        requested_sequence: int,
        rejection_reason: ActionRejectionReason,
        validation_errors: list[str],
        risk_evaluation: RiskEvaluation | None = None,
    ) -> ActionReceipt:
        rejected_record = self._core.emit(
            EventType.ACTION_REJECTED,
            {
                "action_id": command.action_id,
                "action_type": command.action_type.value,
                "rejection_reason": rejection_reason.value,
                "requested_sequence": requested_sequence,
                "risk_evaluation": risk_evaluation.to_payload() if risk_evaluation is not None else None,
                "validation_errors": validation_errors,
            },
        )
        return ActionReceipt(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ActionStatus.REJECTED,
            requested_sequence=requested_sequence,
            decision_sequence=rejected_record.sequence,
            rejection_reason=rejection_reason,
            message="; ".join(validation_errors) if validation_errors else None,
        )

    def _validate(
        self,
        command: ActionCommand,
        projection: SourceOfTruthProjection,
    ) -> tuple[ActionRejectionReason | None, list[str]]:
        if command.action_id in projection.actions:
            return ActionRejectionReason.DUPLICATE_ACTION_ID, ["action_id already exists"]

        payload = command.to_payload()["payload"]
        if not isinstance(payload, dict):
            return ActionRejectionReason.VALIDATION_FAILED, ["payload must be a JSON object"]

        if command.action_type == ActionType.PLACE_ORDER:
            rejection_reason, errors = _validate_place_order_payload(payload)
            if rejection_reason is not None:
                return rejection_reason, errors
            lineage_errors = _validate_place_order_lineage(command, payload, projection)
            if lineage_errors:
                return ActionRejectionReason.VALIDATION_FAILED, lineage_errors
            client_order_id = command.idempotency_key or command.action_id
            existing_order = projection.orders_by_client_order_id.get(client_order_id)
            if existing_order is not None and existing_order.action_id != command.action_id:
                return (
                    ActionRejectionReason.DUPLICATE_ORDER_IDENTITY,
                    [f"client_order_id already belongs to action_id {existing_order.action_id}"],
                )
            return None, []
        if command.action_type == ActionType.CANCEL_ORDER:
            return _validate_cancel_order_payload(payload)

        return ActionRejectionReason.VALIDATION_FAILED, ["unsupported action_type"]

    def _emit_logical_order_created(self, command: ActionCommand, projection: SourceOfTruthProjection) -> None:
        if command.action_type != ActionType.PLACE_ORDER:
            return

        payload = command.to_payload()["payload"]
        if not isinstance(payload, dict):
            return

        logical_order_id = _logical_order_id(command, payload)
        if logical_order_id in projection.logical_orders_by_id:
            return

        order_record = LogicalOrderRecord(
            logical_order_id=logical_order_id,
            lineage_relation=_lineage_relation(payload),
            root_order_id=_string_or_none(payload.get("root_order_id")),
            parent_order_id=_string_or_none(payload.get("parent_order_id")),
            source_order_ids=_source_order_ids(payload),
            product_id=str(payload["product_id"]),
            side=OrderSide(str(payload["side"])),
            size=str(payload["size"]),
            limit_price=_string_or_none(payload.get("limit_price")),
            created_by_action_id=command.action_id,
            metadata=_payload_dict(payload.get("metadata")),
        )
        self._core.emit(EventType.ORDER_LOGICAL_CREATED, order_record.to_payload())

    def _emit_staged_order_placement_recorded(self, command: ActionCommand) -> None:
        if command.action_type != ActionType.PLACE_ORDER or not _is_staged_release(command):
            return

        payload = command.to_payload()["payload"]
        if not isinstance(payload, dict):
            return

        placement_record = OrderPlacementRecord(
            placement_id=command.action_id,
            logical_order_id=_logical_order_id(command, payload),
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id=str(payload["product_id"]),
            side=OrderSide(str(payload["side"])),
            size=str(payload["size"]),
            limit_price=_string_or_none(payload.get("limit_price")),
            action_id=command.action_id,
            metadata=_payload_dict(payload.get("metadata")),
        )
        self._core.emit(EventType.ORDER_PLACEMENT_RECORDED, placement_record.to_payload())

    def _emit_order_placement_recorded(self, command: ActionCommand, result: ExecutionResult) -> None:
        if command.action_type != ActionType.PLACE_ORDER or result.status != ExecutionStatus.ACCEPTED:
            return

        payload = command.to_payload()["payload"]
        if not isinstance(payload, dict):
            return

        placement_record = OrderPlacementRecord(
            placement_id=command.action_id,
            logical_order_id=_logical_order_id(command, payload),
            placement_kind=_placement_kind(payload),
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id=str(payload["product_id"]),
            side=OrderSide(str(payload["side"])),
            size=str(payload["size"]),
            limit_price=_string_or_none(payload.get("limit_price")),
            action_id=command.action_id,
            metadata=_payload_dict(payload.get("metadata")),
            venue_client_order_id=result.client_order_id,
            exchange_order_id=result.exchange_order_id,
        )
        self._core.emit(EventType.ORDER_PLACEMENT_RECORDED, placement_record.to_payload())

    def _evaluate_risk(
        self,
        command: ActionCommand,
        projection: SourceOfTruthProjection,
    ) -> RiskEvaluation:
        if self._risk_gate is None:
            return RiskEvaluation(status=RiskCheckStatus.PASS, checks=())
        return self._risk_gate.evaluate(command, projection)


def _validate_place_order_payload(payload: Mapping[str, JsonValue]) -> tuple[ActionRejectionReason | None, list[str]]:
    errors: list[str] = []
    for required_field in ("product_id", "side", "order_type", "size"):
        if not _non_empty_string(payload.get(required_field)):
            errors.append(f"{required_field} is required")

    if _enum_or_none(OrderSide, payload.get("side")) is None:
        errors.append("side is invalid")
    order_type = _enum_or_none(OrderType, payload.get("order_type"))
    if order_type is None:
        errors.append("order_type is invalid")
    if order_type == OrderType.LIMIT and not _non_empty_string(payload.get("limit_price")):
        errors.append("limit_price is required for limit orders")
    if _enum_or_none(TimeInForce, payload.get("time_in_force")) is None:
        errors.append("time_in_force is invalid")
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        errors.append("metadata must be a JSON object")
    margin_type = payload.get("margin_type")
    if margin_type is not None and _enum_or_none(MarginType, margin_type) is None:
        errors.append("margin_type is invalid")
    post_only = payload.get("post_only")
    if post_only is not None and not isinstance(post_only, bool):
        errors.append("post_only must be a bool")
    reduce_only = payload.get("reduce_only")
    if reduce_only is not None and not isinstance(reduce_only, bool):
        errors.append("reduce_only must be a bool")

    if errors:
        return ActionRejectionReason.VALIDATION_FAILED, errors
    return None, []


def _validate_place_order_lineage(
    command: ActionCommand,
    payload: Mapping[str, JsonValue],
    projection: SourceOfTruthProjection,
) -> list[str]:
    errors: list[str] = []
    if _positive_decimal_string(payload.get("size")) is None:
        errors.append("size must be a positive decimal string")
    limit_price = payload.get("limit_price")
    if limit_price is not None and _positive_decimal_string(limit_price) is None:
        errors.append("limit_price must be a positive decimal string")
    if _optional_string_is_invalid(payload.get("logical_order_id")):
        errors.append("logical_order_id must be a non-empty string")
    if _optional_string_is_invalid(payload.get("root_order_id")):
        errors.append("root_order_id must be a non-empty string")
    if _optional_string_is_invalid(payload.get("parent_order_id")):
        errors.append("parent_order_id must be a non-empty string")

    lineage_relation = _lineage_relation_or_none(payload.get("lineage_relation"))
    if payload.get("lineage_relation") is not None and lineage_relation is None:
        errors.append("lineage_relation is invalid")
    placement_kind = _placement_kind_or_none(payload.get("placement_kind"))
    if payload.get("placement_kind") is not None and placement_kind is None:
        errors.append("placement_kind is invalid")

    source_order_ids = _source_order_ids_or_none(payload.get("source_order_ids"))
    if payload.get("source_order_ids") is not None and source_order_ids is None:
        errors.append("source_order_ids must be a list of unique non-empty strings")
    if errors:
        return errors

    logical_order_id = _logical_order_id(command, payload)
    relation = lineage_relation or OrderLineageRelation.ROOT
    kind = placement_kind or OrderPlacementKind.INITIAL
    existing_logical_order = projection.logical_orders_by_id.get(logical_order_id)

    if existing_logical_order is not None:
        if kind not in {
            OrderPlacementKind.AMEND,
            OrderPlacementKind.CANCEL_REPLACE,
            OrderPlacementKind.RELEASE,
            OrderPlacementKind.STAGED_RELEASE,
        }:
            errors.append(
                "existing logical_order_id requires amend, cancel_replace, release, "
                "or staged_release placement_kind"
            )
        if existing_logical_order.product_id != payload.get("product_id"):
            errors.append("product_id must match existing logical_order_id")
        if existing_logical_order.side.value != payload.get("side"):
            errors.append("side must match existing logical_order_id")
        return errors

    if kind in {
        OrderPlacementKind.AMEND,
        OrderPlacementKind.CANCEL_REPLACE,
        OrderPlacementKind.RELEASE,
    }:
        errors.append("amend, cancel_replace, and release placement_kind require an existing logical_order_id")

    parent_order_id = _string_or_none(payload.get("parent_order_id"))
    root_order_id = _string_or_none(payload.get("root_order_id"))
    source_ids = source_order_ids or ()
    try:
        LogicalOrderRecord(
            logical_order_id=logical_order_id,
            lineage_relation=relation,
            root_order_id=root_order_id,
            parent_order_id=parent_order_id,
            source_order_ids=source_ids,
            product_id=str(payload["product_id"]),
            side=OrderSide(str(payload["side"])),
            size=str(payload["size"]),
            limit_price=_string_or_none(payload.get("limit_price")),
            created_by_action_id=command.action_id,
        )
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        return errors

    if relation in {OrderLineageRelation.FOLLOWUP_AFTER_FILL, OrderLineageRelation.SPLIT_CHILD}:
        parent = projection.logical_orders_by_id.get(parent_order_id or "")
        if parent is None:
            errors.append("parent_order_id must reference an existing logical order")
        if root_order_id not in projection.logical_orders_by_id:
            errors.append("root_order_id must reference an existing logical order")
        missing_sources = [source_id for source_id in source_ids if source_id not in projection.logical_orders_by_id]
        if missing_sources:
            errors.append("source_order_ids must reference existing logical orders")
        if parent is not None and parent.product_id != payload.get("product_id"):
            errors.append("product_id must match parent_order_id")
        if relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL and parent is not None:
            if parent.side.value == payload.get("side"):
                errors.append("followup_after_fill side must be opposite parent side")
        if relation == OrderLineageRelation.SPLIT_CHILD and parent is not None:
            if parent.side.value != payload.get("side"):
                errors.append("split_child side must match parent side")

    if relation == OrderLineageRelation.CONSOLIDATION:
        missing_sources = [source_id for source_id in source_ids if source_id not in projection.logical_orders_by_id]
        if missing_sources:
            errors.append("source_order_ids must reference existing logical orders")
        for source_id in source_ids:
            source = projection.logical_orders_by_id.get(source_id)
            if source is None:
                continue
            if source.product_id != payload.get("product_id"):
                errors.append("product_id must match source_order_ids")
            if source.side.value != payload.get("side"):
                errors.append("side must match source_order_ids")
    return errors


def _validate_cancel_order_payload(payload: Mapping[str, JsonValue]) -> tuple[ActionRejectionReason | None, list[str]]:
    if _non_empty_string(payload.get("exchange_order_id")) or _non_empty_string(payload.get("client_order_id")):
        return None, []
    return ActionRejectionReason.VALIDATION_FAILED, ["exchange_order_id or client_order_id is required"]


def _execution_failure_reason(result: ExecutionResult) -> ActionFailureReason | None:
    if result.status == ExecutionStatus.REJECTED:
        return ActionFailureReason.EXECUTION_REJECTED
    if result.status == ExecutionStatus.FAILED:
        return ActionFailureReason.EXECUTION_FAILED
    return None


def _execution_failure_message(result: ExecutionResult) -> str:
    if result.error_message:
        return result.error_message
    if result.error_code is not None:
        return f"executor returned {result.status.value}: {result.error_code}"
    return f"executor returned {result.status.value}"


def _validate_execution_result(
    command: ActionCommand,
    result: ExecutionResult,
    projection: SourceOfTruthProjection,
) -> None:
    if result.action_id != command.action_id:
        raise ActionExecutorContractError(
            "executor returned mismatched action_id: "
            f"expected {command.action_id}, observed {result.action_id}",
            context={
                "expected_action_id": command.action_id,
                "observed_action_id": result.action_id,
            },
        )
    if result.action_type != command.action_type:
        raise ActionExecutorContractError(
            "executor returned mismatched action_type: "
            f"expected {command.action_type.value}, observed {result.action_type.value}",
            context={
                "expected_action_type": command.action_type.value,
                "observed_action_type": result.action_type.value,
            },
        )
    valid_statuses = valid_execution_statuses_for_action_type(command.action_type)
    if result.status not in valid_statuses:
        raise ActionExecutorContractError(
            "executor returned invalid status for action_type: "
            f"action_type {command.action_type.value}, observed status {result.status.value}",
            context={
                "action_type": command.action_type.value,
                "allowed_statuses": [status.value for status in valid_statuses],
                "observed_status": result.status.value,
            },
        )
    if (
        command.action_type == ActionType.PLACE_ORDER
        and result.status == ExecutionStatus.ACCEPTED
        and result.client_order_id is None
        and result.exchange_order_id is None
    ):
        raise ActionExecutorContractError(
            "executor accepted place order without a venue order identifier",
            context={
                "action_id": command.action_id,
                "action_type": command.action_type.value,
                "execution_status": result.status.value,
            },
        )
    expected_client_order_id = _expected_place_client_order_id(command)
    if (
        expected_client_order_id is not None
        and result.client_order_id is not None
        and result.client_order_id != expected_client_order_id
    ):
        raise ActionExecutorContractError(
            "executor returned mismatched client_order_id: "
            f"expected {expected_client_order_id}, observed {result.client_order_id}",
            context={
                "expected_client_order_id": expected_client_order_id,
                "observed_client_order_id": result.client_order_id,
            },
        )
    if command.action_type == ActionType.PLACE_ORDER:
        _assert_order_identifier_is_available(
            action_id=command.action_id,
            identifier=result.client_order_id,
            identifier_type="client_order_id",
            indexed_orders=projection.orders_by_client_order_id,
        )
        _assert_order_identifier_is_available(
            action_id=command.action_id,
            identifier=result.exchange_order_id,
            identifier_type="exchange_order_id",
            indexed_orders=projection.orders_by_exchange_order_id,
        )


def _expected_place_client_order_id(command: ActionCommand) -> str | None:
    if command.action_type != ActionType.PLACE_ORDER:
        return None
    return command.idempotency_key or command.action_id


def _logical_order_id(command: ActionCommand, payload: Mapping[str, JsonValue]) -> str:
    return _string_or_none(payload.get("logical_order_id")) or command.action_id


def _lineage_relation(payload: Mapping[str, JsonValue]) -> OrderLineageRelation:
    return _lineage_relation_or_none(payload.get("lineage_relation")) or OrderLineageRelation.ROOT


def _lineage_relation_or_none(value: Any) -> OrderLineageRelation | None:
    try:
        return OrderLineageRelation(value)
    except (TypeError, ValueError):
        return None


def _placement_kind(payload: Mapping[str, JsonValue]) -> OrderPlacementKind:
    return _placement_kind_or_none(payload.get("placement_kind")) or OrderPlacementKind.INITIAL


def _placement_kind_or_none(value: Any) -> OrderPlacementKind | None:
    try:
        return OrderPlacementKind(value)
    except (TypeError, ValueError):
        return None


def _is_staged_release(command: ActionCommand) -> bool:
    if command.action_type != ActionType.PLACE_ORDER:
        return False
    payload = command.to_payload()["payload"]
    if not isinstance(payload, dict):
        return False
    return _placement_kind(payload) == OrderPlacementKind.STAGED_RELEASE


def _assert_order_identifier_is_available(
    *,
    action_id: str,
    identifier: str | None,
    identifier_type: str,
    indexed_orders: Mapping[str, Any],
) -> None:
    if identifier is None:
        return
    existing_order = indexed_orders.get(identifier)
    existing_action_id = getattr(existing_order, "action_id", None)
    if existing_order is None or existing_action_id == action_id:
        return
    raise ActionExecutorContractError(
        f"executor returned duplicate {identifier_type}: "
        f"{identifier} already belongs to action_id {existing_action_id}",
        context={
            "conflicting_action_id": existing_action_id,
            "identifier": identifier,
            "identifier_type": identifier_type,
            "observed_action_id": action_id,
        },
    )


def _enum_or_none(enum_type: type[Enum], value: Any) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError):
        return None


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _payload_dict(value: Any) -> dict[str, JsonValue]:
    if isinstance(value, Mapping):
        normalized = normalize_json(value)
        if isinstance(normalized, dict):
            return normalized
    return {}


def _decimal_strings_equal(left: str, right: str) -> bool:
    try:
        return Decimal(left) == Decimal(right)
    except (InvalidOperation, ValueError):
        return False


def _optional_string_is_invalid(value: Any) -> bool:
    return value is not None and not _non_empty_string(value)


def _source_order_ids(payload: Mapping[str, JsonValue]) -> tuple[str, ...]:
    return _source_order_ids_or_none(payload.get("source_order_ids")) or ()


def _source_order_ids_or_none(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return ()
    if not isinstance(value, list):
        return None
    source_order_ids: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            return None
        source_order_ids.append(item)
    if len(set(source_order_ids)) != len(source_order_ids):
        return None
    return tuple(source_order_ids)


def _positive_decimal_string(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = Decimal(value)
        if not parsed.is_finite() or parsed <= 0:
            return None
    except (InvalidOperation, ValueError):
        return None
    return value
