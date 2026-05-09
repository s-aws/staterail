from __future__ import annotations

from hashlib import sha256

from actions.execution import ExecutionResult
from actions.gateway import ActionCommand
from core.enums import ActionType, ErrorCode, ExecutionMode, ExecutionStatus
from core.json_tools import canonical_json


class DryRunExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        if command.action_type == ActionType.PLACE_ORDER:
            return self._place_order(command)
        if command.action_type == ActionType.CANCEL_ORDER:
            return self._cancel_order(command)
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=ExecutionMode.DRY_RUN,
            error_code=ErrorCode.UNSUPPORTED_ACTION_TYPE,
            error_message="unsupported action type",
            raw_response={"action": command.to_payload()},
        )

    def _place_order(self, command: ActionCommand) -> ExecutionResult:
        client_order_id = command.idempotency_key or command.action_id
        exchange_order_id = f"dry-run-{_stable_suffix(command)}"
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.ACCEPTED,
            mode=ExecutionMode.DRY_RUN,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            raw_response={
                "accepted": True,
                "client_order_id": client_order_id,
                "exchange_order_id": exchange_order_id,
                "simulated": True,
            },
        )

    def _cancel_order(self, command: ActionCommand) -> ExecutionResult:
        payload = command.to_payload()["payload"]
        client_order_id = _string_or_none(payload.get("client_order_id")) if isinstance(payload, dict) else None
        exchange_order_id = _string_or_none(payload.get("exchange_order_id")) if isinstance(payload, dict) else None
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.CANCELLED,
            mode=ExecutionMode.DRY_RUN,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            raw_response={
                "cancelled": True,
                "client_order_id": client_order_id,
                "exchange_order_id": exchange_order_id,
                "simulated": True,
            },
        )


def _stable_suffix(command: ActionCommand) -> str:
    return sha256(canonical_json(command.to_payload()).encode("utf-8")).hexdigest()[:16]


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
