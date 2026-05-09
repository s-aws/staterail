from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from core.enums import ActionType, ErrorCategory, ErrorCode, ExecutionMode, ExecutionStatus
from core.json_tools import JsonValue, normalize_json


_VALID_EXECUTION_STATUSES_BY_ACTION_TYPE = {
    ActionType.CANCEL_ORDER: (
        ExecutionStatus.CANCELLED,
        ExecutionStatus.REJECTED,
        ExecutionStatus.FAILED,
    ),
    ActionType.PLACE_ORDER: (
        ExecutionStatus.ACCEPTED,
        ExecutionStatus.REJECTED,
        ExecutionStatus.FAILED,
    ),
}


@dataclass(frozen=True)
class ExecutionResult:
    action_id: str
    action_type: ActionType
    status: ExecutionStatus
    mode: ExecutionMode
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    raw_response: Mapping[str, Any] = field(default_factory=dict)
    error_category: ErrorCategory | None = None
    error_code: ErrorCode | str | None = None
    error_message: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if not self.action_id:
            raise ValueError("action_id is required")
        if not isinstance(self.action_type, ActionType):
            raise TypeError("action_type must be an ActionType")
        if not isinstance(self.status, ExecutionStatus):
            raise TypeError("status must be an ExecutionStatus")
        if not isinstance(self.mode, ExecutionMode):
            raise TypeError("mode must be an ExecutionMode")
        if self.status in {ExecutionStatus.FAILED, ExecutionStatus.REJECTED} and not self.error_message:
            raise ValueError("error_message is required for failed or rejected execution results")
        if self.error_category is not None and not isinstance(self.error_category, ErrorCategory):
            raise TypeError("error_category must be an ErrorCategory")
        if self.error_code is not None and not (
            isinstance(self.error_code, ErrorCode)
            or (isinstance(self.error_code, str) and self.error_code)
        ):
            raise TypeError("error_code must be an ErrorCode or non-empty string")
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable must be a bool")

    def to_payload(self) -> dict[str, JsonValue]:
        raw_response = normalize_json(self.raw_response)
        if not isinstance(raw_response, dict):
            raise TypeError("raw_response must normalize to a JSON object")
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.value,
            "client_order_id": self.client_order_id,
            "error_category": self.error_category.value if self.error_category is not None else None,
            "error_code": self.error_code.value if isinstance(self.error_code, ErrorCode) else self.error_code,
            "error_message": self.error_message,
            "exchange_order_id": self.exchange_order_id,
            "mode": self.mode.value,
            "raw_response": raw_response,
            "retryable": self.retryable,
            "status": self.status.value,
        }


def valid_execution_statuses_for_action_type(action_type: ActionType) -> tuple[ExecutionStatus, ...]:
    if not isinstance(action_type, ActionType):
        raise TypeError("action_type must be an ActionType")
    return _VALID_EXECUTION_STATUSES_BY_ACTION_TYPE.get(action_type, ())
