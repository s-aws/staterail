from __future__ import annotations

import pytest

from core.enums import ErrorCategory, ErrorCode, RuntimeTask
from core.errors import (
    ActionExecutorContractError,
    ERROR_ENVELOPE_SCHEMA_VERSION,
    ErrorEnvelope,
    FeedSourceError,
    exception_to_error_payload,
)


def test_bot_error_payload_uses_typed_category_and_preserves_context():
    payload = exception_to_error_payload(
        FeedSourceError("socket dropped", context={"source_id": "coinbase-primary"}),
        context={"attempt": 2},
    )

    assert payload["error_category"] == ErrorCategory.FEED_SOURCE.value
    assert payload["error_code"] == ErrorCode.FEED_SOURCE_FAILED.value
    assert payload["retryable"] is True
    assert payload["source_id"] == "coinbase-primary"
    assert payload["attempt"] == 2
    assert payload["error"] == {
        "category": ErrorCategory.FEED_SOURCE.value,
        "code": ErrorCode.FEED_SOURCE_FAILED.value,
        "context": {"source_id": "coinbase-primary", "attempt": 2},
        "exception_type": "FeedSourceError",
        "message": "socket dropped",
        "retryable": True,
        "schema_version": ERROR_ENVELOPE_SCHEMA_VERSION,
    }


def test_action_executor_contract_error_uses_specific_error_code():
    payload = exception_to_error_payload(
        ActionExecutorContractError(
            "executor returned mismatched action_id",
            context={"expected_action_id": "place-1", "observed_action_id": "place-2"},
        ),
        context={"execution_started_sequence": 3},
    )

    assert payload["error_category"] == ErrorCategory.ACTION_EXECUTOR.value
    assert payload["error_code"] == ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED.value
    assert payload["exception_type"] == "ActionExecutorContractError"
    assert payload["expected_action_id"] == "place-1"
    assert payload["observed_action_id"] == "place-2"
    assert payload["execution_started_sequence"] == 3


def test_plain_exception_payload_uses_call_site_category():
    payload = exception_to_error_payload(
        RuntimeError("task failed"),
        category=ErrorCategory.RUNTIME_TASK,
        context={"task_id": RuntimeTask.WATCHDOG},
        error_code=ErrorCode.RUNTIME_TASK_FAILED,
    )

    assert payload["error_category"] == ErrorCategory.RUNTIME_TASK.value
    assert payload["error_code"] == ErrorCode.RUNTIME_TASK_FAILED.value
    assert payload["task_id"] == RuntimeTask.WATCHDOG.value
    assert payload["exception_type"] == "RuntimeError"


def test_error_envelope_requires_typed_category():
    with pytest.raises(TypeError, match="ErrorCategory"):
        ErrorEnvelope(category="runtime_task", message="failed")
