from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from core.enums import ErrorCategory, ErrorCode
from core.json_tools import JsonValue, normalize_json


ERROR_ENVELOPE_SCHEMA_VERSION = 1
ErrorCodeValue = ErrorCode | str | None
_DEFAULT_ERROR_CODE_BY_CATEGORY = {
    ErrorCategory.ACTION_EXECUTOR: ErrorCode.ACTION_EXECUTOR_FAILED,
    ErrorCategory.AUDIT_LEDGER: ErrorCode.AUDIT_LEDGER_FAILED,
    ErrorCategory.CONFIG: ErrorCode.CONFIG_INVALID,
    ErrorCategory.EXCHANGE_AUTH: ErrorCode.EXCHANGE_AUTH_FAILED,
    ErrorCategory.EXCHANGE_RATE_LIMIT: ErrorCode.EXCHANGE_RATE_LIMITED,
    ErrorCategory.EXCHANGE_TRANSPORT: ErrorCode.EXCHANGE_TRANSPORT_FAILED,
    ErrorCategory.FEED_SOURCE: ErrorCode.FEED_SOURCE_FAILED,
    ErrorCategory.HOOK: ErrorCode.HOOK_FAILED,
    ErrorCategory.RECONCILIATION: ErrorCode.RECONCILIATION_LOOKUP_FAILED,
    ErrorCategory.RUNTIME_TASK: ErrorCode.RUNTIME_TASK_FAILED,
    ErrorCategory.STRATEGY: ErrorCode.STRATEGY_EVALUATION_FAILED,
    ErrorCategory.UNEXPECTED: ErrorCode.UNEXPECTED_EXCEPTION,
}


class BotError(Exception):
    category: ErrorCategory = ErrorCategory.UNEXPECTED
    default_error_code: ErrorCode = ErrorCode.UNEXPECTED_EXCEPTION
    default_retryable = False

    def __init__(
        self,
        message: str,
        *,
        context: Mapping[str, Any] | None = None,
        error_code: ErrorCodeValue = None,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.context = _context_payload(context)
        self.error_code = _error_code_value(error_code or self.default_error_code)
        self.retryable = self.default_retryable if retryable is None else retryable


class ConfigError(BotError, ValueError):
    category = ErrorCategory.CONFIG
    default_error_code = ErrorCode.CONFIG_INVALID


class AuditLedgerError(BotError, RuntimeError):
    category = ErrorCategory.AUDIT_LEDGER
    default_error_code = ErrorCode.AUDIT_LEDGER_FAILED


class AuditIntegrityError(AuditLedgerError):
    default_error_code = ErrorCode.AUDIT_INTEGRITY_FAILED


class ExchangeTransportError(BotError):
    category = ErrorCategory.EXCHANGE_TRANSPORT
    default_error_code = ErrorCode.EXCHANGE_TRANSPORT_FAILED
    default_retryable = True


class ExchangeAuthError(BotError):
    category = ErrorCategory.EXCHANGE_AUTH
    default_error_code = ErrorCode.EXCHANGE_AUTH_FAILED


class ExchangeRateLimitError(BotError):
    category = ErrorCategory.EXCHANGE_RATE_LIMIT
    default_error_code = ErrorCode.EXCHANGE_RATE_LIMITED
    default_retryable = True


class FeedSourceError(BotError):
    category = ErrorCategory.FEED_SOURCE
    default_error_code = ErrorCode.FEED_SOURCE_FAILED
    default_retryable = True


class RuntimeTaskError(BotError):
    category = ErrorCategory.RUNTIME_TASK
    default_error_code = ErrorCode.RUNTIME_TASK_FAILED


class ActionExecutorError(BotError):
    category = ErrorCategory.ACTION_EXECUTOR
    default_error_code = ErrorCode.ACTION_EXECUTOR_FAILED


class ActionExecutorContractError(ActionExecutorError, ValueError):
    default_error_code = ErrorCode.ACTION_EXECUTOR_CONTRACT_FAILED


class ReconciliationError(BotError):
    category = ErrorCategory.RECONCILIATION
    default_error_code = ErrorCode.RECONCILIATION_LOOKUP_FAILED


class StrategyError(BotError):
    category = ErrorCategory.STRATEGY
    default_error_code = ErrorCode.STRATEGY_EVALUATION_FAILED


class StrategyContractError(StrategyError, ValueError):
    default_error_code = ErrorCode.STRATEGY_CONTRACT_FAILED


class StrategyActionSubmissionError(StrategyError):
    default_error_code = ErrorCode.STRATEGY_ACTION_FAILED


class StrategyInputUnavailableError(StrategyError):
    default_error_code = ErrorCode.STRATEGY_INPUT_UNAVAILABLE


@dataclass(frozen=True)
class ErrorEnvelope:
    category: ErrorCategory
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)
    error_code: ErrorCodeValue = None
    exception_type: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.category, ErrorCategory):
            raise TypeError("category must be an ErrorCategory")
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable must be a bool")
        if not self.message:
            raise ValueError("message is required")

    def to_payload(self) -> dict[str, JsonValue]:
        context = _context_payload(self.context)
        error_code = _error_code_value(self.error_code)
        envelope: dict[str, JsonValue] = {
            "category": self.category.value,
            "code": error_code,
            "context": context,
            "exception_type": self.exception_type,
            "message": self.message,
            "retryable": self.retryable,
            "schema_version": ERROR_ENVELOPE_SCHEMA_VERSION,
        }
        payload: dict[str, JsonValue] = {
            "error": envelope,
            "error_category": self.category.value,
            "error_code": error_code,
            "error_schema_version": ERROR_ENVELOPE_SCHEMA_VERSION,
            "exception_type": self.exception_type,
            "message": self.message,
            "retryable": self.retryable,
        }
        for key, value in context.items():
            if key not in payload:
                payload[key] = value
        return payload


def exception_to_error_payload(
    exc: Exception,
    *,
    category: ErrorCategory = ErrorCategory.UNEXPECTED,
    context: Mapping[str, Any] | None = None,
    error_code: ErrorCodeValue = None,
    retryable: bool | None = None,
) -> dict[str, JsonValue]:
    if isinstance(exc, BotError):
        category = exc.category
        error_code = error_code or exc.error_code
        retryable = exc.retryable if retryable is None else retryable
        merged_context = {**exc.context, **_context_payload(context)}
    else:
        error_code = error_code or _DEFAULT_ERROR_CODE_BY_CATEGORY.get(category, ErrorCode.UNEXPECTED_EXCEPTION)
        retryable = False if retryable is None else retryable
        merged_context = _context_payload(context)

    return ErrorEnvelope(
        category=category,
        context=merged_context,
        error_code=error_code,
        exception_type=exc.__class__.__name__,
        message=str(exc) or exc.__class__.__name__,
        retryable=retryable,
    ).to_payload()


def error_event_payload(
    *,
    category: ErrorCategory,
    message: str,
    context: Mapping[str, Any] | None = None,
    error_code: ErrorCodeValue = None,
    exception_type: str | None = None,
    retryable: bool = False,
) -> dict[str, JsonValue]:
    return ErrorEnvelope(
        category=category,
        context=_context_payload(context),
        error_code=error_code,
        exception_type=exception_type,
        message=message,
        retryable=retryable,
    ).to_payload()


def _context_payload(context: Mapping[str, Any] | None) -> dict[str, JsonValue]:
    if context is None:
        return {}
    normalized = normalize_json(context)
    if not isinstance(normalized, dict):
        raise TypeError("error context must normalize to a JSON object")
    return normalized


def _error_code_value(error_code: ErrorCodeValue) -> str | None:
    if isinstance(error_code, ErrorCode):
        return error_code.value
    if error_code is None:
        return None
    if isinstance(error_code, str) and error_code:
        return error_code
    raise TypeError("error_code must be an ErrorCode or non-empty string")
