from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.bootstrap import CoinbaseApplicationConfig
from app.config_fingerprint import (
    CONFIG_FINGERPRINT_ALGORITHM,
    application_config_fingerprint,
)
from audit.ledger import AuditLedger, AuditRecord
from core.engine import AuditCore
from core.enums import (
    EventType,
    PreflightGateIssue,
    PreflightStep,
    ReadinessRequirement,
    ReadinessStatus,
)
from core.errors import ConfigError
from core.json_tools import JsonValue, normalize_json


LIVE_NO_ORDER_PREFLIGHT_RESULT_SCHEMA_VERSION = 1
LIVE_NO_ORDER_PREFLIGHT_STEP_ORDER = (
    PreflightStep.READINESS,
    PreflightStep.PRODUCT_CATALOG_SMOKE,
    PreflightStep.FEED_SMOKE,
    PreflightStep.EXCHANGE_STATE_SMOKE,
)


def record_live_no_order_preflight_result(
    config: CoinbaseApplicationConfig,
    payload: dict[str, JsonValue],
) -> AuditRecord:
    record_payload = live_no_order_preflight_record_payload(config, payload)
    return AuditCore(AuditLedger(config.ledger_path)).emit(
        EventType.LIVE_PREFLIGHT_RESULT,
        record_payload,
    )


def live_no_order_preflight_record_payload(
    config: CoinbaseApplicationConfig,
    payload: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    record_payload = {
        "completed_step_names": _string_list(payload.get("completed_step_names")),
        "config_fingerprint": application_config_fingerprint(config),
        "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
        "ledger_path": config.ledger_path.as_posix(),
        "order_endpoint_called": payload.get("order_endpoint_called") is True,
        "runtime_tasks_started": payload.get("runtime_tasks_started") is True,
        "schema_version": LIVE_NO_ORDER_PREFLIGHT_RESULT_SCHEMA_VERSION,
        "skipped_step_names": _string_list(payload.get("skipped_step_names")),
        "status": _string_or_none(payload.get("status")),
        "step_statuses": _step_statuses(payload.get("steps")),
        "stopped_after_step": _string_or_none(payload.get("stopped_after_step")),
        "strategy_tasks_started": payload.get("strategy_tasks_started") is True,
    }
    normalized = normalize_json(record_payload)
    if not isinstance(normalized, dict):
        raise TypeError("live preflight result payload must normalize to an object")
    return normalized


def live_no_order_preflight_gate_payload(
    config: CoinbaseApplicationConfig,
    *,
    max_age: timedelta | None = None,
    now: datetime | None = None,
) -> dict[str, JsonValue]:
    if max_age is not None and max_age <= timedelta(0):
        raise ValueError("max_age must be positive")

    checked_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    current_fingerprint = application_config_fingerprint(config)
    records = AuditLedger(config.ledger_path).iter_records()
    result_records = tuple(
        record for record in records if record.event_type == EventType.LIVE_PREFLIGHT_RESULT
    )
    latest_result = result_records[-1] if result_records else None

    matching_record: AuditRecord | None = None
    matching_issues: tuple[PreflightGateIssue, ...] = ()
    for record in reversed(result_records):
        issues = _preflight_record_issues(
            record,
            current_fingerprint=current_fingerprint,
            max_age=max_age,
            now=checked_at,
        )
        if not issues:
            matching_record = record
            break
        matching_issues = issues

    if matching_record is None:
        attention_reasons = (
            (PreflightGateIssue.MISSING,)
            if latest_result is None
            else matching_issues
        )
    else:
        attention_reasons = ()

    payload = {
        "attention_reasons": [reason.value for reason in attention_reasons],
        "checked_at": checked_at.isoformat(),
        "config_fingerprint": current_fingerprint,
        "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
        "latest_result": _record_summary(latest_result),
        "ledger_path": config.ledger_path.as_posix(),
        "max_age_seconds": max_age.total_seconds() if max_age is not None else None,
        "matching_result": _record_summary(matching_record),
        "record_count": len(records),
        "result_record_count": len(result_records),
        "schema_version": LIVE_NO_ORDER_PREFLIGHT_RESULT_SCHEMA_VERSION,
        "status": (
            ReadinessStatus.ATTENTION_REQUIRED
            if attention_reasons
            else ReadinessStatus.OK
        ),
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("live preflight gate payload must normalize to an object")
    return normalized


def enforce_live_no_order_preflight_gate(
    config: CoinbaseApplicationConfig,
    *,
    max_age: timedelta | None = None,
) -> None:
    if not config.bot.live_rest_execution_enabled():
        return
    payload = live_no_order_preflight_gate_payload(config, max_age=max_age)
    if payload.get("status") == ReadinessStatus.OK.value:
        return
    raise ConfigError(
        "live REST execution requires a clean "
        f"{ReadinessRequirement.LIVE_NO_ORDER_PREFLIGHT.value} result for the current config fingerprint",
        context=payload,
    )


def _preflight_record_issues(
    record: AuditRecord,
    *,
    current_fingerprint: str,
    max_age: timedelta | None,
    now: datetime,
) -> tuple[PreflightGateIssue, ...]:
    payload = _payload_dict(record.payload)
    issues: list[PreflightGateIssue] = []

    if payload.get("schema_version") != LIVE_NO_ORDER_PREFLIGHT_RESULT_SCHEMA_VERSION:
        issues.append(PreflightGateIssue.UNSUPPORTED_SCHEMA_VERSION)
    if payload.get("config_fingerprint") != current_fingerprint:
        issues.append(PreflightGateIssue.CONFIG_FINGERPRINT_MISMATCH)
    if payload.get("fingerprint_algorithm") != CONFIG_FINGERPRINT_ALGORITHM:
        issues.append(PreflightGateIssue.CONFIG_FINGERPRINT_MISMATCH)
    if payload.get("status") != ReadinessStatus.OK.value:
        issues.append(PreflightGateIssue.STATUS_NOT_OK)
    if _string_list(payload.get("completed_step_names")) != [
        step.value for step in LIVE_NO_ORDER_PREFLIGHT_STEP_ORDER
    ]:
        issues.append(PreflightGateIssue.STEPS_INCOMPLETE)
    if _string_list(payload.get("skipped_step_names")):
        issues.append(PreflightGateIssue.STEPS_INCOMPLETE)
    if any(status.get("status") != ReadinessStatus.OK.value for status in _step_statuses(payload.get("step_statuses"))):
        issues.append(PreflightGateIssue.STEP_NOT_OK)
    if payload.get("order_endpoint_called") is True:
        issues.append(PreflightGateIssue.ORDER_ENDPOINT_CALLED)
    if payload.get("runtime_tasks_started") is True:
        issues.append(PreflightGateIssue.RUNTIME_TASKS_STARTED)
    if payload.get("strategy_tasks_started") is True:
        issues.append(PreflightGateIssue.STRATEGY_TASKS_STARTED)
    if max_age is not None and now - record.occurred_at.astimezone(timezone.utc) > max_age:
        issues.append(PreflightGateIssue.EXPIRED)

    return tuple(dict.fromkeys(issues))


def _record_summary(record: AuditRecord | None) -> dict[str, JsonValue] | None:
    if record is None:
        return None
    payload = _payload_dict(record.payload)
    summary = {
        "completed_step_names": _string_list(payload.get("completed_step_names")),
        "config_fingerprint": _string_or_none(payload.get("config_fingerprint")),
        "occurred_at": record.occurred_at.astimezone(timezone.utc).isoformat(),
        "record_hash": record.record_hash,
        "sequence": record.sequence,
        "status": _string_or_none(payload.get("status")),
    }
    normalized = normalize_json(summary)
    if not isinstance(normalized, dict):
        raise TypeError("live preflight record summary must normalize to an object")
    return normalized


def _step_statuses(value: JsonValue) -> list[dict[str, JsonValue]]:
    if not isinstance(value, list):
        return []
    statuses: list[dict[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        statuses.append(
            {
                "name": _string_or_none(item.get("name")),
                "status": _string_or_none(item.get("status")),
            }
        )
    return statuses


def _payload_dict(value: JsonValue) -> dict[str, JsonValue]:
    normalized = normalize_json(value)
    return normalized if isinstance(normalized, dict) else {}


def _string_list(value: JsonValue) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
