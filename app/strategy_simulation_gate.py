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
    ExecutionMode,
    ReadinessRequirement,
    ReadinessStatus,
    StrategyEvaluationStatus,
    StrategySimulationGateIssue,
    StrategySimulationStatus,
)
from core.errors import ConfigError
from core.json_tools import JsonValue, normalize_json


STRATEGY_SIMULATION_RESULT_SCHEMA_VERSION = 1


def record_strategy_simulation_result(
    config: CoinbaseApplicationConfig,
    payload: dict[str, JsonValue],
) -> AuditRecord:
    record_payload = strategy_simulation_result_record_payload(config, payload)
    return AuditCore(AuditLedger(config.ledger_path)).emit(
        EventType.STRATEGY_SIMULATION_RESULT,
        record_payload,
    )


def strategy_simulation_result_record_payload(
    config: CoinbaseApplicationConfig,
    payload: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    ledger = _payload_dict(payload.get("ledger"))
    record_payload = {
        "accepted_action_count": _int_or_zero(payload.get("accepted_action_count")),
        "as_of_sequence": _int_or_none(payload.get("as_of_sequence")),
        "completed_count": _int_or_zero(payload.get("completed_count")),
        "config_fingerprint": application_config_fingerprint(config),
        "evaluated_at": _string_or_none(payload.get("evaluated_at")),
        "evaluation_statuses": _evaluation_statuses(payload.get("evaluations")),
        "execution_mode": _string_or_none(payload.get("execution_mode")),
        "failed_count": _int_or_zero(payload.get("failed_count")),
        "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
        "intent_count": _int_or_zero(payload.get("intent_count")),
        "ledger_path": config.ledger_path.as_posix(),
        "order_endpoint_called": False,
        "read_only": payload.get("read_only") is True,
        "rejected_action_count": _int_or_zero(payload.get("rejected_action_count")),
        "runtime_tasks_started": False,
        "schema_version": STRATEGY_SIMULATION_RESULT_SCHEMA_VERSION,
        "simulated_ledger": {
            "last_hash": _string_or_none(ledger.get("last_hash")),
            "ledger_path": _string_or_none(ledger.get("ledger_path")),
            "record_count": _int_or_zero(ledger.get("record_count")),
            "verified": ledger.get("verified") is True,
        },
        "status": _string_or_none(payload.get("status")),
        "strategy_count": _int_or_zero(payload.get("strategy_count")),
        "strategy_ids": list(config.bot.strategies.strategy_ids),
        "strategy_tasks_started": False,
    }
    normalized = normalize_json(record_payload)
    if not isinstance(normalized, dict):
        raise TypeError("strategy simulation result payload must normalize to an object")
    return normalized


def strategy_simulation_gate_payload(
    config: CoinbaseApplicationConfig,
    *,
    max_age: timedelta | None = None,
    now: datetime | None = None,
) -> dict[str, JsonValue]:
    if max_age is not None and max_age <= timedelta(0):
        raise ValueError("max_age must be positive")

    checked_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    current_fingerprint = application_config_fingerprint(config)
    current_strategy_ids = list(config.bot.strategies.strategy_ids)
    required = config.bot.live_rest_execution_enabled() and config.bot.strategies.schedule.enabled
    records = AuditLedger(config.ledger_path).iter_records()
    result_records = tuple(
        record for record in records if record.event_type == EventType.STRATEGY_SIMULATION_RESULT
    )
    latest_result = result_records[-1] if result_records else None

    matching_record: AuditRecord | None = None
    matching_issues: tuple[StrategySimulationGateIssue, ...] = ()
    for record in reversed(result_records):
        issues = _simulation_record_issues(
            record,
            current_execution_mode=config.bot.rest.execution_mode,
            current_fingerprint=current_fingerprint,
            current_strategy_ids=current_strategy_ids,
            max_age=max_age,
            now=checked_at,
        )
        if not issues:
            matching_record = record
            break
        matching_issues = issues

    if not required:
        attention_reasons = ()
    elif matching_record is None:
        attention_reasons = (
            (StrategySimulationGateIssue.MISSING,)
            if latest_result is None
            else matching_issues
        )
    else:
        attention_reasons = ()

    payload = {
        "attention_reasons": [reason.value for reason in attention_reasons],
        "checked_at": checked_at.isoformat(),
        "config_fingerprint": current_fingerprint,
        "execution_mode": config.bot.rest.execution_mode.value,
        "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
        "latest_result": _record_summary(latest_result),
        "ledger_path": config.ledger_path.as_posix(),
        "matching_result": _record_summary(matching_record),
        "max_age_seconds": max_age.total_seconds() if max_age is not None else None,
        "record_count": len(records),
        "required": required,
        "result_record_count": len(result_records),
        "schema_version": STRATEGY_SIMULATION_RESULT_SCHEMA_VERSION,
        "status": (
            ReadinessStatus.ATTENTION_REQUIRED
            if attention_reasons
            else ReadinessStatus.OK
        ),
        "strategy_ids": current_strategy_ids,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("strategy simulation gate payload must normalize to an object")
    return normalized


def enforce_live_strategy_simulation_gate(
    config: CoinbaseApplicationConfig,
    *,
    max_age: timedelta | None = None,
) -> None:
    if not config.bot.live_rest_execution_enabled():
        return
    if not config.bot.strategies.schedule.enabled:
        return

    payload = strategy_simulation_gate_payload(config, max_age=max_age)
    if payload.get("status") == ReadinessStatus.OK.value:
        return
    raise ConfigError(
        "live strategy execution requires a clean "
        f"{ReadinessRequirement.STRATEGY_SIMULATION.value} result for the current config fingerprint",
        context=payload,
    )


def _simulation_record_issues(
    record: AuditRecord,
    *,
    current_execution_mode: ExecutionMode,
    current_fingerprint: str,
    current_strategy_ids: list[str],
    max_age: timedelta | None,
    now: datetime,
) -> tuple[StrategySimulationGateIssue, ...]:
    payload = _payload_dict(record.payload)
    issues: list[StrategySimulationGateIssue] = []

    if payload.get("schema_version") != STRATEGY_SIMULATION_RESULT_SCHEMA_VERSION:
        issues.append(StrategySimulationGateIssue.UNSUPPORTED_SCHEMA_VERSION)
    if payload.get("config_fingerprint") != current_fingerprint:
        issues.append(StrategySimulationGateIssue.CONFIG_FINGERPRINT_MISMATCH)
    if payload.get("fingerprint_algorithm") != CONFIG_FINGERPRINT_ALGORITHM:
        issues.append(StrategySimulationGateIssue.CONFIG_FINGERPRINT_MISMATCH)
    if _string_list(payload.get("strategy_ids")) != current_strategy_ids:
        issues.append(StrategySimulationGateIssue.STRATEGY_IDS_MISMATCH)
    if payload.get("execution_mode") != current_execution_mode.value:
        issues.append(StrategySimulationGateIssue.EXECUTION_MODE_MISMATCH)
    if payload.get("status") != StrategySimulationStatus.OK.value:
        issues.append(StrategySimulationGateIssue.STATUS_NOT_OK)
    if payload.get("read_only") is not True:
        issues.append(StrategySimulationGateIssue.READ_ONLY_FALSE)
    failed_count = _int_or_none(payload.get("failed_count"))
    if failed_count is None or failed_count < 0:
        issues.append(StrategySimulationGateIssue.PAYLOAD_INVALID)
    elif failed_count > 0:
        issues.append(StrategySimulationGateIssue.FAILED_EVALUATION)
    rejected_action_count = _int_or_none(payload.get("rejected_action_count"))
    if rejected_action_count is None or rejected_action_count < 0:
        issues.append(StrategySimulationGateIssue.PAYLOAD_INVALID)
    elif rejected_action_count > 0:
        issues.append(StrategySimulationGateIssue.REJECTED_ACTION_PREVIEW)
    simulated_ledger = _payload_dict(payload.get("simulated_ledger"))
    if simulated_ledger.get("verified") is not True:
        issues.append(StrategySimulationGateIssue.PAYLOAD_INVALID)
    if payload.get("order_endpoint_called") is True:
        issues.append(StrategySimulationGateIssue.ORDER_ENDPOINT_CALLED)
    if payload.get("runtime_tasks_started") is True:
        issues.append(StrategySimulationGateIssue.RUNTIME_TASKS_STARTED)
    if payload.get("strategy_tasks_started") is True:
        issues.append(StrategySimulationGateIssue.STRATEGY_TASKS_STARTED)
    if max_age is not None and now - record.occurred_at.astimezone(timezone.utc) > max_age:
        issues.append(StrategySimulationGateIssue.EXPIRED)

    return tuple(dict.fromkeys(issues))


def _record_summary(record: AuditRecord | None) -> dict[str, JsonValue] | None:
    if record is None:
        return None
    payload = _payload_dict(record.payload)
    summary = {
        "accepted_action_count": _int_or_zero(payload.get("accepted_action_count")),
        "config_fingerprint": _string_or_none(payload.get("config_fingerprint")),
        "failed_count": _int_or_zero(payload.get("failed_count")),
        "occurred_at": record.occurred_at.astimezone(timezone.utc).isoformat(),
        "record_hash": record.record_hash,
        "rejected_action_count": _int_or_zero(payload.get("rejected_action_count")),
        "sequence": record.sequence,
        "status": _string_or_none(payload.get("status")),
        "strategy_ids": _string_list(payload.get("strategy_ids")),
    }
    normalized = normalize_json(summary)
    if not isinstance(normalized, dict):
        raise TypeError("strategy simulation record summary must normalize to an object")
    return normalized


def _evaluation_statuses(value: JsonValue) -> list[dict[str, JsonValue]]:
    if not isinstance(value, list):
        return []
    statuses: list[dict[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        statuses.append(
            {
                "accepted_action_count": _int_or_zero(item.get("accepted_action_count")),
                "intent_count": _int_or_zero(item.get("intent_count")),
                "rejected_action_count": _int_or_zero(item.get("rejected_action_count")),
                "status": _string_or_none(item.get("status")),
                "strategy_id": _string_or_none(item.get("strategy_id")),
            }
        )
    return statuses


def _payload_dict(value: JsonValue) -> dict[str, JsonValue]:
    normalized = normalize_json(value)
    return normalized if isinstance(normalized, dict) else {}


def _int_or_none(value: JsonValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _int_or_zero(value: JsonValue) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None and parsed >= 0 else 0


def _string_list(value: JsonValue) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
