from __future__ import annotations

import hashlib
from pathlib import Path

from app.ledger_health import ledger_health_payload
from audit.ledger import AuditLedger, AuditRecord
from core.engine import AuditCore
from core.enums import (
    EventType,
    LedgerHealthAcknowledgementIssue,
    LedgerHealthStatus,
)
from core.errors import ConfigError
from core.json_tools import JsonValue, canonical_json, normalize_json


LEDGER_HEALTH_ACKNOWLEDGEMENT_SCHEMA_VERSION = 1


def acknowledge_ledger_health(
    path: str | Path,
    *,
    acknowledged_by: str,
    reason: str,
) -> dict[str, JsonValue]:
    health = ledger_health_payload(path)
    if health.get("status") != LedgerHealthStatus.ATTENTION_REQUIRED.value:
        raise ConfigError(
            "ledger health acknowledgement requires attention_required health",
            context={
                "ledger_path": Path(path).as_posix(),
                "ledger_health_status": _string_or_none(health.get("status")),
            },
        )

    record_payload = ledger_health_acknowledgement_record_payload(
        path,
        health_payload=health,
        acknowledged_by=acknowledged_by,
        reason=reason,
    )
    record = AuditCore(AuditLedger(path)).emit(
        EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED,
        record_payload,
    )
    status = ledger_health_acknowledgement_status(
        path,
        health_payload=ledger_health_payload(path),
    )
    payload = {
        "acknowledgement": {
            "record_hash": record.record_hash,
            "sequence": record.sequence,
            **record_payload,
        },
        "acknowledgement_status": status,
        "ledger_health_status": health.get("status"),
        "ledger_path": Path(path).as_posix(),
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("ledger health acknowledgement payload must normalize to an object")
    return normalized


def ledger_health_acknowledgement_record_payload(
    path: str | Path,
    *,
    health_payload: dict[str, JsonValue],
    acknowledged_by: str,
    reason: str,
) -> dict[str, JsonValue]:
    acknowledged_by = acknowledged_by.strip()
    reason = reason.strip()
    if not acknowledged_by:
        raise ValueError("acknowledged_by is required")
    if not reason:
        raise ValueError("reason is required")

    payload = {
        "acknowledged_by": acknowledged_by,
        "acknowledged_health_status": _string_or_none(health_payload.get("status")),
        "acknowledged_through_hash": _string_or_none(health_payload.get("last_hash")),
        "acknowledged_through_sequence": _int_or_none(health_payload.get("last_sequence")),
        "attention_check_count": len(ledger_health_attention_check_summaries(health_payload)),
        "attention_checks": ledger_health_attention_check_summaries(health_payload),
        "ledger_health_attention_digest": ledger_health_attention_digest(health_payload),
        "ledger_path": Path(path).as_posix(),
        "reason": reason,
        "schema_version": LEDGER_HEALTH_ACKNOWLEDGEMENT_SCHEMA_VERSION,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("ledger health acknowledgement record payload must normalize to an object")
    return normalized


def ledger_health_acknowledgement_status(
    path: str | Path,
    *,
    health_payload: dict[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    health = health_payload if health_payload is not None else ledger_health_payload(path)
    digest = ledger_health_attention_digest(health)
    ledger = AuditLedger(path)
    records = ledger.iter_records()
    latest_acknowledgement: dict[str, JsonValue] | None = None
    latest_issues: tuple[LedgerHealthAcknowledgementIssue, ...] = ()

    for record in reversed(records):
        if record.event_type != EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED:
            continue

        issues = _acknowledgement_issues(
            record,
            records=records,
            expected_digest=digest,
        )
        summary = _acknowledgement_summary(record, issues=issues)
        if latest_acknowledgement is None:
            latest_acknowledgement = summary
            latest_issues = issues
        if not issues:
            return _acknowledgement_status_payload(
                acknowledged=True,
                digest=digest,
                matching_acknowledgement=summary,
                latest_acknowledgement=latest_acknowledgement,
                latest_issues=latest_issues,
            )

    return _acknowledgement_status_payload(
        acknowledged=False,
        digest=digest,
        matching_acknowledgement=None,
        latest_acknowledgement=latest_acknowledgement,
        latest_issues=latest_issues,
    )


def ledger_health_attention_digest(health_payload: dict[str, JsonValue]) -> str:
    digest_payload = {
        "attention_checks": ledger_health_attention_check_summaries(health_payload),
        "ledger_health_status": _string_or_none(health_payload.get("status")),
        "schema_version": LEDGER_HEALTH_ACKNOWLEDGEMENT_SCHEMA_VERSION,
    }
    return hashlib.sha256(canonical_json(digest_payload).encode("utf-8")).hexdigest()


def ledger_health_attention_checks(
    health_payload: dict[str, JsonValue],
) -> list[dict[str, JsonValue]]:
    checks = health_payload.get("checks")
    if not isinstance(checks, list):
        return []

    attention_checks: list[dict[str, JsonValue]] = []
    for item in checks:
        if not isinstance(item, dict):
            continue
        status = _string_or_none(item.get("status"))
        if status == LedgerHealthStatus.OK.value:
            continue
        name = _string_or_none(item.get("name"))
        if name is None or status is None:
            continue
        details = item.get("details")
        normalized_details = normalize_json(details if details is not None else {})
        attention_checks.append(
            {
                "count": _int_or_none(item.get("count")) or 0,
                "details": normalized_details if isinstance(normalized_details, dict) else {},
                "name": name,
                "status": status,
            }
        )
    return sorted(attention_checks, key=lambda check: str(check["name"]))


def ledger_health_attention_check_summaries(
    health_payload: dict[str, JsonValue],
) -> list[dict[str, JsonValue]]:
    return [
        {
            "count": check["count"],
            "name": check["name"],
            "status": check["status"],
        }
        for check in ledger_health_attention_checks(health_payload)
    ]


def _acknowledgement_issues(
    record: AuditRecord,
    *,
    records: tuple[AuditRecord, ...],
    expected_digest: str,
) -> tuple[LedgerHealthAcknowledgementIssue, ...]:
    payload = _payload_dict(record.payload)
    issues: list[LedgerHealthAcknowledgementIssue] = []

    if payload.get("schema_version") != LEDGER_HEALTH_ACKNOWLEDGEMENT_SCHEMA_VERSION:
        issues.append(LedgerHealthAcknowledgementIssue.UNSUPPORTED_SCHEMA_VERSION)
    if payload.get("ledger_health_attention_digest") != expected_digest:
        issues.append(LedgerHealthAcknowledgementIssue.DIGEST_MISMATCH)
    if _string_or_none(payload.get("acknowledged_by")) is None or _string_or_none(payload.get("reason")) is None:
        issues.append(LedgerHealthAcknowledgementIssue.MISSING_OPERATOR_REVIEW)

    acknowledged_sequence = _int_or_none(payload.get("acknowledged_through_sequence"))
    acknowledged_hash = _string_or_none(payload.get("acknowledged_through_hash"))
    acknowledged_record = (
        next((item for item in records if item.sequence == acknowledged_sequence), None)
        if acknowledged_sequence is not None
        else None
    )
    if acknowledged_record is None or acknowledged_record.record_hash != acknowledged_hash:
        issues.append(LedgerHealthAcknowledgementIssue.ACKNOWLEDGED_THROUGH_MISMATCH)

    stale_records = [
        item
        for item in records
        if acknowledged_sequence is not None
        and item.sequence > acknowledged_sequence
        and item.event_type != EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED
    ]
    if stale_records:
        issues.append(LedgerHealthAcknowledgementIssue.STALE_AFTER_ACKNOWLEDGEMENT)

    return tuple(dict.fromkeys(issues))


def _acknowledgement_status_payload(
    *,
    acknowledged: bool,
    digest: str,
    matching_acknowledgement: dict[str, JsonValue] | None,
    latest_acknowledgement: dict[str, JsonValue] | None,
    latest_issues: tuple[LedgerHealthAcknowledgementIssue, ...],
) -> dict[str, JsonValue]:
    payload = {
        "acknowledged": acknowledged,
        "attention_digest": digest,
        "latest_acknowledgement": latest_acknowledgement,
        "latest_issues": [issue.value for issue in latest_issues],
        "matching_acknowledgement": matching_acknowledgement,
        "schema_version": LEDGER_HEALTH_ACKNOWLEDGEMENT_SCHEMA_VERSION,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("ledger health acknowledgement status must normalize to an object")
    return normalized


def _acknowledgement_summary(
    record: AuditRecord,
    *,
    issues: tuple[LedgerHealthAcknowledgementIssue, ...],
) -> dict[str, JsonValue]:
    payload = _payload_dict(record.payload)
    summary = {
        "acknowledged_by": _string_or_none(payload.get("acknowledged_by")),
        "acknowledged_through_hash": _string_or_none(payload.get("acknowledged_through_hash")),
        "acknowledged_through_sequence": _int_or_none(payload.get("acknowledged_through_sequence")),
        "attention_check_count": _int_or_none(payload.get("attention_check_count")),
        "ledger_health_attention_digest": _string_or_none(
            payload.get("ledger_health_attention_digest")
        ),
        "reason": _string_or_none(payload.get("reason")),
        "record_hash": record.record_hash,
        "sequence": record.sequence,
        "validation_issues": [issue.value for issue in issues],
    }
    normalized = normalize_json(summary)
    if not isinstance(normalized, dict):
        raise TypeError("ledger health acknowledgement summary must normalize to an object")
    return normalized


def _payload_dict(value: JsonValue) -> dict[str, JsonValue]:
    normalized = normalize_json(value)
    return normalized if isinstance(normalized, dict) else {}


def _int_or_none(value: JsonValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _string_or_none(value: JsonValue) -> str | None:
    return value if isinstance(value, str) and value else None
