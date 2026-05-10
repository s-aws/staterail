from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

from app.config_fingerprint import APPLICATION_CONFIG_SCHEMA_VERSION, CONFIG_FINGERPRINT_ALGORITHM
from actions.execution import valid_execution_statuses_for_action_type
from app.ledger_view import load_verified_ledger_view
from audit.anchors import LedgerAnchorReceipt
from audit.archives import LedgerArchiveReceipt
from audit.ledger import AuditRecord
from core.enums import (
    ActionStatus,
    ActionType,
    AnchorStoreType,
    ErrorCategory,
    EventType,
    ExchangeLookupStatus,
    ExecutionMode,
    ExecutionStatus,
    FeedStopReason,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OperatorCanaryEvidenceIssue,
    PreflightStep,
    ReadinessStatus,
    RuntimeComponent,
    RuntimeStopReason,
    RuntimeTask,
    MarketDataKind,
    StrategyEvaluationStatus,
    StrategyInputStatus,
    StrategySimulationStatus,
    TriggerRelation,
)
from core.json_tools import JsonValue, canonical_json, normalize_json
from core.order_update_contract import OrderUpdateContractResult, validate_exchange_order_update
from exchanges.coinbase.venues import COINBASE_LIVE_EXECUTION_PRODUCT_VENUES
from projections.state import ErrorSnapshot, FeedDegradationSnapshot, SourceOfTruthProjection, SystemStartSnapshot
from reconciliation.exchange_state_contract import (
    validate_exchange_balance_snapshot,
    validate_exchange_position_snapshot,
)


AnchorReceiptVerifier = Callable[[LedgerAnchorReceipt], Mapping[str, JsonValue] | None]
ArchiveReceiptVerifier = Callable[[LedgerArchiveReceipt], Mapping[str, JsonValue] | None]


@dataclass(frozen=True)
class LedgerHealthCheckResult:
    name: LedgerHealthCheckName
    status: LedgerHealthStatus
    count: int = 0
    details: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, LedgerHealthCheckName):
            raise TypeError("name must be a LedgerHealthCheckName")
        if not isinstance(self.status, LedgerHealthStatus):
            raise TypeError("status must be a LedgerHealthStatus")
        if self.count < 0:
            raise ValueError("count must not be negative")

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "count": self.count,
            "details": self.details,
            "name": self.name,
            "status": self.status,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Ledger health check payload must normalize to an object")
        return normalized


@dataclass(frozen=True)
class LedgerHealth:
    ledger_path: Path
    status: LedgerHealthStatus
    verified: bool
    audit_anchor_count: int
    audit_archive_count: int
    audit_checkpoint_count: int
    checks: tuple[LedgerHealthCheckResult, ...]
    last_hash: str
    last_sequence: int
    next_sequence: int
    record_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.status, LedgerHealthStatus):
            raise TypeError("status must be a LedgerHealthStatus")

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "audit_anchor_count": self.audit_anchor_count,
            "audit_archive_count": self.audit_archive_count,
            "audit_checkpoint_count": self.audit_checkpoint_count,
            "checks": [check.to_payload() for check in self.checks],
            "last_hash": self.last_hash,
            "last_sequence": self.last_sequence,
            "ledger_path": self.ledger_path.as_posix(),
            "next_sequence": self.next_sequence,
            "record_count": self.record_count,
            "status": self.status,
            "verified": self.verified,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Ledger health payload must normalize to an object")
        return normalized


def ledger_health(
    path: str | Path,
    *,
    anchor_receipt_verifier: AnchorReceiptVerifier | None = None,
    archive_receipt_verifier: ArchiveReceiptVerifier | None = None,
    max_records_after_anchor: int | None = None,
    max_records_after_archive: int | None = None,
) -> LedgerHealth:
    if max_records_after_anchor is not None and max_records_after_anchor < 0:
        raise ValueError("max_records_after_anchor must not be negative")
    if max_records_after_archive is not None and max_records_after_archive < 0:
        raise ValueError("max_records_after_archive must not be negative")

    view = load_verified_ledger_view(path)
    projection = view.projection

    unresolved_reconciliation_count = sum(
        1
        for key in projection.reconciliation_mismatches
        if key not in projection.reconciliation_recoveries
    )
    failed_reconciliation_recovery_count = sum(
        1
        for recovery in projection.reconciliation_recoveries.values()
        if recovery.lookup_status == ExchangeLookupStatus.FAILED
    )
    reconciliation_drift_count = projection.reconciliation_drift_count
    failed_action_count = sum(1 for action in projection.actions.values() if action.status == ActionStatus.FAILED)
    execution_unknown_order_count = sum(
        1
        for order in projection.orders_by_action_id.values()
        if order.lifecycle_status == OrderLifecycleStatus.EXECUTION_UNKNOWN
    )
    sequence_anomaly_count = len(projection.sequence_gap_sequences) + len(projection.out_of_order_sequences)

    checks = (
        LedgerHealthCheckResult(
            name=LedgerHealthCheckName.AUDIT_INTEGRITY,
            status=LedgerHealthStatus.OK,
            count=len(view.records),
            details={
                "audit_anchor_count": view.audit_anchor_count,
                "audit_archive_count": view.audit_archive_count,
                "audit_checkpoint_count": view.audit_checkpoint_count,
            },
        ),
        _startup_config_contract_check(view.records),
        _system_lifecycle_contract_check(view.records),
        _check(
            LedgerHealthCheckName.ANCHOR_COVERAGE,
            count=_unanchored_checkpoint_count(projection),
            details=_anchor_coverage_details(projection),
        ),
        _anchor_freshness_check(
            projection,
            max_records_after_anchor=max_records_after_anchor,
        ),
        _anchor_remote_verification_check(
            projection,
            anchor_receipt_verifier=anchor_receipt_verifier,
        ),
        _archive_freshness_check(
            projection,
            max_records_after_archive=max_records_after_archive,
        ),
        _archive_remote_verification_check(
            projection,
            archive_receipt_verifier=archive_receipt_verifier,
        ),
        _check(
            LedgerHealthCheckName.ERROR_EVENTS,
            count=projection.error_count,
            details=_error_event_details(
                projection.errors,
                projection_parse_issue_count=projection.error_count - len(projection.errors),
            ),
        ),
        _feed_lifecycle_contract_check(view.records),
        _data_flow_contract_check(view.records),
        _trigger_contract_check(view.records),
        _strategy_contract_check(view.records),
        _strategy_simulation_contract_check(view.records),
        _operator_canary_evidence_result_contract_check(view.records),
        _ledger_health_acknowledgement_contract_check(view.records),
        _runtime_health_check_result_contract_check(view.records),
        _runtime_task_contract_check(view.records),
        _action_execution_contract_check(view.records),
        _action_lifecycle_contract_check(view.records),
        _order_identity_contract_check(view.records),
        _order_lineage_contract_check(view.records),
        _order_update_contract_check(view.records),
        _check(
            LedgerHealthCheckName.EXECUTION_UNCERTAINTY,
            count=failed_action_count + execution_unknown_order_count,
            details={
                "execution_unknown_order_count": execution_unknown_order_count,
                "failed_action_count": failed_action_count,
            },
        ),
        _exchange_fill_contract_check(view.records, projection),
        _exchange_state_contract_check(view.records),
        _live_preflight_contract_check(view.records),
        _live_execution_venue_check(projection),
        _check(
            LedgerHealthCheckName.FEED_DEGRADATION,
            count=projection.feed_degraded_count,
            details=_feed_degradation_details(projection.feed_degradations),
        ),
        _product_catalog_freshness_check(projection),
        _check(
            LedgerHealthCheckName.RECONCILIATION,
            count=(
                unresolved_reconciliation_count
                + failed_reconciliation_recovery_count
                + reconciliation_drift_count
            ),
            details={
                "exchange_state_drift_count": reconciliation_drift_count,
                "failed_recovery_count": failed_reconciliation_recovery_count,
                "unresolved_mismatch_count": unresolved_reconciliation_count,
            },
        ),
        _check(
            LedgerHealthCheckName.SEQUENCE_ANOMALIES,
            count=sequence_anomaly_count,
            details={
                "out_of_order_count": len(projection.out_of_order_sequences),
                "sequence_gap_count": len(projection.sequence_gap_sequences),
            },
        ),
    )
    status = (
        LedgerHealthStatus.ATTENTION_REQUIRED
        if any(check.status != LedgerHealthStatus.OK for check in checks)
        else LedgerHealthStatus.OK
    )
    return LedgerHealth(
        audit_anchor_count=view.audit_anchor_count,
        audit_archive_count=view.audit_archive_count,
        audit_checkpoint_count=view.audit_checkpoint_count,
        checks=checks,
        last_hash=view.state.last_hash,
        last_sequence=view.projection.last_sequence,
        ledger_path=view.ledger_path,
        next_sequence=view.state.next_sequence,
        record_count=len(view.records),
        status=status,
        verified=True,
    )


def ledger_health_payload(
    path: str | Path,
    *,
    anchor_receipt_verifier: AnchorReceiptVerifier | None = None,
    archive_receipt_verifier: ArchiveReceiptVerifier | None = None,
    max_records_after_anchor: int | None = None,
    max_records_after_archive: int | None = None,
) -> dict[str, JsonValue]:
    return ledger_health(
        path,
        anchor_receipt_verifier=anchor_receipt_verifier,
        archive_receipt_verifier=archive_receipt_verifier,
        max_records_after_anchor=max_records_after_anchor,
        max_records_after_archive=max_records_after_archive,
    ).to_payload()


def _check(
    name: LedgerHealthCheckName,
    *,
    count: int,
    details: dict[str, JsonValue] | None = None,
) -> LedgerHealthCheckResult:
    return LedgerHealthCheckResult(
        count=count,
        details=details or {},
        name=name,
        status=LedgerHealthStatus.ATTENTION_REQUIRED if count else LedgerHealthStatus.OK,
    )


def _startup_config_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    system_start_count = 0

    for record in records:
        if record.event_type != EventType.SYSTEM_STARTED:
            continue

        system_start_count += 1
        payload = _dict_or_empty(record.payload)
        startup_metadata = _dict_or_empty(payload.get("startup_metadata"))
        application_config = _dict_or_empty(startup_metadata.get("application_config"))
        snapshot = _dict_or_empty(application_config.get("snapshot"))
        fingerprint = _string_or_none(application_config.get("fingerprint"))
        fingerprint_algorithm = _string_or_none(application_config.get("fingerprint_algorithm"))
        schema_version = _int_or_none(application_config.get("schema_version"))
        snapshot_schema_version = _int_or_none(snapshot.get("schema_version"))
        calculated_fingerprint = _sha256(canonical_json(snapshot)) if snapshot else None

        application_config_present = bool(application_config)
        snapshot_present = bool(snapshot)
        fingerprint_present = fingerprint is not None
        fingerprint_algorithm_valid = fingerprint_algorithm == CONFIG_FINGERPRINT_ALGORITHM
        schema_version_present = schema_version is not None
        snapshot_schema_version_matches = (
            schema_version is not None
            and snapshot_schema_version is not None
            and snapshot_schema_version == schema_version
        )
        fingerprint_matches = (
            fingerprint is not None
            and calculated_fingerprint is not None
            and fingerprint == calculated_fingerprint
        )

        if (
            application_config_present
            and snapshot_present
            and fingerprint_present
            and fingerprint_algorithm_valid
            and schema_version_present
            and snapshot_schema_version_matches
            and fingerprint_matches
        ):
            continue

        anomalies.append(
            {
                "application_config_present": application_config_present,
                "calculated_fingerprint": calculated_fingerprint,
                "component": _string_or_none(payload.get("component")),
                "expected_fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
                "event_type": record.event_type.value,
                "fingerprint": fingerprint,
                "fingerprint_algorithm": fingerprint_algorithm,
                "fingerprint_algorithm_valid": fingerprint_algorithm_valid,
                "fingerprint_matches": fingerprint_matches,
                "fingerprint_present": fingerprint_present,
                "schema_version": schema_version,
                "schema_version_present": schema_version_present,
                "sequence": record.sequence,
                "snapshot_present": snapshot_present,
                "snapshot_schema_version": snapshot_schema_version,
                "snapshot_schema_version_matches": snapshot_schema_version_matches,
            }
        )

    return _check(
        LedgerHealthCheckName.STARTUP_CONFIG_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "current_application_config_schema_version": APPLICATION_CONFIG_SCHEMA_VERSION,
            "expected_fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
            "system_start_count": system_start_count,
        },
    )


def _system_lifecycle_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    active_starts_by_component: dict[RuntimeComponent, int] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        if record.event_type not in {EventType.SYSTEM_STARTED, EventType.SYSTEM_STOPPED}:
            continue

        payload = _dict_or_empty(record.payload)
        component = _runtime_component_or_none(payload.get("component"))
        component_valid = component is not None

        if record.event_type == EventType.SYSTEM_STARTED:
            started_at = _string_or_none(payload.get("started_at"))
            task_count = _int_or_none(payload.get("task_count"))
            task_count_valid = task_count is not None and task_count > 0
            already_started_sequence = (
                active_starts_by_component.get(component) if component is not None else None
            )
            already_started = already_started_sequence is not None

            if component is None or started_at is None or not task_count_valid or already_started:
                anomalies.append(
                    {
                        "already_started": already_started,
                        "already_started_sequence": already_started_sequence,
                        "component": _string_or_none(payload.get("component")),
                        "component_valid": component_valid,
                        "event_type": record.event_type.value,
                        "sequence": record.sequence,
                        "started_at": started_at,
                        "started_at_present": started_at is not None,
                        "task_count": task_count,
                        "task_count_valid": task_count_valid,
                    }
                )
            if component is not None and started_at is not None and task_count_valid:
                active_starts_by_component[component] = record.sequence
            continue

        stopped_at = _string_or_none(payload.get("stopped_at"))
        completed_cycles = _int_or_none(payload.get("completed_cycles"))
        reason = _runtime_stop_reason_or_none(payload.get("reason"))
        completed_cycles_valid = completed_cycles is not None and completed_cycles >= 0
        active_start_sequence = active_starts_by_component.get(component) if component is not None else None
        active_start_found = active_start_sequence is not None

        if (
            component is None
            or stopped_at is None
            or not completed_cycles_valid
            or reason is None
            or not active_start_found
        ):
            anomalies.append(
                {
                    "active_start_found": active_start_found,
                    "active_start_sequence": active_start_sequence,
                    "completed_cycles": completed_cycles,
                    "completed_cycles_valid": completed_cycles_valid,
                    "component": _string_or_none(payload.get("component")),
                    "component_valid": component_valid,
                    "event_type": record.event_type.value,
                    "reason": _string_or_none(payload.get("reason")),
                    "reason_valid": reason is not None,
                    "sequence": record.sequence,
                    "stopped_at": stopped_at,
                    "stopped_at_present": stopped_at is not None,
                }
            )
        if component is not None and active_start_found:
            active_starts_by_component.pop(component, None)

    return _check(
        LedgerHealthCheckName.SYSTEM_LIFECYCLE_CONTRACT,
        count=len(anomalies),
        details={
            "active_components": [component.value for component in active_starts_by_component],
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _anchor_coverage_details(projection: SourceOfTruthProjection) -> dict[str, JsonValue]:
    audit_checkpoints = projection.audit_checkpoints
    audit_anchors = projection.audit_anchors
    anchored_checkpoint_hashes = {anchor.checkpoint_hash for anchor in audit_anchors}
    unanchored = [
        checkpoint
        for checkpoint in audit_checkpoints
        if checkpoint.checkpoint_hash not in anchored_checkpoint_hashes
    ]
    return {
        "anchor_count": len(audit_anchors),
        "anchored_checkpoint_count": len(audit_checkpoints) - len(unanchored),
        "checkpoint_count": len(audit_checkpoints),
        "latest_anchor_checkpoint_hash": audit_anchors[-1].checkpoint_hash if audit_anchors else None,
        "latest_checkpoint_hash": audit_checkpoints[-1].checkpoint_hash if audit_checkpoints else None,
        "unanchored_checkpoint_count": len(unanchored),
        "unanchored_checkpoint_hashes": [checkpoint.checkpoint_hash for checkpoint in unanchored],
    }


def _unanchored_checkpoint_count(projection: SourceOfTruthProjection) -> int:
    details = _anchor_coverage_details(projection)
    count = details["unanchored_checkpoint_count"]
    if not isinstance(count, int):
        raise TypeError("unanchored checkpoint count must be an int")
    return count


def _anchor_freshness_check(
    projection: SourceOfTruthProjection,
    *,
    max_records_after_anchor: int | None,
) -> LedgerHealthCheckResult:
    details = _anchor_freshness_details(
        projection,
        max_records_after_anchor=max_records_after_anchor,
    )
    records_after_anchor = details["records_after_latest_anchor_checkpoint"]
    if not isinstance(records_after_anchor, int):
        raise TypeError("records_after_latest_anchor_checkpoint must be an int")
    count = (
        records_after_anchor
        if max_records_after_anchor is not None and records_after_anchor > max_records_after_anchor
        else 0
    )
    return _check(
        LedgerHealthCheckName.ANCHOR_FRESHNESS,
        count=count,
        details=details,
    )


def _anchor_freshness_details(
    projection: SourceOfTruthProjection,
    *,
    max_records_after_anchor: int | None,
) -> dict[str, JsonValue]:
    latest_anchor = projection.audit_anchors[-1] if projection.audit_anchors else None
    latest_checkpoint = projection.audit_checkpoints[-1] if projection.audit_checkpoints else None
    latest_anchor_through_sequence = (
        latest_anchor.checkpoint_through_sequence
        if latest_anchor is not None
        else None
    )
    latest_checkpoint_through_sequence = (
        latest_checkpoint.through_sequence
        if latest_checkpoint is not None
        else None
    )
    return {
        "latest_anchor_checkpoint_through_sequence": latest_anchor_through_sequence,
        "latest_anchor_record_sequence": latest_anchor.sequence if latest_anchor is not None else None,
        "latest_checkpoint_record_sequence": latest_checkpoint.sequence if latest_checkpoint is not None else None,
        "latest_checkpoint_through_sequence": latest_checkpoint_through_sequence,
        "max_records_after_anchor": max_records_after_anchor,
        "policy_configured": max_records_after_anchor is not None,
        "records_after_latest_anchor_checkpoint": _records_after_sequence(
            projection.last_sequence,
            latest_anchor_through_sequence,
        ),
        "records_after_latest_checkpoint": _records_after_sequence(
            projection.last_sequence,
            latest_checkpoint_through_sequence,
        ),
    }


def _anchor_remote_verification_check(
    projection: SourceOfTruthProjection,
    *,
    anchor_receipt_verifier: AnchorReceiptVerifier | None,
) -> LedgerHealthCheckResult:
    worm_anchors = [
        anchor
        for anchor in projection.audit_anchors
        if anchor.store_type == AnchorStoreType.WORM_OBJECT
    ]
    if anchor_receipt_verifier is None:
        return LedgerHealthCheckResult(
            count=0,
            details={
                "enabled": False,
                "verified_count": 0,
                "worm_anchor_count": len(worm_anchors),
            },
            name=LedgerHealthCheckName.ANCHOR_REMOTE_VERIFICATION,
            status=LedgerHealthStatus.OK,
        )

    failures: list[dict[str, JsonValue]] = []
    verified: list[dict[str, JsonValue]] = []
    for anchor in worm_anchors:
        try:
            receipt = LedgerAnchorReceipt.from_payload(anchor.payload)
            verification_payload = anchor_receipt_verifier(receipt) or {}
            normalized = normalize_json(verification_payload)
            if not isinstance(normalized, dict):
                raise TypeError("anchor receipt verifier must return a JSON object or None")
            verified.append(
                {
                    **normalized,
                    "artifact_uri": anchor.artifact_uri,
                    "sequence": anchor.sequence,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "artifact_uri": anchor.artifact_uri,
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "sequence": anchor.sequence,
                }
            )

    return _check(
        LedgerHealthCheckName.ANCHOR_REMOTE_VERIFICATION,
        count=len(failures),
        details={
            "enabled": True,
            "failure_count": len(failures),
            "failures": failures,
            "verified": verified,
            "verified_count": len(verified),
            "worm_anchor_count": len(worm_anchors),
        },
    )


def _archive_freshness_check(
    projection: SourceOfTruthProjection,
    *,
    max_records_after_archive: int | None,
) -> LedgerHealthCheckResult:
    details = _archive_freshness_details(
        projection,
        max_records_after_archive=max_records_after_archive,
    )
    records_after_archive = details["records_after_latest_archive"]
    if not isinstance(records_after_archive, int):
        raise TypeError("records_after_latest_archive must be an int")
    count = (
        records_after_archive
        if max_records_after_archive is not None and records_after_archive > max_records_after_archive
        else 0
    )
    return _check(
        LedgerHealthCheckName.ARCHIVE_FRESHNESS,
        count=count,
        details=details,
    )


def _archive_freshness_details(
    projection: SourceOfTruthProjection,
    *,
    max_records_after_archive: int | None,
) -> dict[str, JsonValue]:
    latest_archive = projection.audit_archives[-1] if projection.audit_archives else None
    latest_archive_through_sequence = (
        latest_archive.through_sequence
        if latest_archive is not None
        else None
    )
    return {
        "archive_count": len(projection.audit_archives),
        "latest_archive_record_count": latest_archive.record_count if latest_archive is not None else None,
        "latest_archive_record_sequence": latest_archive.sequence if latest_archive is not None else None,
        "latest_archive_through_sequence": latest_archive_through_sequence,
        "max_records_after_archive": max_records_after_archive,
        "policy_configured": max_records_after_archive is not None,
        "records_after_latest_archive": _records_after_sequence(
            projection.last_sequence,
            latest_archive_through_sequence,
        ),
    }


def _archive_remote_verification_check(
    projection: SourceOfTruthProjection,
    *,
    archive_receipt_verifier: ArchiveReceiptVerifier | None,
) -> LedgerHealthCheckResult:
    worm_archives = [
        archive
        for archive in projection.audit_archives
        if archive.store_type == AnchorStoreType.WORM_OBJECT
    ]
    if archive_receipt_verifier is None:
        return LedgerHealthCheckResult(
            count=0,
            details={
                "enabled": False,
                "verified_count": 0,
                "worm_archive_count": len(worm_archives),
            },
            name=LedgerHealthCheckName.ARCHIVE_REMOTE_VERIFICATION,
            status=LedgerHealthStatus.OK,
        )

    failures: list[dict[str, JsonValue]] = []
    verified: list[dict[str, JsonValue]] = []
    for archive in worm_archives:
        try:
            receipt = LedgerArchiveReceipt.from_payload(archive.payload)
            verification_payload = archive_receipt_verifier(receipt) or {}
            normalized = normalize_json(verification_payload)
            if not isinstance(normalized, dict):
                raise TypeError("archive receipt verifier must return a JSON object or None")
            verified.append(
                {
                    **normalized,
                    "artifact_uri": archive.artifact_uri,
                    "sequence": archive.sequence,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "artifact_uri": archive.artifact_uri,
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "sequence": archive.sequence,
                }
            )

    return _check(
        LedgerHealthCheckName.ARCHIVE_REMOTE_VERIFICATION,
        count=len(failures),
        details={
            "enabled": True,
            "failure_count": len(failures),
            "failures": failures,
            "verified": verified,
            "verified_count": len(verified),
            "worm_archive_count": len(worm_archives),
        },
    )


def _records_after_sequence(last_sequence: int, sequence: int | None) -> int:
    if sequence is None:
        return last_sequence
    return max(0, last_sequence - sequence)


def _error_event_details(
    errors: list[ErrorSnapshot],
    *,
    projection_parse_issue_count: int,
) -> dict[str, JsonValue]:
    by_category: dict[str, int] = {category.value: 0 for category in ErrorCategory}
    retryable_count = 0
    unclassified_count = 0
    for error in errors:
        if error.category is None:
            unclassified_count += 1
        else:
            by_category[error.category.value] += 1
        if error.retryable is True:
            retryable_count += 1
    return {
        "by_category": {category: count for category, count in by_category.items() if count},
        "projection_parse_issue_count": projection_parse_issue_count,
        "retryable_count": retryable_count,
        "unclassified_count": unclassified_count,
    }


def _feed_degradation_details(degradations: list[FeedDegradationSnapshot]) -> dict[str, JsonValue]:
    if not degradations:
        return {}
    latest = degradations[-1]
    return {
        "latest_connected_sources": list(latest.connected_sources),
        "latest_disconnected_sources": list(latest.disconnected_sources),
        "latest_live_count": latest.live_count,
        "latest_min_live_sources": latest.min_live_sources,
        "latest_sequence": latest.sequence,
        "latest_stale_sources": list(latest.stale_sources),
    }


def _feed_lifecycle_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    active_attempts_by_source: dict[str, int] = {}
    disconnected_attempts_by_source: dict[str, int] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        if record.event_type not in {
            EventType.FEED_CONNECTED,
            EventType.FEED_DEGRADED,
            EventType.FEED_DISCONNECTED,
            EventType.FEED_HEARTBEAT,
            EventType.FEED_RECONNECT_SCHEDULED,
        }:
            continue

        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.FEED_CONNECTED:
            source_id = _string_or_none(payload.get("source_id"))
            attempt = _int_or_none(payload.get("attempt"))
            attempt_valid = attempt is not None and attempt > 0
            already_connected = source_id in active_attempts_by_source if source_id is not None else False

            if source_id is None or not attempt_valid or already_connected:
                anomalies.append(
                    {
                        "already_connected": already_connected,
                        "attempt": attempt,
                        "attempt_valid": attempt_valid,
                        "event_type": record.event_type.value,
                        "sequence": record.sequence,
                        "source_id": source_id,
                        "source_id_present": source_id is not None,
                    }
                )
            if source_id is not None and attempt_valid:
                active_attempts_by_source[source_id] = attempt
            continue

        if record.event_type == EventType.FEED_DISCONNECTED:
            source_id = _string_or_none(payload.get("source_id"))
            attempt = _int_or_none(payload.get("attempt"))
            reason = _feed_stop_reason_or_none(payload.get("reason"))
            attempt_valid = attempt is not None and attempt > 0
            active_attempt = active_attempts_by_source.get(source_id) if source_id is not None else None
            active_connection_found = active_attempt is not None
            attempt_matches_active = (
                active_attempt == attempt if active_attempt is not None and attempt is not None else False
            )

            if (
                source_id is None
                or not attempt_valid
                or reason is None
                or not active_connection_found
                or not attempt_matches_active
            ):
                anomalies.append(
                    {
                        "active_attempt": active_attempt,
                        "active_connection_found": active_connection_found,
                        "attempt": attempt,
                        "attempt_matches_active": attempt_matches_active,
                        "attempt_valid": attempt_valid,
                        "event_type": record.event_type.value,
                        "reason": _string_or_none(payload.get("reason")),
                        "reason_valid": reason is not None,
                        "sequence": record.sequence,
                        "source_id": source_id,
                        "source_id_present": source_id is not None,
                    }
                )
            if source_id is not None:
                active_attempts_by_source.pop(source_id, None)
                if attempt_valid:
                    disconnected_attempts_by_source[source_id] = attempt
            continue

        if record.event_type == EventType.FEED_RECONNECT_SCHEDULED:
            source_id = _string_or_none(payload.get("source_id"))
            attempt = _int_or_none(payload.get("attempt"))
            delay_seconds = _float_or_none(payload.get("delay_seconds"))
            prior_disconnect_attempt = (
                disconnected_attempts_by_source.get(source_id) if source_id is not None else None
            )
            attempt_valid = attempt is not None and attempt > 0
            delay_valid = delay_seconds is not None and delay_seconds >= 0
            attempt_follows_disconnect = (
                prior_disconnect_attempt is not None
                and attempt is not None
                and attempt == prior_disconnect_attempt + 1
            )

            if source_id is None or not attempt_valid or not delay_valid or not attempt_follows_disconnect:
                anomalies.append(
                    {
                        "attempt": attempt,
                        "attempt_follows_disconnect": attempt_follows_disconnect,
                        "attempt_valid": attempt_valid,
                        "delay_seconds": delay_seconds,
                        "delay_valid": delay_valid,
                        "event_type": record.event_type.value,
                        "prior_disconnect_attempt": prior_disconnect_attempt,
                        "sequence": record.sequence,
                        "source_id": source_id,
                        "source_id_present": source_id is not None,
                    }
                )
            continue

        if record.event_type == EventType.FEED_HEARTBEAT:
            source_id = _string_or_none(payload.get("source_id"))
            message_key = _string_or_none(payload.get("message_key"))
            message_event_type = _event_type_or_none(payload.get("message_event_type"))
            received_at = _string_or_none(payload.get("received_at"))
            message_event_type_valid = message_event_type == EventType.FEED_HEARTBEAT

            if (
                source_id is None
                or message_key is None
                or received_at is None
                or not message_event_type_valid
            ):
                anomalies.append(
                    {
                        "event_type": record.event_type.value,
                        "message_event_type": _string_or_none(payload.get("message_event_type")),
                        "message_event_type_valid": message_event_type_valid,
                        "message_key": message_key,
                        "message_key_present": message_key is not None,
                        "received_at": received_at,
                        "received_at_present": received_at is not None,
                        "sequence": record.sequence,
                        "source_id": source_id,
                        "source_id_present": source_id is not None,
                    }
                )
            continue

        connected_sources_valid = _is_string_list(payload.get("connected_sources"))
        disconnected_sources_valid = _is_string_list(payload.get("disconnected_sources"))
        stale_sources_valid = _is_string_list(payload.get("stale_sources"))
        connected_sources = _string_list(payload.get("connected_sources"))
        disconnected_sources = _string_list(payload.get("disconnected_sources"))
        stale_sources = _string_list(payload.get("stale_sources"))
        live_count = _int_or_none(payload.get("live_count"))
        min_live_sources = _int_or_none(payload.get("min_live_sources"))
        live_count_valid = live_count is not None and live_count >= 0
        min_live_sources_valid = min_live_sources is not None and min_live_sources > 0
        live_count_matches_connected_sources = (
            live_count is not None and connected_sources_valid and live_count == len(connected_sources)
        )
        degraded_condition_valid = (
            live_count is not None
            and min_live_sources is not None
            and live_count < min_live_sources
        )
        all_sources = connected_sources + disconnected_sources + stale_sources
        source_sets_disjoint = len(all_sources) == len(set(all_sources))

        if (
            not connected_sources_valid
            or not disconnected_sources_valid
            or not stale_sources_valid
            or not live_count_valid
            or not min_live_sources_valid
            or not live_count_matches_connected_sources
            or not degraded_condition_valid
            or not source_sets_disjoint
        ):
            anomalies.append(
                {
                    "connected_sources": connected_sources,
                    "connected_sources_valid": connected_sources_valid,
                    "degraded_condition_valid": degraded_condition_valid,
                    "disconnected_sources": disconnected_sources,
                    "disconnected_sources_valid": disconnected_sources_valid,
                    "event_type": record.event_type.value,
                    "live_count": live_count,
                    "live_count_matches_connected_sources": live_count_matches_connected_sources,
                    "live_count_valid": live_count_valid,
                    "min_live_sources": min_live_sources,
                    "min_live_sources_valid": min_live_sources_valid,
                    "sequence": record.sequence,
                    "source_sets_disjoint": source_sets_disjoint,
                    "stale_sources": stale_sources,
                    "stale_sources_valid": stale_sources_valid,
                }
            )

    return _check(
        LedgerHealthCheckName.FEED_LIFECYCLE_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _data_flow_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    received_by_sequence: dict[int, dict[str, JsonValue]] = {}
    accepted_sequences_by_message_key: dict[str, int] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.DATA_RECEIVED:
            received_by_sequence[record.sequence] = payload
            message_key = _string_or_none(payload.get("message_key"))
            source_id = _string_or_none(payload.get("source_id"))
            message_event_type = _event_type_or_none(payload.get("message_event_type"))
            if message_key is not None and source_id is not None and message_event_type is not None:
                continue
            anomalies.append(
                {
                    "event_type": record.event_type.value,
                    "message_event_type": _string_or_none(payload.get("message_event_type")),
                    "message_event_type_valid": message_event_type is not None,
                    "message_key": message_key,
                    "message_key_present": message_key is not None,
                    "sequence": record.sequence,
                    "source_id": source_id,
                    "source_id_present": source_id is not None,
                }
            )
            continue

        if record.event_type not in {EventType.DATA_ACCEPTED, EventType.DATA_DUPLICATE}:
            continue

        message_key = _string_or_none(payload.get("message_key"))
        source_id = _string_or_none(payload.get("source_id"))
        received_sequence = _int_or_none(payload.get("received_sequence"))
        received = received_by_sequence.get(received_sequence) if received_sequence is not None else None
        received_message_key = _string_or_none(received.get("message_key")) if received is not None else None
        received_source_id = _string_or_none(received.get("source_id")) if received is not None else None
        received_message_event_type = (
            _event_type_or_none(received.get("message_event_type")) if received is not None else None
        )
        accepted_message_event_type = _event_type_or_none(payload.get("message_event_type"))
        prior_accepted_sequence = accepted_sequences_by_message_key.get(message_key) if message_key is not None else None

        received_reference_found = received is not None
        message_key_matches = (
            message_key is not None and received_message_key is not None and message_key == received_message_key
        )
        source_id_matches = source_id is not None and received_source_id is not None and source_id == received_source_id
        message_event_type_matches = (
            accepted_message_event_type is not None
            and received_message_event_type is not None
            and accepted_message_event_type == received_message_event_type
        )
        duplicate_accepted = record.event_type == EventType.DATA_ACCEPTED and prior_accepted_sequence is not None
        duplicate_has_prior_accept = record.event_type == EventType.DATA_DUPLICATE and prior_accepted_sequence is not None

        if record.event_type == EventType.DATA_ACCEPTED:
            valid = (
                received_reference_found
                and message_key_matches
                and source_id_matches
                and message_event_type_matches
                and not duplicate_accepted
            )
        else:
            valid = (
                received_reference_found
                and message_key_matches
                and source_id_matches
                and duplicate_has_prior_accept
            )

        if not valid:
            anomalies.append(
                {
                    "duplicate_accepted": duplicate_accepted if record.event_type == EventType.DATA_ACCEPTED else None,
                    "duplicate_has_prior_accept": (
                        duplicate_has_prior_accept if record.event_type == EventType.DATA_DUPLICATE else None
                    ),
                    "event_type": record.event_type.value,
                    "message_event_type": _string_or_none(payload.get("message_event_type")),
                    "message_event_type_matches": (
                        message_event_type_matches if record.event_type == EventType.DATA_ACCEPTED else None
                    ),
                    "message_key": message_key,
                    "message_key_matches": message_key_matches,
                    "prior_accepted_sequence": prior_accepted_sequence,
                    "received_message_event_type": (
                        received_message_event_type.value if received_message_event_type is not None else None
                    ),
                    "received_message_key": received_message_key,
                    "received_reference_found": received_reference_found,
                    "received_sequence": received_sequence,
                    "received_source_id": received_source_id,
                    "sequence": record.sequence,
                    "source_id": source_id,
                    "source_id_matches": source_id_matches,
                }
            )

        if record.event_type == EventType.DATA_ACCEPTED and message_key is not None and not duplicate_accepted:
            accepted_sequences_by_message_key[message_key] = record.sequence

    return _check(
        LedgerHealthCheckName.DATA_FLOW_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _trigger_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    records_by_sequence = {record.sequence: record for record in records}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        if record.event_type != EventType.TRIGGER_FIRED:
            continue

        payload = _dict_or_empty(record.payload)
        trigger_id = _string_or_none(payload.get("trigger_id"))
        relation = _trigger_relation_or_none(payload.get("relation"))
        matched_sequence = _int_or_none(payload.get("matched_sequence"))
        matched_event_type = _event_type_or_none(payload.get("matched_event_type"))
        matched_record = records_by_sequence.get(matched_sequence) if matched_sequence is not None else None

        trigger_id_present = trigger_id is not None
        relation_valid = relation is not None
        matched_pair_present = matched_sequence is not None and matched_event_type is not None
        matched_pair_absent = matched_sequence is None and matched_event_type is None
        matched_pair_consistent = matched_pair_present or matched_pair_absent
        matched_reference_found = matched_record is not None if matched_sequence is not None else None
        matched_event_type_matches = (
            matched_record is not None
            and matched_event_type is not None
            and matched_record.event_type == matched_event_type
            if matched_sequence is not None
            else None
        )
        relation_order_valid = (
            _trigger_relation_order_valid(
                relation=relation,
                trigger_sequence=record.sequence,
                matched_sequence=matched_sequence,
            )
            if matched_sequence is not None
            else None
        )

        if (
            trigger_id_present
            and relation_valid
            and matched_pair_consistent
            and (
                matched_sequence is None
                or (
                    matched_reference_found
                    and matched_event_type_matches
                    and relation_order_valid
                )
            )
        ):
            continue

        anomalies.append(
            {
                "event_type": record.event_type.value,
                "matched_event_type": _string_or_none(payload.get("matched_event_type")),
                "matched_event_type_matches": matched_event_type_matches,
                "matched_event_type_valid": (
                    matched_event_type is not None
                    if payload.get("matched_event_type") is not None
                    else None
                ),
                "matched_pair_consistent": matched_pair_consistent,
                "matched_reference_found": matched_reference_found,
                "matched_sequence": matched_sequence,
                "relation": _string_or_none(payload.get("relation")),
                "relation_order_valid": relation_order_valid,
                "relation_valid": relation_valid,
                "sequence": record.sequence,
                "trigger_id": trigger_id,
                "trigger_id_present": trigger_id_present,
            }
        )

    return _check(
        LedgerHealthCheckName.TRIGGER_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _trigger_relation_order_valid(
    *,
    relation: TriggerRelation | None,
    trigger_sequence: int,
    matched_sequence: int | None,
) -> bool:
    if relation is None or matched_sequence is None or matched_sequence == trigger_sequence:
        return False
    if relation == TriggerRelation.BEFORE:
        return trigger_sequence < matched_sequence
    return trigger_sequence > matched_sequence


def _strategy_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    starts_by_sequence: dict[int, str] = {}
    closure_sequences_by_start: dict[int, list[int]] = {}
    anomalies: list[dict[str, JsonValue]] = []
    closed_count = 0
    failed_count = 0

    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.STRATEGY_EVALUATION_STARTED:
            strategy_id = _string_or_none(payload.get("strategy_id"))
            if strategy_id is None:
                anomalies.append(
                    {
                        "closed": False,
                        "event_type": record.event_type.value,
                        "sequence": record.sequence,
                        "started_reference_found": None,
                        "started_sequence": None,
                        "strategy_id": None,
                        "strategy_id_matches": None,
                        "strategy_id_present": False,
                    }
                )
                continue
            starts_by_sequence[record.sequence] = strategy_id
            closure_sequences_by_start[record.sequence] = []
            continue

        if record.event_type == EventType.STRATEGY_EVALUATION_COMPLETED:
            closed_count += 1
            _check_strategy_closure(
                anomalies=anomalies,
                closure_sequences_by_start=closure_sequences_by_start,
                payload=payload,
                record=record,
                starts_by_sequence=starts_by_sequence,
            )
            continue

        if record.event_type == EventType.STRATEGY_EVALUATION_FAILED:
            closed_count += 1
            failed_count += 1
            _check_strategy_closure(
                anomalies=anomalies,
                closure_sequences_by_start=closure_sequences_by_start,
                payload=payload,
                record=record,
                starts_by_sequence=starts_by_sequence,
            )

    for started_sequence, strategy_id in starts_by_sequence.items():
        if closure_sequences_by_start[started_sequence]:
            continue
        anomalies.append(
            {
                "closed": False,
                "event_type": EventType.STRATEGY_EVALUATION_STARTED.value,
                "sequence": started_sequence,
                "started_reference_found": True,
                "started_sequence": started_sequence,
                "strategy_id": strategy_id,
                "strategy_id_matches": True,
                "strategy_id_present": True,
            }
        )

    return _check(
        LedgerHealthCheckName.STRATEGY_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "closed_count": closed_count,
            "failed_count": failed_count,
            "started_count": len(starts_by_sequence),
        },
    )


def _strategy_simulation_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    for record in records:
        if record.event_type != EventType.STRATEGY_SIMULATION_RESULT:
            continue

        payload = _dict_or_empty(record.payload)
        missing_fields: list[str] = []
        invalid_fields: list[str] = []
        for field_name in (
            "accepted_action_count",
            "as_of_sequence",
            "completed_count",
            "config_fingerprint",
            "evaluated_at",
            "evaluation_statuses",
            "execution_mode",
            "failed_count",
            "fingerprint_algorithm",
            "intent_count",
            "ledger_path",
            "order_endpoint_called",
            "read_only",
            "rejected_action_count",
            "runtime_tasks_started",
            "schema_version",
            "simulated_ledger",
            "status",
            "strategy_count",
            "strategy_ids",
            "strategy_tasks_started",
        ):
            if field_name not in payload:
                missing_fields.append(field_name)

        status = _strategy_simulation_status_or_none(payload.get("status"))
        strategy_ids = _string_list(payload.get("strategy_ids"))
        evaluation_statuses = _object_list_or_none(payload.get("evaluation_statuses"))
        strategy_count = _int_or_none(payload.get("strategy_count"))
        completed_count = _int_or_none(payload.get("completed_count"))
        failed_count = _int_or_none(payload.get("failed_count"))
        rejected_action_count = _int_or_none(payload.get("rejected_action_count"))

        if payload.get("schema_version") != 1:
            invalid_fields.append("schema_version")
        if _string_or_none(payload.get("config_fingerprint")) is None:
            invalid_fields.append("config_fingerprint")
        if payload.get("fingerprint_algorithm") != CONFIG_FINGERPRINT_ALGORITHM:
            invalid_fields.append("fingerprint_algorithm")
        if _string_or_none(payload.get("ledger_path")) is None:
            invalid_fields.append("ledger_path")
        if _execution_mode_or_none(payload.get("execution_mode")) is None:
            invalid_fields.append("execution_mode")
        if status is None:
            invalid_fields.append("status")
        if not _is_string_list(payload.get("strategy_ids")) or not strategy_ids:
            invalid_fields.append("strategy_ids")
        if evaluation_statuses is None:
            invalid_fields.append("evaluation_statuses")
        for field_name in (
            "accepted_action_count",
            "as_of_sequence",
            "completed_count",
            "failed_count",
            "intent_count",
            "rejected_action_count",
            "strategy_count",
        ):
            parsed = _int_or_none(payload.get(field_name))
            if parsed is None or parsed < 0:
                invalid_fields.append(field_name)
        for field_name in ("order_endpoint_called", "runtime_tasks_started", "strategy_tasks_started"):
            if payload.get(field_name) is not False:
                invalid_fields.append(field_name)
        if payload.get("read_only") is not True:
            invalid_fields.append("read_only")
        if _string_or_none(payload.get("evaluated_at")) is None:
            invalid_fields.append("evaluated_at")

        simulated_ledger = _dict_or_empty(payload.get("simulated_ledger"))
        if not simulated_ledger:
            invalid_fields.append("simulated_ledger")
        else:
            if simulated_ledger.get("verified") is not True:
                invalid_fields.append("simulated_ledger.verified")
            if _string_or_none(simulated_ledger.get("ledger_path")) is None:
                invalid_fields.append("simulated_ledger.ledger_path")
            simulated_record_count = _int_or_none(simulated_ledger.get("record_count"))
            if simulated_record_count is None or simulated_record_count < 0:
                invalid_fields.append("simulated_ledger.record_count")
            if (
                simulated_ledger.get("last_hash") is not None
                and _string_or_none(simulated_ledger.get("last_hash")) is None
            ):
                invalid_fields.append("simulated_ledger.last_hash")

        if (
            strategy_count is not None
            and completed_count is not None
            and failed_count is not None
            and completed_count + failed_count != strategy_count
        ):
            invalid_fields.append("completed_count")
            invalid_fields.append("failed_count")
            invalid_fields.append("strategy_count")
        if evaluation_statuses is not None:
            evaluation_strategy_ids = []
            for evaluation in evaluation_statuses:
                evaluation_strategy_id = _string_or_none(evaluation.get("strategy_id"))
                evaluation_strategy_ids.append(evaluation_strategy_id)
                if evaluation_strategy_id is None:
                    invalid_fields.append("evaluation_statuses.strategy_id")
                if _strategy_evaluation_status_or_none(evaluation.get("status")) is None:
                    invalid_fields.append("evaluation_statuses.status")
                for field_name in (
                    "accepted_action_count",
                    "intent_count",
                    "rejected_action_count",
                ):
                    parsed = _int_or_none(evaluation.get(field_name))
                    if parsed is None or parsed < 0:
                        invalid_fields.append(f"evaluation_statuses.{field_name}")
            if strategy_count is not None and len(evaluation_statuses) != strategy_count:
                invalid_fields.append("evaluation_statuses")
            if any(strategy_id is None for strategy_id in evaluation_strategy_ids):
                invalid_fields.append("evaluation_statuses")
            elif [strategy_id for strategy_id in evaluation_strategy_ids if strategy_id] != strategy_ids:
                invalid_fields.append("evaluation_statuses")
                invalid_fields.append("strategy_ids")

        if status == StrategySimulationStatus.OK:
            if failed_count != 0:
                invalid_fields.append("failed_count")
            if rejected_action_count != 0:
                invalid_fields.append("rejected_action_count")
        elif status == StrategySimulationStatus.ATTENTION_REQUIRED:
            if (failed_count or 0) <= 0 and (rejected_action_count or 0) <= 0:
                invalid_fields.append("status")

        if not missing_fields and not invalid_fields:
            continue

        anomalies.append(
            {
                "invalid_fields": sorted(set(invalid_fields)),
                "missing_fields": missing_fields,
                "sequence": record.sequence,
                "status": _string_or_none(payload.get("status")),
                "strategy_ids": strategy_ids,
            }
        )

    return _check(
        LedgerHealthCheckName.STRATEGY_SIMULATION_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _operator_canary_evidence_result_contract_check(
    records: tuple[AuditRecord, ...],
) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    for record in records:
        if record.event_type != EventType.OPERATOR_CANARY_EVIDENCE_RESULT:
            continue

        payload = _dict_or_empty(record.payload)
        missing_fields: list[str] = []
        invalid_fields: list[str] = []
        for field_name in (
            "cancel_action_count",
            "config_fingerprint",
            "evidence_ledger",
            "evidence_read_only",
            "fingerprint_algorithm",
            "issue_count",
            "issue_names",
            "ledger_path",
            "open_order_count",
            "order_endpoint_called",
            "recording_writes_ledger",
            "runtime_tasks_started",
            "schema_version",
            "status",
            "websocket_started",
        ):
            if field_name not in payload:
                missing_fields.append(field_name)

        status = _readiness_status_or_none(payload.get("status"))
        issue_count = _int_or_none(payload.get("issue_count"))
        issue_names = _string_list(payload.get("issue_names"))

        if payload.get("schema_version") != 1:
            invalid_fields.append("schema_version")
        if _string_or_none(payload.get("config_fingerprint")) is None:
            invalid_fields.append("config_fingerprint")
        if payload.get("fingerprint_algorithm") != CONFIG_FINGERPRINT_ALGORITHM:
            invalid_fields.append("fingerprint_algorithm")
        if _string_or_none(payload.get("ledger_path")) is None:
            invalid_fields.append("ledger_path")
        if status is None:
            invalid_fields.append("status")
        for field_name in ("cancel_action_count", "issue_count", "open_order_count"):
            parsed = _int_or_none(payload.get(field_name))
            if parsed is None or parsed < 0:
                invalid_fields.append(field_name)
        if not _is_string_list(payload.get("issue_names")):
            invalid_fields.append("issue_names")
        else:
            for issue_name in issue_names:
                if _operator_canary_evidence_issue_or_none(issue_name) is None:
                    invalid_fields.append("issue_names")
                    break
        if issue_count is not None and issue_count >= 0 and len(issue_names) != issue_count:
            invalid_fields.append("issue_count")
            invalid_fields.append("issue_names")
        if status == ReadinessStatus.OK and issue_count != 0:
            invalid_fields.append("issue_count")
            invalid_fields.append("status")
        if status == ReadinessStatus.ATTENTION_REQUIRED and issue_count == 0:
            invalid_fields.append("issue_count")
            invalid_fields.append("status")
        for field_name in ("order_endpoint_called", "runtime_tasks_started", "websocket_started"):
            if payload.get(field_name) is not False:
                invalid_fields.append(field_name)
        if payload.get("evidence_read_only") is not True:
            invalid_fields.append("evidence_read_only")
        if payload.get("recording_writes_ledger") is not True:
            invalid_fields.append("recording_writes_ledger")

        evidence_ledger = _dict_or_empty(payload.get("evidence_ledger"))
        if not evidence_ledger:
            invalid_fields.append("evidence_ledger")
        else:
            if evidence_ledger.get("verified") is not True:
                invalid_fields.append("evidence_ledger.verified")
            next_sequence = _int_or_none(evidence_ledger.get("next_sequence"))
            record_count = _int_or_none(evidence_ledger.get("record_count"))
            if next_sequence is None or next_sequence < 0:
                invalid_fields.append("evidence_ledger.next_sequence")
            if record_count is None or record_count < 0:
                invalid_fields.append("evidence_ledger.record_count")
            if (
                evidence_ledger.get("last_hash") is not None
                and _string_or_none(evidence_ledger.get("last_hash")) is None
            ):
                invalid_fields.append("evidence_ledger.last_hash")

        if not missing_fields and not invalid_fields:
            continue

        anomalies.append(
            {
                "invalid_fields": sorted(set(invalid_fields)),
                "issue_names": issue_names,
                "missing_fields": missing_fields,
                "sequence": record.sequence,
                "status": _string_or_none(payload.get("status")),
            }
        )

    return _check(
        LedgerHealthCheckName.OPERATOR_CANARY_EVIDENCE_RESULT_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _ledger_health_acknowledgement_contract_check(
    records: tuple[AuditRecord, ...],
) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    acknowledgement_count = 0
    required_fields = (
        "acknowledged_by",
        "acknowledged_health_status",
        "acknowledged_through_hash",
        "acknowledged_through_sequence",
        "attention_check_count",
        "attention_checks",
        "ledger_health_attention_digest",
        "ledger_path",
        "reason",
        "schema_version",
    )

    for record in records:
        if record.event_type != EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED:
            continue

        acknowledgement_count += 1
        payload = _dict_or_empty(record.payload)
        missing_fields = [field_name for field_name in required_fields if field_name not in payload]
        invalid_fields: list[str] = []

        if payload.get("schema_version") != 1:
            invalid_fields.append("schema_version")
        if _string_or_none(payload.get("acknowledged_by")) is None:
            invalid_fields.append("acknowledged_by")
        if _string_or_none(payload.get("reason")) is None:
            invalid_fields.append("reason")
        if _string_or_none(payload.get("ledger_path")) is None:
            invalid_fields.append("ledger_path")
        if _ledger_health_status_or_none(payload.get("acknowledged_health_status")) != (
            LedgerHealthStatus.ATTENTION_REQUIRED
        ):
            invalid_fields.append("acknowledged_health_status")
        acknowledged_sequence = _int_or_none(payload.get("acknowledged_through_sequence"))
        if acknowledged_sequence is None or acknowledged_sequence <= 0:
            invalid_fields.append("acknowledged_through_sequence")
        if not _is_sha256_hex(payload.get("acknowledged_through_hash")):
            invalid_fields.append("acknowledged_through_hash")
        if not _is_sha256_hex(payload.get("ledger_health_attention_digest")):
            invalid_fields.append("ledger_health_attention_digest")

        attention_check_count = _int_or_none(payload.get("attention_check_count"))
        attention_checks = _object_list_or_none(payload.get("attention_checks"))
        if attention_check_count is None or attention_check_count < 0:
            invalid_fields.append("attention_check_count")
        if attention_checks is None:
            invalid_fields.append("attention_checks")
        else:
            if attention_check_count is not None and attention_check_count != len(attention_checks):
                invalid_fields.append("attention_check_count")
                invalid_fields.append("attention_checks")
            for check in attention_checks:
                if _ledger_health_check_name_or_none(check.get("name")) is None:
                    invalid_fields.append("attention_checks.name")
                if _ledger_health_status_or_none(check.get("status")) is None:
                    invalid_fields.append("attention_checks.status")
                check_count = _int_or_none(check.get("count"))
                if check_count is None or check_count < 0:
                    invalid_fields.append("attention_checks.count")

        if not missing_fields and not invalid_fields:
            continue

        anomalies.append(
            {
                "invalid_fields": sorted(set(invalid_fields)),
                "missing_fields": missing_fields,
                "sequence": record.sequence,
            }
        )

    return _check(
        LedgerHealthCheckName.LEDGER_HEALTH_ACKNOWLEDGEMENT_CONTRACT,
        count=len(anomalies),
        details={
            "acknowledgement_count": acknowledgement_count,
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _runtime_health_check_result_contract_check(
    records: tuple[AuditRecord, ...],
) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    for record in records:
        if record.event_type != EventType.RUNTIME_HEALTH_CHECK_RESULT:
            continue

        payload = _dict_or_empty(record.payload)
        missing_fields: list[str] = []
        invalid_fields: list[str] = []
        for field_name in (
            "attention_check_count",
            "attention_checks",
            "checked_health_status",
            "checked_through_sequence",
            "ledger_path",
            "record_count",
            "schema_version",
        ):
            if field_name not in payload:
                missing_fields.append(field_name)

        status = _ledger_health_status_or_none(payload.get("checked_health_status"))
        attention_check_count = _int_or_none(payload.get("attention_check_count"))
        attention_checks = _string_list(payload.get("attention_checks"))
        checked_through_sequence = _int_or_none(payload.get("checked_through_sequence"))
        record_count = _int_or_none(payload.get("record_count"))

        if payload.get("schema_version") != 1:
            invalid_fields.append("schema_version")
        if _string_or_none(payload.get("ledger_path")) is None:
            invalid_fields.append("ledger_path")
        if status is None:
            invalid_fields.append("checked_health_status")
        for field_name, parsed in (
            ("attention_check_count", attention_check_count),
            ("checked_through_sequence", checked_through_sequence),
            ("record_count", record_count),
        ):
            if parsed is None or parsed < 0:
                invalid_fields.append(field_name)
        if not _is_string_list(payload.get("attention_checks")):
            invalid_fields.append("attention_checks")
        if (
            attention_check_count is not None
            and attention_check_count >= 0
            and len(attention_checks) != attention_check_count
        ):
            invalid_fields.append("attention_check_count")
            invalid_fields.append("attention_checks")
        if status == LedgerHealthStatus.OK and attention_check_count != 0:
            invalid_fields.append("attention_check_count")
            invalid_fields.append("checked_health_status")
        if status == LedgerHealthStatus.ATTENTION_REQUIRED and attention_check_count == 0:
            invalid_fields.append("attention_check_count")
            invalid_fields.append("checked_health_status")

        if not missing_fields and not invalid_fields:
            continue

        anomalies.append(
            {
                "checked_health_status": _string_or_none(payload.get("checked_health_status")),
                "invalid_fields": sorted(set(invalid_fields)),
                "missing_fields": missing_fields,
                "sequence": record.sequence,
            }
        )

    return _check(
        LedgerHealthCheckName.RUNTIME_HEALTH_CHECK_RESULT_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _check_strategy_closure(
    *,
    anomalies: list[dict[str, JsonValue]],
    closure_sequences_by_start: dict[int, list[int]],
    payload: dict[str, JsonValue],
    record: AuditRecord,
    starts_by_sequence: dict[int, str],
) -> None:
    strategy_id = _string_or_none(payload.get("strategy_id"))
    started_sequence = _int_or_none(payload.get("started_sequence"))
    started_strategy_id = starts_by_sequence.get(started_sequence) if started_sequence is not None else None
    started_reference_found = started_strategy_id is not None
    strategy_id_matches = strategy_id is not None and started_strategy_id is not None and strategy_id == started_strategy_id
    duplicate_closure = (
        started_sequence is not None
        and bool(closure_sequences_by_start.get(started_sequence))
    )
    payload_anomalies = _strategy_closure_payload_anomalies(payload, record)

    if started_sequence is not None and strategy_id_matches and not duplicate_closure:
        closure_sequences_by_start.setdefault(started_sequence, []).append(record.sequence)
        anomalies.extend(payload_anomalies)
        return

    anomalies.extend(payload_anomalies)
    anomalies.append(
        {
            "closed": True,
            "duplicate_closure": duplicate_closure,
            "event_type": record.event_type.value,
            "sequence": record.sequence,
            "started_reference_found": started_reference_found,
            "started_sequence": started_sequence,
            "strategy_id": strategy_id,
            "strategy_id_matches": strategy_id_matches,
            "strategy_id_present": strategy_id is not None,
        }
    )


def _strategy_closure_payload_anomalies(
    payload: dict[str, JsonValue],
    record: AuditRecord,
) -> list[dict[str, JsonValue]]:
    anomalies: list[dict[str, JsonValue]] = []
    expected_status = (
        StrategyEvaluationStatus.COMPLETED
        if record.event_type == EventType.STRATEGY_EVALUATION_COMPLETED
        else StrategyEvaluationStatus.FAILED
    )
    observed_status = _strategy_evaluation_status_or_none(payload.get("status"))
    if observed_status != expected_status:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field="status",
                expected=expected_status.value,
                observed=payload.get("status"),
            )
        )

    action_receipts = _object_list_or_none(payload.get("action_receipts"))
    submitted_action_count = _int_or_none(payload.get("submitted_action_count"))
    if action_receipts is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field="action_receipts",
                expected="list",
                observed=_json_type_name(payload.get("action_receipts")),
            )
        )
    elif submitted_action_count != len(action_receipts):
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field="submitted_action_count",
                expected=len(action_receipts),
                observed=submitted_action_count,
            )
        )
    if submitted_action_count is None or submitted_action_count < 0:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field="submitted_action_count",
                expected="non-negative integer",
                observed=payload.get("submitted_action_count"),
            )
        )
    if _int_or_none(payload.get("intent_count")) is None or _int_or_none(payload.get("intent_count")) < 0:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field="intent_count",
                expected="non-negative integer",
                observed=payload.get("intent_count"),
            )
        )
    for index, receipt in enumerate(action_receipts or ()):
        anomalies.extend(_strategy_action_receipt_anomalies(record, receipt, index=index))

    if "input_freshness" in payload:
        input_freshness = _object_list_or_none(payload.get("input_freshness"))
        if input_freshness is None:
            anomalies.append(
                _strategy_payload_anomaly(
                    record,
                    field="input_freshness",
                    expected="list",
                    observed=_json_type_name(payload.get("input_freshness")),
                )
            )
        for index, freshness in enumerate(input_freshness or ()):
            anomalies.extend(_strategy_input_freshness_anomalies(record, freshness, index=index))
    return anomalies


def _strategy_action_receipt_anomalies(
    record: AuditRecord,
    receipt: dict[str, JsonValue],
    *,
    index: int,
) -> list[dict[str, JsonValue]]:
    anomalies: list[dict[str, JsonValue]] = []
    if _string_or_none(receipt.get("action_id")) is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"action_receipts[{index}].action_id",
                expected="non-empty string",
                observed=receipt.get("action_id"),
            )
        )
    if _action_type_or_none(receipt.get("action_type")) is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"action_receipts[{index}].action_type",
                expected="ActionType",
                observed=receipt.get("action_type"),
            )
        )
    if _action_status_or_none(receipt.get("status")) is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"action_receipts[{index}].status",
                expected="ActionStatus",
                observed=receipt.get("status"),
            )
        )
    for field_name in ("requested_sequence", "decision_sequence"):
        if _int_or_none(receipt.get(field_name)) is None:
            anomalies.append(
                _strategy_payload_anomaly(
                    record,
                    field=f"action_receipts[{index}].{field_name}",
                    expected="integer",
                    observed=receipt.get(field_name),
                )
            )
    return anomalies


def _strategy_input_freshness_anomalies(
    record: AuditRecord,
    freshness: dict[str, JsonValue],
    *,
    index: int,
) -> list[dict[str, JsonValue]]:
    anomalies: list[dict[str, JsonValue]] = []
    status = _strategy_input_status_or_none(freshness.get("status"))
    if _market_data_kind_or_none(freshness.get("data_kind")) is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].data_kind",
                expected="MarketDataKind",
                observed=freshness.get("data_kind"),
            )
        )
    if status is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].status",
                expected="StrategyInputStatus",
                observed=freshness.get("status"),
            )
        )
    if _string_or_none(freshness.get("product_id")) is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].product_id",
                expected="non-empty string",
                observed=freshness.get("product_id"),
            )
        )
    max_age_seconds = _float_or_none(freshness.get("max_age_seconds"))
    if max_age_seconds is None or max_age_seconds <= 0:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].max_age_seconds",
                expected="positive number",
                observed=freshness.get("max_age_seconds"),
            )
        )
    is_ok = freshness.get("is_ok")
    if not isinstance(is_ok, bool):
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].is_ok",
                expected="bool",
                observed=is_ok,
            )
        )
    elif status is not None and is_ok != (status == StrategyInputStatus.OK):
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].is_ok",
                expected=status == StrategyInputStatus.OK,
                observed=is_ok,
            )
        )
    age_seconds = freshness.get("age_seconds")
    if age_seconds is not None and _float_or_none(age_seconds) is None:
        anomalies.append(
            _strategy_payload_anomaly(
                record,
                field=f"input_freshness[{index}].age_seconds",
                expected="number or null",
                observed=age_seconds,
            )
        )
    return anomalies


def _strategy_payload_anomaly(
    record: AuditRecord,
    *,
    expected: JsonValue,
    field: str,
    observed: JsonValue,
) -> dict[str, JsonValue]:
    return {
        "closed": True,
        "event_type": record.event_type.value,
        "expected": expected,
        "field": field,
        "issue": "strategy_closure_payload_contract",
        "observed": observed,
        "sequence": record.sequence,
    }


def _runtime_task_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    starts_by_sequence: dict[int, RuntimeTask] = {}
    closure_sequences_by_start: dict[int, list[int]] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.RUNTIME_TASK_STARTED:
            task_id = _runtime_task_or_none(payload.get("task_id"))
            if task_id is None:
                anomalies.append(
                    {
                        "closed": False,
                        "event_type": record.event_type.value,
                        "sequence": record.sequence,
                        "started_reference_found": None,
                        "started_sequence": None,
                        "task_id": _string_or_none(payload.get("task_id")),
                        "task_id_matches": None,
                        "task_id_valid": False,
                    }
                )
                continue
            starts_by_sequence[record.sequence] = task_id
            closure_sequences_by_start[record.sequence] = []
            continue

        if record.event_type == EventType.RUNTIME_TASK_COMPLETED:
            _check_runtime_task_closure(
                anomalies=anomalies,
                closure_sequences_by_start=closure_sequences_by_start,
                payload=payload,
                record=record,
                starts_by_sequence=starts_by_sequence,
            )
            continue

        if record.event_type == EventType.ERROR and _error_category_or_none(payload) == ErrorCategory.RUNTIME_TASK:
            _check_runtime_task_closure(
                anomalies=anomalies,
                closure_sequences_by_start=closure_sequences_by_start,
                payload=payload,
                record=record,
                starts_by_sequence=starts_by_sequence,
            )

    for started_sequence, task_id in starts_by_sequence.items():
        if closure_sequences_by_start[started_sequence]:
            continue
        anomalies.append(
            {
                "closed": False,
                "event_type": EventType.RUNTIME_TASK_STARTED.value,
                "sequence": started_sequence,
                "started_reference_found": True,
                "started_sequence": started_sequence,
                "task_id": task_id.value,
                "task_id_matches": True,
                "task_id_valid": True,
            }
        )

    return _check(
        LedgerHealthCheckName.RUNTIME_TASK_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _check_runtime_task_closure(
    *,
    anomalies: list[dict[str, JsonValue]],
    closure_sequences_by_start: dict[int, list[int]],
    payload: dict[str, JsonValue],
    record: AuditRecord,
    starts_by_sequence: dict[int, RuntimeTask],
) -> None:
    task_id = _runtime_task_or_none(payload.get("task_id"))
    started_sequence = _int_or_none(payload.get("started_sequence"))
    started_task_id = starts_by_sequence.get(started_sequence) if started_sequence is not None else None
    started_reference_found = started_task_id is not None
    task_id_matches = task_id is not None and started_task_id is not None and task_id == started_task_id
    duplicate_closure = (
        started_sequence is not None
        and bool(closure_sequences_by_start.get(started_sequence))
    )

    if started_sequence is not None and task_id_matches and not duplicate_closure:
        closure_sequences_by_start.setdefault(started_sequence, []).append(record.sequence)
        return

    anomalies.append(
        {
            "closed": True,
            "duplicate_closure": duplicate_closure,
            "event_type": record.event_type.value,
            "sequence": record.sequence,
            "started_reference_found": started_reference_found,
            "started_sequence": started_sequence,
            "task_id": task_id.value if task_id is not None else _string_or_none(payload.get("task_id")),
            "task_id_matches": task_id_matches,
            "task_id_valid": task_id is not None,
        }
    )


def _action_execution_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    expected_place_client_order_ids: dict[str, str] = {}
    mismatches: list[dict[str, JsonValue]] = []
    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.ACTION_REQUESTED:
            action_id = _string_or_none(payload.get("action_id"))
            if action_id is not None and payload.get("action_type") == ActionType.PLACE_ORDER.value:
                expected_place_client_order_ids[action_id] = _string_or_none(payload.get("idempotency_key")) or action_id
            continue

        if record.event_type != EventType.ACTION_EXECUTED:
            continue

        execution_result = _dict_or_empty(payload.get("execution_result"))
        action_id = _string_or_none(payload.get("action_id"))
        result_action_id = _string_or_none(execution_result.get("action_id"))
        action_type = _string_or_none(payload.get("action_type"))
        result_action_type = _string_or_none(execution_result.get("action_type"))
        action_type_value = _action_type_or_none(action_type)
        result_mode = _execution_mode_or_none(execution_result.get("mode"))
        result_status = _execution_status_or_none(execution_result.get("status"))
        allowed_statuses = (
            valid_execution_statuses_for_action_type(action_type_value)
            if action_type_value is not None
            else ()
        )
        expected_client_order_id = (
            expected_place_client_order_ids.get(action_id)
            if action_id is not None and action_type == ActionType.PLACE_ORDER.value
            else None
        )
        result_client_order_id = _string_or_none(execution_result.get("client_order_id"))
        action_id_matches = action_id is not None and action_id == result_action_id
        action_type_matches = action_type is not None and action_type == result_action_type
        client_order_id_matches = (
            expected_client_order_id is None
            or result_client_order_id is None
            or result_client_order_id == expected_client_order_id
        )
        mode_valid = result_mode is not None
        status_matches_action_type = result_status is not None and result_status in allowed_statuses

        if (
            action_id_matches
            and action_type_matches
            and client_order_id_matches
            and mode_valid
            and status_matches_action_type
        ):
            continue

        mismatches.append(
            {
                "action_id": action_id,
                "action_id_matches": action_id_matches,
                "action_type": action_type,
                "action_type_matches": action_type_matches,
                "allowed_statuses": [status.value for status in allowed_statuses],
                "client_order_id_matches": client_order_id_matches,
                "expected_client_order_id": expected_client_order_id,
                "mode_valid": mode_valid,
                "result_action_id": result_action_id,
                "result_action_type": result_action_type,
                "result_client_order_id": result_client_order_id,
                "result_mode": result_mode.value if result_mode is not None else None,
                "result_status": result_status.value if result_status is not None else None,
                "sequence": record.sequence,
                "status_matches_action_type": status_matches_action_type,
            }
        )

    return _check(
        LedgerHealthCheckName.ACTION_EXECUTION_CONTRACT,
        count=len(mismatches),
        details={
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
        },
    )


def _exchange_fill_contract_check(
    records: tuple[AuditRecord, ...],
    projection: SourceOfTruthProjection,
) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    for record in records:
        if record.event_type != EventType.EXCHANGE_FILL:
            continue

        payload = _dict_or_empty(record.payload)
        fill_id = _string_or_none(payload.get("fill_id"))
        order_id = _string_or_none(payload.get("order_id"))
        order = projection.orders_by_exchange_order_id.get(order_id) if order_id is not None else None
        action_id = _string_or_none(payload.get("action_id"))
        client_order_id = _string_or_none(payload.get("client_order_id"))
        product_id = _string_or_none(payload.get("product_id"))
        fill_side = _normalized_order_side_or_none(payload.get("side"))
        expected_action_id = order.action_id if order is not None else None
        expected_client_order_id = order.client_order_id if order is not None else None
        expected_product_id = order.product_id if order is not None else None
        expected_side = order.side.value if order is not None and order.side is not None else None

        fill_id_present = fill_id is not None
        order_id_present = order_id is not None
        order_found = order is not None
        action_id_matches = action_id is None or expected_action_id is None or action_id == expected_action_id
        client_order_id_matches = (
            client_order_id is None
            or expected_client_order_id is None
            or client_order_id == expected_client_order_id
        )
        product_id_matches = product_id is None or expected_product_id is None or product_id == expected_product_id
        side_matches = fill_side is None or expected_side is None or fill_side == expected_side

        if (
            fill_id_present
            and order_id_present
            and order_found
            and action_id_matches
            and client_order_id_matches
            and product_id_matches
            and side_matches
        ):
            continue

        anomalies.append(
            {
                "action_id": action_id,
                "action_id_matches": action_id_matches,
                "client_order_id": client_order_id,
                "client_order_id_matches": client_order_id_matches,
                "expected_action_id": expected_action_id,
                "expected_client_order_id": expected_client_order_id,
                "expected_product_id": expected_product_id,
                "expected_side": expected_side,
                "fill_id": fill_id,
                "fill_id_present": fill_id_present,
                "order_found": order_found,
                "order_id": order_id,
                "order_id_present": order_id_present,
                "product_id": product_id,
                "product_id_matches": product_id_matches,
                "sequence": record.sequence,
                "side": fill_side,
                "side_matches": side_matches,
            }
        )

    return _check(
        LedgerHealthCheckName.FILL_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _order_identity_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    requested_place_client_order_ids: dict[str, str] = {}
    received_payloads_by_sequence: dict[int, dict[str, JsonValue]] = {}
    owners_by_client_order_id: dict[str, str] = {}
    owners_by_exchange_order_id: dict[str, str] = {}
    collisions: list[dict[str, JsonValue]] = []

    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.ACTION_REQUESTED:
            action_id = _string_or_none(payload.get("action_id"))
            if action_id is not None and payload.get("action_type") == ActionType.PLACE_ORDER.value:
                requested_place_client_order_ids[action_id] = _string_or_none(payload.get("idempotency_key")) or action_id
            continue

        if record.event_type == EventType.ACTION_ACCEPTED:
            action_id = _string_or_none(payload.get("action_id"))
            if action_id is None or payload.get("action_type") != ActionType.PLACE_ORDER.value:
                continue
            _claim_order_identifier(
                action_id=action_id,
                collisions=collisions,
                identifier=requested_place_client_order_ids.get(action_id),
                identifier_type="client_order_id",
                owners_by_identifier=owners_by_client_order_id,
                record=record,
            )
            continue

        if record.event_type == EventType.ACTION_EXECUTED:
            action_id = _string_or_none(payload.get("action_id"))
            action_type = _action_type_or_none(payload.get("action_type"))
            if action_id is None or action_type != ActionType.PLACE_ORDER:
                continue
            execution_result = _dict_or_empty(payload.get("execution_result"))
            _claim_order_identifier(
                action_id=action_id,
                collisions=collisions,
                identifier=_string_or_none(execution_result.get("client_order_id")),
                identifier_type="client_order_id",
                owners_by_identifier=owners_by_client_order_id,
                record=record,
            )
            _claim_order_identifier(
                action_id=action_id,
                collisions=collisions,
                identifier=_string_or_none(execution_result.get("exchange_order_id")),
                identifier_type="exchange_order_id",
                owners_by_identifier=owners_by_exchange_order_id,
                record=record,
            )
            continue

        if record.event_type == EventType.DATA_RECEIVED:
            received_payloads_by_sequence[record.sequence] = payload
            continue

        if record.event_type == EventType.DATA_ACCEPTED:
            if payload.get("message_event_type") != EventType.EXCHANGE_ORDER_UPDATE.value:
                continue
            received_sequence = _int_or_none(payload.get("received_sequence"))
            received_payload = (
                received_payloads_by_sequence.get(received_sequence)
                if received_sequence is not None
                else None
            )
            feed_payload = _dict_or_empty(received_payload.get("payload")) if received_payload is not None else {}
            order_update = _dict_or_empty(feed_payload.get("order"))
            _claim_order_update_identities(
                collisions=collisions,
                owners_by_client_order_id=owners_by_client_order_id,
                owners_by_exchange_order_id=owners_by_exchange_order_id,
                order_update=order_update,
                record=record,
            )
            continue

        if record.event_type == EventType.RECONCILIATION_RECOVERY:
            order_update = _dict_or_empty(payload.get("order_update"))
            if not order_update:
                continue
            action_id = _string_or_none(payload.get("action_id"))
            _claim_order_update_identities(
                action_id=action_id,
                collisions=collisions,
                owners_by_client_order_id=owners_by_client_order_id,
                owners_by_exchange_order_id=owners_by_exchange_order_id,
                order_update=order_update,
                record=record,
            )

    return _check(
        LedgerHealthCheckName.ORDER_IDENTITY_CONTRACT,
        count=len(collisions),
        details={
            "collision_count": len(collisions),
            "collisions": collisions,
        },
    )


def _claim_order_update_identities(
    *,
    collisions: list[dict[str, JsonValue]],
    owners_by_client_order_id: dict[str, str],
    owners_by_exchange_order_id: dict[str, str],
    order_update: dict[str, JsonValue],
    record: AuditRecord,
    action_id: str | None = None,
) -> None:
    client_order_id = _string_or_none(order_update.get("client_order_id"))
    exchange_order_id = _string_or_none(order_update.get("order_id"))
    owner_action_id = (
        action_id
        or (owners_by_exchange_order_id.get(exchange_order_id) if exchange_order_id is not None else None)
        or (owners_by_client_order_id.get(client_order_id) if client_order_id is not None else None)
        or client_order_id
        or exchange_order_id
    )
    if owner_action_id is None:
        return

    _claim_order_identifier(
        action_id=owner_action_id,
        collisions=collisions,
        identifier=client_order_id,
        identifier_type="client_order_id",
        owners_by_identifier=owners_by_client_order_id,
        record=record,
    )
    _claim_order_identifier(
        action_id=owner_action_id,
        collisions=collisions,
        identifier=exchange_order_id,
        identifier_type="exchange_order_id",
        owners_by_identifier=owners_by_exchange_order_id,
        record=record,
    )


def _claim_order_identifier(
    *,
    action_id: str,
    collisions: list[dict[str, JsonValue]],
    identifier: str | None,
    identifier_type: str,
    owners_by_identifier: dict[str, str],
    record: AuditRecord,
) -> None:
    if identifier is None:
        return
    existing_action_id = owners_by_identifier.get(identifier)
    if existing_action_id is None:
        owners_by_identifier[identifier] = action_id
        return
    if existing_action_id == action_id:
        return
    collisions.append(
        {
            "existing_action_id": existing_action_id,
            "identifier": identifier,
            "identifier_type": identifier_type,
            "observed_action_id": action_id,
            "sequence": record.sequence,
            "event_type": record.event_type.value,
        }
    )


def _order_lineage_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    accepted_execution_action_ids: dict[str, dict[str, JsonValue]] = {}
    requested_placement_kinds_by_action_id: dict[str, OrderPlacementKind] = {}
    placement_action_ids: set[str] = set()
    seen_action_ids: set[str] = set()
    logical_orders: dict[str, dict[str, JsonValue]] = {}
    placements_by_id: dict[str, dict[str, JsonValue]] = {}
    placement_counts_by_logical_order_id: dict[str, int] = {}
    placement_sequences_by_id: dict[str, int] = {}
    released_stage_placement_ids: dict[str, str] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.ACTION_REQUESTED:
            action_id = _string_or_none(payload.get("action_id"))
            if action_id is not None:
                seen_action_ids.add(action_id)
                if _action_type_or_none(payload.get("action_type")) == ActionType.PLACE_ORDER:
                    action_payload = _dict_or_empty(payload.get("payload"))
                    requested_placement_kinds_by_action_id[action_id] = (
                        _order_placement_kind_or_none(action_payload.get("placement_kind")) or OrderPlacementKind.INITIAL
                    )
            continue

        if record.event_type == EventType.ACTION_EXECUTED:
            action_id = _string_or_none(payload.get("action_id"))
            action_type = _action_type_or_none(payload.get("action_type"))
            execution_result = _dict_or_empty(payload.get("execution_result"))
            execution_status = _execution_status_or_none(execution_result.get("status"))
            if (
                action_id is not None
                and action_type == ActionType.PLACE_ORDER
                and execution_status == ExecutionStatus.ACCEPTED
            ):
                accepted_execution_action_ids[action_id] = {
                    "action_id": action_id,
                    "exchange_order_id": _string_or_none(execution_result.get("exchange_order_id")),
                    "sequence": record.sequence,
                    "venue_client_order_id": _string_or_none(execution_result.get("client_order_id")),
                }
                requested_placement_kind = requested_placement_kinds_by_action_id.get(action_id)
                if requested_placement_kind == OrderPlacementKind.STAGED_RELEASE:
                    anomalies.append(
                        {
                            "action_id": action_id,
                            "event_type": EventType.ACTION_EXECUTED.value,
                            "exchange_order_id": _string_or_none(execution_result.get("exchange_order_id")),
                            "placement_kind": requested_placement_kind.value,
                            "sequence": record.sequence,
                            "staged_release_executed": True,
                            "venue_client_order_id": _string_or_none(execution_result.get("client_order_id")),
                        }
                    )
            continue

        if record.event_type == EventType.ORDER_LOGICAL_CREATED:
            _evaluate_logical_order_contract(
                anomalies=anomalies,
                logical_orders=logical_orders,
                payload=payload,
                record=record,
            )
            continue

        if record.event_type == EventType.ORDER_PLACEMENT_RECORDED:
            _evaluate_order_placement_contract(
                anomalies=anomalies,
                logical_orders=logical_orders,
                payload=payload,
                placement_counts_by_logical_order_id=placement_counts_by_logical_order_id,
                placements_by_id=placements_by_id,
                released_stage_placement_ids=released_stage_placement_ids,
                record=record,
                seen_action_ids=seen_action_ids,
            )
            placement_id = _string_or_none(payload.get("placement_id"))
            if placement_id is not None:
                placement_sequences_by_id.setdefault(placement_id, record.sequence)
            action_id = _string_or_none(payload.get("action_id"))
            if action_id is not None:
                placement_action_ids.add(action_id)

    for action_id, execution in accepted_execution_action_ids.items():
        if action_id in placement_action_ids:
            continue
        anomalies.append(
            {
                "action_id": action_id,
                "event_type": EventType.ACTION_EXECUTED.value,
                "exchange_order_id": execution.get("exchange_order_id"),
                "placement_record_found": False,
                "sequence": execution["sequence"],
                "venue_client_order_id": execution.get("venue_client_order_id"),
            }
        )

    passive_quote_summaries = _passive_market_making_quote_summaries(
        placements_by_id=placements_by_id,
        placement_sequences_by_id=placement_sequences_by_id,
        released_stage_placement_ids=released_stage_placement_ids,
    )
    passive_quote_contract_anomalies = _passive_market_making_quote_contract_anomalies(
        placements_by_id=placements_by_id,
        placement_sequences_by_id=placement_sequences_by_id,
    )
    anomalies.extend(passive_quote_contract_anomalies)

    return _check(
        LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT,
        count=len(anomalies),
        details={
            "accepted_execution_without_placement_count": sum(
                1 for action_id in accepted_execution_action_ids if action_id not in placement_action_ids
            ),
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "logical_order_count": len(logical_orders),
            "passive_market_making_quote_count": len(passive_quote_summaries),
            "passive_market_making_quote_contract_anomaly_count": len(
                passive_quote_contract_anomalies
            ),
            "passive_market_making_quotes": passive_quote_summaries,
            "passive_market_making_released_quote_count": sum(
                1 for quote in passive_quote_summaries if quote.get("released") is True
            ),
            "passive_market_making_unreleased_quote_count": sum(
                1 for quote in passive_quote_summaries if quote.get("released") is False
            ),
            "placement_count": len(placements_by_id),
            "release_placement_count": sum(
                1
                for placement in placements_by_id.values()
                if _order_placement_kind_or_none(placement.get("placement_kind")) == OrderPlacementKind.RELEASE
            ),
            "staged_release_execution_count": sum(
                1 for anomaly in anomalies if anomaly.get("staged_release_executed") is True
            ),
        },
    )


def _passive_market_making_quote_summaries(
    *,
    placements_by_id: dict[str, dict[str, JsonValue]],
    placement_sequences_by_id: dict[str, int],
    released_stage_placement_ids: dict[str, str],
) -> list[dict[str, JsonValue]]:
    summaries: list[dict[str, JsonValue]] = []
    for placement_id, placement in sorted(placements_by_id.items()):
        if _order_placement_kind_or_none(placement.get("placement_kind")) != OrderPlacementKind.STAGED_RELEASE:
            continue
        if _order_placement_status_or_none(placement.get("placement_status")) != OrderPlacementStatus.STAGED:
            continue
        metadata = _dict_or_empty(placement.get("metadata"))
        passive_market_making = _dict_or_empty(metadata.get("passive_market_making"))
        if not passive_market_making:
            continue
        release_placement_id = released_stage_placement_ids.get(placement_id)
        summaries.append(
            {
                "action_id": _string_or_none(placement.get("action_id")),
                "ask_price": _string_or_none(passive_market_making.get("ask_price")),
                "bid_price": _string_or_none(passive_market_making.get("bid_price")),
                "half_spread_bps": _string_or_none(passive_market_making.get("half_spread_bps")),
                "limit_price": _string_or_none(placement.get("limit_price")),
                "logical_order_id": _string_or_none(placement.get("logical_order_id")),
                "midpoint": _string_or_none(passive_market_making.get("midpoint")),
                "placement_id": placement_id,
                "product_id": _string_or_none(placement.get("product_id")),
                "release_placement_id": release_placement_id,
                "released": release_placement_id is not None,
                "sequence": placement_sequences_by_id.get(placement_id),
                "side": _string_or_none(placement.get("side")),
                "size": _string_or_none(placement.get("size")),
            }
        )
    return summaries


def _passive_market_making_quote_contract_anomalies(
    *,
    placements_by_id: dict[str, dict[str, JsonValue]],
    placement_sequences_by_id: dict[str, int],
) -> list[dict[str, JsonValue]]:
    anomalies: list[dict[str, JsonValue]] = []
    for placement_id, placement in sorted(placements_by_id.items()):
        metadata = _dict_or_empty(placement.get("metadata"))
        raw_passive_market_making = metadata.get("passive_market_making")
        if raw_passive_market_making is None:
            continue
        passive_market_making = _dict_or_empty(raw_passive_market_making)
        metadata_object_valid = bool(passive_market_making)
        placement_kind = _order_placement_kind_or_none(placement.get("placement_kind"))
        placement_status = _order_placement_status_or_none(placement.get("placement_status"))
        placement_side = _order_side_or_none(placement.get("side"))
        metadata_side = _order_side_or_none(passive_market_making.get("side"))
        product_id = _string_or_none(placement.get("product_id"))
        metadata_product_id = _string_or_none(passive_market_making.get("product_id"))
        ask_price = _positive_decimal_or_none(passive_market_making.get("ask_price"))
        bid_price = _positive_decimal_or_none(passive_market_making.get("bid_price"))
        half_spread_bps = _positive_decimal_or_none(passive_market_making.get("half_spread_bps"))
        midpoint = _positive_decimal_or_none(passive_market_making.get("midpoint"))

        placement_is_staged_release = (
            placement_kind == OrderPlacementKind.STAGED_RELEASE
            and placement_status == OrderPlacementStatus.STAGED
        )
        product_matches_metadata = (
            product_id is not None
            and metadata_product_id is not None
            and product_id == metadata_product_id
        )
        side_matches_metadata = (
            placement_side is not None
            and metadata_side is not None
            and placement_side == metadata_side
        )
        price_order_valid = (
            ask_price is not None
            and bid_price is not None
            and midpoint is not None
            and bid_price < midpoint < ask_price
        )
        limit_price_matches_passive_side_price: bool | None = None
        if placement_side == OrderSide.BUY:
            limit_price_matches_passive_side_price = _decimal_values_equal(
                placement.get("limit_price"),
                passive_market_making.get("bid_price"),
            )
        elif placement_side == OrderSide.SELL:
            limit_price_matches_passive_side_price = _decimal_values_equal(
                placement.get("limit_price"),
                passive_market_making.get("ask_price"),
            )

        valid = (
            metadata_object_valid
            and placement_is_staged_release
            and product_matches_metadata
            and side_matches_metadata
            and ask_price is not None
            and bid_price is not None
            and half_spread_bps is not None
            and midpoint is not None
            and price_order_valid
            and limit_price_matches_passive_side_price is True
        )
        if valid:
            continue

        anomalies.append(
            {
                "action_id": _string_or_none(placement.get("action_id")),
                "ask_price": _string_or_none(passive_market_making.get("ask_price")),
                "ask_price_valid": ask_price is not None,
                "bid_price": _string_or_none(passive_market_making.get("bid_price")),
                "bid_price_valid": bid_price is not None,
                "event_type": EventType.ORDER_PLACEMENT_RECORDED.value,
                "half_spread_bps": _string_or_none(passive_market_making.get("half_spread_bps")),
                "half_spread_bps_valid": half_spread_bps is not None,
                "limit_price": _string_or_none(placement.get("limit_price")),
                "limit_price_matches_passive_side_price": limit_price_matches_passive_side_price,
                "metadata_object_valid": metadata_object_valid,
                "metadata_product_id": metadata_product_id,
                "metadata_side": _string_or_none(passive_market_making.get("side")),
                "midpoint": _string_or_none(passive_market_making.get("midpoint")),
                "midpoint_valid": midpoint is not None,
                "passive_market_making_quote_contract_valid": False,
                "placement_id": placement_id,
                "placement_is_staged_release": placement_is_staged_release,
                "placement_kind": _string_or_none(placement.get("placement_kind")),
                "placement_status": _string_or_none(placement.get("placement_status")),
                "price_order_valid": price_order_valid,
                "product_id": product_id,
                "product_matches_metadata": product_matches_metadata,
                "sequence": placement_sequences_by_id.get(placement_id),
                "side": _string_or_none(placement.get("side")),
                "side_matches_metadata": side_matches_metadata,
            }
        )
    return anomalies


def _evaluate_logical_order_contract(
    *,
    anomalies: list[dict[str, JsonValue]],
    logical_orders: dict[str, dict[str, JsonValue]],
    payload: dict[str, JsonValue],
    record: AuditRecord,
) -> None:
    logical_order_id = _string_or_none(payload.get("logical_order_id"))
    root_order_id = _string_or_none(payload.get("root_order_id"))
    parent_order_id = _string_or_none(payload.get("parent_order_id"))
    lineage_relation = _order_lineage_relation_or_none(payload.get("lineage_relation"))
    product_id = _string_or_none(payload.get("product_id"))
    side = _order_side_or_none(payload.get("side"))
    size = _positive_decimal_or_none(payload.get("size"))
    source_order_ids = _string_list(payload.get("source_order_ids"))
    schema_version = _int_or_none(payload.get("schema_version"))
    metadata = _dict_or_empty(payload.get("metadata"))

    duplicate_logical_order_id = logical_order_id in logical_orders if logical_order_id is not None else False
    source_order_ids_valid = _is_string_list(payload.get("source_order_ids"))
    source_references_found = all(source_order_id in logical_orders for source_order_id in source_order_ids)
    parent = logical_orders.get(parent_order_id) if parent_order_id is not None else None
    root = logical_orders.get(root_order_id) if root_order_id is not None else None
    parent_reference_found = parent is not None if parent_order_id is not None else None
    root_reference_found = root is not None if root_order_id is not None else None
    root_self_reference = logical_order_id is not None and root_order_id == logical_order_id
    parent_is_root = parent_order_id is not None and parent_order_id == root_order_id

    relation_valid = lineage_relation is not None
    relation_shape_valid = False
    relationship_product_valid: bool | None = None
    relationship_side_valid: bool | None = None
    manual_association_approval_valid: bool | None = None
    if lineage_relation == OrderLineageRelation.ROOT:
        relation_shape_valid = root_self_reference and parent_order_id is None and not source_order_ids
    elif lineage_relation == OrderLineageRelation.EXTERNAL_IMPORT:
        relation_shape_valid = root_self_reference and parent_order_id is None
    elif lineage_relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL:
        relation_shape_valid = (
            parent_order_id is not None
            and root_order_id is not None
            and parent_is_root
            and bool(source_order_ids)
        )
        if parent is not None and product_id is not None and side is not None:
            relationship_product_valid = parent.get("product_id") == product_id
            relationship_side_valid = parent.get("side") != side.value
    elif lineage_relation == OrderLineageRelation.SPLIT_CHILD:
        relation_shape_valid = (
            parent_order_id is not None
            and root_order_id is not None
            and parent_is_root
            and bool(source_order_ids)
        )
        if parent is not None and product_id is not None and side is not None:
            relationship_product_valid = parent.get("product_id") == product_id
            relationship_side_valid = parent.get("side") == side.value
    elif lineage_relation == OrderLineageRelation.CONSOLIDATION:
        relation_shape_valid = parent_order_id is None and len(source_order_ids) >= 2
        source_orders = [logical_orders[source_order_id] for source_order_id in source_order_ids if source_order_id in logical_orders]
        if source_orders and product_id is not None and side is not None:
            relationship_product_valid = all(source.get("product_id") == product_id for source in source_orders)
            relationship_side_valid = all(source.get("side") == side.value for source in source_orders)
    elif lineage_relation == OrderLineageRelation.MANUAL_ASSOCIATION:
        relation_shape_valid = bool(source_order_ids)
        manual_association_approval_valid = _manual_association_approval_valid(metadata)

    valid = (
        schema_version == 1
        and logical_order_id is not None
        and root_order_id is not None
        and relation_valid
        and product_id is not None
        and side is not None
        and size is not None
        and source_order_ids_valid
        and not duplicate_logical_order_id
        and relation_shape_valid
        and (source_references_found or lineage_relation in {OrderLineageRelation.ROOT, OrderLineageRelation.EXTERNAL_IMPORT})
        and (
            root_self_reference
            or (root_reference_found is True)
            or lineage_relation in {OrderLineageRelation.ROOT, OrderLineageRelation.EXTERNAL_IMPORT}
        )
        and (parent_order_id is None or parent_reference_found is True)
        and relationship_product_valid is not False
        and relationship_side_valid is not False
        and manual_association_approval_valid is not False
    )

    if not valid:
        anomalies.append(
            {
                "duplicate_logical_order_id": duplicate_logical_order_id,
                "event_type": EventType.ORDER_LOGICAL_CREATED.value,
                "lineage_relation": _string_or_none(payload.get("lineage_relation")),
                "lineage_relation_valid": relation_valid,
                "logical_order_id": logical_order_id,
                "manual_association_approval_valid": manual_association_approval_valid,
                "parent_is_root": parent_is_root if parent_order_id is not None else None,
                "parent_order_id": parent_order_id,
                "parent_reference_found": parent_reference_found,
                "product_id": product_id,
                "product_id_present": product_id is not None,
                "relation_shape_valid": relation_shape_valid,
                "relationship_product_valid": relationship_product_valid,
                "relationship_side_valid": relationship_side_valid,
                "root_order_id": root_order_id,
                "root_reference_found": root_reference_found,
                "root_self_reference": root_self_reference,
                "schema_version": schema_version,
                "schema_version_valid": schema_version == 1,
                "sequence": record.sequence,
                "side": side.value if side is not None else _string_or_none(payload.get("side")),
                "side_valid": side is not None,
                "size": _string_or_none(payload.get("size")),
                "size_valid": size is not None,
                "source_order_ids": source_order_ids,
                "source_order_ids_valid": source_order_ids_valid,
                "source_references_found": source_references_found,
            }
        )

    if logical_order_id is not None and not duplicate_logical_order_id:
        logical_orders[logical_order_id] = {
            "logical_order_id": logical_order_id,
            "product_id": product_id,
            "root_order_id": root_order_id,
            "side": side.value if side is not None else None,
        }


def _evaluate_order_placement_contract(
    *,
    anomalies: list[dict[str, JsonValue]],
    logical_orders: dict[str, dict[str, JsonValue]],
    payload: dict[str, JsonValue],
    placement_counts_by_logical_order_id: dict[str, int],
    placements_by_id: dict[str, dict[str, JsonValue]],
    record: AuditRecord,
    released_stage_placement_ids: dict[str, str],
    seen_action_ids: set[str],
) -> None:
    placement_id = _string_or_none(payload.get("placement_id"))
    logical_order_id = _string_or_none(payload.get("logical_order_id"))
    placement_kind = _order_placement_kind_or_none(payload.get("placement_kind"))
    placement_status = _order_placement_status_or_none(payload.get("placement_status"))
    product_id = _string_or_none(payload.get("product_id"))
    side = _order_side_or_none(payload.get("side"))
    size = _positive_decimal_or_none(payload.get("size"))
    schema_version = _int_or_none(payload.get("schema_version"))
    action_id = _string_or_none(payload.get("action_id"))
    venue_client_order_id = _string_or_none(payload.get("venue_client_order_id"))
    exchange_order_id = _string_or_none(payload.get("exchange_order_id"))
    logical_order = logical_orders.get(logical_order_id) if logical_order_id is not None else None
    prior_placement_count = (
        placement_counts_by_logical_order_id.get(logical_order_id, 0) if logical_order_id is not None else 0
    )

    duplicate_placement_id = placement_id in placements_by_id if placement_id is not None else False
    logical_order_found = logical_order is not None
    placement_kind_valid = placement_kind is not None
    placement_status_valid = placement_status is not None
    product_matches_logical_order = (
        logical_order is not None and product_id is not None and logical_order.get("product_id") == product_id
    )
    side_matches_logical_order = (
        logical_order is not None and side is not None and logical_order.get("side") == side.value
    )
    venue_identifier_present = venue_client_order_id is not None or exchange_order_id is not None
    venue_identifier_required = placement_status is not None and placement_status != OrderPlacementStatus.STAGED
    action_reference_found = action_id in seen_action_ids if action_id is not None else None
    prior_placement_required = placement_kind in {
        OrderPlacementKind.AMEND,
        OrderPlacementKind.CANCEL_REPLACE,
        OrderPlacementKind.RELEASE,
    }
    prior_placement_found = prior_placement_count > 0
    release_contract = _release_placement_contract(
        logical_order_id=logical_order_id,
        placement_kind=placement_kind,
        placement_payload=payload,
        placements_by_id=placements_by_id,
        released_stage_placement_ids=released_stage_placement_ids,
    )

    valid = (
        schema_version == 1
        and placement_id is not None
        and logical_order_id is not None
        and placement_kind_valid
        and placement_status_valid
        and product_id is not None
        and side is not None
        and size is not None
        and not duplicate_placement_id
        and logical_order_found
        and product_matches_logical_order
        and side_matches_logical_order
        and (not venue_identifier_required or venue_identifier_present)
        and (action_id is None or action_reference_found is True)
        and (not prior_placement_required or prior_placement_found)
        and release_contract["release_contract_valid"] is not False
    )

    if not valid:
        anomalies.append(
            {
                "action_id": action_id,
                "action_reference_found": action_reference_found,
                "duplicate_placement_id": duplicate_placement_id,
                "event_type": EventType.ORDER_PLACEMENT_RECORDED.value,
                "exchange_order_id": exchange_order_id,
                "logical_order_found": logical_order_found,
                "logical_order_id": logical_order_id,
                "placement_id": placement_id,
                "placement_kind": _string_or_none(payload.get("placement_kind")),
                "placement_kind_valid": placement_kind_valid,
                "placement_status": _string_or_none(payload.get("placement_status")),
                "placement_status_valid": placement_status_valid,
                "prior_placement_count": prior_placement_count,
                "prior_placement_found": prior_placement_found,
                "prior_placement_required": prior_placement_required,
                "product_id": product_id,
                "product_matches_logical_order": product_matches_logical_order,
                "release_contract": release_contract,
                "release_contract_valid": release_contract["release_contract_valid"],
                "schema_version": schema_version,
                "schema_version_valid": schema_version == 1,
                "sequence": record.sequence,
                "side": side.value if side is not None else _string_or_none(payload.get("side")),
                "side_matches_logical_order": side_matches_logical_order,
                "side_valid": side is not None,
                "size": _string_or_none(payload.get("size")),
                "size_valid": size is not None,
                "venue_client_order_id": venue_client_order_id,
                "venue_identifier_present": venue_identifier_present,
                "venue_identifier_required": venue_identifier_required,
            }
        )

    if placement_id is not None and not duplicate_placement_id:
        placements_by_id[placement_id] = payload
    if logical_order_id is not None:
        placement_counts_by_logical_order_id[logical_order_id] = prior_placement_count + 1
    release_of_placement_id = _string_or_none(release_contract.get("release_of_placement_id"))
    if (
        placement_id is not None
        and release_contract["release_contract_valid"] is True
        and release_of_placement_id is not None
    ):
        released_stage_placement_ids[release_of_placement_id] = placement_id


def _release_placement_contract(
    *,
    logical_order_id: str | None,
    placement_kind: OrderPlacementKind | None,
    placement_payload: dict[str, JsonValue],
    placements_by_id: dict[str, dict[str, JsonValue]],
    released_stage_placement_ids: dict[str, str],
) -> dict[str, JsonValue]:
    if placement_kind != OrderPlacementKind.RELEASE:
        return {"release_contract_valid": None}

    metadata = _dict_or_empty(placement_payload.get("metadata"))
    staged_release = _dict_or_empty(metadata.get("staged_release"))
    release_of_placement_id = _string_or_none(staged_release.get("release_of_placement_id"))
    release_of_action_id = _string_or_none(staged_release.get("release_of_action_id"))
    staged_placement = (
        placements_by_id.get(release_of_placement_id)
        if release_of_placement_id is not None
        else None
    )
    staged_kind = (
        _order_placement_kind_or_none(staged_placement.get("placement_kind"))
        if staged_placement is not None
        else None
    )
    staged_status = (
        _order_placement_status_or_none(staged_placement.get("placement_status"))
        if staged_placement is not None
        else None
    )
    staged_action_id = (
        _string_or_none(staged_placement.get("action_id"))
        if staged_placement is not None
        else None
    )
    already_released_by_placement_id = (
        released_stage_placement_ids.get(release_of_placement_id)
        if release_of_placement_id is not None
        else None
    )
    staged_placement_found = staged_placement is not None
    staged_placement_is_staged_release = (
        staged_kind == OrderPlacementKind.STAGED_RELEASE
        and staged_status == OrderPlacementStatus.STAGED
    )
    release_of_action_matches = (
        release_of_action_id is not None
        and staged_action_id is not None
        and release_of_action_id == staged_action_id
    )
    logical_order_matches = (
        staged_placement is not None
        and logical_order_id is not None
        and _string_or_none(staged_placement.get("logical_order_id")) == logical_order_id
    )
    product_matches = (
        staged_placement is not None
        and _string_or_none(staged_placement.get("product_id"))
        == _string_or_none(placement_payload.get("product_id"))
    )
    side_matches = (
        staged_placement is not None
        and _string_or_none(staged_placement.get("side"))
        == _string_or_none(placement_payload.get("side"))
    )
    size_matches = _decimal_values_equal(
        staged_placement.get("size") if staged_placement is not None else None,
        placement_payload.get("size"),
    )
    limit_price_matches = _optional_decimal_values_equal(
        staged_placement.get("limit_price") if staged_placement is not None else None,
        placement_payload.get("limit_price"),
    )
    duplicate_release = already_released_by_placement_id is not None
    release_contract_valid = (
        release_of_placement_id is not None
        and release_of_action_id is not None
        and staged_placement_found
        and staged_placement_is_staged_release
        and release_of_action_matches
        and logical_order_matches
        and product_matches
        and side_matches
        and size_matches
        and limit_price_matches
        and not duplicate_release
    )

    return {
        "already_released_by_placement_id": already_released_by_placement_id,
        "duplicate_release": duplicate_release,
        "limit_price_matches": limit_price_matches,
        "logical_order_matches": logical_order_matches,
        "product_matches": product_matches,
        "release_contract_valid": release_contract_valid,
        "release_of_action_id": release_of_action_id,
        "release_of_action_matches": release_of_action_matches,
        "release_of_placement_id": release_of_placement_id,
        "side_matches": side_matches,
        "size_matches": size_matches,
        "staged_action_id": staged_action_id,
        "staged_placement_found": staged_placement_found,
        "staged_placement_is_staged_release": staged_placement_is_staged_release,
    }


def _order_update_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    received_payloads_by_sequence: dict[int, dict[str, JsonValue]] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        payload = _dict_or_empty(record.payload)
        if record.event_type == EventType.DATA_RECEIVED:
            received_payloads_by_sequence[record.sequence] = payload
            continue

        if record.event_type == EventType.DATA_ACCEPTED:
            if payload.get("message_event_type") != EventType.EXCHANGE_ORDER_UPDATE.value:
                continue
            received_sequence = _int_or_none(payload.get("received_sequence"))
            received_payload = (
                received_payloads_by_sequence.get(received_sequence)
                if received_sequence is not None
                else None
            )
            feed_payload = _dict_or_empty(received_payload.get("payload")) if received_payload is not None else {}
            order_update = _dict_or_empty(feed_payload.get("order"))
            contract = validate_exchange_order_update(order_update)
            if contract.valid:
                continue
            anomalies.append(
                {
                    **_order_update_anomaly_fields(record.sequence, order_update, contract),
                    "event_type": record.event_type.value,
                    "message_key": _string_or_none(payload.get("message_key")),
                    "received_found": received_payload is not None,
                    "received_sequence": received_sequence,
                    "source_id": _string_or_none(payload.get("source_id")),
                }
            )
            continue

        if record.event_type == EventType.RECONCILIATION_RECOVERY:
            order_update = _dict_or_empty(payload.get("order_update"))
            if not order_update:
                continue
            contract = validate_exchange_order_update(order_update)
            if contract.valid:
                continue
            anomalies.append(
                {
                    **_order_update_anomaly_fields(record.sequence, order_update, contract),
                    "action_id": _string_or_none(payload.get("action_id")),
                    "event_type": record.event_type.value,
                    "reason": _string_or_none(payload.get("reason")),
                }
            )

    return _check(
        LedgerHealthCheckName.ORDER_UPDATE_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _order_update_anomaly_fields(
    sequence: int,
    order_update: dict[str, JsonValue],
    contract: OrderUpdateContractResult,
) -> dict[str, JsonValue]:
    return {
        "client_order_id": _string_or_none(order_update.get("client_order_id")),
        "invalid_fields": list(contract.invalid_fields),
        "missing_fields": list(contract.missing_fields),
        "order_id": _string_or_none(order_update.get("order_id")),
        "product_id": _string_or_none(order_update.get("product_id")),
        "sequence": sequence,
        "status": _string_or_none(order_update.get("status")),
    }


def _exchange_state_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    for record in records:
        if record.event_type == EventType.EXCHANGE_BALANCE_SNAPSHOT:
            payload = _dict_or_empty(record.payload)
            contract = validate_exchange_balance_snapshot(payload)
        elif record.event_type == EventType.EXCHANGE_POSITION_SNAPSHOT:
            payload = _dict_or_empty(record.payload)
            contract = validate_exchange_position_snapshot(payload)
        else:
            continue

        if contract.valid:
            continue

        anomalies.append(
            {
                "account_id": _string_or_none(payload.get("account_id")),
                "currency": _string_or_none(payload.get("currency")),
                "event_type": record.event_type.value,
                "invalid_fields": list(contract.invalid_fields),
                "missing_fields": list(contract.missing_fields),
                "net_size": _string_or_none(payload.get("net_size")),
                "product_id": _string_or_none(payload.get("product_id")),
                "sequence": record.sequence,
                "venue": _string_or_none(payload.get("venue")),
            }
        )

    return _check(
        LedgerHealthCheckName.EXCHANGE_STATE_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _live_preflight_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    anomalies: list[dict[str, JsonValue]] = []
    expected_steps = [step.value for step in _LIVE_PREFLIGHT_EXPECTED_STEPS]
    for record in records:
        if record.event_type != EventType.LIVE_PREFLIGHT_RESULT:
            continue

        payload = _dict_or_empty(record.payload)
        missing_fields: list[str] = []
        invalid_fields: list[str] = []
        for field_name in (
            "completed_step_names",
            "config_fingerprint",
            "fingerprint_algorithm",
            "ledger_path",
            "order_endpoint_called",
            "runtime_tasks_started",
            "schema_version",
            "skipped_step_names",
            "status",
            "step_statuses",
            "strategy_tasks_started",
        ):
            if field_name not in payload:
                missing_fields.append(field_name)

        status = _readiness_status_or_none(payload.get("status"))
        completed_steps = _string_list(payload.get("completed_step_names"))
        skipped_steps = _string_list(payload.get("skipped_step_names"))
        stopped_after = _string_or_none(payload.get("stopped_after_step"))
        step_statuses = _object_list_or_none(payload.get("step_statuses"))

        if payload.get("schema_version") != 1:
            invalid_fields.append("schema_version")
        if _string_or_none(payload.get("config_fingerprint")) is None:
            invalid_fields.append("config_fingerprint")
        if payload.get("fingerprint_algorithm") != CONFIG_FINGERPRINT_ALGORITHM:
            invalid_fields.append("fingerprint_algorithm")
        if _string_or_none(payload.get("ledger_path")) is None:
            invalid_fields.append("ledger_path")
        if status is None:
            invalid_fields.append("status")
        if not _is_string_list(payload.get("completed_step_names")):
            invalid_fields.append("completed_step_names")
        if not _is_string_list(payload.get("skipped_step_names")):
            invalid_fields.append("skipped_step_names")
        if step_statuses is None:
            invalid_fields.append("step_statuses")
        for field_name in ("order_endpoint_called", "runtime_tasks_started", "strategy_tasks_started"):
            if payload.get(field_name) is not False:
                invalid_fields.append(field_name)

        steps_partition_valid = _preflight_steps_partition_valid(
            completed_steps=completed_steps,
            expected_steps=expected_steps,
            skipped_steps=skipped_steps,
            status=status,
            stopped_after=stopped_after,
        )
        if not steps_partition_valid:
            invalid_fields.append("completed_step_names")
            invalid_fields.append("skipped_step_names")
            invalid_fields.append("stopped_after_step")

        if step_statuses is not None and not _preflight_step_statuses_valid(
            completed_steps=completed_steps,
            status=status,
            step_statuses=step_statuses,
        ):
            invalid_fields.append("step_statuses")

        if not missing_fields and not invalid_fields:
            continue

        anomalies.append(
            {
                "completed_step_names": completed_steps,
                "invalid_fields": sorted(set(invalid_fields)),
                "missing_fields": missing_fields,
                "sequence": record.sequence,
                "skipped_step_names": skipped_steps,
                "status": _string_or_none(payload.get("status")),
                "stopped_after_step": stopped_after,
            }
        )

    return _check(
        LedgerHealthCheckName.LIVE_PREFLIGHT_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


_LIVE_PREFLIGHT_EXPECTED_STEPS = (
    PreflightStep.READINESS,
    PreflightStep.PRODUCT_CATALOG_SMOKE,
    PreflightStep.FEED_SMOKE,
    PreflightStep.EXCHANGE_STATE_SMOKE,
)


def _preflight_steps_partition_valid(
    *,
    completed_steps: list[str],
    expected_steps: list[str],
    skipped_steps: list[str],
    status: ReadinessStatus | None,
    stopped_after: str | None,
) -> bool:
    if len(completed_steps) != len(set(completed_steps)):
        return False
    if len(skipped_steps) != len(set(skipped_steps)):
        return False
    if any(step not in expected_steps for step in completed_steps + skipped_steps):
        return False
    if set(completed_steps).intersection(skipped_steps):
        return False
    if status == ReadinessStatus.OK:
        return completed_steps == expected_steps and not skipped_steps and stopped_after is None
    if status == ReadinessStatus.ATTENTION_REQUIRED:
        if stopped_after not in completed_steps:
            return False
        stopped_index = expected_steps.index(stopped_after)
        return (
            completed_steps == expected_steps[: stopped_index + 1]
            and skipped_steps == expected_steps[stopped_index + 1 :]
        )
    return False


def _preflight_step_statuses_valid(
    *,
    completed_steps: list[str],
    status: ReadinessStatus | None,
    step_statuses: list[dict[str, JsonValue]],
) -> bool:
    if len(step_statuses) != len(completed_steps):
        return False
    names = [_string_or_none(step.get("name")) for step in step_statuses]
    statuses = [_readiness_status_or_none(step.get("status")) for step in step_statuses]
    if names != completed_steps or any(step_status is None for step_status in statuses):
        return False
    if not statuses:
        return False
    if status == ReadinessStatus.OK:
        return all(step_status == ReadinessStatus.OK for step_status in statuses)
    if status == ReadinessStatus.ATTENTION_REQUIRED:
        return (
            all(step_status == ReadinessStatus.OK for step_status in statuses[:-1])
            and statuses[-1] == ReadinessStatus.ATTENTION_REQUIRED
        )
    return False


@dataclass(frozen=True)
class _ActionLifecycleRef:
    action_id: str
    action_type: str


_ACTION_LIFECYCLE_EVENTS = frozenset(
    {
        EventType.ACTION_ACCEPTED,
        EventType.ACTION_EXECUTION_FAILED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTED,
        EventType.ACTION_REJECTED,
        EventType.ACTION_REQUESTED,
    }
)


def _action_lifecycle_contract_check(records: tuple[AuditRecord, ...]) -> LedgerHealthCheckResult:
    requested_by_sequence: dict[int, _ActionLifecycleRef] = {}
    accepted_by_sequence: dict[int, _ActionLifecycleRef] = {}
    started_by_sequence: dict[int, _ActionLifecycleRef] = {}
    anomalies: list[dict[str, JsonValue]] = []

    for record in records:
        if record.event_type not in _ACTION_LIFECYCLE_EVENTS:
            continue

        payload = _dict_or_empty(record.payload)
        action_ref = _action_lifecycle_ref(record, payload, anomalies)
        if record.event_type == EventType.ACTION_REQUESTED:
            if action_ref is not None:
                requested_by_sequence[record.sequence] = action_ref
            continue

        if record.event_type in {EventType.ACTION_ACCEPTED, EventType.ACTION_REJECTED}:
            requested_matches = _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="requested_sequence",
                payload=payload,
                referenced_by_sequence=requested_by_sequence,
                anomalies=anomalies,
            )
            if action_ref is not None and requested_matches and record.event_type == EventType.ACTION_ACCEPTED:
                accepted_by_sequence[record.sequence] = action_ref
            continue

        if record.event_type == EventType.ACTION_EXECUTION_STARTED:
            requested_matches = _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="requested_sequence",
                payload=payload,
                referenced_by_sequence=requested_by_sequence,
                anomalies=anomalies,
            )
            accepted_matches = _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="accepted_sequence",
                payload=payload,
                referenced_by_sequence=accepted_by_sequence,
                anomalies=anomalies,
            )
            if action_ref is not None and requested_matches and accepted_matches:
                started_by_sequence[record.sequence] = action_ref
            continue

        if record.event_type == EventType.ACTION_EXECUTED:
            _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="requested_sequence",
                payload=payload,
                referenced_by_sequence=requested_by_sequence,
                anomalies=anomalies,
            )
            _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="execution_started_sequence",
                payload=payload,
                referenced_by_sequence=started_by_sequence,
                anomalies=anomalies,
            )
            continue

        if record.event_type == EventType.ACTION_EXECUTION_FAILED:
            _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="requested_sequence",
                payload=payload,
                referenced_by_sequence=requested_by_sequence,
                anomalies=anomalies,
            )
            _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="accepted_sequence",
                payload=payload,
                referenced_by_sequence=accepted_by_sequence,
                anomalies=anomalies,
            )
            _append_lifecycle_reference_anomaly(
                record,
                action_ref=action_ref,
                field_name="execution_started_sequence",
                payload=payload,
                referenced_by_sequence=started_by_sequence,
                anomalies=anomalies,
            )

    return _check(
        LedgerHealthCheckName.ACTION_LIFECYCLE_CONTRACT,
        count=len(anomalies),
        details={
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        },
    )


def _action_lifecycle_ref(
    record: AuditRecord,
    payload: dict[str, JsonValue],
    anomalies: list[dict[str, JsonValue]],
) -> _ActionLifecycleRef | None:
    action_id = _string_or_none(payload.get("action_id"))
    action_type = _string_or_none(payload.get("action_type"))
    missing_fields = [
        field_name
        for field_name, value in (("action_id", action_id), ("action_type", action_type))
        if value is None
    ]
    if missing_fields:
        anomalies.append(
            {
                "action_id": action_id,
                "action_type": action_type,
                "event_type": record.event_type.value,
                "missing_fields": missing_fields,
                "sequence": record.sequence,
            }
        )
        return None
    return _ActionLifecycleRef(action_id=action_id, action_type=action_type)


def _append_lifecycle_reference_anomaly(
    record: AuditRecord,
    *,
    action_ref: _ActionLifecycleRef | None,
    field_name: str,
    payload: dict[str, JsonValue],
    referenced_by_sequence: dict[int, _ActionLifecycleRef],
    anomalies: list[dict[str, JsonValue]],
) -> bool:
    referenced_sequence = _int_or_none(payload.get(field_name))
    referenced = referenced_by_sequence.get(referenced_sequence) if referenced_sequence is not None else None
    reference_found = referenced is not None
    action_id_matches = (
        action_ref is not None
        and referenced is not None
        and action_ref.action_id == referenced.action_id
    )
    action_type_matches = (
        action_ref is not None
        and referenced is not None
        and action_ref.action_type == referenced.action_type
    )
    if reference_found and action_id_matches and action_type_matches:
        return True

    anomalies.append(
        {
            "action_id": action_ref.action_id if action_ref is not None else None,
            "action_id_matches": action_id_matches,
            "action_type": action_ref.action_type if action_ref is not None else None,
            "action_type_matches": action_type_matches,
            "event_type": record.event_type.value,
            "field": field_name,
            "field_present": referenced_sequence is not None,
            "reference_found": reference_found,
            "referenced_action_id": referenced.action_id if referenced is not None else None,
            "referenced_action_type": referenced.action_type if referenced is not None else None,
            "referenced_sequence": referenced_sequence,
            "sequence": record.sequence,
        }
    )
    return False


def _product_catalog_freshness_check(projection: SourceOfTruthProjection) -> LedgerHealthCheckResult:
    config = _latest_product_catalog_config(projection)
    schedule = _dict_or_empty(config.get("schedule"))
    enabled = schedule.get("enabled") is True
    run_on_start = schedule.get("run_on_start") is True
    latest_start = _latest_orchestrator_start(projection)
    latest_start_sequence = latest_start.sequence if latest_start is not None else None
    latest_snapshot_sequence = (
        projection.exchange_product_snapshot_sequences[-1]
        if projection.exchange_product_snapshot_sequences
        else None
    )
    missing_snapshot = enabled and latest_snapshot_sequence is None
    missing_startup_snapshot = (
        enabled
        and run_on_start
        and latest_start_sequence is not None
        and (latest_snapshot_sequence is None or latest_snapshot_sequence < latest_start_sequence)
    )
    return _check(
        LedgerHealthCheckName.PRODUCT_CATALOG_FRESHNESS,
        count=int(missing_snapshot or missing_startup_snapshot),
        details={
            "configured_product_ids": _string_list(config.get("product_ids")),
            "enabled": enabled,
            "interval_seconds": _float_or_none(schedule.get("interval_seconds")),
            "latest_product_snapshot_sequence": latest_snapshot_sequence,
            "latest_start_sequence": latest_start_sequence,
            "missing_snapshot": missing_snapshot,
            "missing_startup_snapshot": missing_startup_snapshot,
            "product_count": projection.exchange_product_count,
            "product_snapshot_count": projection.exchange_product_snapshot_count,
            "records_after_latest_product_snapshot": _records_after_sequence(
                projection.last_sequence,
                latest_snapshot_sequence,
            ),
            "run_on_start": run_on_start,
        },
    )


def _live_execution_venue_check(projection: SourceOfTruthProjection) -> LedgerHealthCheckResult:
    allowed_venues = COINBASE_LIVE_EXECUTION_PRODUCT_VENUES
    unsupported_orders: list[dict[str, JsonValue]] = []
    missing_metadata_orders: list[dict[str, JsonValue]] = []

    for order in projection.orders_by_action_id.values():
        if order.execution_mode != ExecutionMode.LIVE or order.execution_status != ExecutionStatus.ACCEPTED:
            continue

        base_details: dict[str, JsonValue] = {
            "action_id": order.action_id,
            "exchange_order_id": order.exchange_order_id,
            "executed_sequence": order.executed_sequence,
            "product_id": order.product_id,
        }
        product = projection.exchange_products_by_product_id.get(order.product_id)
        if product is None:
            missing_metadata_orders.append(base_details)
            continue
        if product.product_venue not in allowed_venues:
            unsupported_orders.append(
                {
                    **base_details,
                    "product_venue": product.product_venue.value,
                }
            )

    return _check(
        LedgerHealthCheckName.LIVE_EXECUTION_VENUE,
        count=len(unsupported_orders) + len(missing_metadata_orders),
        details={
            "allowed_product_venues": [venue.value for venue in allowed_venues],
            "missing_product_metadata_count": len(missing_metadata_orders),
            "missing_product_metadata_orders": missing_metadata_orders,
            "unsupported_product_venue_count": len(unsupported_orders),
            "unsupported_product_venue_orders": unsupported_orders,
        },
    )


def _latest_product_catalog_config(projection: SourceOfTruthProjection) -> dict[str, JsonValue]:
    latest_start = _latest_orchestrator_start(projection)
    if latest_start is None:
        return {}
    startup_metadata = latest_start.startup_metadata
    application_config = _dict_or_empty(startup_metadata.get("application_config"))
    snapshot = _dict_or_empty(application_config.get("snapshot"))
    bot = _dict_or_empty(snapshot.get("bot"))
    return _dict_or_empty(bot.get("product_catalog"))


def _latest_orchestrator_start(projection: SourceOfTruthProjection) -> SystemStartSnapshot | None:
    for start in reversed(projection.system_starts):
        if start.component == RuntimeComponent.ORCHESTRATOR:
            return start
    return None


def _dict_or_empty(value: JsonValue) -> dict[str, JsonValue]:
    if isinstance(value, dict):
        return value
    return {}


def _string_or_none(value: JsonValue) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _manual_association_approval_valid(metadata: dict[str, JsonValue]) -> bool:
    approval = _dict_or_empty(metadata.get("manual_association_approval"))
    return all(
        _string_or_none(approval.get(field_name)) is not None
        for field_name in ("approved_at", "approved_by", "reason")
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _action_type_or_none(value: JsonValue) -> ActionType | None:
    try:
        return ActionType(value)
    except (TypeError, ValueError):
        return None


def _action_status_or_none(value: JsonValue) -> ActionStatus | None:
    try:
        return ActionStatus(value)
    except (TypeError, ValueError):
        return None


def _event_type_or_none(value: JsonValue) -> EventType | None:
    try:
        return EventType(value)
    except (TypeError, ValueError):
        return None


def _execution_status_or_none(value: JsonValue) -> ExecutionStatus | None:
    try:
        return ExecutionStatus(value)
    except (TypeError, ValueError):
        return None


def _execution_mode_or_none(value: JsonValue) -> ExecutionMode | None:
    try:
        return ExecutionMode(value)
    except (TypeError, ValueError):
        return None


def _feed_stop_reason_or_none(value: JsonValue) -> FeedStopReason | None:
    try:
        return FeedStopReason(value)
    except (TypeError, ValueError):
        return None


def _error_category_or_none(payload: dict[str, JsonValue]) -> ErrorCategory | None:
    error = _dict_or_empty(payload.get("error"))
    raw_category = payload.get("error_category") or error.get("category")
    try:
        return ErrorCategory(raw_category)
    except (TypeError, ValueError):
        return None


def _runtime_component_or_none(value: JsonValue) -> RuntimeComponent | None:
    try:
        return RuntimeComponent(value)
    except (TypeError, ValueError):
        return None


def _runtime_stop_reason_or_none(value: JsonValue) -> RuntimeStopReason | None:
    try:
        return RuntimeStopReason(value)
    except (TypeError, ValueError):
        return None


def _readiness_status_or_none(value: JsonValue) -> ReadinessStatus | None:
    try:
        return ReadinessStatus(value)
    except (TypeError, ValueError):
        return None


def _ledger_health_status_or_none(value: JsonValue) -> LedgerHealthStatus | None:
    try:
        return LedgerHealthStatus(value)
    except (TypeError, ValueError):
        return None


def _ledger_health_check_name_or_none(value: JsonValue) -> LedgerHealthCheckName | None:
    try:
        return LedgerHealthCheckName(value)
    except (TypeError, ValueError):
        return None


def _runtime_task_or_none(value: JsonValue) -> RuntimeTask | None:
    try:
        return RuntimeTask(value)
    except (TypeError, ValueError):
        return None


def _market_data_kind_or_none(value: JsonValue) -> MarketDataKind | None:
    try:
        return MarketDataKind(value)
    except (TypeError, ValueError):
        return None


def _strategy_evaluation_status_or_none(value: JsonValue) -> StrategyEvaluationStatus | None:
    try:
        return StrategyEvaluationStatus(value)
    except (TypeError, ValueError):
        return None


def _strategy_simulation_status_or_none(value: JsonValue) -> StrategySimulationStatus | None:
    try:
        return StrategySimulationStatus(value)
    except (TypeError, ValueError):
        return None


def _operator_canary_evidence_issue_or_none(
    value: JsonValue,
) -> OperatorCanaryEvidenceIssue | None:
    try:
        return OperatorCanaryEvidenceIssue(value)
    except (TypeError, ValueError):
        return None


def _strategy_input_status_or_none(value: JsonValue) -> StrategyInputStatus | None:
    try:
        return StrategyInputStatus(value)
    except (TypeError, ValueError):
        return None


def _trigger_relation_or_none(value: JsonValue) -> TriggerRelation | None:
    try:
        return TriggerRelation(value)
    except (TypeError, ValueError):
        return None


def _normalized_order_side_or_none(value: JsonValue) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized in {"buy", "sell"}:
        return normalized
    return None


def _order_side_or_none(value: JsonValue) -> OrderSide | None:
    try:
        return OrderSide(value)
    except (TypeError, ValueError):
        return None


def _order_lineage_relation_or_none(value: JsonValue) -> OrderLineageRelation | None:
    try:
        return OrderLineageRelation(value)
    except (TypeError, ValueError):
        return None


def _order_placement_kind_or_none(value: JsonValue) -> OrderPlacementKind | None:
    try:
        return OrderPlacementKind(value)
    except (TypeError, ValueError):
        return None


def _order_placement_status_or_none(value: JsonValue) -> OrderPlacementStatus | None:
    try:
        return OrderPlacementStatus(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: JsonValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _float_or_none(value: JsonValue) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _positive_decimal_or_none(value: JsonValue) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not decimal.is_finite() or decimal <= 0:
        return None
    return decimal


def _decimal_values_equal(left: JsonValue, right: JsonValue) -> bool:
    left_decimal = _positive_decimal_or_none(left)
    right_decimal = _positive_decimal_or_none(right)
    return left_decimal is not None and right_decimal is not None and left_decimal == right_decimal


def _optional_decimal_values_equal(left: JsonValue, right: JsonValue) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return _decimal_values_equal(left, right)


def _string_list(value: JsonValue) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _object_list_or_none(value: JsonValue) -> list[dict[str, JsonValue]] | None:
    if not isinstance(value, list):
        return None
    objects: list[dict[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, dict):
            return None
        objects.append(item)
    return objects


def _json_type_name(value: JsonValue) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _is_string_list(value: JsonValue) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) and bool(item) for item in value)


def _is_sha256_hex(value: JsonValue) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )
