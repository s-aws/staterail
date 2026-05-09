from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from audit.ledger import AuditLedger, AuditRecord, GENESIS_HASH
from core.clock import Clock, SystemClock
from core.enums import DigestAlgorithm, EventType
from core.errors import AuditIntegrityError
from core.json_tools import JsonValue, canonical_json, normalize_json


CHECKPOINT_SCHEMA_VERSION = 1


class LedgerCheckpointError(AuditIntegrityError):
    pass


@dataclass(frozen=True)
class LedgerCheckpoint:
    created_at: datetime
    digest_algorithm: DigestAlgorithm
    ledger_path: str
    record_count: int
    records_digest: str
    through_hash: str
    through_sequence: int
    checkpoint_hash: str
    schema_version: int = CHECKPOINT_SCHEMA_VERSION

    def to_payload(self) -> dict[str, JsonValue]:
        return _checkpoint_payload(
            created_at=self.created_at,
            digest_algorithm=self.digest_algorithm,
            ledger_path=self.ledger_path,
            record_count=self.record_count,
            records_digest=self.records_digest,
            through_hash=self.through_hash,
            through_sequence=self.through_sequence,
            checkpoint_hash=self.checkpoint_hash,
            schema_version=self.schema_version,
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "LedgerCheckpoint":
        try:
            return cls(
                checkpoint_hash=str(payload["checkpoint_hash"]),
                created_at=datetime.fromisoformat(str(payload["created_at"])).astimezone(timezone.utc),
                digest_algorithm=DigestAlgorithm(payload["digest_algorithm"]),
                ledger_path=str(payload["ledger_path"]),
                record_count=int(payload["record_count"]),
                records_digest=str(payload["records_digest"]),
                schema_version=int(payload["schema_version"]),
                through_hash=str(payload["through_hash"]),
                through_sequence=int(payload["through_sequence"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerCheckpointError("Malformed ledger checkpoint") from exc


@dataclass(frozen=True)
class RecordedLedgerCheckpoint:
    audit_record_hash: str
    audit_record_sequence: int
    checkpoint: LedgerCheckpoint

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "audit_record_hash": self.audit_record_hash,
            "audit_record_sequence": self.audit_record_sequence,
            "checkpoint": self.checkpoint.to_payload(),
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Recorded checkpoint payload must normalize to an object")
        return normalized

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "RecordedLedgerCheckpoint":
        checkpoint_payload = payload.get("checkpoint")
        if not isinstance(checkpoint_payload, Mapping):
            raise LedgerCheckpointError("Malformed recorded ledger checkpoint")
        try:
            return cls(
                audit_record_hash=str(payload["audit_record_hash"]),
                audit_record_sequence=int(payload["audit_record_sequence"]),
                checkpoint=LedgerCheckpoint.from_payload(checkpoint_payload),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerCheckpointError("Malformed recorded ledger checkpoint") from exc


def create_ledger_checkpoint(
    ledger: AuditLedger,
    *,
    clock: Clock | None = None,
) -> LedgerCheckpoint:
    snapshot = ledger.snapshot()
    state = snapshot.state
    records = snapshot.records
    created_at = (clock or SystemClock()).now().astimezone(timezone.utc)
    through_sequence = state.next_sequence - 1
    records_digest = _records_digest(records)
    unsigned_payload = _checkpoint_payload(
        created_at=created_at,
        digest_algorithm=DigestAlgorithm.SHA256,
        ledger_path=ledger.path.as_posix(),
        record_count=len(records),
        records_digest=records_digest,
        through_hash=state.last_hash,
        through_sequence=through_sequence,
        checkpoint_hash=None,
        schema_version=CHECKPOINT_SCHEMA_VERSION,
    )
    unsigned_payload.pop("checkpoint_hash")
    checkpoint_hash = _sha256(canonical_json(unsigned_payload))
    return LedgerCheckpoint(
        checkpoint_hash=checkpoint_hash,
        created_at=created_at,
        digest_algorithm=DigestAlgorithm.SHA256,
        ledger_path=ledger.path.as_posix(),
        record_count=len(records),
        records_digest=records_digest,
        through_hash=state.last_hash,
        through_sequence=through_sequence,
    )


def record_ledger_checkpoint(
    path: str | Path,
    *,
    clock: Clock | None = None,
) -> RecordedLedgerCheckpoint:
    ledger_path = Path(path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger does not exist: {ledger_path}")

    ledger = AuditLedger(ledger_path, clock=clock)
    checkpoint = create_ledger_checkpoint(ledger, clock=clock)
    record = ledger.append(EventType.AUDIT_CHECKPOINT, checkpoint.to_payload())
    return RecordedLedgerCheckpoint(
        audit_record_hash=record.record_hash,
        audit_record_sequence=record.sequence,
        checkpoint=checkpoint,
    )


def latest_recorded_ledger_checkpoint(path: str | Path) -> RecordedLedgerCheckpoint:
    ledger_path = Path(path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger does not exist: {ledger_path}")

    latest_record: AuditRecord | None = None
    ledger = AuditLedger(ledger_path)
    for record in ledger.iter_records():
        if record.event_type == EventType.AUDIT_CHECKPOINT:
            latest_record = record
    if latest_record is None:
        raise LedgerCheckpointError("Ledger has no recorded checkpoints")
    payload = latest_record.payload
    if not isinstance(payload, Mapping):
        raise LedgerCheckpointError("Malformed ledger checkpoint payload")
    checkpoint = LedgerCheckpoint.from_payload(payload)
    if checkpoint.through_sequence >= latest_record.sequence:
        raise LedgerCheckpointError("Checkpoint cannot include its own audit record")
    verify_ledger_checkpoint(ledger, checkpoint)
    return RecordedLedgerCheckpoint(
        audit_record_hash=latest_record.record_hash,
        audit_record_sequence=latest_record.sequence,
        checkpoint=checkpoint,
    )


def verify_ledger_checkpoint(ledger: AuditLedger, checkpoint: LedgerCheckpoint) -> None:
    _assert_checkpoint_hash(checkpoint)
    records = _prefix_records(ledger.iter_records(), checkpoint.through_sequence)
    if checkpoint.record_count != len(records):
        raise LedgerCheckpointError("Checkpoint record count does not match ledger prefix")
    if checkpoint.records_digest != _records_digest(records):
        raise LedgerCheckpointError("Checkpoint records digest does not match ledger prefix")

    through_hash = records[-1].record_hash if records else GENESIS_HASH
    if checkpoint.through_hash != through_hash:
        raise LedgerCheckpointError("Checkpoint through hash does not match ledger prefix")


def verify_recorded_ledger_checkpoints(ledger: AuditLedger) -> int:
    verified = 0
    for record in ledger.iter_records():
        if record.event_type != EventType.AUDIT_CHECKPOINT:
            continue
        payload = record.payload
        if not isinstance(payload, dict):
            raise LedgerCheckpointError("Malformed ledger checkpoint payload")
        checkpoint = LedgerCheckpoint.from_payload(payload)
        if checkpoint.through_sequence >= record.sequence:
            raise LedgerCheckpointError("Checkpoint cannot include its own audit record")
        verify_ledger_checkpoint(ledger, checkpoint)
        verified += 1
    return verified


def _assert_checkpoint_hash(checkpoint: LedgerCheckpoint) -> None:
    unsigned_payload = checkpoint.to_payload()
    unsigned_payload.pop("checkpoint_hash")
    expected = _sha256(canonical_json(unsigned_payload))
    if checkpoint.checkpoint_hash != expected:
        raise LedgerCheckpointError("Checkpoint hash mismatch")


def _checkpoint_payload(
    *,
    checkpoint_hash: str | None,
    created_at: datetime,
    digest_algorithm: DigestAlgorithm,
    ledger_path: str,
    record_count: int,
    records_digest: str,
    schema_version: int,
    through_hash: str,
    through_sequence: int,
) -> dict[str, JsonValue]:
    payload = {
        "checkpoint_hash": checkpoint_hash,
        "created_at": created_at,
        "digest_algorithm": digest_algorithm,
        "ledger_path": ledger_path,
        "record_count": record_count,
        "records_digest": records_digest,
        "schema_version": schema_version,
        "through_hash": through_hash,
        "through_sequence": through_sequence,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("Checkpoint payload must normalize to an object")
    return normalized


def _prefix_records(records: Iterable[AuditRecord], through_sequence: int) -> tuple[AuditRecord, ...]:
    return tuple(record for record in records if record.sequence <= through_sequence)


def _records_digest(records: Iterable[AuditRecord]) -> str:
    return _sha256(canonical_json([record.to_dict() for record in records]))


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
