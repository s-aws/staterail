from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any

from core.clock import Clock, SystemClock
from core.enums import EventType
from core.errors import AuditIntegrityError
from core.json_tools import JsonValue, canonical_json, normalize_json


GENESIS_HASH = "0" * 64
SCHEMA_VERSION = 1


class LedgerCorruptionError(AuditIntegrityError):
    pass


class LedgerLockError(AuditIntegrityError):
    pass


@dataclass(frozen=True)
class LedgerState:
    next_sequence: int
    last_hash: str


@dataclass(frozen=True)
class LedgerSnapshot:
    state: LedgerState
    records: tuple["AuditRecord", ...]


@dataclass(frozen=True)
class AuditRecord:
    sequence: int
    record_id: str
    occurred_at: datetime
    event_type: EventType
    payload: JsonValue
    previous_hash: str
    payload_hash: str
    record_hash: str
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "occurred_at": self.occurred_at.astimezone(timezone.utc).isoformat(),
            "payload": normalize_json(self.payload),
            "payload_hash": self.payload_hash,
            "previous_hash": self.previous_hash,
            "record_hash": self.record_hash,
            "record_id": self.record_id,
            "schema_version": self.schema_version,
            "sequence": self.sequence,
        }


class AuditLedger:
    def __init__(self, path: str | Path, clock: Clock | None = None) -> None:
        self._path = Path(path)
        self._clock = clock or SystemClock()
        self._lock = RLock()
        self._lock_path = self._path.with_name(f"{self._path.name}.lock")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._process_lock():
                self._state = self._verify_locked()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def lock_path(self) -> Path:
        return self._lock_path

    def append(self, event_type: EventType, payload: Mapping[str, Any] | None = None) -> AuditRecord:
        if not isinstance(event_type, EventType):
            raise TypeError("event_type must be an EventType")

        with self._lock:
            with self._process_lock():
                self._state = self._verify_locked()
                normalized_payload = normalize_json(payload or {})
                payload_hash = _sha256(canonical_json(normalized_payload))
                unsigned = {
                    "event_type": event_type.value,
                    "occurred_at": self._clock.now().astimezone(timezone.utc).isoformat(),
                    "payload": normalized_payload,
                    "payload_hash": payload_hash,
                    "previous_hash": self._state.last_hash,
                    "record_id": uuid.uuid4().hex,
                    "schema_version": SCHEMA_VERSION,
                    "sequence": self._state.next_sequence,
                }
                record_hash = _sha256(canonical_json(unsigned))
                raw_record = {**unsigned, "record_hash": record_hash}
                record = audit_record_from_dict(raw_record)

                with self._path.open("a", encoding="utf-8", newline="\n") as handle:
                    handle.write(canonical_json(raw_record))
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())

                self._state = LedgerState(
                    next_sequence=record.sequence + 1,
                    last_hash=record.record_hash,
                )
                return record

    def iter_records(self) -> tuple[AuditRecord, ...]:
        return self.snapshot().records

    def verify(self) -> LedgerState:
        return self.snapshot().state

    def snapshot(self) -> LedgerSnapshot:
        with self._lock:
            with self._process_lock():
                self._state = self._verify_locked()
                records = tuple(self._read_records_locked())
                return LedgerSnapshot(state=self._state, records=records)

    @contextmanager
    def _process_lock(self) -> Iterator[None]:
        try:
            descriptor = os.open(
                self._lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise LedgerLockError(f"Ledger is locked by another process: {self._lock_path}") from exc

        try:
            os.write(descriptor, f"{os.getpid()}\n".encode("utf-8"))
            yield
        finally:
            os.close(descriptor)
            try:
                self._lock_path.unlink()
            except FileNotFoundError:
                pass

    def _verify_locked(self) -> LedgerState:
        self._assert_append_boundary_locked()
        previous_hash = GENESIS_HASH
        expected_sequence = 1
        for line_number, record in self._iter_records_with_line_numbers():
            previous_hash = _verify_audit_record(
                record,
                expected_sequence=expected_sequence,
                line_number=line_number,
                previous_hash=previous_hash,
            )
            expected_sequence += 1

        return LedgerState(next_sequence=expected_sequence, last_hash=previous_hash)

    def _assert_append_boundary_locked(self) -> None:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return
        with self._path.open("rb") as handle:
            handle.seek(-1, os.SEEK_END)
            if handle.read(1) != b"\n":
                raise LedgerCorruptionError("Ledger does not end with an append boundary newline")

    def _read_records_locked(self) -> Iterator[AuditRecord]:
        for _, record in self._iter_records_with_line_numbers():
            yield record

    def _iter_records_with_line_numbers(self) -> Iterator[tuple[int, AuditRecord]]:
        if not self._path.exists():
            return

        with self._path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    raise LedgerCorruptionError(f"Blank ledger line at {line_number}")
                try:
                    raw_record = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise LedgerCorruptionError(f"Invalid JSON on line {line_number}") from exc
                yield line_number, audit_record_from_dict(raw_record)


def audit_record_from_dict(raw_record: Mapping[str, Any]) -> AuditRecord:
    try:
        occurred_at = datetime.fromisoformat(str(raw_record["occurred_at"])).astimezone(timezone.utc)
        return AuditRecord(
            event_type=EventType(raw_record["event_type"]),
            occurred_at=occurred_at,
            payload=normalize_json(raw_record["payload"]),
            payload_hash=str(raw_record["payload_hash"]),
            previous_hash=str(raw_record["previous_hash"]),
            record_hash=str(raw_record["record_hash"]),
            record_id=str(raw_record["record_id"]),
            schema_version=int(raw_record["schema_version"]),
            sequence=int(raw_record["sequence"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise LedgerCorruptionError("Malformed ledger record") from exc


def verify_audit_records(records: Iterable[AuditRecord]) -> LedgerState:
    previous_hash = GENESIS_HASH
    expected_sequence = 1
    for index, record in enumerate(records, start=1):
        previous_hash = _verify_audit_record(
            record,
            expected_sequence=expected_sequence,
            line_number=index,
            previous_hash=previous_hash,
        )
        expected_sequence += 1
    return LedgerState(next_sequence=expected_sequence, last_hash=previous_hash)


def _verify_audit_record(
    record: AuditRecord,
    *,
    expected_sequence: int,
    line_number: int,
    previous_hash: str,
) -> str:
    if record.schema_version != SCHEMA_VERSION:
        raise LedgerCorruptionError(f"Unsupported schema version on line {line_number}")
    if record.sequence != expected_sequence:
        raise LedgerCorruptionError(f"Unexpected sequence on line {line_number}")
    if record.previous_hash != previous_hash:
        raise LedgerCorruptionError(f"Broken hash chain on line {line_number}")

    expected_payload_hash = _sha256(canonical_json(record.payload))
    if record.payload_hash != expected_payload_hash:
        raise LedgerCorruptionError(f"Payload hash mismatch on line {line_number}")

    unsigned = record.to_dict()
    unsigned.pop("record_hash")
    expected_record_hash = _sha256(canonical_json(unsigned))
    if record.record_hash != expected_record_hash:
        raise LedgerCorruptionError(f"Record hash mismatch on line {line_number}")

    return record.record_hash


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
