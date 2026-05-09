from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from audit.ledger import (
    AuditLedger,
    AuditRecord,
    GENESIS_HASH,
    LedgerCorruptionError,
    audit_record_from_dict,
    verify_audit_records,
)
from core.clock import Clock, SystemClock
from core.enums import AnchorImmutabilityMode, AnchorStoreType, DigestAlgorithm, EventType
from core.errors import AuditIntegrityError
from core.json_tools import JsonValue, canonical_json, normalize_json


ARCHIVE_RECEIPT_SCHEMA_VERSION = 1


class LedgerArchiveError(AuditIntegrityError):
    pass


@dataclass(frozen=True)
class LedgerArchiveArtifact:
    artifact_digest: str
    artifact_jsonl: str
    artifact_name: str
    record_count: int
    through_hash: str
    through_sequence: int


@dataclass(frozen=True)
class LedgerArchiveReceipt:
    artifact_digest: str
    artifact_uri: str
    digest_algorithm: DigestAlgorithm
    published_at: datetime
    receipt_hash: str
    record_count: int
    store_type: AnchorStoreType
    through_hash: str
    through_sequence: int
    schema_version: int = ARCHIVE_RECEIPT_SCHEMA_VERSION
    immutability_mode: AnchorImmutabilityMode | None = None
    retention_until: datetime | None = None
    store_metadata: dict[str, JsonValue] = field(default_factory=dict)
    version_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.digest_algorithm, DigestAlgorithm):
            raise TypeError("digest_algorithm must be a DigestAlgorithm")
        if not isinstance(self.store_type, AnchorStoreType):
            raise TypeError("store_type must be an AnchorStoreType")
        if self.immutability_mode is not None and not isinstance(
            self.immutability_mode,
            AnchorImmutabilityMode,
        ):
            raise TypeError("immutability_mode must be an AnchorImmutabilityMode")
        if self.retention_until is not None:
            object.__setattr__(self, "retention_until", _normalize_utc(self.retention_until))
        normalized_metadata = normalize_json(self.store_metadata)
        if not isinstance(normalized_metadata, dict):
            raise TypeError("store_metadata must normalize to a JSON object")
        object.__setattr__(self, "store_metadata", normalized_metadata)

    def to_payload(self) -> dict[str, JsonValue]:
        return _archive_receipt_payload(
            artifact_digest=self.artifact_digest,
            artifact_uri=self.artifact_uri,
            digest_algorithm=self.digest_algorithm,
            immutability_mode=self.immutability_mode,
            published_at=self.published_at,
            receipt_hash=self.receipt_hash,
            record_count=self.record_count,
            retention_until=self.retention_until,
            schema_version=self.schema_version,
            store_metadata=self.store_metadata,
            store_type=self.store_type,
            through_hash=self.through_hash,
            through_sequence=self.through_sequence,
            version_id=self.version_id,
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "LedgerArchiveReceipt":
        try:
            return cls(
                artifact_digest=str(payload["artifact_digest"]),
                artifact_uri=str(payload["artifact_uri"]),
                digest_algorithm=DigestAlgorithm(payload["digest_algorithm"]),
                immutability_mode=_immutability_mode_from_payload(payload.get("immutability_mode")),
                published_at=_datetime_from_payload(payload["published_at"]),
                receipt_hash=str(payload["receipt_hash"]),
                record_count=int(payload["record_count"]),
                retention_until=_optional_datetime_from_payload(payload.get("retention_until")),
                schema_version=int(payload.get("schema_version", ARCHIVE_RECEIPT_SCHEMA_VERSION)),
                store_metadata=_metadata_from_payload(payload.get("store_metadata")),
                store_type=AnchorStoreType(payload["store_type"]),
                through_hash=str(payload["through_hash"]),
                through_sequence=int(payload["through_sequence"]),
                version_id=_optional_string_from_payload(payload.get("version_id")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerArchiveError("Malformed ledger archive receipt") from exc


@dataclass(frozen=True)
class RecordedLedgerArchive:
    audit_record_hash: str
    audit_record_sequence: int
    receipt: LedgerArchiveReceipt

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "audit_record_hash": self.audit_record_hash,
            "audit_record_sequence": self.audit_record_sequence,
            "receipt": self.receipt.to_payload(),
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Recorded archive payload must normalize to an object")
        return normalized


class LedgerArchiveStore(Protocol):
    def publish(
        self,
        artifact: LedgerArchiveArtifact,
        *,
        clock: Clock | None = None,
    ) -> LedgerArchiveReceipt:
        raise NotImplementedError


def create_ledger_archive_artifact(ledger: AuditLedger) -> LedgerArchiveArtifact:
    if not isinstance(ledger, AuditLedger):
        raise TypeError("ledger must be an AuditLedger")
    snapshot = ledger.snapshot()
    return _archive_artifact_from_records(snapshot.records)


def create_ledger_archive_receipt(
    *,
    artifact_digest: str,
    artifact_uri: str,
    record_count: int,
    store_type: AnchorStoreType,
    through_hash: str,
    through_sequence: int,
    clock: Clock | None = None,
    immutability_mode: AnchorImmutabilityMode | None = None,
    retention_until: datetime | None = None,
    store_metadata: Mapping[str, Any] | None = None,
    version_id: str | None = None,
) -> LedgerArchiveReceipt:
    published_at = (clock or SystemClock()).now().astimezone(timezone.utc)
    normalized_metadata = _metadata_from_payload(store_metadata)
    unsigned_payload = _archive_receipt_payload(
        artifact_digest=artifact_digest,
        artifact_uri=artifact_uri,
        digest_algorithm=DigestAlgorithm.SHA256,
        immutability_mode=immutability_mode,
        published_at=published_at,
        receipt_hash=None,
        record_count=record_count,
        retention_until=retention_until,
        schema_version=ARCHIVE_RECEIPT_SCHEMA_VERSION,
        store_metadata=normalized_metadata,
        store_type=store_type,
        through_hash=through_hash,
        through_sequence=through_sequence,
        version_id=version_id,
    )
    unsigned_payload.pop("receipt_hash")
    receipt_hash = _sha256(canonical_json(unsigned_payload))
    return LedgerArchiveReceipt(
        artifact_digest=artifact_digest,
        artifact_uri=artifact_uri,
        digest_algorithm=DigestAlgorithm.SHA256,
        immutability_mode=immutability_mode,
        published_at=published_at,
        receipt_hash=receipt_hash,
        record_count=record_count,
        retention_until=retention_until,
        store_metadata=normalized_metadata,
        store_type=store_type,
        through_hash=through_hash,
        through_sequence=through_sequence,
        version_id=version_id,
    )


def create_worm_ledger_archive_receipt(
    *,
    artifact_digest: str,
    artifact_uri: str,
    record_count: int,
    through_hash: str,
    through_sequence: int,
    immutability_mode: AnchorImmutabilityMode,
    retention_until: datetime,
    clock: Clock | None = None,
    store_metadata: Mapping[str, Any] | None = None,
    version_id: str | None = None,
) -> LedgerArchiveReceipt:
    return create_ledger_archive_receipt(
        artifact_digest=artifact_digest,
        artifact_uri=artifact_uri,
        clock=clock,
        immutability_mode=immutability_mode,
        record_count=record_count,
        retention_until=retention_until,
        store_metadata=store_metadata,
        store_type=AnchorStoreType.WORM_OBJECT,
        through_hash=through_hash,
        through_sequence=through_sequence,
        version_id=version_id,
    )


def publish_ledger_archive(
    path: str | Path,
    store: LedgerArchiveStore,
    *,
    clock: Clock | None = None,
) -> RecordedLedgerArchive:
    ledger_path = Path(path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger does not exist: {ledger_path}")
    ledger = AuditLedger(ledger_path, clock=clock)
    artifact = create_ledger_archive_artifact(ledger)
    receipt = store.publish(artifact, clock=clock)
    _assert_receipt_matches_artifact(receipt, artifact)
    record = ledger.append(EventType.AUDIT_LEDGER_ARCHIVED, receipt.to_payload())
    return RecordedLedgerArchive(
        audit_record_hash=record.record_hash,
        audit_record_sequence=record.sequence,
        receipt=receipt,
    )


def verify_ledger_archive_receipt(receipt: LedgerArchiveReceipt) -> None:
    unsigned_payload = receipt.to_payload()
    unsigned_payload.pop("receipt_hash")
    expected = _sha256(canonical_json(unsigned_payload))
    if receipt.receipt_hash != expected:
        raise LedgerArchiveError("Ledger archive receipt hash mismatch")


def verify_worm_ledger_archive_receipt(receipt: LedgerArchiveReceipt) -> None:
    verify_ledger_archive_receipt(receipt)
    if receipt.store_type != AnchorStoreType.WORM_OBJECT:
        raise LedgerArchiveError("Ledger archive receipt is not a WORM object receipt")
    if receipt.immutability_mode is None:
        raise LedgerArchiveError("WORM ledger archive receipt is missing immutability mode")
    if receipt.retention_until is None:
        raise LedgerArchiveError("WORM ledger archive receipt is missing retention timestamp")
    if receipt.version_id is None:
        raise LedgerArchiveError("WORM ledger archive receipt is missing object version ID")
    if receipt.store_metadata.get("object_content_verified") is not True:
        raise LedgerArchiveError("WORM ledger archive receipt is missing object content verification")
    if receipt.store_metadata.get("object_sha256") != receipt.artifact_digest:
        raise LedgerArchiveError("WORM ledger archive receipt object digest does not match artifact digest")


def verify_ledger_archive_artifact(
    artifact_jsonl: str | bytes,
    *,
    expected_digest: str | None = None,
    expected_record_count: int | None = None,
    expected_through_hash: str | None = None,
    expected_through_sequence: int | None = None,
) -> LedgerArchiveArtifact:
    if isinstance(artifact_jsonl, bytes):
        try:
            artifact_text = artifact_jsonl.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise LedgerArchiveError("Ledger archive artifact is not valid UTF-8") from exc
    elif isinstance(artifact_jsonl, str):
        artifact_text = artifact_jsonl
    else:
        raise TypeError("artifact_jsonl must be a string or bytes")

    if expected_digest is not None and _sha256(artifact_text) != expected_digest:
        raise LedgerArchiveError("Ledger archive artifact digest mismatch")

    records = _archive_records_from_jsonl(artifact_text)
    try:
        verify_audit_records(records)
    except LedgerCorruptionError as exc:
        raise LedgerArchiveError(f"Ledger archive artifact is not replayable: {exc}") from exc

    artifact = _archive_artifact_from_records(records)
    _assert_archive_artifact_expectations(
        artifact,
        expected_digest=expected_digest,
        expected_record_count=expected_record_count,
        expected_through_hash=expected_through_hash,
        expected_through_sequence=expected_through_sequence,
    )
    return artifact


def verify_ledger_archive_artifact_for_receipt(
    artifact_jsonl: str | bytes,
    receipt: LedgerArchiveReceipt,
) -> LedgerArchiveArtifact:
    verify_ledger_archive_receipt(receipt)
    return verify_ledger_archive_artifact(
        artifact_jsonl,
        expected_digest=receipt.artifact_digest,
        expected_record_count=receipt.record_count,
        expected_through_hash=receipt.through_hash,
        expected_through_sequence=receipt.through_sequence,
    )


def verify_recorded_ledger_archive_receipts(ledger: AuditLedger) -> int:
    verified = 0
    records = ledger.iter_records()
    for record in records:
        if record.event_type != EventType.AUDIT_LEDGER_ARCHIVED:
            continue
        payload = record.payload
        if not isinstance(payload, dict):
            raise LedgerArchiveError("Malformed ledger archive receipt payload")
        receipt = LedgerArchiveReceipt.from_payload(payload)
        _assert_receipt_prefix_matches_ledger(receipt, records, archive_record_sequence=record.sequence)
        if receipt.store_type == AnchorStoreType.WORM_OBJECT:
            verify_worm_ledger_archive_receipt(receipt)
        else:
            verify_ledger_archive_receipt(receipt)
        verified += 1
    return verified


def _archive_artifact_from_records(records: Iterable[AuditRecord]) -> LedgerArchiveArtifact:
    archive_records = tuple(records)
    through_sequence = archive_records[-1].sequence if archive_records else 0
    through_hash = archive_records[-1].record_hash if archive_records else GENESIS_HASH
    artifact_jsonl = "".join(f"{canonical_json(record.to_dict())}\n" for record in archive_records)
    artifact_digest = _sha256(artifact_jsonl)
    return LedgerArchiveArtifact(
        artifact_digest=artifact_digest,
        artifact_jsonl=artifact_jsonl,
        artifact_name=f"ledger-through-{through_sequence:020d}-{through_hash}.jsonl",
        record_count=len(archive_records),
        through_hash=through_hash,
        through_sequence=through_sequence,
    )


def _archive_records_from_jsonl(artifact_jsonl: str) -> tuple[AuditRecord, ...]:
    if artifact_jsonl and not artifact_jsonl.endswith("\n"):
        raise LedgerArchiveError("Ledger archive artifact does not end with an append boundary newline")

    records: list[AuditRecord] = []
    for line_number, line in enumerate(artifact_jsonl.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            raise LedgerArchiveError(f"Blank ledger archive line at {line_number}")
        try:
            raw_record = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise LedgerArchiveError(f"Invalid ledger archive JSON on line {line_number}") from exc
        if not isinstance(raw_record, Mapping):
            raise LedgerArchiveError(f"Ledger archive record on line {line_number} is not an object")
        try:
            records.append(audit_record_from_dict(raw_record))
        except LedgerCorruptionError as exc:
            raise LedgerArchiveError(f"Malformed ledger archive record on line {line_number}") from exc
    return tuple(records)


def _assert_receipt_matches_artifact(
    receipt: LedgerArchiveReceipt,
    artifact: LedgerArchiveArtifact,
) -> None:
    if receipt.artifact_digest != artifact.artifact_digest:
        raise LedgerArchiveError("Ledger archive receipt artifact digest does not match uploaded artifact")
    if receipt.record_count != artifact.record_count:
        raise LedgerArchiveError("Ledger archive receipt record count does not match uploaded artifact")
    if receipt.through_hash != artifact.through_hash:
        raise LedgerArchiveError("Ledger archive receipt through hash does not match uploaded artifact")
    if receipt.through_sequence != artifact.through_sequence:
        raise LedgerArchiveError("Ledger archive receipt through sequence does not match uploaded artifact")


def _assert_archive_artifact_expectations(
    artifact: LedgerArchiveArtifact,
    *,
    expected_digest: str | None,
    expected_record_count: int | None,
    expected_through_hash: str | None,
    expected_through_sequence: int | None,
) -> None:
    if expected_digest is not None and artifact.artifact_digest != expected_digest:
        raise LedgerArchiveError("Ledger archive artifact digest mismatch")
    if expected_record_count is not None and artifact.record_count != expected_record_count:
        raise LedgerArchiveError("Ledger archive artifact record count mismatch")
    if expected_through_hash is not None and artifact.through_hash != expected_through_hash:
        raise LedgerArchiveError("Ledger archive artifact through hash mismatch")
    if expected_through_sequence is not None and artifact.through_sequence != expected_through_sequence:
        raise LedgerArchiveError("Ledger archive artifact through sequence mismatch")


def _assert_receipt_prefix_matches_ledger(
    receipt: LedgerArchiveReceipt,
    records: tuple[AuditRecord, ...],
    *,
    archive_record_sequence: int,
) -> None:
    if receipt.through_sequence >= archive_record_sequence:
        raise LedgerArchiveError("Ledger archive cannot reference its own or future archive record")
    prefix_records = tuple(record for record in records if record.sequence <= receipt.through_sequence)
    artifact = _archive_artifact_from_records(prefix_records)
    _assert_receipt_matches_artifact(receipt, artifact)


def _archive_receipt_payload(
    *,
    artifact_digest: str,
    artifact_uri: str,
    digest_algorithm: DigestAlgorithm,
    immutability_mode: AnchorImmutabilityMode | None,
    published_at: datetime,
    receipt_hash: str | None,
    record_count: int,
    retention_until: datetime | None,
    schema_version: int,
    store_metadata: Mapping[str, Any] | None,
    store_type: AnchorStoreType,
    through_hash: str,
    through_sequence: int,
    version_id: str | None,
) -> dict[str, JsonValue]:
    payload = {
        "artifact_digest": artifact_digest,
        "artifact_uri": artifact_uri,
        "digest_algorithm": digest_algorithm,
        "immutability_mode": immutability_mode,
        "published_at": published_at,
        "receipt_hash": receipt_hash,
        "record_count": record_count,
        "retention_until": retention_until,
        "schema_version": schema_version,
        "store_metadata": store_metadata or {},
        "store_type": store_type,
        "through_hash": through_hash,
        "through_sequence": through_sequence,
        "version_id": version_id,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("Archive receipt payload must normalize to an object")
    return normalized


def _datetime_from_payload(value: Any) -> datetime:
    return _normalize_utc(datetime.fromisoformat(str(value)))


def _optional_datetime_from_payload(value: Any) -> datetime | None:
    if value is None:
        return None
    return _datetime_from_payload(value)


def _immutability_mode_from_payload(value: Any) -> AnchorImmutabilityMode | None:
    if value is None:
        return None
    return AnchorImmutabilityMode(value)


def _metadata_from_payload(value: Any) -> dict[str, JsonValue]:
    if value is None:
        return {}
    normalized = normalize_json(value)
    if not isinstance(normalized, dict):
        raise TypeError("store_metadata must normalize to a JSON object")
    return normalized


def _normalize_utc(value: datetime) -> datetime:
    timestamp = value
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _optional_string_from_payload(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
