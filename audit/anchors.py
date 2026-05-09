from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from audit.checkpoints import LedgerCheckpoint, RecordedLedgerCheckpoint, verify_ledger_checkpoint
from audit.ledger import AuditLedger
from core.clock import Clock, SystemClock
from core.enums import AnchorImmutabilityMode, AnchorStoreType, DigestAlgorithm, EventType
from core.errors import AuditIntegrityError
from core.json_tools import JsonValue, canonical_json, normalize_json


ANCHOR_ARTIFACT_SCHEMA_VERSION = 1
ANCHOR_RECEIPT_SCHEMA_VERSION = 2


class LedgerAnchorError(AuditIntegrityError):
    pass


class LedgerAnchorStore(Protocol):
    def publish(
        self,
        recorded_checkpoint: RecordedLedgerCheckpoint,
        *,
        clock: Clock | None = None,
    ) -> "LedgerAnchorReceipt":
        raise NotImplementedError


@dataclass(frozen=True)
class LedgerAnchorArtifact:
    artifact_digest: str
    artifact_json: str
    artifact_name: str


@dataclass(frozen=True)
class LedgerAnchorReceipt:
    artifact_digest: str
    artifact_uri: str
    checkpoint_hash: str
    checkpoint_through_hash: str
    checkpoint_through_sequence: int
    digest_algorithm: DigestAlgorithm
    published_at: datetime
    receipt_hash: str
    store_type: AnchorStoreType
    schema_version: int = ANCHOR_RECEIPT_SCHEMA_VERSION
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
            object.__setattr__(
                self,
                "retention_until",
                _normalize_utc(self.retention_until),
            )

        normalized_metadata = normalize_json(self.store_metadata)
        if not isinstance(normalized_metadata, dict):
            raise TypeError("store_metadata must normalize to a JSON object")
        object.__setattr__(self, "store_metadata", normalized_metadata)

    def to_payload(self) -> dict[str, JsonValue]:
        return _anchor_receipt_payload(
            artifact_digest=self.artifact_digest,
            artifact_uri=self.artifact_uri,
            checkpoint_hash=self.checkpoint_hash,
            checkpoint_through_hash=self.checkpoint_through_hash,
            checkpoint_through_sequence=self.checkpoint_through_sequence,
            digest_algorithm=self.digest_algorithm,
            published_at=self.published_at,
            receipt_hash=self.receipt_hash,
            schema_version=self.schema_version,
            immutability_mode=self.immutability_mode,
            retention_until=self.retention_until,
            store_metadata=self.store_metadata,
            store_type=self.store_type,
            version_id=self.version_id,
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "LedgerAnchorReceipt":
        try:
            schema_version = int(payload.get("schema_version", 1))
            return cls(
                artifact_digest=str(payload["artifact_digest"]),
                artifact_uri=str(payload["artifact_uri"]),
                checkpoint_hash=str(payload["checkpoint_hash"]),
                checkpoint_through_hash=str(payload["checkpoint_through_hash"]),
                checkpoint_through_sequence=int(payload["checkpoint_through_sequence"]),
                digest_algorithm=DigestAlgorithm(payload["digest_algorithm"]),
                published_at=_datetime_from_payload(payload["published_at"]),
                receipt_hash=str(payload["receipt_hash"]),
                schema_version=schema_version,
                immutability_mode=_immutability_mode_from_payload(payload.get("immutability_mode")),
                retention_until=_optional_datetime_from_payload(payload.get("retention_until")),
                store_metadata=_metadata_from_payload(payload.get("store_metadata")),
                store_type=AnchorStoreType(payload["store_type"]),
                version_id=_optional_string_from_payload(payload.get("version_id")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerAnchorError("Malformed ledger anchor receipt") from exc


@dataclass(frozen=True)
class RecordedLedgerAnchor:
    audit_record_hash: str
    audit_record_sequence: int
    receipt: LedgerAnchorReceipt

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "audit_record_hash": self.audit_record_hash,
            "audit_record_sequence": self.audit_record_sequence,
            "receipt": self.receipt.to_payload(),
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Recorded anchor payload must normalize to an object")
        return normalized


class LocalFileLedgerAnchorStore:
    def __init__(self, anchor_dir: str | Path) -> None:
        self._anchor_dir = Path(anchor_dir)

    @property
    def anchor_dir(self) -> Path:
        return self._anchor_dir

    def publish(
        self,
        recorded_checkpoint: RecordedLedgerCheckpoint,
        *,
        clock: Clock | None = None,
    ) -> LedgerAnchorReceipt:
        if not isinstance(recorded_checkpoint, RecordedLedgerCheckpoint):
            raise TypeError("recorded_checkpoint must be a RecordedLedgerCheckpoint")

        artifact = create_ledger_anchor_artifact(recorded_checkpoint)
        self._anchor_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self._anchor_dir / artifact.artifact_name

        if artifact_path.exists():
            existing_json = artifact_path.read_text(encoding="utf-8").strip()
            if existing_json != artifact.artifact_json:
                raise LedgerAnchorError(f"Anchor artifact already exists with different content: {artifact_path}")
        else:
            with artifact_path.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(artifact.artifact_json)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())

        return create_ledger_anchor_receipt(
            artifact_digest=artifact.artifact_digest,
            artifact_uri=artifact_path.as_posix(),
            checkpoint_hash=recorded_checkpoint.checkpoint.checkpoint_hash,
            checkpoint_through_hash=recorded_checkpoint.checkpoint.through_hash,
            checkpoint_through_sequence=recorded_checkpoint.checkpoint.through_sequence,
            clock=clock,
            store_type=AnchorStoreType.LOCAL_FILE,
        )


def create_ledger_anchor_artifact(recorded_checkpoint: RecordedLedgerCheckpoint) -> LedgerAnchorArtifact:
    if not isinstance(recorded_checkpoint, RecordedLedgerCheckpoint):
        raise TypeError("recorded_checkpoint must be a RecordedLedgerCheckpoint")
    artifact_json = canonical_json(_anchor_artifact_payload(recorded_checkpoint))
    return LedgerAnchorArtifact(
        artifact_digest=_sha256(artifact_json),
        artifact_json=artifact_json,
        artifact_name=_artifact_name(recorded_checkpoint),
    )


def create_ledger_anchor_receipt(
    *,
    artifact_digest: str,
    artifact_uri: str,
    checkpoint_hash: str,
    checkpoint_through_hash: str,
    checkpoint_through_sequence: int,
    clock: Clock | None = None,
    immutability_mode: AnchorImmutabilityMode | None = None,
    retention_until: datetime | None = None,
    store_metadata: Mapping[str, Any] | None = None,
    store_type: AnchorStoreType,
    version_id: str | None = None,
) -> LedgerAnchorReceipt:
    published_at = (clock or SystemClock()).now().astimezone(timezone.utc)
    normalized_metadata = _metadata_from_payload(store_metadata)
    unsigned_payload = _anchor_receipt_payload(
        artifact_digest=artifact_digest,
        artifact_uri=artifact_uri,
        checkpoint_hash=checkpoint_hash,
        checkpoint_through_hash=checkpoint_through_hash,
        checkpoint_through_sequence=checkpoint_through_sequence,
        digest_algorithm=DigestAlgorithm.SHA256,
        published_at=published_at,
        receipt_hash=None,
        schema_version=ANCHOR_RECEIPT_SCHEMA_VERSION,
        immutability_mode=immutability_mode,
        retention_until=retention_until,
        store_metadata=normalized_metadata,
        store_type=store_type,
        version_id=version_id,
    )
    unsigned_payload.pop("receipt_hash")
    receipt_hash = _sha256(canonical_json(unsigned_payload))
    return LedgerAnchorReceipt(
        artifact_digest=artifact_digest,
        artifact_uri=artifact_uri,
        checkpoint_hash=checkpoint_hash,
        checkpoint_through_hash=checkpoint_through_hash,
        checkpoint_through_sequence=checkpoint_through_sequence,
        digest_algorithm=DigestAlgorithm.SHA256,
        published_at=published_at,
        receipt_hash=receipt_hash,
        immutability_mode=immutability_mode,
        retention_until=retention_until,
        store_metadata=normalized_metadata,
        store_type=store_type,
        version_id=version_id,
    )


def create_worm_ledger_anchor_receipt(
    *,
    artifact_digest: str,
    artifact_uri: str,
    checkpoint_hash: str,
    checkpoint_through_hash: str,
    checkpoint_through_sequence: int,
    immutability_mode: AnchorImmutabilityMode,
    retention_until: datetime,
    clock: Clock | None = None,
    store_metadata: Mapping[str, Any] | None = None,
    version_id: str | None = None,
) -> LedgerAnchorReceipt:
    return create_ledger_anchor_receipt(
        artifact_digest=artifact_digest,
        artifact_uri=artifact_uri,
        checkpoint_hash=checkpoint_hash,
        checkpoint_through_hash=checkpoint_through_hash,
        checkpoint_through_sequence=checkpoint_through_sequence,
        clock=clock,
        immutability_mode=immutability_mode,
        retention_until=retention_until,
        store_metadata=store_metadata,
        store_type=AnchorStoreType.WORM_OBJECT,
        version_id=version_id,
    )


def publish_recorded_ledger_checkpoint_anchor(
    path: str | Path,
    recorded_checkpoint: RecordedLedgerCheckpoint,
    store: LedgerAnchorStore,
    *,
    clock: Clock | None = None,
) -> RecordedLedgerAnchor:
    ledger_path = Path(path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger does not exist: {ledger_path}")

    ledger = AuditLedger(ledger_path, clock=clock)
    _assert_recorded_checkpoint_in_ledger(ledger, recorded_checkpoint)
    verify_ledger_checkpoint(ledger, recorded_checkpoint.checkpoint)
    receipt = store.publish(recorded_checkpoint, clock=clock)
    record = ledger.append(EventType.AUDIT_ANCHOR_PUBLISHED, receipt.to_payload())
    return RecordedLedgerAnchor(
        audit_record_hash=record.record_hash,
        audit_record_sequence=record.sequence,
        receipt=receipt,
    )


def verify_ledger_anchor_receipt(receipt: LedgerAnchorReceipt) -> None:
    unsigned_payload = receipt.to_payload()
    unsigned_payload.pop("receipt_hash")
    expected = _sha256(canonical_json(unsigned_payload))
    if receipt.receipt_hash != expected:
        raise LedgerAnchorError("Ledger anchor receipt hash mismatch")


def verify_local_ledger_anchor_receipt(
    receipt: LedgerAnchorReceipt,
    *,
    ledger: AuditLedger | None = None,
) -> None:
    verify_ledger_anchor_receipt(receipt)
    if receipt.store_type != AnchorStoreType.LOCAL_FILE:
        raise LedgerAnchorError("Ledger anchor receipt is not a local file receipt")

    artifact_path = Path(receipt.artifact_uri)
    if not artifact_path.exists():
        raise LedgerAnchorError(f"Ledger anchor artifact does not exist: {artifact_path}")
    artifact_json = artifact_path.read_text(encoding="utf-8").strip()
    if receipt.artifact_digest != _sha256(artifact_json):
        raise LedgerAnchorError("Ledger anchor artifact digest mismatch")
    recorded_checkpoint = _recorded_checkpoint_from_artifact_json(artifact_json)
    checkpoint = recorded_checkpoint.checkpoint
    if checkpoint.checkpoint_hash != receipt.checkpoint_hash:
        raise LedgerAnchorError("Ledger anchor artifact checkpoint hash does not match receipt")
    if checkpoint.through_hash != receipt.checkpoint_through_hash:
        raise LedgerAnchorError("Ledger anchor artifact through hash does not match receipt")
    if checkpoint.through_sequence != receipt.checkpoint_through_sequence:
        raise LedgerAnchorError("Ledger anchor artifact through sequence does not match receipt")
    if ledger is not None:
        _assert_recorded_checkpoint_in_ledger(ledger, recorded_checkpoint)
        verify_ledger_checkpoint(ledger, checkpoint)


def verify_worm_ledger_anchor_receipt(
    receipt: LedgerAnchorReceipt,
    *,
    minimum_retention_until: datetime | None = None,
) -> None:
    verify_ledger_anchor_receipt(receipt)
    if receipt.store_type != AnchorStoreType.WORM_OBJECT:
        raise LedgerAnchorError("Ledger anchor receipt is not a WORM object receipt")
    if receipt.immutability_mode is None:
        raise LedgerAnchorError("WORM ledger anchor receipt is missing immutability mode")
    if receipt.retention_until is None:
        raise LedgerAnchorError("WORM ledger anchor receipt is missing retention timestamp")
    if receipt.version_id is None:
        raise LedgerAnchorError("WORM ledger anchor receipt is missing object version ID")
    if receipt.store_metadata.get("object_content_verified") is not True:
        raise LedgerAnchorError("WORM ledger anchor receipt is missing object content verification")
    if receipt.store_metadata.get("object_sha256") != receipt.artifact_digest:
        raise LedgerAnchorError("WORM ledger anchor receipt object digest does not match artifact digest")
    if minimum_retention_until is not None:
        minimum_retention = _normalize_utc(minimum_retention_until)
        if receipt.retention_until < minimum_retention:
            raise LedgerAnchorError("WORM ledger anchor retention is shorter than required")


def verify_recorded_ledger_anchor_receipts(ledger: AuditLedger) -> int:
    verified = 0
    for record in ledger.iter_records():
        if record.event_type != EventType.AUDIT_ANCHOR_PUBLISHED:
            continue
        payload = record.payload
        if not isinstance(payload, dict):
            raise LedgerAnchorError("Malformed ledger anchor receipt payload")
        receipt = LedgerAnchorReceipt.from_payload(payload)
        _assert_anchor_checkpoint_in_ledger(
            ledger,
            receipt,
            anchor_record_sequence=record.sequence,
        )
        if receipt.store_type == AnchorStoreType.LOCAL_FILE:
            verify_local_ledger_anchor_receipt(receipt, ledger=ledger)
        elif receipt.store_type == AnchorStoreType.WORM_OBJECT:
            verify_worm_ledger_anchor_receipt(receipt)
        else:
            verify_ledger_anchor_receipt(receipt)
        verified += 1
    return verified


def _assert_recorded_checkpoint_in_ledger(
    ledger: AuditLedger,
    recorded_checkpoint: RecordedLedgerCheckpoint,
) -> None:
    for record in ledger.iter_records():
        if record.sequence != recorded_checkpoint.audit_record_sequence:
            continue
        if record.record_hash != recorded_checkpoint.audit_record_hash:
            raise LedgerAnchorError("Recorded checkpoint audit hash does not match ledger")
        if record.event_type != EventType.AUDIT_CHECKPOINT:
            raise LedgerAnchorError("Recorded checkpoint audit event type does not match ledger")
        if record.payload != recorded_checkpoint.checkpoint.to_payload():
            raise LedgerAnchorError("Recorded checkpoint payload does not match ledger")
        return
    raise LedgerAnchorError("Recorded checkpoint audit record is missing from ledger")


def _assert_anchor_checkpoint_in_ledger(
    ledger: AuditLedger,
    receipt: LedgerAnchorReceipt,
    *,
    anchor_record_sequence: int,
) -> None:
    for record in ledger.iter_records():
        if record.event_type != EventType.AUDIT_CHECKPOINT:
            continue
        if record.sequence >= anchor_record_sequence:
            raise LedgerAnchorError("Ledger anchor cannot reference its own or future checkpoint record")
        payload = record.payload
        if not isinstance(payload, dict):
            raise LedgerAnchorError("Malformed ledger checkpoint payload while verifying anchor")
        try:
            checkpoint = LedgerCheckpoint.from_payload(payload)
        except Exception as exc:
            raise LedgerAnchorError("Malformed ledger checkpoint while verifying anchor") from exc
        if checkpoint.checkpoint_hash != receipt.checkpoint_hash:
            continue
        if checkpoint.through_hash != receipt.checkpoint_through_hash:
            raise LedgerAnchorError("Ledger anchor checkpoint through hash does not match ledger")
        if checkpoint.through_sequence != receipt.checkpoint_through_sequence:
            raise LedgerAnchorError("Ledger anchor checkpoint through sequence does not match ledger")
        return
    raise LedgerAnchorError("Ledger anchor checkpoint record is missing from ledger")


def _anchor_artifact_payload(recorded_checkpoint: RecordedLedgerCheckpoint) -> dict[str, JsonValue]:
    payload = {
        "recorded_checkpoint": recorded_checkpoint.to_payload(),
        "schema_version": ANCHOR_ARTIFACT_SCHEMA_VERSION,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("Anchor artifact payload must normalize to an object")
    return normalized


def _recorded_checkpoint_from_artifact_json(artifact_json: str) -> RecordedLedgerCheckpoint:
    try:
        payload = json.loads(artifact_json)
    except json.JSONDecodeError as exc:
        raise LedgerAnchorError("Ledger anchor artifact is not valid JSON") from exc

    if not isinstance(payload, dict) or payload.get("schema_version") != ANCHOR_ARTIFACT_SCHEMA_VERSION:
        raise LedgerAnchorError("Ledger anchor artifact schema version is invalid")
    recorded_checkpoint_payload = payload.get("recorded_checkpoint")
    if not isinstance(recorded_checkpoint_payload, dict):
        raise LedgerAnchorError("Ledger anchor artifact missing recorded checkpoint")
    try:
        return RecordedLedgerCheckpoint.from_payload(recorded_checkpoint_payload)
    except Exception as exc:
        raise LedgerAnchorError("Ledger anchor artifact has malformed recorded checkpoint") from exc


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


def _anchor_receipt_payload(
    *,
    artifact_digest: str,
    artifact_uri: str,
    checkpoint_hash: str,
    checkpoint_through_hash: str,
    checkpoint_through_sequence: int,
    digest_algorithm: DigestAlgorithm,
    published_at: datetime,
    receipt_hash: str | None,
    schema_version: int,
    immutability_mode: AnchorImmutabilityMode | None,
    retention_until: datetime | None,
    store_metadata: Mapping[str, Any] | None,
    store_type: AnchorStoreType,
    version_id: str | None,
) -> dict[str, JsonValue]:
    payload = {
        "artifact_digest": artifact_digest,
        "artifact_uri": artifact_uri,
        "checkpoint_hash": checkpoint_hash,
        "checkpoint_through_hash": checkpoint_through_hash,
        "checkpoint_through_sequence": checkpoint_through_sequence,
        "digest_algorithm": digest_algorithm,
        "published_at": published_at,
        "receipt_hash": receipt_hash,
        "schema_version": schema_version,
        "store_type": store_type,
    }
    if schema_version >= 2:
        payload.update(
            {
                "immutability_mode": immutability_mode,
                "retention_until": retention_until,
                "store_metadata": store_metadata or {},
                "version_id": version_id,
            }
        )
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("Anchor receipt payload must normalize to an object")
    return normalized


def _artifact_name(recorded_checkpoint: RecordedLedgerCheckpoint) -> str:
    checkpoint = recorded_checkpoint.checkpoint
    return f"checkpoint-{checkpoint.through_sequence:020d}-{checkpoint.checkpoint_hash}.json"


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
