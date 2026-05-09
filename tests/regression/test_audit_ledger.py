from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from audit.checkpoints import (
    LedgerCheckpointError,
    create_ledger_checkpoint,
    latest_recorded_ledger_checkpoint,
    record_ledger_checkpoint,
    verify_ledger_checkpoint,
    verify_recorded_ledger_checkpoints,
)
from audit.anchors import (
    LedgerAnchorError,
    LedgerAnchorReceipt,
    LocalFileLedgerAnchorStore,
    create_ledger_anchor_receipt,
    create_worm_ledger_anchor_receipt,
    publish_recorded_ledger_checkpoint_anchor,
    verify_ledger_anchor_receipt,
    verify_local_ledger_anchor_receipt,
    verify_recorded_ledger_anchor_receipts,
    verify_worm_ledger_anchor_receipt,
)
from audit.ledger import AuditLedger, LedgerCorruptionError, LedgerLockError
from audit.replay import ReplayEngine
from core.clock import FixedClock
from core.enums import AnchorImmutabilityMode, AnchorStoreType, DigestAlgorithm, EventType
from core.json_tools import canonical_json


class FakeWormLedgerAnchorStore:
    def __init__(self, retention_until: datetime) -> None:
        self._retention_until = retention_until

    def publish(self, recorded_checkpoint, *, clock=None):
        artifact_payload = {
            "recorded_checkpoint": recorded_checkpoint.to_payload(),
            "schema_version": 1,
        }
        artifact_json = canonical_json(artifact_payload)
        artifact_digest = hashlib.sha256(artifact_json.encode("utf-8")).hexdigest()
        checkpoint = recorded_checkpoint.checkpoint
        return create_worm_ledger_anchor_receipt(
            artifact_digest=artifact_digest,
            artifact_uri=f"worm://audit-anchors/{checkpoint.checkpoint_hash}.json",
            checkpoint_hash=checkpoint.checkpoint_hash,
            checkpoint_through_hash=checkpoint.through_hash,
            checkpoint_through_sequence=checkpoint.through_sequence,
            clock=clock,
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            retention_until=self._retention_until,
            store_metadata={
                "object_content_verified": True,
                "object_sha256": artifact_digest,
                "provider": "regression",
            },
            version_id="version-1",
        )


def test_ledger_appends_hash_chained_records_and_replays(workspace_tmp_path):
    ledger = AuditLedger(
        workspace_tmp_path / "audit.jsonl",
        clock=FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc)),
    )

    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    ledger.append(EventType.ACTION_ACCEPTED, {"client_order_id": "order-1"})

    records = ledger.iter_records()
    assert [record.sequence for record in records] == [1, 2]
    assert records[1].previous_hash == records[0].record_hash

    replayed = []
    count = ReplayEngine(ledger).replay(replayed.append)

    assert count == 2
    assert [record.event_type for record in replayed] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
    ]
    assert not ledger.lock_path.exists()


def test_ledger_snapshot_reads_verified_state_and_records_together(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    first = ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    second = ledger.append(EventType.ACTION_ACCEPTED, {"client_order_id": "order-1"})

    snapshot = ledger.snapshot()

    assert snapshot.state.next_sequence == 3
    assert snapshot.state.last_hash == second.record_hash
    assert snapshot.records == (first, second)
    assert not ledger.lock_path.exists()


def test_ledger_rejects_existing_process_lock(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.lock_path.write_text("other-process\n", encoding="utf-8")

    try:
        with pytest.raises(LedgerLockError, match="locked"):
            ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    finally:
        ledger.lock_path.unlink(missing_ok=True)


def test_ledger_detects_payload_tampering(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})

    raw_record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    raw_record["payload"]["client_order_id"] = "order-2"
    path.write_text(json.dumps(raw_record, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(LedgerCorruptionError):
        AuditLedger(path)


def test_ledger_rejects_missing_append_boundary(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    path.write_text(path.read_text(encoding="utf-8").rstrip("\n"), encoding="utf-8")

    with pytest.raises(LedgerCorruptionError, match="append boundary"):
        AuditLedger(path)


def test_ledger_checkpoint_records_verified_head_and_validates(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path, clock=clock)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    last_record = ledger.append(EventType.ACTION_ACCEPTED, {"client_order_id": "order-1"})

    checkpoint = create_ledger_checkpoint(ledger, clock=clock)
    verify_ledger_checkpoint(ledger, checkpoint)
    recorded = record_ledger_checkpoint(path, clock=clock)
    reloaded = AuditLedger(path, clock=clock)
    records = reloaded.iter_records()

    assert checkpoint.digest_algorithm == DigestAlgorithm.SHA256
    assert checkpoint.record_count == 2
    assert checkpoint.through_hash == last_record.record_hash
    assert checkpoint.through_sequence == 2
    assert recorded.audit_record_sequence == 3
    assert records[-1].event_type == EventType.AUDIT_CHECKPOINT
    assert records[-1].payload == recorded.checkpoint.to_payload()
    assert verify_recorded_ledger_checkpoints(reloaded) == 1


def test_latest_recorded_ledger_checkpoint_returns_latest_without_appending(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    first = record_ledger_checkpoint(path)
    ledger.append(EventType.ACTION_ACCEPTED, {"client_order_id": "order-1"})
    second = record_ledger_checkpoint(path)
    before_records = AuditLedger(path).iter_records()

    latest = latest_recorded_ledger_checkpoint(path)
    after_records = AuditLedger(path).iter_records()

    assert latest == second
    assert latest != first
    assert len(after_records) == len(before_records)
    assert after_records[-1].event_type == EventType.AUDIT_CHECKPOINT


def test_latest_recorded_ledger_checkpoint_requires_existing_checkpoint(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    AuditLedger(path).append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})

    with pytest.raises(LedgerCheckpointError, match="no recorded checkpoints"):
        latest_recorded_ledger_checkpoint(path)


def test_ledger_checkpoint_detects_checkpoint_tampering(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    checkpoint = create_ledger_checkpoint(ledger)
    tampered = replace(checkpoint, record_count=2)

    with pytest.raises(LedgerCheckpointError, match="hash mismatch"):
        verify_ledger_checkpoint(ledger, tampered)


def test_local_anchor_store_publishes_checkpoint_artifact_and_audits_receipt(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    store = LocalFileLedgerAnchorStore(workspace_tmp_path / "anchors")

    recorded_anchor = publish_recorded_ledger_checkpoint_anchor(path, recorded_checkpoint, store)
    reloaded = AuditLedger(path)
    records = reloaded.iter_records()

    assert recorded_anchor.audit_record_sequence == 3
    assert recorded_anchor.receipt.store_type == AnchorStoreType.LOCAL_FILE
    assert recorded_anchor.receipt.checkpoint_hash == recorded_checkpoint.checkpoint.checkpoint_hash
    assert Path(recorded_anchor.receipt.artifact_uri).exists()
    assert records[-1].event_type == EventType.AUDIT_ANCHOR_PUBLISHED
    assert records[-1].payload == recorded_anchor.receipt.to_payload()
    assert verify_recorded_ledger_anchor_receipts(reloaded) == 1
    verify_local_ledger_anchor_receipt(recorded_anchor.receipt)


def test_local_anchor_store_rejects_conflicting_existing_artifact(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    store = LocalFileLedgerAnchorStore(workspace_tmp_path / "anchors")
    first_anchor = publish_recorded_ledger_checkpoint_anchor(path, recorded_checkpoint, store)
    Path(first_anchor.receipt.artifact_uri).write_text("{}", encoding="utf-8")

    with pytest.raises(LedgerAnchorError, match="different content"):
        store.publish(recorded_checkpoint)


def test_recorded_local_anchor_verification_detects_artifact_tampering(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    recorded_anchor = publish_recorded_ledger_checkpoint_anchor(
        path,
        recorded_checkpoint,
        LocalFileLedgerAnchorStore(workspace_tmp_path / "anchors"),
    )
    Path(recorded_anchor.receipt.artifact_uri).write_text("tampered\n", encoding="utf-8")

    with pytest.raises(LedgerAnchorError, match="artifact digest"):
        verify_recorded_ledger_anchor_receipts(AuditLedger(path))


def test_local_anchor_verification_detects_artifact_receipt_mismatch(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    first_checkpoint = record_ledger_checkpoint(path)
    ledger.append(EventType.ACTION_ACCEPTED, {"client_order_id": "order-1"})
    second_checkpoint = record_ledger_checkpoint(path)
    artifact_payload = {
        "recorded_checkpoint": second_checkpoint.to_payload(),
        "schema_version": 1,
    }
    artifact_json = json.dumps(artifact_payload, separators=(",", ":"), sort_keys=True)
    artifact_path = workspace_tmp_path / "anchors" / "mismatched.json"
    artifact_path.parent.mkdir()
    artifact_path.write_text(f"{artifact_json}\n", encoding="utf-8")
    receipt = create_ledger_anchor_receipt(
        artifact_digest=hashlib.sha256(artifact_json.encode("utf-8")).hexdigest(),
        artifact_uri=artifact_path.as_posix(),
        checkpoint_hash=first_checkpoint.checkpoint.checkpoint_hash,
        checkpoint_through_hash=first_checkpoint.checkpoint.through_hash,
        checkpoint_through_sequence=first_checkpoint.checkpoint.through_sequence,
        store_type=AnchorStoreType.LOCAL_FILE,
    )

    with pytest.raises(LedgerAnchorError, match="checkpoint hash"):
        verify_local_ledger_anchor_receipt(receipt)


def test_recorded_local_anchor_verification_requires_checkpoint_record_membership(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    fake_recorded_checkpoint = replace(recorded_checkpoint, audit_record_hash="not-the-checkpoint-record")
    artifact_payload = {
        "recorded_checkpoint": fake_recorded_checkpoint.to_payload(),
        "schema_version": 1,
    }
    artifact_json = json.dumps(artifact_payload, separators=(",", ":"), sort_keys=True)
    artifact_path = workspace_tmp_path / "anchors" / "fake-recorded-checkpoint.json"
    artifact_path.parent.mkdir()
    artifact_path.write_text(f"{artifact_json}\n", encoding="utf-8")
    receipt = create_ledger_anchor_receipt(
        artifact_digest=hashlib.sha256(artifact_json.encode("utf-8")).hexdigest(),
        artifact_uri=artifact_path.as_posix(),
        checkpoint_hash=recorded_checkpoint.checkpoint.checkpoint_hash,
        checkpoint_through_hash=recorded_checkpoint.checkpoint.through_hash,
        checkpoint_through_sequence=recorded_checkpoint.checkpoint.through_sequence,
        store_type=AnchorStoreType.LOCAL_FILE,
    )

    verify_local_ledger_anchor_receipt(receipt)
    ledger.append(EventType.AUDIT_ANCHOR_PUBLISHED, receipt.to_payload())

    with pytest.raises(LedgerAnchorError, match="audit hash"):
        verify_recorded_ledger_anchor_receipts(AuditLedger(path))


def test_worm_anchor_store_publishes_retention_evidence_and_audits_receipt(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    retention_until = datetime(2033, 1, 1, tzinfo=timezone.utc)
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path, clock=clock)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path, clock=clock)
    store = FakeWormLedgerAnchorStore(retention_until)

    recorded_anchor = publish_recorded_ledger_checkpoint_anchor(path, recorded_checkpoint, store, clock=clock)
    reloaded = AuditLedger(path, clock=clock)
    records = reloaded.iter_records()

    assert recorded_anchor.receipt.store_type == AnchorStoreType.WORM_OBJECT
    assert recorded_anchor.receipt.immutability_mode == AnchorImmutabilityMode.COMPLIANCE
    assert recorded_anchor.receipt.retention_until == retention_until
    assert recorded_anchor.receipt.store_metadata["object_content_verified"] is True
    assert recorded_anchor.receipt.store_metadata["object_sha256"] == recorded_anchor.receipt.artifact_digest
    assert recorded_anchor.receipt.store_metadata["provider"] == "regression"
    assert recorded_anchor.receipt.version_id == "version-1"
    assert records[-1].event_type == EventType.AUDIT_ANCHOR_PUBLISHED
    assert records[-1].payload == recorded_anchor.receipt.to_payload()
    verify_worm_ledger_anchor_receipt(
        recorded_anchor.receipt,
        minimum_retention_until=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )
    assert verify_recorded_ledger_anchor_receipts(reloaded) == 1


def test_worm_anchor_verification_requires_immutability_and_retention_evidence():
    receipt = create_ledger_anchor_receipt(
        artifact_digest="artifact-digest",
        artifact_uri="worm://audit-anchors/checkpoint.json",
        checkpoint_hash="checkpoint-hash",
        checkpoint_through_hash="through-hash",
        checkpoint_through_sequence=10,
        store_type=AnchorStoreType.WORM_OBJECT,
    )

    with pytest.raises(LedgerAnchorError, match="immutability mode"):
        verify_worm_ledger_anchor_receipt(receipt)


def test_worm_anchor_verification_requires_version_and_content_evidence():
    missing_version = create_worm_ledger_anchor_receipt(
        artifact_digest="artifact-digest",
        artifact_uri="worm://audit-anchors/checkpoint.json",
        checkpoint_hash="checkpoint-hash",
        checkpoint_through_hash="through-hash",
        checkpoint_through_sequence=10,
        immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
        retention_until=datetime(2030, 1, 1, tzinfo=timezone.utc),
        store_metadata={
            "object_content_verified": True,
            "object_sha256": "artifact-digest",
        },
    )
    missing_content = create_worm_ledger_anchor_receipt(
        artifact_digest="artifact-digest",
        artifact_uri="worm://audit-anchors/checkpoint.json",
        checkpoint_hash="checkpoint-hash",
        checkpoint_through_hash="through-hash",
        checkpoint_through_sequence=10,
        immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
        retention_until=datetime(2030, 1, 1, tzinfo=timezone.utc),
        version_id="version-1",
    )
    mismatched_digest = create_worm_ledger_anchor_receipt(
        artifact_digest="artifact-digest",
        artifact_uri="worm://audit-anchors/checkpoint.json",
        checkpoint_hash="checkpoint-hash",
        checkpoint_through_hash="through-hash",
        checkpoint_through_sequence=10,
        immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
        retention_until=datetime(2030, 1, 1, tzinfo=timezone.utc),
        store_metadata={
            "object_content_verified": True,
            "object_sha256": "different-digest",
        },
        version_id="version-1",
    )

    with pytest.raises(LedgerAnchorError, match="version ID"):
        verify_worm_ledger_anchor_receipt(missing_version)

    with pytest.raises(LedgerAnchorError, match="content verification"):
        verify_worm_ledger_anchor_receipt(missing_content)

    with pytest.raises(LedgerAnchorError, match="object digest"):
        verify_worm_ledger_anchor_receipt(mismatched_digest)


def test_worm_anchor_verification_rejects_short_retention():
    receipt = create_worm_ledger_anchor_receipt(
        artifact_digest="artifact-digest",
        artifact_uri="worm://audit-anchors/checkpoint.json",
        checkpoint_hash="checkpoint-hash",
        checkpoint_through_hash="through-hash",
        checkpoint_through_sequence=10,
        immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
        retention_until=datetime(2027, 1, 1, tzinfo=timezone.utc),
        store_metadata={
            "object_content_verified": True,
            "object_sha256": "artifact-digest",
        },
        version_id="version-1",
    )

    with pytest.raises(LedgerAnchorError, match="shorter than required"):
        verify_worm_ledger_anchor_receipt(
            receipt,
            minimum_retention_until=datetime(2028, 1, 1, tzinfo=timezone.utc),
        )


def test_recorded_worm_anchor_verification_requires_checkpoint_record_membership(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    receipt = create_worm_ledger_anchor_receipt(
        artifact_digest="artifact-digest",
        artifact_uri="worm://audit-anchors/checkpoint.json",
        checkpoint_hash="checkpoint-hash",
        checkpoint_through_hash="through-hash",
        checkpoint_through_sequence=10,
        immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
        retention_until=datetime(2030, 1, 1, tzinfo=timezone.utc),
        store_metadata={
            "object_content_verified": True,
            "object_sha256": "artifact-digest",
        },
        version_id="version-1",
    )
    ledger.append(EventType.AUDIT_ANCHOR_PUBLISHED, receipt.to_payload())

    with pytest.raises(LedgerAnchorError, match="checkpoint record"):
        verify_recorded_ledger_anchor_receipts(AuditLedger(path))


def test_legacy_v1_anchor_receipts_remain_hash_verifiable():
    published_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    unsigned_payload = {
        "artifact_digest": "artifact-digest",
        "artifact_uri": "anchors/checkpoint.json",
        "checkpoint_hash": "checkpoint-hash",
        "checkpoint_through_hash": "through-hash",
        "checkpoint_through_sequence": 10,
        "digest_algorithm": DigestAlgorithm.SHA256.value,
        "published_at": published_at.isoformat(),
        "schema_version": 1,
        "store_type": AnchorStoreType.LOCAL_FILE.value,
    }
    payload = dict(unsigned_payload)
    payload["receipt_hash"] = hashlib.sha256(canonical_json(unsigned_payload).encode("utf-8")).hexdigest()

    receipt = LedgerAnchorReceipt.from_payload(payload)

    verify_ledger_anchor_receipt(receipt)
    assert receipt.to_payload() == payload


def test_ledger_rejects_non_enum_event_type(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")

    with pytest.raises(TypeError):
        ledger.append("action.requested", {})
