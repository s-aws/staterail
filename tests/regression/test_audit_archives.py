from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import pytest

from app.ledger_view import load_verified_ledger_view
from audit.archives import (
    LedgerArchiveError,
    create_ledger_archive_artifact,
    create_worm_ledger_archive_receipt,
    publish_ledger_archive,
    verify_ledger_archive_artifact,
    verify_ledger_archive_artifact_for_receipt,
    verify_recorded_ledger_archive_receipts,
    verify_worm_ledger_archive_receipt,
)
from audit.ledger import AuditLedger
from audit.s3_object_lock import (
    S3ObjectLockLedgerArchiveConfig,
    S3ObjectLockLedgerArchiveStore,
    verify_s3_object_lock_ledger_archive_receipt,
)
from core.clock import FixedClock
from core.enums import AnchorImmutabilityMode, AnchorStoreType, EventType, LedgerAnchorStoreProvider
from projections.state import SourceOfTruthProjection


class FakeS3ObjectLockArchiveClient:
    def __init__(
        self,
        *,
        object_body_response: bytes | None = None,
        retention_response: dict[str, Any] | None = None,
    ) -> None:
        self.get_bucket_versioning_calls: list[dict[str, Any]] = []
        self.get_object_calls: list[dict[str, Any]] = []
        self.get_object_lock_configuration_calls: list[dict[str, Any]] = []
        self.get_object_retention_calls: list[dict[str, Any]] = []
        self.put_object_calls: list[dict[str, Any]] = []
        self._object_body_response = object_body_response
        self._retention_response = retention_response
        self._uploaded_body: bytes | None = None
        self._uploaded_retention: dict[str, Any] | None = None

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        self.get_bucket_versioning_calls.append(kwargs)
        return {"Status": "Enabled"}

    def get_object_lock_configuration(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_lock_configuration_calls.append(kwargs)
        return {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_object_calls.append(kwargs)
        self._uploaded_body = kwargs["Body"]
        self._uploaded_retention = {
            "Mode": kwargs["ObjectLockMode"],
            "RetainUntilDate": kwargs["ObjectLockRetainUntilDate"],
        }
        return {"ETag": '"archive-etag"', "VersionId": "archive-version-1"}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_calls.append(kwargs)
        body = self._object_body_response if self._object_body_response is not None else self._uploaded_body
        return {"Body": BytesIO(body or b"")}

    def get_object_retention(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_retention_calls.append(kwargs)
        return self._retention_response or {"Retention": self._uploaded_retention}


def test_s3_object_lock_archive_store_uploads_verified_ledger_records_and_audits_receipt(
    workspace_tmp_path,
):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path, clock=clock)
    ledger.append(EventType.SYSTEM_STARTED, {"component": "test"})
    ledger.append(EventType.SYSTEM_STOPPED, {"component": "test", "completed_cycles": 0})
    artifact = create_ledger_archive_artifact(ledger)
    client = FakeS3ObjectLockArchiveClient()
    store = S3ObjectLockLedgerArchiveStore(
        S3ObjectLockLedgerArchiveConfig(
            bucket="audit-bucket",
            expected_bucket_owner="123456789012",
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            key_prefix="/staterail/ledger-archives/",
            retention_period=timedelta(days=7),
        ),
        s3_client=client,
    )

    recorded_archive = publish_ledger_archive(path, store, clock=clock)
    replayed_records = AuditLedger(path, clock=clock).iter_records()
    projection = SourceOfTruthProjection.from_records(replayed_records)
    view = load_verified_ledger_view(path)

    put_call = client.put_object_calls[0]
    body = put_call["Body"]
    assert body == artifact.artifact_jsonl.encode("utf-8")
    assert put_call["Bucket"] == "audit-bucket"
    assert put_call["ContentMD5"] == base64.b64encode(hashlib.md5(body).digest()).decode("ascii")
    assert put_call["ContentType"] == "application/x-ndjson"
    assert put_call["ExpectedBucketOwner"] == "123456789012"
    assert put_call["Key"] == f"staterail/ledger-archives/{artifact.artifact_name}"
    assert put_call["Metadata"]["audit-artifact-type"] == "ledger-archive"
    assert put_call["Metadata"]["audit-record-count"] == "2"
    assert put_call["ObjectLockMode"] == "COMPLIANCE"
    assert client.get_object_calls == [
        {
            "Bucket": "audit-bucket",
            "ExpectedBucketOwner": "123456789012",
            "Key": f"staterail/ledger-archives/{artifact.artifact_name}",
            "VersionId": "archive-version-1",
        }
    ]

    receipt = recorded_archive.receipt
    assert recorded_archive.audit_record_sequence == 3
    assert replayed_records[-1].event_type == EventType.AUDIT_LEDGER_ARCHIVED
    assert receipt.artifact_digest == artifact.artifact_digest
    assert receipt.artifact_uri == f"s3://audit-bucket/staterail/ledger-archives/{artifact.artifact_name}"
    assert receipt.immutability_mode == AnchorImmutabilityMode.COMPLIANCE
    assert receipt.record_count == 2
    assert receipt.store_metadata["provider"] == "aws_s3_object_lock"
    assert receipt.store_type == AnchorStoreType.WORM_OBJECT
    assert receipt.through_hash == artifact.through_hash
    assert receipt.through_sequence == 2
    assert receipt.version_id == "archive-version-1"
    verify_worm_ledger_archive_receipt(receipt)
    verification = verify_s3_object_lock_ledger_archive_receipt(receipt, s3_client=client)
    assert verification.key == f"staterail/ledger-archives/{artifact.artifact_name}"
    assert verification.to_payload()["ledger_archive_replay_verified"] is True
    assert verify_recorded_ledger_archive_receipts(AuditLedger(path, clock=clock)) == 1
    assert projection.audit_archive_count == 1
    assert view.audit_archive_count == 1


def test_ledger_archive_artifact_verification_replays_hash_chain(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.SYSTEM_STARTED, {"component": "test"})
    ledger.append(EventType.SYSTEM_STOPPED, {"component": "test", "completed_cycles": 0})
    artifact = create_ledger_archive_artifact(ledger)

    verified = verify_ledger_archive_artifact(
        artifact.artifact_jsonl,
        expected_digest=artifact.artifact_digest,
        expected_record_count=artifact.record_count,
        expected_through_hash=artifact.through_hash,
        expected_through_sequence=artifact.through_sequence,
    )

    assert verified == artifact
    with pytest.raises(LedgerArchiveError, match="append boundary newline"):
        verify_ledger_archive_artifact(artifact.artifact_jsonl.rstrip("\n"))
    with pytest.raises(LedgerArchiveError, match="not replayable"):
        verify_ledger_archive_artifact(
            artifact.artifact_jsonl.replace('"component":"test"', '"component":"tampered"', 1)
        )


def test_ledger_archive_artifact_verification_matches_receipt(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.SYSTEM_STARTED, {"component": "test"})
    artifact = create_ledger_archive_artifact(ledger)
    receipt = create_worm_ledger_archive_receipt(
        artifact_digest=artifact.artifact_digest,
        artifact_uri="s3://audit-bucket/archive.jsonl",
        immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
        record_count=artifact.record_count,
        retention_until=datetime(2035, 1, 1, tzinfo=timezone.utc),
        store_metadata={
            "object_content_verified": True,
            "object_sha256": artifact.artifact_digest,
        },
        through_hash=artifact.through_hash,
        through_sequence=artifact.through_sequence,
        version_id="archive-version-1",
    )

    verified = verify_ledger_archive_artifact_for_receipt(artifact.artifact_jsonl, receipt)

    assert verified.artifact_digest == receipt.artifact_digest
    with pytest.raises(LedgerArchiveError, match="digest mismatch"):
        verify_ledger_archive_artifact_for_receipt(b'{"not":"the-same-archive"}\n', receipt)


def test_s3_archive_receipt_verification_rejects_digest_valid_but_unreplayable_archive():
    body = b'{"not":"a-ledger-record"}\n'
    body_digest = hashlib.sha256(body).hexdigest()
    retention_until = datetime(2035, 1, 1, tzinfo=timezone.utc)
    receipt = create_worm_ledger_archive_receipt(
        artifact_digest=body_digest,
        artifact_uri="s3://audit-bucket/archive.jsonl",
        immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
        record_count=1,
        retention_until=retention_until,
        store_metadata={
            "bucket": "audit-bucket",
            "key": "archive.jsonl",
            "object_content_verified": True,
            "object_sha256": body_digest,
            "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
        },
        through_hash="0" * 64,
        through_sequence=1,
        version_id="archive-version-1",
    )
    client = FakeS3ObjectLockArchiveClient(
        object_body_response=body,
        retention_response={
            "Retention": {
                "Mode": "COMPLIANCE",
                "RetainUntilDate": retention_until,
            }
        },
    )

    with pytest.raises(LedgerArchiveError, match="Malformed ledger archive record"):
        verify_s3_object_lock_ledger_archive_receipt(receipt, s3_client=client)


def test_s3_object_lock_archive_store_rejects_object_readback_digest_mismatch(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.SYSTEM_STARTED, {"component": "test"})
    store = S3ObjectLockLedgerArchiveStore(
        S3ObjectLockLedgerArchiveConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=FakeS3ObjectLockArchiveClient(object_body_response=b"not the uploaded archive"),
    )

    with pytest.raises(LedgerArchiveError, match="digest mismatch"):
        publish_ledger_archive(path, store)
