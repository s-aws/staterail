from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import pytest

from audit.anchors import (
    LedgerAnchorError,
    create_ledger_anchor_artifact,
    publish_recorded_ledger_checkpoint_anchor,
    verify_recorded_ledger_anchor_receipts,
    verify_worm_ledger_anchor_receipt,
)
from audit.checkpoints import record_ledger_checkpoint
from audit.ledger import AuditLedger
from audit.s3_object_lock import S3ObjectLockAnchorConfig, S3ObjectLockLedgerAnchorStore
from audit.s3_object_lock import verify_s3_object_lock_anchor_receipt
from core.clock import FixedClock
from core.enums import AnchorImmutabilityMode, AnchorStoreType, EventType


class FakeS3ObjectLockClient:
    def __init__(
        self,
        *,
        object_lock_response: dict[str, Any] | None = None,
        object_body_response: bytes | None = None,
        put_response: dict[str, Any] | None = None,
        retention_response: dict[str, Any] | None = None,
        versioning_response: dict[str, Any] | None = None,
    ) -> None:
        self.get_bucket_versioning_calls: list[dict[str, Any]] = []
        self.get_object_calls: list[dict[str, Any]] = []
        self.get_object_lock_configuration_calls: list[dict[str, Any]] = []
        self.get_object_retention_calls: list[dict[str, Any]] = []
        self.put_object_calls: list[dict[str, Any]] = []
        self._object_lock_response = object_lock_response or {
            "ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}
        }
        self._object_body_response = object_body_response
        self._put_response = put_response or {"ETag": '"etag-1"', "VersionId": "version-1"}
        self._retention_response = retention_response
        self._uploaded_body: bytes | None = None
        self._uploaded_retention: dict[str, Any] | None = None
        self._versioning_response = versioning_response or {"Status": "Enabled"}

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        self.get_bucket_versioning_calls.append(kwargs)
        return self._versioning_response

    def get_object_lock_configuration(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_lock_configuration_calls.append(kwargs)
        return self._object_lock_response

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_object_calls.append(kwargs)
        self._uploaded_body = kwargs["Body"]
        self._uploaded_retention = {
            "Mode": kwargs["ObjectLockMode"],
            "RetainUntilDate": kwargs["ObjectLockRetainUntilDate"],
        }
        return self._put_response

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_calls.append(kwargs)
        body = self._object_body_response if self._object_body_response is not None else self._uploaded_body
        return {"Body": BytesIO(body or b"")}

    def get_object_retention(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_retention_calls.append(kwargs)
        return self._retention_response or {"Retention": self._uploaded_retention}


def test_s3_object_lock_anchor_store_uploads_artifact_and_receipts_verified_retention(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path, clock=clock)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path, clock=clock)
    client = FakeS3ObjectLockClient()
    config = S3ObjectLockAnchorConfig(
        bucket="audit-bucket",
        expected_bucket_owner="123456789012",
        immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
        key_prefix="/staterail/anchors/",
        retention_period=timedelta(days=7),
    )
    store = S3ObjectLockLedgerAnchorStore(config, s3_client=client)

    recorded_anchor = publish_recorded_ledger_checkpoint_anchor(path, recorded_checkpoint, store, clock=clock)

    artifact = create_ledger_anchor_artifact(recorded_checkpoint)
    assert client.get_bucket_versioning_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.get_object_lock_configuration_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    put_call = client.put_object_calls[0]
    body = put_call["Body"]
    assert body == artifact.artifact_json.encode("utf-8")
    assert put_call["Bucket"] == "audit-bucket"
    assert put_call["Key"] == f"staterail/anchors/{artifact.artifact_name}"
    assert put_call["ContentMD5"] == base64.b64encode(hashlib.md5(body).digest()).decode("ascii")
    assert put_call["ExpectedBucketOwner"] == "123456789012"
    assert put_call["ObjectLockMode"] == "COMPLIANCE"
    assert put_call["ObjectLockRetainUntilDate"] == datetime(2026, 1, 8, tzinfo=timezone.utc)
    assert put_call["Metadata"]["audit-checkpoint-hash"] == recorded_checkpoint.checkpoint.checkpoint_hash
    assert client.get_object_retention_calls == [
        {
            "Bucket": "audit-bucket",
            "ExpectedBucketOwner": "123456789012",
            "Key": f"staterail/anchors/{artifact.artifact_name}",
            "VersionId": "version-1",
        }
    ]
    assert client.get_object_calls == [
        {
            "Bucket": "audit-bucket",
            "ExpectedBucketOwner": "123456789012",
            "Key": f"staterail/anchors/{artifact.artifact_name}",
            "VersionId": "version-1",
        }
    ]
    assert recorded_anchor.receipt.artifact_digest == artifact.artifact_digest
    assert recorded_anchor.receipt.artifact_uri == f"s3://audit-bucket/staterail/anchors/{artifact.artifact_name}"
    assert recorded_anchor.receipt.immutability_mode == AnchorImmutabilityMode.COMPLIANCE
    assert recorded_anchor.receipt.retention_until == datetime(2026, 1, 8, tzinfo=timezone.utc)
    assert recorded_anchor.receipt.store_metadata["provider"] == "aws_s3_object_lock"
    assert recorded_anchor.receipt.store_metadata["expected_bucket_owner"] == "123456789012"
    assert recorded_anchor.receipt.store_metadata["object_content_verified"] is True
    assert recorded_anchor.receipt.store_metadata["object_sha256"] == artifact.artifact_digest
    assert recorded_anchor.receipt.store_type == AnchorStoreType.WORM_OBJECT
    assert recorded_anchor.receipt.version_id == "version-1"
    verify_worm_ledger_anchor_receipt(recorded_anchor.receipt)
    verification = verify_s3_object_lock_anchor_receipt(recorded_anchor.receipt, s3_client=client)
    assert verification.bucket == "audit-bucket"
    assert verification.key == f"staterail/anchors/{artifact.artifact_name}"
    assert verification.object_sha256 == artifact.artifact_digest
    assert verification.retention_until == datetime(2026, 1, 8, tzinfo=timezone.utc)
    assert verification.to_payload()["retention_verified"] is True
    assert verify_recorded_ledger_anchor_receipts(AuditLedger(path, clock=clock)) == 1


def test_s3_object_lock_anchor_store_rejects_bucket_without_versioning(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    client = FakeS3ObjectLockClient(versioning_response={"Status": "Suspended"})
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=client,
    )

    with pytest.raises(LedgerAnchorError, match="versioning"):
        store.publish(recorded_checkpoint)

    assert client.put_object_calls == []


def test_s3_object_lock_anchor_store_rejects_bucket_without_object_lock(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    client = FakeS3ObjectLockClient(object_lock_response={"ObjectLockConfiguration": {}})
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=client,
    )

    with pytest.raises(LedgerAnchorError, match="Object Lock"):
        store.publish(recorded_checkpoint)

    assert client.put_object_calls == []


def test_s3_object_lock_anchor_store_requires_uploaded_object_version(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=FakeS3ObjectLockClient(put_response={"ETag": '"etag-1"'}),
    )

    with pytest.raises(LedgerAnchorError, match="VersionId"):
        store.publish(recorded_checkpoint)


def test_s3_object_lock_anchor_store_rejects_object_readback_digest_mismatch(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=FakeS3ObjectLockClient(object_body_response=b"not the uploaded object"),
    )

    with pytest.raises(LedgerAnchorError, match="digest mismatch"):
        store.publish(recorded_checkpoint)


def test_s3_object_lock_receipt_verification_rejects_remote_digest_mismatch(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    client = FakeS3ObjectLockClient()
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=client,
    )
    recorded_anchor = publish_recorded_ledger_checkpoint_anchor(path, recorded_checkpoint, store)
    client._object_body_response = b"modified after publication"

    with pytest.raises(LedgerAnchorError, match="digest mismatch"):
        verify_s3_object_lock_anchor_receipt(recorded_anchor.receipt, s3_client=client)


def test_s3_object_lock_anchor_store_rejects_retention_mode_mismatch(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            retention_period=timedelta(days=1),
        ),
        s3_client=FakeS3ObjectLockClient(
            retention_response={
                "Retention": {
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": datetime(2026, 1, 2, tzinfo=timezone.utc),
                }
            }
        ),
    )

    with pytest.raises(LedgerAnchorError, match="mode"):
        store.publish(recorded_checkpoint, clock=FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc)))


def test_s3_object_lock_anchor_store_rejects_short_retention(workspace_tmp_path):
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path)
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.GOVERNANCE,
            retention_period=timedelta(days=7),
        ),
        s3_client=FakeS3ObjectLockClient(
            retention_response={
                "Retention": {
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": datetime(2026, 1, 2, tzinfo=timezone.utc),
                }
            }
        ),
    )

    with pytest.raises(LedgerAnchorError, match="shorter"):
        store.publish(recorded_checkpoint, clock=FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc)))


def test_s3_object_lock_receipt_verification_rejects_short_remote_retention(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(path, clock=clock)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    recorded_checkpoint = record_ledger_checkpoint(path, clock=clock)
    client = FakeS3ObjectLockClient()
    store = S3ObjectLockLedgerAnchorStore(
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            retention_period=timedelta(days=7),
        ),
        s3_client=client,
    )
    recorded_anchor = publish_recorded_ledger_checkpoint_anchor(path, recorded_checkpoint, store, clock=clock)
    client._retention_response = {
        "Retention": {
            "Mode": "COMPLIANCE",
            "RetainUntilDate": datetime(2026, 1, 2, tzinfo=timezone.utc),
        }
    }

    with pytest.raises(LedgerAnchorError, match="shorter"):
        verify_s3_object_lock_anchor_receipt(recorded_anchor.receipt, s3_client=client)


def test_s3_object_lock_anchor_config_requires_typed_mode():
    with pytest.raises(TypeError):
        S3ObjectLockAnchorConfig(
            bucket="audit-bucket",
            immutability_mode="compliance",
            retention_period=timedelta(days=1),
        )
