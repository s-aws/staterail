from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from urllib.parse import urlparse

from audit.anchors import (
    LedgerAnchorError,
    LedgerAnchorReceipt,
    create_ledger_anchor_artifact,
    create_worm_ledger_anchor_receipt,
    verify_worm_ledger_anchor_receipt,
)
from audit.archives import (
    LedgerArchiveArtifact,
    LedgerArchiveError,
    LedgerArchiveReceipt,
    create_worm_ledger_archive_receipt,
    verify_ledger_archive_artifact_for_receipt,
    verify_worm_ledger_archive_receipt,
)
from audit.checkpoints import RecordedLedgerCheckpoint
from core.clock import Clock, SystemClock
from core.enums import AnchorImmutabilityMode, LedgerAnchorStoreProvider
from core.json_tools import JsonValue, normalize_json


class S3ObjectLockClient(Protocol):
    def get_bucket_versioning(self, **kwargs: Any) -> Mapping[str, Any]:
        raise NotImplementedError

    def get_object_lock_configuration(self, **kwargs: Any) -> Mapping[str, Any]:
        raise NotImplementedError

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]:
        raise NotImplementedError

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]:
        raise NotImplementedError

    def get_object_retention(self, **kwargs: Any) -> Mapping[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class S3ObjectLockAnchorConfig:
    bucket: str
    retention_period: timedelta
    immutability_mode: AnchorImmutabilityMode
    key_prefix: str = "audit-anchors"
    expected_bucket_owner: str | None = None
    verify_bucket_configuration: bool = True

    def __post_init__(self) -> None:
        _validate_s3_object_lock_config(
            bucket=self.bucket,
            expected_bucket_owner=self.expected_bucket_owner,
            immutability_mode=self.immutability_mode,
            key_prefix=self.key_prefix,
            retention_period=self.retention_period,
            verify_bucket_configuration=self.verify_bucket_configuration,
        )


@dataclass(frozen=True)
class S3ObjectLockLedgerArchiveConfig:
    bucket: str
    retention_period: timedelta
    immutability_mode: AnchorImmutabilityMode
    key_prefix: str = "audit-ledger-archives"
    expected_bucket_owner: str | None = None
    verify_bucket_configuration: bool = True

    def __post_init__(self) -> None:
        _validate_s3_object_lock_config(
            bucket=self.bucket,
            expected_bucket_owner=self.expected_bucket_owner,
            immutability_mode=self.immutability_mode,
            key_prefix=self.key_prefix,
            retention_period=self.retention_period,
            verify_bucket_configuration=self.verify_bucket_configuration,
        )


class S3ObjectLockLedgerAnchorStore:
    def __init__(
        self,
        config: S3ObjectLockAnchorConfig,
        *,
        s3_client: S3ObjectLockClient | None = None,
    ) -> None:
        if not isinstance(config, S3ObjectLockAnchorConfig):
            raise TypeError("config must be an S3ObjectLockAnchorConfig")
        self._config = config
        self._s3_client = s3_client or _default_boto3_s3_client(error_cls=LedgerAnchorError)

    @property
    def config(self) -> S3ObjectLockAnchorConfig:
        return self._config

    def publish(
        self,
        recorded_checkpoint: RecordedLedgerCheckpoint,
        *,
        clock: Clock | None = None,
    ) -> LedgerAnchorReceipt:
        if not isinstance(recorded_checkpoint, RecordedLedgerCheckpoint):
            raise TypeError("recorded_checkpoint must be a RecordedLedgerCheckpoint")

        if self._config.verify_bucket_configuration:
            self.verify_bucket_configuration()

        artifact = create_ledger_anchor_artifact(recorded_checkpoint)
        body = artifact.artifact_json.encode("utf-8")
        published = _publish_verified_object_lock_artifact(
            artifact_digest=artifact.artifact_digest,
            body=body,
            bucket=self._config.bucket,
            clock=clock,
            content_type="application/json",
            expected_bucket_owner=self._config.expected_bucket_owner,
            immutability_mode=self._config.immutability_mode,
            key_prefix=self._config.key_prefix,
            metadata=_object_metadata(recorded_checkpoint),
            retention_period=self._config.retention_period,
            s3_client=self._s3_client,
            upload_error_cls=LedgerAnchorError,
            upload_error_message="S3 Object Lock anchor upload failed",
            artifact_name=artifact.artifact_name,
        )

        return create_worm_ledger_anchor_receipt(
            artifact_digest=artifact.artifact_digest,
            artifact_uri=published.artifact_uri,
            checkpoint_hash=recorded_checkpoint.checkpoint.checkpoint_hash,
            checkpoint_through_hash=recorded_checkpoint.checkpoint.through_hash,
            checkpoint_through_sequence=recorded_checkpoint.checkpoint.through_sequence,
            clock=clock,
            immutability_mode=published.verified_retention.mode,
            retention_until=published.verified_retention.retention_until,
            store_metadata={
                "bucket": self._config.bucket,
                "etag": published.etag,
                "expected_bucket_owner": self._config.expected_bucket_owner,
                "key": published.key,
                "object_content_length": published.verified_object.content_length,
                "object_content_verified": True,
                "object_sha256": published.verified_object.sha256_digest,
                "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
                "retention_verified": True,
            },
            version_id=published.version_id,
        )

    def verify_bucket_configuration(self) -> None:
        _verify_bucket_configuration(
            bucket=self._config.bucket,
            expected_bucket_owner=self._config.expected_bucket_owner,
            s3_client=self._s3_client,
            error_cls=LedgerAnchorError,
        )


class S3ObjectLockLedgerArchiveStore:
    def __init__(
        self,
        config: S3ObjectLockLedgerArchiveConfig,
        *,
        s3_client: S3ObjectLockClient | None = None,
    ) -> None:
        if not isinstance(config, S3ObjectLockLedgerArchiveConfig):
            raise TypeError("config must be an S3ObjectLockLedgerArchiveConfig")
        self._config = config
        self._s3_client = s3_client or _default_boto3_s3_client(error_cls=LedgerArchiveError)

    @property
    def config(self) -> S3ObjectLockLedgerArchiveConfig:
        return self._config

    def publish(
        self,
        artifact: LedgerArchiveArtifact,
        *,
        clock: Clock | None = None,
    ) -> LedgerArchiveReceipt:
        if not isinstance(artifact, LedgerArchiveArtifact):
            raise TypeError("artifact must be a LedgerArchiveArtifact")

        if self._config.verify_bucket_configuration:
            self.verify_bucket_configuration()

        body = artifact.artifact_jsonl.encode("utf-8")
        published = _publish_verified_object_lock_artifact(
            artifact_digest=artifact.artifact_digest,
            artifact_name=artifact.artifact_name,
            body=body,
            bucket=self._config.bucket,
            clock=clock,
            content_type="application/x-ndjson",
            expected_bucket_owner=self._config.expected_bucket_owner,
            immutability_mode=self._config.immutability_mode,
            key_prefix=self._config.key_prefix,
            metadata=_archive_object_metadata(artifact),
            retention_period=self._config.retention_period,
            s3_client=self._s3_client,
            upload_error_cls=LedgerArchiveError,
            upload_error_message="S3 Object Lock ledger archive upload failed",
        )

        return create_worm_ledger_archive_receipt(
            artifact_digest=artifact.artifact_digest,
            artifact_uri=published.artifact_uri,
            clock=clock,
            immutability_mode=published.verified_retention.mode,
            record_count=artifact.record_count,
            retention_until=published.verified_retention.retention_until,
            store_metadata={
                "bucket": self._config.bucket,
                "etag": published.etag,
                "expected_bucket_owner": self._config.expected_bucket_owner,
                "key": published.key,
                "object_content_length": published.verified_object.content_length,
                "object_content_verified": True,
                "object_sha256": published.verified_object.sha256_digest,
                "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
                "retention_verified": True,
            },
            through_hash=artifact.through_hash,
            through_sequence=artifact.through_sequence,
            version_id=published.version_id,
        )

    def verify_bucket_configuration(self) -> None:
        _verify_bucket_configuration(
            bucket=self._config.bucket,
            expected_bucket_owner=self._config.expected_bucket_owner,
            s3_client=self._s3_client,
            error_cls=LedgerArchiveError,
        )


@dataclass(frozen=True)
class _VerifiedS3Retention:
    mode: AnchorImmutabilityMode
    retention_until: datetime


@dataclass(frozen=True)
class _VerifiedS3Object:
    body: bytes
    content_length: int
    sha256_digest: str


@dataclass(frozen=True)
class _PublishedS3ObjectLockArtifact:
    artifact_uri: str
    etag: str | None
    key: str
    verified_object: _VerifiedS3Object
    verified_retention: _VerifiedS3Retention
    version_id: str


@dataclass(frozen=True)
class S3ObjectLockReceiptVerification:
    artifact_uri: str
    bucket: str
    key: str
    object_content_length: int
    object_sha256: str
    retention_until: datetime
    immutability_mode: AnchorImmutabilityMode
    version_id: str
    ledger_archive_replay_verified: bool | None = None

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "artifact_uri": self.artifact_uri,
            "bucket": self.bucket,
            "immutability_mode": self.immutability_mode,
            "key": self.key,
            "object_content_length": self.object_content_length,
            "object_content_verified": True,
            "object_sha256": self.object_sha256,
            "retention_until": self.retention_until,
            "retention_verified": True,
            "version_id": self.version_id,
        }
        if self.ledger_archive_replay_verified is not None:
            payload["ledger_archive_replay_verified"] = self.ledger_archive_replay_verified
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("S3 Object Lock receipt verification payload must normalize to an object")
        return normalized


def verify_s3_object_lock_anchor_receipt(
    receipt: LedgerAnchorReceipt,
    *,
    expected_bucket_owner: str | None = None,
    s3_client: S3ObjectLockClient | None = None,
) -> S3ObjectLockReceiptVerification:
    verify_worm_ledger_anchor_receipt(receipt)
    provider = receipt.store_metadata.get("provider")
    if provider is not None and provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value:
        raise LedgerAnchorError("Ledger anchor receipt is not an AWS S3 Object Lock receipt")

    bucket, key = _bucket_key_from_s3_uri(receipt.artifact_uri)
    _assert_receipt_metadata_matches(receipt, bucket=bucket, key=key)
    version_id = _required_string(receipt.version_id, "S3 Object Lock receipt missing object VersionId")
    immutability_mode = _required_immutability_mode(receipt)
    retention_until = _required_retention_until(receipt)
    owner = expected_bucket_owner or _optional_string(receipt.store_metadata.get("expected_bucket_owner"))
    client = s3_client or _default_boto3_s3_client(error_cls=LedgerAnchorError)

    verified_object = _get_verified_object(
        bucket=bucket,
        expected_artifact_digest=receipt.artifact_digest,
        expected_bucket_owner=owner,
        key=key,
        s3_client=client,
        version_id=version_id,
    )
    verified_retention = _get_verified_retention(
        bucket=bucket,
        expected_bucket_owner=owner,
        key=key,
        minimum_retention_until=retention_until,
        s3_client=client,
        version_id=version_id,
    )
    if verified_retention.mode != immutability_mode:
        raise LedgerAnchorError("S3 Object Lock receipt retention mode does not match remote object")

    return S3ObjectLockReceiptVerification(
        artifact_uri=receipt.artifact_uri,
        bucket=bucket,
        immutability_mode=verified_retention.mode,
        key=key,
        object_content_length=verified_object.content_length,
        object_sha256=verified_object.sha256_digest,
        retention_until=verified_retention.retention_until,
        version_id=version_id,
    )


def verify_s3_object_lock_ledger_archive_receipt(
    receipt: LedgerArchiveReceipt,
    *,
    expected_bucket_owner: str | None = None,
    s3_client: S3ObjectLockClient | None = None,
) -> S3ObjectLockReceiptVerification:
    verify_worm_ledger_archive_receipt(receipt)
    provider = receipt.store_metadata.get("provider")
    if provider is not None and provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value:
        raise LedgerArchiveError("Ledger archive receipt is not an AWS S3 Object Lock receipt")

    bucket, key = _bucket_key_from_s3_uri(receipt.artifact_uri, error_cls=LedgerArchiveError)
    _assert_receipt_metadata_matches(
        receipt,
        bucket=bucket,
        error_cls=LedgerArchiveError,
        key=key,
    )
    version_id = _required_archive_string(receipt.version_id, "S3 Object Lock archive receipt missing object VersionId")
    immutability_mode = _required_archive_immutability_mode(receipt)
    retention_until = _required_archive_retention_until(receipt)
    owner = expected_bucket_owner or _optional_string(receipt.store_metadata.get("expected_bucket_owner"))
    client = s3_client or _default_boto3_s3_client(error_cls=LedgerArchiveError)

    try:
        verified_object = _get_verified_object(
            bucket=bucket,
            expected_artifact_digest=receipt.artifact_digest,
            expected_bucket_owner=owner,
            key=key,
            s3_client=client,
            version_id=version_id,
        )
        verified_retention = _get_verified_retention(
            bucket=bucket,
            expected_bucket_owner=owner,
            key=key,
            minimum_retention_until=retention_until,
            s3_client=client,
            version_id=version_id,
        )
        verify_ledger_archive_artifact_for_receipt(verified_object.body, receipt)
    except Exception as exc:
        raise LedgerArchiveError(str(exc)) from exc
        if verified_retention.mode != immutability_mode:
            raise LedgerArchiveError("S3 Object Lock archive retention mode does not match remote object")

    return S3ObjectLockReceiptVerification(
        artifact_uri=receipt.artifact_uri,
        bucket=bucket,
        immutability_mode=verified_retention.mode,
        key=key,
        ledger_archive_replay_verified=True,
        object_content_length=verified_object.content_length,
        object_sha256=verified_object.sha256_digest,
        retention_until=verified_retention.retention_until,
        version_id=version_id,
    )


def _artifact_key(key_prefix: str, artifact_name: str) -> str:
    prefix = key_prefix.strip("/")
    if not prefix:
        return artifact_name
    return f"{prefix}/{artifact_name}"


def _validate_s3_object_lock_config(
    *,
    bucket: str,
    expected_bucket_owner: str | None,
    immutability_mode: AnchorImmutabilityMode,
    key_prefix: str,
    retention_period: timedelta,
    verify_bucket_configuration: bool,
) -> None:
    if not isinstance(bucket, str) or not bucket.strip():
        raise ValueError("bucket must be a non-empty string")
    if not isinstance(retention_period, timedelta):
        raise TypeError("retention_period must be a datetime.timedelta")
    if retention_period.total_seconds() <= 0:
        raise ValueError("retention_period must be positive")
    if not isinstance(immutability_mode, AnchorImmutabilityMode):
        raise TypeError("immutability_mode must be an AnchorImmutabilityMode")
    if not isinstance(key_prefix, str):
        raise TypeError("key_prefix must be a string")
    if expected_bucket_owner is not None and not expected_bucket_owner.strip():
        raise ValueError("expected_bucket_owner must be non-empty when provided")
    if not isinstance(verify_bucket_configuration, bool):
        raise TypeError("verify_bucket_configuration must be a bool")


def _bucket_kwargs(*, bucket: str, expected_bucket_owner: str | None) -> dict[str, str]:
    kwargs = {"Bucket": bucket}
    if expected_bucket_owner is not None:
        kwargs["ExpectedBucketOwner"] = expected_bucket_owner
    return kwargs


def _verify_bucket_configuration(
    *,
    bucket: str,
    expected_bucket_owner: str | None,
    s3_client: S3ObjectLockClient,
    error_cls: type[Exception],
) -> None:
    kwargs = _bucket_kwargs(bucket=bucket, expected_bucket_owner=expected_bucket_owner)
    try:
        versioning = s3_client.get_bucket_versioning(**kwargs)
    except Exception as exc:
        raise error_cls("S3 Object Lock bucket versioning preflight failed") from exc
    if versioning.get("Status") != "Enabled":
        raise error_cls("S3 Object Lock bucket versioning is not enabled")

    try:
        object_lock = s3_client.get_object_lock_configuration(**kwargs)
    except Exception as exc:
        raise error_cls("S3 Object Lock bucket configuration preflight failed") from exc
    configuration = object_lock.get("ObjectLockConfiguration")
    if not isinstance(configuration, Mapping) or configuration.get("ObjectLockEnabled") != "Enabled":
        raise error_cls("S3 Object Lock is not enabled for the bucket")


def _publish_verified_object_lock_artifact(
    *,
    artifact_digest: str,
    artifact_name: str,
    body: bytes,
    bucket: str,
    clock: Clock | None,
    content_type: str,
    expected_bucket_owner: str | None,
    immutability_mode: AnchorImmutabilityMode,
    key_prefix: str,
    metadata: Mapping[str, str],
    retention_period: timedelta,
    s3_client: S3ObjectLockClient,
    upload_error_cls: type[Exception],
    upload_error_message: str,
) -> _PublishedS3ObjectLockArtifact:
    key = _artifact_key(key_prefix, artifact_name)
    retention_until = _retention_until(clock or SystemClock(), retention_period)
    put_response = _put_object_lock_object(
        body=body,
        bucket=bucket,
        content_type=content_type,
        expected_bucket_owner=expected_bucket_owner,
        immutability_mode=immutability_mode,
        key=key,
        metadata=metadata,
        retention_until=retention_until,
        s3_client=s3_client,
        upload_error_cls=upload_error_cls,
        upload_error_message=upload_error_message,
    )
    version_id = _required_version_id(
        put_response.get("VersionId"),
        error_cls=upload_error_cls,
    )
    try:
        verified_object = _get_verified_object(
            bucket=bucket,
            expected_artifact_digest=artifact_digest,
            expected_bucket_owner=expected_bucket_owner,
            key=key,
            s3_client=s3_client,
            version_id=version_id,
        )
        verified_retention = _get_verified_retention(
            bucket=bucket,
            expected_bucket_owner=expected_bucket_owner,
            key=key,
            minimum_retention_until=retention_until,
            s3_client=s3_client,
            version_id=version_id,
        )
    except Exception as exc:
        raise upload_error_cls(str(exc)) from exc
    if verified_retention.mode != immutability_mode:
        raise upload_error_cls("S3 Object Lock retention mode does not match requested mode")
    return _PublishedS3ObjectLockArtifact(
        artifact_uri=f"s3://{bucket}/{key}",
        etag=_optional_string(put_response.get("ETag")),
        key=key,
        verified_object=verified_object,
        verified_retention=verified_retention,
        version_id=version_id,
    )


def _put_object_lock_object(
    *,
    body: bytes,
    bucket: str,
    content_type: str,
    expected_bucket_owner: str | None,
    immutability_mode: AnchorImmutabilityMode,
    key: str,
    metadata: Mapping[str, str],
    retention_until: datetime,
    s3_client: S3ObjectLockClient,
    upload_error_cls: type[Exception],
    upload_error_message: str,
) -> Mapping[str, Any]:
    kwargs: dict[str, Any] = {
        "Body": body,
        "Bucket": bucket,
        "ContentMD5": _content_md5(body),
        "ContentType": content_type,
        "Key": key,
        "Metadata": dict(metadata),
        "ObjectLockMode": immutability_mode.name,
        "ObjectLockRetainUntilDate": retention_until,
    }
    if expected_bucket_owner is not None:
        kwargs["ExpectedBucketOwner"] = expected_bucket_owner
    try:
        return s3_client.put_object(**kwargs)
    except Exception as exc:
        raise upload_error_cls(upload_error_message) from exc


def _required_version_id(value: Any, *, error_cls: type[Exception]) -> str:
    if not isinstance(value, str) or not value:
        raise error_cls("S3 Object Lock upload missing VersionId")
    return value


def _content_md5(body: bytes) -> str:
    return base64.b64encode(hashlib.md5(body).digest()).decode("ascii")


def _default_boto3_s3_client(*, error_cls: type[Exception]) -> S3ObjectLockClient:
    try:
        import boto3
    except ImportError as exc:
        raise error_cls("boto3 is required for S3 Object Lock operations") from exc
    return boto3.client("s3")


def _normalize_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("value must be a datetime")
    timestamp = value
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _object_metadata(recorded_checkpoint: RecordedLedgerCheckpoint) -> dict[str, str]:
    checkpoint = recorded_checkpoint.checkpoint
    return {
        "audit-artifact-schema": "1",
        "audit-checkpoint-hash": checkpoint.checkpoint_hash,
        "audit-digest-algorithm": checkpoint.digest_algorithm.value,
        "audit-record-sequence": str(recorded_checkpoint.audit_record_sequence),
        "audit-through-sequence": str(checkpoint.through_sequence),
    }


def _archive_object_metadata(artifact: LedgerArchiveArtifact) -> dict[str, str]:
    return {
        "audit-artifact-schema": "1",
        "audit-artifact-type": "ledger-archive",
        "audit-digest-algorithm": "sha256",
        "audit-record-count": str(artifact.record_count),
        "audit-through-hash": artifact.through_hash,
        "audit-through-sequence": str(artifact.through_sequence),
    }


def _read_object_body(body: Any) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode("utf-8")
    read = getattr(body, "read", None)
    if callable(read):
        data = read()
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
    raise LedgerAnchorError("S3 Object Lock object read-back body is malformed")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _required_string(value: Any, message: str) -> str:
    if not isinstance(value, str) or not value:
        raise LedgerAnchorError(message)
    return value


def _bucket_key_from_s3_uri(
    uri: str,
    *,
    error_cls: type[Exception] = LedgerAnchorError,
) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise error_cls("S3 Object Lock receipt artifact_uri is not a valid s3:// URI")
    return parsed.netloc, parsed.path.lstrip("/")


def _assert_receipt_metadata_matches(
    receipt: LedgerAnchorReceipt | LedgerArchiveReceipt,
    *,
    bucket: str,
    error_cls: type[Exception] = LedgerAnchorError,
    key: str,
) -> None:
    metadata_bucket = receipt.store_metadata.get("bucket")
    metadata_key = receipt.store_metadata.get("key")
    if metadata_bucket is not None and metadata_bucket != bucket:
        raise error_cls("S3 Object Lock receipt bucket metadata does not match artifact_uri")
    if metadata_key is not None and metadata_key != key:
        raise error_cls("S3 Object Lock receipt key metadata does not match artifact_uri")


def _required_immutability_mode(receipt: LedgerAnchorReceipt) -> AnchorImmutabilityMode:
    if receipt.immutability_mode is None:
        raise LedgerAnchorError("S3 Object Lock receipt missing immutability mode")
    return receipt.immutability_mode


def _required_retention_until(receipt: LedgerAnchorReceipt) -> datetime:
    if receipt.retention_until is None:
        raise LedgerAnchorError("S3 Object Lock receipt missing retention timestamp")
    return receipt.retention_until


def _required_archive_string(value: Any, message: str) -> str:
    if not isinstance(value, str) or not value:
        raise LedgerArchiveError(message)
    return value


def _required_archive_immutability_mode(receipt: LedgerArchiveReceipt) -> AnchorImmutabilityMode:
    if receipt.immutability_mode is None:
        raise LedgerArchiveError("S3 Object Lock archive receipt missing immutability mode")
    return receipt.immutability_mode


def _required_archive_retention_until(receipt: LedgerArchiveReceipt) -> datetime:
    if receipt.retention_until is None:
        raise LedgerArchiveError("S3 Object Lock archive receipt missing retention timestamp")
    return receipt.retention_until


def _get_verified_object(
    *,
    bucket: str,
    expected_artifact_digest: str,
    expected_bucket_owner: str | None,
    key: str,
    s3_client: S3ObjectLockClient,
    version_id: str,
) -> _VerifiedS3Object:
    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "VersionId": version_id,
    }
    if expected_bucket_owner is not None:
        kwargs["ExpectedBucketOwner"] = expected_bucket_owner
    try:
        response = s3_client.get_object(**kwargs)
    except Exception as exc:
        raise LedgerAnchorError("S3 Object Lock receipt object verification failed") from exc

    body = _read_object_body(response.get("Body"))
    observed_digest = hashlib.sha256(body).hexdigest()
    if observed_digest != expected_artifact_digest:
        raise LedgerAnchorError("S3 Object Lock receipt object digest mismatch")
    return _VerifiedS3Object(
        body=body,
        content_length=len(body),
        sha256_digest=observed_digest,
    )


def _get_verified_retention(
    *,
    bucket: str,
    expected_bucket_owner: str | None,
    key: str,
    minimum_retention_until: datetime,
    s3_client: S3ObjectLockClient,
    version_id: str,
) -> _VerifiedS3Retention:
    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "VersionId": version_id,
    }
    if expected_bucket_owner is not None:
        kwargs["ExpectedBucketOwner"] = expected_bucket_owner
    try:
        response = s3_client.get_object_retention(**kwargs)
    except Exception as exc:
        raise LedgerAnchorError("S3 Object Lock receipt retention verification failed") from exc

    retention = response.get("Retention")
    if not isinstance(retention, Mapping):
        raise LedgerAnchorError("S3 Object Lock receipt retention response is missing Retention")
    try:
        mode = AnchorImmutabilityMode(str(retention["Mode"]).lower())
        retention_until = _normalize_utc(retention["RetainUntilDate"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LedgerAnchorError("S3 Object Lock receipt retention response is malformed") from exc
    if retention_until < minimum_retention_until:
        raise LedgerAnchorError("S3 Object Lock receipt retention timestamp is shorter than recorded")
    return _VerifiedS3Retention(mode=mode, retention_until=retention_until)


def _retention_until(clock: Clock, retention_period: timedelta) -> datetime:
    return _normalize_utc(clock.now() + retention_period).replace(microsecond=0)
