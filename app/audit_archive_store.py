from __future__ import annotations

from collections.abc import Callable

from audit.archives import LedgerArchiveStore
from audit.s3_object_lock import (
    S3ObjectLockLedgerArchiveConfig,
    S3ObjectLockLedgerArchiveStore,
)
from config.assembly import AuditArchiveStoreConfig
from core.enums import LedgerAnchorStoreProvider


S3ArchiveStoreFactory = Callable[[S3ObjectLockLedgerArchiveConfig], S3ObjectLockLedgerArchiveStore]


def ledger_archive_store_from_config(
    config: AuditArchiveStoreConfig,
    *,
    s3_archive_store_factory: S3ArchiveStoreFactory | None = None,
) -> LedgerArchiveStore:
    if not isinstance(config, AuditArchiveStoreConfig):
        raise TypeError("config must be an AuditArchiveStoreConfig")
    if config.provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        raise ValueError("ledger archive stores must use aws_s3_object_lock")

    s3_config = s3_object_lock_archive_config_from_store_config(config)
    if s3_archive_store_factory is not None:
        return s3_archive_store_factory(s3_config)
    return S3ObjectLockLedgerArchiveStore(s3_config)


def s3_object_lock_archive_config_from_store_config(
    config: AuditArchiveStoreConfig,
) -> S3ObjectLockLedgerArchiveConfig:
    if not isinstance(config, AuditArchiveStoreConfig):
        raise TypeError("config must be an AuditArchiveStoreConfig")
    if config.provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        raise ValueError("config must use aws_s3_object_lock provider")
    if config.s3_bucket is None:
        raise ValueError("s3_bucket is required")
    if config.s3_immutability_mode is None:
        raise ValueError("s3_immutability_mode is required")
    if config.s3_retention_period is None:
        raise ValueError("s3_retention_period is required")

    return S3ObjectLockLedgerArchiveConfig(
        bucket=config.s3_bucket,
        expected_bucket_owner=config.s3_expected_bucket_owner,
        immutability_mode=config.s3_immutability_mode,
        key_prefix=config.s3_key_prefix,
        retention_period=config.s3_retention_period,
        verify_bucket_configuration=config.s3_verify_bucket_configuration,
    )
