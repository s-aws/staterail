from __future__ import annotations

from collections.abc import Callable

from audit.anchors import LedgerAnchorStore, LocalFileLedgerAnchorStore
from audit.s3_object_lock import S3ObjectLockAnchorConfig, S3ObjectLockLedgerAnchorStore
from config.assembly import AuditAnchorStoreConfig
from core.enums import LedgerAnchorStoreProvider


S3AnchorStoreFactory = Callable[[S3ObjectLockAnchorConfig], S3ObjectLockLedgerAnchorStore]


def ledger_anchor_store_from_config(
    config: AuditAnchorStoreConfig,
    *,
    s3_anchor_store_factory: S3AnchorStoreFactory | None = None,
) -> LedgerAnchorStore:
    if not isinstance(config, AuditAnchorStoreConfig):
        raise TypeError("config must be an AuditAnchorStoreConfig")

    if config.provider == LedgerAnchorStoreProvider.LOCAL_FILE:
        if config.local_anchor_dir is None:
            raise ValueError("local_anchor_dir is required for local_file anchor stores")
        return LocalFileLedgerAnchorStore(config.local_anchor_dir)

    if config.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        s3_config = s3_object_lock_anchor_config_from_store_config(config)
        if s3_anchor_store_factory is not None:
            return s3_anchor_store_factory(s3_config)
        return S3ObjectLockLedgerAnchorStore(s3_config)

    raise ValueError(f"unsupported ledger anchor store provider: {config.provider.value}")


def s3_object_lock_anchor_config_from_store_config(
    config: AuditAnchorStoreConfig,
) -> S3ObjectLockAnchorConfig:
    if not isinstance(config, AuditAnchorStoreConfig):
        raise TypeError("config must be an AuditAnchorStoreConfig")
    if config.provider != LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        raise ValueError("config must use aws_s3_object_lock provider")
    if config.s3_bucket is None:
        raise ValueError("s3_bucket is required")
    if config.s3_immutability_mode is None:
        raise ValueError("s3_immutability_mode is required")
    if config.s3_retention_period is None:
        raise ValueError("s3_retention_period is required")

    return S3ObjectLockAnchorConfig(
        bucket=config.s3_bucket,
        expected_bucket_owner=config.s3_expected_bucket_owner,
        immutability_mode=config.s3_immutability_mode,
        key_prefix=config.s3_key_prefix,
        retention_period=config.s3_retention_period,
        verify_bucket_configuration=config.s3_verify_bucket_configuration,
    )
