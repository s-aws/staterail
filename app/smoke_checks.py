from __future__ import annotations

from typing import NoReturn

from app.bootstrap import CoinbaseApplicationConfig
from app.live_safety import config_placeholder_paths
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import ErrorCategory, EventType
from core.errors import ConfigError, exception_to_error_payload


def validate_no_config_placeholders(
    config: CoinbaseApplicationConfig,
    *,
    stage: str,
) -> None:
    placeholder_paths = config_placeholder_paths(config)
    if placeholder_paths:
        audit_smoke_config_error(
            config,
            stage=stage,
            message=f"{stage} requires all REPLACE_WITH_ config placeholders to be replaced",
            context={"placeholder_paths": placeholder_paths},
        )


def audit_smoke_config_error(
    config: CoinbaseApplicationConfig,
    *,
    stage: str,
    message: str,
    context: dict[str, object] | None = None,
) -> NoReturn:
    exc = ConfigError(message, context={"stage": stage, **(context or {})})
    AuditCore(AuditLedger(config.ledger_path)).emit(
        EventType.ERROR,
        exception_to_error_payload(
            exc,
            category=ErrorCategory.CONFIG,
            context={"stage": stage},
        ),
    )
    raise exc
