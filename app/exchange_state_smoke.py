from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from app.bootstrap import CoinbaseApplicationConfig, build_coinbase_application
from app.config_fingerprint import application_config_startup_metadata
from app.smoke_checks import validate_no_config_placeholders
from core.enums import ErrorCategory, EventType, ReadinessStatus, RuntimeComponent, RuntimeStopReason
from core.errors import ConfigError, exception_to_error_payload
from core.json_tools import JsonValue, normalize_json
from exchanges.coinbase.advanced_trade_rest import HttpTransport
from exchanges.coinbase.advanced_trade_ws import JwtFactory
from exchanges.coinbase.auth import TokenProvider


EXCHANGE_STATE_SMOKE_STAGE = RuntimeComponent.EXCHANGE_STATE_SMOKE.value


def exchange_state_smoke_payload(
    config: CoinbaseApplicationConfig,
    *,
    jwt_factory: JwtFactory | None = None,
    token_provider: TokenProvider | None = None,
    transport: HttpTransport | None = None,
) -> dict[str, JsonValue]:
    validate_no_config_placeholders(config, stage=EXCHANGE_STATE_SMOKE_STAGE)
    application = build_coinbase_application(
        config,
        jwt_factory=jwt_factory,
        token_provider=token_provider,
        transport=transport,
    )
    reconciliation = application.assembly.exchange_state_reconciliation
    if reconciliation is None:
        exc = ConfigError(
            "exchange-state smoke requires assembled account or position lookup clients",
            context={"stage": EXCHANGE_STATE_SMOKE_STAGE},
        )
        application.core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.CONFIG,
                context={"stage": EXCHANGE_STATE_SMOKE_STAGE},
            ),
        )
        raise exc

    started_record = application.core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.EXCHANGE_STATE_SMOKE.value,
            "started_at": _utc_now(),
            "startup_metadata": application_config_startup_metadata(config),
            "task_count": 1,
        },
    )
    stopped_record = None
    result = None
    try:
        result = reconciliation.reconcile()
    except Exception as exc:
        application.core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.EXCHANGE_TRANSPORT,
                context={"stage": EXCHANGE_STATE_SMOKE_STAGE},
            ),
        )
        raise
    finally:
        stopped_record = application.core.emit(
            EventType.SYSTEM_STOPPED,
            {
                "component": RuntimeComponent.EXCHANGE_STATE_SMOKE.value,
                "completed_cycles": 0,
                "reason": RuntimeStopReason.STOP_REQUESTED.value,
                "started_sequence": started_record.sequence,
                "stopped_at": _utc_now(),
            },
        )

    records = tuple(
        record
        for record in application.ledger.iter_records()
        if started_record.sequence <= record.sequence <= stopped_record.sequence
    )
    event_counts = Counter(record.event_type for record in records)
    result_payload = {
        "balance_snapshots": result.balance_snapshots,
        "drift_count": result.drift_count,
        "error_count": result.error_count,
        "new_drift_record_count": result.new_drift_record_count,
        "position_snapshots": result.position_snapshots,
    }
    attention_reasons = _attention_reasons(
        drift_count=result.drift_count,
        error_count=result.error_count,
    )
    payload = {
        "attention_reasons": attention_reasons,
        "event_counts": {
            event_type.value: event_counts[event_type]
            for event_type in sorted(event_counts, key=lambda item: item.value)
        },
        "ledger_path": application.ledger.path.as_posix(),
        "order_endpoint_called": False,
        "result": result_payload,
        "runtime_tasks_started": False,
        "sequence_range": {
            "started": started_record.sequence,
            "stopped": stopped_record.sequence,
        },
        "status": ReadinessStatus.ATTENTION_REQUIRED if attention_reasons else ReadinessStatus.OK,
        "strategy_tasks_started": False,
        "websocket_started": False,
        "writes_ledger": True,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("exchange-state smoke payload must normalize to an object")
    return normalized


def _attention_reasons(*, drift_count: int, error_count: int) -> tuple[str, ...]:
    reasons: list[str] = []
    if error_count:
        reasons.append("errors_observed")
    if drift_count:
        reasons.append("position_drift")
    return tuple(reasons)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
