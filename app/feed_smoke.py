from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone

from app.bootstrap import CoinbaseApplicationConfig, build_coinbase_application
from app.config_fingerprint import application_config_startup_metadata
from app.smoke_checks import audit_smoke_config_error, validate_no_config_placeholders
from config.assembly import WebSocketSourceFactory
from core.enums import ErrorCategory, EventType, ReadinessStatus, RuntimeComponent, RuntimeStopReason
from core.errors import ConfigError, exception_to_error_payload
from core.json_tools import JsonValue, normalize_json
from exchanges.coinbase.advanced_trade_rest import HttpTransport
from exchanges.coinbase.advanced_trade_ws import JwtFactory
from exchanges.coinbase.auth import TokenProvider


FEED_SMOKE_STAGE = "feed_smoke"


async def feed_smoke_payload(
    config: CoinbaseApplicationConfig,
    *,
    duration: timedelta,
    jwt_factory: JwtFactory | None = None,
    token_provider: TokenProvider | None = None,
    transport: HttpTransport | None = None,
    websocket_source_factory: WebSocketSourceFactory | None = None,
) -> dict[str, JsonValue]:
    _validate_feed_smoke_config(config, duration=duration)
    application = build_coinbase_application(
        config,
        jwt_factory=jwt_factory,
        token_provider=token_provider,
        transport=transport,
        websocket_source_factory=websocket_source_factory,
    )
    feed_supervisor = application.assembly.feed_supervisor
    feed_router = application.assembly.feed_router
    if feed_supervisor is None or feed_router is None:
        exc = ConfigError(
            "feed smoke requires assembled websocket feed supervisor and router",
            context={"stage": FEED_SMOKE_STAGE},
        )
        application.core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.CONFIG,
                context={"stage": FEED_SMOKE_STAGE},
            ),
        )
        raise exc

    started_record = application.core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.FEED_SMOKE.value,
            "started_at": _utc_now(),
            "startup_metadata": application_config_startup_metadata(config),
            "task_count": len(application.assembly.websocket_feed_sources),
        },
    )
    stopped_record = None
    try:
        await _run_feed_supervisor_for(feed_supervisor, duration=duration)
    except Exception as exc:
        application.core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.FEED_SOURCE,
                context={"stage": FEED_SMOKE_STAGE},
            ),
        )
        raise
    finally:
        stopped_record = application.core.emit(
            EventType.SYSTEM_STOPPED,
            {
                "component": RuntimeComponent.FEED_SMOKE.value,
                "completed_cycles": 0,
                "duration_seconds": duration.total_seconds(),
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
    connected_source_ids = tuple(
        sorted(
            {
                str(record.payload.get("source_id"))
                for record in records
                if record.event_type == EventType.FEED_CONNECTED
                and isinstance(record.payload, dict)
                and record.payload.get("source_id")
            }
        )
    )
    configured_source_ids = tuple(source.source_id for source in config.bot.websocket_sources)
    missing_connected_source_ids = tuple(
        source_id for source_id in configured_source_ids if source_id not in connected_source_ids
    )
    final_source_states = tuple(
        {
            "last_seen": state.last_seen.isoformat() if state.last_seen is not None else None,
            "source_id": state.source_id,
            "status": state.status.value,
        }
        for state in feed_router.source_states()
    )
    error_count = event_counts[EventType.ERROR]
    sequence_anomaly_count = event_counts[EventType.DATA_OUT_OF_ORDER] + event_counts[EventType.DATA_SEQUENCE_GAP]
    data_signal_count = event_counts[EventType.DATA_ACCEPTED] + event_counts[EventType.FEED_HEARTBEAT]
    min_connected_sources = min(config.bot.feed.min_live_sources, len(configured_source_ids))
    attention_reasons = _attention_reasons(
        connected_source_count=len(connected_source_ids),
        data_signal_count=data_signal_count,
        error_count=error_count,
        feed_degraded_count=event_counts[EventType.FEED_DEGRADED],
        min_connected_sources=min_connected_sources,
        sequence_anomaly_count=sequence_anomaly_count,
    )
    payload = {
        "attention_reasons": attention_reasons,
        "configured_source_ids": configured_source_ids,
        "connected_source_ids": connected_source_ids,
        "duration_seconds": duration.total_seconds(),
        "event_counts": {
            event_type.value: event_counts[event_type]
            for event_type in sorted(event_counts, key=lambda item: item.value)
        },
        "final_source_states": final_source_states,
        "ledger_path": application.ledger.path.as_posix(),
        "missing_connected_source_ids": missing_connected_source_ids,
        "order_endpoint_called": False,
        "runtime_tasks_started": False,
        "sequence_range": {
            "started": started_record.sequence,
            "stopped": stopped_record.sequence,
        },
        "status": ReadinessStatus.ATTENTION_REQUIRED if attention_reasons else ReadinessStatus.OK,
        "strategy_tasks_started": False,
        "websocket_started": True,
        "writes_ledger": True,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("feed smoke payload must normalize to an object")
    return normalized


def _validate_feed_smoke_config(config: CoinbaseApplicationConfig, *, duration: timedelta) -> None:
    validate_no_config_placeholders(config, stage=FEED_SMOKE_STAGE)
    if duration <= timedelta(0):
        audit_smoke_config_error(
            config,
            stage=FEED_SMOKE_STAGE,
            message="feed smoke duration must be positive",
            context={"duration_seconds": duration.total_seconds()},
        )
    if not config.bot.websocket_sources:
        audit_smoke_config_error(
            config,
            stage=FEED_SMOKE_STAGE,
            message="feed smoke requires configured websocket sources",
        )


async def _run_feed_supervisor_for(feed_supervisor: object, *, duration: timedelta) -> None:
    from feeds.supervisor import FeedSupervisor

    if not isinstance(feed_supervisor, FeedSupervisor):
        raise TypeError("feed_supervisor must be a FeedSupervisor")
    task = asyncio.create_task(feed_supervisor.run(), name="feed-smoke-supervisor")
    try:
        await asyncio.sleep(duration.total_seconds())
    finally:
        feed_supervisor.stop()
        task.cancel()
        results = await asyncio.gather(task, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result


def _attention_reasons(
    *,
    connected_source_count: int,
    data_signal_count: int,
    error_count: int,
    feed_degraded_count: int,
    min_connected_sources: int,
    sequence_anomaly_count: int,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if connected_source_count < min_connected_sources:
        reasons.append("insufficient_connected_sources")
    if data_signal_count == 0:
        reasons.append("no_feed_data_or_heartbeat")
    if error_count:
        reasons.append("errors_observed")
    if feed_degraded_count:
        reasons.append("feed_degraded")
    if sequence_anomaly_count:
        reasons.append("sequence_anomalies")
    return tuple(reasons)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
