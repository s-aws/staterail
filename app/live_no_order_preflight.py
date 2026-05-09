from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta

from app.bootstrap import CoinbaseApplicationConfig
from app.exchange_state_smoke import exchange_state_smoke_payload
from app.feed_smoke import feed_smoke_payload
from app.live_preflight_gate import (
    LIVE_NO_ORDER_PREFLIGHT_STEP_ORDER,
    record_live_no_order_preflight_result,
)
from app.product_catalog_smoke import product_catalog_smoke_payload
from app.readiness import ReadinessCheckResult, readiness_payload
from config.assembly import WebSocketSourceFactory
from core.enums import PreflightStep, ReadinessStatus
from core.json_tools import JsonValue, normalize_json
from exchanges.coinbase.advanced_trade_rest import HttpTransport
from exchanges.coinbase.advanced_trade_ws import JwtFactory
from exchanges.coinbase.auth import TokenProvider
from products.tasks import ProductCatalogLookup


async def live_no_order_preflight_payload(
    config: CoinbaseApplicationConfig,
    *,
    duration: timedelta,
    allow_config_fingerprint_mismatch: bool = False,
    jwt_factory: JwtFactory | None = None,
    live_trading_approved: bool = False,
    product_catalog_client: ProductCatalogLookup | None = None,
    readiness_extra_checks: Iterable[ReadinessCheckResult] = (),
    token_provider: TokenProvider | None = None,
    transport: HttpTransport | None = None,
    websocket_source_factory: WebSocketSourceFactory | None = None,
) -> dict[str, JsonValue]:
    steps: list[dict[str, JsonValue]] = []

    readiness = readiness_payload(
        config,
        allow_config_fingerprint_mismatch=allow_config_fingerprint_mismatch,
        extra_checks=readiness_extra_checks,
        jwt_factory_configured=jwt_factory is not None,
        live_trading_approved=live_trading_approved,
        token_provider_configured=token_provider is not None,
    )
    steps.append(_step_payload(PreflightStep.READINESS, readiness))
    if _requires_attention(readiness):
        return _preflight_payload(config, steps=steps, stopped_after=PreflightStep.READINESS)

    product_catalog = product_catalog_smoke_payload(
        config,
        jwt_factory=jwt_factory,
        product_catalog_client=product_catalog_client,
        token_provider=token_provider,
        transport=transport,
    )
    steps.append(_step_payload(PreflightStep.PRODUCT_CATALOG_SMOKE, product_catalog))
    if _requires_attention(product_catalog):
        return _preflight_payload(
            config,
            steps=steps,
            stopped_after=PreflightStep.PRODUCT_CATALOG_SMOKE,
        )

    feed = await feed_smoke_payload(
        config,
        duration=duration,
        jwt_factory=jwt_factory,
        token_provider=token_provider,
        transport=transport,
        websocket_source_factory=websocket_source_factory,
    )
    steps.append(_step_payload(PreflightStep.FEED_SMOKE, feed))
    if _requires_attention(feed):
        return _preflight_payload(config, steps=steps, stopped_after=PreflightStep.FEED_SMOKE)

    exchange_state = exchange_state_smoke_payload(
        config,
        jwt_factory=jwt_factory,
        token_provider=token_provider,
        transport=transport,
    )
    steps.append(_step_payload(PreflightStep.EXCHANGE_STATE_SMOKE, exchange_state))
    stopped_after = (
        PreflightStep.EXCHANGE_STATE_SMOKE if _requires_attention(exchange_state) else None
    )
    return _preflight_payload(config, steps=steps, stopped_after=stopped_after)


def _preflight_payload(
    config: CoinbaseApplicationConfig,
    *,
    steps: list[dict[str, JsonValue]],
    stopped_after: PreflightStep | None,
) -> dict[str, JsonValue]:
    completed_step_names = tuple(str(step["name"]) for step in steps)
    skipped_step_names = tuple(
        step.value
        for step in LIVE_NO_ORDER_PREFLIGHT_STEP_ORDER
        if step.value not in completed_step_names
    )
    attention_required = stopped_after is not None
    payload = {
        "completed_step_names": completed_step_names,
        "ledger_path": config.ledger_path.as_posix(),
        "order_endpoint_called": False,
        "runtime_tasks_started": False,
        "skipped_step_names": skipped_step_names,
        "status": ReadinessStatus.ATTENTION_REQUIRED if attention_required else ReadinessStatus.OK,
        "stopped_after_step": stopped_after.value if stopped_after is not None else None,
        "steps": steps,
        "strategy_tasks_started": False,
        "writes_ledger": any(
            bool(step["payload"].get("writes_ledger"))
            for step in steps
            if isinstance(step.get("payload"), dict)
        ),
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("live no-order preflight payload must normalize to an object")
    if normalized.get("writes_ledger") is True:
        record = record_live_no_order_preflight_result(config, normalized)
        normalized["preflight_result_sequence"] = record.sequence
    else:
        normalized["preflight_result_sequence"] = None
    return normalized


def _step_payload(step: PreflightStep, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
    normalized_payload = normalize_json(payload)
    if not isinstance(normalized_payload, dict):
        raise TypeError("preflight step payload must normalize to an object")
    return {
        "name": step.value,
        "payload": normalized_payload,
        "status": normalized_payload.get("status"),
    }


def _requires_attention(payload: dict[str, JsonValue]) -> bool:
    return payload.get("status") != ReadinessStatus.OK.value
