from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from app.bootstrap import CoinbaseApplicationConfig, build_coinbase_application
from app.smoke_checks import audit_smoke_config_error, validate_no_config_placeholders
from config.assembly import RiskPolicyConfig, effective_risk_policy_config
from core.enums import ErrorCategory, EventType, OrderSide, PolicyViabilityReason, ReadinessStatus
from core.errors import ConfigError, exception_to_error_payload
from core.json_tools import JsonValue, normalize_json
from exchanges.coinbase.advanced_trade_rest import HttpTransport
from exchanges.coinbase.advanced_trade_ws import JwtFactory
from exchanges.coinbase.auth import TokenProvider
from exchanges.coinbase.venues import COINBASE_LIVE_EXECUTION_PRODUCT_VENUES
from products.catalog import ProductMetadata
from products.tasks import ProductCatalogLookup
from strategies.passive_market_making import (
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    PassiveMarketMakingStrategy,
)


PRODUCT_CATALOG_SMOKE_STAGE = "product_catalog_smoke"
REFERENCE_PRICE_FIELDS = ("mid_market_price", "price", "best_bid_price", "best_ask_price")


def product_catalog_smoke_payload(
    config: CoinbaseApplicationConfig,
    *,
    jwt_factory: JwtFactory | None = None,
    product_catalog_client: ProductCatalogLookup | None = None,
    token_provider: TokenProvider | None = None,
    transport: HttpTransport | None = None,
) -> dict[str, JsonValue]:
    _validate_product_catalog_smoke_config(config)
    application = build_coinbase_application(
        config,
        jwt_factory=jwt_factory,
        product_catalog_client=product_catalog_client,
        token_provider=token_provider,
        transport=transport,
    )
    refresh_task = application.assembly.product_catalog_refresh_task
    if refresh_task is None:
        exc = ConfigError(
            "product catalog smoke requires an assembled product catalog refresh task",
            context={"stage": PRODUCT_CATALOG_SMOKE_STAGE},
        )
        application.core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.CONFIG,
                context={"stage": PRODUCT_CATALOG_SMOKE_STAGE},
            ),
        )
        raise exc

    try:
        refresh_result = refresh_task.refresh()
    except Exception as exc:
        application.core.emit(
            EventType.ERROR,
            exception_to_error_payload(
                exc,
                category=ErrorCategory.EXCHANGE_TRANSPORT,
                context={
                    "configured_product_ids": config.bot.product_catalog.product_ids,
                    "stage": PRODUCT_CATALOG_SMOKE_STAGE,
                },
            ),
        )
        raise

    catalog = application.assembly.product_catalog
    products = tuple(sorted(catalog.values(), key=lambda product: product.product_id)) if catalog is not None else ()
    product_ids = tuple(product.product_id for product in products)
    configured_product_ids = config.bot.product_catalog.product_ids
    missing_product_ids = tuple(product_id for product_id in configured_product_ids if product_id not in product_ids)
    unsupported_product_venues = tuple(
        {
            product.product_venue
            for product in products
            if product.product_id in configured_product_ids
            and product.product_venue not in COINBASE_LIVE_EXECUTION_PRODUCT_VENUES
        }
    )
    untradable_product_ids = tuple(
        product.product_id
        for product in products
        if product.product_id in configured_product_ids and not product.tradable_for_new_orders
    )
    policy_viability = _policy_viability_payload(config, products)
    attention_count = (
        len(missing_product_ids)
        + len(unsupported_product_venues)
        + len(untradable_product_ids)
        + int(policy_viability["status"] != ReadinessStatus.OK.value)
    )
    payload = {
        "configured_product_ids": configured_product_ids,
        "ledger_path": application.ledger.path.as_posix(),
        "missing_product_ids": missing_product_ids,
        "order_endpoint_called": False,
        "policy_viability": policy_viability,
        "products": [product.to_payload() for product in products],
        "refresh_result": refresh_result,
        "runtime_tasks_started": False,
        "status": (
            ReadinessStatus.ATTENTION_REQUIRED
            if attention_count
            else ReadinessStatus.OK
        ),
        "untradable_product_ids": untradable_product_ids,
        "unsupported_product_venues": tuple(
            venue.value for venue in sorted(unsupported_product_venues, key=lambda venue: venue.value)
        ),
        "websocket_started": False,
        "writes_ledger": True,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("product catalog smoke payload must normalize to an object")
    return normalized


def _validate_product_catalog_smoke_config(config: CoinbaseApplicationConfig) -> None:
    validate_no_config_placeholders(config, stage=PRODUCT_CATALOG_SMOKE_STAGE)
    if not config.bot.product_catalog.schedule.enabled:
        audit_smoke_config_error(
            config,
            stage=PRODUCT_CATALOG_SMOKE_STAGE,
            message="product catalog smoke requires bot.product_catalog.enabled=true",
        )
    if not config.bot.product_catalog.product_ids:
        audit_smoke_config_error(
            config,
            stage=PRODUCT_CATALOG_SMOKE_STAGE,
            message="product catalog smoke requires bot.product_catalog.product_ids",
        )


def _policy_viability_payload(
    config: CoinbaseApplicationConfig,
    products: tuple[ProductMetadata, ...],
) -> dict[str, JsonValue]:
    product_by_id = {product.product_id: product for product in products}
    configured_product_ids = tuple(
        product_id
        for product_id in config.bot.product_catalog.product_ids
        if product_id in product_by_id
    )
    risk = effective_risk_policy_config(config.bot)
    product_checks = tuple(
        _product_policy_check(product_by_id[product_id], risk)
        for product_id in configured_product_ids
    )
    passive_market_making = _passive_market_making_capacity_check(
        config,
        configured_product_ids=configured_product_ids,
    )
    attention_count = sum(
        1
        for check in product_checks
        if check["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    ) + int(passive_market_making["status"] == ReadinessStatus.ATTENTION_REQUIRED.value)
    payload = {
        "attention_count": attention_count,
        "passive_market_making": passive_market_making,
        "product_checks": product_checks,
        "status": (
            ReadinessStatus.ATTENTION_REQUIRED
            if attention_count
            else ReadinessStatus.OK
        ),
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("policy viability payload must normalize to an object")
    return normalized


def _product_policy_check(
    product: ProductMetadata,
    risk: RiskPolicyConfig,
) -> dict[str, JsonValue]:
    reference_price, reference_price_source = _reference_price(product)
    minimum_size = (
        product.minimum_valid_size(reference_price)
        if reference_price is not None
        else None
    )
    minimum_notional = (
        product.notional(minimum_size, reference_price)
        if minimum_size is not None and reference_price is not None
        else None
    )
    max_order_notional = risk.max_order_notional
    max_visible_notional = risk.max_visible_notional
    reasons: list[str] = []
    if (
        minimum_notional is not None
        and max_order_notional is not None
        and minimum_notional > max_order_notional
    ):
        reasons.append(PolicyViabilityReason.MINIMUM_ORDER_NOTIONAL_EXCEEDS_MAX_ORDER_NOTIONAL.value)
    if (
        minimum_notional is not None
        and max_visible_notional is not None
        and minimum_notional > max_visible_notional
    ):
        reasons.append(PolicyViabilityReason.MINIMUM_ORDER_NOTIONAL_EXCEEDS_MAX_VISIBLE_NOTIONAL.value)

    payload = {
        "base_min_size": _decimal_payload(product.base_min_size),
        "contract_size": _decimal_payload(product.contract_size),
        "evaluated": minimum_notional is not None,
        "max_order_notional": _decimal_payload(max_order_notional),
        "max_visible_notional": _decimal_payload(max_visible_notional),
        "minimum_order_notional": _decimal_payload(minimum_notional),
        "minimum_order_size": _decimal_payload(minimum_size),
        "notional_multiplier": str(product.notional_multiplier),
        "product_id": product.product_id,
        "reasons": reasons,
        "reference_price": _decimal_payload(reference_price),
        "reference_price_source": reference_price_source,
        "status": ReadinessStatus.ATTENTION_REQUIRED if reasons else ReadinessStatus.OK,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("product policy viability check must normalize to an object")
    return normalized


def _passive_market_making_capacity_check(
    config: CoinbaseApplicationConfig,
    *,
    configured_product_ids: tuple[str, ...],
) -> dict[str, JsonValue]:
    if PASSIVE_MARKET_MAKING_STRATEGY_ID not in config.bot.strategies.strategy_ids:
        payload = {
            "evaluated": False,
            "reason": PolicyViabilityReason.STRATEGY_NOT_SELECTED,
            "status": ReadinessStatus.OK,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("PMM capacity payload must normalize to an object")
        return normalized

    parameters = config.bot.strategies.strategy_parameters.get(
        PASSIVE_MARKET_MAKING_STRATEGY_ID,
        {},
    )
    strategy = PassiveMarketMakingStrategy.from_parameters(parameters)
    risk = effective_risk_policy_config(config.bot)
    operator_policy = config.bot.strategies.operator_policy
    scoped_product_ids = (
        operator_policy.scope.products
        if operator_policy is not None
        else risk.allowed_products or configured_product_ids
    )
    available_scoped_product_ids = tuple(
        product_id for product_id in scoped_product_ids if product_id in configured_product_ids
    )
    evaluated_product_count = min(
        len(available_scoped_product_ids),
        strategy.max_products_per_evaluation,
    )
    allowed_sides = risk.allowed_sides or (OrderSide.BUY, OrderSide.SELL)
    side_count = sum(1 for side in (OrderSide.BUY, OrderSide.SELL) if side in allowed_sides)
    expected_new_staged_order_count = (
        evaluated_product_count
        * side_count
        * strategy.max_staged_release_count_per_side
    )
    max_open_orders = risk.max_open_orders
    open_order_capacity_ok = (
        max_open_orders is None
        or expected_new_staged_order_count <= max_open_orders
    )
    payload = {
        "available_scoped_product_count": len(available_scoped_product_ids),
        "evaluated": True,
        "evaluated_product_count": evaluated_product_count,
        "expected_new_staged_order_count": expected_new_staged_order_count,
        "max_open_orders": max_open_orders,
        "max_products_per_evaluation": strategy.max_products_per_evaluation,
        "max_staged_release_count_per_side": strategy.max_staged_release_count_per_side,
        "open_order_capacity_ok": open_order_capacity_ok,
        "scoped_product_count": len(scoped_product_ids),
        "side_count": side_count,
        "status": (
            ReadinessStatus.OK
            if open_order_capacity_ok
            else ReadinessStatus.ATTENTION_REQUIRED
        ),
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("PMM capacity payload must normalize to an object")
    return normalized


def _reference_price(product: ProductMetadata) -> tuple[Decimal | None, str | None]:
    raw = product.raw
    for field_name in REFERENCE_PRICE_FIELDS:
        price = _decimal_or_none(raw.get(field_name) if isinstance(raw, Mapping) else None)
        if price is not None and price > 0:
            return price, field_name
    return None, None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _decimal_payload(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None
