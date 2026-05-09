from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from actions.gateway import PlaceOrderIntent
from core.enums import (
    MarketDataKind,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderSide,
    OrderType,
    StrategyManagerGate,
    StrategyManagerSkipReason,
)
from core.errors import StrategyContractError, StrategyInputUnavailableError
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductMetadata
from strategies.execution_plans import quote_pair_prices
from strategies.harness import (
    StrategyDecision,
    StrategySnapshot,
    strategy_action_id,
    strategy_staged_release_intents,
)
from strategies.parameters import (
    decimal_parameter,
    positive_int_parameter,
    reject_unknown_parameters,
)


PASSIVE_MARKET_MAKING_STRATEGY_ID = "passive-market-making"
_BPS_DENOMINATOR = Decimal("10000")


@dataclass(frozen=True)
class PassiveMarketMakingStrategy:
    target_notional_usd: Decimal = Decimal("5")
    half_spread_bps: Decimal = Decimal("50")
    max_products_per_evaluation: int = 1
    max_staged_release_count_per_side: int = 1

    @classmethod
    def from_parameters(
        cls,
        parameters: Mapping[str, Any] | None = None,
    ) -> "PassiveMarketMakingStrategy":
        raw = {} if parameters is None else parameters
        if not isinstance(raw, Mapping):
            raise TypeError("passive-market-making parameters must be a mapping")
        reject_unknown_parameters(
            PASSIVE_MARKET_MAKING_STRATEGY_ID,
            raw,
            {
                "half_spread_bps",
                "max_products_per_evaluation",
                "max_staged_release_count_per_side",
                "target_notional_usd",
            },
        )
        defaults = cls()
        return cls(
            half_spread_bps=decimal_parameter(
                raw.get("half_spread_bps", defaults.half_spread_bps),
                "passive-market-making.half_spread_bps",
            ),
            max_products_per_evaluation=positive_int_parameter(
                raw.get("max_products_per_evaluation", defaults.max_products_per_evaluation),
                "passive-market-making.max_products_per_evaluation",
            ),
            max_staged_release_count_per_side=positive_int_parameter(
                raw.get(
                    "max_staged_release_count_per_side",
                    defaults.max_staged_release_count_per_side,
                ),
                "passive-market-making.max_staged_release_count_per_side",
            ),
            target_notional_usd=decimal_parameter(
                raw.get("target_notional_usd", defaults.target_notional_usd),
                "passive-market-making.target_notional_usd",
            ),
        )

    def __post_init__(self) -> None:
        _positive_decimal(self.target_notional_usd, "target_notional_usd")
        _positive_decimal(self.half_spread_bps, "half_spread_bps")
        if self.half_spread_bps >= _BPS_DENOMINATOR:
            raise ValueError("half_spread_bps must be less than 10000")
        if (
            not isinstance(self.max_products_per_evaluation, int)
            or isinstance(self.max_products_per_evaluation, bool)
        ):
            raise TypeError("max_products_per_evaluation must be an integer")
        if self.max_products_per_evaluation <= 0:
            raise ValueError("max_products_per_evaluation must be positive")
        if (
            not isinstance(self.max_staged_release_count_per_side, int)
            or isinstance(self.max_staged_release_count_per_side, bool)
        ):
            raise TypeError("max_staged_release_count_per_side must be an integer")
        if self.max_staged_release_count_per_side <= 0:
            raise ValueError("max_staged_release_count_per_side must be positive")

    @property
    def strategy_id(self) -> str:
        return PASSIVE_MARKET_MAKING_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")

        metadata: dict[str, Any] = {
            "created_quotes": [],
            "half_spread_bps": str(self.half_spread_bps),
            "max_products_per_evaluation": self.max_products_per_evaluation,
            "max_staged_release_count_per_side": self.max_staged_release_count_per_side,
            "skipped_products": [],
            "skipped_sides": [],
            "target_notional_usd": str(self.target_notional_usd),
        }
        policy = snapshot.operator_policy
        if policy is None:
            metadata["passive_market_making_gate"] = StrategyManagerGate.OPERATOR_POLICY_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))
        if not policy.staged_or_hidden_release.enabled:
            metadata["passive_market_making_gate"] = StrategyManagerGate.STAGED_RELEASE_DISABLED.value
            return StrategyDecision(metadata=_metadata(metadata))
        if (
            policy.order_behavior.default_order_type != OrderType.LIMIT
            or not policy.order_behavior.post_only
            or not policy.market_data_requirements.require_order_book
        ):
            metadata["passive_market_making_gate"] = (
                StrategyManagerGate.PASSIVE_MARKET_MAKING_POLICY_UNSAFE.value
            )
            return StrategyDecision(metadata=_metadata(metadata))
        if snapshot.product_catalog is None:
            metadata["passive_market_making_gate"] = StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))

        intents: list[PlaceOrderIntent] = []
        for product_id in policy.scope.products[: self.max_products_per_evaluation]:
            freshness = snapshot.market_data_freshness(
                data_kind=MarketDataKind.ORDER_BOOK,
                max_age=policy.market_data_requirements.max_order_book_age,
                product_id=product_id,
            )
            if not freshness.is_ok:
                metadata["skipped_products"].append(
                    {
                        "freshness": freshness.to_payload(),
                        "product_id": product_id,
                        "reason": StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value,
                    }
                )
                continue
            try:
                product = snapshot.product_catalog.require(product_id)
            except KeyError:
                metadata["skipped_products"].append(
                    {
                        "product_id": product_id,
                        "reason": StrategyManagerSkipReason.STRATEGY_INPUT_UNAVAILABLE.value,
                    }
                )
                continue

            book = snapshot.projection.order_book(product_id)
            quote_plan = _quote_plan(
                best_ask=book.best_ask_price if book is not None else None,
                best_bid=book.best_bid_price if book is not None else None,
                half_spread_bps=self.half_spread_bps,
                product=product,
                target_notional_usd=min(
                    self.target_notional_usd,
                    policy.risk_limits.max_order_notional_usd,
                    policy.staged_or_hidden_release.max_visible_notional_usd,
                ),
            )
            if quote_plan is None:
                metadata["skipped_products"].append(
                    {
                        "product_id": product_id,
                        "reason": StrategyManagerSkipReason.MARKET_DATA_INVALID.value,
                    }
                )
                continue

            for side in (OrderSide.BUY, OrderSide.SELL):
                if side not in policy.risk_limits.allowed_sides:
                    metadata["skipped_sides"].append(
                        {
                            "product_id": product_id,
                            "reason": StrategyManagerSkipReason.SIDE_OUTSIDE_OPERATOR_POLICY.value,
                            "side": side.value,
                        }
                    )
                    continue
                if _has_active_passive_quote(snapshot, product_id=product_id, side=side):
                    metadata["skipped_sides"].append(
                        {
                            "product_id": product_id,
                            "reason": StrategyManagerSkipReason.ACTIVE_QUOTE_EXISTS.value,
                            "side": side.value,
                        }
                    )
                    continue
                limit_price = quote_plan.bid_price if side == OrderSide.BUY else quote_plan.ask_price
                try:
                    side_intents = _staged_quote_intents(
                        snapshot=snapshot,
                        product=product,
                        product_id=product_id,
                        side=side,
                        limit_price=limit_price,
                        max_staged_release_count=self.max_staged_release_count_per_side,
                        size=quote_plan.size,
                        quote_plan=quote_plan,
                        half_spread_bps=self.half_spread_bps,
                    )
                except (StrategyContractError, StrategyInputUnavailableError) as exc:
                    metadata["skipped_sides"].append(
                        {
                            "message": str(exc),
                            "product_id": product_id,
                            "reason": (
                                StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value
                                if isinstance(exc, StrategyContractError)
                                else StrategyManagerSkipReason.STRATEGY_INPUT_UNAVAILABLE.value
                            ),
                            "side": side.value,
                        }
                    )
                    continue
                duplicate_action_ids = tuple(
                    intent.action_id
                    for intent in side_intents
                    if intent.action_id in snapshot.projection.actions
                )
                if duplicate_action_ids:
                    metadata["skipped_sides"].append(
                        {
                            "action_ids": list(duplicate_action_ids),
                            "product_id": product_id,
                            "reason": StrategyManagerSkipReason.ACTIVE_QUOTE_EXISTS.value,
                            "side": side.value,
                        }
                    )
                    continue
                metadata["created_quotes"].append(
                    {
                        "action_ids": [intent.action_id for intent in side_intents],
                        "limit_price": str(limit_price),
                        "product_id": product_id,
                        "side": side.value,
                        "size": str(quote_plan.size),
                    }
                )
                intents.extend(side_intents)

        return StrategyDecision(intents=tuple(intents), metadata=_metadata(metadata))


@dataclass(frozen=True)
class _QuotePlan:
    ask_price: Decimal
    bid_price: Decimal
    midpoint: Decimal
    size: Decimal


def _quote_plan(
    *,
    best_ask: str | None,
    best_bid: str | None,
    half_spread_bps: Decimal,
    product: ProductMetadata,
    target_notional_usd: Decimal,
) -> _QuotePlan | None:
    bid = _positive_decimal_or_none(best_bid)
    ask = _positive_decimal_or_none(best_ask)
    if bid is None or ask is None or bid >= ask:
        return None
    midpoint = (bid + ask) / Decimal("2")
    try:
        prices = quote_pair_prices(
            product,
            midpoint=midpoint,
            spread_bps=half_spread_bps * Decimal("2"),
        )
    except ValueError:
        return None
    if not prices.is_ok or prices.bid_price is None or prices.ask_price is None:
        return None
    bid_price = prices.bid_price
    ask_price = prices.ask_price
    if bid_price <= 0 or ask_price <= 0 or bid_price >= ask_price:
        return None
    size = _target_size_for_notional(
        product=product,
        reference_price=midpoint,
        target_notional_usd=target_notional_usd,
    )
    if size is None:
        return None
    return _QuotePlan(
        ask_price=ask_price,
        bid_price=bid_price,
        midpoint=midpoint,
        size=size,
    )


def _staged_quote_intents(
    *,
    snapshot: StrategySnapshot,
    product: ProductMetadata,
    product_id: str,
    side: OrderSide,
    limit_price: Decimal,
    max_staged_release_count: int,
    size: Decimal,
    quote_plan: _QuotePlan,
    half_spread_bps: Decimal,
) -> tuple[PlaceOrderIntent, ...]:
    policy = snapshot.operator_policy
    assert policy is not None
    decision = snapshot.plan_staged_release_sizes(
        product_id=product_id,
        total_size=size,
        limit_price=limit_price,
        max_release_count=max_staged_release_count,
    )
    if not decision.accepted:
        raise StrategyContractError(
            "passive market making staged sizing decision rejected",
            context={
                "product_id": product_id,
                "side": side.value,
                "sizing_decision": decision.to_payload(),
            },
        )
    identity = {
        "ask_price": str(quote_plan.ask_price),
        "bid_price": str(quote_plan.bid_price),
        "midpoint": str(quote_plan.midpoint),
        "product_id": product_id,
        "side": side.value,
        "size": str(size),
    }
    base_intent = PlaceOrderIntent(
        action_id=strategy_action_id(PASSIVE_MARKET_MAKING_STRATEGY_ID, "quote-base", identity),
        leverage=(
            str(policy.order_behavior.default_leverage)
            if policy.order_behavior.default_leverage is not None
            else None
        ),
        limit_price=str(limit_price),
        lineage_relation=OrderLineageRelation.ROOT,
        logical_order_id=strategy_action_id(PASSIVE_MARKET_MAKING_STRATEGY_ID, "quote-logical", identity),
        margin_type=policy.order_behavior.default_margin_type,
        metadata={
            "passive_market_making": {
                "ask_price": str(quote_plan.ask_price),
                "bid_price": str(quote_plan.bid_price),
                "half_spread_bps": str(half_spread_bps),
                "midpoint": str(quote_plan.midpoint),
                "product_id": product.product_id,
                "side": side.value,
            },
        },
        order_type=policy.order_behavior.default_order_type,
        post_only=policy.order_behavior.post_only,
        product_id=product_id,
        reduce_only=policy.risk_limits.reduce_only_first,
        side=side,
        size=str(size),
        time_in_force=policy.order_behavior.time_in_force,
    )
    return strategy_staged_release_intents(
        PASSIVE_MARKET_MAKING_STRATEGY_ID,
        "quote",
        base_intent,
        decision,
        identity,
    )


def _has_active_passive_quote(snapshot: StrategySnapshot, *, product_id: str, side: OrderSide) -> bool:
    for quote in snapshot.projection.passive_market_making_quotes:
        if quote.product_id != product_id or quote.side != side:
            continue
        if not quote.released:
            return True
        release_placement = (
            snapshot.projection.placements_by_id.get(quote.release_placement_id)
            if quote.release_placement_id is not None
            else None
        )
        release_order = (
            snapshot.projection.orders_by_action_id.get(release_placement.action_id)
            if release_placement is not None and release_placement.action_id is not None
            else None
        )
        if release_order is not None and release_order.lifecycle_status == OrderLifecycleStatus.OPEN:
            return True
    return False


def _target_size_for_notional(
    *,
    product: ProductMetadata,
    reference_price: Decimal,
    target_notional_usd: Decimal,
) -> Decimal | None:
    desired_size = _floor_to_increment(
        target_notional_usd / (reference_price * product.notional_multiplier),
        product.base_increment,
    )
    minimum_size = product.minimum_valid_size(reference_price) or Decimal("0")
    size = max(desired_size, minimum_size)
    if size <= 0:
        return None
    return size


def _floor_to_increment(value: Decimal, increment: Decimal | None) -> Decimal:
    if increment is None or increment <= 0:
        return value
    return ((value // increment) * increment).quantize(increment)


def _positive_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _positive_decimal_or_none(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _metadata(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(raw)
    if not isinstance(normalized, dict):
        raise TypeError("passive market making metadata must normalize to an object")
    return normalized
