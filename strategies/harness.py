from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from actions.gateway import (
    ActionCommand,
    ActionExecutor,
    ActionGateway,
    ActionReceipt,
    CancelOrderIntent,
    PlaceOrderIntent,
)
from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ActionType,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    IncrementRoundingMode,
    MarketDataKind,
    OperatorPolicyDistanceType,
    OperatorPolicyFollowupSizeMode,
    OperatorPolicyPermission,
    OperatorPolicyVenue,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OrderType,
    ProductVenue,
    ScheduledSliceStatus,
    StrategyEvaluationStatus,
    StrategyInputStatus,
)
from core.errors import (
    StrategyActionSubmissionError,
    StrategyContractError,
    StrategyInputUnavailableError,
    exception_to_error_payload,
)
from core.json_tools import JsonValue, canonical_json, normalize_json
from products.catalog import ProductCatalog, ProductMetadata
from products.capabilities import (
    ProductCapabilities,
    VenueCapabilities,
    product_capabilities as catalog_product_capabilities,
    venue_capabilities as normalized_venue_capabilities,
)
from projections.state import OrderSnapshot, SourceOfTruthProjection
from strategies.exposure import (
    OrderCapacity,
    ProductExposure,
    order_capacity,
    product_exposure,
)
from strategies.execution_plans import (
    LadderPlan,
    ladder_plan as product_ladder_plan,
    quote_pair_prices,
)
from strategies.market_data import (
    BestBidAsk,
    LatestMarketTrade,
    MarketCandles,
    MarketMidpoint,
    MarketOrderBookStats,
    MarketSpread,
    MarketWindowStats,
    RollingTradeCount,
    RollingTradeVolume,
    TradeWindow,
    best_bid_ask,
    candles,
    latest_trade,
    market_window_stats,
    midpoint,
    order_book_stats,
    rolling_trade_count,
    rolling_trade_volume,
    spread,
    trade_window,
)
from strategies.product_rules import (
    ProductRuleProposal,
    ProductRuleValidation,
    price_tick_proposal as product_price_tick_proposal,
    size_increment_proposal as product_size_increment_proposal,
    validate_limit_price as product_validate_limit_price,
    validate_notional as product_validate_notional,
    validate_order_size as product_validate_order_size,
)
from strategies.schedules import (
    ScheduledSlicePlan,
    scheduled_slice_sizes,
)

if TYPE_CHECKING:
    from orders.sizing import AmountInput, OrderSizingDecision
    from strategies.operator_policy import OperatorPolicy


NOOP_STRATEGY_ID = "noop"
_IDENTIFIER_COMPONENT_LENGTH = 16
_IDENTIFIER_DIGEST_LENGTH = 20
StrategyIntent = ActionCommand | PlaceOrderIntent | CancelOrderIntent


class Strategy(Protocol):
    @property
    def strategy_id(self) -> str:
        ...

    def evaluate(self, snapshot: "StrategySnapshot") -> "StrategyDecision":
        ...


@dataclass(frozen=True)
class StrategySnapshot:
    as_of_sequence: int
    evaluated_at: datetime
    execution_mode: ExecutionMode
    ledger_path: Path
    projection: SourceOfTruthProjection
    operator_policy: OperatorPolicy | None = None
    product_catalog: ProductCatalog | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.as_of_sequence, int) or self.as_of_sequence < 0:
            raise ValueError("as_of_sequence must be a non-negative integer")
        if not isinstance(self.evaluated_at, datetime):
            raise TypeError("evaluated_at must be a datetime")
        if not isinstance(self.execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")
        if not isinstance(self.ledger_path, Path):
            raise TypeError("ledger_path must be a pathlib.Path")
        if not isinstance(self.projection, SourceOfTruthProjection):
            raise TypeError("projection must be a SourceOfTruthProjection")
        if self.operator_policy is not None:
            from strategies.operator_policy import OperatorPolicy

            if not isinstance(self.operator_policy, OperatorPolicy):
                raise TypeError("operator_policy must be an OperatorPolicy when provided")
        _metadata_payload(self.metadata)

    def market_data_freshness(
        self,
        *,
        data_kind: MarketDataKind,
        max_age: timedelta,
        product_id: str,
    ) -> "StrategyInputFreshness":
        if not isinstance(data_kind, MarketDataKind):
            raise TypeError("data_kind must be a MarketDataKind")
        if not isinstance(max_age, timedelta):
            raise TypeError("max_age must be a datetime.timedelta")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        if not isinstance(product_id, str) or not product_id:
            raise ValueError("product_id must be a non-empty string")

        observed_snapshot = self._market_data_snapshot(data_kind=data_kind, product_id=product_id)
        max_age_seconds = max_age.total_seconds()
        if observed_snapshot is None:
            return StrategyInputFreshness(
                data_kind=data_kind,
                max_age_seconds=max_age_seconds,
                product_id=product_id,
                status=StrategyInputStatus.MISSING,
            )

        observed_at = getattr(observed_snapshot, "observed_at", None)
        sequence = getattr(observed_snapshot, "sequence", None)
        if not isinstance(observed_at, datetime):
            return StrategyInputFreshness(
                data_kind=data_kind,
                max_age_seconds=max_age_seconds,
                product_id=product_id,
                sequence=sequence if isinstance(sequence, int) else None,
                status=StrategyInputStatus.MISSING,
            )

        age_seconds = (self.evaluated_at - observed_at).total_seconds()
        return StrategyInputFreshness(
            age_seconds=age_seconds,
            data_kind=data_kind,
            max_age_seconds=max_age_seconds,
            observed_at=observed_at,
            product_id=product_id,
            sequence=sequence if isinstance(sequence, int) else None,
            status=(
                StrategyInputStatus.OK
                if age_seconds <= max_age_seconds
                else StrategyInputStatus.STALE
            ),
        )

    def best_bid_ask(self, product_id: str) -> BestBidAsk:
        return best_bid_ask(self.projection, product_id)

    def midpoint(self, product_id: str) -> MarketMidpoint:
        return midpoint(self.projection, product_id)

    def spread(self, product_id: str) -> MarketSpread:
        return spread(self.projection, product_id)

    def order_book_stats(
        self,
        product_id: str,
        *,
        levels: int | None = None,
        max_distance_bps: "AmountInput | None" = None,
    ) -> MarketOrderBookStats:
        return order_book_stats(
            self.projection,
            product_id,
            levels=levels,
            max_distance_bps=max_distance_bps,
            product_catalog=self.product_catalog,
        )

    def latest_trade(self, product_id: str) -> LatestMarketTrade:
        return latest_trade(self.projection, product_id)

    def trade_window(self, product_id: str, *, lookback: timedelta) -> TradeWindow:
        return trade_window(
            self.projection,
            as_of=self.evaluated_at,
            lookback=lookback,
            product_id=product_id,
        )

    def market_window_stats(self, product_id: str, *, lookback: timedelta) -> MarketWindowStats:
        return market_window_stats(
            self.projection,
            as_of=self.evaluated_at,
            lookback=lookback,
            product_id=product_id,
        )

    def candles(
        self,
        product_id: str,
        *,
        interval: timedelta,
        lookback: timedelta,
    ) -> MarketCandles:
        return candles(
            self.projection,
            as_of=self.evaluated_at,
            interval=interval,
            lookback=lookback,
            product_id=product_id,
        )

    def rolling_trade_volume(
        self,
        product_id: str,
        *,
        lookback: timedelta,
    ) -> RollingTradeVolume:
        return rolling_trade_volume(
            self.projection,
            as_of=self.evaluated_at,
            lookback=lookback,
            product_id=product_id,
        )

    def rolling_trade_count(self, product_id: str, *, lookback: timedelta) -> RollingTradeCount:
        return rolling_trade_count(
            self.projection,
            as_of=self.evaluated_at,
            lookback=lookback,
            product_id=product_id,
        )

    def open_orders(
        self,
        *,
        product_id: str | None = None,
        side: OrderSide | None = None,
    ) -> tuple[OrderSnapshot, ...]:
        if product_id is not None and (not isinstance(product_id, str) or not product_id):
            raise ValueError("product_id must be a non-empty string when provided")
        if side is not None and not isinstance(side, OrderSide):
            raise TypeError("side must be an OrderSide when provided")
        return tuple(
            order
            for order in self.projection.open_orders
            if (product_id is None or order.product_id == product_id)
            and (side is None or order.side == side)
        )

    def product_exposure(self, product_id: str) -> ProductExposure:
        return product_exposure(
            self.projection,
            product_id,
            product_catalog=self.product_catalog,
        )

    def order_capacity(
        self,
        product_id: str,
        *,
        side: OrderSide | None = None,
    ) -> OrderCapacity:
        return order_capacity(
            self.projection,
            product_id,
            now=self.evaluated_at,
            operator_policy=self.operator_policy,
            product_catalog=self.product_catalog,
            side=side,
        )

    def product_rules(self, product_id: str) -> ProductMetadata | None:
        if not isinstance(product_id, str) or not product_id:
            raise ValueError("product_id must be a non-empty string")
        if self.product_catalog is None:
            return None
        return self.product_catalog.get(product_id)

    def venue_capabilities(
        self,
        venue: ProductVenue | OperatorPolicyVenue | str,
    ) -> VenueCapabilities:
        return normalized_venue_capabilities(venue)

    def product_capabilities(self, product_id: str) -> ProductCapabilities:
        return catalog_product_capabilities(self.product_catalog, product_id)

    def notional(self, *, product_id: str, size: "AmountInput", price: "AmountInput") -> Decimal:
        if not isinstance(product_id, str) or not product_id:
            raise ValueError("product_id must be a non-empty string")
        if self.product_catalog is None:
            raise StrategyInputUnavailableError(
                "product catalog is required to calculate notional",
                context={"product_id": product_id},
            )
        try:
            product = self.product_catalog.require(product_id)
        except KeyError as exc:
            raise StrategyInputUnavailableError(
                "product metadata is required to calculate notional",
                context={"product_id": product_id},
            ) from exc
        resolved = product.notional(
            _positive_decimal_input(size, "size"),
            _positive_decimal_input(price, "price"),
        )
        if resolved is None:
            raise StrategyContractError(
                "notional could not be calculated",
                context={"product_id": product_id},
            )
        return resolved

    def validate_order_size(
        self,
        *,
        product_id: str,
        size: "AmountInput",
    ) -> ProductRuleValidation:
        return product_validate_order_size(
            self._require_product_for_rules(product_id, purpose="validate order size"),
            size,
        )

    def validate_limit_price(
        self,
        *,
        price: "AmountInput",
        product_id: str,
    ) -> ProductRuleValidation:
        return product_validate_limit_price(
            self._require_product_for_rules(product_id, purpose="validate limit price"),
            price,
        )

    def validate_notional(
        self,
        *,
        price: "AmountInput",
        product_id: str,
        size: "AmountInput",
    ) -> ProductRuleValidation:
        return product_validate_notional(
            self._require_product_for_rules(product_id, purpose="validate notional"),
            price=price,
            size=size,
        )

    def price_tick_proposal(
        self,
        *,
        mode: IncrementRoundingMode,
        price: "AmountInput",
        product_id: str,
    ) -> ProductRuleProposal:
        return product_price_tick_proposal(
            self._require_product_for_rules(product_id, purpose="propose price tick"),
            mode=mode,
            price=price,
        )

    def size_increment_proposal(
        self,
        *,
        mode: IncrementRoundingMode,
        product_id: str,
        size: "AmountInput",
    ) -> ProductRuleProposal:
        return product_size_increment_proposal(
            self._require_product_for_rules(product_id, purpose="propose size increment"),
            mode=mode,
            size=size,
        )

    def ladder_plan(
        self,
        *,
        anchor_price: "AmountInput",
        levels: int,
        product_id: str,
        side: OrderSide,
        size_per_level: "AmountInput",
        step_bps: "AmountInput",
    ) -> LadderPlan:
        return product_ladder_plan(
            self._require_product_for_rules(product_id, purpose="create ladder plan"),
            anchor_price=anchor_price,
            levels=levels,
            side=side,
            size_per_level=size_per_level,
            step_bps=step_bps,
        )

    def plan_staged_release_sizes(
        self,
        *,
        product_id: str,
        total_size: "AmountInput",
        limit_price: "AmountInput",
        max_visible_notional: "AmountInput | None" = None,
        max_release_count: int | None = None,
    ) -> "OrderSizingDecision":
        if not isinstance(product_id, str) or not product_id:
            raise ValueError("product_id must be a non-empty string")
        if self.product_catalog is None:
            raise StrategyInputUnavailableError(
                "product catalog is required to plan staged release sizes",
                context={"product_id": product_id},
            )
        try:
            product = self.product_catalog.require(product_id)
        except KeyError as exc:
            raise StrategyInputUnavailableError(
                "product metadata is required to plan staged release sizes",
                context={"product_id": product_id},
            ) from exc
        if self.operator_policy is not None and product_id not in self.operator_policy.scope.products:
            raise StrategyContractError(
                "product_id is outside operator policy scope",
                context={"product_id": product_id},
            )

        visible_cap = max_visible_notional
        if visible_cap is None:
            if self.operator_policy is None:
                raise StrategyContractError(
                    "max_visible_notional is required without an operator policy",
                    context={"product_id": product_id},
                )
            if not self.operator_policy.staged_or_hidden_release.enabled:
                raise StrategyContractError(
                    "operator policy staged_or_hidden_release is disabled",
                    context={"product_id": product_id},
                )
            visible_cap = self.operator_policy.staged_or_hidden_release.max_visible_notional_usd

        from orders.sizing import LineageSizingPolicy

        return LineageSizingPolicy.from_values(product=product).staged_release_sizes(
            limit_price=limit_price,
            max_release_count=max_release_count,
            max_visible_notional=visible_cap,
            total_size=total_size,
        )

    def quote_pair_intents(
        self,
        *,
        product_id: str,
        size: "AmountInput",
        spread_bps: "AmountInput",
        strategy_id: str,
        max_release_count: int | None = None,
        placement_kind: OrderPlacementKind = OrderPlacementKind.STAGED_RELEASE,
        purpose: str = "quote-pair",
    ) -> tuple[PlaceOrderIntent, ...]:
        if not isinstance(strategy_id, str) or not strategy_id:
            raise ValueError("strategy_id must be a non-empty string")
        if not isinstance(purpose, str) or not purpose:
            raise ValueError("purpose must be a non-empty string")
        if not isinstance(placement_kind, OrderPlacementKind):
            raise TypeError("placement_kind must be an OrderPlacementKind")
        if placement_kind not in {OrderPlacementKind.INITIAL, OrderPlacementKind.STAGED_RELEASE}:
            raise StrategyContractError(
                "quote pair builder supports only initial or staged_release placement",
                context={"placement_kind": placement_kind.value},
            )

        policy = self.operator_policy
        if policy is None:
            raise StrategyContractError(
                "operator policy is required to create quote pair intents",
                context={"product_id": product_id},
            )
        if product_id not in policy.scope.products:
            raise StrategyContractError(
                "product_id is outside operator policy scope",
                context={"product_id": product_id},
            )
        if (
            OrderSide.BUY not in policy.risk_limits.allowed_sides
            or OrderSide.SELL not in policy.risk_limits.allowed_sides
        ):
            raise StrategyContractError(
                "quote pair intents require both buy and sell sides to be allowed",
                context={
                    "allowed_sides": [side.value for side in policy.risk_limits.allowed_sides],
                    "product_id": product_id,
                },
            )
        if policy.order_behavior.default_order_type != OrderType.LIMIT:
            raise StrategyContractError(
                "quote pair intents require limit order behavior",
                context={
                    "default_order_type": policy.order_behavior.default_order_type.value,
                    "product_id": product_id,
                },
            )
        if placement_kind == OrderPlacementKind.STAGED_RELEASE and not policy.staged_or_hidden_release.enabled:
            raise StrategyContractError(
                "quote pair staged release requires enabled staged_or_hidden_release policy",
                context={"product_id": product_id},
            )

        product = self._require_product_for_rules(product_id, purpose="create quote pair intents")
        book = self.order_book_stats(product_id, levels=1)
        if not book.is_ok or book.midpoint is None:
            raise StrategyInputUnavailableError(
                "order book midpoint is required to create quote pair intents",
                context={"order_book": book.to_payload(), "product_id": product_id},
            )

        try:
            price_plan = quote_pair_prices(
                product,
                midpoint=book.midpoint,
                spread_bps=spread_bps,
            )
        except ValueError as exc:
            raise StrategyContractError(
                "quote pair price inputs are invalid",
                context={"product_id": product_id},
            ) from exc
        if not price_plan.is_ok or price_plan.bid_price is None or price_plan.ask_price is None:
            raise StrategyContractError(
                "quote pair price proposal rejected",
                context={"price_plan": price_plan.to_payload(), "product_id": product_id},
            )

        resolved_size = _positive_decimal_input(size, "quote_pair.size")
        size_check = product_validate_order_size(product, resolved_size)
        if not size_check.is_ok:
            raise StrategyContractError(
                "quote pair size rejected by product rules",
                context={"product_id": product_id, "size_check": size_check.to_payload()},
            )

        intents: list[PlaceOrderIntent] = []
        for side, limit_price in (
            (OrderSide.BUY, price_plan.bid_price),
            (OrderSide.SELL, price_plan.ask_price),
        ):
            notional_check = product_validate_notional(
                product,
                price=limit_price,
                size=resolved_size,
            )
            if not notional_check.is_ok:
                raise StrategyContractError(
                    "quote pair notional rejected by product rules",
                    context={
                        "notional_check": notional_check.to_payload(),
                        "product_id": product_id,
                        "side": side.value,
                    },
                )
            if (
                notional_check.notional is not None
                and notional_check.notional > policy.risk_limits.max_order_notional_usd
            ):
                raise StrategyContractError(
                    "quote pair notional exceeds operator policy max order notional",
                    context={
                        "max_order_notional_usd": str(policy.risk_limits.max_order_notional_usd),
                        "notional": str(notional_check.notional),
                        "product_id": product_id,
                        "side": side.value,
                    },
                )

            identity = {
                "ask_price": str(price_plan.ask_price),
                "bid_price": str(price_plan.bid_price),
                "midpoint": str(price_plan.midpoint),
                "placement_kind": placement_kind.value,
                "product_id": product_id,
                "side": side.value,
                "size": str(resolved_size),
                "spread_bps": str(price_plan.spread_bps),
            }
            base_intent = PlaceOrderIntent(
                action_id=strategy_action_id(strategy_id, f"{purpose}-{side.value}", identity),
                idempotency_key=strategy_client_order_id(
                    strategy_id,
                    f"{purpose}-{side.value}",
                    identity,
                ),
                leverage=(
                    str(policy.order_behavior.default_leverage)
                    if policy.order_behavior.default_leverage is not None
                    else None
                ),
                limit_price=str(limit_price),
                lineage_relation=OrderLineageRelation.ROOT,
                logical_order_id=strategy_action_id(
                    strategy_id,
                    f"{purpose}-logical-{side.value}",
                    identity,
                ),
                margin_type=policy.order_behavior.default_margin_type,
                metadata={
                    "quote_pair": {
                        "ask_price": str(price_plan.ask_price),
                        "bid_price": str(price_plan.bid_price),
                        "midpoint": str(price_plan.midpoint),
                        "price_plan": price_plan.to_payload(),
                        "product_id": product_id,
                        "side": side.value,
                        "spread_bps": str(price_plan.spread_bps),
                    },
                },
                order_type=policy.order_behavior.default_order_type,
                placement_kind=(
                    OrderPlacementKind.INITIAL
                    if placement_kind == OrderPlacementKind.INITIAL
                    else None
                ),
                post_only=policy.order_behavior.post_only,
                product_id=product_id,
                reduce_only=policy.risk_limits.reduce_only_first,
                side=side,
                size=str(resolved_size),
                time_in_force=policy.order_behavior.time_in_force,
            )
            if placement_kind == OrderPlacementKind.STAGED_RELEASE:
                decision = self.plan_staged_release_sizes(
                    product_id=product_id,
                    total_size=resolved_size,
                    limit_price=limit_price,
                    max_release_count=max_release_count,
                )
                if not decision.accepted:
                    raise StrategyContractError(
                        "quote pair staged release sizing decision rejected",
                        context={
                            "product_id": product_id,
                            "side": side.value,
                            "sizing_decision": decision.to_payload(),
                        },
                    )
                intents.extend(
                    strategy_staged_release_intents(
                        strategy_id,
                        purpose,
                        base_intent,
                        decision,
                        identity,
                    )
                )
            else:
                intents.append(base_intent)

        existing_action_ids = tuple(
            intent.action_id
            for intent in intents
            if intent.action_id in self.projection.actions
        )
        if existing_action_ids:
            raise StrategyContractError(
                "quote pair action already exists",
                context={
                    "action_ids": list(existing_action_ids),
                    "product_id": product_id,
                },
            )
        return tuple(intents)

    def scheduled_slice_plan(
        self,
        *,
        interval: timedelta,
        product_id: str,
        slices: int,
        strategy_id: str,
        total_size: "AmountInput",
        purpose: str = "scheduled-slice",
        schedule_id: str = "default",
        start_at: datetime | None = None,
    ) -> ScheduledSlicePlan:
        if not isinstance(strategy_id, str) or not strategy_id:
            raise ValueError("strategy_id must be a non-empty string")
        if not isinstance(purpose, str) or not purpose:
            raise ValueError("purpose must be a non-empty string")
        if not isinstance(schedule_id, str) or not schedule_id:
            raise ValueError("schedule_id must be a non-empty string")
        if not isinstance(interval, timedelta):
            raise TypeError("interval must be a datetime.timedelta")
        if interval <= timedelta(0):
            raise ValueError("interval must be positive")
        if start_at is not None and not isinstance(start_at, datetime):
            raise TypeError("start_at must be a datetime when provided")
        if not isinstance(slices, int) or isinstance(slices, bool):
            raise TypeError("slices must be an integer")
        if slices <= 0:
            raise ValueError("slices must be positive")

        product = self._require_product_for_rules(product_id, purpose="create scheduled slice plan")
        total = _positive_decimal_input(total_size, "scheduled_slice.total_size")
        slice_sizes, size_failures, size_reasons = scheduled_slice_sizes(
            product,
            slices=slices,
            total_size=total,
        )
        if size_failures or size_reasons:
            return ScheduledSlicePlan(
                completed_slice_count=0,
                evaluated_at=self.evaluated_at,
                interval=interval,
                product_id=product_id,
                reasons=size_reasons,
                remaining_slice_count=slices,
                schedule_id=schedule_id,
                scheduled_start_at=start_at,
                size_failures=size_failures,
                slices=slices,
                status=ScheduledSliceStatus.BLOCKED,
                strategy_id=strategy_id,
                total_size=total,
            )

        expected_action_ids = tuple(
            strategy_action_id(
                strategy_id,
                purpose,
                _scheduled_slice_identity(
                    interval=interval,
                    product_id=product_id,
                    schedule_id=schedule_id,
                    slice_index=index,
                    slices=slices,
                    start_at=start_at,
                    total_size=total,
                ),
            )
            for index in range(1, slices + 1)
        )
        completed_action_ids: list[str] = []
        next_index: int | None = None
        for index, action_id in enumerate(expected_action_ids, start=1):
            action = self.projection.actions.get(action_id)
            if action is None:
                next_index = index
                break
            if action.status not in _SCHEDULED_SLICE_CONSUMED_STATUSES:
                return ScheduledSlicePlan(
                    completed_action_ids=tuple(completed_action_ids),
                    completed_slice_count=len(completed_action_ids),
                    evaluated_at=self.evaluated_at,
                    interval=interval,
                    product_id=product_id,
                    reasons=(f"slice {index} action is {action.status.value}",),
                    remaining_slice_count=slices - len(completed_action_ids),
                    schedule_id=schedule_id,
                    scheduled_start_at=start_at,
                    slices=slices,
                    status=ScheduledSliceStatus.BLOCKED,
                    strategy_id=strategy_id,
                    suggested_action_id=action_id,
                    total_size=total,
                )
            completed_action_ids.append(action_id)

        if next_index is None:
            return ScheduledSlicePlan(
                completed_action_ids=tuple(completed_action_ids),
                completed_slice_count=slices,
                evaluated_at=self.evaluated_at,
                interval=interval,
                product_id=product_id,
                remaining_slice_count=0,
                schedule_id=schedule_id,
                scheduled_start_at=start_at,
                slices=slices,
                status=ScheduledSliceStatus.COMPLETE,
                strategy_id=strategy_id,
                total_size=total,
            )

        out_of_order_action_ids = tuple(
            action_id
            for action_id in expected_action_ids[next_index:]
            if action_id in self.projection.actions
        )
        if out_of_order_action_ids:
            return ScheduledSlicePlan(
                completed_action_ids=tuple(completed_action_ids),
                completed_slice_count=len(completed_action_ids),
                evaluated_at=self.evaluated_at,
                interval=interval,
                product_id=product_id,
                reasons=("later slice action exists before the next expected slice",),
                remaining_slice_count=slices - len(completed_action_ids),
                schedule_id=schedule_id,
                scheduled_start_at=start_at,
                slices=slices,
                status=ScheduledSliceStatus.BLOCKED,
                strategy_id=strategy_id,
                suggested_action_id=out_of_order_action_ids[0],
                total_size=total,
            )

        next_due_at = _scheduled_slice_due_at(
            completed_action_ids=tuple(completed_action_ids),
            interval=interval,
            projection=self.projection,
            start_at=start_at,
            evaluated_at=self.evaluated_at,
            slice_index=next_index,
        )
        if next_due_at is None:
            return ScheduledSlicePlan(
                completed_action_ids=tuple(completed_action_ids),
                completed_slice_count=len(completed_action_ids),
                evaluated_at=self.evaluated_at,
                interval=interval,
                product_id=product_id,
                reasons=("prior slice action time is unavailable",),
                remaining_slice_count=slices - len(completed_action_ids),
                schedule_id=schedule_id,
                scheduled_start_at=start_at,
                slices=slices,
                status=ScheduledSliceStatus.BLOCKED,
                strategy_id=strategy_id,
                total_size=total,
            )

        identity = _scheduled_slice_identity(
            interval=interval,
            product_id=product_id,
            schedule_id=schedule_id,
            slice_index=next_index,
            slices=slices,
            start_at=start_at,
            total_size=total,
        )
        due_in_seconds = max(
            0.0,
            (next_due_at - self.evaluated_at).total_seconds(),
        )
        return ScheduledSlicePlan(
            completed_action_ids=tuple(completed_action_ids),
            completed_slice_count=len(completed_action_ids),
            due_in_seconds=due_in_seconds,
            evaluated_at=self.evaluated_at,
            interval=interval,
            next_due_at=next_due_at,
            product_id=product_id,
            remaining_slice_count=slices - len(completed_action_ids),
            schedule_id=schedule_id,
            scheduled_start_at=start_at,
            slice_index=next_index,
            slice_size=slice_sizes[next_index - 1],
            slices=slices,
            status=(
                ScheduledSliceStatus.DUE
                if self.evaluated_at >= next_due_at
                else ScheduledSliceStatus.NOT_DUE
            ),
            strategy_id=strategy_id,
            suggested_action_id=strategy_action_id(strategy_id, purpose, identity),
            suggested_client_order_id=strategy_client_order_id(strategy_id, purpose, identity),
            total_size=total,
        )

    def _require_product_for_rules(self, product_id: str, *, purpose: str) -> ProductMetadata:
        if not isinstance(product_id, str) or not product_id:
            raise ValueError("product_id must be a non-empty string")
        if self.product_catalog is None:
            raise StrategyInputUnavailableError(
                f"product catalog is required to {purpose}",
                context={"product_id": product_id},
            )
        try:
            return self.product_catalog.require(product_id)
        except KeyError as exc:
            raise StrategyInputUnavailableError(
                f"product metadata is required to {purpose}",
                context={"product_id": product_id},
            ) from exc

    def _market_data_snapshot(self, *, data_kind: MarketDataKind, product_id: str) -> Any:
        if data_kind == MarketDataKind.TICKER:
            return self.projection.latest_ticker(product_id)
        if data_kind == MarketDataKind.ORDER_BOOK:
            return self.projection.order_book(product_id)
        if data_kind == MarketDataKind.TRADE:
            trades = self.projection.market_trades_for_product(product_id)
            if not trades:
                return None
            return max(trades, key=lambda trade: trade.sequence)
        raise ValueError(f"unsupported market data kind: {data_kind.value}")


@dataclass(frozen=True)
class StrategyInputFreshness:
    product_id: str
    data_kind: MarketDataKind
    status: StrategyInputStatus
    max_age_seconds: float
    age_seconds: float | None = None
    observed_at: datetime | None = None
    sequence: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.product_id, str) or not self.product_id:
            raise ValueError("product_id must be a non-empty string")
        if not isinstance(self.data_kind, MarketDataKind):
            raise TypeError("data_kind must be a MarketDataKind")
        if not isinstance(self.status, StrategyInputStatus):
            raise TypeError("status must be a StrategyInputStatus")
        if (
            not isinstance(self.max_age_seconds, int | float)
            or isinstance(self.max_age_seconds, bool)
        ):
            raise TypeError("max_age_seconds must be numeric")
        if self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        if self.age_seconds is not None and (
            not isinstance(self.age_seconds, int | float) or isinstance(self.age_seconds, bool)
        ):
            raise TypeError("age_seconds must be numeric when provided")
        if self.observed_at is not None and not isinstance(self.observed_at, datetime):
            raise TypeError("observed_at must be a datetime when provided")
        if self.sequence is not None and (
            not isinstance(self.sequence, int) or isinstance(self.sequence, bool) or self.sequence < 0
        ):
            raise ValueError("sequence must be a non-negative integer when provided")

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyInputStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "age_seconds": self.age_seconds,
            "data_kind": self.data_kind,
            "is_ok": self.is_ok,
            "max_age_seconds": self.max_age_seconds,
            "observed_at": self.observed_at,
            "product_id": self.product_id,
            "sequence": self.sequence,
            "status": self.status,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Strategy input freshness payload must normalize to an object")
        return normalized


@dataclass(frozen=True)
class StrategyInputRequirement:
    product_id: str
    data_kind: MarketDataKind
    max_age: timedelta

    def __post_init__(self) -> None:
        if not isinstance(self.product_id, str) or not self.product_id:
            raise ValueError("product_id must be a non-empty string")
        if not isinstance(self.data_kind, MarketDataKind):
            raise TypeError("data_kind must be a MarketDataKind")
        if not isinstance(self.max_age, timedelta):
            raise TypeError("max_age must be a datetime.timedelta")
        if self.max_age <= timedelta(0):
            raise ValueError("max_age must be positive")

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyInputFreshness:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")
        return snapshot.market_data_freshness(
            data_kind=self.data_kind,
            max_age=self.max_age,
            product_id=self.product_id,
        )

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "data_kind": self.data_kind,
            "max_age_seconds": self.max_age.total_seconds(),
            "product_id": self.product_id,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Strategy input requirement payload must normalize to an object")
        return normalized


@dataclass(frozen=True)
class StrategyDecision:
    intents: tuple[StrategyIntent, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.intents, tuple):
            raise TypeError("intents must be a tuple")
        for intent in self.intents:
            if not isinstance(intent, (ActionCommand, PlaceOrderIntent, CancelOrderIntent)):
                raise TypeError("intents must contain strategy intent values")
        _metadata_payload(self.metadata)

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "intent_count": len(self.intents),
            "metadata": _metadata_payload(self.metadata),
        }


@dataclass(frozen=True)
class StrategyEvaluationReceipt:
    strategy_id: str
    status: StrategyEvaluationStatus
    started_sequence: int
    closed_sequence: int
    action_receipts: tuple[ActionReceipt, ...] = ()
    error_sequence: int | None = None
    input_freshness: tuple[StrategyInputFreshness, ...] = ()
    intent_count: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.strategy_id:
            raise ValueError("strategy_id is required")
        if not isinstance(self.status, StrategyEvaluationStatus):
            raise TypeError("status must be a StrategyEvaluationStatus")
        if self.started_sequence <= 0:
            raise ValueError("started_sequence must be positive")
        if self.closed_sequence <= 0:
            raise ValueError("closed_sequence must be positive")
        if self.error_sequence is not None and self.error_sequence <= 0:
            raise ValueError("error_sequence must be positive when provided")
        if self.intent_count < 0:
            raise ValueError("intent_count must not be negative")
        if not isinstance(self.action_receipts, tuple):
            raise TypeError("action_receipts must be a tuple")
        if not isinstance(self.input_freshness, tuple):
            raise TypeError("input_freshness must be a tuple")
        for freshness in self.input_freshness:
            if not isinstance(freshness, StrategyInputFreshness):
                raise TypeError("input_freshness must contain StrategyInputFreshness values")
        _metadata_payload(self.metadata)

    @property
    def submitted_action_count(self) -> int:
        return len(self.action_receipts)

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "action_receipts": [_action_receipt_payload(receipt) for receipt in self.action_receipts],
            "closed_sequence": self.closed_sequence,
            "error_sequence": self.error_sequence,
            "input_freshness": [freshness.to_payload() for freshness in self.input_freshness],
            "intent_count": self.intent_count,
            "metadata": _metadata_payload(self.metadata),
            "started_sequence": self.started_sequence,
            "status": self.status.value,
            "strategy_id": self.strategy_id,
            "submitted_action_count": self.submitted_action_count,
        }


class NoOpStrategy:
    @property
    def strategy_id(self) -> str:
        return NOOP_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        del snapshot
        return StrategyDecision()


class StrategyEvaluationTask:
    def __init__(
        self,
        core: AuditCore,
        *,
        action_gateway: ActionGateway,
        allow_live_execution: bool = False,
        executor: ActionExecutor,
        execution_mode: ExecutionMode,
        strategies: tuple[Strategy, ...],
        market_data_requirements: tuple[StrategyInputRequirement, ...] = (),
        clock: Clock | None = None,
        operator_policy: OperatorPolicy | None = None,
        product_catalog: ProductCatalog | None = None,
    ) -> None:
        if not strategies:
            raise ValueError("at least one strategy is required")
        if not isinstance(execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")
        if not isinstance(allow_live_execution, bool):
            raise TypeError("allow_live_execution must be a bool")
        if execution_mode == ExecutionMode.LIVE and not allow_live_execution:
            raise ValueError("strategy live execution requires allow_live_execution")
        if not isinstance(market_data_requirements, tuple):
            raise TypeError("market_data_requirements must be a tuple")
        for requirement in market_data_requirements:
            if not isinstance(requirement, StrategyInputRequirement):
                raise TypeError(
                    "market_data_requirements must contain StrategyInputRequirement values"
                )
        self._core = core
        self._action_gateway = action_gateway
        self._executor = executor
        self._execution_mode = execution_mode
        self._market_data_requirements = market_data_requirements
        self._operator_policy = _operator_policy_or_none(operator_policy)
        self._strategies = _validated_strategies(strategies)
        self._clock = clock or SystemClock()
        self._product_catalog = product_catalog

    def run(self) -> dict[str, JsonValue]:
        receipts_list: list[StrategyEvaluationReceipt] = []
        for index, strategy in enumerate(self._strategies):
            receipt = self._evaluate_strategy(strategy, strategy_index=index)
            receipts_list.append(receipt)
            if receipt.status == StrategyEvaluationStatus.FAILED:
                break
        receipts = tuple(receipts_list)
        completed_count = sum(1 for receipt in receipts if receipt.status == StrategyEvaluationStatus.COMPLETED)
        failed_count = sum(1 for receipt in receipts if receipt.status == StrategyEvaluationStatus.FAILED)
        submitted_action_count = sum(receipt.submitted_action_count for receipt in receipts)
        return {
            "completed_count": completed_count,
            "evaluations": [receipt.to_payload() for receipt in receipts],
            "failed_count": failed_count,
            "strategy_count": len(receipts),
            "submitted_action_count": submitted_action_count,
        }

    def _evaluate_strategy(self, strategy: Strategy, *, strategy_index: int) -> StrategyEvaluationReceipt:
        strategy_id = _strategy_id(strategy)
        snapshot = self._snapshot()
        started_record = self._core.emit(
            EventType.STRATEGY_EVALUATION_STARTED,
            {
                "as_of_sequence": snapshot.as_of_sequence,
                "evaluated_at": snapshot.evaluated_at,
                "execution_mode": self._execution_mode.value,
                "market_data_requirements": [
                    requirement.to_payload() for requirement in self._market_data_requirements
                ],
                "strategy_id": strategy_id,
                "strategy_index": strategy_index,
            },
        )
        action_receipts: list[ActionReceipt] = []
        input_freshness = tuple(
            requirement.evaluate(snapshot) for requirement in self._market_data_requirements
        )
        intent_count = 0
        metadata: Mapping[str, Any] = {}
        try:
            stale_or_missing_inputs = tuple(
                freshness for freshness in input_freshness if not freshness.is_ok
            )
            if stale_or_missing_inputs:
                raise StrategyInputUnavailableError(
                    "strategy input requirements are not satisfied",
                    context={
                        "input_freshness": [
                            freshness.to_payload() for freshness in input_freshness
                        ],
                        "strategy_id": strategy_id,
                    },
                )
            decision = strategy.evaluate(snapshot)
            if not isinstance(decision, StrategyDecision):
                raise StrategyContractError(
                    "strategy evaluate must return a StrategyDecision",
                    context={"observed_type": decision.__class__.__name__, "strategy_id": strategy_id},
                )
            intent_count = len(decision.intents)
            metadata = decision.metadata
            for command in _commands_from_decision(strategy_id, decision):
                receipt = self._action_gateway.submit_and_execute(command, self._executor)
                action_receipts.append(receipt)
                if receipt.status not in _STRATEGY_ACTION_SUCCESS_STATUSES:
                    raise StrategyActionSubmissionError(
                        "strategy action submission failed",
                        context={
                            "action_id": receipt.action_id,
                            "action_type": receipt.action_type.value,
                            "failure_reason": (
                                receipt.failure_reason.value
                                if receipt.failure_reason is not None
                                else None
                            ),
                            "rejection_reason": (
                                receipt.rejection_reason.value
                                if receipt.rejection_reason is not None
                                else None
                            ),
                            "status": receipt.status.value,
                            "strategy_id": strategy_id,
                        },
                    )
            completed_record = self._core.emit(
                EventType.STRATEGY_EVALUATION_COMPLETED,
                {
                    "action_receipts": [_action_receipt_payload(receipt) for receipt in action_receipts],
                    "as_of_sequence": snapshot.as_of_sequence,
                    "input_freshness": [
                        freshness.to_payload() for freshness in input_freshness
                    ],
                    "intent_count": intent_count,
                    "metadata": _metadata_payload(metadata),
                    "started_sequence": started_record.sequence,
                    "status": StrategyEvaluationStatus.COMPLETED.value,
                    "strategy_id": strategy_id,
                    "submitted_action_count": len(action_receipts),
                },
            )
            return StrategyEvaluationReceipt(
                action_receipts=tuple(action_receipts),
                closed_sequence=completed_record.sequence,
                input_freshness=input_freshness,
                intent_count=intent_count,
                metadata=metadata,
                started_sequence=started_record.sequence,
                status=StrategyEvaluationStatus.COMPLETED,
                strategy_id=strategy_id,
            )
        except Exception as exc:
            error_code = (
                ErrorCode.STRATEGY_CONTRACT_FAILED
                if isinstance(exc, StrategyContractError)
                else ErrorCode.STRATEGY_ACTION_FAILED
                if isinstance(exc, StrategyActionSubmissionError)
                else ErrorCode.STRATEGY_INPUT_UNAVAILABLE
                if isinstance(exc, StrategyInputUnavailableError)
                else ErrorCode.STRATEGY_EVALUATION_FAILED
            )
            error_record = self._core.emit(
                EventType.ERROR,
                exception_to_error_payload(
                    exc,
                    category=ErrorCategory.STRATEGY,
                    context={
                        "started_sequence": started_record.sequence,
                        "strategy_id": strategy_id,
                    },
                    error_code=error_code,
                ),
            )
            failed_record = self._core.emit(
                EventType.STRATEGY_EVALUATION_FAILED,
                {
                    "action_receipts": [_action_receipt_payload(receipt) for receipt in action_receipts],
                    "as_of_sequence": snapshot.as_of_sequence,
                    "error_sequence": error_record.sequence,
                    "input_freshness": [
                        freshness.to_payload() for freshness in input_freshness
                    ],
                    "intent_count": intent_count,
                    "metadata": _metadata_payload(metadata),
                    "started_sequence": started_record.sequence,
                    "status": StrategyEvaluationStatus.FAILED.value,
                    "strategy_id": strategy_id,
                    "submitted_action_count": len(action_receipts),
                },
            )
            return StrategyEvaluationReceipt(
                action_receipts=tuple(action_receipts),
                closed_sequence=failed_record.sequence,
                error_sequence=error_record.sequence,
                input_freshness=input_freshness,
                intent_count=intent_count,
                metadata=metadata,
                started_sequence=started_record.sequence,
                status=StrategyEvaluationStatus.FAILED,
                strategy_id=strategy_id,
            )

    def _snapshot(self) -> StrategySnapshot:
        projection = SourceOfTruthProjection.from_ledger(self._core.ledger)
        return StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=self._clock.now(),
            execution_mode=self._execution_mode,
            ledger_path=self._core.ledger.path,
            operator_policy=self._operator_policy,
            product_catalog=self._product_catalog,
            projection=projection,
        )


def select_strategies(
    available_strategies: tuple[Strategy, ...],
    strategy_ids: tuple[str, ...],
) -> tuple[Strategy, ...]:
    if not strategy_ids:
        raise ValueError("strategy_ids must not be empty when strategy evaluation is enabled")

    strategies_by_id: dict[str, Strategy] = {}
    for strategy in _validated_strategies(available_strategies):
        strategies_by_id[_strategy_id(strategy)] = strategy

    missing = tuple(strategy_id for strategy_id in strategy_ids if strategy_id not in strategies_by_id)
    if missing:
        raise ValueError(f"unknown strategy_id(s): {', '.join(missing)}")
    return tuple(strategies_by_id[strategy_id] for strategy_id in strategy_ids)


def strategy_action_id(strategy_id: str, purpose: str, *parts: Any) -> str:
    return _strategy_identifier("act", strategy_id, purpose, parts)


def strategy_client_order_id(strategy_id: str, purpose: str, *parts: Any) -> str:
    return _strategy_identifier("coid", strategy_id, purpose, parts)


def strategy_staged_release_intents(
    strategy_id: str,
    purpose: str,
    base_intent: PlaceOrderIntent,
    decision: "OrderSizingDecision",
    *parts: Any,
) -> tuple[PlaceOrderIntent, ...]:
    from orders.sizing import OrderSizingDecision

    if not isinstance(base_intent, PlaceOrderIntent):
        raise TypeError("base_intent must be a PlaceOrderIntent")
    if not isinstance(decision, OrderSizingDecision):
        raise TypeError("decision must be an OrderSizingDecision")
    if not decision.accepted:
        raise StrategyContractError(
            "accepted staged release sizing decision required",
            context={"product_id": decision.product_id},
        )
    if not decision.output_sizes:
        raise StrategyContractError(
            "staged release sizing decision must contain output sizes",
            context={"product_id": decision.product_id},
        )
    if decision.product_id != base_intent.product_id:
        raise StrategyContractError(
            "staged release sizing decision product_id must match base intent",
            context={
                "base_product_id": base_intent.product_id,
                "decision_product_id": decision.product_id,
            },
        )

    limit_price = base_intent.limit_price
    if decision.limit_price is not None:
        decision_limit_price = str(decision.limit_price)
        if limit_price is not None and not _decimal_strings_equal(limit_price, decision_limit_price):
            raise StrategyContractError(
                "staged release sizing decision limit_price must match base intent",
                context={
                    "base_limit_price": limit_price,
                    "decision_limit_price": decision_limit_price,
                    "product_id": decision.product_id,
                },
            )
        limit_price = decision_limit_price

    purpose_key = f"{purpose}-{OrderPlacementKind.STAGED_RELEASE.value}"
    chunk_count = len(decision.output_sizes)
    intents: list[PlaceOrderIntent] = []
    for index, output_size in enumerate(decision.output_sizes, start=1):
        chunk_identity = {
            "chunk_count": chunk_count,
            "chunk_index": index,
            "size": str(output_size),
        }
        metadata = {
            **base_intent.metadata,
            "staged_release": {
                "chunk_count": chunk_count,
                "chunk_index": index,
                "size": str(output_size),
            },
        }
        intents.append(
            replace(
                base_intent,
                action_id=strategy_action_id(strategy_id, purpose_key, *parts, chunk_identity),
                idempotency_key=strategy_client_order_id(strategy_id, purpose_key, *parts, chunk_identity),
                limit_price=limit_price,
                metadata=metadata,
                placement_kind=OrderPlacementKind.STAGED_RELEASE,
                size=str(output_size),
            )
        )
    return tuple(intents)


def strategy_release_staged_placement_intent(
    strategy_id: str,
    purpose: str,
    snapshot: StrategySnapshot,
    staged_placement_id: str,
    *parts: Any,
    allow_live_overlap: bool = False,
) -> PlaceOrderIntent:
    if not isinstance(snapshot, StrategySnapshot):
        raise TypeError("snapshot must be a StrategySnapshot")
    if not isinstance(staged_placement_id, str) or not staged_placement_id:
        raise ValueError("staged_placement_id must be a non-empty string")
    if not isinstance(allow_live_overlap, bool):
        raise TypeError("allow_live_overlap must be a bool")

    projection = snapshot.projection
    placement = projection.placements_by_id.get(staged_placement_id)
    if placement is None:
        raise StrategyInputUnavailableError(
            "staged placement is not available in the source-of-truth projection",
            context={"staged_placement_id": staged_placement_id},
        )
    if (
        placement.placement_kind != OrderPlacementKind.STAGED_RELEASE
        or placement.placement_status != OrderPlacementStatus.STAGED
    ):
        raise StrategyContractError(
            "release requires a staged release placement",
            context={
                "placement_id": placement.placement_id,
                "placement_kind": placement.placement_kind.value,
                "placement_status": placement.placement_status.value,
            },
        )
    if _released_staged_placement_exists(snapshot, staged_placement_id):
        raise StrategyContractError(
            "staged placement already has a recorded release placement",
            context={"staged_placement_id": staged_placement_id},
        )
    if not allow_live_overlap:
        open_release_action_ids = _open_live_action_ids_for_logical_order(
            snapshot,
            placement.logical_order_id,
        )
        if open_release_action_ids:
            raise StrategyContractError(
                "staged placement release would overlap an existing live order for the logical order",
                context={
                    "live_action_ids": list(open_release_action_ids),
                    "logical_order_id": placement.logical_order_id,
                    "staged_placement_id": staged_placement_id,
                },
            )

    staged_action = (
        projection.orders_by_action_id.get(placement.action_id)
        if placement.action_id is not None
        else None
    )
    if staged_action is None:
        raise StrategyInputUnavailableError(
            "staged placement source order is not available in the source-of-truth projection",
            context={"staged_placement_id": staged_placement_id},
        )
    if staged_action.order_type is None:
        raise StrategyInputUnavailableError(
            "staged placement source order_type is not available in the source-of-truth projection",
            context={"staged_placement_id": staged_placement_id},
        )
    time_in_force = staged_action.time_in_force
    if time_in_force is None:
        raise StrategyInputUnavailableError(
            "staged placement source time_in_force is not available in the source-of-truth projection",
            context={"staged_placement_id": staged_placement_id},
        )

    release_identity = {
        "logical_order_id": placement.logical_order_id,
        "staged_placement_id": staged_placement_id,
    }
    purpose_key = f"{purpose}-{OrderPlacementKind.RELEASE.value}"
    return PlaceOrderIntent(
        action_id=strategy_action_id(strategy_id, purpose_key, *parts, release_identity),
        idempotency_key=strategy_client_order_id(strategy_id, purpose_key, *parts, release_identity),
        leverage=staged_action.leverage,
        limit_price=placement.limit_price,
        logical_order_id=placement.logical_order_id,
        margin_type=staged_action.margin_type,
        metadata={
            "staged_release": {
                "release_of_action_id": placement.action_id,
                "release_of_placement_id": staged_placement_id,
            },
        },
        order_type=staged_action.order_type,
        placement_kind=OrderPlacementKind.RELEASE,
        post_only=bool(staged_action.post_only),
        product_id=placement.product_id,
        reduce_only=bool(staged_action.reduce_only),
        side=placement.side,
        size=placement.size,
        time_in_force=time_in_force,
    )


def strategy_followup_after_fill_intent(
    strategy_id: str,
    purpose: str,
    snapshot: StrategySnapshot,
    fill_id: str,
    *parts: Any,
    limit_price: Any | None = None,
) -> PlaceOrderIntent:
    if not isinstance(snapshot, StrategySnapshot):
        raise TypeError("snapshot must be a StrategySnapshot")
    if not isinstance(fill_id, str) or not fill_id:
        raise ValueError("fill_id must be a non-empty string")

    policy = snapshot.operator_policy
    if policy is None:
        raise StrategyContractError(
            "operator policy is required to create followup-after-fill intents",
            context={"fill_id": fill_id},
        )
    if policy.lineage.followup_on_fill != OperatorPolicyPermission.ALLOWED:
        raise StrategyContractError(
            "operator policy does not allow followup-after-fill lineage",
            context={"fill_id": fill_id},
        )
    if policy.partial_fills.followup_size_mode != OperatorPolicyFollowupSizeMode.PERCENT_OF_FILLED_SIZE:
        raise StrategyContractError(
            "unsupported followup size mode",
            context={
                "fill_id": fill_id,
                "followup_size_mode": policy.partial_fills.followup_size_mode.value,
            },
        )
    if snapshot.product_catalog is None:
        raise StrategyInputUnavailableError(
            "product catalog is required to create followup-after-fill intents",
            context={"fill_id": fill_id},
        )

    projection = snapshot.projection
    fill = projection.fills_by_id.get(fill_id)
    if fill is None:
        raise StrategyInputUnavailableError(
            "fill is not available in the source-of-truth projection",
            context={"fill_id": fill_id},
        )
    if fill.order_id is None:
        raise StrategyInputUnavailableError(
            "fill order_id is required to create a followup intent",
            context={"fill_id": fill_id},
        )

    parent_order = projection.orders_by_exchange_order_id.get(fill.order_id)
    if parent_order is None:
        raise StrategyInputUnavailableError(
            "filled order is not available in the source-of-truth projection",
            context={"fill_id": fill_id, "order_id": fill.order_id},
        )
    parent_logical_order_id = projection.logical_order_id_by_action_id.get(parent_order.action_id)
    parent_logical_order = (
        projection.logical_orders_by_id.get(parent_logical_order_id)
        if parent_logical_order_id is not None
        else None
    )
    if parent_logical_order is None:
        raise StrategyInputUnavailableError(
            "filled order logical identity is not available in the source-of-truth projection",
            context={"action_id": parent_order.action_id, "fill_id": fill_id},
        )

    product_id = fill.product_id or parent_order.product_id or parent_logical_order.product_id
    if product_id not in policy.scope.products:
        raise StrategyContractError(
            "fill product_id is outside operator policy scope",
            context={"fill_id": fill_id, "product_id": product_id},
        )
    try:
        product = snapshot.product_catalog.require(product_id)
    except KeyError as exc:
        raise StrategyInputUnavailableError(
            "product metadata is required to create followup-after-fill intents",
            context={"fill_id": fill_id, "product_id": product_id},
        ) from exc

    fill_size = _positive_decimal_input(fill.size, "fill.size")
    parent_size = _positive_decimal_input(
        parent_order.size or parent_logical_order.size,
        "parent_order.size",
    )
    followup_size = fill_size * policy.partial_fills.followup_percent / Decimal("100")
    followup_price = _followup_price(
        limit_price if limit_price is not None else fill.price,
        fill_id=fill_id,
        policy=policy,
        side=_opposite_side(parent_logical_order.side),
    )
    followup_notional = product.notional(followup_size, followup_price)
    if followup_notional is None:
        raise StrategyContractError(
            "followup notional could not be evaluated",
            context={"product_id": product_id},
        )
    if followup_notional < policy.partial_fills.min_followup_notional_usd:
        raise StrategyContractError(
            "followup notional is below configured minimum",
            context={
                "fill_id": fill_id,
                "followup_notional": str(followup_notional),
                "min_followup_notional_usd": str(policy.partial_fills.min_followup_notional_usd),
            },
        )

    from orders.sizing import LineageSizingPolicy

    decision = LineageSizingPolicy.from_values(
        allow_partial_followup=policy.partial_fills.followup_enabled,
        product=product,
    ).followup_size(
        filled_size=followup_size,
        limit_price=followup_price,
        parent_size=parent_size,
    )
    if not decision.accepted:
        raise StrategyContractError(
            "followup sizing decision rejected",
            context={"fill_id": fill_id, "sizing_decision": decision.to_payload()},
        )

    identity = {
        "fill_id": fill_id,
        "parent_logical_order_id": parent_logical_order.logical_order_id,
        "size": decision.single_output_size(),
    }
    purpose_key = f"{purpose}-{OrderLineageRelation.FOLLOWUP_AFTER_FILL.value}"
    action_id = strategy_action_id(strategy_id, purpose_key, *parts, identity)
    if action_id in projection.actions:
        raise StrategyContractError(
            "followup action already exists for fill",
            context={"action_id": action_id, "fill_id": fill_id},
        )

    order_type = policy.order_behavior.default_order_type
    return PlaceOrderIntent(
        action_id=action_id,
        idempotency_key=strategy_client_order_id(strategy_id, purpose_key, *parts, identity),
        limit_price=(str(decision.limit_price) if order_type == OrderType.LIMIT else None),
        lineage_relation=OrderLineageRelation.FOLLOWUP_AFTER_FILL,
        logical_order_id=action_id,
        metadata={
            "followup_after_fill": {
                "fill_id": fill_id,
                "fill_sequence": fill.sequence,
                "parent_action_id": parent_order.action_id,
                "parent_logical_order_id": parent_logical_order.logical_order_id,
                "sizing_decision": decision.to_payload(),
            },
        },
        order_type=order_type,
        parent_order_id=parent_logical_order.logical_order_id,
        post_only=policy.order_behavior.post_only,
        product_id=product_id,
        reduce_only=policy.risk_limits.reduce_only_first,
        root_order_id=parent_logical_order.root_order_id,
        side=_opposite_side(parent_logical_order.side),
        size=decision.single_output_size(),
        source_order_ids=(parent_logical_order.logical_order_id,),
        time_in_force=policy.order_behavior.time_in_force,
    )


def strategy_consolidation_intent(
    strategy_id: str,
    purpose: str,
    snapshot: StrategySnapshot,
    source_logical_order_ids: Iterable[str],
    *parts: Any,
    limit_price: Any | None = None,
    placement_kind: OrderPlacementKind | None = None,
) -> PlaceOrderIntent:
    if not isinstance(snapshot, StrategySnapshot):
        raise TypeError("snapshot must be a StrategySnapshot")
    if placement_kind is not None and not isinstance(placement_kind, OrderPlacementKind):
        raise TypeError("placement_kind must be an OrderPlacementKind when provided")
    source_ids = _source_logical_order_ids(source_logical_order_ids)

    policy = snapshot.operator_policy
    if policy is None:
        raise StrategyContractError(
            "operator policy is required to create consolidation intents",
            context={"source_order_ids": list(source_ids)},
        )
    if policy.lineage.merge_orders != OperatorPolicyPermission.ALLOWED:
        raise StrategyContractError(
            "operator policy does not allow consolidation lineage",
            context={"source_order_ids": list(source_ids)},
        )
    if policy.order_behavior.default_order_type != OrderType.LIMIT:
        raise StrategyContractError(
            "consolidation intents require limit order behavior",
            context={
                "default_order_type": policy.order_behavior.default_order_type.value,
                "source_order_ids": list(source_ids),
            },
        )
    if snapshot.product_catalog is None:
        raise StrategyInputUnavailableError(
            "product catalog is required to create consolidation intents",
            context={"source_order_ids": list(source_ids)},
        )

    sources = tuple(snapshot.projection.logical_orders_by_id.get(source_id) for source_id in source_ids)
    missing_source_ids = tuple(
        source_id for source_id, source in zip(source_ids, sources, strict=True) if source is None
    )
    if missing_source_ids:
        raise StrategyInputUnavailableError(
            "source logical orders are not available in the source-of-truth projection",
            context={"source_order_ids": list(source_ids), "missing_source_order_ids": list(missing_source_ids)},
        )
    source_orders = tuple(source for source in sources if source is not None)

    product_id = source_orders[0].product_id
    side = source_orders[0].side
    if any(source.product_id != product_id for source in source_orders):
        raise StrategyContractError(
            "consolidation source orders must share product_id",
            context={"source_order_ids": list(source_ids)},
        )
    if any(source.side != side for source in source_orders):
        raise StrategyContractError(
            "consolidation source orders must share side",
            context={"source_order_ids": list(source_ids)},
        )
    if product_id not in policy.scope.products:
        raise StrategyContractError(
            "consolidation product_id is outside operator policy scope",
            context={"product_id": product_id, "source_order_ids": list(source_ids)},
        )

    try:
        product = snapshot.product_catalog.require(product_id)
    except KeyError as exc:
        raise StrategyInputUnavailableError(
            "product metadata is required to create consolidation intents",
            context={"product_id": product_id, "source_order_ids": list(source_ids)},
        ) from exc

    resolved_limit_price = _consolidation_limit_price(
        explicit_limit_price=limit_price,
        source_orders=source_orders,
        source_order_ids=source_ids,
    )

    from orders.sizing import LineageSizingPolicy

    decision = LineageSizingPolicy.from_values(product=product).consolidated_size(
        limit_price=resolved_limit_price,
        source_sizes=tuple(source.size for source in source_orders),
    )
    if not decision.accepted:
        raise StrategyContractError(
            "consolidation sizing decision rejected",
            context={"sizing_decision": decision.to_payload(), "source_order_ids": list(source_ids)},
        )

    identity = {
        "limit_price": str(decision.limit_price),
        "placement_kind": placement_kind.value if placement_kind is not None else None,
        "source_logical_order_ids": source_ids,
        "size": decision.single_output_size(),
    }
    purpose_key = f"{purpose}-{OrderLineageRelation.CONSOLIDATION.value}"
    action_id = strategy_action_id(strategy_id, purpose_key, *parts, identity)
    if action_id in snapshot.projection.actions:
        raise StrategyContractError(
            "consolidation action already exists",
            context={"action_id": action_id, "source_order_ids": list(source_ids)},
        )

    return PlaceOrderIntent(
        action_id=action_id,
        idempotency_key=strategy_client_order_id(strategy_id, purpose_key, *parts, identity),
        limit_price=str(decision.limit_price),
        lineage_relation=OrderLineageRelation.CONSOLIDATION,
        logical_order_id=action_id,
        metadata={
            "consolidation": {
                "source_order_ids": list(source_ids),
                "sizing_decision": decision.to_payload(),
            },
        },
        order_type=policy.order_behavior.default_order_type,
        placement_kind=placement_kind,
        post_only=policy.order_behavior.post_only,
        product_id=product_id,
        reduce_only=policy.risk_limits.reduce_only_first,
        side=side,
        size=decision.single_output_size(),
        source_order_ids=source_ids,
        time_in_force=policy.order_behavior.time_in_force,
    )


def strategy_decision_commands(strategy_id: str, decision: StrategyDecision) -> tuple[ActionCommand, ...]:
    return _commands_from_decision(strategy_id, decision)


def _validated_strategies(strategies: tuple[Strategy, ...]) -> tuple[Strategy, ...]:
    strategy_ids = tuple(_strategy_id(strategy) for strategy in strategies)
    if len(strategy_ids) != len(set(strategy_ids)):
        raise ValueError("strategy_ids must be unique")
    return tuple(strategies)


def _operator_policy_or_none(operator_policy: OperatorPolicy | None) -> OperatorPolicy | None:
    if operator_policy is None:
        return None
    from strategies.operator_policy import OperatorPolicy

    if not isinstance(operator_policy, OperatorPolicy):
        raise TypeError("operator_policy must be an OperatorPolicy when provided")
    return operator_policy


def _strategy_id(strategy: Strategy) -> str:
    strategy_id = strategy.strategy_id
    if not isinstance(strategy_id, str) or not strategy_id:
        raise ValueError("strategy_id must be a non-empty string")
    return strategy_id


_STRATEGY_ACTION_SUCCESS_STATUSES = frozenset(
    {
        ActionStatus.ACCEPTED,
        ActionStatus.EXECUTED,
    }
)


def _strategy_identifier(kind: str, strategy_id: str, purpose: str, parts: tuple[Any, ...]) -> str:
    strategy_slug = _identifier_component(strategy_id, "strategy_id")
    purpose_slug = _identifier_component(purpose, "purpose")
    payload = {
        "kind": kind,
        "parts": list(parts),
        "purpose": purpose,
        "strategy_id": strategy_id,
        "version": 1,
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{kind}-{strategy_slug}-{purpose_slug}-{digest[:_IDENTIFIER_DIGEST_LENGTH]}"


def _identifier_component(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not slug:
        raise ValueError(f"{field_name} must contain at least one ASCII letter or digit")
    return slug[:_IDENTIFIER_COMPONENT_LENGTH]


def _decimal_strings_equal(left: str, right: str) -> bool:
    try:
        return str(left) == str(right) or Decimal(str(left)) == Decimal(str(right))
    except Exception:
        return False


def _released_staged_placement_exists(
    snapshot: StrategySnapshot,
    staged_placement_id: str,
) -> bool:
    return snapshot.projection.release_placement_for_staged_placement(staged_placement_id) is not None


def _open_live_action_ids_for_logical_order(
    snapshot: StrategySnapshot,
    logical_order_id: str,
) -> tuple[str, ...]:
    projection = snapshot.projection
    action_ids: list[str] = []
    for placement in projection.placements_for_logical_order(logical_order_id):
        if placement.placement_kind == OrderPlacementKind.STAGED_RELEASE:
            continue
        if placement.action_id is None:
            continue
        order = projection.orders_by_action_id.get(placement.action_id)
        if order is None:
            continue
        if order.lifecycle_status in _LIVE_ORDER_STATUSES:
            action_ids.append(placement.action_id)
    return tuple(sorted(set(action_ids)))


def _followup_price(
    value: Any,
    *,
    fill_id: str,
    policy: OperatorPolicy,
    side: OrderSide,
) -> Decimal:
    if value is None:
        raise StrategyInputUnavailableError(
            "fill price or explicit limit_price is required to create a followup intent",
            context={"fill_id": fill_id},
        )
    price = _positive_decimal_input(value, "followup.limit_price")
    if policy.target_movement is None:
        return price
    if policy.target_movement_type != OperatorPolicyDistanceType.PERCENT:
        raise StrategyContractError(
            "unsupported target movement type",
            context={
                "fill_id": fill_id,
                "target_movement_type": (
                    policy.target_movement_type.value
                    if policy.target_movement_type is not None
                    else None
                ),
            },
        )
    multiplier = (
        Decimal("1") + policy.target_movement
        if side == OrderSide.SELL
        else Decimal("1") - policy.target_movement
    )
    target_price = price * multiplier
    if target_price <= 0:
        raise StrategyContractError(
            "target movement produced a non-positive followup price",
            context={"fill_id": fill_id, "target_price": str(target_price)},
        )
    return target_price


def _source_logical_order_ids(source_logical_order_ids: Iterable[str]) -> tuple[str, ...]:
    if isinstance(source_logical_order_ids, str | bytes | bytearray):
        raise TypeError("source_logical_order_ids must be an iterable of strings")
    source_ids = tuple(source_logical_order_ids)
    if len(source_ids) < 2:
        raise StrategyContractError(
            "consolidation requires at least two source logical orders",
            context={"source_order_ids": list(source_ids)},
        )
    if any(not isinstance(source_id, str) or not source_id for source_id in source_ids):
        raise ValueError("source_logical_order_ids must contain non-empty strings")
    if len(set(source_ids)) != len(source_ids):
        raise StrategyContractError(
            "consolidation source logical orders must be unique",
            context={"source_order_ids": list(source_ids)},
        )
    return tuple(sorted(source_ids))


def _consolidation_limit_price(
    *,
    explicit_limit_price: Any | None,
    source_orders: tuple[Any, ...],
    source_order_ids: tuple[str, ...],
) -> Decimal:
    if explicit_limit_price is not None:
        return _positive_decimal_input(explicit_limit_price, "consolidation.limit_price")

    source_prices = tuple(source.limit_price for source in source_orders)
    if any(price is None for price in source_prices):
        raise StrategyInputUnavailableError(
            "consolidation limit_price is required when any source order has no limit_price",
            context={"source_order_ids": list(source_order_ids)},
        )
    first_price = source_prices[0]
    assert first_price is not None
    if any(not _decimal_strings_equal(first_price, price or "") for price in source_prices[1:]):
        raise StrategyContractError(
            "consolidation source orders must share limit_price unless an explicit limit_price is provided",
            context={"source_order_ids": list(source_order_ids)},
        )
    return _positive_decimal_input(first_price, "consolidation.limit_price")


def _opposite_side(side: OrderSide) -> OrderSide:
    if side == OrderSide.BUY:
        return OrderSide.SELL
    if side == OrderSide.SELL:
        return OrderSide.BUY
    raise ValueError(f"unsupported order side: {side.value}")


def _positive_decimal_input(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _scheduled_slice_identity(
    *,
    interval: timedelta,
    product_id: str,
    schedule_id: str,
    slice_index: int,
    slices: int,
    start_at: datetime | None,
    total_size: Decimal,
) -> dict[str, JsonValue]:
    return {
        "interval_seconds": interval.total_seconds(),
        "product_id": product_id,
        "schedule_id": schedule_id,
        "slice_index": slice_index,
        "slices": slices,
        "start_at": start_at.isoformat() if start_at is not None else None,
        "total_size": str(total_size),
    }


def _scheduled_slice_due_at(
    *,
    completed_action_ids: tuple[str, ...],
    evaluated_at: datetime,
    interval: timedelta,
    projection: SourceOfTruthProjection,
    slice_index: int,
    start_at: datetime | None,
) -> datetime | None:
    if start_at is not None:
        return start_at + (interval * (slice_index - 1))
    if not completed_action_ids:
        return evaluated_at
    last_action = projection.actions.get(completed_action_ids[-1])
    if last_action is None:
        return None
    last_action_at = _action_occurred_at(projection, last_action)
    if last_action_at is None:
        return None
    return last_action_at + interval


def _action_occurred_at(projection: SourceOfTruthProjection, action: Any) -> datetime | None:
    for sequence in (
        action.executed_sequence,
        action.accepted_sequence,
        action.requested_sequence,
    ):
        if sequence is None:
            continue
        occurred_at = projection.record_occurred_at_by_sequence.get(sequence)
        if occurred_at is not None:
            return occurred_at
    return None


_LIVE_ORDER_STATUSES = frozenset(
    {
        OrderLifecycleStatus.ACCEPTED,
        OrderLifecycleStatus.CANCEL_QUEUED,
        OrderLifecycleStatus.EXECUTION_UNKNOWN,
        OrderLifecycleStatus.OPEN,
        OrderLifecycleStatus.PENDING,
        OrderLifecycleStatus.REQUESTED,
    }
)

_SCHEDULED_SLICE_CONSUMED_STATUSES = frozenset(
    {
        ActionStatus.ACCEPTED,
        ActionStatus.EXECUTED,
    }
)


def _commands_from_decision(strategy_id: str, decision: StrategyDecision) -> tuple[ActionCommand, ...]:
    commands = tuple(_command_from_intent(strategy_id, intent) for intent in decision.intents)
    _validate_decision_command_set(strategy_id, commands)
    return commands


def _command_from_intent(strategy_id: str, intent: StrategyIntent) -> ActionCommand:
    if isinstance(intent, ActionCommand):
        command = intent
    elif isinstance(intent, (PlaceOrderIntent, CancelOrderIntent)):
        command = intent.to_command()
    else:
        raise StrategyContractError(
            "strategy decision contains an unsupported intent",
            context={
                "intent_type": intent.__class__.__name__,
                "strategy_id": strategy_id,
            },
        )
    return replace(command, requested_by=f"strategy:{strategy_id}")


def _validate_decision_command_set(strategy_id: str, commands: tuple[ActionCommand, ...]) -> None:
    duplicate_action_ids = _duplicates(command.action_id for command in commands)
    if duplicate_action_ids:
        raise StrategyContractError(
            "strategy decision contains duplicate action_id values",
            context={
                "duplicate_action_ids": list(duplicate_action_ids),
                "strategy_id": strategy_id,
            },
        )

    place_order_client_ids = tuple(
        command.idempotency_key or command.action_id
        for command in commands
        if command.action_type == ActionType.PLACE_ORDER
    )
    duplicate_client_order_ids = _duplicates(place_order_client_ids)
    if duplicate_client_order_ids:
        raise StrategyContractError(
            "strategy decision contains duplicate place-order client identities",
            context={
                "duplicate_client_order_ids": list(duplicate_client_order_ids),
                "strategy_id": strategy_id,
            },
        )


def _duplicates(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)
    return tuple(sorted(duplicates))


def _action_receipt_payload(receipt: ActionReceipt) -> dict[str, JsonValue]:
    return {
        "action_id": receipt.action_id,
        "action_type": receipt.action_type.value,
        "decision_sequence": receipt.decision_sequence,
        "failure_reason": receipt.failure_reason.value if receipt.failure_reason is not None else None,
        "message": receipt.message,
        "rejection_reason": receipt.rejection_reason.value if receipt.rejection_reason is not None else None,
        "requested_sequence": receipt.requested_sequence,
        "status": receipt.status.value,
    }


def _metadata_payload(metadata: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(metadata)
    if not isinstance(normalized, dict):
        raise TypeError("strategy metadata must normalize to a JSON object")
    return normalized
