from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from actions.gateway import CancelOrderIntent, PlaceOrderIntent
from core.enums import (
    MarketDataKind,
    OperatorPolicyDistanceType,
    OperatorPolicyPermission,
    OperatorPolicyReferencePriceSource,
    OrderLifecycleStatus,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    StrategyManagerGate,
    StrategyManagerSkipReason,
)
from core.errors import StrategyContractError, StrategyInputUnavailableError
from core.json_tools import JsonValue, normalize_json
from projections.state import OrderSnapshot
from strategies.harness import (
    StrategyDecision,
    StrategySnapshot,
    strategy_action_id,
    strategy_client_order_id,
)
from strategies.parameters import positive_int_parameter, reject_unknown_parameters
from strategies.policy_calculations import anchored_price


ANCHOR_REPRICING_MANAGER_STRATEGY_ID = "anchor-repricing-manager"


@dataclass(frozen=True)
class AnchorRepricingManagerStrategy:
    max_moves_per_evaluation: int = 1

    @classmethod
    def from_parameters(
        cls,
        parameters: Mapping[str, Any] | None = None,
    ) -> "AnchorRepricingManagerStrategy":
        raw = {} if parameters is None else parameters
        if not isinstance(raw, Mapping):
            raise TypeError("anchor-repricing-manager parameters must be a mapping")
        reject_unknown_parameters(
            ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
            raw,
            {"max_moves_per_evaluation"},
        )
        defaults = cls()
        return cls(
            max_moves_per_evaluation=positive_int_parameter(
                raw.get("max_moves_per_evaluation", defaults.max_moves_per_evaluation),
                "anchor-repricing-manager.max_moves_per_evaluation",
            ),
        )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_moves_per_evaluation, int)
            or isinstance(self.max_moves_per_evaluation, bool)
        ):
            raise TypeError("max_moves_per_evaluation must be an integer")
        if self.max_moves_per_evaluation <= 0:
            raise ValueError("max_moves_per_evaluation must be positive")

    @property
    def strategy_id(self) -> str:
        return ANCHOR_REPRICING_MANAGER_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")

        metadata: dict[str, Any] = {
            "created_moves": [],
            "max_moves_per_evaluation": self.max_moves_per_evaluation,
            "skipped_orders": [],
        }
        policy = snapshot.operator_policy
        if policy is None:
            metadata["anchor_repricing_gate"] = StrategyManagerGate.OPERATOR_POLICY_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))
        if policy.anchor_repricing is None or not policy.anchor_repricing.enabled:
            metadata["anchor_repricing_gate"] = StrategyManagerGate.ANCHOR_REPRICING_DISABLED.value
            return StrategyDecision(metadata=_metadata(metadata))
        if (
            policy.lineage.move_same_side_orders != OperatorPolicyPermission.ALLOWED
            or not policy.moves.cancel_replace_when_amend_not_supported
            or policy.anchor_repricing.reference_price_source
            != OperatorPolicyReferencePriceSource.MIDPOINT
            or policy.anchor_repricing.distance_type != OperatorPolicyDistanceType.PERCENT
        ):
            metadata["anchor_repricing_gate"] = (
                StrategyManagerGate.ANCHOR_REPRICING_POLICY_UNSAFE.value
            )
            return StrategyDecision(metadata=_metadata(metadata))
        if snapshot.product_catalog is None:
            metadata["anchor_repricing_gate"] = StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))

        intents = []
        sources, skipped_orders = _candidate_sources(snapshot)
        metadata["skipped_orders"].extend(skipped_orders)
        for source in sources:
            if len(metadata["created_moves"]) >= self.max_moves_per_evaluation:
                break
            if source.product_id not in policy.scope.products:
                metadata["skipped_orders"].append(
                    {
                        "logical_order_id": source.logical_order_id,
                        "reason": StrategyManagerSkipReason.PRODUCT_OUTSIDE_OPERATOR_POLICY_SCOPE.value,
                        "source_action_id": source.action_id,
                    }
                )
                continue
            if source.side not in policy.risk_limits.allowed_sides:
                metadata["skipped_orders"].append(
                    {
                        "logical_order_id": source.logical_order_id,
                        "reason": StrategyManagerSkipReason.SIDE_OUTSIDE_OPERATOR_POLICY.value,
                        "source_action_id": source.action_id,
                    }
                )
                continue

            freshness_payload = None
            if policy.market_data_requirements.require_order_book:
                freshness = snapshot.market_data_freshness(
                    data_kind=MarketDataKind.ORDER_BOOK,
                    max_age=policy.market_data_requirements.max_order_book_age,
                    product_id=source.product_id,
                )
                freshness_payload = freshness.to_payload()
                if not freshness.is_ok:
                    metadata["skipped_orders"].append(
                        {
                            "freshness": freshness_payload,
                            "logical_order_id": source.logical_order_id,
                            "reason": StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value,
                            "source_action_id": source.action_id,
                        }
                    )
                    continue

            try:
                cancel_intent, replacement_intent, move_payload = _move_intents(snapshot, source)
            except _MoveSkipped as exc:
                metadata["skipped_orders"].append(
                    {
                        "logical_order_id": source.logical_order_id,
                        "message": str(exc),
                        "reason": exc.reason.value,
                        "source_action_id": source.action_id,
                        **exc.context,
                    }
                )
                continue
            except (StrategyContractError, StrategyInputUnavailableError) as exc:
                metadata["skipped_orders"].append(
                    {
                        "logical_order_id": source.logical_order_id,
                        "message": str(exc),
                        "reason": (
                            StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value
                            if isinstance(exc, StrategyContractError)
                            else StrategyManagerSkipReason.STRATEGY_INPUT_UNAVAILABLE.value
                        ),
                        "source_action_id": source.action_id,
                    }
                )
                continue

            duplicate_action_ids = tuple(
                action_id
                for action_id in (cancel_intent.action_id, replacement_intent.action_id)
                if action_id in snapshot.projection.actions
            )
            if duplicate_action_ids:
                metadata["skipped_orders"].append(
                    {
                        "action_ids": list(duplicate_action_ids),
                        "logical_order_id": source.logical_order_id,
                        "reason": StrategyManagerSkipReason.MOVE_EXISTS.value,
                        "source_action_id": source.action_id,
                    }
                )
                continue

            metadata["created_moves"].append(move_payload)
            if freshness_payload is not None:
                metadata.setdefault("anchor_repricing_input_freshness", []).append(freshness_payload)
            intents.extend((cancel_intent, replacement_intent))

        return StrategyDecision(intents=tuple(intents), metadata=_metadata(metadata))


@dataclass(frozen=True)
class _MoveSource:
    action_id: str
    client_order_id: str | None
    exchange_order_id: str
    limit_price: Decimal
    logical_order_id: str
    product_id: str
    sequence: int
    side: OrderSide
    size: str
    order: OrderSnapshot


class _MoveSkipped(Exception):
    def __init__(
        self,
        message: str,
        *,
        context: Mapping[str, JsonValue] | None = None,
        reason: StrategyManagerSkipReason,
    ) -> None:
        super().__init__(message)
        self.context = dict(context or {})
        self.reason = reason


def _candidate_sources(
    snapshot: StrategySnapshot,
) -> tuple[tuple[_MoveSource, ...], list[dict[str, JsonValue]]]:
    open_orders_by_logical_id: dict[str, list[OrderSnapshot]] = {}
    skipped_orders: list[dict[str, JsonValue]] = []
    for order in snapshot.projection.open_orders:
        logical_order_id = snapshot.projection.logical_order_id_by_action_id.get(order.action_id)
        if logical_order_id is None:
            skipped_orders.append(
                {
                    "reason": StrategyManagerSkipReason.ORDER_NOT_CANCELABLE.value,
                    "source_action_id": order.action_id,
                }
            )
            continue
        open_orders_by_logical_id.setdefault(logical_order_id, []).append(order)

    sources: list[_MoveSource] = []
    for logical_order_id, orders in sorted(open_orders_by_logical_id.items()):
        if len(orders) != 1:
            skipped_orders.append(
                {
                    "logical_order_id": logical_order_id,
                    "reason": StrategyManagerSkipReason.ORDER_NOT_CANCELABLE.value,
                    "source_action_ids": [order.action_id for order in orders],
                }
            )
            continue
        source = _candidate_source(snapshot, logical_order_id, orders[0])
        if source is None:
            skipped_orders.append(
                {
                    "logical_order_id": logical_order_id,
                    "reason": StrategyManagerSkipReason.ORDER_NOT_CANCELABLE.value,
                    "source_action_id": orders[0].action_id,
                }
            )
            continue
        sources.append(source)

    return (tuple(sorted(sources, key=lambda source: source.sequence)), skipped_orders)


def _candidate_source(
    snapshot: StrategySnapshot,
    logical_order_id: str,
    order: OrderSnapshot,
) -> _MoveSource | None:
    del snapshot
    if order.lifecycle_status not in {OrderLifecycleStatus.ACCEPTED, OrderLifecycleStatus.OPEN}:
        return None
    if order.exchange_order_id is None:
        return None
    if order.product_id is None or order.side is None:
        return None
    if order.order_type != OrderType.LIMIT:
        return None
    if order.size is None:
        return None
    if order.time_in_force is None:
        return None
    if _decimal_or_zero(order.filled_size) != Decimal("0"):
        return None
    limit_price = _positive_decimal_or_none(order.limit_price)
    if limit_price is None:
        return None
    return _MoveSource(
        action_id=order.action_id,
        client_order_id=order.client_order_id,
        exchange_order_id=order.exchange_order_id,
        limit_price=limit_price,
        logical_order_id=logical_order_id,
        order=order,
        product_id=order.product_id,
        sequence=order.accepted_sequence or order.requested_sequence or 0,
        side=order.side,
        size=order.size,
    )


def _move_intents(
    snapshot: StrategySnapshot,
    source: _MoveSource,
) -> tuple[CancelOrderIntent, PlaceOrderIntent, dict[str, JsonValue]]:
    policy = snapshot.operator_policy
    assert policy is not None
    anchor = policy.anchor_repricing
    assert anchor is not None
    assert snapshot.product_catalog is not None

    try:
        product = snapshot.product_catalog.require(source.product_id)
    except KeyError as exc:
        raise StrategyInputUnavailableError(
            "product metadata is required to move an anchored order",
            context={"product_id": source.product_id, "source_action_id": source.action_id},
        ) from exc

    midpoint = _book_midpoint(snapshot, product_id=source.product_id)
    if midpoint is None:
        raise StrategyInputUnavailableError(
            "order book midpoint is required to move an anchored order",
            context={"product_id": source.product_id, "source_action_id": source.action_id},
        )
    target_price = anchored_price(
        current_price=source.limit_price,
        distance_type=anchor.distance_type,
        max_distance=anchor.max_distance,
        max_step_per_reprice=anchor.max_step_per_reprice if anchor.slide_mode else None,
        reference_price=midpoint,
        slide_mode=anchor.slide_mode,
    )
    target_price = _round_move_target(
        current_price=source.limit_price,
        increment=product.price_increment,
        target_price=target_price,
    )
    if target_price <= 0:
        raise StrategyContractError(
            "anchor repricing produced a non-positive target price",
            context={"source_action_id": source.action_id, "target_price": str(target_price)},
        )

    min_price_change = _minimum_price_change(
        anchor_min_price_change=anchor.min_price_change,
        price_increment=product.price_increment,
        ticks=policy.moves.min_price_change_ticks,
    )
    if abs(target_price - source.limit_price) < min_price_change:
        raise _MoveSkipped(
            "anchored target price does not require movement",
            context={
                "current_price": str(source.limit_price),
                "minimum_price_change": str(min_price_change),
                "source_action_id": source.action_id,
                "target_price": str(target_price),
            },
            reason=StrategyManagerSkipReason.ANCHOR_PRICE_UNCHANGED,
        )

    cooldown = max(policy.moves.cooldown, anchor.min_reprice_interval)
    _enforce_reprice_cadence(snapshot, source, cooldown=cooldown)
    _enforce_hourly_reprice_limit(snapshot, source, max_reprices_per_hour=anchor.max_reprices_per_hour)

    identity = {
        "current_price": str(source.limit_price),
        "logical_order_id": source.logical_order_id,
        "reference_price": str(midpoint),
        "source_action_id": source.action_id,
        "target_price": str(target_price),
    }
    cancel_action_id = strategy_action_id(
        ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
        "cancel-anchor-source",
        identity,
    )
    move_action_id = strategy_action_id(
        ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
        "cancel-replace-anchor",
        identity,
    )
    cancel_intent = CancelOrderIntent(
        action_id=cancel_action_id,
        client_order_id=source.client_order_id,
        exchange_order_id=source.exchange_order_id,
        idempotency_key=strategy_client_order_id(
            ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
            "cancel-anchor-source",
            identity,
        ),
    )
    replacement_intent = PlaceOrderIntent(
        action_id=move_action_id,
        idempotency_key=strategy_client_order_id(
            ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
            "cancel-replace-anchor",
            identity,
        ),
        leverage=source.order.leverage,
        limit_price=str(target_price),
        logical_order_id=source.logical_order_id,
        margin_type=source.order.margin_type,
        metadata={
            "anchor_repricing": {
                "cancel_action_id": cancel_action_id,
                "current_price": str(source.limit_price),
                "max_distance": str(anchor.max_distance),
                "midpoint": str(midpoint),
                "source_action_id": source.action_id,
                "source_exchange_order_id": source.exchange_order_id,
                "target_price": str(target_price),
            },
        },
        order_type=source.order.order_type,
        placement_kind=OrderPlacementKind.CANCEL_REPLACE,
        post_only=bool(source.order.post_only),
        product_id=source.product_id,
        reduce_only=bool(source.order.reduce_only),
        side=source.side,
        size=source.size,
        time_in_force=source.order.time_in_force,
    )
    move_payload = {
        "cancel_action_id": cancel_action_id,
        "current_price": str(source.limit_price),
        "logical_order_id": source.logical_order_id,
        "placement_action_id": move_action_id,
        "product_id": source.product_id,
        "reference_price": str(midpoint),
        "side": source.side.value,
        "source_action_id": source.action_id,
        "target_price": str(target_price),
    }
    normalized = normalize_json(move_payload)
    if not isinstance(normalized, dict):
        raise TypeError("anchor repricing move payload must normalize to an object")
    return cancel_intent, replacement_intent, normalized


def _book_midpoint(snapshot: StrategySnapshot, *, product_id: str) -> Decimal | None:
    book = snapshot.projection.order_book(product_id)
    if book is None:
        return None
    bid = _positive_decimal_or_none(book.best_bid_price)
    ask = _positive_decimal_or_none(book.best_ask_price)
    if bid is None or ask is None or bid >= ask:
        return None
    return (bid + ask) / Decimal("2")


def _round_move_target(
    *,
    current_price: Decimal,
    increment: Decimal | None,
    target_price: Decimal,
) -> Decimal:
    if increment is None or increment <= 0:
        return target_price
    if target_price > current_price:
        return _ceil_to_increment(target_price, increment)
    if target_price < current_price:
        return _floor_to_increment(target_price, increment)
    return target_price.quantize(increment)


def _minimum_price_change(
    *,
    anchor_min_price_change: Decimal,
    price_increment: Decimal | None,
    ticks: int,
) -> Decimal:
    threshold = anchor_min_price_change
    if price_increment is not None and price_increment > 0:
        threshold = max(threshold, price_increment * ticks)
    return threshold


def _enforce_reprice_cadence(
    snapshot: StrategySnapshot,
    source: _MoveSource,
    *,
    cooldown: timedelta,
) -> None:
    latest_move_time = _latest_move_time(snapshot, source.logical_order_id)
    if latest_move_time is None:
        return
    if snapshot.evaluated_at - latest_move_time < cooldown:
        raise _MoveSkipped(
            "anchor repricing cooldown is active",
            context={
                "cooldown_seconds": cooldown.total_seconds(),
                "latest_move_at": latest_move_time.isoformat(),
                "logical_order_id": source.logical_order_id,
                "source_action_id": source.action_id,
            },
            reason=StrategyManagerSkipReason.REPRICE_COOLDOWN_ACTIVE,
        )


def _enforce_hourly_reprice_limit(
    snapshot: StrategySnapshot,
    source: _MoveSource,
    *,
    max_reprices_per_hour: int,
) -> None:
    window_start = snapshot.evaluated_at - timedelta(hours=1)
    move_count = sum(
        1
        for occurred_at in _move_times(snapshot, source.logical_order_id)
        if occurred_at >= window_start
    )
    if move_count >= max_reprices_per_hour:
        raise _MoveSkipped(
            "anchor repricing hourly move limit reached",
            context={
                "logical_order_id": source.logical_order_id,
                "max_reprices_per_hour": max_reprices_per_hour,
                "move_count": move_count,
                "source_action_id": source.action_id,
            },
            reason=StrategyManagerSkipReason.REPRICE_LIMIT_REACHED,
        )


def _latest_move_time(snapshot: StrategySnapshot, logical_order_id: str) -> datetime | None:
    times = _move_times(snapshot, logical_order_id)
    return max(times) if times else None


def _move_times(snapshot: StrategySnapshot, logical_order_id: str) -> tuple[datetime, ...]:
    times: list[datetime] = []
    for placement in snapshot.projection.placements_for_logical_order(logical_order_id):
        if placement.placement_kind not in {OrderPlacementKind.AMEND, OrderPlacementKind.CANCEL_REPLACE}:
            continue
        occurred_at = snapshot.projection.record_occurred_at_by_sequence.get(placement.sequence)
        if occurred_at is not None:
            times.append(occurred_at)
    return tuple(times)


def _floor_to_increment(value: Decimal, increment: Decimal) -> Decimal:
    return ((value // increment) * increment).quantize(increment)


def _ceil_to_increment(value: Decimal, increment: Decimal) -> Decimal:
    if value == 0 or value % increment == 0:
        return value.quantize(increment)
    return (((value // increment) + 1) * increment).quantize(increment)


def _decimal_or_zero(value: str | None) -> Decimal:
    parsed = _positive_decimal_or_none(value)
    return parsed if parsed is not None else Decimal("0")


def _positive_decimal_or_none(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _metadata(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(raw)
    if not isinstance(normalized, dict):
        raise TypeError("anchor repricing manager metadata must normalize to an object")
    return normalized
