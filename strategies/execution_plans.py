from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import (
    IncrementRoundingMode,
    OrderSide,
    ProductRuleCheckStatus,
    ProductRuleFailure,
)
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductMetadata
from strategies.product_rules import (
    price_tick_proposal,
    validate_limit_price,
    validate_notional,
    validate_order_size,
)


AmountInput = Decimal | str | int | float
_BPS_DENOMINATOR = Decimal("10000")


@dataclass(frozen=True)
class QuotePairPrices:
    product_id: str
    midpoint: Decimal
    spread_bps: Decimal
    bid_price: Decimal | None
    ask_price: Decimal | None
    status: ProductRuleCheckStatus
    failures: tuple[ProductRuleFailure, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == ProductRuleCheckStatus.ACCEPTED

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "ask_price": self.ask_price,
                "bid_price": self.bid_price,
                "failures": tuple(failure.value for failure in self.failures),
                "is_ok": self.is_ok,
                "midpoint": self.midpoint,
                "product_id": self.product_id,
                "spread_bps": self.spread_bps,
                "status": self.status,
            }
        )


@dataclass(frozen=True)
class LadderPlanRow:
    index: int
    product_id: str
    side: OrderSide
    price: Decimal | None
    size: Decimal | None
    notional: Decimal | None
    status: ProductRuleCheckStatus
    failures: tuple[ProductRuleFailure, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == ProductRuleCheckStatus.ACCEPTED

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "failures": tuple(failure.value for failure in self.failures),
                "index": self.index,
                "is_ok": self.is_ok,
                "notional": self.notional,
                "price": self.price,
                "product_id": self.product_id,
                "side": self.side,
                "size": self.size,
                "status": self.status,
            }
        )


@dataclass(frozen=True)
class LadderPlan:
    product_id: str
    side: OrderSide
    anchor_price: Decimal
    step_bps: Decimal
    levels: int
    size_per_level: Decimal
    rows: tuple[LadderPlanRow, ...]
    status: ProductRuleCheckStatus
    failures: tuple[ProductRuleFailure, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == ProductRuleCheckStatus.ACCEPTED

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "anchor_price": self.anchor_price,
                "failures": tuple(failure.value for failure in self.failures),
                "is_ok": self.is_ok,
                "levels": self.levels,
                "product_id": self.product_id,
                "rows": tuple(row.to_payload() for row in self.rows),
                "side": self.side,
                "size_per_level": self.size_per_level,
                "status": self.status,
                "step_bps": self.step_bps,
            }
        )


def ladder_plan(
    product: ProductMetadata,
    *,
    anchor_price: AmountInput,
    levels: int,
    side: OrderSide,
    size_per_level: AmountInput,
    step_bps: AmountInput,
) -> LadderPlan:
    if not isinstance(product, ProductMetadata):
        raise TypeError("product must be ProductMetadata")
    if not isinstance(side, OrderSide):
        raise TypeError("side must be an OrderSide")
    if not isinstance(levels, int) or isinstance(levels, bool):
        raise TypeError("levels must be an integer")
    if levels <= 0:
        raise ValueError("levels must be positive")
    anchor = _positive_decimal(anchor_price, "anchor_price")
    step = _positive_decimal(step_bps, "step_bps")
    size = _positive_decimal(size_per_level, "size_per_level")
    if step >= _BPS_DENOMINATOR:
        raise ValueError("step_bps must be less than 10000")

    rows: list[LadderPlanRow] = []
    for index in range(levels):
        raw_price = _ladder_price(anchor=anchor, index=index, side=side, step_bps=step)
        mode = IncrementRoundingMode.DOWN if side == OrderSide.BUY else IncrementRoundingMode.UP
        price_proposal = price_tick_proposal(product, price=raw_price, mode=mode)
        price = price_proposal.proposed_value
        failures = price_proposal.failures
        if price is not None:
            price_check = validate_limit_price(product, price)
            size_check = validate_order_size(product, size)
            notional_check = validate_notional(product, price=price, size=size)
            failures = _unique_failures(
                (
                    *failures,
                    *price_check.failures,
                    *size_check.failures,
                    *notional_check.failures,
                )
            )
            notional = notional_check.notional
        else:
            notional = None
        status = (
            ProductRuleCheckStatus.REJECTED
            if failures
            else ProductRuleCheckStatus.ACCEPTED
        )
        rows.append(
            LadderPlanRow(
                failures=failures,
                index=index,
                notional=notional,
                price=price,
                product_id=product.product_id,
                side=side,
                size=size,
                status=status,
            )
        )
    plan_failures = _unique_failures(tuple(failure for row in rows for failure in row.failures))
    return LadderPlan(
        anchor_price=anchor,
        failures=plan_failures,
        levels=levels,
        product_id=product.product_id,
        rows=tuple(rows),
        side=side,
        size_per_level=size,
        status=(
            ProductRuleCheckStatus.REJECTED
            if plan_failures
            else ProductRuleCheckStatus.ACCEPTED
        ),
        step_bps=step,
    )


def quote_pair_prices(
    product: ProductMetadata,
    *,
    midpoint: AmountInput,
    spread_bps: AmountInput,
) -> QuotePairPrices:
    if not isinstance(product, ProductMetadata):
        raise TypeError("product must be ProductMetadata")
    resolved_midpoint = _positive_decimal(midpoint, "midpoint")
    resolved_spread_bps = _positive_decimal(spread_bps, "spread_bps")
    if resolved_spread_bps >= (_BPS_DENOMINATOR * Decimal("2")):
        raise ValueError("spread_bps must be less than 20000")

    half_spread_fraction = (resolved_spread_bps / Decimal("2")) / _BPS_DENOMINATOR
    bid_raw = resolved_midpoint * (Decimal("1") - half_spread_fraction)
    ask_raw = resolved_midpoint * (Decimal("1") + half_spread_fraction)
    bid_proposal = price_tick_proposal(
        product,
        price=bid_raw,
        mode=IncrementRoundingMode.DOWN,
    )
    ask_proposal = price_tick_proposal(
        product,
        price=ask_raw,
        mode=IncrementRoundingMode.UP,
    )
    failures = _unique_failures((*bid_proposal.failures, *ask_proposal.failures))
    bid_price = bid_proposal.proposed_value
    ask_price = ask_proposal.proposed_value
    if bid_price is not None and ask_price is not None:
        bid_check = validate_limit_price(product, bid_price)
        ask_check = validate_limit_price(product, ask_price)
        failures = _unique_failures(
            (*failures, *bid_check.failures, *ask_check.failures)
        )
        if not failures and bid_price >= ask_price:
            raise ValueError("quote pair produced a crossed or locked quote")
    return QuotePairPrices(
        ask_price=ask_price,
        bid_price=bid_price,
        failures=failures,
        midpoint=resolved_midpoint,
        product_id=product.product_id,
        spread_bps=resolved_spread_bps,
        status=(
            ProductRuleCheckStatus.REJECTED
            if failures
            else ProductRuleCheckStatus.ACCEPTED
        ),
    )


def _ladder_price(
    *,
    anchor: Decimal,
    index: int,
    side: OrderSide,
    step_bps: Decimal,
) -> Decimal:
    step_fraction = (step_bps / _BPS_DENOMINATOR) * Decimal(index)
    multiplier = (
        Decimal("1") - step_fraction
        if side == OrderSide.BUY
        else Decimal("1") + step_fraction
    )
    price = anchor * multiplier
    if price <= 0:
        raise ValueError("ladder step produced a non-positive price")
    return price


def _positive_decimal(value: AmountInput, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _unique_failures(failures: tuple[ProductRuleFailure, ...]) -> tuple[ProductRuleFailure, ...]:
    return tuple(dict.fromkeys(failures))


def _payload(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(
        {
            key: _json_safe(value)
            for key, value in raw.items()
        }
    )
    if not isinstance(normalized, dict):
        raise TypeError("execution plan payload must normalize to an object")
    return normalized


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value
