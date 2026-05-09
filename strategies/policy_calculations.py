from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import OperatorPolicyDistanceType


AmountInput = Decimal | str | int | float


def visible_notional(*, size: AmountInput, price: AmountInput) -> Decimal:
    return _positive_decimal(size, "size") * _positive_decimal(price, "price")


def tranche_release_sizes(
    *,
    total_size: AmountInput,
    tranche_schedule: tuple[Decimal, ...],
) -> tuple[Decimal, ...]:
    total = _positive_decimal(total_size, "total_size")
    if not isinstance(tranche_schedule, tuple):
        raise TypeError("tranche_schedule must be a tuple")
    _validate_tranche_schedule(tranche_schedule)

    sizes: list[Decimal] = []
    previous = Decimal("0")
    for cumulative_fraction in tranche_schedule:
        sizes.append(total * (cumulative_fraction - previous))
        previous = cumulative_fraction
    return tuple(sizes)


def adaptive_reveal_size(
    *,
    base_size: AmountInput,
    market_volume: AmountInput,
    baseline_volume: AmountInput,
    reveal_multiplier: AmountInput,
    max_reveal_percentage: AmountInput | None = None,
) -> Decimal:
    base = _positive_decimal(base_size, "base_size")
    market = _positive_decimal(market_volume, "market_volume")
    baseline = _positive_decimal(baseline_volume, "baseline_volume")
    multiplier = _positive_decimal(reveal_multiplier, "reveal_multiplier")
    output = base * (market / baseline) * multiplier
    if max_reveal_percentage is None:
        return output
    cap_fraction = _positive_decimal(max_reveal_percentage, "max_reveal_percentage")
    if cap_fraction > Decimal("1"):
        raise ValueError("max_reveal_percentage must be less than or equal to 1")
    return min(output, base * cap_fraction)


def slide_price_toward(
    *,
    current_price: AmountInput,
    desired_price: AmountInput,
    max_step: AmountInput,
) -> Decimal:
    current = _positive_decimal(current_price, "current_price")
    desired = _positive_decimal(desired_price, "desired_price")
    step = _positive_decimal(max_step, "max_step")
    distance = desired - current
    if abs(distance) <= step:
        return desired
    if distance > 0:
        return current + step
    return current - step


def anchored_price(
    *,
    current_price: AmountInput,
    reference_price: AmountInput,
    max_distance: AmountInput,
    distance_type: OperatorPolicyDistanceType,
    slide_mode: bool = False,
    max_step_per_reprice: AmountInput | None = None,
) -> Decimal:
    current = _positive_decimal(current_price, "current_price")
    reference = _positive_decimal(reference_price, "reference_price")
    if not isinstance(distance_type, OperatorPolicyDistanceType):
        raise TypeError("distance_type must be an OperatorPolicyDistanceType")
    if not isinstance(slide_mode, bool):
        raise TypeError("slide_mode must be a bool")

    max_distance_value = _positive_decimal(max_distance, "max_distance")
    if distance_type == OperatorPolicyDistanceType.PERCENT:
        distance = reference * max_distance_value
    else:
        raise ValueError(f"unsupported distance_type: {distance_type.value}")

    lower_bound = reference - distance
    upper_bound = reference + distance
    if current < lower_bound:
        target = lower_bound
    elif current > upper_bound:
        target = upper_bound
    else:
        target = current

    if not slide_mode or target == current:
        return target
    if max_step_per_reprice is None:
        raise ValueError("max_step_per_reprice is required when slide_mode is enabled")
    return slide_price_toward(
        current_price=current,
        desired_price=target,
        max_step=max_step_per_reprice,
    )


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    decimal = _decimal(value, field_name)
    if decimal <= 0:
        raise ValueError(f"{field_name} must be positive")
    return decimal


def _decimal(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not decimal.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return decimal


def _validate_tranche_schedule(values: tuple[Decimal, ...]) -> None:
    if not values:
        raise ValueError("tranche_schedule must not be empty")
    previous: Decimal | None = None
    for value in values:
        if not isinstance(value, Decimal):
            raise TypeError("tranche_schedule must contain Decimal values")
        if value <= 0:
            raise ValueError("tranche_schedule values must be positive")
        if value > Decimal("1"):
            raise ValueError("tranche_schedule values must be less than or equal to 1")
        if previous is not None and value <= previous:
            raise ValueError("tranche_schedule must be strictly increasing")
        previous = value
    if values[-1] != Decimal("1"):
        raise ValueError("tranche_schedule must end at 1")
