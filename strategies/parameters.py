from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any


def reject_unknown_parameters(
    strategy_id: str,
    parameters: Mapping[str, Any],
    allowed: set[str],
) -> None:
    if not isinstance(strategy_id, str) or not strategy_id:
        raise ValueError("strategy_id must be a non-empty string")
    if not isinstance(parameters, Mapping):
        raise TypeError(f"{strategy_id} parameters must be a mapping")
    unknown = tuple(sorted(str(key) for key in parameters if key not in allowed))
    if unknown:
        raise ValueError(f"unknown {strategy_id} parameter(s): {', '.join(unknown)}")


def decimal_parameter(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def positive_int_parameter(value: Any, field_name: str) -> int:
    return int_parameter_at_least(value, field_name, minimum=1)


def int_parameter_at_least(value: Any, field_name: str, *, minimum: int) -> int:
    if not isinstance(minimum, int) or isinstance(minimum, bool):
        raise TypeError("minimum must be an integer")
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if str(value) != str(parsed):
        raise ValueError(f"{field_name} must be an integer")
    if parsed < minimum:
        if minimum == 1:
            raise ValueError(f"{field_name} must be positive")
        raise ValueError(f"{field_name} must be at least {minimum}")
    return parsed


def bool_parameter(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be a bool")
    return value
