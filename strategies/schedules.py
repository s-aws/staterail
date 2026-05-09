from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import ProductRuleFailure, ScheduledSliceStatus
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductMetadata
from strategies.product_rules import validate_order_size


AmountInput = Decimal | str | int | float


@dataclass(frozen=True)
class ScheduledSlicePlan:
    strategy_id: str
    product_id: str
    schedule_id: str
    status: ScheduledSliceStatus
    evaluated_at: datetime
    interval: timedelta
    total_size: Decimal
    slices: int
    completed_slice_count: int
    remaining_slice_count: int
    completed_action_ids: tuple[str, ...] = ()
    due_in_seconds: float | None = None
    next_due_at: datetime | None = None
    reasons: tuple[str, ...] = ()
    scheduled_start_at: datetime | None = None
    size_failures: tuple[ProductRuleFailure, ...] = ()
    slice_index: int | None = None
    slice_size: Decimal | None = None
    suggested_action_id: str | None = None
    suggested_client_order_id: str | None = None

    @property
    def is_due(self) -> bool:
        return self.status == ScheduledSliceStatus.DUE

    @property
    def is_complete(self) -> bool:
        return self.status == ScheduledSliceStatus.COMPLETE

    @property
    def is_blocked(self) -> bool:
        return self.status == ScheduledSliceStatus.BLOCKED

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "completed_action_ids": self.completed_action_ids,
                "completed_slice_count": self.completed_slice_count,
                "due_in_seconds": self.due_in_seconds,
                "evaluated_at": self.evaluated_at,
                "interval_seconds": self.interval.total_seconds(),
                "is_blocked": self.is_blocked,
                "is_complete": self.is_complete,
                "is_due": self.is_due,
                "next_due_at": self.next_due_at,
                "product_id": self.product_id,
                "reasons": self.reasons,
                "remaining_slice_count": self.remaining_slice_count,
                "schedule_id": self.schedule_id,
                "scheduled_start_at": self.scheduled_start_at,
                "size_failures": tuple(failure.value for failure in self.size_failures),
                "slice_index": self.slice_index,
                "slice_size": self.slice_size,
                "slices": self.slices,
                "status": self.status,
                "strategy_id": self.strategy_id,
                "suggested_action_id": self.suggested_action_id,
                "suggested_client_order_id": self.suggested_client_order_id,
                "total_size": self.total_size,
            }
        )


def scheduled_slice_sizes(
    product: ProductMetadata,
    *,
    slices: int,
    total_size: AmountInput,
) -> tuple[tuple[Decimal, ...], tuple[ProductRuleFailure, ...], tuple[str, ...]]:
    if not isinstance(product, ProductMetadata):
        raise TypeError("product must be ProductMetadata")
    if not isinstance(slices, int) or isinstance(slices, bool):
        raise TypeError("slices must be an integer")
    if slices <= 0:
        raise ValueError("slices must be positive")

    total = _positive_decimal(total_size, "total_size")
    slice_size = total / Decimal(slices)
    reasons: tuple[str, ...] = ()
    if slice_size * Decimal(slices) != total:
        reasons = ("total_size cannot be split evenly into the requested slice count",)

    failures = _unique_failures(
        tuple(
            failure
            for _ in range(slices)
            for failure in validate_order_size(product, slice_size).failures
        )
    )
    if failures or reasons:
        return (), failures, reasons
    return tuple(slice_size for _ in range(slices)), (), ()


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
        raise TypeError("scheduled slice plan payload must normalize to an object")
    return normalized


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value
