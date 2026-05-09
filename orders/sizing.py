from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from core.enums import OrderLineageRelation, OrderSizingDecisionStatus
from core.json_tools import JsonValue
from products.catalog import ProductMetadata


AmountInput = Decimal | str | int


@dataclass(frozen=True)
class OrderSizingDecision:
    status: OrderSizingDecisionStatus
    lineage_relation: OrderLineageRelation
    product_id: str
    requested_sizes: tuple[Decimal, ...]
    output_sizes: tuple[Decimal, ...]
    limit_price: Decimal | None = None
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, OrderSizingDecisionStatus):
            raise TypeError("status must be an OrderSizingDecisionStatus")
        if not isinstance(self.lineage_relation, OrderLineageRelation):
            raise TypeError("lineage_relation must be an OrderLineageRelation")
        if not isinstance(self.product_id, str) or not self.product_id:
            raise ValueError("product_id is required")
        _validate_decimal_tuple("requested_sizes", self.requested_sizes)
        _validate_decimal_tuple("output_sizes", self.output_sizes)
        if self.limit_price is not None and not isinstance(self.limit_price, Decimal):
            raise TypeError("limit_price must be a Decimal when provided")
        if not isinstance(self.reasons, tuple) or any(not isinstance(reason, str) or not reason for reason in self.reasons):
            raise ValueError("reasons must be a tuple of non-empty strings")

    @property
    def accepted(self) -> bool:
        return self.status == OrderSizingDecisionStatus.ACCEPTED

    def single_output_size(self) -> str:
        if not self.accepted:
            raise ValueError("accepted sizing decision required")
        if len(self.output_sizes) != 1:
            raise ValueError("sizing decision must have exactly one output size")
        output_size = _decimal_payload(self.output_sizes[0])
        if output_size is None:
            raise ValueError("sizing decision output size is required")
        return output_size

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "limit_price": _decimal_payload(self.limit_price),
            "lineage_relation": self.lineage_relation.value,
            "output_sizes": [_decimal_payload(size) for size in self.output_sizes],
            "product_id": self.product_id,
            "reasons": list(self.reasons),
            "requested_sizes": [_decimal_payload(size) for size in self.requested_sizes],
            "status": self.status.value,
        }


@dataclass(frozen=True)
class LineageSizingPolicy:
    product: ProductMetadata
    allow_partial_followup: bool = True
    partial_followup_min_size: Decimal | None = None
    partial_followup_min_fraction: Decimal | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.product, ProductMetadata):
            raise TypeError("product must be ProductMetadata")
        if not isinstance(self.allow_partial_followup, bool):
            raise TypeError("allow_partial_followup must be bool")
        _validate_optional_positive_decimal("partial_followup_min_size", self.partial_followup_min_size)
        _validate_optional_fraction("partial_followup_min_fraction", self.partial_followup_min_fraction)

    @classmethod
    def from_values(
        cls,
        *,
        product: ProductMetadata,
        allow_partial_followup: bool = True,
        partial_followup_min_size: AmountInput | None = None,
        partial_followup_min_fraction: AmountInput | None = None,
    ) -> "LineageSizingPolicy":
        return cls(
            product=product,
            allow_partial_followup=allow_partial_followup,
            partial_followup_min_size=_decimal_or_none(partial_followup_min_size, "partial_followup_min_size"),
            partial_followup_min_fraction=_decimal_or_none(
                partial_followup_min_fraction,
                "partial_followup_min_fraction",
            ),
        )

    def followup_size(
        self,
        *,
        parent_size: AmountInput,
        filled_size: AmountInput,
        limit_price: AmountInput | None = None,
    ) -> OrderSizingDecision:
        parent = _positive_decimal(parent_size, "parent_size")
        filled = _positive_decimal(filled_size, "filled_size")
        price = _decimal_or_none(limit_price, "limit_price")
        reasons: list[str] = []

        if filled > parent:
            reasons.append("filled_size cannot exceed parent_size")
        if not self.allow_partial_followup and filled != parent:
            reasons.append("partial followup orders are disabled")
        if self.partial_followup_min_size is not None and filled < self.partial_followup_min_size:
            reasons.append("filled_size is below configured partial followup minimum size")
        if self.partial_followup_min_fraction is not None and filled / parent < self.partial_followup_min_fraction:
            reasons.append("filled_size is below configured partial followup minimum fraction")
        if reasons:
            return self._rejected(
                OrderLineageRelation.FOLLOWUP_AFTER_FILL,
                requested_sizes=(filled,),
                limit_price=price,
                reasons=tuple(reasons),
            )
        return self._evaluate_output_sizes(
            OrderLineageRelation.FOLLOWUP_AFTER_FILL,
            requested_sizes=(filled,),
            output_sizes=(filled,),
            limit_price=price,
        )

    def split_sizes(
        self,
        *,
        total_size: AmountInput,
        child_count: int,
        limit_price: AmountInput | None = None,
    ) -> OrderSizingDecision:
        total = _positive_decimal(total_size, "total_size")
        price = _decimal_or_none(limit_price, "limit_price")
        if not isinstance(child_count, int) or isinstance(child_count, bool):
            raise TypeError("child_count must be an integer")
        if child_count < 2:
            return self._rejected(
                OrderLineageRelation.SPLIT_CHILD,
                requested_sizes=(total,),
                limit_price=price,
                reasons=("child_count must be at least 2",),
            )

        child_size = total / Decimal(child_count)
        if child_size * child_count != total:
            return self._rejected(
                OrderLineageRelation.SPLIT_CHILD,
                requested_sizes=(total,),
                limit_price=price,
                reasons=("total_size cannot be split evenly without changing total exposure",),
            )
        return self._evaluate_output_sizes(
            OrderLineageRelation.SPLIT_CHILD,
            requested_sizes=(total,),
            output_sizes=tuple(child_size for _ in range(child_count)),
            limit_price=price,
        )

    def consolidated_size(
        self,
        *,
        source_sizes: Iterable[AmountInput],
        limit_price: AmountInput | None = None,
    ) -> OrderSizingDecision:
        sizes = tuple(_positive_decimal(size, "source_size") for size in source_sizes)
        price = _decimal_or_none(limit_price, "limit_price")
        if len(sizes) < 2:
            return self._rejected(
                OrderLineageRelation.CONSOLIDATION,
                requested_sizes=sizes,
                limit_price=price,
                reasons=("consolidation requires at least two source sizes",),
            )
        total = sum(sizes, Decimal("0"))
        return self._evaluate_output_sizes(
            OrderLineageRelation.CONSOLIDATION,
            requested_sizes=sizes,
            output_sizes=(total,),
            limit_price=price,
        )

    def staged_release_sizes(
        self,
        *,
        total_size: AmountInput,
        limit_price: AmountInput,
        max_visible_notional: AmountInput,
        max_release_count: int | None = None,
    ) -> OrderSizingDecision:
        total = _positive_decimal(total_size, "total_size")
        price = _positive_decimal(limit_price, "limit_price")
        visible_cap = _positive_decimal(max_visible_notional, "max_visible_notional")
        if max_release_count is not None:
            if not isinstance(max_release_count, int) or isinstance(max_release_count, bool):
                raise TypeError("max_release_count must be an integer when provided")
            if max_release_count <= 0:
                raise ValueError("max_release_count must be positive when provided")

        max_release_size = _maximum_release_size(
            product=self.product,
            limit_price=price,
            max_visible_notional=visible_cap,
        )
        min_release_size = _minimum_release_size(self.product, price)
        if max_release_size is None or max_release_size < min_release_size:
            return self._rejected(
                OrderLineageRelation.ROOT,
                requested_sizes=(total,),
                limit_price=price,
                reasons=("max_visible_notional is below minimum valid release size",),
            )

        outputs = _staged_release_outputs(
            product=self.product,
            total_size=total,
            limit_price=price,
            max_release_size=max_release_size,
            min_release_size=min_release_size,
            max_visible_notional=visible_cap,
        )
        if outputs is None:
            return self._rejected(
                OrderLineageRelation.ROOT,
                requested_sizes=(total,),
                limit_price=price,
                reasons=("total_size cannot be split into valid staged releases",),
            )
        if max_release_count is not None and len(outputs) > max_release_count:
            return self._rejected(
                OrderLineageRelation.ROOT,
                requested_sizes=(total,),
                limit_price=price,
                reasons=("staged release count exceeds configured maximum",),
            )
        visible_cap_breaches = tuple(
            output_size
            for output_size in outputs
            if (self.product.notional(output_size, price) or Decimal("0")) > visible_cap
        )
        if visible_cap_breaches:
            return self._rejected(
                OrderLineageRelation.ROOT,
                requested_sizes=(total,),
                limit_price=price,
                reasons=("output notional exceeds max_visible_notional",),
            )
        return self._evaluate_output_sizes(
            OrderLineageRelation.ROOT,
            requested_sizes=(total,),
            output_sizes=outputs,
            limit_price=price,
        )

    def _evaluate_output_sizes(
        self,
        lineage_relation: OrderLineageRelation,
        *,
        requested_sizes: tuple[Decimal, ...],
        output_sizes: tuple[Decimal, ...],
        limit_price: Decimal | None,
    ) -> OrderSizingDecision:
        reasons: list[str] = []
        if limit_price is not None and not self.product.price_is_valid(limit_price):
            reasons.append("limit_price violates product price increment")
        for output_size in output_sizes:
            if not self.product.size_is_valid(output_size):
                reasons.append("output size violates product base size rules")
            if _quote_rules_required(self.product) and limit_price is None:
                reasons.append("limit_price is required to evaluate product quote size rules")
            elif not self.product.notional_is_valid(output_size, limit_price):
                reasons.append("output notional violates product quote size rules")
        if reasons:
            return self._rejected(
                lineage_relation,
                requested_sizes=requested_sizes,
                limit_price=limit_price,
                reasons=_unique_reasons(reasons),
            )
        return OrderSizingDecision(
            status=OrderSizingDecisionStatus.ACCEPTED,
            lineage_relation=lineage_relation,
            product_id=self.product.product_id,
            requested_sizes=requested_sizes,
            output_sizes=output_sizes,
            limit_price=limit_price,
        )

    def _rejected(
        self,
        lineage_relation: OrderLineageRelation,
        *,
        requested_sizes: tuple[Decimal, ...],
        limit_price: Decimal | None,
        reasons: tuple[str, ...],
    ) -> OrderSizingDecision:
        return OrderSizingDecision(
            status=OrderSizingDecisionStatus.REJECTED,
            lineage_relation=lineage_relation,
            product_id=self.product.product_id,
            requested_sizes=requested_sizes,
            output_sizes=(),
            limit_price=limit_price,
            reasons=reasons,
        )


def _quote_rules_required(product: ProductMetadata) -> bool:
    return product.quote_min_size is not None or product.quote_max_size is not None


def _staged_release_outputs(
    *,
    product: ProductMetadata,
    total_size: Decimal,
    limit_price: Decimal,
    max_release_size: Decimal,
    min_release_size: Decimal,
    max_visible_notional: Decimal,
) -> tuple[Decimal, ...] | None:
    if total_size <= max_release_size:
        return (total_size,)

    outputs: list[Decimal] = []
    remaining = total_size
    while remaining > max_release_size:
        outputs.append(max_release_size)
        remaining -= max_release_size

    if remaining == 0:
        return tuple(outputs)
    if _release_size_is_valid(
        product=product,
        size=remaining,
        limit_price=limit_price,
        max_visible_notional=max_visible_notional,
    ):
        outputs.append(remaining)
        return tuple(outputs)
    return _rebalance_final_release(
        product=product,
        outputs=outputs,
        remaining=remaining,
        limit_price=limit_price,
        min_release_size=min_release_size,
        max_visible_notional=max_visible_notional,
    )


def _rebalance_final_release(
    *,
    product: ProductMetadata,
    outputs: list[Decimal],
    remaining: Decimal,
    limit_price: Decimal,
    min_release_size: Decimal,
    max_visible_notional: Decimal,
) -> tuple[Decimal, ...] | None:
    if not outputs:
        return None
    target_final_size = _ceil_to_increment(
        max(remaining, min_release_size),
        product.base_increment,
    )
    amount_to_borrow = target_final_size - remaining
    if amount_to_borrow <= 0:
        return None
    adjusted_previous = outputs[-1] - amount_to_borrow
    if not _release_size_is_valid(
        product=product,
        size=adjusted_previous,
        limit_price=limit_price,
        max_visible_notional=max_visible_notional,
    ):
        return None
    if not _release_size_is_valid(
        product=product,
        size=target_final_size,
        limit_price=limit_price,
        max_visible_notional=max_visible_notional,
    ):
        return None
    return (*outputs[:-1], adjusted_previous, target_final_size)


def _maximum_release_size(
    *,
    product: ProductMetadata,
    limit_price: Decimal,
    max_visible_notional: Decimal,
) -> Decimal | None:
    visible_size = max_visible_notional / (limit_price * product.notional_multiplier)
    if product.base_max_size is not None:
        visible_size = min(visible_size, product.base_max_size)
    floored = _floor_to_increment(visible_size, product.base_increment)
    if floored <= 0:
        return None
    return floored


def _minimum_release_size(product: ProductMetadata, limit_price: Decimal) -> Decimal:
    minimum = Decimal("0")
    if product.base_min_size is not None:
        minimum = max(minimum, product.base_min_size)
    if product.quote_min_size is not None:
        minimum = max(minimum, product.quote_min_size / (limit_price * product.notional_multiplier))
    return _ceil_to_increment(minimum, product.base_increment)


def _release_size_is_valid(
    *,
    product: ProductMetadata,
    size: Decimal,
    limit_price: Decimal,
    max_visible_notional: Decimal,
) -> bool:
    return (
        product.size_is_valid(size)
        and product.notional_is_valid(size, limit_price)
        and (product.notional(size, limit_price) or Decimal("0")) <= max_visible_notional
    )


def _floor_to_increment(value: Decimal, increment: Decimal | None) -> Decimal:
    if increment is None or increment <= 0:
        return value
    return (value // increment) * increment


def _ceil_to_increment(value: Decimal, increment: Decimal | None) -> Decimal:
    if increment is None or increment <= 0:
        return value
    if value == 0 or value % increment == 0:
        return value
    return ((value // increment) + 1) * increment


def _decimal_or_none(value: AmountInput | None, field_name: str) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value, field_name)


def _positive_decimal(value: AmountInput, field_name: str) -> Decimal:
    decimal = _decimal(value, field_name)
    if decimal <= 0:
        raise ValueError(f"{field_name} must be positive")
    return decimal


def _decimal(value: AmountInput, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be a decimal-compatible value")
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not decimal.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return decimal


def _validate_optional_positive_decimal(field_name: str, value: Decimal | None) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal when provided")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _validate_optional_fraction(field_name: str, value: Decimal | None) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal when provided")
    if value <= 0 or value > 1:
        raise ValueError(f"{field_name} must be greater than 0 and less than or equal to 1")


def _validate_decimal_tuple(field_name: str, values: tuple[Decimal, ...]) -> None:
    if not isinstance(values, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    if any(not isinstance(value, Decimal) for value in values):
        raise TypeError(f"{field_name} must contain only Decimal values")


def _unique_reasons(reasons: list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(reasons))


def _decimal_payload(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None
