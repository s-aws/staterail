from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import IncrementRoundingMode, ProductRuleCheckStatus, ProductRuleFailure
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductMetadata


AmountInput = Decimal | str | int | float


@dataclass(frozen=True)
class ProductRuleValidation:
    product_id: str
    status: ProductRuleCheckStatus
    failures: tuple[ProductRuleFailure, ...] = ()
    size: Decimal | None = None
    price: Decimal | None = None
    notional: Decimal | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == ProductRuleCheckStatus.ACCEPTED

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "failures": tuple(failure.value for failure in self.failures),
                "is_ok": self.is_ok,
                "notional": self.notional,
                "price": self.price,
                "product_id": self.product_id,
                "size": self.size,
                "status": self.status,
            }
        )


@dataclass(frozen=True)
class ProductRuleProposal:
    product_id: str
    status: ProductRuleCheckStatus
    mode: IncrementRoundingMode
    failures: tuple[ProductRuleFailure, ...] = ()
    original_value: Decimal | None = None
    proposed_value: Decimal | None = None
    increment: Decimal | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == ProductRuleCheckStatus.ACCEPTED

    @property
    def changed(self) -> bool:
        return (
            self.original_value is not None
            and self.proposed_value is not None
            and self.original_value != self.proposed_value
        )

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "changed": self.changed,
                "failures": tuple(failure.value for failure in self.failures),
                "increment": self.increment,
                "is_ok": self.is_ok,
                "mode": self.mode,
                "original_value": self.original_value,
                "product_id": self.product_id,
                "proposed_value": self.proposed_value,
                "status": self.status,
            }
        )


def validate_order_size(product: ProductMetadata, size: AmountInput) -> ProductRuleValidation:
    _validate_product(product)
    parsed_size, failures = _positive_decimal_or_failures(size)
    if parsed_size is not None:
        failures = (
            *failures,
            *_size_rule_failures(product=product, size=parsed_size),
        )
    return ProductRuleValidation(
        failures=_unique_failures(failures),
        product_id=product.product_id,
        size=parsed_size,
        status=_status(failures),
    )


def validate_limit_price(product: ProductMetadata, price: AmountInput) -> ProductRuleValidation:
    _validate_product(product)
    parsed_price, failures = _positive_decimal_or_failures(price)
    if (
        parsed_price is not None
        and product.price_increment is not None
        and product.price_increment > 0
        and parsed_price % product.price_increment != 0
    ):
        failures = (*failures, ProductRuleFailure.PRICE_INCREMENT)
    return ProductRuleValidation(
        failures=_unique_failures(failures),
        price=parsed_price,
        product_id=product.product_id,
        status=_status(failures),
    )


def validate_notional(
    product: ProductMetadata,
    *,
    price: AmountInput,
    size: AmountInput,
) -> ProductRuleValidation:
    _validate_product(product)
    parsed_size, size_failures = _positive_decimal_or_failures(size)
    parsed_price, price_failures = _positive_decimal_or_failures(price)
    failures = (*size_failures, *price_failures)
    notional = product.notional(parsed_size, parsed_price)
    if notional is None:
        failures = (*failures, ProductRuleFailure.NOTIONAL_REQUIRES_PRICE)
    else:
        if product.quote_min_size is not None and notional < product.quote_min_size:
            failures = (*failures, ProductRuleFailure.NOTIONAL_BELOW_MIN)
        if product.quote_max_size is not None and notional > product.quote_max_size:
            failures = (*failures, ProductRuleFailure.NOTIONAL_ABOVE_MAX)
    return ProductRuleValidation(
        failures=_unique_failures(failures),
        notional=notional,
        price=parsed_price,
        product_id=product.product_id,
        size=parsed_size,
        status=_status(failures),
    )


def price_tick_proposal(
    product: ProductMetadata,
    *,
    mode: IncrementRoundingMode,
    price: AmountInput,
) -> ProductRuleProposal:
    _validate_product(product)
    return _increment_proposal(
        product=product,
        increment=product.price_increment,
        mode=mode,
        value=price,
    )


def size_increment_proposal(
    product: ProductMetadata,
    *,
    mode: IncrementRoundingMode,
    size: AmountInput,
) -> ProductRuleProposal:
    _validate_product(product)
    return _increment_proposal(
        product=product,
        increment=product.base_increment,
        mode=mode,
        value=size,
    )


def _increment_proposal(
    *,
    product: ProductMetadata,
    increment: Decimal | None,
    mode: IncrementRoundingMode,
    value: AmountInput,
) -> ProductRuleProposal:
    if not isinstance(mode, IncrementRoundingMode):
        raise TypeError("mode must be an IncrementRoundingMode")
    parsed_value, failures = _positive_decimal_or_failures(value)
    if parsed_value is None:
        return ProductRuleProposal(
            failures=_unique_failures(failures),
            mode=mode,
            product_id=product.product_id,
            status=ProductRuleCheckStatus.REJECTED,
        )
    proposed_value = _round_to_increment(parsed_value, increment, mode=mode)
    if proposed_value <= 0:
        failures = (*failures, ProductRuleFailure.VALUE_NOT_POSITIVE)
    return ProductRuleProposal(
        failures=_unique_failures(failures),
        increment=increment if increment is not None and increment > 0 else None,
        mode=mode,
        original_value=parsed_value,
        product_id=product.product_id,
        proposed_value=proposed_value,
        status=_status(failures),
    )


def _round_to_increment(
    value: Decimal,
    increment: Decimal | None,
    *,
    mode: IncrementRoundingMode,
) -> Decimal:
    if increment is None or increment <= 0:
        return value
    floor = ((value // increment) * increment).quantize(increment)
    if value == floor:
        return floor
    ceiling = (((value // increment) + 1) * increment).quantize(increment)
    if mode == IncrementRoundingMode.DOWN:
        return floor
    if mode == IncrementRoundingMode.UP:
        return ceiling
    if mode == IncrementRoundingMode.NEAREST:
        return ceiling if (ceiling - value) <= (value - floor) else floor
    raise ValueError(f"unsupported rounding mode: {mode.value}")


def _size_rule_failures(
    *,
    product: ProductMetadata,
    size: Decimal,
) -> tuple[ProductRuleFailure, ...]:
    failures: list[ProductRuleFailure] = []
    if product.base_min_size is not None and size < product.base_min_size:
        failures.append(ProductRuleFailure.SIZE_BELOW_MIN)
    if product.base_max_size is not None and size > product.base_max_size:
        failures.append(ProductRuleFailure.SIZE_ABOVE_MAX)
    if (
        product.base_increment is not None
        and product.base_increment > 0
        and size % product.base_increment != 0
    ):
        failures.append(ProductRuleFailure.SIZE_INCREMENT)
    return tuple(failures)


def _positive_decimal_or_failures(
    value: AmountInput,
) -> tuple[Decimal | None, tuple[ProductRuleFailure, ...]]:
    if isinstance(value, bool):
        return None, (ProductRuleFailure.VALUE_NOT_DECIMAL_COMPATIBLE,)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None, (ProductRuleFailure.VALUE_NOT_DECIMAL_COMPATIBLE,)
    if not parsed.is_finite():
        return None, (ProductRuleFailure.VALUE_NOT_DECIMAL_COMPATIBLE,)
    if parsed <= 0:
        return parsed, (ProductRuleFailure.VALUE_NOT_POSITIVE,)
    return parsed, ()


def _status(failures: tuple[ProductRuleFailure, ...]) -> ProductRuleCheckStatus:
    return (
        ProductRuleCheckStatus.REJECTED
        if failures
        else ProductRuleCheckStatus.ACCEPTED
    )


def _unique_failures(failures: tuple[ProductRuleFailure, ...]) -> tuple[ProductRuleFailure, ...]:
    return tuple(dict.fromkeys(failures))


def _validate_product(product: ProductMetadata) -> None:
    if not isinstance(product, ProductMetadata):
        raise TypeError("product must be ProductMetadata")


def _payload(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(
        {
            key: _json_safe(value)
            for key, value in raw.items()
        }
    )
    if not isinstance(normalized, dict):
        raise TypeError("product rule helper payload must normalize to an object")
    return normalized


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value
