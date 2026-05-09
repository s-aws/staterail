from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from threading import RLock
from typing import Any

from core.enums import OrderType, ProductType, ProductVenue
from core.json_tools import JsonValue, normalize_json


@dataclass(frozen=True)
class ProductMetadata:
    product_id: str
    product_type: ProductType
    product_venue: ProductVenue = ProductVenue.UNKNOWN
    contract_size: Decimal | None = None
    base_increment: Decimal | None = None
    quote_increment: Decimal | None = None
    price_increment: Decimal | None = None
    base_min_size: Decimal | None = None
    base_max_size: Decimal | None = None
    quote_min_size: Decimal | None = None
    quote_max_size: Decimal | None = None
    trading_disabled: bool = False
    cancel_only: bool = False
    limit_only: bool = False
    post_only: bool = False
    view_only: bool = False
    is_disabled: bool = False
    raw: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_coinbase_payload(cls, payload: Mapping[str, Any]) -> "ProductMetadata":
        return _metadata_from_payload(payload, raw=payload)

    @classmethod
    def from_audit_payload(cls, payload: Mapping[str, Any]) -> "ProductMetadata":
        raw = payload.get("raw")
        return _metadata_from_payload(payload, raw=raw if isinstance(raw, Mapping) else payload)

    @property
    def tradable_for_new_orders(self) -> bool:
        return not any((self.trading_disabled, self.cancel_only, self.view_only, self.is_disabled))

    def allows_order_type(self, order_type: OrderType | None) -> bool:
        if order_type is None:
            return False
        if self.limit_only and order_type != OrderType.LIMIT:
            return False
        return True

    def size_is_valid(self, size: Decimal | None) -> bool:
        if size is None:
            return False
        if self.base_min_size is not None and size < self.base_min_size:
            return False
        if self.base_max_size is not None and size > self.base_max_size:
            return False
        if self.base_increment is not None and not _is_multiple(size, self.base_increment):
            return False
        return True

    def price_is_valid(self, price: Decimal | None) -> bool:
        if price is None:
            return False
        if self.price_increment is not None and not _is_multiple(price, self.price_increment):
            return False
        return True

    def notional_is_valid(self, size: Decimal | None, price: Decimal | None) -> bool:
        if self.quote_min_size is None and self.quote_max_size is None:
            return True
        notional = self.notional(size, price)
        if notional is None:
            return False
        if self.quote_min_size is not None and notional < self.quote_min_size:
            return False
        if self.quote_max_size is not None and notional > self.quote_max_size:
            return False
        return True

    @property
    def notional_multiplier(self) -> Decimal:
        if (
            self.product_type == ProductType.FUTURE
            and self.contract_size is not None
            and self.contract_size > 0
        ):
            return self.contract_size
        return Decimal("1")

    def notional(self, size: Decimal | None, price: Decimal | None) -> Decimal | None:
        if size is None or price is None:
            return None
        return size * price * self.notional_multiplier

    def minimum_valid_size(self, reference_price: Decimal) -> Decimal | None:
        if reference_price <= 0:
            return None
        minimum = Decimal("0")
        if self.base_min_size is not None:
            minimum = max(minimum, self.base_min_size)
        if self.quote_min_size is not None:
            minimum = max(
                minimum,
                self.quote_min_size / (reference_price * self.notional_multiplier),
            )
        if minimum <= 0:
            return None
        return _ceil_to_increment(minimum, self.base_increment)

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "base_increment": _decimal_payload(self.base_increment),
            "base_max_size": _decimal_payload(self.base_max_size),
            "base_min_size": _decimal_payload(self.base_min_size),
            "cancel_only": self.cancel_only,
            "contract_size": _decimal_payload(self.contract_size),
            "is_disabled": self.is_disabled,
            "limit_only": self.limit_only,
            "post_only": self.post_only,
            "price_increment": _decimal_payload(self.price_increment),
            "product_id": self.product_id,
            "product_type": self.product_type,
            "product_venue": self.product_venue,
            "quote_increment": _decimal_payload(self.quote_increment),
            "quote_max_size": _decimal_payload(self.quote_max_size),
            "quote_min_size": _decimal_payload(self.quote_min_size),
            "raw": _json_safe(self.raw),
            "trading_disabled": self.trading_disabled,
            "tradable_for_new_orders": self.tradable_for_new_orders,
            "view_only": self.view_only,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Product metadata payload must normalize to an object")
        return normalized


def _metadata_from_payload(payload: Mapping[str, Any], *, raw: Mapping[str, Any]) -> ProductMetadata:
    return ProductMetadata(
        product_id=_required_string(payload, "product_id"),
        product_type=_product_type(payload.get("product_type")),
        product_venue=_product_venue(payload.get("product_venue")),
        contract_size=_contract_size(payload, raw),
        base_increment=_decimal_or_none(payload.get("base_increment")),
        quote_increment=_decimal_or_none(payload.get("quote_increment")),
        price_increment=_decimal_or_none(payload.get("price_increment")),
        base_min_size=_decimal_or_none(payload.get("base_min_size")),
        base_max_size=_decimal_or_none(payload.get("base_max_size")),
        quote_min_size=_decimal_or_none(payload.get("quote_min_size")),
        quote_max_size=_decimal_or_none(payload.get("quote_max_size")),
        trading_disabled=bool(payload.get("trading_disabled", False)),
        cancel_only=bool(payload.get("cancel_only", False)),
        limit_only=bool(payload.get("limit_only", False)),
        post_only=bool(payload.get("post_only", False)),
        view_only=bool(payload.get("view_only", False)),
        is_disabled=bool(payload.get("is_disabled", False)),
        raw=raw,
    )


def _contract_size(payload: Mapping[str, Any], raw: Mapping[str, Any]) -> Decimal | None:
    direct = _decimal_or_none(payload.get("contract_size"))
    if direct is not None:
        return direct
    future_details = raw.get("future_product_details")
    if isinstance(future_details, Mapping):
        return _decimal_or_none(future_details.get("contract_size"))
    return None


class ProductCatalog:
    def __init__(self, products: Iterable[ProductMetadata] = ()) -> None:
        self._lock = RLock()
        self._products = {product.product_id: product for product in products}

    @classmethod
    def from_coinbase_payloads(cls, payloads: Iterable[Mapping[str, Any]]) -> "ProductCatalog":
        return cls(ProductMetadata.from_coinbase_payload(payload) for payload in payloads)

    def add(self, product: ProductMetadata) -> None:
        with self._lock:
            self._products[product.product_id] = product

    def get(self, product_id: str | None) -> ProductMetadata | None:
        if product_id is None:
            return None
        with self._lock:
            return self._products.get(product_id)

    def require(self, product_id: str) -> ProductMetadata:
        product = self.get(product_id)
        if product is None:
            raise KeyError(product_id)
        return product

    def values(self) -> tuple[ProductMetadata, ...]:
        with self._lock:
            return tuple(self._products.values())

    def update(self, products: Iterable[ProductMetadata]) -> None:
        with self._lock:
            for product in products:
                self._products[product.product_id] = product


def _required_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _product_type(value: Any) -> ProductType:
    try:
        return ProductType(value)
    except (TypeError, ValueError):
        return ProductType.UNKNOWN


def _product_venue(value: Any) -> ProductVenue:
    try:
        return ProductVenue(value)
    except (TypeError, ValueError):
        return ProductVenue.UNKNOWN


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not decimal.is_finite():
        return None
    return decimal


def _is_multiple(value: Decimal, increment: Decimal) -> bool:
    if increment <= 0:
        return True
    return value % increment == 0


def _ceil_to_increment(value: Decimal, increment: Decimal | None) -> Decimal:
    if increment is None or increment <= 0:
        return value
    if value == 0 or value % increment == 0:
        return value.quantize(increment)
    return (((value // increment) + 1) * increment).quantize(increment)


def _decimal_payload(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _json_safe(value: Any) -> JsonValue:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe(item) for item in value]
    return normalize_json(value)
