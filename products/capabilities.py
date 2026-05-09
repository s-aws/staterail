from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from core.enums import (
    OperatorPolicyVenue,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyHelperStatus,
    TimeInForce,
)
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductCatalog, ProductMetadata


@dataclass(frozen=True)
class VenueCapabilities:
    venue: ProductVenue | OperatorPolicyVenue
    status: StrategyHelperStatus
    product_venues: tuple[ProductVenue, ...] = ()
    supports_live_execution: bool = False
    supports_place_orders: bool = False
    supports_cancel_orders: bool = False
    supports_amend: bool = False
    supports_post_only: bool = False
    supports_reduce_only: bool = False
    supports_market_orders: bool = False
    supports_attached_orders: bool = False
    supported_order_types: tuple[OrderType, ...] = ()
    supported_time_in_force: tuple[TimeInForce, ...] = ()
    notes: tuple[str, ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.venue, ProductVenue | OperatorPolicyVenue):
            raise TypeError("venue must be a ProductVenue or OperatorPolicyVenue")
        if not isinstance(self.status, StrategyHelperStatus):
            raise TypeError("status must be a StrategyHelperStatus")
        _validate_enum_tuple(self.product_venues, ProductVenue, "product_venues")
        _validate_enum_tuple(self.supported_order_types, OrderType, "supported_order_types")
        _validate_enum_tuple(self.supported_time_in_force, TimeInForce, "supported_time_in_force")
        _validate_string_tuple(self.notes, "notes")
        if self.reason is not None and not self.reason:
            raise ValueError("reason must be non-empty when provided")

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyHelperStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _object_payload(
            {
                "notes": list(self.notes),
                "product_venues": [venue.value for venue in self.product_venues],
                "reason": self.reason,
                "status": self.status.value,
                "supported_order_types": [order_type.value for order_type in self.supported_order_types],
                "supported_time_in_force": [time_in_force.value for time_in_force in self.supported_time_in_force],
                "supports_amend": self.supports_amend,
                "supports_attached_orders": self.supports_attached_orders,
                "supports_cancel_orders": self.supports_cancel_orders,
                "supports_live_execution": self.supports_live_execution,
                "supports_market_orders": self.supports_market_orders,
                "supports_place_orders": self.supports_place_orders,
                "supports_post_only": self.supports_post_only,
                "supports_reduce_only": self.supports_reduce_only,
                "venue": self.venue.value,
            },
            "venue capabilities payload",
        )


@dataclass(frozen=True)
class ProductCapabilities:
    product_id: str
    status: StrategyHelperStatus
    product_type: ProductType | None = None
    product_venue: ProductVenue | None = None
    venue_capabilities: VenueCapabilities | None = None
    tradable_for_new_orders: bool = False
    supports_place_orders: bool = False
    supports_cancel_orders: bool = False
    supports_amend: bool = False
    supports_post_only: bool = False
    requires_post_only: bool = False
    supports_reduce_only: bool = False
    supports_market_orders: bool = False
    supports_attached_orders: bool = False
    supported_order_types: tuple[OrderType, ...] = ()
    supported_time_in_force: tuple[TimeInForce, ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.product_id, str) or not self.product_id:
            raise ValueError("product_id must be a non-empty string")
        if not isinstance(self.status, StrategyHelperStatus):
            raise TypeError("status must be a StrategyHelperStatus")
        if self.product_type is not None and not isinstance(self.product_type, ProductType):
            raise TypeError("product_type must be a ProductType when provided")
        if self.product_venue is not None and not isinstance(self.product_venue, ProductVenue):
            raise TypeError("product_venue must be a ProductVenue when provided")
        if self.venue_capabilities is not None and not isinstance(
            self.venue_capabilities,
            VenueCapabilities,
        ):
            raise TypeError("venue_capabilities must be VenueCapabilities when provided")
        _validate_enum_tuple(self.supported_order_types, OrderType, "supported_order_types")
        _validate_enum_tuple(self.supported_time_in_force, TimeInForce, "supported_time_in_force")
        if self.reason is not None and not self.reason:
            raise ValueError("reason must be non-empty when provided")

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyHelperStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _object_payload(
            {
                "product_id": self.product_id,
                "product_type": self.product_type.value if self.product_type is not None else None,
                "product_venue": self.product_venue.value if self.product_venue is not None else None,
                "reason": self.reason,
                "requires_post_only": self.requires_post_only,
                "status": self.status.value,
                "supported_order_types": [order_type.value for order_type in self.supported_order_types],
                "supported_time_in_force": [time_in_force.value for time_in_force in self.supported_time_in_force],
                "supports_amend": self.supports_amend,
                "supports_attached_orders": self.supports_attached_orders,
                "supports_cancel_orders": self.supports_cancel_orders,
                "supports_market_orders": self.supports_market_orders,
                "supports_place_orders": self.supports_place_orders,
                "supports_post_only": self.supports_post_only,
                "supports_reduce_only": self.supports_reduce_only,
                "tradable_for_new_orders": self.tradable_for_new_orders,
                "venue_capabilities": (
                    self.venue_capabilities.to_payload()
                    if self.venue_capabilities is not None
                    else None
                ),
            },
            "product capabilities payload",
        )


def venue_capabilities(venue: ProductVenue | OperatorPolicyVenue | str) -> VenueCapabilities:
    resolved = _venue_or_policy_venue(venue)
    if resolved == OperatorPolicyVenue.COINBASE_CFM:
        base = _coinbase_supported_capabilities(ProductVenue.FCM)
        return VenueCapabilities(
            **{
                **base.__dict__,
                "product_venues": (ProductVenue.FCM,),
                "venue": OperatorPolicyVenue.COINBASE_CFM,
            }
        )
    if resolved in {ProductVenue.CBE, ProductVenue.FCM}:
        return _coinbase_supported_capabilities(resolved)
    if resolved == ProductVenue.INTX:
        return VenueCapabilities(
            notes=(
                "Coinbase INTX metadata can be replayed, but this adapter does not enable live INTX order routing.",
            ),
            product_venues=(ProductVenue.INTX,),
            status=StrategyHelperStatus.OK,
            venue=ProductVenue.INTX,
        )
    return VenueCapabilities(
        notes=("No normalized capabilities are available for this venue.",),
        product_venues=(),
        reason="unknown_venue",
        status=StrategyHelperStatus.MISSING,
        venue=ProductVenue.UNKNOWN,
    )


def product_capabilities(
    product_catalog: ProductCatalog | None,
    product_id: str,
) -> ProductCapabilities:
    if not isinstance(product_id, str) or not product_id:
        raise ValueError("product_id must be a non-empty string")
    if product_catalog is None:
        return ProductCapabilities(
            product_id=product_id,
            reason="product_catalog_missing",
            status=StrategyHelperStatus.MISSING,
        )
    product = product_catalog.get(product_id)
    if product is None:
        return ProductCapabilities(
            product_id=product_id,
            reason="product_metadata_missing",
            status=StrategyHelperStatus.MISSING,
        )
    return product_capabilities_from_metadata(product)


def product_capabilities_from_metadata(product: ProductMetadata) -> ProductCapabilities:
    if not isinstance(product, ProductMetadata):
        raise TypeError("product must be ProductMetadata")
    venue = venue_capabilities(product.product_venue)
    supports_place_orders = venue.supports_place_orders and product.tradable_for_new_orders
    supports_limit_orders = supports_place_orders and product.allows_order_type(OrderType.LIMIT)
    supports_market_orders = (
        supports_place_orders
        and venue.supports_market_orders
        and product.allows_order_type(OrderType.MARKET)
    )
    supported_order_types = tuple(
        order_type
        for order_type, supported in (
            (OrderType.LIMIT, supports_limit_orders),
            (OrderType.MARKET, supports_market_orders),
        )
        if supported
    )
    return ProductCapabilities(
        product_id=product.product_id,
        product_type=product.product_type,
        product_venue=product.product_venue,
        reason=None if venue.is_ok else "venue_capabilities_missing",
        requires_post_only=product.post_only,
        status=StrategyHelperStatus.OK if venue.is_ok else StrategyHelperStatus.MISSING,
        supported_order_types=supported_order_types,
        supported_time_in_force=venue.supported_time_in_force if supported_order_types else (),
        supports_amend=venue.supports_amend and supports_place_orders,
        supports_attached_orders=venue.supports_attached_orders and supports_place_orders,
        supports_cancel_orders=venue.supports_cancel_orders,
        supports_market_orders=supports_market_orders,
        supports_place_orders=supports_place_orders,
        supports_post_only=supports_limit_orders and venue.supports_post_only,
        supports_reduce_only=venue.supports_reduce_only and supports_place_orders,
        tradable_for_new_orders=product.tradable_for_new_orders,
        venue_capabilities=venue,
    )


def _coinbase_supported_capabilities(venue: ProductVenue) -> VenueCapabilities:
    if venue not in {ProductVenue.CBE, ProductVenue.FCM}:
        raise ValueError("coinbase supported capabilities require CBE or FCM venue")
    return VenueCapabilities(
        notes=(
            "Capabilities reflect the current StateRail Coinbase adapter, not every exchange feature Coinbase may expose.",
            "Amend, reduce-only forwarding, and attached orders are intentionally not enabled in this adapter.",
        ),
        product_venues=(venue,),
        status=StrategyHelperStatus.OK,
        supported_order_types=(OrderType.LIMIT, OrderType.MARKET),
        supported_time_in_force=(
            TimeInForce.GOOD_UNTIL_CANCELLED,
            TimeInForce.IMMEDIATE_OR_CANCEL,
            TimeInForce.FILL_OR_KILL,
        ),
        supports_cancel_orders=True,
        supports_live_execution=True,
        supports_market_orders=True,
        supports_place_orders=True,
        supports_post_only=True,
        venue=venue,
    )


def _venue_or_policy_venue(value: ProductVenue | OperatorPolicyVenue | str) -> ProductVenue | OperatorPolicyVenue:
    if isinstance(value, ProductVenue | OperatorPolicyVenue):
        return value
    if not isinstance(value, str) or not value:
        raise TypeError("venue must be a ProductVenue, OperatorPolicyVenue, or non-empty string")
    try:
        return OperatorPolicyVenue(value)
    except ValueError:
        pass
    try:
        return ProductVenue(value)
    except ValueError:
        return ProductVenue.UNKNOWN


def _validate_enum_tuple(values: tuple[Enum, ...], enum_type: type[Enum], field_name: str) -> None:
    if not isinstance(values, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    if any(not isinstance(value, enum_type) for value in values):
        raise TypeError(f"{field_name} must contain {enum_type.__name__} values")


def _validate_string_tuple(values: tuple[str, ...], field_name: str) -> None:
    if not isinstance(values, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    if any(not isinstance(value, str) or not value for value in values):
        raise TypeError(f"{field_name} must contain non-empty strings")


def _object_payload(payload: Mapping[str, Any], field_name: str) -> dict[str, JsonValue]:
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must normalize to a JSON object")
    return normalized
