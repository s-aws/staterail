from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from core.enums import OrderSide, RiskCheckStatus, StrategyHelperStatus
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductCatalog
from projections.state import SourceOfTruthProjection
from risk.gate import daily_notional_usage, live_open_orders, order_notional_value

if TYPE_CHECKING:
    from strategies.operator_policy import OperatorPolicy


@dataclass(frozen=True)
class ProductExposure:
    product_id: str
    status: StrategyHelperStatus
    open_order_count: int = 0
    open_buy_order_count: int = 0
    open_sell_order_count: int = 0
    open_order_notional: Decimal = Decimal("0")
    open_buy_notional: Decimal = Decimal("0")
    open_sell_notional: Decimal = Decimal("0")
    net_size: Decimal = Decimal("0")
    gross_buy_size: Decimal = Decimal("0")
    gross_sell_size: Decimal = Decimal("0")
    gross_buy_notional: Decimal = Decimal("0")
    gross_sell_notional: Decimal = Decimal("0")
    fill_count: int = 0
    unverifiable_order_ids: tuple[str, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyHelperStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "fill_count": self.fill_count,
                "gross_buy_notional": self.gross_buy_notional,
                "gross_buy_size": self.gross_buy_size,
                "gross_sell_notional": self.gross_sell_notional,
                "gross_sell_size": self.gross_sell_size,
                "is_ok": self.is_ok,
                "net_size": self.net_size,
                "open_buy_notional": self.open_buy_notional,
                "open_buy_order_count": self.open_buy_order_count,
                "open_order_count": self.open_order_count,
                "open_order_notional": self.open_order_notional,
                "open_sell_notional": self.open_sell_notional,
                "open_sell_order_count": self.open_sell_order_count,
                "product_id": self.product_id,
                "status": self.status,
                "unverifiable_order_ids": self.unverifiable_order_ids,
            }
        )


@dataclass(frozen=True)
class OrderCapacity:
    product_id: str
    status: RiskCheckStatus
    side: OrderSide | None = None
    policy_present: bool = False
    product_allowed: bool = False
    side_allowed: bool = False
    kill_switch_enabled: bool = False
    max_order_notional: Decimal | None = None
    max_daily_notional: Decimal | None = None
    max_open_orders: int | None = None
    remaining_max_order_notional: Decimal | None = None
    daily_notional_used: Decimal = Decimal("0")
    remaining_daily_notional: Decimal | None = None
    open_order_count: int = 0
    product_open_order_count: int = 0
    remaining_open_order_slots: int | None = None
    unverifiable_daily_order_ids: tuple[str, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == RiskCheckStatus.PASS

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "daily_notional_used": self.daily_notional_used,
                "is_ok": self.is_ok,
                "kill_switch_enabled": self.kill_switch_enabled,
                "max_daily_notional": self.max_daily_notional,
                "max_open_orders": self.max_open_orders,
                "max_order_notional": self.max_order_notional,
                "open_order_count": self.open_order_count,
                "policy_present": self.policy_present,
                "product_allowed": self.product_allowed,
                "product_id": self.product_id,
                "product_open_order_count": self.product_open_order_count,
                "remaining_daily_notional": self.remaining_daily_notional,
                "remaining_max_order_notional": self.remaining_max_order_notional,
                "remaining_open_order_slots": self.remaining_open_order_slots,
                "side": self.side,
                "side_allowed": self.side_allowed,
                "status": self.status,
                "unverifiable_daily_order_ids": self.unverifiable_daily_order_ids,
            }
        )


def product_exposure(
    projection: SourceOfTruthProjection,
    product_id: str,
    *,
    product_catalog: ProductCatalog | None = None,
) -> ProductExposure:
    _validate_projection(projection)
    _validate_product_id(product_id)
    _validate_product_catalog(product_catalog)
    product = product_catalog.get(product_id) if product_catalog is not None else None
    open_orders = live_open_orders(projection, product_id=product_id)
    unverifiable_order_ids: list[str] = []
    open_order_notional = Decimal("0")
    open_buy_notional = Decimal("0")
    open_sell_notional = Decimal("0")
    open_buy_count = 0
    open_sell_count = 0
    for order in open_orders:
        notional = order_notional_value(order.size, order.limit_price, product)
        if notional is None:
            unverifiable_order_ids.append(order.action_id)
        else:
            open_order_notional += notional
            if order.side == OrderSide.BUY:
                open_buy_notional += notional
            elif order.side == OrderSide.SELL:
                open_sell_notional += notional
        if order.side == OrderSide.BUY:
            open_buy_count += 1
        elif order.side == OrderSide.SELL:
            open_sell_count += 1

    position = projection.positions_by_product_id.get(product_id)
    status = (
        StrategyHelperStatus.INSUFFICIENT_DATA
        if unverifiable_order_ids
        else StrategyHelperStatus.OK
    )
    return ProductExposure(
        fill_count=position.fill_count if position is not None else 0,
        gross_buy_notional=_decimal_or_zero(getattr(position, "gross_buy_notional", None)),
        gross_buy_size=_decimal_or_zero(getattr(position, "gross_buy_size", None)),
        gross_sell_notional=_decimal_or_zero(getattr(position, "gross_sell_notional", None)),
        gross_sell_size=_decimal_or_zero(getattr(position, "gross_sell_size", None)),
        net_size=_decimal_or_zero(getattr(position, "net_size", None)),
        open_buy_notional=open_buy_notional,
        open_buy_order_count=open_buy_count,
        open_order_count=len(open_orders),
        open_order_notional=open_order_notional,
        open_sell_notional=open_sell_notional,
        open_sell_order_count=open_sell_count,
        product_id=product_id,
        status=status,
        unverifiable_order_ids=tuple(sorted(unverifiable_order_ids)),
    )


def order_capacity(
    projection: SourceOfTruthProjection,
    product_id: str,
    *,
    now: datetime,
    operator_policy: "OperatorPolicy | None",
    product_catalog: ProductCatalog | None = None,
    side: OrderSide | None = None,
) -> OrderCapacity:
    _validate_projection(projection)
    _validate_product_id(product_id)
    _validate_product_catalog(product_catalog)
    if side is not None and not isinstance(side, OrderSide):
        raise TypeError("side must be an OrderSide when provided")
    if not isinstance(now, datetime):
        raise TypeError("now must be a datetime")
    if operator_policy is None:
        return OrderCapacity(product_id=product_id, side=side, status=RiskCheckStatus.FAIL)
    if not hasattr(operator_policy, "risk_limits") or not hasattr(operator_policy, "scope"):
        raise TypeError("operator_policy must be an OperatorPolicy when provided")

    open_orders = live_open_orders(projection)
    product_open_orders = live_open_orders(projection, product_id=product_id)
    usage = daily_notional_usage(
        projection,
        now=now,
        product_catalog=product_catalog,
    )
    max_daily = operator_policy.risk_limits.max_daily_notional_usd
    max_open_orders = operator_policy.risk_limits.max_open_orders
    remaining_daily = max(max_daily - usage.notional, Decimal("0"))
    remaining_slots = max(max_open_orders - len(open_orders), 0)
    product_allowed = product_id in operator_policy.scope.products
    side_allowed = side is None or side in operator_policy.risk_limits.allowed_sides
    status = (
        RiskCheckStatus.PASS
        if (
            product_allowed
            and side_allowed
            and not operator_policy.risk_limits.kill_switch_enabled
            and not usage.unverifiable_action_ids
            and remaining_daily > 0
            and remaining_slots > 0
        )
        else RiskCheckStatus.FAIL
    )
    return OrderCapacity(
        daily_notional_used=usage.notional,
        kill_switch_enabled=operator_policy.risk_limits.kill_switch_enabled,
        max_daily_notional=max_daily,
        max_open_orders=max_open_orders,
        max_order_notional=operator_policy.risk_limits.max_order_notional_usd,
        open_order_count=len(open_orders),
        policy_present=True,
        product_allowed=product_allowed,
        product_id=product_id,
        product_open_order_count=len(product_open_orders),
        remaining_daily_notional=remaining_daily,
        remaining_max_order_notional=operator_policy.risk_limits.max_order_notional_usd,
        remaining_open_order_slots=remaining_slots,
        side=side,
        side_allowed=side_allowed,
        status=status,
        unverifiable_daily_order_ids=usage.unverifiable_action_ids,
    )


def _validate_projection(projection: SourceOfTruthProjection) -> None:
    if not isinstance(projection, SourceOfTruthProjection):
        raise TypeError("projection must be a SourceOfTruthProjection")


def _validate_product_id(product_id: str) -> None:
    if not isinstance(product_id, str) or not product_id:
        raise ValueError("product_id must be a non-empty string")


def _validate_product_catalog(product_catalog: ProductCatalog | None) -> None:
    if product_catalog is not None and not isinstance(product_catalog, ProductCatalog):
        raise TypeError("product_catalog must be a ProductCatalog when provided")


def _decimal_or_zero(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        return Decimal("0")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")
    if not parsed.is_finite():
        return Decimal("0")
    return parsed


def _payload(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(
        {
            key: _json_safe(value)
            for key, value in raw.items()
        }
    )
    if not isinstance(normalized, dict):
        raise TypeError("exposure helper payload must normalize to an object")
    return normalized


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value
