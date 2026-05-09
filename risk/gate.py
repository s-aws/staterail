from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from core.enums import (
    ActionType,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    RiskCheckStatus,
    RiskRule,
    TimeInForce,
)
from core.json_tools import JsonValue
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection


class RiskCommand(Protocol):
    action_type: ActionType

    def to_payload(self) -> dict[str, JsonValue]:
        ...


@dataclass(frozen=True)
class RiskPolicy:
    allowed_products: frozenset[str] | None = None
    allowed_order_types: frozenset[OrderType] | None = None
    allowed_sides: frozenset[OrderSide] | None = None
    allowed_time_in_force: frozenset[TimeInForce] | None = None
    allowed_lineage_relations: frozenset[OrderLineageRelation] | None = None
    allowed_placement_kinds: frozenset[OrderPlacementKind] | None = None
    product_catalog: ProductCatalog | None = None
    max_order_size: Decimal | None = None
    max_order_notional: Decimal | None = None
    max_daily_notional: Decimal | None = None
    max_open_orders: int | None = None
    max_leverage: Decimal | None = None
    max_visible_notional: Decimal | None = None
    max_order_replacements: int | None = None
    require_reduce_only: bool = False
    require_post_only: bool = False
    require_staged_release_above_visible_limit: bool = False
    kill_switch_enabled: bool = False

    @classmethod
    def from_values(
        cls,
        *,
        allowed_products: Iterable[str] | None = None,
        allowed_order_types: Iterable[OrderType] | None = None,
        allowed_sides: Iterable[OrderSide] | None = None,
        allowed_time_in_force: Iterable[TimeInForce] | None = None,
        allowed_lineage_relations: Iterable[OrderLineageRelation] | None = None,
        allowed_placement_kinds: Iterable[OrderPlacementKind] | None = None,
        product_catalog: ProductCatalog | None = None,
        max_order_size: str | Decimal | None = None,
        max_order_notional: str | Decimal | None = None,
        max_daily_notional: str | Decimal | None = None,
        max_open_orders: int | None = None,
        max_leverage: str | Decimal | None = None,
        max_visible_notional: str | Decimal | None = None,
        max_order_replacements: int | None = None,
        require_reduce_only: bool = False,
        require_post_only: bool = False,
        require_staged_release_above_visible_limit: bool = False,
        kill_switch_enabled: bool = False,
    ) -> "RiskPolicy":
        return cls(
            allowed_products=frozenset(allowed_products) if allowed_products is not None else None,
            allowed_order_types=frozenset(allowed_order_types) if allowed_order_types is not None else None,
            allowed_sides=frozenset(allowed_sides) if allowed_sides is not None else None,
            allowed_time_in_force=(
                frozenset(allowed_time_in_force) if allowed_time_in_force is not None else None
            ),
            allowed_lineage_relations=(
                frozenset(allowed_lineage_relations)
                if allowed_lineage_relations is not None
                else None
            ),
            allowed_placement_kinds=(
                frozenset(allowed_placement_kinds)
                if allowed_placement_kinds is not None
                else None
            ),
            product_catalog=product_catalog,
            max_order_size=_decimal_or_none(max_order_size),
            max_order_notional=_decimal_or_none(max_order_notional),
            max_daily_notional=_decimal_or_none(max_daily_notional),
            max_open_orders=max_open_orders,
            max_leverage=_decimal_or_none(max_leverage),
            max_visible_notional=_decimal_or_none(max_visible_notional),
            max_order_replacements=max_order_replacements,
            require_reduce_only=require_reduce_only,
            require_post_only=require_post_only,
            require_staged_release_above_visible_limit=require_staged_release_above_visible_limit,
            kill_switch_enabled=kill_switch_enabled,
        )


@dataclass(frozen=True)
class RiskCheckResult:
    rule: RiskRule
    status: RiskCheckStatus
    message: str
    observed: str | None = None
    limit: str | None = None

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "limit": self.limit,
            "message": self.message,
            "observed": self.observed,
            "rule": self.rule.value,
            "status": self.status.value,
        }


@dataclass(frozen=True)
class RiskEvaluation:
    status: RiskCheckStatus
    checks: tuple[RiskCheckResult, ...]

    @property
    def passed(self) -> bool:
        return self.status == RiskCheckStatus.PASS

    def failure_messages(self) -> list[str]:
        return [check.message for check in self.checks if check.status == RiskCheckStatus.FAIL]

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "checks": [check.to_payload() for check in self.checks],
            "status": self.status.value,
        }


@dataclass(frozen=True)
class DailyNotionalUsage:
    notional: Decimal
    unverifiable_action_ids: tuple[str, ...] = ()


class RiskGate:
    def __init__(self, policy: RiskPolicy) -> None:
        self._policy = policy

    def evaluate(
        self,
        command: RiskCommand,
        projection: SourceOfTruthProjection,
        *,
        now: datetime | None = None,
    ) -> RiskEvaluation:
        checks: list[RiskCheckResult] = []

        if command.action_type != ActionType.PLACE_ORDER:
            return RiskEvaluation(
                status=RiskCheckStatus.PASS,
                checks=(
                    RiskCheckResult(
                        rule=RiskRule.KILL_SWITCH,
                        status=RiskCheckStatus.PASS,
                        message="risk gate does not block non-order-placement actions",
                    ),
                ),
            )

        payload = command.to_payload()["payload"]
        if not isinstance(payload, dict):
            return RiskEvaluation(
                status=RiskCheckStatus.FAIL,
                checks=(
                    RiskCheckResult(
                        rule=RiskRule.ALLOWED_PRODUCT,
                        status=RiskCheckStatus.FAIL,
                        message="action payload must be a JSON object",
                    ),
                ),
            )

        checks.append(self._check_kill_switch())
        checks.append(self._check_allowed_product(payload))
        checks.append(self._check_allowed_order_type(payload))
        checks.extend(_optional_check(self._check_allowed_side(payload)))
        checks.extend(_optional_check(self._check_allowed_time_in_force(payload)))
        checks.extend(_optional_check(self._check_lineage_relation(payload)))
        checks.extend(_optional_check(self._check_placement_kind(payload)))
        checks.extend(self._check_product_metadata(payload))
        checks.append(self._check_size(payload))
        checks.append(self._check_notional(payload))
        checks.extend(_optional_check(self._check_daily_notional(payload, projection, now=now)))
        checks.extend(_optional_check(self._check_open_orders(payload, projection)))
        checks.append(self._check_leverage(payload))
        checks.extend(_optional_check(self._check_visible_notional(payload)))
        checks.extend(_optional_check(self._check_order_replacements(payload, projection)))
        checks.extend(_optional_check(self._check_post_only(payload)))
        checks.append(self._check_reduce_only(payload))

        status = (
            RiskCheckStatus.PASS
            if all(check.status == RiskCheckStatus.PASS for check in checks)
            else RiskCheckStatus.FAIL
        )
        return RiskEvaluation(status=status, checks=tuple(checks))

    def _check_kill_switch(self) -> RiskCheckResult:
        if self._policy.kill_switch_enabled:
            return RiskCheckResult(
                rule=RiskRule.KILL_SWITCH,
                status=RiskCheckStatus.FAIL,
                message="kill switch is enabled",
            )
        return RiskCheckResult(
            rule=RiskRule.KILL_SWITCH,
            status=RiskCheckStatus.PASS,
            message="kill switch is disabled",
        )

    def _check_allowed_product(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult:
        product_id = _string(payload.get("product_id"))
        if self._policy.allowed_products is None:
            return _pass(RiskRule.ALLOWED_PRODUCT, "product allowlist is not configured", observed=product_id)
        if product_id in self._policy.allowed_products:
            return _pass(RiskRule.ALLOWED_PRODUCT, "product is allowed", observed=product_id)
        return _fail(
            RiskRule.ALLOWED_PRODUCT,
            "product is not allowed",
            observed=product_id,
            limit=",".join(sorted(self._policy.allowed_products)),
        )

    def _check_allowed_order_type(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult:
        order_type = _order_type_or_none(payload.get("order_type"))
        if self._policy.allowed_order_types is None:
            return _pass(
                RiskRule.ALLOWED_ORDER_TYPE,
                "order type allowlist is not configured",
                observed=order_type.value if order_type else None,
            )
        if order_type in self._policy.allowed_order_types:
            return _pass(RiskRule.ALLOWED_ORDER_TYPE, "order type is allowed", observed=order_type.value)
        return _fail(
            RiskRule.ALLOWED_ORDER_TYPE,
            "order type is not allowed",
            observed=order_type.value if order_type else _string(payload.get("order_type")),
            limit=",".join(sorted(order_type.value for order_type in self._policy.allowed_order_types)),
        )

    def _check_allowed_side(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult | None:
        if self._policy.allowed_sides is None:
            return None
        side = _side_or_none(payload.get("side"))
        if side in self._policy.allowed_sides:
            return _pass(RiskRule.ALLOWED_SIDE, "side is allowed", observed=side.value)
        return _fail(
            RiskRule.ALLOWED_SIDE,
            "side is not allowed",
            observed=side.value if side else _string(payload.get("side")),
            limit=",".join(sorted(side.value for side in self._policy.allowed_sides)),
        )

    def _check_allowed_time_in_force(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult | None:
        if self._policy.allowed_time_in_force is None:
            return None
        time_in_force = _time_in_force_or_none(payload.get("time_in_force"))
        if time_in_force in self._policy.allowed_time_in_force:
            return _pass(
                RiskRule.ALLOWED_TIME_IN_FORCE,
                "time_in_force is allowed",
                observed=time_in_force.value,
            )
        return _fail(
            RiskRule.ALLOWED_TIME_IN_FORCE,
            "time_in_force is not allowed",
            observed=time_in_force.value if time_in_force else _string(payload.get("time_in_force")),
            limit=",".join(
                sorted(time_in_force.value for time_in_force in self._policy.allowed_time_in_force)
            ),
        )

    def _check_lineage_relation(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult | None:
        if self._policy.allowed_lineage_relations is None:
            return None
        relation = _lineage_relation_or_none(payload.get("lineage_relation")) or OrderLineageRelation.ROOT
        if relation in self._policy.allowed_lineage_relations:
            return _pass(
                RiskRule.LINEAGE_RELATION_ALLOWED,
                "lineage relation is allowed",
                observed=relation.value,
            )
        return _fail(
            RiskRule.LINEAGE_RELATION_ALLOWED,
            "lineage relation is not allowed",
            observed=relation.value,
            limit=",".join(
                sorted(relation.value for relation in self._policy.allowed_lineage_relations)
            ),
        )

    def _check_placement_kind(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult | None:
        if self._policy.allowed_placement_kinds is None:
            return None
        kind = _placement_kind_or_none(payload.get("placement_kind")) or OrderPlacementKind.INITIAL
        if kind in self._policy.allowed_placement_kinds:
            return _pass(
                RiskRule.PLACEMENT_KIND_ALLOWED,
                "placement kind is allowed",
                observed=kind.value,
            )
        return _fail(
            RiskRule.PLACEMENT_KIND_ALLOWED,
            "placement kind is not allowed",
            observed=kind.value,
            limit=",".join(sorted(kind.value for kind in self._policy.allowed_placement_kinds)),
        )

    def _check_product_metadata(self, payload: Mapping[str, JsonValue]) -> tuple[RiskCheckResult, ...]:
        if self._policy.product_catalog is None:
            return ()

        product_id = _string(payload.get("product_id"))
        product = self._policy.product_catalog.get(product_id)
        order_type = _order_type_or_none(payload.get("order_type"))
        size = _decimal_or_none(payload.get("size"))
        limit_price = _decimal_or_none(payload.get("limit_price"))

        if product is None:
            return (
                _fail(
                    RiskRule.PRODUCT_TRADABLE,
                    "product metadata is missing",
                    observed=product_id,
                ),
            )

        checks = [
            self._check_product_tradable(product, order_type),
            self._check_product_base_size(product, size),
        ]
        if order_type == OrderType.LIMIT:
            checks.append(self._check_product_price_increment(product, limit_price))
        checks.append(self._check_product_quote_notional(product, size, limit_price))
        return tuple(checks)

    def _check_product_tradable(
        self,
        product: ProductMetadata,
        order_type: OrderType | None,
    ) -> RiskCheckResult:
        if not product.tradable_for_new_orders:
            return _fail(
                RiskRule.PRODUCT_TRADABLE,
                "product is not tradable for new orders",
                observed=product.product_id,
            )
        if not product.allows_order_type(order_type):
            return _fail(
                RiskRule.PRODUCT_TRADABLE,
                "product does not allow this order type",
                observed=order_type.value if order_type else None,
            )
        return _pass(
            RiskRule.PRODUCT_TRADABLE,
            "product is tradable",
            observed=product.product_id,
        )

    def _check_product_base_size(
        self,
        product: ProductMetadata,
        size: Decimal | None,
    ) -> RiskCheckResult:
        if product.size_is_valid(size):
            return _pass(
                RiskRule.PRODUCT_BASE_SIZE,
                "size satisfies product base size rules",
                observed=str(size) if size is not None else None,
                limit=_product_size_limit(product),
            )
        return _fail(
            RiskRule.PRODUCT_BASE_SIZE,
            "size violates product base size rules",
            observed=str(size) if size is not None else None,
            limit=_product_size_limit(product),
        )

    def _check_product_price_increment(
        self,
        product: ProductMetadata,
        price: Decimal | None,
    ) -> RiskCheckResult:
        if product.price_is_valid(price):
            return _pass(
                RiskRule.PRODUCT_PRICE_INCREMENT,
                "price satisfies product price increment",
                observed=str(price) if price is not None else None,
                limit=str(product.price_increment) if product.price_increment is not None else None,
            )
        return _fail(
            RiskRule.PRODUCT_PRICE_INCREMENT,
            "price violates product price increment",
            observed=str(price) if price is not None else None,
            limit=str(product.price_increment) if product.price_increment is not None else None,
        )

    def _check_product_quote_notional(
        self,
        product: ProductMetadata,
        size: Decimal | None,
        price: Decimal | None,
    ) -> RiskCheckResult:
        if product.notional_is_valid(size, price):
            return _pass(
                RiskRule.PRODUCT_QUOTE_NOTIONAL,
                "notional satisfies product quote size rules",
                observed=_notional(size, price, product),
                limit=_product_quote_limit(product),
            )
        return _fail(
            RiskRule.PRODUCT_QUOTE_NOTIONAL,
            "notional violates product quote size rules",
            observed=_notional(size, price, product),
            limit=_product_quote_limit(product),
        )

    def _check_size(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult:
        if self._policy.max_order_size is None:
            return _pass(RiskRule.MAX_ORDER_SIZE, "max order size is not configured")
        size = _decimal_or_none(payload.get("size"))
        if size is None:
            return _fail(RiskRule.MAX_ORDER_SIZE, "size must be decimal", limit=str(self._policy.max_order_size))
        if size <= self._policy.max_order_size:
            return _pass(
                RiskRule.MAX_ORDER_SIZE,
                "size is within limit",
                observed=str(size),
                limit=str(self._policy.max_order_size),
            )
        return _fail(
            RiskRule.MAX_ORDER_SIZE,
            "size exceeds limit",
            observed=str(size),
            limit=str(self._policy.max_order_size),
        )

    def _check_notional(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult:
        if self._policy.max_order_notional is None:
            return _pass(RiskRule.MAX_ORDER_NOTIONAL, "max order notional is not configured")
        size = _decimal_or_none(payload.get("size"))
        limit_price = _decimal_or_none(payload.get("limit_price"))
        if size is None:
            return _fail(
                RiskRule.MAX_ORDER_NOTIONAL,
                "size must be decimal before notional can be checked",
                limit=str(self._policy.max_order_notional),
            )
        if limit_price is None:
            return _fail(
                RiskRule.MAX_ORDER_NOTIONAL,
                "limit_price is required when max notional is configured",
                limit=str(self._policy.max_order_notional),
            )
        notional = _payload_notional(payload, self._product_for_payload(payload))
        if notional is None:
            return _fail(
                RiskRule.MAX_ORDER_NOTIONAL,
                "size and limit_price are required when max notional is configured",
                limit=str(self._policy.max_order_notional),
            )
        if notional <= self._policy.max_order_notional:
            return _pass(
                RiskRule.MAX_ORDER_NOTIONAL,
                "notional is within limit",
                observed=str(notional),
                limit=str(self._policy.max_order_notional),
            )
        return _fail(
            RiskRule.MAX_ORDER_NOTIONAL,
            "notional exceeds limit",
            observed=str(notional),
            limit=str(self._policy.max_order_notional),
        )

    def _check_daily_notional(
        self,
        payload: Mapping[str, JsonValue],
        projection: SourceOfTruthProjection,
        *,
        now: datetime | None,
    ) -> RiskCheckResult | None:
        if self._policy.max_daily_notional is None:
            return None
        if _is_staged_release_payload(payload):
            return _pass(
                RiskRule.MAX_DAILY_NOTIONAL,
                "staged order does not consume daily live-order notional",
                limit=str(self._policy.max_daily_notional),
            )

        current_product = self._product_for_payload(payload)
        current_notional = _payload_notional(payload, current_product)
        if current_notional is None:
            return _fail(
                RiskRule.MAX_DAILY_NOTIONAL,
                "size and limit_price are required when max daily notional is configured",
                limit=str(self._policy.max_daily_notional),
            )

        usage = daily_notional_usage(
            projection,
            now=now,
            product_catalog=self._policy.product_catalog,
        )
        if usage.unverifiable_action_ids:
            return _fail(
                RiskRule.MAX_DAILY_NOTIONAL,
                "accepted order notional cannot be verified",
                observed=",".join(usage.unverifiable_action_ids),
                limit=str(self._policy.max_daily_notional),
            )

        proposed = usage.notional + current_notional
        if proposed <= self._policy.max_daily_notional:
            return _pass(
                RiskRule.MAX_DAILY_NOTIONAL,
                "daily notional is within limit",
                observed=str(proposed),
                limit=str(self._policy.max_daily_notional),
            )
        return _fail(
            RiskRule.MAX_DAILY_NOTIONAL,
            "daily notional exceeds limit",
            observed=str(proposed),
            limit=str(self._policy.max_daily_notional),
        )

    def _check_open_orders(
        self,
        payload: Mapping[str, JsonValue],
        projection: SourceOfTruthProjection,
    ) -> RiskCheckResult | None:
        if self._policy.max_open_orders is None:
            return None
        open_count = len(live_open_orders(projection))
        proposed_increment = 0 if _is_existing_logical_placement(payload) or _is_staged_release_payload(payload) else 1
        proposed_count = open_count + proposed_increment
        if proposed_count <= self._policy.max_open_orders:
            return _pass(
                RiskRule.MAX_OPEN_ORDERS,
                "open order count is within limit",
                observed=str(proposed_count),
                limit=str(self._policy.max_open_orders),
            )
        return _fail(
            RiskRule.MAX_OPEN_ORDERS,
            "open order count exceeds limit",
            observed=str(proposed_count),
            limit=str(self._policy.max_open_orders),
        )

    def _check_leverage(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult:
        if self._policy.max_leverage is None:
            return _pass(RiskRule.MAX_LEVERAGE, "max leverage is not configured")
        leverage = _decimal_or_none(payload.get("leverage"))
        if leverage is None:
            return _fail(RiskRule.MAX_LEVERAGE, "leverage is required", limit=str(self._policy.max_leverage))
        if leverage <= self._policy.max_leverage:
            return _pass(
                RiskRule.MAX_LEVERAGE,
                "leverage is within limit",
                observed=str(leverage),
                limit=str(self._policy.max_leverage),
            )
        return _fail(
            RiskRule.MAX_LEVERAGE,
            "leverage exceeds limit",
            observed=str(leverage),
            limit=str(self._policy.max_leverage),
        )

    def _check_visible_notional(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult | None:
        if (
            self._policy.max_visible_notional is None
            or not self._policy.require_staged_release_above_visible_limit
        ):
            return None
        notional = _payload_notional(payload, self._product_for_payload(payload))
        if notional is None:
            return _fail(
                RiskRule.MAX_VISIBLE_NOTIONAL,
                "size and limit_price are required when max visible notional is configured",
                limit=str(self._policy.max_visible_notional),
            )
        if notional <= self._policy.max_visible_notional:
            return _pass(
                RiskRule.MAX_VISIBLE_NOTIONAL,
                "visible notional is within limit",
                observed=str(notional),
                limit=str(self._policy.max_visible_notional),
            )
        if _is_staged_release_payload(payload):
            return _pass(
                RiskRule.MAX_VISIBLE_NOTIONAL,
                "oversized order is staged instead of visible",
                observed=str(notional),
                limit=str(self._policy.max_visible_notional),
            )
        return _fail(
            RiskRule.MAX_VISIBLE_NOTIONAL,
            "visible notional exceeds limit and must be staged",
            observed=str(notional),
            limit=str(self._policy.max_visible_notional),
        )

    def _check_order_replacements(
        self,
        payload: Mapping[str, JsonValue],
        projection: SourceOfTruthProjection,
    ) -> RiskCheckResult | None:
        if self._policy.max_order_replacements is None:
            return None
        kind = _placement_kind_or_none(payload.get("placement_kind")) or OrderPlacementKind.INITIAL
        if kind not in {OrderPlacementKind.AMEND, OrderPlacementKind.CANCEL_REPLACE}:
            return _pass(
                RiskRule.MAX_ORDER_REPLACEMENTS,
                "command is not an order replacement",
                limit=str(self._policy.max_order_replacements),
            )

        logical_order_id = _string(payload.get("logical_order_id"))
        existing_count = 0
        if logical_order_id is not None:
            existing_count = sum(
                1
                for placement in projection.placements_by_logical_order_id.get(logical_order_id, ())
                if placement.placement_kind in {OrderPlacementKind.AMEND, OrderPlacementKind.CANCEL_REPLACE}
            )
        proposed_count = existing_count + 1
        if proposed_count <= self._policy.max_order_replacements:
            return _pass(
                RiskRule.MAX_ORDER_REPLACEMENTS,
                "order replacement count is within limit",
                observed=str(proposed_count),
                limit=str(self._policy.max_order_replacements),
            )
        return _fail(
            RiskRule.MAX_ORDER_REPLACEMENTS,
            "order replacement count exceeds limit",
            observed=str(proposed_count),
            limit=str(self._policy.max_order_replacements),
        )

    def _check_post_only(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult | None:
        if not self._policy.require_post_only:
            return None
        if payload.get("post_only") is True:
            return _pass(RiskRule.POST_ONLY_REQUIRED, "order is post-only")
        return _fail(RiskRule.POST_ONLY_REQUIRED, "post-only mode is required")

    def _check_reduce_only(self, payload: Mapping[str, JsonValue]) -> RiskCheckResult:
        if not self._policy.require_reduce_only:
            return _pass(RiskRule.REDUCE_ONLY_REQUIRED, "reduce-only mode is not required")
        if payload.get("reduce_only") is True:
            return _pass(RiskRule.REDUCE_ONLY_REQUIRED, "order is reduce-only")
        return _fail(RiskRule.REDUCE_ONLY_REQUIRED, "reduce-only mode is required")

    def _product_for_payload(self, payload: Mapping[str, JsonValue]) -> ProductMetadata | None:
        return self._product_for_id(_string(payload.get("product_id")))

    def _product_for_id(self, product_id: Any) -> ProductMetadata | None:
        if self._policy.product_catalog is None or not isinstance(product_id, str):
            return None
        return self._policy.product_catalog.get(product_id)


def live_open_orders(
    projection: SourceOfTruthProjection,
    *,
    product_id: str | None = None,
    side: OrderSide | None = None,
) -> tuple[Any, ...]:
    return tuple(
        order
        for order in projection.orders_by_action_id.values()
        if _is_live_open_order(order, projection)
        and (product_id is None or order.product_id == product_id)
        and (side is None or order.side == side)
    )


def daily_notional_usage(
    projection: SourceOfTruthProjection,
    *,
    now: datetime | None = None,
    product_catalog: ProductCatalog | None = None,
    product_id: str | None = None,
) -> DailyNotionalUsage:
    evaluation_time = now or _latest_projection_time(projection) or datetime.now(timezone.utc)
    daily_notional = Decimal("0")
    unverifiable_action_ids: list[str] = []
    for order in projection.orders_by_action_id.values():
        if (
            order.accepted_sequence is None
            or _action_has_staged_placement(order.action_id, projection)
            or not _order_counts_toward_daily_notional(order)
            or (product_id is not None and order.product_id != product_id)
        ):
            continue
        accepted_at = projection.record_occurred_at_by_sequence.get(order.accepted_sequence)
        if accepted_at is None or _utc_date(accepted_at) != _utc_date(evaluation_time):
            continue
        product = product_catalog.get(order.product_id) if product_catalog is not None else None
        order_notional = order_notional_value(order.size, order.limit_price, product)
        if order_notional is None:
            unverifiable_action_ids.append(order.action_id)
            continue
        daily_notional += order_notional
    return DailyNotionalUsage(
        notional=daily_notional,
        unverifiable_action_ids=tuple(sorted(unverifiable_action_ids)),
    )


def order_notional_value(
    size: Any,
    price: Any,
    product: ProductMetadata | None = None,
) -> Decimal | None:
    return _order_notional(size, price, product)


def _pass(
    rule: RiskRule,
    message: str,
    *,
    observed: str | None = None,
    limit: str | None = None,
) -> RiskCheckResult:
    return RiskCheckResult(
        rule=rule,
        status=RiskCheckStatus.PASS,
        message=message,
        observed=observed,
        limit=limit,
    )


def _fail(
    rule: RiskRule,
    message: str,
    *,
    observed: str | None = None,
    limit: str | None = None,
) -> RiskCheckResult:
    return RiskCheckResult(
        rule=rule,
        status=RiskCheckStatus.FAIL,
        message=message,
        observed=observed,
        limit=limit,
    )


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


def _order_type_or_none(value: Any) -> OrderType | None:
    try:
        return OrderType(value)
    except (TypeError, ValueError):
        return None


def _side_or_none(value: Any) -> OrderSide | None:
    try:
        return OrderSide(value)
    except (TypeError, ValueError):
        return None


def _time_in_force_or_none(value: Any) -> TimeInForce | None:
    try:
        return TimeInForce(value)
    except (TypeError, ValueError):
        return None


def _lineage_relation_or_none(value: Any) -> OrderLineageRelation | None:
    try:
        return OrderLineageRelation(value)
    except (TypeError, ValueError):
        return None


def _placement_kind_or_none(value: Any) -> OrderPlacementKind | None:
    try:
        return OrderPlacementKind(value)
    except (TypeError, ValueError):
        return None


def _string(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _notional(size: Decimal | None, price: Decimal | None, product: ProductMetadata | None = None) -> str | None:
    resolved = _order_notional(size, price, product)
    if resolved is None:
        return None
    return str(resolved)


def _payload_notional(payload: Mapping[str, JsonValue], product: ProductMetadata | None = None) -> Decimal | None:
    return _order_notional(payload.get("size"), payload.get("limit_price"), product)


def _order_notional(size: Any, price: Any, product: ProductMetadata | None = None) -> Decimal | None:
    resolved_size = _decimal_or_none(size)
    resolved_price = _decimal_or_none(price)
    if resolved_size is None or resolved_price is None:
        return None
    if product is not None:
        return product.notional(resolved_size, resolved_price)
    return resolved_size * resolved_price


def _is_staged_release_payload(payload: Mapping[str, JsonValue]) -> bool:
    return _placement_kind_or_none(payload.get("placement_kind")) == OrderPlacementKind.STAGED_RELEASE


def _is_existing_logical_placement(payload: Mapping[str, JsonValue]) -> bool:
    return _placement_kind_or_none(payload.get("placement_kind")) in {
        OrderPlacementKind.AMEND,
        OrderPlacementKind.CANCEL_REPLACE,
    }


def _order_counts_toward_daily_notional(order: Any) -> bool:
    return getattr(order, "lifecycle_status", None) in {
        OrderLifecycleStatus.ACCEPTED,
        OrderLifecycleStatus.CANCEL_QUEUED,
        OrderLifecycleStatus.EXECUTION_UNKNOWN,
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.OPEN,
        OrderLifecycleStatus.PENDING,
    }


def _is_live_open_order(order: Any, projection: SourceOfTruthProjection) -> bool:
    if _action_has_staged_placement(getattr(order, "action_id", None), projection):
        return False
    return getattr(order, "lifecycle_status", None) in {
        OrderLifecycleStatus.ACCEPTED,
        OrderLifecycleStatus.CANCEL_QUEUED,
        OrderLifecycleStatus.EXECUTION_UNKNOWN,
        OrderLifecycleStatus.OPEN,
        OrderLifecycleStatus.PENDING,
        OrderLifecycleStatus.REQUESTED,
    }


def _action_has_staged_placement(action_id: Any, projection: SourceOfTruthProjection) -> bool:
    if not isinstance(action_id, str):
        return False
    for placement_id in projection.placement_ids_by_action_id.get(action_id, ()):
        placement = projection.placements_by_id.get(placement_id)
        if placement is not None and placement.placement_kind == OrderPlacementKind.STAGED_RELEASE:
            return True
    return False


def _utc_date(value: datetime) -> object:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).date()


def _latest_projection_time(projection: SourceOfTruthProjection) -> datetime | None:
    if projection.last_sequence <= 0:
        return None
    return projection.record_occurred_at_by_sequence.get(projection.last_sequence)


def _optional_check(check: RiskCheckResult | None) -> tuple[RiskCheckResult, ...]:
    return () if check is None else (check,)


def _product_size_limit(product: ProductMetadata) -> str | None:
    parts: list[str] = []
    if product.base_min_size is not None:
        parts.append(f"min={product.base_min_size}")
    if product.base_max_size is not None:
        parts.append(f"max={product.base_max_size}")
    if product.base_increment is not None:
        parts.append(f"increment={product.base_increment}")
    return ",".join(parts) if parts else None


def _product_quote_limit(product: ProductMetadata) -> str | None:
    parts: list[str] = []
    if product.quote_min_size is not None:
        parts.append(f"min={product.quote_min_size}")
    if product.quote_max_size is not None:
        parts.append(f"max={product.quote_max_size}")
    return ",".join(parts) if parts else None
