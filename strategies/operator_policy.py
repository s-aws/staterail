from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from core.enums import (
    MarginType,
    MarketDataKind,
    OperatorPolicyDistanceType,
    OperatorPolicyFollowupSizeMode,
    OperatorPolicyLineageModel,
    OperatorPolicyPermission,
    OperatorPolicyReferencePriceSource,
    OperatorPolicySizingStrategy,
    OperatorPolicyUpdateMode,
    OperatorPolicyVenue,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    TimeInForce,
)
from core.json_tools import JsonValue, normalize_json
from strategies.harness import StrategyInputRequirement

if TYPE_CHECKING:
    from config.assembly import RiskPolicyConfig


OPERATOR_POLICY_SCHEMA_VERSION = 1
EnumValue = TypeVar("EnumValue")


@dataclass(frozen=True)
class OperatorPolicyScope:
    products: tuple[str, ...]
    venue: OperatorPolicyVenue
    live_orders_allowed: bool

    def __post_init__(self) -> None:
        _non_empty_unique_strings(self.products, "scope.products")
        if not isinstance(self.venue, OperatorPolicyVenue):
            raise TypeError("scope.venue must be an OperatorPolicyVenue")
        if not isinstance(self.live_orders_allowed, bool):
            raise TypeError("scope.live_orders_allowed must be a bool")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "live_orders_allowed": self.live_orders_allowed,
            "products": list(self.products),
            "venue": self.venue.value,
        }


@dataclass(frozen=True)
class OperatorPolicyRiskLimits:
    max_order_notional_usd: Decimal
    max_daily_notional_usd: Decimal
    max_open_orders: int
    allowed_sides: tuple[OrderSide, ...]
    reduce_only_first: bool
    kill_switch_enabled: bool

    def __post_init__(self) -> None:
        _positive_decimal_value(self.max_order_notional_usd, "risk_limits.max_order_notional_usd")
        _positive_decimal_value(self.max_daily_notional_usd, "risk_limits.max_daily_notional_usd")
        if not isinstance(self.max_open_orders, int) or isinstance(self.max_open_orders, bool):
            raise TypeError("risk_limits.max_open_orders must be an integer")
        if self.max_open_orders <= 0:
            raise ValueError("risk_limits.max_open_orders must be positive")
        if not self.allowed_sides:
            raise ValueError("risk_limits.allowed_sides must not be empty")
        if any(not isinstance(side, OrderSide) for side in self.allowed_sides):
            raise TypeError("risk_limits.allowed_sides must contain OrderSide values")
        if len(self.allowed_sides) != len(set(self.allowed_sides)):
            raise ValueError("risk_limits.allowed_sides must be unique")
        if not isinstance(self.reduce_only_first, bool):
            raise TypeError("risk_limits.reduce_only_first must be a bool")
        if not isinstance(self.kill_switch_enabled, bool):
            raise TypeError("risk_limits.kill_switch_enabled must be a bool")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "allowed_sides": [side.value for side in self.allowed_sides],
            "kill_switch_enabled": self.kill_switch_enabled,
            "max_daily_notional_usd": str(self.max_daily_notional_usd),
            "max_open_orders": self.max_open_orders,
            "max_order_notional_usd": str(self.max_order_notional_usd),
            "reduce_only_first": self.reduce_only_first,
        }


@dataclass(frozen=True)
class OperatorPolicyMarketDataRequirements:
    require_order_book: bool
    max_order_book_age: timedelta
    require_product_metadata: bool
    require_redundant_feeds: bool

    def __post_init__(self) -> None:
        if not isinstance(self.require_order_book, bool):
            raise TypeError("market_data_requirements.require_order_book must be a bool")
        if not isinstance(self.max_order_book_age, timedelta):
            raise TypeError("market_data_requirements.max_order_book_age must be a timedelta")
        if self.max_order_book_age <= timedelta(0):
            raise ValueError("market_data_requirements.max_order_book_age must be positive")
        if not isinstance(self.require_product_metadata, bool):
            raise TypeError("market_data_requirements.require_product_metadata must be a bool")
        if not isinstance(self.require_redundant_feeds, bool):
            raise TypeError("market_data_requirements.require_redundant_feeds must be a bool")

    def to_strategy_input_requirements(
        self,
        products: tuple[str, ...],
    ) -> tuple[StrategyInputRequirement, ...]:
        if not self.require_order_book:
            return ()
        return tuple(
            StrategyInputRequirement(
                data_kind=MarketDataKind.ORDER_BOOK,
                max_age=self.max_order_book_age,
                product_id=product_id,
            )
            for product_id in products
        )

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "max_order_book_age_seconds": self.max_order_book_age.total_seconds(),
            "require_order_book": self.require_order_book,
            "require_product_metadata": self.require_product_metadata,
            "require_redundant_feeds": self.require_redundant_feeds,
        }


@dataclass(frozen=True)
class OperatorPolicyOrderBehavior:
    default_order_type: OrderType
    allow_market_orders: bool
    post_only: bool
    time_in_force: TimeInForce
    default_leverage: Decimal | None = None
    default_margin_type: MarginType | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.default_order_type, OrderType):
            raise TypeError("order_behavior.default_order_type must be an OrderType")
        if not isinstance(self.allow_market_orders, bool):
            raise TypeError("order_behavior.allow_market_orders must be a bool")
        if self.default_order_type == OrderType.MARKET and not self.allow_market_orders:
            raise ValueError("market default_order_type requires allow_market_orders")
        if not isinstance(self.post_only, bool):
            raise TypeError("order_behavior.post_only must be a bool")
        if not isinstance(self.time_in_force, TimeInForce):
            raise TypeError("order_behavior.time_in_force must be a TimeInForce")
        if self.default_leverage is not None:
            _positive_decimal_value(self.default_leverage, "order_behavior.default_leverage")
        if self.default_margin_type is not None and not isinstance(self.default_margin_type, MarginType):
            raise TypeError("order_behavior.default_margin_type must be a MarginType")
        if self.post_only and self.default_order_type != OrderType.LIMIT:
            raise ValueError("post_only requires limit default_order_type")
        if self.post_only and self.time_in_force != TimeInForce.GOOD_UNTIL_CANCELLED:
            raise ValueError("post_only requires good_until_cancelled time_in_force")

    @property
    def allowed_order_types(self) -> tuple[OrderType, ...]:
        order_types = [self.default_order_type]
        if self.allow_market_orders:
            order_types.append(OrderType.MARKET)
        return tuple(dict.fromkeys(order_types))

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "allow_market_orders": self.allow_market_orders,
            "default_leverage": str(self.default_leverage) if self.default_leverage is not None else None,
            "default_margin_type": (
                self.default_margin_type.value if self.default_margin_type is not None else None
            ),
            "default_order_type": self.default_order_type.value,
            "post_only": self.post_only,
            "time_in_force": self.time_in_force.value,
        }


@dataclass(frozen=True)
class OperatorPolicyLineage:
    model: OperatorPolicyLineageModel
    allow_manual_adoption: bool
    followup_on_fill: OperatorPolicyPermission
    move_same_side_orders: OperatorPolicyPermission
    split_orders: OperatorPolicyPermission
    merge_orders: OperatorPolicyPermission

    def __post_init__(self) -> None:
        if not isinstance(self.model, OperatorPolicyLineageModel):
            raise TypeError("lineage.model must be an OperatorPolicyLineageModel")
        if not isinstance(self.allow_manual_adoption, bool):
            raise TypeError("lineage.allow_manual_adoption must be a bool")
        for field_name, value in (
            ("followup_on_fill", self.followup_on_fill),
            ("move_same_side_orders", self.move_same_side_orders),
            ("split_orders", self.split_orders),
            ("merge_orders", self.merge_orders),
        ):
            if not isinstance(value, OperatorPolicyPermission):
                raise TypeError(f"lineage.{field_name} must be an OperatorPolicyPermission")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "allow_manual_adoption": self.allow_manual_adoption,
            "followup_on_fill": self.followup_on_fill.value,
            "merge_orders": self.merge_orders.value,
            "model": self.model.value,
            "move_same_side_orders": self.move_same_side_orders.value,
            "split_orders": self.split_orders.value,
        }


@dataclass(frozen=True)
class OperatorPolicyPartialFills:
    followup_enabled: bool
    followup_size_mode: OperatorPolicyFollowupSizeMode
    followup_percent: Decimal
    min_followup_notional_usd: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.followup_enabled, bool):
            raise TypeError("partial_fills.followup_enabled must be a bool")
        if not isinstance(self.followup_size_mode, OperatorPolicyFollowupSizeMode):
            raise TypeError("partial_fills.followup_size_mode must be an OperatorPolicyFollowupSizeMode")
        _positive_decimal_value(self.followup_percent, "partial_fills.followup_percent")
        if self.followup_percent > Decimal("100"):
            raise ValueError("partial_fills.followup_percent must be less than or equal to 100")
        _positive_decimal_value(self.min_followup_notional_usd, "partial_fills.min_followup_notional_usd")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "followup_enabled": self.followup_enabled,
            "followup_percent": str(self.followup_percent),
            "followup_size_mode": self.followup_size_mode.value,
            "min_followup_notional_usd": str(self.min_followup_notional_usd),
        }


@dataclass(frozen=True)
class OperatorPolicyMoves:
    min_price_change_ticks: int
    cooldown: timedelta
    cancel_replace_when_amend_not_supported: bool

    def __post_init__(self) -> None:
        if not isinstance(self.min_price_change_ticks, int) or isinstance(self.min_price_change_ticks, bool):
            raise TypeError("moves.min_price_change_ticks must be an integer")
        if self.min_price_change_ticks <= 0:
            raise ValueError("moves.min_price_change_ticks must be positive")
        if not isinstance(self.cooldown, timedelta):
            raise TypeError("moves.cooldown must be a timedelta")
        if self.cooldown < timedelta(0):
            raise ValueError("moves.cooldown must not be negative")
        if not isinstance(self.cancel_replace_when_amend_not_supported, bool):
            raise TypeError("moves.cancel_replace_when_amend_not_supported must be a bool")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "cancel_replace_when_amend_not_supported": self.cancel_replace_when_amend_not_supported,
            "cooldown_seconds": self.cooldown.total_seconds(),
            "min_price_change_ticks": self.min_price_change_ticks,
        }


@dataclass(frozen=True)
class OperatorPolicyStagedRelease:
    enabled: bool
    release_only_when_conditions_match: bool
    max_visible_notional_usd: Decimal
    allow_release: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise TypeError("staged_or_hidden_release.enabled must be a bool")
        if not isinstance(self.release_only_when_conditions_match, bool):
            raise TypeError("staged_or_hidden_release.release_only_when_conditions_match must be a bool")
        if not isinstance(self.allow_release, bool):
            raise TypeError("staged_or_hidden_release.allow_release must be a bool")
        _positive_decimal_value(
            self.max_visible_notional_usd,
            "staged_or_hidden_release.max_visible_notional_usd",
        )

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "allow_release": self.allow_release,
            "enabled": self.enabled,
            "max_visible_notional_usd": str(self.max_visible_notional_usd),
            "release_only_when_conditions_match": self.release_only_when_conditions_match,
        }


@dataclass(frozen=True)
class OperatorPolicyAnchorRepricing:
    enabled: bool
    reference_price_source: OperatorPolicyReferencePriceSource
    distance_type: OperatorPolicyDistanceType
    target_distance: Decimal
    max_distance: Decimal
    update_mode: OperatorPolicyUpdateMode
    min_price_change: Decimal
    hysteresis_bps: Decimal
    min_reprice_interval: timedelta
    max_reprices_per_hour: int
    slide_mode: bool
    max_step_per_reprice: Decimal
    post_only_required: bool
    follow_up_retreat_distance: Decimal
    follow_up_retreat_jitter: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise TypeError("anchor_repricing.enabled must be a bool")
        if not isinstance(self.reference_price_source, OperatorPolicyReferencePriceSource):
            raise TypeError("anchor_repricing.reference_price_source must be an OperatorPolicyReferencePriceSource")
        if not isinstance(self.distance_type, OperatorPolicyDistanceType):
            raise TypeError("anchor_repricing.distance_type must be an OperatorPolicyDistanceType")
        _positive_decimal_value(self.target_distance, "anchor_repricing.target_distance")
        _positive_decimal_value(self.max_distance, "anchor_repricing.max_distance")
        if self.max_distance < self.target_distance:
            raise ValueError("anchor_repricing.max_distance must be greater than or equal to target_distance")
        if not isinstance(self.update_mode, OperatorPolicyUpdateMode):
            raise TypeError("anchor_repricing.update_mode must be an OperatorPolicyUpdateMode")
        _positive_decimal_value(self.min_price_change, "anchor_repricing.min_price_change")
        _non_negative_decimal_value(self.hysteresis_bps, "anchor_repricing.hysteresis_bps")
        if not isinstance(self.min_reprice_interval, timedelta):
            raise TypeError("anchor_repricing.min_reprice_interval must be a timedelta")
        if self.min_reprice_interval <= timedelta(0):
            raise ValueError("anchor_repricing.min_reprice_interval must be positive")
        if not isinstance(self.max_reprices_per_hour, int) or isinstance(self.max_reprices_per_hour, bool):
            raise TypeError("anchor_repricing.max_reprices_per_hour must be an integer")
        if self.max_reprices_per_hour <= 0:
            raise ValueError("anchor_repricing.max_reprices_per_hour must be positive")
        if not isinstance(self.slide_mode, bool):
            raise TypeError("anchor_repricing.slide_mode must be a bool")
        _positive_decimal_value(self.max_step_per_reprice, "anchor_repricing.max_step_per_reprice")
        if not isinstance(self.post_only_required, bool):
            raise TypeError("anchor_repricing.post_only_required must be a bool")
        _non_negative_decimal_value(
            self.follow_up_retreat_distance,
            "anchor_repricing.follow_up_retreat_distance",
        )
        _non_negative_decimal_value(
            self.follow_up_retreat_jitter,
            "anchor_repricing.follow_up_retreat_jitter",
        )

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "distance_type": self.distance_type.value,
            "enabled": self.enabled,
            "follow_up_retreat_distance": str(self.follow_up_retreat_distance),
            "follow_up_retreat_jitter": str(self.follow_up_retreat_jitter),
            "hysteresis_bps": str(self.hysteresis_bps),
            "max_distance": str(self.max_distance),
            "max_reprices_per_hour": self.max_reprices_per_hour,
            "max_step_per_reprice": str(self.max_step_per_reprice),
            "min_price_change": str(self.min_price_change),
            "min_reprice_interval_seconds": self.min_reprice_interval.total_seconds(),
            "post_only_required": self.post_only_required,
            "reference_price_source": self.reference_price_source.value,
            "slide_mode": self.slide_mode,
            "target_distance": str(self.target_distance),
            "update_mode": self.update_mode.value,
        }


@dataclass(frozen=True)
class OperatorPolicySizing:
    strategy: OperatorPolicySizingStrategy
    tranche_schedule: tuple[Decimal, ...]
    iceberg_mode: bool
    adaptive_base_size: Decimal | None
    adaptive_reveal_multiplier: Decimal
    adaptive_max_reveal_percentage: Decimal
    adaptive_volume_window: timedelta

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, OperatorPolicySizingStrategy):
            raise TypeError("sizing.strategy must be an OperatorPolicySizingStrategy")
        if not isinstance(self.tranche_schedule, tuple):
            raise TypeError("sizing.tranche_schedule must be a tuple")
        _validate_tranche_schedule(self.tranche_schedule)
        if not isinstance(self.iceberg_mode, bool):
            raise TypeError("sizing.iceberg_mode must be a bool")
        if self.adaptive_base_size is not None:
            _positive_decimal_value(self.adaptive_base_size, "sizing.adaptive_base_size")
        _positive_decimal_value(self.adaptive_reveal_multiplier, "sizing.adaptive_reveal_multiplier")
        _positive_decimal_value(
            self.adaptive_max_reveal_percentage,
            "sizing.adaptive_max_reveal_percentage",
        )
        if self.adaptive_max_reveal_percentage > Decimal("1"):
            raise ValueError("sizing.adaptive_max_reveal_percentage must be less than or equal to 1")
        if not isinstance(self.adaptive_volume_window, timedelta):
            raise TypeError("sizing.adaptive_volume_window must be a timedelta")
        if self.adaptive_volume_window <= timedelta(0):
            raise ValueError("sizing.adaptive_volume_window must be positive")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "adaptive_base_size": str(self.adaptive_base_size) if self.adaptive_base_size is not None else None,
            "adaptive_max_reveal_percentage": str(self.adaptive_max_reveal_percentage),
            "adaptive_reveal_multiplier": str(self.adaptive_reveal_multiplier),
            "adaptive_volume_window_seconds": self.adaptive_volume_window.total_seconds(),
            "iceberg_mode": self.iceberg_mode,
            "strategy": self.strategy.value,
            "tranche_schedule": [str(value) for value in self.tranche_schedule],
        }


@dataclass(frozen=True)
class OperatorPolicy:
    policy_name: str
    scope: OperatorPolicyScope
    risk_limits: OperatorPolicyRiskLimits
    market_data_requirements: OperatorPolicyMarketDataRequirements
    order_behavior: OperatorPolicyOrderBehavior
    lineage: OperatorPolicyLineage
    partial_fills: OperatorPolicyPartialFills
    moves: OperatorPolicyMoves
    staged_or_hidden_release: OperatorPolicyStagedRelease
    anchor_repricing: OperatorPolicyAnchorRepricing | None = None
    sizing: OperatorPolicySizing | None = None
    max_order_replacements: int | None = None
    target_movement: Decimal | None = None
    target_movement_type: OperatorPolicyDistanceType | None = None
    allow_partial_fills: bool | None = None
    enable_hotpoint_replication: bool | None = None
    schema_version: int = OPERATOR_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OPERATOR_POLICY_SCHEMA_VERSION:
            raise ValueError(f"operator policy schema_version must be {OPERATOR_POLICY_SCHEMA_VERSION}")
        if not isinstance(self.policy_name, str) or not self.policy_name:
            raise ValueError("policy_name is required")
        if not isinstance(self.scope, OperatorPolicyScope):
            raise TypeError("scope must be an OperatorPolicyScope")
        if not isinstance(self.risk_limits, OperatorPolicyRiskLimits):
            raise TypeError("risk_limits must be an OperatorPolicyRiskLimits")
        if not isinstance(self.market_data_requirements, OperatorPolicyMarketDataRequirements):
            raise TypeError("market_data_requirements must be an OperatorPolicyMarketDataRequirements")
        if not isinstance(self.order_behavior, OperatorPolicyOrderBehavior):
            raise TypeError("order_behavior must be an OperatorPolicyOrderBehavior")
        if not isinstance(self.lineage, OperatorPolicyLineage):
            raise TypeError("lineage must be an OperatorPolicyLineage")
        if not isinstance(self.partial_fills, OperatorPolicyPartialFills):
            raise TypeError("partial_fills must be an OperatorPolicyPartialFills")
        if not isinstance(self.moves, OperatorPolicyMoves):
            raise TypeError("moves must be an OperatorPolicyMoves")
        if not isinstance(self.staged_or_hidden_release, OperatorPolicyStagedRelease):
            raise TypeError("staged_or_hidden_release must be an OperatorPolicyStagedRelease")
        if self.anchor_repricing is not None and not isinstance(
            self.anchor_repricing,
            OperatorPolicyAnchorRepricing,
        ):
            raise TypeError("anchor_repricing must be an OperatorPolicyAnchorRepricing")
        if self.sizing is not None and not isinstance(self.sizing, OperatorPolicySizing):
            raise TypeError("sizing must be an OperatorPolicySizing")
        if self.max_order_replacements is not None:
            if not isinstance(self.max_order_replacements, int) or isinstance(self.max_order_replacements, bool):
                raise TypeError("max_order_replacements must be an integer")
            if self.max_order_replacements <= 0:
                raise ValueError("max_order_replacements must be positive")
        if self.target_movement is not None:
            _positive_decimal_value(self.target_movement, "target_movement")
        if self.target_movement_type is not None and not isinstance(
            self.target_movement_type,
            OperatorPolicyDistanceType,
        ):
            raise TypeError("target_movement_type must be an OperatorPolicyDistanceType")
        if (self.target_movement is None) != (self.target_movement_type is None):
            raise ValueError("target_movement and target_movement_type must be configured together")
        if self.allow_partial_fills is not None and not isinstance(self.allow_partial_fills, bool):
            raise TypeError("allow_partial_fills must be a bool")
        if self.enable_hotpoint_replication is not None and not isinstance(
            self.enable_hotpoint_replication,
            bool,
        ):
            raise TypeError("enable_hotpoint_replication must be a bool")

    def to_risk_policy_config(self) -> "RiskPolicyConfig":
        from config.assembly import RiskPolicyConfig

        return RiskPolicyConfig(
            allowed_lineage_relations=self.allowed_lineage_relations(),
            allowed_order_types=self.order_behavior.allowed_order_types,
            allowed_placement_kinds=self.allowed_placement_kinds(),
            allowed_products=self.scope.products,
            allowed_sides=self.risk_limits.allowed_sides,
            allowed_time_in_force=(self.order_behavior.time_in_force,),
            kill_switch_enabled=self.risk_limits.kill_switch_enabled,
            max_daily_notional=self.risk_limits.max_daily_notional_usd,
            max_open_orders=self.risk_limits.max_open_orders,
            max_order_notional=self.risk_limits.max_order_notional_usd,
            max_order_replacements=self.max_order_replacements,
            max_visible_notional=self.staged_or_hidden_release.max_visible_notional_usd,
            require_post_only=self.order_behavior.post_only
            or (
                self.anchor_repricing is not None
                and self.anchor_repricing.post_only_required
            ),
            require_reduce_only=self.risk_limits.reduce_only_first,
            require_staged_release_above_visible_limit=(
                self.staged_or_hidden_release.enabled
                and self.staged_or_hidden_release.release_only_when_conditions_match
            ),
        )

    def allowed_lineage_relations(self) -> tuple[OrderLineageRelation, ...]:
        relations = [OrderLineageRelation.ROOT]
        if self.lineage.followup_on_fill == OperatorPolicyPermission.ALLOWED:
            relations.append(OrderLineageRelation.FOLLOWUP_AFTER_FILL)
        if self.lineage.split_orders == OperatorPolicyPermission.ALLOWED:
            relations.append(OrderLineageRelation.SPLIT_CHILD)
        if self.lineage.merge_orders == OperatorPolicyPermission.ALLOWED:
            relations.append(OrderLineageRelation.CONSOLIDATION)
        if self.lineage.allow_manual_adoption:
            relations.append(OrderLineageRelation.MANUAL_ASSOCIATION)
        return tuple(dict.fromkeys(relations))

    def allowed_placement_kinds(self) -> tuple[OrderPlacementKind, ...]:
        kinds = [OrderPlacementKind.INITIAL]
        if self.lineage.move_same_side_orders == OperatorPolicyPermission.ALLOWED:
            kinds.extend((OrderPlacementKind.AMEND, OrderPlacementKind.CANCEL_REPLACE))
        if self.staged_or_hidden_release.enabled:
            kinds.append(OrderPlacementKind.STAGED_RELEASE)
            if self.staged_or_hidden_release.allow_release:
                kinds.append(OrderPlacementKind.RELEASE)
        return tuple(dict.fromkeys(kinds))

    def strategy_input_requirements(self) -> tuple[StrategyInputRequirement, ...]:
        return self.market_data_requirements.to_strategy_input_requirements(self.scope.products)

    def runtime_config_fragment(self) -> dict[str, JsonValue]:
        payload = {
            "bot": {
                "feed": {
                    "min_live_sources": 2 if self.market_data_requirements.require_redundant_feeds else 1,
                },
                "product_catalog": {
                    "enabled": self.market_data_requirements.require_product_metadata,
                    "product_ids": list(self.scope.products),
                },
                "risk": {
                    "allowed_lineage_relations": [
                        relation.value for relation in self.allowed_lineage_relations()
                    ],
                    "allowed_order_types": [
                        order_type.value for order_type in self.order_behavior.allowed_order_types
                    ],
                    "allowed_placement_kinds": [
                        kind.value for kind in self.allowed_placement_kinds()
                    ],
                    "allowed_products": list(self.scope.products),
                    "allowed_sides": [side.value for side in self.risk_limits.allowed_sides],
                    "allowed_time_in_force": [self.order_behavior.time_in_force.value],
                    "kill_switch_enabled": self.risk_limits.kill_switch_enabled,
                    "max_daily_notional": str(self.risk_limits.max_daily_notional_usd),
                    "max_open_orders": self.risk_limits.max_open_orders,
                    "max_order_notional": str(self.risk_limits.max_order_notional_usd),
                    "max_order_replacements": self.max_order_replacements,
                    "max_visible_notional": str(
                        self.staged_or_hidden_release.max_visible_notional_usd
                    ),
                    "require_post_only": self.order_behavior.post_only
                    or (
                        self.anchor_repricing is not None
                        and self.anchor_repricing.post_only_required
                    ),
                    "require_reduce_only": self.risk_limits.reduce_only_first,
                    "require_staged_release_above_visible_limit": (
                        self.staged_or_hidden_release.enabled
                        and self.staged_or_hidden_release.release_only_when_conditions_match
                    ),
                },
                "strategies": {
                    "allow_live_execution": self.scope.live_orders_allowed,
                    "market_data_requirements": [
                        requirement.to_payload() for requirement in self.strategy_input_requirements()
                    ],
                },
            }
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("operator policy runtime config fragment must normalize to an object")
        return normalized

    def review_notes(self) -> tuple[str, ...]:
        notes: list[str] = []
        if self.scope.live_orders_allowed and self.risk_limits.kill_switch_enabled:
            notes.append("live orders are allowed by policy, but kill_switch_enabled blocks new order placement")
        if self.risk_limits.max_daily_notional_usd < self.risk_limits.max_order_notional_usd:
            notes.append("max_daily_notional_usd is lower than max_order_notional_usd")
        if self.staged_or_hidden_release.enabled:
            notes.append("staged_or_hidden_release requires strategies to emit staged release intents")
        if self.staged_or_hidden_release.enabled and not self.staged_or_hidden_release.allow_release:
            notes.append("staged release placement is enabled, but release placement is blocked by policy")
        if self.anchor_repricing is not None and self.anchor_repricing.post_only_required and not self.order_behavior.post_only:
            notes.append("anchor_repricing requires post_only, but order_behavior.post_only is false")
        if self.sizing is not None and self.sizing.strategy == OperatorPolicySizingStrategy.ADAPTIVE:
            notes.append("adaptive sizing requires strategy-provided or policy-provided base size")
        if self.enable_hotpoint_replication:
            notes.append("hotpoint replication can compound exposure and must remain behind explicit risk gates")
        return tuple(notes)

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "allow_partial_fills": self.allow_partial_fills,
            "anchor_repricing": self.anchor_repricing.to_payload() if self.anchor_repricing is not None else None,
            "enable_hotpoint_replication": self.enable_hotpoint_replication,
            "lineage": self.lineage.to_payload(),
            "market_data_requirements": self.market_data_requirements.to_payload(),
            "max_order_replacements": self.max_order_replacements,
            "moves": self.moves.to_payload(),
            "order_behavior": self.order_behavior.to_payload(),
            "partial_fills": self.partial_fills.to_payload(),
            "policy_name": self.policy_name,
            "review_notes": list(self.review_notes()),
            "risk_limits": self.risk_limits.to_payload(),
            "runtime_config_fragment": self.runtime_config_fragment(),
            "schema_version": self.schema_version,
            "sizing": self.sizing.to_payload() if self.sizing is not None else None,
            "scope": self.scope.to_payload(),
            "staged_or_hidden_release": self.staged_or_hidden_release.to_payload(),
            "target_movement": str(self.target_movement) if self.target_movement is not None else None,
            "target_movement_type": self.target_movement_type.value if self.target_movement_type is not None else None,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("operator policy payload must normalize to an object")
        return normalized


def load_operator_policy_from_json_file(path: Path) -> OperatorPolicy:
    if not isinstance(path, Path):
        raise TypeError("path must be a pathlib.Path")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"operator policy file must be valid JSON: {exc}") from exc
    return operator_policy_from_mapping(raw)


def operator_policy_from_mapping(raw: object) -> OperatorPolicy:
    data = _mapping(raw, "operator_policy")
    _reject_unknown_fields(
        data,
        "operator_policy",
        {
            "allow_partial_fills",
            "anchor_repricing",
            "enable_hotpoint_replication",
            "lineage",
            "market_data_requirements",
            "max_order_replacements",
            "moves",
            "order_behavior",
            "partial_fills",
            "policy_name",
            "risk_limits",
            "schema_version",
            "sizing",
            "scope",
            "staged_or_hidden_release",
            "target_movement",
            "target_movement_type",
        },
    )
    anchor_repricing = data.get("anchor_repricing")
    sizing = data.get("sizing")
    target_movement = data.get("target_movement")
    target_movement_type = data.get("target_movement_type")
    max_order_replacements = data.get("max_order_replacements")
    allow_partial_fills = data.get("allow_partial_fills")
    enable_hotpoint_replication = data.get("enable_hotpoint_replication")
    return OperatorPolicy(
        allow_partial_fills=(
            _bool(allow_partial_fills, "allow_partial_fills")
            if allow_partial_fills is not None
            else None
        ),
        anchor_repricing=(
            _anchor_repricing(_mapping(anchor_repricing, "anchor_repricing"))
            if anchor_repricing is not None
            else None
        ),
        enable_hotpoint_replication=(
            _bool(enable_hotpoint_replication, "enable_hotpoint_replication")
            if enable_hotpoint_replication is not None
            else None
        ),
        lineage=_lineage(_mapping(data.get("lineage"), "lineage")),
        market_data_requirements=_market_data_requirements(
            _mapping(data.get("market_data_requirements"), "market_data_requirements")
        ),
        max_order_replacements=(
            _int(max_order_replacements, "max_order_replacements")
            if max_order_replacements is not None
            else None
        ),
        moves=_moves(_mapping(data.get("moves"), "moves")),
        order_behavior=_order_behavior(_mapping(data.get("order_behavior"), "order_behavior")),
        partial_fills=_partial_fills(_mapping(data.get("partial_fills"), "partial_fills")),
        policy_name=_string(data.get("policy_name"), "policy_name"),
        risk_limits=_risk_limits(_mapping(data.get("risk_limits"), "risk_limits")),
        schema_version=_int(data.get("schema_version"), "schema_version"),
        sizing=(_sizing(_mapping(sizing, "sizing")) if sizing is not None else None),
        scope=_scope(_mapping(data.get("scope"), "scope")),
        staged_or_hidden_release=_staged_release(
            _mapping(data.get("staged_or_hidden_release"), "staged_or_hidden_release")
        ),
        target_movement=(
            _decimal(target_movement, "target_movement")
            if target_movement is not None
            else None
        ),
        target_movement_type=(
            _enum(OperatorPolicyDistanceType, target_movement_type, "target_movement_type")
            if target_movement_type is not None
            else None
        ),
    )


def _scope(data: Mapping[str, Any]) -> OperatorPolicyScope:
    _reject_unknown_fields(data, "scope", {"live_orders_allowed", "products", "venue"})
    return OperatorPolicyScope(
        live_orders_allowed=_bool(data.get("live_orders_allowed"), "scope.live_orders_allowed"),
        products=tuple(_string(item, "scope.products[]") for item in _sequence(data.get("products"), "scope.products")),
        venue=_enum(OperatorPolicyVenue, data.get("venue"), "scope.venue"),
    )


def _risk_limits(data: Mapping[str, Any]) -> OperatorPolicyRiskLimits:
    _reject_unknown_fields(
        data,
        "risk_limits",
        {
            "allowed_sides",
            "kill_switch_enabled",
            "max_daily_notional_usd",
            "max_open_orders",
            "max_order_notional_usd",
            "reduce_only_first",
        },
    )
    return OperatorPolicyRiskLimits(
        allowed_sides=tuple(
            _enum(OrderSide, item, "risk_limits.allowed_sides[]")
            for item in _sequence(data.get("allowed_sides"), "risk_limits.allowed_sides")
        ),
        kill_switch_enabled=_bool(data.get("kill_switch_enabled"), "risk_limits.kill_switch_enabled"),
        max_daily_notional_usd=_decimal(data.get("max_daily_notional_usd"), "risk_limits.max_daily_notional_usd"),
        max_open_orders=_int(data.get("max_open_orders"), "risk_limits.max_open_orders"),
        max_order_notional_usd=_decimal(data.get("max_order_notional_usd"), "risk_limits.max_order_notional_usd"),
        reduce_only_first=_bool(data.get("reduce_only_first"), "risk_limits.reduce_only_first"),
    )


def _market_data_requirements(data: Mapping[str, Any]) -> OperatorPolicyMarketDataRequirements:
    _reject_unknown_fields(
        data,
        "market_data_requirements",
        {
            "max_order_book_age_seconds",
            "require_order_book",
            "require_product_metadata",
            "require_redundant_feeds",
        },
    )
    return OperatorPolicyMarketDataRequirements(
        max_order_book_age=timedelta(
            seconds=_positive_number(
                data.get("max_order_book_age_seconds"),
                "market_data_requirements.max_order_book_age_seconds",
            )
        ),
        require_order_book=_bool(data.get("require_order_book"), "market_data_requirements.require_order_book"),
        require_product_metadata=_bool(
            data.get("require_product_metadata"),
            "market_data_requirements.require_product_metadata",
        ),
        require_redundant_feeds=_bool(
            data.get("require_redundant_feeds"),
            "market_data_requirements.require_redundant_feeds",
        ),
    )


def _order_behavior(data: Mapping[str, Any]) -> OperatorPolicyOrderBehavior:
    _reject_unknown_fields(
        data,
        "order_behavior",
        {
            "allow_market_orders",
            "default_leverage",
            "default_margin_type",
            "default_order_type",
            "post_only",
            "time_in_force",
        },
    )
    return OperatorPolicyOrderBehavior(
        allow_market_orders=_bool(data.get("allow_market_orders"), "order_behavior.allow_market_orders"),
        default_leverage=_optional_decimal(
            data.get("default_leverage"),
            "order_behavior.default_leverage",
        ),
        default_margin_type=_optional_enum(
            MarginType,
            data.get("default_margin_type"),
            "order_behavior.default_margin_type",
        ),
        default_order_type=_enum(OrderType, data.get("default_order_type"), "order_behavior.default_order_type"),
        post_only=_bool(data.get("post_only"), "order_behavior.post_only"),
        time_in_force=_enum(TimeInForce, data.get("time_in_force"), "order_behavior.time_in_force"),
    )


def _lineage(data: Mapping[str, Any]) -> OperatorPolicyLineage:
    _reject_unknown_fields(
        data,
        "lineage",
        {
            "allow_manual_adoption",
            "followup_on_fill",
            "merge_orders",
            "model",
            "move_same_side_orders",
            "split_orders",
        },
    )
    return OperatorPolicyLineage(
        allow_manual_adoption=_bool(data.get("allow_manual_adoption"), "lineage.allow_manual_adoption"),
        followup_on_fill=_enum(
            OperatorPolicyPermission,
            data.get("followup_on_fill"),
            "lineage.followup_on_fill",
        ),
        merge_orders=_enum(OperatorPolicyPermission, data.get("merge_orders"), "lineage.merge_orders"),
        model=_enum(OperatorPolicyLineageModel, data.get("model"), "lineage.model"),
        move_same_side_orders=_enum(
            OperatorPolicyPermission,
            data.get("move_same_side_orders"),
            "lineage.move_same_side_orders",
        ),
        split_orders=_enum(OperatorPolicyPermission, data.get("split_orders"), "lineage.split_orders"),
    )


def _partial_fills(data: Mapping[str, Any]) -> OperatorPolicyPartialFills:
    _reject_unknown_fields(
        data,
        "partial_fills",
        {
            "followup_enabled",
            "followup_percent",
            "followup_size_mode",
            "min_followup_notional_usd",
        },
    )
    return OperatorPolicyPartialFills(
        followup_enabled=_bool(data.get("followup_enabled"), "partial_fills.followup_enabled"),
        followup_percent=_decimal(data.get("followup_percent"), "partial_fills.followup_percent"),
        followup_size_mode=_enum(
            OperatorPolicyFollowupSizeMode,
            data.get("followup_size_mode"),
            "partial_fills.followup_size_mode",
        ),
        min_followup_notional_usd=_decimal(
            data.get("min_followup_notional_usd"),
            "partial_fills.min_followup_notional_usd",
        ),
    )


def _moves(data: Mapping[str, Any]) -> OperatorPolicyMoves:
    _reject_unknown_fields(
        data,
        "moves",
        {"cancel_replace_when_amend_not_supported", "cooldown_seconds", "min_price_change_ticks"},
    )
    return OperatorPolicyMoves(
        cancel_replace_when_amend_not_supported=_bool(
            data.get("cancel_replace_when_amend_not_supported"),
            "moves.cancel_replace_when_amend_not_supported",
        ),
        cooldown=timedelta(seconds=_non_negative_number(data.get("cooldown_seconds"), "moves.cooldown_seconds")),
        min_price_change_ticks=_int(data.get("min_price_change_ticks"), "moves.min_price_change_ticks"),
    )


def _staged_release(data: Mapping[str, Any]) -> OperatorPolicyStagedRelease:
    _reject_unknown_fields(
        data,
        "staged_or_hidden_release",
        {"allow_release", "enabled", "max_visible_notional_usd", "release_only_when_conditions_match"},
    )
    return OperatorPolicyStagedRelease(
        allow_release=_bool(
            data.get("allow_release", True),
            "staged_or_hidden_release.allow_release",
        ),
        enabled=_bool(data.get("enabled"), "staged_or_hidden_release.enabled"),
        max_visible_notional_usd=_decimal(
            data.get("max_visible_notional_usd"),
            "staged_or_hidden_release.max_visible_notional_usd",
        ),
        release_only_when_conditions_match=_bool(
            data.get("release_only_when_conditions_match"),
            "staged_or_hidden_release.release_only_when_conditions_match",
        ),
    )


def _anchor_repricing(data: Mapping[str, Any]) -> OperatorPolicyAnchorRepricing:
    _reject_unknown_fields(
        data,
        "anchor_repricing",
        {
            "distance_type",
            "enabled",
            "follow_up_retreat_distance",
            "follow_up_retreat_jitter",
            "hysteresis_bps",
            "max_distance",
            "max_reprices_per_hour",
            "max_step_per_reprice",
            "min_price_change",
            "min_reprice_interval_seconds",
            "post_only_required",
            "reference_price_source",
            "slide_mode",
            "target_distance",
            "update_mode",
        },
    )
    return OperatorPolicyAnchorRepricing(
        distance_type=_enum(
            OperatorPolicyDistanceType,
            data.get("distance_type"),
            "anchor_repricing.distance_type",
        ),
        enabled=_bool(data.get("enabled"), "anchor_repricing.enabled"),
        follow_up_retreat_distance=_decimal(
            data.get("follow_up_retreat_distance"),
            "anchor_repricing.follow_up_retreat_distance",
        ),
        follow_up_retreat_jitter=_decimal(
            data.get("follow_up_retreat_jitter"),
            "anchor_repricing.follow_up_retreat_jitter",
        ),
        hysteresis_bps=_decimal(data.get("hysteresis_bps"), "anchor_repricing.hysteresis_bps"),
        max_distance=_decimal(data.get("max_distance"), "anchor_repricing.max_distance"),
        max_reprices_per_hour=_int(
            data.get("max_reprices_per_hour"),
            "anchor_repricing.max_reprices_per_hour",
        ),
        max_step_per_reprice=_decimal(
            data.get("max_step_per_reprice"),
            "anchor_repricing.max_step_per_reprice",
        ),
        min_price_change=_decimal(data.get("min_price_change"), "anchor_repricing.min_price_change"),
        min_reprice_interval=timedelta(
            seconds=_positive_number(
                data.get("min_reprice_interval_seconds"),
                "anchor_repricing.min_reprice_interval_seconds",
            )
        ),
        post_only_required=_bool(data.get("post_only_required"), "anchor_repricing.post_only_required"),
        reference_price_source=_enum(
            OperatorPolicyReferencePriceSource,
            data.get("reference_price_source"),
            "anchor_repricing.reference_price_source",
        ),
        slide_mode=_bool(data.get("slide_mode"), "anchor_repricing.slide_mode"),
        target_distance=_decimal(data.get("target_distance"), "anchor_repricing.target_distance"),
        update_mode=_enum(OperatorPolicyUpdateMode, data.get("update_mode"), "anchor_repricing.update_mode"),
    )


def _sizing(data: Mapping[str, Any]) -> OperatorPolicySizing:
    _reject_unknown_fields(
        data,
        "sizing",
        {
            "adaptive_base_size",
            "adaptive_max_reveal_percentage",
            "adaptive_reveal_multiplier",
            "adaptive_volume_window",
            "adaptive_volume_window_seconds",
            "iceberg_mode",
            "strategy",
            "tranche_schedule",
        },
    )
    adaptive_window = (
        data.get("adaptive_volume_window_seconds")
        if "adaptive_volume_window_seconds" in data
        else data.get("adaptive_volume_window")
    )
    adaptive_base_size = data.get("adaptive_base_size")
    return OperatorPolicySizing(
        adaptive_base_size=(
            _decimal(adaptive_base_size, "sizing.adaptive_base_size")
            if adaptive_base_size is not None
            else None
        ),
        adaptive_max_reveal_percentage=_decimal(
            data.get("adaptive_max_reveal_percentage"),
            "sizing.adaptive_max_reveal_percentage",
        ),
        adaptive_reveal_multiplier=_decimal(
            data.get("adaptive_reveal_multiplier"),
            "sizing.adaptive_reveal_multiplier",
        ),
        adaptive_volume_window=timedelta(
            seconds=_positive_number(adaptive_window, "sizing.adaptive_volume_window_seconds")
        ),
        iceberg_mode=_bool(data.get("iceberg_mode"), "sizing.iceberg_mode"),
        strategy=_enum(OperatorPolicySizingStrategy, data.get("strategy"), "sizing.strategy"),
        tranche_schedule=tuple(
            _decimal(item, "sizing.tranche_schedule[]")
            for item in _sequence(data.get("tranche_schedule"), "sizing.tranche_schedule")
        ),
    )


def _mapping(raw: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise TypeError(f"{field_name} must be a JSON object")
    return raw


def _reject_unknown_fields(data: Mapping[str, Any], field_name: str, allowed_fields: set[str]) -> None:
    unknown_fields = sorted(set(data) - allowed_fields)
    if unknown_fields:
        raise ValueError(f"{field_name} has unknown fields: {', '.join(unknown_fields)}")


def _string(raw: object, field_name: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{field_name} must be a non-empty string")
    return raw


def _bool(raw: object, field_name: str) -> bool:
    if not isinstance(raw, bool):
        raise TypeError(f"{field_name} must be a bool")
    return raw


def _int(raw: object, field_name: str) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise TypeError(f"{field_name} must be an integer")
    return raw


def _sequence(raw: object, field_name: str) -> Sequence[object]:
    if not isinstance(raw, list):
        raise TypeError(f"{field_name} must be a list")
    return raw


def _enum(enum_type: type[EnumValue], raw: object, field_name: str) -> EnumValue:
    if isinstance(raw, enum_type):
        return raw
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{field_name} must be a non-empty string")
    try:
        return enum_type(raw)  # type: ignore[call-arg]
    except ValueError as exc:
        raise ValueError(f"{field_name} has unsupported value: {raw}") from exc


def _optional_enum(
    enum_type: type[EnumValue],
    raw: object,
    field_name: str,
) -> EnumValue | None:
    if raw is None:
        return None
    return _enum(enum_type, raw, field_name)


def _decimal(raw: object, field_name: str) -> Decimal:
    if isinstance(raw, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return value


def _optional_decimal(raw: object, field_name: str) -> Decimal | None:
    if raw is None:
        return None
    return _decimal(raw, field_name)


def _positive_number(raw: object, field_name: str) -> float:
    value = _number(raw, field_name)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _non_negative_number(raw: object, field_name: str) -> float:
    value = _number(raw, field_name)
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")
    return value


def _number(raw: object, field_name: str) -> float:
    if not isinstance(raw, int | float) or isinstance(raw, bool):
        raise TypeError(f"{field_name} must be numeric")
    return float(raw)


def _non_empty_unique_strings(values: tuple[str, ...], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must not be empty")
    if any(not isinstance(value, str) or not value for value in values):
        raise TypeError(f"{field_name} must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must be unique")


def _positive_decimal_value(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _non_negative_decimal_value(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{field_name} must not be negative")


def _validate_tranche_schedule(values: tuple[Decimal, ...]) -> None:
    if not values:
        raise ValueError("sizing.tranche_schedule must not be empty")
    previous: Decimal | None = None
    for value in values:
        _positive_decimal_value(value, "sizing.tranche_schedule[]")
        if value > Decimal("1"):
            raise ValueError("sizing.tranche_schedule[] must be less than or equal to 1")
        if previous is not None and value <= previous:
            raise ValueError("sizing.tranche_schedule must be strictly increasing")
        previous = value
    if values[-1] != Decimal("1"):
        raise ValueError("sizing.tranche_schedule must end at 1")
