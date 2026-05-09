from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from actions.gateway import CancelOrderIntent
from core.enums import (
    MarketDataKind,
    OperatorPolicyPermission,
    OrderLifecycleStatus,
    OrderPlacementKind,
    OrderSide,
    StrategyManagerGate,
    StrategyManagerSkipReason,
)
from core.errors import StrategyContractError, StrategyInputUnavailableError
from core.json_tools import JsonValue, normalize_json
from projections.state import LogicalOrderSnapshot, OrderSnapshot
from strategies.harness import (
    StrategyDecision,
    StrategySnapshot,
    strategy_action_id,
    strategy_client_order_id,
    strategy_consolidation_intent,
)
from strategies.parameters import int_parameter_at_least, positive_int_parameter, reject_unknown_parameters


CONSOLIDATION_MANAGER_STRATEGY_ID = "consolidation-manager"


@dataclass(frozen=True)
class ConsolidationManagerStrategy:
    max_consolidations_per_evaluation: int = 1
    max_source_orders_per_consolidation: int = 2

    @classmethod
    def from_parameters(
        cls,
        parameters: Mapping[str, Any] | None = None,
    ) -> "ConsolidationManagerStrategy":
        raw = {} if parameters is None else parameters
        if not isinstance(raw, Mapping):
            raise TypeError("consolidation-manager parameters must be a mapping")
        reject_unknown_parameters(
            CONSOLIDATION_MANAGER_STRATEGY_ID,
            raw,
            {"max_consolidations_per_evaluation", "max_source_orders_per_consolidation"},
        )
        defaults = cls()
        return cls(
            max_consolidations_per_evaluation=positive_int_parameter(
                raw.get(
                    "max_consolidations_per_evaluation",
                    defaults.max_consolidations_per_evaluation,
                ),
                "consolidation-manager.max_consolidations_per_evaluation",
            ),
            max_source_orders_per_consolidation=int_parameter_at_least(
                raw.get(
                    "max_source_orders_per_consolidation",
                    defaults.max_source_orders_per_consolidation,
                ),
                "consolidation-manager.max_source_orders_per_consolidation",
                minimum=2,
            ),
        )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_consolidations_per_evaluation, int)
            or isinstance(self.max_consolidations_per_evaluation, bool)
        ):
            raise TypeError("max_consolidations_per_evaluation must be an integer")
        if self.max_consolidations_per_evaluation <= 0:
            raise ValueError("max_consolidations_per_evaluation must be positive")
        if (
            not isinstance(self.max_source_orders_per_consolidation, int)
            or isinstance(self.max_source_orders_per_consolidation, bool)
        ):
            raise TypeError("max_source_orders_per_consolidation must be an integer")
        if self.max_source_orders_per_consolidation < 2:
            raise ValueError("max_source_orders_per_consolidation must be at least 2")

    @property
    def strategy_id(self) -> str:
        return CONSOLIDATION_MANAGER_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")

        metadata: dict[str, Any] = {
            "created_consolidations": [],
            "max_consolidations_per_evaluation": self.max_consolidations_per_evaluation,
            "max_source_orders_per_consolidation": self.max_source_orders_per_consolidation,
            "skipped_groups": [],
            "skipped_source_orders": [],
        }
        policy = snapshot.operator_policy
        if policy is None:
            metadata["consolidation_gate"] = StrategyManagerGate.OPERATOR_POLICY_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))
        if policy.lineage.merge_orders != OperatorPolicyPermission.ALLOWED:
            metadata["consolidation_gate"] = StrategyManagerGate.CONSOLIDATION_DISABLED.value
            return StrategyDecision(metadata=_metadata(metadata))
        if snapshot.product_catalog is None:
            metadata["consolidation_gate"] = StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))

        groups, skipped_sources = _candidate_groups(snapshot)
        metadata["skipped_source_orders"].extend(skipped_sources)

        intents = []
        for group in groups:
            if len(metadata["created_consolidations"]) >= self.max_consolidations_per_evaluation:
                break
            selected_sources = group.sources[: self.max_source_orders_per_consolidation]
            source_ids = tuple(source.logical_order_id for source in selected_sources)
            if group.product_id not in policy.scope.products:
                metadata["skipped_groups"].append(
                    {
                        "product_id": group.product_id,
                        "reason": StrategyManagerSkipReason.PRODUCT_OUTSIDE_OPERATOR_POLICY_SCOPE.value,
                        "source_order_ids": list(source_ids),
                    }
                )
                continue
            freshness_payload = None
            if policy.market_data_requirements.require_order_book:
                freshness = snapshot.market_data_freshness(
                    data_kind=MarketDataKind.ORDER_BOOK,
                    max_age=policy.market_data_requirements.max_order_book_age,
                    product_id=group.product_id,
                )
                freshness_payload = freshness.to_payload()
                if not freshness.is_ok:
                    metadata["skipped_groups"].append(
                        {
                            "freshness": freshness_payload,
                            "product_id": group.product_id,
                            "reason": StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value,
                            "source_order_ids": list(source_ids),
                        }
                    )
                    continue

            try:
                cancel_intents = tuple(
                    _cancel_source_intent(self.strategy_id, source) for source in selected_sources
                )
                existing_cancel_action_ids = tuple(
                    intent.action_id
                    for intent in cancel_intents
                    if intent.action_id in snapshot.projection.actions
                )
                if existing_cancel_action_ids:
                    raise StrategyContractError(
                        "consolidation cancel action already exists",
                        context={
                            "cancel_action_ids": list(existing_cancel_action_ids),
                            "source_order_ids": list(source_ids),
                        },
                    )
                consolidation_intent = strategy_consolidation_intent(
                    self.strategy_id,
                    "tidy",
                    snapshot,
                    source_ids,
                    {
                        "limit_price": str(group.limit_price),
                        "product_id": group.product_id,
                        "side": group.side.value,
                    },
                    limit_price=str(group.limit_price),
                )
            except (StrategyContractError, StrategyInputUnavailableError) as exc:
                metadata["skipped_groups"].append(
                    {
                        "message": str(exc),
                        "product_id": group.product_id,
                        "reason": (
                            StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value
                            if isinstance(exc, StrategyContractError)
                            else StrategyManagerSkipReason.STRATEGY_INPUT_UNAVAILABLE.value
                        ),
                        "source_order_ids": list(source_ids),
                    }
                )
                continue

            metadata["created_consolidations"].append(
                {
                    "cancel_action_ids": [intent.action_id for intent in cancel_intents],
                    "limit_price": str(group.limit_price),
                    "placement_action_id": consolidation_intent.action_id,
                    "product_id": group.product_id,
                    "side": group.side.value,
                    "source_order_ids": list(source_ids),
                }
            )
            if freshness_payload is not None:
                metadata.setdefault("consolidation_input_freshness", []).append(freshness_payload)
            intents.extend((*cancel_intents, consolidation_intent))

        return StrategyDecision(intents=tuple(intents), metadata=_metadata(metadata))


@dataclass(frozen=True)
class _CandidateSource:
    action_id: str
    client_order_id: str | None
    exchange_order_id: str | None
    limit_price: Decimal
    logical_order_id: str
    product_id: str
    sequence: int
    side: OrderSide


@dataclass(frozen=True)
class _CandidateGroup:
    limit_price: Decimal
    product_id: str
    side: OrderSide
    sources: tuple[_CandidateSource, ...]


def _candidate_groups(
    snapshot: StrategySnapshot,
) -> tuple[tuple[_CandidateGroup, ...], list[dict[str, JsonValue]]]:
    groups: dict[tuple[str, OrderSide, Decimal], list[_CandidateSource]] = {}
    skipped_sources: list[dict[str, JsonValue]] = []
    for logical_order in sorted(
        snapshot.projection.logical_orders_by_id.values(),
        key=lambda item: item.sequence,
    ):
        source, skip = _candidate_source(snapshot, logical_order)
        if skip is not None:
            skipped_sources.append(skip)
        if source is None:
            continue
        groups.setdefault(
            (source.product_id, source.side, source.limit_price),
            [],
        ).append(source)

    candidate_groups = [
        _CandidateGroup(
            limit_price=limit_price,
            product_id=product_id,
            side=side,
            sources=tuple(sorted(sources, key=lambda source: source.sequence)),
        )
        for (product_id, side, limit_price), sources in groups.items()
        if len(sources) >= 2
    ]
    return (
        tuple(
            sorted(
                candidate_groups,
                key=lambda group: (group.sources[0].sequence, group.product_id, group.side.value, group.limit_price),
            )
        ),
        skipped_sources,
    )


def _candidate_source(
    snapshot: StrategySnapshot,
    logical_order: LogicalOrderSnapshot,
) -> tuple[_CandidateSource | None, dict[str, JsonValue] | None]:
    live_orders: list[OrderSnapshot] = []
    for placement in snapshot.projection.placements_for_logical_order(logical_order.logical_order_id):
        if placement.placement_kind == OrderPlacementKind.STAGED_RELEASE:
            continue
        if placement.action_id is None:
            continue
        order = snapshot.projection.orders_by_action_id.get(placement.action_id)
        if order is None or order.lifecycle_status not in _CONSOLIDATABLE_ORDER_STATUSES:
            continue
        live_orders.append(order)

    if not live_orders:
        return None, None
    if len(live_orders) > 1:
        return (
            None,
            {
                "logical_order_id": logical_order.logical_order_id,
                "reason": StrategyManagerSkipReason.ORDER_NOT_CANCELABLE.value,
            },
        )

    order = live_orders[0]
    if order.exchange_order_id is None and order.client_order_id is None:
        return (
            None,
            {
                "logical_order_id": logical_order.logical_order_id,
                "reason": StrategyManagerSkipReason.ORDER_NOT_CANCELABLE.value,
            },
        )
    if _decimal_or_zero(order.filled_size) != Decimal("0"):
        return None, None
    if order.product_id is None or order.side is None:
        return None, None
    limit_price = _positive_decimal_or_none(order.limit_price)
    if limit_price is None:
        return None, None
    return (
        _CandidateSource(
            action_id=order.action_id,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            limit_price=limit_price,
            logical_order_id=logical_order.logical_order_id,
            product_id=order.product_id,
            sequence=logical_order.sequence,
            side=order.side,
        ),
        None,
    )


def _cancel_source_intent(strategy_id: str, source: _CandidateSource) -> CancelOrderIntent:
    identity = {
        "source_action_id": source.action_id,
        "source_logical_order_id": source.logical_order_id,
    }
    action_id = strategy_action_id(strategy_id, "cancel-consolidation-source", identity)
    return CancelOrderIntent(
        action_id=action_id,
        client_order_id=source.client_order_id,
        exchange_order_id=source.exchange_order_id,
        idempotency_key=strategy_client_order_id(strategy_id, "cancel-consolidation-source", identity),
    )


def _decimal_or_zero(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return Decimal("0")
    if not parsed.is_finite():
        return Decimal("0")
    return parsed


def _positive_decimal_or_none(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _metadata(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(raw)
    if not isinstance(normalized, dict):
        raise TypeError("consolidation manager metadata must normalize to an object")
    return normalized


_CONSOLIDATABLE_ORDER_STATUSES = frozenset(
    {
        OrderLifecycleStatus.ACCEPTED,
        OrderLifecycleStatus.OPEN,
    }
)
