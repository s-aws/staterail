from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import MarketDataKind, OrderSide, StrategyManagerGate, StrategyManagerSkipReason
from core.errors import StrategyContractError, StrategyInputUnavailableError
from core.json_tools import JsonValue, normalize_json
from projections.state import OrderPlacementSnapshot
from strategies.harness import (
    StrategyDecision,
    StrategySnapshot,
    strategy_release_staged_placement_intent,
)
from strategies.parameters import bool_parameter, positive_int_parameter, reject_unknown_parameters


STAGED_RELEASE_MANAGER_STRATEGY_ID = "staged-release-manager"


@dataclass(frozen=True)
class StagedReleaseManagerStrategy:
    max_releases_per_evaluation: int = 1
    allow_live_overlap: bool = False

    @classmethod
    def from_parameters(
        cls,
        parameters: Mapping[str, Any] | None = None,
    ) -> "StagedReleaseManagerStrategy":
        raw = {} if parameters is None else parameters
        if not isinstance(raw, Mapping):
            raise TypeError("staged-release-manager parameters must be a mapping")
        reject_unknown_parameters(
            STAGED_RELEASE_MANAGER_STRATEGY_ID,
            raw,
            {"allow_live_overlap", "max_releases_per_evaluation"},
        )
        defaults = cls()
        return cls(
            allow_live_overlap=bool_parameter(
                raw.get("allow_live_overlap", defaults.allow_live_overlap),
                "staged-release-manager.allow_live_overlap",
            ),
            max_releases_per_evaluation=positive_int_parameter(
                raw.get("max_releases_per_evaluation", defaults.max_releases_per_evaluation),
                "staged-release-manager.max_releases_per_evaluation",
            ),
        )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_releases_per_evaluation, int)
            or isinstance(self.max_releases_per_evaluation, bool)
        ):
            raise TypeError("max_releases_per_evaluation must be an integer")
        if self.max_releases_per_evaluation <= 0:
            raise ValueError("max_releases_per_evaluation must be positive")
        if not isinstance(self.allow_live_overlap, bool):
            raise TypeError("allow_live_overlap must be a bool")

    @property
    def strategy_id(self) -> str:
        return STAGED_RELEASE_MANAGER_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")

        policy = snapshot.operator_policy
        metadata: dict[str, Any] = {
            "allow_live_overlap": self.allow_live_overlap,
            "max_releases_per_evaluation": self.max_releases_per_evaluation,
            "released_staged_placement_ids": [],
            "skipped_staged_placements": [],
        }
        if policy is None:
            metadata["release_gate"] = StrategyManagerGate.OPERATOR_POLICY_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))
        if not policy.staged_or_hidden_release.enabled:
            metadata["release_gate"] = StrategyManagerGate.STAGED_RELEASE_DISABLED.value
            return StrategyDecision(metadata=_metadata(metadata))
        if not policy.staged_or_hidden_release.allow_release:
            metadata["release_gate"] = StrategyManagerGate.RELEASE_DISABLED.value
            return StrategyDecision(metadata=_metadata(metadata))

        intents = []
        for placement in sorted(
            snapshot.projection.unreleased_staged_order_placements,
            key=lambda item: item.sequence,
        ):
            if len(intents) >= self.max_releases_per_evaluation:
                break
            if placement.product_id not in policy.scope.products:
                metadata["skipped_staged_placements"].append(
                    {
                        "placement_id": placement.placement_id,
                        "reason": StrategyManagerSkipReason.PRODUCT_OUTSIDE_OPERATOR_POLICY_SCOPE.value,
                    }
                )
                continue
            freshness_payload = None
            order_book_required = (
                policy.market_data_requirements.require_order_book
                or policy.staged_or_hidden_release.release_only_when_conditions_match
            )
            if order_book_required:
                freshness = snapshot.market_data_freshness(
                    data_kind=MarketDataKind.ORDER_BOOK,
                    max_age=policy.market_data_requirements.max_order_book_age,
                    product_id=placement.product_id,
                )
                freshness_payload = freshness.to_payload()
                if not freshness.is_ok:
                    metadata["skipped_staged_placements"].append(
                        {
                            "freshness": freshness_payload,
                            "placement_id": placement.placement_id,
                            "reason": StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value,
                        }
                    )
                    continue
            if policy.staged_or_hidden_release.release_only_when_conditions_match:
                match_result = _release_conditions_match(snapshot, placement)
                if not match_result.matched:
                    metadata["skipped_staged_placements"].append(
                        {
                            **match_result.to_payload(),
                            "placement_id": placement.placement_id,
                            "reason": match_result.reason.value,
                        }
                    )
                    continue
            try:
                intent = strategy_release_staged_placement_intent(
                    self.strategy_id,
                    "release",
                    snapshot,
                    placement.placement_id,
                    {"placement_sequence": placement.sequence},
                    allow_live_overlap=self.allow_live_overlap,
                )
            except (StrategyContractError, StrategyInputUnavailableError) as exc:
                metadata["skipped_staged_placements"].append(
                    {
                        "message": str(exc),
                        "placement_id": placement.placement_id,
                        "reason": (
                            StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value
                            if isinstance(exc, StrategyContractError)
                            else StrategyManagerSkipReason.STRATEGY_INPUT_UNAVAILABLE.value
                        ),
                    }
                )
                continue
            metadata["released_staged_placement_ids"].append(placement.placement_id)
            if freshness_payload is not None:
                metadata.setdefault("release_input_freshness", []).append(freshness_payload)
            intents.append(intent)

        return StrategyDecision(intents=tuple(intents), metadata=_metadata(metadata))


@dataclass(frozen=True)
class _ReleaseConditionMatchResult:
    matched: bool
    reason: StrategyManagerSkipReason
    best_ask_price: str | None = None
    best_bid_price: str | None = None
    limit_price: str | None = None
    side: OrderSide | None = None

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "best_ask_price": self.best_ask_price,
            "best_bid_price": self.best_bid_price,
            "limit_price": self.limit_price,
            "matched": self.matched,
            "side": self.side.value if self.side is not None else None,
        }


def _metadata(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(raw)
    if not isinstance(normalized, dict):
        raise TypeError("staged release manager metadata must normalize to an object")
    return normalized


def _release_conditions_match(
    snapshot: StrategySnapshot,
    placement: OrderPlacementSnapshot,
) -> _ReleaseConditionMatchResult:
    book = snapshot.projection.order_book(placement.product_id)
    if book is None:
        return _ReleaseConditionMatchResult(
            matched=False,
            reason=StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH,
            limit_price=placement.limit_price,
            side=placement.side,
        )

    limit_price = _positive_decimal_or_none(placement.limit_price)
    best_bid = _positive_decimal_or_none(book.best_bid_price)
    best_ask = _positive_decimal_or_none(book.best_ask_price)
    if limit_price is None or best_bid is None or best_ask is None or best_bid >= best_ask:
        return _ReleaseConditionMatchResult(
            best_ask_price=book.best_ask_price,
            best_bid_price=book.best_bid_price,
            limit_price=placement.limit_price,
            matched=False,
            reason=StrategyManagerSkipReason.RELEASE_CONDITIONS_NOT_MATCHED,
            side=placement.side,
        )

    if placement.side == OrderSide.BUY:
        matched = limit_price < best_ask
    elif placement.side == OrderSide.SELL:
        matched = limit_price > best_bid
    else:
        matched = False
    return _ReleaseConditionMatchResult(
        best_ask_price=book.best_ask_price,
        best_bid_price=book.best_bid_price,
        limit_price=placement.limit_price,
        matched=matched,
        reason=StrategyManagerSkipReason.RELEASE_CONDITIONS_NOT_MATCHED,
        side=placement.side,
    )


def _positive_decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed
