from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from core.enums import (
    OperatorPolicyPermission,
    StrategyManagerGate,
    StrategyManagerSkipReason,
)
from core.errors import StrategyContractError, StrategyInputUnavailableError
from core.json_tools import JsonValue, normalize_json
from strategies.harness import (
    StrategyDecision,
    StrategySnapshot,
    strategy_followup_after_fill_intent,
)
from strategies.parameters import positive_int_parameter, reject_unknown_parameters


FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID = "followup-on-fill-manager"


@dataclass(frozen=True)
class FollowupOnFillManagerStrategy:
    max_followups_per_evaluation: int = 1

    @classmethod
    def from_parameters(
        cls,
        parameters: Mapping[str, Any] | None = None,
    ) -> "FollowupOnFillManagerStrategy":
        raw = {} if parameters is None else parameters
        if not isinstance(raw, Mapping):
            raise TypeError("followup-on-fill-manager parameters must be a mapping")
        reject_unknown_parameters(
            FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
            raw,
            {"max_followups_per_evaluation"},
        )
        defaults = cls()
        return cls(
            max_followups_per_evaluation=positive_int_parameter(
                raw.get("max_followups_per_evaluation", defaults.max_followups_per_evaluation),
                "followup-on-fill-manager.max_followups_per_evaluation",
            ),
        )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_followups_per_evaluation, int)
            or isinstance(self.max_followups_per_evaluation, bool)
        ):
            raise TypeError("max_followups_per_evaluation must be an integer")
        if self.max_followups_per_evaluation <= 0:
            raise ValueError("max_followups_per_evaluation must be positive")

    @property
    def strategy_id(self) -> str:
        return FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")

        metadata: dict[str, Any] = {
            "created_followup_fill_ids": [],
            "max_followups_per_evaluation": self.max_followups_per_evaluation,
            "skipped_fills": [],
        }
        policy = snapshot.operator_policy
        if policy is None:
            metadata["followup_gate"] = StrategyManagerGate.OPERATOR_POLICY_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))
        if (
            policy.lineage.followup_on_fill != OperatorPolicyPermission.ALLOWED
            or not policy.partial_fills.followup_enabled
        ):
            metadata["followup_gate"] = StrategyManagerGate.FOLLOWUP_DISABLED.value
            return StrategyDecision(metadata=_metadata(metadata))
        if snapshot.product_catalog is None:
            metadata["followup_gate"] = StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
            return StrategyDecision(metadata=_metadata(metadata))

        intents = []
        for fill in sorted(snapshot.projection.fills_by_id.values(), key=lambda item: item.sequence):
            if len(intents) >= self.max_followups_per_evaluation:
                break
            try:
                intent = strategy_followup_after_fill_intent(
                    self.strategy_id,
                    "fill",
                    snapshot,
                    fill.fill_id,
                )
            except (StrategyContractError, StrategyInputUnavailableError) as exc:
                metadata["skipped_fills"].append(
                    {
                        "fill_id": fill.fill_id,
                        "message": str(exc),
                        "reason": _skip_reason(exc),
                    }
                )
                continue
            metadata["created_followup_fill_ids"].append(fill.fill_id)
            intents.append(intent)

        return StrategyDecision(intents=tuple(intents), metadata=_metadata(metadata))


def _skip_reason(exc: Exception) -> str:
    if isinstance(exc, StrategyInputUnavailableError):
        return StrategyManagerSkipReason.STRATEGY_INPUT_UNAVAILABLE.value
    if isinstance(exc, StrategyContractError):
        return StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value
    return StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value


def _metadata(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(raw)
    if not isinstance(normalized, dict):
        raise TypeError("followup manager metadata must normalize to an object")
    return normalized
