from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from actions.gateway import ActionCommand, CancelOrderIntent, PlaceOrderIntent
from core.json_tools import JsonValue, normalize_json
from strategies.harness import StrategyDecision, StrategyIntent, StrategySnapshot


@dataclass(frozen=True)
class StaticIntentStrategy:
    strategy_id: str
    intents: tuple[StrategyIntent, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id:
            raise ValueError("strategy_id must be a non-empty string")
        if not isinstance(self.intents, tuple):
            raise TypeError("intents must be a tuple")
        StrategyDecision(intents=self.intents, metadata=self.metadata)

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")
        return StrategyDecision(intents=self.intents, metadata=self.metadata)

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "intent_count": len(self.intents),
            "intents": [_intent_payload(intent) for intent in self.intents],
            "metadata": self.metadata,
            "strategy_id": self.strategy_id,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("static intent strategy payload must normalize to an object")
        return normalized


def _intent_payload(intent: StrategyIntent) -> dict[str, JsonValue]:
    if isinstance(intent, ActionCommand):
        return intent.to_payload()
    if isinstance(intent, (PlaceOrderIntent, CancelOrderIntent)):
        return intent.to_command().to_payload()
    raise TypeError("unsupported static intent type")
