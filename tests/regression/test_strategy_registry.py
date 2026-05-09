from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from strategies import (
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
    AnchorRepricingManagerStrategy,
    ConsolidationManagerStrategy,
    FollowupOnFillManagerStrategy,
    PassiveMarketMakingStrategy,
    StagedReleaseManagerStrategy,
    StrategyDecision,
    available_entry_point_strategy_ids,
    configured_strategies,
    load_entry_point_strategies,
)
from strategies.registry import STRATEGY_ENTRY_POINT_GROUP


class PluginStrategy:
    def __init__(self, strategy_id: str) -> None:
        self._strategy_id = strategy_id

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def evaluate(self, snapshot: object) -> StrategyDecision:
        del snapshot
        return StrategyDecision()


@dataclass
class FakeEntryPoint:
    name: str
    value: Any
    load_count: int = 0

    def load(self) -> Any:
        self.load_count += 1
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


def test_strategy_entry_point_group_uses_public_package_name():
    assert STRATEGY_ENTRY_POINT_GROUP == "staterail.strategies"


def test_entry_point_loader_loads_only_requested_strategy_instances():
    selected = FakeEntryPoint("plugin-a", PluginStrategy("plugin-a"))
    unselected = FakeEntryPoint("plugin-b", RuntimeError("should not load"))

    strategies = load_entry_point_strategies(
        ("plugin-a",),
        entry_points_source=(selected, unselected),
    )

    assert [strategy.strategy_id for strategy in strategies] == ["plugin-a"]
    assert selected.load_count == 1
    assert unselected.load_count == 0


def test_available_entry_point_strategy_ids_reports_names_without_loading():
    first = FakeEntryPoint("plugin-b", RuntimeError("should not load"))
    second = FakeEntryPoint("plugin-a", RuntimeError("should not load"))

    strategy_ids = available_entry_point_strategy_ids(entry_points_source=(first, second))

    assert strategy_ids == ("plugin-a", "plugin-b")
    assert first.load_count == 0
    assert second.load_count == 0


def test_entry_point_loader_accepts_no_argument_strategy_factories():
    strategies = load_entry_point_strategies(
        ("factory-strategy",),
        entry_points_source=(
            FakeEntryPoint("factory-strategy", lambda: PluginStrategy("factory-strategy")),
        ),
    )

    assert [strategy.strategy_id for strategy in strategies] == ["factory-strategy"]


def test_entry_point_loader_passes_parameters_to_opt_in_strategy_factories():
    observed_parameters: dict[str, Any] = {}

    def factory(*, parameters: dict[str, Any]) -> PluginStrategy:
        observed_parameters.update(parameters)
        return PluginStrategy("factory-strategy")

    strategies = load_entry_point_strategies(
        ("factory-strategy",),
        entry_points_source=(FakeEntryPoint("factory-strategy", factory),),
        strategy_parameters={"factory-strategy": {"limit": 2}},
    )

    assert [strategy.strategy_id for strategy in strategies] == ["factory-strategy"]
    assert observed_parameters == {"limit": 2}


def test_entry_point_loader_rejects_parameters_for_no_argument_strategy_factories():
    with pytest.raises(ValueError, match="does not accept parameters"):
        load_entry_point_strategies(
            ("factory-strategy",),
            entry_points_source=(
                FakeEntryPoint("factory-strategy", lambda: PluginStrategy("factory-strategy")),
            ),
            strategy_parameters={"factory-strategy": {"limit": 2}},
        )


def test_entry_point_loader_rejects_parameters_for_strategy_instances():
    with pytest.raises(ValueError, match="Strategy instance"):
        load_entry_point_strategies(
            ("plugin-a",),
            entry_points_source=(FakeEntryPoint("plugin-a", PluginStrategy("plugin-a")),),
            strategy_parameters={"plugin-a": {"limit": 2}},
        )


def test_entry_point_loader_rejects_duplicate_selected_names():
    with pytest.raises(ValueError, match="duplicate strategy entry point"):
        load_entry_point_strategies(
            ("plugin-a",),
            entry_points_source=(
                FakeEntryPoint("plugin-a", PluginStrategy("plugin-a")),
                FakeEntryPoint("plugin-a", PluginStrategy("plugin-a")),
            ),
        )


def test_entry_point_loader_rejects_strategy_id_mismatches():
    with pytest.raises(ValueError, match="must match strategy_id"):
        load_entry_point_strategies(
            ("plugin-a",),
            entry_points_source=(FakeEntryPoint("plugin-a", PluginStrategy("plugin-b")),),
        )


def test_entry_point_loader_wraps_load_errors_with_strategy_context():
    with pytest.raises(RuntimeError, match="strategy entry point failed to load: plugin-a"):
        load_entry_point_strategies(
            ("plugin-a",),
            entry_points_source=(FakeEntryPoint("plugin-a", RuntimeError("broken import")),),
        )


def test_configured_strategies_applies_passive_market_making_parameters():
    strategies = configured_strategies(
        (PASSIVE_MARKET_MAKING_STRATEGY_ID,),
        strategy_parameters={
            PASSIVE_MARKET_MAKING_STRATEGY_ID: {
                "half_spread_bps": "25",
                "max_products_per_evaluation": 1,
                "max_staged_release_count_per_side": 2,
                "target_notional_usd": "7.50",
            }
        },
    )
    strategy = strategies[0]

    assert isinstance(strategy, PassiveMarketMakingStrategy)
    assert strategy.half_spread_bps == Decimal("25")
    assert strategy.max_products_per_evaluation == 1
    assert strategy.max_staged_release_count_per_side == 2
    assert strategy.target_notional_usd == Decimal("7.50")


def test_configured_strategies_applies_staged_release_manager_parameters():
    strategies = configured_strategies(
        (STAGED_RELEASE_MANAGER_STRATEGY_ID,),
        strategy_parameters={
            STAGED_RELEASE_MANAGER_STRATEGY_ID: {
                "allow_live_overlap": True,
                "max_releases_per_evaluation": 2,
            }
        },
    )
    strategy = strategies[0]

    assert isinstance(strategy, StagedReleaseManagerStrategy)
    assert strategy.allow_live_overlap is True
    assert strategy.max_releases_per_evaluation == 2


def test_configured_strategies_applies_manager_strategy_parameters():
    strategies = configured_strategies(
        (
            ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
            CONSOLIDATION_MANAGER_STRATEGY_ID,
            FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
        ),
        strategy_parameters={
            ANCHOR_REPRICING_MANAGER_STRATEGY_ID: {"max_moves_per_evaluation": 2},
            CONSOLIDATION_MANAGER_STRATEGY_ID: {
                "max_consolidations_per_evaluation": 3,
                "max_source_orders_per_consolidation": 4,
            },
            FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID: {"max_followups_per_evaluation": 5},
        },
    )

    anchor, consolidation, followup = strategies

    assert isinstance(anchor, AnchorRepricingManagerStrategy)
    assert anchor.max_moves_per_evaluation == 2
    assert isinstance(consolidation, ConsolidationManagerStrategy)
    assert consolidation.max_consolidations_per_evaluation == 3
    assert consolidation.max_source_orders_per_consolidation == 4
    assert isinstance(followup, FollowupOnFillManagerStrategy)
    assert followup.max_followups_per_evaluation == 5


def test_configured_strategies_rejects_parameters_for_unselected_strategies():
    with pytest.raises(ValueError, match="unselected strategy_id"):
        configured_strategies(
            (PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            strategy_parameters={"noop": {}},
        )


def test_configured_strategies_rejects_parameters_for_unparameterized_builtins():
    with pytest.raises(ValueError, match="not supported"):
        configured_strategies(
            ("noop",),
            strategy_parameters={"noop": {"limit": 2}},
        )


def test_configured_strategies_rejects_unknown_staged_release_manager_parameters():
    with pytest.raises(ValueError, match="unknown staged-release-manager parameter"):
        configured_strategies(
            (STAGED_RELEASE_MANAGER_STRATEGY_ID,),
            strategy_parameters={STAGED_RELEASE_MANAGER_STRATEGY_ID: {"unknown": 1}},
        )


def test_configured_strategies_rejects_invalid_manager_strategy_parameters():
    with pytest.raises(ValueError, match="must be at least 2"):
        configured_strategies(
            (CONSOLIDATION_MANAGER_STRATEGY_ID,),
            strategy_parameters={
                CONSOLIDATION_MANAGER_STRATEGY_ID: {
                    "max_source_orders_per_consolidation": 1,
                }
            },
        )
