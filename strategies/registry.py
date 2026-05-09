from __future__ import annotations

from collections.abc import Iterable, Mapping
from inspect import Parameter, signature
from importlib.metadata import entry_points
from typing import Any, Protocol

from strategies.anchor_repricing_manager import (
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    AnchorRepricingManagerStrategy,
)
from strategies.consolidation_manager import (
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    ConsolidationManagerStrategy,
)
from strategies.followup_manager import (
    FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
    FollowupOnFillManagerStrategy,
)
from strategies.harness import NOOP_STRATEGY_ID, NoOpStrategy, Strategy, select_strategies
from strategies.passive_market_making import (
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    PassiveMarketMakingStrategy,
)
from strategies.policy_probe import POLICY_PROBE_STRATEGY_ID, PolicyProbeStrategy
from strategies.staged_release_manager import (
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
    StagedReleaseManagerStrategy,
)


STRATEGY_ENTRY_POINT_GROUP = "staterail.strategies"


class StrategyEntryPoint(Protocol):
    name: str

    def load(self) -> Any:
        ...


def load_entry_point_strategies(
    strategy_ids: tuple[str, ...],
    *,
    entry_points_source: Iterable[StrategyEntryPoint] | None = None,
    strategy_parameters: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[Strategy, ...]:
    if not isinstance(strategy_ids, tuple):
        raise TypeError("strategy_ids must be a tuple")
    if not strategy_ids:
        return ()
    parsed_parameters = _validated_strategy_parameters(strategy_ids, strategy_parameters)

    requested_ids = set(strategy_ids)
    available_entry_points = tuple(
        entry_points(group=STRATEGY_ENTRY_POINT_GROUP)
        if entry_points_source is None
        else entry_points_source
    )
    selected_entry_points: dict[str, StrategyEntryPoint] = {}
    duplicate_names: set[str] = set()
    for entry_point in available_entry_points:
        if entry_point.name not in requested_ids:
            continue
        if entry_point.name in selected_entry_points:
            duplicate_names.add(entry_point.name)
            continue
        selected_entry_points[entry_point.name] = entry_point

    if duplicate_names:
        duplicates = ", ".join(sorted(duplicate_names))
        raise ValueError(f"duplicate strategy entry point(s): {duplicates}")

    loaded_strategies: list[Strategy] = []
    for strategy_id in strategy_ids:
        entry_point = selected_entry_points.get(strategy_id)
        if entry_point is None:
            continue
        loaded_strategies.append(
            _load_strategy_entry_point(
                entry_point,
                expected_strategy_id=strategy_id,
                parameters=parsed_parameters.get(strategy_id),
            )
        )
    return tuple(loaded_strategies)


def configured_strategies(
    strategy_ids: tuple[str, ...],
    *,
    static_strategies: tuple[Strategy, ...] = (),
    strategy_parameters: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[Strategy, ...]:
    return select_strategies(
        available_strategies(
            strategy_ids=strategy_ids,
            static_strategies=static_strategies,
            strategy_parameters=strategy_parameters,
        ),
        strategy_ids,
    )


def available_strategies(
    *,
    strategy_ids: tuple[str, ...],
    static_strategies: tuple[Strategy, ...] = (),
    strategy_parameters: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[Strategy, ...]:
    parsed_parameters = _validated_strategy_parameters(strategy_ids, strategy_parameters)
    static_available = (
        NoOpStrategy(),
        AnchorRepricingManagerStrategy.from_parameters(
            parsed_parameters.get(ANCHOR_REPRICING_MANAGER_STRATEGY_ID)
        ),
        PolicyProbeStrategy(),
        ConsolidationManagerStrategy.from_parameters(
            parsed_parameters.get(CONSOLIDATION_MANAGER_STRATEGY_ID)
        ),
        FollowupOnFillManagerStrategy.from_parameters(
            parsed_parameters.get(FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID)
        ),
        PassiveMarketMakingStrategy.from_parameters(
            parsed_parameters.get(PASSIVE_MARKET_MAKING_STRATEGY_ID)
        ),
        StagedReleaseManagerStrategy.from_parameters(
            parsed_parameters.get(STAGED_RELEASE_MANAGER_STRATEGY_ID)
        ),
        *static_strategies,
    )
    static_strategy_ids = {
        strategy_id
        for strategy in static_available
        if isinstance(strategy_id := getattr(strategy, "strategy_id", None), str)
    }
    _reject_unapplied_static_strategy_parameters(parsed_parameters, static_strategy_ids)
    entry_point_strategy_ids = tuple(strategy_id for strategy_id in strategy_ids if strategy_id not in static_strategy_ids)
    if not entry_point_strategy_ids:
        return static_available
    return (
        *static_available,
        *load_entry_point_strategies(
            entry_point_strategy_ids,
            strategy_parameters={
                strategy_id: parameters
                for strategy_id, parameters in parsed_parameters.items()
                if strategy_id in entry_point_strategy_ids
            },
        ),
    )


def available_entry_point_strategy_ids(
    *,
    entry_points_source: Iterable[StrategyEntryPoint] | None = None,
) -> tuple[str, ...]:
    available_entry_points = tuple(
        entry_points(group=STRATEGY_ENTRY_POINT_GROUP)
        if entry_points_source is None
        else entry_points_source
    )
    return tuple(sorted({entry_point.name for entry_point in available_entry_points if entry_point.name}))


def validate_strategy_parameters(
    strategy_ids: tuple[str, ...],
    strategy_parameters: Mapping[str, Mapping[str, Any]] | None,
) -> None:
    _validated_strategy_parameters(strategy_ids, strategy_parameters)


def _validated_strategy_parameters(
    strategy_ids: tuple[str, ...],
    strategy_parameters: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, Mapping[str, Any]]:
    if strategy_parameters is None:
        return {}
    if not isinstance(strategy_parameters, Mapping):
        raise TypeError("strategy_parameters must be a mapping")
    selected_strategy_ids = set(strategy_ids)
    parsed: dict[str, Mapping[str, Any]] = {}
    for strategy_id, parameters in strategy_parameters.items():
        if not isinstance(strategy_id, str) or not strategy_id:
            raise ValueError("strategy_parameters keys must be non-empty strategy IDs")
        if strategy_id not in selected_strategy_ids:
            raise ValueError(f"strategy_parameters configured for unselected strategy_id: {strategy_id}")
        if not isinstance(parameters, Mapping):
            raise TypeError(f"strategy_parameters[{strategy_id}] must be a mapping")
        if strategy_id in _UNPARAMETERIZED_BUILTIN_STRATEGY_IDS and parameters:
            raise ValueError(f"strategy parameters are not supported for strategy_id: {strategy_id}")
        parsed[strategy_id] = parameters
    return parsed


_PARAMETERIZED_BUILTIN_STRATEGY_IDS = {
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
}
_UNPARAMETERIZED_BUILTIN_STRATEGY_IDS = {
    NOOP_STRATEGY_ID,
    POLICY_PROBE_STRATEGY_ID,
}


def _reject_unapplied_static_strategy_parameters(
    strategy_parameters: Mapping[str, Mapping[str, Any]],
    static_strategy_ids: set[str],
) -> None:
    for strategy_id, parameters in strategy_parameters.items():
        if (
            parameters
            and strategy_id in static_strategy_ids
            and strategy_id not in _PARAMETERIZED_BUILTIN_STRATEGY_IDS
        ):
            raise ValueError(
                "strategy parameters cannot be applied to prebuilt static strategy_id: "
                f"{strategy_id}"
            )


def _load_strategy_entry_point(
    entry_point: StrategyEntryPoint,
    *,
    expected_strategy_id: str,
    parameters: Mapping[str, Any] | None = None,
) -> Strategy:
    try:
        loaded = entry_point.load()
    except Exception as exc:
        raise RuntimeError(f"strategy entry point failed to load: {expected_strategy_id}") from exc

    raw_parameters = {} if parameters is None else parameters
    if not isinstance(raw_parameters, Mapping):
        raise TypeError(f"strategy parameters must be a mapping: {expected_strategy_id}")
    if _looks_like_strategy(loaded):
        if raw_parameters:
            raise ValueError(
                "strategy entry point returned a Strategy instance, but parameters were configured: "
                f"{expected_strategy_id}"
            )
        strategy = loaded
    else:
        strategy = _strategy_from_factory(loaded, expected_strategy_id, raw_parameters)
    observed_strategy_id = strategy.strategy_id
    if observed_strategy_id != expected_strategy_id:
        raise ValueError(
            "strategy entry point name must match strategy_id: "
            f"expected {expected_strategy_id}, observed {observed_strategy_id}"
        )
    return strategy


def _strategy_from_factory(
    value: Any,
    expected_strategy_id: str,
    parameters: Mapping[str, Any],
) -> Strategy:
    if not callable(value):
        raise TypeError(f"strategy entry point is not a Strategy or no-argument factory: {expected_strategy_id}")
    accepts_parameters = _factory_accepts_parameters(value)
    if parameters and not accepts_parameters:
        raise ValueError(f"strategy entry point factory does not accept parameters: {expected_strategy_id}")
    strategy = value(parameters=parameters) if accepts_parameters else value()
    if not _looks_like_strategy(strategy):
        raise TypeError(f"strategy entry point factory did not return a Strategy: {expected_strategy_id}")
    return strategy


def _factory_accepts_parameters(value: Any) -> bool:
    try:
        parameters = signature(value).parameters
    except (TypeError, ValueError):
        return False
    if "parameters" in parameters:
        return True
    return any(parameter.kind == Parameter.VAR_KEYWORD for parameter in parameters.values())


def _looks_like_strategy(value: Any) -> bool:
    return isinstance(getattr(value, "strategy_id", None), str) and callable(getattr(value, "evaluate", None))
