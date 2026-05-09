from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from app.bootstrap import CoinbaseApplicationConfig
from config.assembly import effective_risk_policy_config, effective_strategy_market_data_requirements
from core.clock import Clock
from core.json_tools import JsonValue
from risk.gate import RiskGate
from strategies import (
    Strategy,
    configured_strategies,
    load_strategy_scenario_from_json_file,
    run_strategy_scenario,
)


def strategy_scenario_payload(
    config: CoinbaseApplicationConfig,
    *,
    scenario_file: Path,
    clock: Clock | None = None,
    strategies: Iterable[Strategy] = (),
) -> dict[str, JsonValue]:
    scenario = load_strategy_scenario_from_json_file(scenario_file)
    strategy_ids = scenario.strategy_ids or config.bot.strategies.strategy_ids
    if not strategy_ids:
        raise ValueError("strategy scenario requires strategy ids from config or scenario")
    selected_strategies = configured_strategies(
        strategy_ids,
        static_strategies=(*tuple(strategies), *scenario.static_strategies),
        strategy_parameters={
            strategy_id: parameters
            for strategy_id, parameters in config.bot.strategies.strategy_parameters.items()
            if strategy_id in strategy_ids
        },
    )
    result = run_strategy_scenario(
        clock=clock,
        ledger_path=config.ledger_path,
        market_data_requirements=effective_strategy_market_data_requirements(config.bot),
        operator_policy=config.bot.strategies.operator_policy,
        product_catalog_from_scenario=True,
        risk_gate_factory=lambda product_catalog: RiskGate(
            effective_risk_policy_config(config.bot).to_policy(product_catalog=product_catalog)
        ),
        scenario=scenario,
        strategies=selected_strategies,
    )
    return result.to_payload()
