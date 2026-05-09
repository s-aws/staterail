from __future__ import annotations

from collections.abc import Iterable

from app.bootstrap import CoinbaseApplicationConfig
from app.ledger_view import load_verified_ledger_view
from config.assembly import effective_risk_policy_config, effective_strategy_market_data_requirements
from core.clock import Clock
from core.json_tools import JsonValue
from products.replay import product_catalog_from_projection
from risk.gate import RiskGate
from strategies import Strategy, configured_strategies, simulate_strategies


def strategy_simulation_payload(
    config: CoinbaseApplicationConfig,
    *,
    clock: Clock | None = None,
    strategies: Iterable[Strategy] = (),
) -> dict[str, JsonValue]:
    if not config.bot.strategies.strategy_ids:
        raise ValueError("strategy simulation requires bot.strategies.strategy_ids")

    view = load_verified_ledger_view(config.ledger_path)
    product_catalog = (
        product_catalog_from_projection(view.projection)
        if config.bot.product_catalog.schedule.enabled
        else None
    )
    selected_strategies = configured_strategies(
        config.bot.strategies.strategy_ids,
        static_strategies=tuple(strategies),
        strategy_parameters=config.bot.strategies.strategy_parameters,
    )
    risk_gate = RiskGate(effective_risk_policy_config(config.bot).to_policy(product_catalog=product_catalog))
    return simulate_strategies(
        clock=clock,
        execution_mode=config.bot.rest.execution_mode,
        ledger_last_hash=view.state.last_hash,
        ledger_path=view.ledger_path,
        ledger_record_count=len(view.records),
        market_data_requirements=effective_strategy_market_data_requirements(config.bot),
        operator_policy=config.bot.strategies.operator_policy,
        product_catalog=product_catalog,
        projection=view.projection,
        risk_gate=risk_gate,
        strategies=selected_strategies,
    ).to_payload()
