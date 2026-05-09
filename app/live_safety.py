from __future__ import annotations

import os
from collections.abc import Mapping

from app.bootstrap import CoinbaseApplicationConfig
from app.config_fingerprint import application_config_snapshot
from config.assembly import effective_risk_policy_config, effective_strategy_allow_live_execution
from core.enums import ReadinessRequirement, RiskControl
from core.errors import ConfigError
from core.json_tools import JsonValue


LIVE_TRADING_APPROVAL_ENV = "STATERAIL_ALLOW_LIVE_TRADING"
CONFIG_PLACEHOLDER_PREFIX = "REPLACE_WITH_"
FALSE_VALUES = frozenset({"0", "false", "no", "off"})
TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


def live_trading_approved_from_env(env: Mapping[str, str] | None = None) -> bool:
    environment = os.environ if env is None else env
    value = environment.get(LIVE_TRADING_APPROVAL_ENV)
    if value is None:
        return False
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ConfigError(f"{LIVE_TRADING_APPROVAL_ENV} must be a boolean value")


def enforce_live_trading_approval(
    config: CoinbaseApplicationConfig,
    *,
    approved: bool,
) -> None:
    if config.bot.live_rest_execution_enabled() and not approved:
        raise ConfigError(
            f"{LIVE_TRADING_APPROVAL_ENV}=true is required for live REST execution"
        )


def enforce_live_runtime_safety(
    config: CoinbaseApplicationConfig,
    *,
    approved: bool,
) -> None:
    missing_requirements = live_runtime_missing_requirements(config, approved=approved)
    if not missing_requirements:
        return
    if ReadinessRequirement.LIVE_TRADING_APPROVAL in missing_requirements:
        raise ConfigError(
            f"{LIVE_TRADING_APPROVAL_ENV}=true is required for live REST execution"
        )
    requirements = ", ".join(requirement.value for requirement in missing_requirements)
    raise ConfigError(f"live REST execution requires: {requirements}")


def live_runtime_missing_requirements(
    config: CoinbaseApplicationConfig,
    *,
    approved: bool,
) -> tuple[ReadinessRequirement, ...]:
    if not config.bot.live_rest_execution_enabled():
        return ()

    missing_requirements: list[ReadinessRequirement] = []
    if not approved:
        missing_requirements.append(ReadinessRequirement.LIVE_TRADING_APPROVAL)
    if not configured_risk_controls(config):
        missing_requirements.append(ReadinessRequirement.RISK_POLICY)
    if not config.bot.product_catalog.schedule.enabled:
        missing_requirements.append(ReadinessRequirement.PRODUCT_CATALOG)
    if config.bot.strategies.schedule.enabled and not effective_strategy_allow_live_execution(config.bot):
        missing_requirements.append(ReadinessRequirement.STRATEGY_LIVE_APPROVAL)
    if config_placeholder_paths(config):
        missing_requirements.append(ReadinessRequirement.CONFIG_PLACEHOLDERS)
    return tuple(missing_requirements)


def configured_risk_controls(config: CoinbaseApplicationConfig) -> tuple[RiskControl, ...]:
    risk = effective_risk_policy_config(config.bot)
    controls: list[RiskControl] = []
    if risk.allowed_lineage_relations:
        controls.append(RiskControl.ALLOWED_LINEAGE_RELATIONS)
    if risk.allowed_products:
        controls.append(RiskControl.ALLOWED_PRODUCTS)
    if risk.allowed_order_types:
        controls.append(RiskControl.ALLOWED_ORDER_TYPES)
    if risk.allowed_placement_kinds:
        controls.append(RiskControl.ALLOWED_PLACEMENT_KINDS)
    if risk.allowed_sides:
        controls.append(RiskControl.ALLOWED_SIDES)
    if risk.allowed_time_in_force:
        controls.append(RiskControl.ALLOWED_TIME_IN_FORCE)
    if risk.max_order_size is not None:
        controls.append(RiskControl.MAX_ORDER_SIZE)
    if risk.max_order_notional is not None:
        controls.append(RiskControl.MAX_ORDER_NOTIONAL)
    if risk.max_daily_notional is not None:
        controls.append(RiskControl.MAX_DAILY_NOTIONAL)
    if risk.max_open_orders is not None:
        controls.append(RiskControl.MAX_OPEN_ORDERS)
    if risk.max_leverage is not None:
        controls.append(RiskControl.MAX_LEVERAGE)
    if risk.max_visible_notional is not None:
        controls.append(RiskControl.MAX_VISIBLE_NOTIONAL)
    if risk.max_order_replacements is not None:
        controls.append(RiskControl.MAX_ORDER_REPLACEMENTS)
    if risk.require_post_only:
        controls.append(RiskControl.REQUIRE_POST_ONLY)
    if risk.require_reduce_only:
        controls.append(RiskControl.REQUIRE_REDUCE_ONLY)
    if risk.require_staged_release_above_visible_limit:
        controls.append(RiskControl.REQUIRE_STAGED_RELEASE_ABOVE_VISIBLE_LIMIT)
    if risk.kill_switch_enabled:
        controls.append(RiskControl.KILL_SWITCH_ENABLED)
    return tuple(controls)


def config_placeholder_paths(config: CoinbaseApplicationConfig) -> tuple[str, ...]:
    return tuple(_placeholder_paths(application_config_snapshot(config), path="$"))


def _placeholder_paths(value: JsonValue, *, path: str) -> tuple[str, ...]:
    if isinstance(value, str):
        return (path,) if value.startswith(CONFIG_PLACEHOLDER_PREFIX) else ()
    if isinstance(value, list):
        paths: list[str] = []
        for index, item in enumerate(value):
            paths.extend(_placeholder_paths(item, path=f"{path}[{index}]"))
        return tuple(paths)
    if isinstance(value, dict):
        paths: list[str] = []
        for key, item in sorted(value.items()):
            paths.extend(_placeholder_paths(item, path=f"{path}.{key}"))
        return tuple(paths)
    return ()
