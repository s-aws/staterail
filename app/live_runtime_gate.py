from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.bootstrap import CoinbaseApplicationConfig
from app.ledger_health_acknowledgement import (
    ledger_health_acknowledgement_status,
    ledger_health_attention_check_summaries,
)
from app.ledger_health import ledger_health_payload
from app.live_preflight_gate import live_no_order_preflight_gate_payload
from app.live_safety import (
    LIVE_TRADING_APPROVAL_ENV,
    configured_risk_controls,
    live_runtime_missing_requirements,
)
from app.strategy_simulation_gate import strategy_simulation_gate_payload
from core.enums import (
    LedgerHealthStatus,
    ReadinessCheckName,
    ReadinessRequirement,
    ReadinessStatus,
)
from core.errors import ConfigError
from core.json_tools import JsonValue, normalize_json


def live_runtime_gate_payload(
    config: CoinbaseApplicationConfig,
    *,
    approved: bool,
    preflight_max_age: timedelta | None = None,
    strategy_simulation_max_age: timedelta | None = None,
    now: datetime | None = None,
) -> dict[str, JsonValue]:
    if preflight_max_age is not None and preflight_max_age <= timedelta(0):
        raise ValueError("preflight_max_age must be positive")
    if strategy_simulation_max_age is not None and strategy_simulation_max_age <= timedelta(0):
        raise ValueError("strategy_simulation_max_age must be positive")

    checked_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    checks = [
        _live_runtime_safety_check(config, approved=approved),
        _ledger_health_check(config),
        _live_no_order_preflight_check(
            config,
            max_age=preflight_max_age,
            now=checked_at,
        ),
        _strategy_simulation_check(
            config,
            max_age=strategy_simulation_max_age,
            now=checked_at,
        ),
    ]
    status = (
        ReadinessStatus.ATTENTION_REQUIRED
        if any(check["status"] != ReadinessStatus.OK.value for check in checks)
        else ReadinessStatus.OK
    )
    payload = {
        "checked_at": checked_at.isoformat(),
        "checks": checks,
        "ledger_path": config.ledger_path.as_posix(),
        "live_rest_execution": config.bot.live_rest_execution_enabled(),
        "runtime_would_start": status == ReadinessStatus.OK,
        "status": status,
        "strategy_schedule_enabled": config.bot.strategies.schedule.enabled,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("live runtime gate payload must normalize to an object")
    return normalized


def enforce_live_runtime_gate(
    config: CoinbaseApplicationConfig,
    *,
    approved: bool,
    preflight_max_age: timedelta | None = None,
    strategy_simulation_max_age: timedelta | None = None,
) -> None:
    if not config.bot.live_rest_execution_enabled():
        return

    payload = live_runtime_gate_payload(
        config,
        approved=approved,
        preflight_max_age=preflight_max_age,
        strategy_simulation_max_age=strategy_simulation_max_age,
    )
    if payload.get("status") == ReadinessStatus.OK.value:
        return
    raise ConfigError(_gate_error_message(payload), context=payload)


def _live_runtime_safety_check(
    config: CoinbaseApplicationConfig,
    *,
    approved: bool,
) -> dict[str, JsonValue]:
    missing_requirements = live_runtime_missing_requirements(config, approved=approved)
    return _check_payload(
        ReadinessCheckName.LIVE_RUNTIME_SAFETY,
        count=len(missing_requirements),
        details={
            "configured_risk_controls": [
                control.value for control in configured_risk_controls(config)
            ],
            "live_trading_approved": approved,
            "missing_requirements": [
                requirement.value for requirement in missing_requirements
            ],
            "required": config.bot.live_rest_execution_enabled(),
            "strategy_allow_live_execution": config.bot.strategies.allow_live_execution,
            "strategy_schedule_enabled": config.bot.strategies.schedule.enabled,
        },
    )


def _ledger_health_check(config: CoinbaseApplicationConfig) -> dict[str, JsonValue]:
    if not config.bot.live_rest_execution_enabled():
        return _check_payload(
            ReadinessCheckName.LEDGER_HEALTH,
            count=0,
            details={"required": False},
        )

    try:
        health = ledger_health_payload(config.ledger_path)
    except Exception as exc:
        return _check_payload(
            ReadinessCheckName.LEDGER_HEALTH,
            count=1,
            details={
                "exception_type": type(exc).__name__,
                "health_status": LedgerHealthStatus.ATTENTION_REQUIRED.value,
                "ledger_path": config.ledger_path.as_posix(),
                "message": str(exc),
                "required": True,
                "verified": False,
            },
        )

    attention_checks = ledger_health_attention_check_summaries(health)
    health_status = _string_or_none(health.get("status"))
    acknowledgement = ledger_health_acknowledgement_status(
        config.ledger_path,
        health_payload=health,
    )
    acknowledged = acknowledgement.get("acknowledged") is True
    count = (
        0
        if health_status == LedgerHealthStatus.OK.value or acknowledged
        else max(1, len(attention_checks))
    )
    return _check_payload(
        ReadinessCheckName.LEDGER_HEALTH,
        count=count,
        details={
            "acknowledgement": acknowledgement,
            "attention_check_count": len(attention_checks),
            "attention_checks": attention_checks,
            "attention_requires_acknowledgement": (
                health_status != LedgerHealthStatus.OK.value and not acknowledged
            ),
            "health_status": health_status,
            "last_sequence": _int_or_none(health.get("last_sequence")),
            "ledger_path": config.ledger_path.as_posix(),
            "record_count": _int_or_none(health.get("record_count")),
            "required": True,
            "verified": health.get("verified") is True,
        },
    )


def _live_no_order_preflight_check(
    config: CoinbaseApplicationConfig,
    *,
    max_age: timedelta | None,
    now: datetime,
) -> dict[str, JsonValue]:
    if not config.bot.live_rest_execution_enabled():
        return _check_payload(
            ReadinessCheckName.LIVE_NO_ORDER_PREFLIGHT,
            count=0,
            details={"required": False},
        )
    gate = live_no_order_preflight_gate_payload(config, max_age=max_age, now=now)
    attention_reasons = _string_list(gate.get("attention_reasons"))
    return _check_payload(
        ReadinessCheckName.LIVE_NO_ORDER_PREFLIGHT,
        count=len(attention_reasons),
        details=gate,
    )


def _strategy_simulation_check(
    config: CoinbaseApplicationConfig,
    *,
    max_age: timedelta | None,
    now: datetime,
) -> dict[str, JsonValue]:
    if not config.bot.live_rest_execution_enabled() or not config.bot.strategies.schedule.enabled:
        return _check_payload(
            ReadinessCheckName.STRATEGY_SIMULATION,
            count=0,
            details={"required": False},
        )
    gate = strategy_simulation_gate_payload(config, max_age=max_age, now=now)
    attention_reasons = _string_list(gate.get("attention_reasons"))
    return _check_payload(
        ReadinessCheckName.STRATEGY_SIMULATION,
        count=len(attention_reasons),
        details=gate,
    )


def _check_payload(
    name: ReadinessCheckName,
    *,
    count: int,
    details: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    payload = {
        "count": count,
        "details": details,
        "name": name,
        "status": ReadinessStatus.ATTENTION_REQUIRED if count else ReadinessStatus.OK,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("live runtime gate check payload must normalize to an object")
    return normalized


def _gate_error_message(payload: dict[str, JsonValue]) -> str:
    checks = _checks_by_name(payload.get("checks"))
    safety = checks.get(ReadinessCheckName.LIVE_RUNTIME_SAFETY.value)
    if safety is not None and safety.get("status") != ReadinessStatus.OK.value:
        missing_requirements = _string_list(
            _details(safety).get("missing_requirements")
        )
        if ReadinessRequirement.LIVE_TRADING_APPROVAL.value in missing_requirements:
            return f"{LIVE_TRADING_APPROVAL_ENV}=true is required for live REST execution"
        requirements = ", ".join(missing_requirements)
        return f"live REST execution requires: {requirements}"

    preflight = checks.get(ReadinessCheckName.LIVE_NO_ORDER_PREFLIGHT.value)
    if preflight is not None and preflight.get("status") != ReadinessStatus.OK.value:
        return (
            "live REST execution requires a clean "
            f"{ReadinessRequirement.LIVE_NO_ORDER_PREFLIGHT.value} result for the current config fingerprint"
        )

    simulation = checks.get(ReadinessCheckName.STRATEGY_SIMULATION.value)
    if simulation is not None and simulation.get("status") != ReadinessStatus.OK.value:
        return (
            "live strategy execution requires a clean "
            f"{ReadinessRequirement.STRATEGY_SIMULATION.value} result for the current config fingerprint"
        )

    ledger = checks.get(ReadinessCheckName.LEDGER_HEALTH.value)
    if ledger is not None and ledger.get("status") != ReadinessStatus.OK.value:
        return (
            "live REST execution requires "
            f"{ReadinessCheckName.LEDGER_HEALTH.value}=ok or a matching operator acknowledgement before runtime startup"
        )

    return "live REST execution requires all runtime admission checks to pass"


def _checks_by_name(value: JsonValue) -> dict[str, dict[str, JsonValue]]:
    if not isinstance(value, list):
        return {}
    checks: dict[str, dict[str, JsonValue]] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        name = _string_or_none(item.get("name"))
        if name is not None:
            checks[name] = item
    return checks


def _details(check: dict[str, JsonValue]) -> dict[str, JsonValue]:
    details = check.get("details")
    return details if isinstance(details, dict) else {}


def _int_or_none(value: JsonValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _string_list(value: JsonValue) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _string_or_none(value: JsonValue) -> str | None:
    return value if isinstance(value, str) and value else None
