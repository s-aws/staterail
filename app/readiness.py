from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from app.config_fingerprint import (
    APPLICATION_CONFIG_SCHEMA_VERSION,
    CONFIG_FINGERPRINT_ALGORITHM,
    application_config_fingerprint,
)
from app.ledger_config import latest_ledger_application_config
from app.live_safety import config_placeholder_paths, configured_risk_controls
from audit.ledger import AuditLedger
from config.assembly import (
    effective_risk_policy_config,
    effective_strategy_allow_live_execution,
    effective_strategy_market_data_requirements,
)
from core.enums import (
    OperatorPolicyPermission,
    OrderType,
    ReadinessCheckName,
    ReadinessCheckSkipReason,
    ReadinessRequirement,
    ReadinessStatus,
)
from core.json_tools import JsonValue, normalize_json
from projections.state import SourceOfTruthProjection
from strategies import (
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
    available_entry_point_strategy_ids,
    available_strategies,
    validate_strategy_parameters,
)

if TYPE_CHECKING:
    from app.bootstrap import CoinbaseApplicationConfig


READINESS_REPORT_SCHEMA_VERSION = 1
MIN_REDUNDANT_WEBSOCKET_SOURCES_PER_SCOPE = 2


@dataclass(frozen=True)
class ReadinessCheckResult:
    name: ReadinessCheckName
    status: ReadinessStatus
    count: int = 0
    details: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, ReadinessCheckName):
            raise TypeError("name must be a ReadinessCheckName")
        if not isinstance(self.status, ReadinessStatus):
            raise TypeError("status must be a ReadinessStatus")
        if self.count < 0:
            raise ValueError("count must not be negative")

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "count": self.count,
            "details": self.details,
            "name": self.name,
            "status": self.status,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Readiness check payload must normalize to an object")
        return normalized


@dataclass(frozen=True)
class ReadinessReport:
    checks: tuple[ReadinessCheckResult, ...]
    config_fingerprint: str
    ledger_path: Path
    status: ReadinessStatus
    schema_version: int = READINESS_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.status, ReadinessStatus):
            raise TypeError("status must be a ReadinessStatus")
        for check in self.checks:
            if not isinstance(check, ReadinessCheckResult):
                raise TypeError("checks must contain ReadinessCheckResult values")

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "checks": [check.to_payload() for check in self.checks],
            "config_fingerprint": self.config_fingerprint,
            "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
            "ledger_path": self.ledger_path.as_posix(),
            "read_only": True,
            "schema_version": self.schema_version,
            "status": self.status,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Readiness report payload must normalize to an object")
        return normalized


def readiness_report(
    config: CoinbaseApplicationConfig,
    *,
    allow_config_fingerprint_mismatch: bool = False,
    audit_anchor_store_configured: bool | None = None,
    available_strategy_ids: Iterable[str] = (),
    extra_checks: Iterable[ReadinessCheckResult] = (),
    jwt_factory_configured: bool = False,
    live_trading_approved: bool = False,
    token_provider_configured: bool = False,
) -> ReadinessReport:
    resolved_strategy_ids = _available_strategy_ids(available_strategy_ids)
    checks = (
        _config_fingerprint_check(
            config,
            allow_config_fingerprint_mismatch=allow_config_fingerprint_mismatch,
        ),
        _config_placeholders_check(config),
        _ledger_path_check(config),
        _credentials_check(
            config,
            jwt_factory_configured=jwt_factory_configured,
            token_provider_configured=token_provider_configured,
        ),
        _live_trading_approval_check(config, live_trading_approved=live_trading_approved),
        _risk_policy_check(config),
        _runtime_tasks_check(
            config,
            audit_anchor_store_configured=audit_anchor_store_configured,
            available_strategy_ids=resolved_strategy_ids,
        ),
        _websocket_sources_check(config),
        *tuple(extra_checks),
    )
    status = (
        ReadinessStatus.ATTENTION_REQUIRED
        if any(check.status != ReadinessStatus.OK for check in checks)
        else ReadinessStatus.OK
    )
    return ReadinessReport(
        checks=checks,
        config_fingerprint=application_config_fingerprint(config),
        ledger_path=config.ledger_path,
        status=status,
    )


def readiness_payload(
    config: CoinbaseApplicationConfig,
    *,
    allow_config_fingerprint_mismatch: bool = False,
    audit_anchor_store_configured: bool | None = None,
    available_strategy_ids: Iterable[str] = (),
    extra_checks: Iterable[ReadinessCheckResult] = (),
    jwt_factory_configured: bool = False,
    live_trading_approved: bool = False,
    token_provider_configured: bool = False,
) -> dict[str, JsonValue]:
    return readiness_report(
        config,
        allow_config_fingerprint_mismatch=allow_config_fingerprint_mismatch,
        audit_anchor_store_configured=audit_anchor_store_configured,
        available_strategy_ids=available_strategy_ids,
        extra_checks=extra_checks,
        jwt_factory_configured=jwt_factory_configured,
        live_trading_approved=live_trading_approved,
        token_provider_configured=token_provider_configured,
    ).to_payload()


def _config_fingerprint_check(
    config: CoinbaseApplicationConfig,
    *,
    allow_config_fingerprint_mismatch: bool,
) -> ReadinessCheckResult:
    fingerprint = application_config_fingerprint(config)
    ledger_details, ledger_count = _ledger_config_fingerprint_details(
        config,
        allow_config_fingerprint_mismatch=allow_config_fingerprint_mismatch,
        current_fingerprint=fingerprint,
    )
    return _check(
        ReadinessCheckName.CONFIG_FINGERPRINT,
        count=ledger_count,
        details={
            "application_config_schema_version": APPLICATION_CONFIG_SCHEMA_VERSION,
            "fingerprint": fingerprint,
            "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
            **ledger_details,
        },
    )


def _ledger_config_fingerprint_details(
    config: CoinbaseApplicationConfig,
    *,
    allow_config_fingerprint_mismatch: bool,
    current_fingerprint: str,
) -> tuple[dict[str, JsonValue], int]:
    ledger_path = config.ledger_path
    lock_path = ledger_path.with_name(f"{ledger_path.name}.lock")
    ledger_exists = ledger_path.exists()
    details: dict[str, JsonValue] = {
        "latest_ledger_config_fingerprint": None,
        "latest_ledger_config_fingerprint_algorithm": None,
        "latest_ledger_config_schema_version": None,
        "latest_ledger_start_sequence": None,
        "ledger_check_error": None,
        "ledger_check_skipped_reason": None,
        "ledger_checked": False,
        "ledger_config_fingerprint_mismatch_allowed": False,
        "ledger_config_fingerprint_matches": None,
        "ledger_config_fingerprint_missing": False,
        "ledger_exists": ledger_exists,
        "ledger_record_count": None,
    }
    if not ledger_exists:
        return details, 0
    if ledger_path.is_dir():
        details["ledger_check_skipped_reason"] = ReadinessCheckSkipReason.LEDGER_PATH_IS_DIRECTORY.value
        return details, 0
    if lock_path.exists():
        details["ledger_check_skipped_reason"] = ReadinessCheckSkipReason.LEDGER_LOCKED.value
        return details, 0

    try:
        ledger = AuditLedger(ledger_path)
        records = ledger.iter_records()
        projection = SourceOfTruthProjection.from_records(records)
    except Exception as exc:
        details["ledger_check_error"] = f"{type(exc).__name__}: {exc}"
        return details, 1

    latest_config = latest_ledger_application_config(projection)
    details.update(
        {
            "latest_ledger_config_fingerprint": latest_config.fingerprint,
            "latest_ledger_config_fingerprint_algorithm": latest_config.fingerprint_algorithm,
            "latest_ledger_config_schema_version": latest_config.schema_version,
            "latest_ledger_start_sequence": latest_config.startup_sequence,
            "ledger_checked": True,
            "ledger_record_count": len(records),
        }
    )

    if latest_config.startup_sequence is None:
        return details, 0

    if latest_config.fingerprint is None:
        details["ledger_config_fingerprint_missing"] = True
        return details, 1

    matches = (
        latest_config.fingerprint == current_fingerprint
        and latest_config.fingerprint_algorithm == CONFIG_FINGERPRINT_ALGORITHM
    )
    details["ledger_config_fingerprint_matches"] = matches
    if (
        not matches
        and allow_config_fingerprint_mismatch
        and latest_config.fingerprint_algorithm == CONFIG_FINGERPRINT_ALGORITHM
    ):
        details["ledger_config_fingerprint_mismatch_allowed"] = True
        return details, 0
    return details, int(not matches)


def _ledger_path_check(config: CoinbaseApplicationConfig) -> ReadinessCheckResult:
    ledger_path = config.ledger_path
    parent = ledger_path.parent
    lock_path = ledger_path.with_name(f"{ledger_path.name}.lock")
    nearest_parent = _nearest_existing_parent(parent)
    ledger_exists = ledger_path.exists()
    ledger_is_directory = ledger_path.is_dir() if ledger_exists else False
    lock_exists = lock_path.exists()
    parent_exists = parent.exists()
    parent_is_directory = parent.is_dir() if parent_exists else False
    nearest_parent_is_directory = nearest_parent.is_dir()
    nearest_parent_writable = os.access(nearest_parent, os.W_OK) if nearest_parent_is_directory else False
    parent_ready = (
        parent_exists
        and parent_is_directory
        and os.access(parent, os.W_OK)
    ) or (
        not parent_exists
        and nearest_parent_is_directory
        and nearest_parent_writable
    )
    count = int(ledger_is_directory or lock_exists or not parent_ready)
    return _check(
        ReadinessCheckName.LEDGER_PATH,
        count=count,
        details={
            "ledger_exists": ledger_exists,
            "ledger_is_directory": ledger_is_directory,
            "lock_exists": lock_exists,
            "lock_path": lock_path.as_posix(),
            "nearest_existing_parent": nearest_parent.as_posix(),
            "nearest_existing_parent_writable": nearest_parent_writable,
            "parent_can_be_created": not parent_exists and nearest_parent_is_directory and nearest_parent_writable,
            "parent_exists": parent_exists,
            "parent_path": parent.as_posix(),
        },
    )


def _config_placeholders_check(config: CoinbaseApplicationConfig) -> ReadinessCheckResult:
    placeholder_paths = config_placeholder_paths(config)
    return _check(
        ReadinessCheckName.CONFIG_PLACEHOLDERS,
        count=len(placeholder_paths),
        details={
            "live_rest_execution_enabled": config.bot.live_rest_execution_enabled(),
            "placeholder_count": len(placeholder_paths),
            "placeholder_prefix": "REPLACE_WITH_",
            "placeholder_paths": list(placeholder_paths),
        },
    )


def _credentials_check(
    config: CoinbaseApplicationConfig,
    *,
    jwt_factory_configured: bool,
    token_provider_configured: bool,
) -> ReadinessCheckResult:
    missing_requirements: list[ReadinessRequirement] = []
    token_provider_required = config.bot.token_provider_required()
    jwt_factory_required = config.bot.jwt_factory_required()
    if token_provider_required and not token_provider_configured:
        missing_requirements.append(ReadinessRequirement.TOKEN_PROVIDER)
    if jwt_factory_required and not jwt_factory_configured:
        missing_requirements.append(ReadinessRequirement.JWT_FACTORY)

    rest_backed_schedules = config.bot.rest_backed_schedules()
    return _check(
        ReadinessCheckName.CREDENTIALS,
        count=len(missing_requirements),
        details={
            "jwt_factory_configured": jwt_factory_configured,
            "jwt_factory_required": jwt_factory_required,
            "live_rest_execution_enabled": config.bot.live_rest_execution_enabled(),
            "missing_requirements": [requirement.value for requirement in missing_requirements],
            "rest_backed_task_ids": [schedule.task_id.value for schedule in rest_backed_schedules],
            "token_provider_configured": token_provider_configured,
            "token_provider_required": token_provider_required,
        },
    )


def _live_trading_approval_check(
    config: CoinbaseApplicationConfig,
    *,
    live_trading_approved: bool,
) -> ReadinessCheckResult:
    live_rest_execution_enabled = config.bot.live_rest_execution_enabled()
    missing_approval = live_rest_execution_enabled and not live_trading_approved
    return _check(
        ReadinessCheckName.LIVE_TRADING_APPROVAL,
        count=int(missing_approval),
        details={
            "live_rest_execution_enabled": live_rest_execution_enabled,
            "live_trading_approved": live_trading_approved,
            "missing_requirements": (
                [ReadinessRequirement.LIVE_TRADING_APPROVAL.value]
                if missing_approval
                else []
            ),
        },
    )


def _risk_policy_check(config: CoinbaseApplicationConfig) -> ReadinessCheckResult:
    risk = effective_risk_policy_config(config.bot)
    configured_controls = configured_risk_controls(config)

    live_rest_execution_enabled = config.bot.live_rest_execution_enabled()
    product_catalog_refresh_enabled = config.bot.product_catalog.schedule.enabled
    unguarded_live_execution = live_rest_execution_enabled and not configured_controls
    live_execution_without_product_catalog = live_rest_execution_enabled and not product_catalog_refresh_enabled
    missing_requirements: list[ReadinessRequirement] = []
    if unguarded_live_execution:
        missing_requirements.append(ReadinessRequirement.RISK_POLICY)
    if live_execution_without_product_catalog:
        missing_requirements.append(ReadinessRequirement.PRODUCT_CATALOG)
    return _check(
        ReadinessCheckName.RISK_POLICY,
        count=len(missing_requirements),
        details={
            "allowed_order_type_count": len(risk.allowed_order_types),
            "allowed_product_count": len(risk.allowed_products),
            "allowed_side_count": len(risk.allowed_sides),
            "allowed_time_in_force_count": len(risk.allowed_time_in_force),
            "configured_controls": [control.value for control in configured_controls],
            "kill_switch_enabled": risk.kill_switch_enabled,
            "live_execution_without_product_catalog": live_execution_without_product_catalog,
            "live_rest_execution_enabled": live_rest_execution_enabled,
            "max_daily_notional_configured": risk.max_daily_notional is not None,
            "max_leverage_configured": risk.max_leverage is not None,
            "max_open_orders_configured": risk.max_open_orders is not None,
            "max_order_notional_configured": risk.max_order_notional is not None,
            "max_order_replacements_configured": risk.max_order_replacements is not None,
            "max_order_size_configured": risk.max_order_size is not None,
            "max_visible_notional_configured": risk.max_visible_notional is not None,
            "missing_requirements": [requirement.value for requirement in missing_requirements],
            "product_catalog_product_ids": list(config.bot.product_catalog.product_ids),
            "product_catalog_refresh_enabled": product_catalog_refresh_enabled,
            "require_post_only": risk.require_post_only,
            "require_reduce_only": risk.require_reduce_only,
            "require_staged_release_above_visible_limit": (
                risk.require_staged_release_above_visible_limit
            ),
            "unguarded_live_execution": unguarded_live_execution,
        },
    )


def _runtime_tasks_check(
    config: CoinbaseApplicationConfig,
    *,
    audit_anchor_store_configured: bool | None,
    available_strategy_ids: tuple[str, ...],
) -> ReadinessCheckResult:
    schedules = config.bot.enabled_schedules()
    effective_strategy_live_allowed = effective_strategy_allow_live_execution(config.bot)
    effective_market_data_requirements = effective_strategy_market_data_requirements(config.bot)
    anchor_store_configured = (
        config.bot.audit_anchor_store is not None
        if audit_anchor_store_configured is None
        else audit_anchor_store_configured
    )
    missing_requirements: list[ReadinessRequirement] = []
    if config.bot.audit_anchor_schedule.enabled and not anchor_store_configured:
        missing_requirements.append(ReadinessRequirement.AUDIT_ANCHOR_STORE)
    archive_store_configured = config.bot.audit_archive_store is not None
    if config.bot.audit_archive_schedule.enabled and not archive_store_configured:
        missing_requirements.append(ReadinessRequirement.AUDIT_ARCHIVE_STORE)
    feed_health_required = bool(config.bot.websocket_sources)
    if feed_health_required and not config.bot.feed_health_schedule.enabled:
        missing_requirements.append(ReadinessRequirement.FEED_HEALTH_TASK)
    if config.bot.strategies.schedule.enabled and not config.bot.strategies.strategy_ids:
        missing_requirements.append(ReadinessRequirement.STRATEGY_IDS)
    unresolved_strategy_ids = tuple(
        strategy_id
        for strategy_id in config.bot.strategies.strategy_ids
        if strategy_id not in available_strategy_ids
    )
    strategy_parameter_error = _strategy_parameter_error(config)
    if config.bot.strategies.schedule.enabled and unresolved_strategy_ids:
        missing_requirements.append(ReadinessRequirement.STRATEGY_RESOLUTION)
    if config.bot.strategies.schedule.enabled and strategy_parameter_error is not None:
        missing_requirements.append(ReadinessRequirement.STRATEGY_PARAMETERS)
    if (
        config.bot.live_rest_execution_enabled()
        and config.bot.strategies.schedule.enabled
        and not effective_strategy_live_allowed
    ):
        missing_requirements.append(ReadinessRequirement.STRATEGY_LIVE_APPROVAL)
    missing_requirements.extend(_anchor_repricing_manager_requirements(config))
    missing_requirements.extend(_staged_release_manager_requirements(config))
    missing_requirements.extend(_followup_on_fill_manager_requirements(config))
    missing_requirements.extend(_consolidation_manager_requirements(config))
    missing_requirements.extend(_passive_market_making_requirements(config))
    missing_requirements = _unique_requirements(missing_requirements)
    anchor_repricing_manager_selected = _anchor_repricing_manager_selected(config)
    consolidation_manager_selected = _consolidation_manager_selected(config)
    passive_market_making_selected = _passive_market_making_selected(config)
    staged_release_manager_selected = _staged_release_manager_selected(config)
    followup_on_fill_manager_selected = _followup_on_fill_manager_selected(config)
    operator_policy = config.bot.strategies.operator_policy
    return _check(
        ReadinessCheckName.RUNTIME_TASKS,
        count=(0 if schedules else 1) + len(missing_requirements),
        details={
            "audit_anchor_store_configured": anchor_store_configured,
            "audit_anchor_store_required": config.bot.audit_anchor_schedule.enabled,
            "audit_archive_store_configured": archive_store_configured,
            "audit_archive_store_required": config.bot.audit_archive_schedule.enabled,
            "anchor_repricing_manager_anchor_enabled": (
                operator_policy.anchor_repricing is not None
                and operator_policy.anchor_repricing.enabled
                if operator_policy is not None
                else None
            ),
            "anchor_repricing_manager_cancel_replace_enabled": (
                operator_policy.moves.cancel_replace_when_amend_not_supported
                if operator_policy is not None
                else None
            ),
            "anchor_repricing_manager_move_enabled": (
                operator_policy.lineage.move_same_side_orders == OperatorPolicyPermission.ALLOWED
                if operator_policy is not None
                else None
            ),
            "anchor_repricing_manager_operator_policy_configured": operator_policy is not None,
            "anchor_repricing_manager_product_catalog_enabled": (
                config.bot.product_catalog.schedule.enabled
            ),
            "anchor_repricing_manager_selected": anchor_repricing_manager_selected,
            "consolidation_manager_merge_enabled": (
                operator_policy.lineage.merge_orders == OperatorPolicyPermission.ALLOWED
                if operator_policy is not None
                else None
            ),
            "consolidation_manager_operator_policy_configured": operator_policy is not None,
            "consolidation_manager_product_catalog_enabled": (
                config.bot.product_catalog.schedule.enabled
            ),
            "consolidation_manager_selected": consolidation_manager_selected,
            "enabled_count": len(schedules),
            "enabled_task_ids": [schedule.task_id.value for schedule in schedules],
            "feed_health_enabled": config.bot.feed_health_schedule.enabled,
            "feed_health_required": feed_health_required,
            "followup_on_fill_manager_followup_enabled": (
                operator_policy.partial_fills.followup_enabled
                if operator_policy is not None
                else None
            ),
            "followup_on_fill_manager_operator_policy_configured": operator_policy is not None,
            "followup_on_fill_manager_product_catalog_enabled": (
                config.bot.product_catalog.schedule.enabled
            ),
            "followup_on_fill_manager_selected": followup_on_fill_manager_selected,
            "missing_requirements": [requirement.value for requirement in missing_requirements],
            "passive_market_making_order_book_required": (
                operator_policy.market_data_requirements.require_order_book
                if operator_policy is not None
                else None
            ),
            "passive_market_making_order_type_limit": (
                operator_policy.order_behavior.default_order_type == OrderType.LIMIT
                if operator_policy is not None
                else None
            ),
            "passive_market_making_operator_policy_configured": operator_policy is not None,
            "passive_market_making_post_only": (
                operator_policy.order_behavior.post_only
                if operator_policy is not None
                else None
            ),
            "passive_market_making_product_catalog_enabled": (
                config.bot.product_catalog.schedule.enabled
            ),
            "passive_market_making_selected": passive_market_making_selected,
            "passive_market_making_staged_release_enabled": (
                operator_policy.staged_or_hidden_release.enabled
                if operator_policy is not None
                else None
            ),
            "product_catalog_product_ids": list(config.bot.product_catalog.product_ids),
            "product_catalog_refresh_enabled": config.bot.product_catalog.schedule.enabled,
            "strategy_allow_live_execution": effective_strategy_live_allowed,
            "strategy_available_ids": list(available_strategy_ids),
            "strategy_count": len(config.bot.strategies.strategy_ids),
            "strategy_ids": list(config.bot.strategies.strategy_ids),
            "strategy_market_data_requirement_count": len(
                effective_market_data_requirements
            ),
            "strategy_market_data_requirements": [
                requirement.to_payload()
                for requirement in effective_market_data_requirements
            ],
            "strategy_parameter_error": strategy_parameter_error,
            "strategy_parameter_ids": list(config.bot.strategies.strategy_parameters),
            "strategy_schedule_enabled": config.bot.strategies.schedule.enabled,
            "strategy_unresolved_ids": list(unresolved_strategy_ids),
            "staged_release_manager_operator_policy_configured": operator_policy is not None,
            "staged_release_manager_release_enabled": (
                operator_policy.staged_or_hidden_release.enabled
                and operator_policy.staged_or_hidden_release.allow_release
                if operator_policy is not None
                else None
            ),
            "staged_release_manager_release_conditions_match_enabled": (
                operator_policy.staged_or_hidden_release.release_only_when_conditions_match
                if operator_policy is not None
                else None
            ),
            "staged_release_manager_order_book_required": (
                operator_policy.market_data_requirements.require_order_book
                if operator_policy is not None
                else None
            ),
            "staged_release_manager_selected": staged_release_manager_selected,
            "trigger_polling_enabled": config.bot.trigger_polling_schedule.enabled,
        },
    )


def _available_strategy_ids(additional_strategy_ids: Iterable[str]) -> tuple[str, ...]:
    static_strategy_ids = {
        strategy.strategy_id
        for strategy in available_strategies(strategy_ids=())
    }
    return tuple(
        sorted(
            {
                *static_strategy_ids,
                *available_entry_point_strategy_ids(),
                *(
                    strategy_id
                    for strategy_id in additional_strategy_ids
                    if isinstance(strategy_id, str) and strategy_id
                ),
            }
        )
    )


def _strategy_parameter_error(config: CoinbaseApplicationConfig) -> str | None:
    try:
        validate_strategy_parameters(
            config.bot.strategies.strategy_ids,
            config.bot.strategies.strategy_parameters,
        )
    except (TypeError, ValueError) as exc:
        return str(exc)
    return None


def _anchor_repricing_manager_requirements(
    config: CoinbaseApplicationConfig,
) -> tuple[ReadinessRequirement, ...]:
    if not _anchor_repricing_manager_selected(config):
        return ()
    operator_policy = config.bot.strategies.operator_policy
    if operator_policy is None:
        return (ReadinessRequirement.OPERATOR_POLICY,)

    requirements: list[ReadinessRequirement] = []
    anchor = operator_policy.anchor_repricing
    if (
        anchor is None
        or not anchor.enabled
        or operator_policy.lineage.move_same_side_orders != OperatorPolicyPermission.ALLOWED
        or not operator_policy.moves.cancel_replace_when_amend_not_supported
        or not operator_policy.market_data_requirements.require_order_book
    ):
        requirements.append(ReadinessRequirement.ANCHOR_REPRICING_POLICY)
    if not config.bot.product_catalog.schedule.enabled:
        requirements.append(ReadinessRequirement.PRODUCT_CATALOG)
    return tuple(requirements)


def _anchor_repricing_manager_selected(config: CoinbaseApplicationConfig) -> bool:
    return (
        config.bot.strategies.schedule.enabled
        and ANCHOR_REPRICING_MANAGER_STRATEGY_ID in config.bot.strategies.strategy_ids
    )


def _staged_release_manager_requirements(
    config: CoinbaseApplicationConfig,
) -> tuple[ReadinessRequirement, ...]:
    if not _staged_release_manager_selected(config):
        return ()
    operator_policy = config.bot.strategies.operator_policy
    if operator_policy is None:
        return (ReadinessRequirement.OPERATOR_POLICY,)
    if (
        not operator_policy.staged_or_hidden_release.enabled
        or not operator_policy.staged_or_hidden_release.allow_release
        or (
            operator_policy.staged_or_hidden_release.release_only_when_conditions_match
            and not operator_policy.market_data_requirements.require_order_book
        )
    ):
        return (ReadinessRequirement.STAGED_RELEASE_POLICY,)
    return ()


def _staged_release_manager_selected(config: CoinbaseApplicationConfig) -> bool:
    return (
        config.bot.strategies.schedule.enabled
        and STAGED_RELEASE_MANAGER_STRATEGY_ID in config.bot.strategies.strategy_ids
    )


def _followup_on_fill_manager_requirements(
    config: CoinbaseApplicationConfig,
) -> tuple[ReadinessRequirement, ...]:
    if not _followup_on_fill_manager_selected(config):
        return ()
    operator_policy = config.bot.strategies.operator_policy
    if operator_policy is None:
        return (ReadinessRequirement.OPERATOR_POLICY,)

    requirements: list[ReadinessRequirement] = []
    if (
        operator_policy.lineage.followup_on_fill != OperatorPolicyPermission.ALLOWED
        or not operator_policy.partial_fills.followup_enabled
    ):
        requirements.append(ReadinessRequirement.FOLLOWUP_POLICY)
    if not config.bot.product_catalog.schedule.enabled:
        requirements.append(ReadinessRequirement.PRODUCT_CATALOG)
    return tuple(requirements)


def _followup_on_fill_manager_selected(config: CoinbaseApplicationConfig) -> bool:
    return (
        config.bot.strategies.schedule.enabled
        and FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID in config.bot.strategies.strategy_ids
    )


def _consolidation_manager_requirements(
    config: CoinbaseApplicationConfig,
) -> tuple[ReadinessRequirement, ...]:
    if not _consolidation_manager_selected(config):
        return ()
    operator_policy = config.bot.strategies.operator_policy
    if operator_policy is None:
        return (ReadinessRequirement.OPERATOR_POLICY,)

    requirements: list[ReadinessRequirement] = []
    if operator_policy.lineage.merge_orders != OperatorPolicyPermission.ALLOWED:
        requirements.append(ReadinessRequirement.CONSOLIDATION_POLICY)
    if not config.bot.product_catalog.schedule.enabled:
        requirements.append(ReadinessRequirement.PRODUCT_CATALOG)
    return tuple(requirements)


def _consolidation_manager_selected(config: CoinbaseApplicationConfig) -> bool:
    return (
        config.bot.strategies.schedule.enabled
        and CONSOLIDATION_MANAGER_STRATEGY_ID in config.bot.strategies.strategy_ids
    )


def _passive_market_making_requirements(
    config: CoinbaseApplicationConfig,
) -> tuple[ReadinessRequirement, ...]:
    if not _passive_market_making_selected(config):
        return ()
    operator_policy = config.bot.strategies.operator_policy
    if operator_policy is None:
        return (ReadinessRequirement.OPERATOR_POLICY,)

    requirements: list[ReadinessRequirement] = []
    if (
        not operator_policy.staged_or_hidden_release.enabled
        or operator_policy.order_behavior.default_order_type != OrderType.LIMIT
        or not operator_policy.order_behavior.post_only
        or not operator_policy.market_data_requirements.require_order_book
    ):
        requirements.append(ReadinessRequirement.PASSIVE_MARKET_MAKING_POLICY)
    if not config.bot.product_catalog.schedule.enabled:
        requirements.append(ReadinessRequirement.PRODUCT_CATALOG)
    return tuple(requirements)


def _passive_market_making_selected(config: CoinbaseApplicationConfig) -> bool:
    return (
        config.bot.strategies.schedule.enabled
        and PASSIVE_MARKET_MAKING_STRATEGY_ID in config.bot.strategies.strategy_ids
    )


def _unique_requirements(requirements: Iterable[ReadinessRequirement]) -> list[ReadinessRequirement]:
    return list(dict.fromkeys(requirements))


def _websocket_sources_check(config: CoinbaseApplicationConfig) -> ReadinessCheckResult:
    sources = config.bot.websocket_sources
    user_source_ids = [source.source_id for source in sources if source.is_user_source()]
    market_source_ids = [source.source_id for source in sources if not source.is_user_source()]
    single_source_scopes = _single_source_websocket_scopes(config)
    missing_live_user_channel = config.bot.live_rest_execution_enabled() and not user_source_ids
    impossible_min_live_sources = bool(sources) and len(sources) < config.bot.feed.min_live_sources
    count = len(single_source_scopes) + int(missing_live_user_channel) + int(impossible_min_live_sources)
    return _check(
        ReadinessCheckName.WEBSOCKET_SOURCES,
        count=count,
        details={
            "configured_min_live_sources": config.bot.feed.min_live_sources,
            "configured_stale_after_seconds": config.bot.feed.stale_after.total_seconds(),
            "impossible_min_live_sources": impossible_min_live_sources,
            "market_source_count": len(market_source_ids),
            "market_source_ids": market_source_ids,
            "minimum_redundant_sources_per_scope": MIN_REDUNDANT_WEBSOCKET_SOURCES_PER_SCOPE,
            "missing_live_user_channel": missing_live_user_channel,
            "single_source_scope_count": len(single_source_scopes),
            "single_source_scopes": single_source_scopes,
            "source_count": len(sources),
            "source_ids": [source.source_id for source in sources],
            "user_source_count": len(user_source_ids),
            "user_source_ids": user_source_ids,
        },
    )


def _single_source_websocket_scopes(config: CoinbaseApplicationConfig) -> list[dict[str, JsonValue]]:
    scope_sources: dict[tuple[str, str, str | None], set[str]] = {}
    for source in config.bot.websocket_sources:
        product_ids: tuple[str | None, ...] = source.product_ids or (None,)
        for channel in source.channels:
            for product_id in product_ids:
                scope_sources.setdefault((source.endpoint.value, channel.value, product_id), set()).add(
                    source.source_id
                )

    scopes: list[dict[str, JsonValue]] = []
    for (endpoint, channel, product_id), source_ids in sorted(
        scope_sources.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2] or ""),
    ):
        if len(source_ids) >= MIN_REDUNDANT_WEBSOCKET_SOURCES_PER_SCOPE:
            continue
        scopes.append(
            {
                "channel": channel,
                "endpoint": endpoint,
                "product_id": product_id,
                "source_ids": sorted(source_ids),
            }
        )
    return scopes


def _check(
    name: ReadinessCheckName,
    *,
    count: int,
    details: dict[str, JsonValue],
) -> ReadinessCheckResult:
    return ReadinessCheckResult(
        count=count,
        details=details,
        name=name,
        status=ReadinessStatus.ATTENTION_REQUIRED if count else ReadinessStatus.OK,
    )


def _nearest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    return current
