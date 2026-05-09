from __future__ import annotations

import copy
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from app.bootstrap import CoinbaseApplicationConfig
from app.config_loading import load_coinbase_application_config_from_mapping
from config.assembly import effective_risk_policy_config
from core.enums import (
    ExecutionMode,
    MarginType,
    OperatorCanaryConfigRole,
    OperatorCanaryPlanIssue,
    OperatorCanaryPlanStep,
    OrderSide,
    OrderType,
    ReadinessStatus,
    TimeInForce,
)
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductCatalog


OPERATOR_CANARY_PLAN_SCHEMA_VERSION = 1
OPERATOR_CANARY_DRY_RUN_CONFIG_SCHEMA_VERSION = 1
EXCHANGE_ORDER_ID_PLACEHOLDER = "REPLACE_WITH_EXCHANGE_ORDER_ID_FROM_LIVE_PLACE_RECEIPT"


def render_operator_canary_dry_run_config(
    *,
    force: bool = False,
    ledger_path: Path,
    source_config_file: Path,
    target_config_file: Path,
) -> dict[str, JsonValue]:
    if not source_config_file.exists():
        raise FileNotFoundError(f"source config file does not exist: {source_config_file}")
    if target_config_file.exists() and not force:
        raise FileExistsError(
            f"target config file already exists: {target_config_file}; use force to overwrite"
        )

    raw = _load_json_mapping(source_config_file)
    rendered = copy.deepcopy(raw)
    disabled_paths: list[str] = []
    enabled_paths: list[str] = []

    rendered["ledger_path"] = ledger_path.as_posix()
    bot = _ensure_mapping(rendered, "bot", "application")

    rest = _ensure_mapping(bot, "rest", "bot")
    rest["execution_mode"] = ExecutionMode.DRY_RUN.value

    product_catalog = _ensure_mapping(bot, "product_catalog", "bot")
    _set_schedule_disabled(product_catalog, "bot.product_catalog", disabled_paths)

    feed = _ensure_mapping(bot, "feed", "bot")
    feed_health = _ensure_mapping(feed, "health", "bot.feed")
    _set_schedule_disabled(feed_health, "bot.feed.health", disabled_paths)

    reconciliation = _ensure_mapping(bot, "reconciliation", "bot")
    for section in ("watchdog", "order_recovery", "fills", "exchange_state"):
        schedule = _ensure_mapping(
            reconciliation,
            section,
            "bot.reconciliation",
        )
        _set_schedule_disabled(
            schedule,
            f"bot.reconciliation.{section}",
            disabled_paths,
        )

    audit_anchor = _ensure_mapping(bot, "audit_anchor", "bot")
    _set_schedule_disabled(audit_anchor, "bot.audit_anchor", disabled_paths)
    audit_archive = _ensure_mapping(bot, "audit_archive", "bot")
    _set_schedule_disabled(audit_archive, "bot.audit_archive", disabled_paths)
    strategies = _ensure_mapping(bot, "strategies", "bot")
    _set_schedule_disabled(strategies, "bot.strategies", disabled_paths)

    bot["websocket_sources"] = []
    disabled_paths.append("bot.websocket_sources")

    trigger_polling = _ensure_mapping(bot, "trigger_polling", "bot")
    trigger_polling["enabled"] = True
    trigger_polling["run_on_start"] = False
    enabled_paths.append("bot.trigger_polling.enabled")

    load_coinbase_application_config_from_mapping(rendered)

    target_config_file.parent.mkdir(parents=True, exist_ok=True)
    target_config_file.write_text(
        json.dumps(rendered, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    payload = {
        "disabled_paths": disabled_paths,
        "enabled_paths": enabled_paths,
        "execution_mode": ExecutionMode.DRY_RUN.value,
        "ledger_path": ledger_path.as_posix(),
        "order_endpoint_called": False,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_CANARY_DRY_RUN_CONFIG_SCHEMA_VERSION,
        "source_config_file": source_config_file.as_posix(),
        "status": ReadinessStatus.OK.value,
        "target_config_file": target_config_file.as_posix(),
        "validated": True,
        "websocket_started": False,
        "writes_config": True,
        "writes_ledger": False,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary dry-run config payload must normalize to an object")
    return normalized


def operator_canary_plan_payload(
    live_config: CoinbaseApplicationConfig,
    dry_run_config: CoinbaseApplicationConfig,
    *,
    dry_run_config_file: str | Path,
    leverage: str | None = None,
    limit_price: str,
    live_config_file: str | Path,
    margin_type: MarginType | None = None,
    operator_id: str,
    order_type: OrderType,
    post_only: bool,
    product_id: str,
    reason: str,
    reduce_only: bool = False,
    side: OrderSide,
    size: str,
    time_in_force: TimeInForce,
    product_catalog: ProductCatalog | None = None,
) -> dict[str, JsonValue]:
    if not isinstance(live_config, CoinbaseApplicationConfig):
        raise TypeError("live_config must be a CoinbaseApplicationConfig")
    if not isinstance(dry_run_config, CoinbaseApplicationConfig):
        raise TypeError("dry_run_config must be a CoinbaseApplicationConfig")
    if not operator_id:
        raise ValueError("operator_id is required")
    if not product_id:
        raise ValueError("product_id is required")
    if not reason:
        raise ValueError("reason is required")
    if not isinstance(side, OrderSide):
        raise TypeError("side must be an OrderSide")
    if not isinstance(order_type, OrderType):
        raise TypeError("order_type must be an OrderType")
    if not isinstance(time_in_force, TimeInForce):
        raise TypeError("time_in_force must be a TimeInForce")
    if margin_type is not None and not isinstance(margin_type, MarginType):
        raise TypeError("margin_type must be a MarginType")
    if product_catalog is not None and not isinstance(product_catalog, ProductCatalog):
        raise TypeError("product_catalog must be a ProductCatalog when provided")

    live_config_path = Path(live_config_file)
    dry_run_config_path = Path(dry_run_config_file)
    live_risk = effective_risk_policy_config(live_config.bot)
    issues = [
        *_config_issues(
            live_config,
            role=OperatorCanaryConfigRole.LIVE,
            product_id=product_id,
            side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
        ),
        *_config_issues(
            dry_run_config,
            role=OperatorCanaryConfigRole.DRY_RUN,
            product_id=product_id,
            side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
        ),
        *_order_shape_issues(
            limit_price=limit_price,
            order_type=order_type,
            post_only=post_only,
            size=size,
        ),
        *_product_metadata_issues(
            product_catalog,
            limit_price=limit_price,
            product_id=product_id,
            risk=live_risk,
            size=size,
        ),
    ]
    status = ReadinessStatus.OK if not issues else ReadinessStatus.ATTENTION_REQUIRED
    order_payload = {
        "leverage": leverage,
        "limit_price": limit_price,
        "margin_type": margin_type.value if margin_type is not None else None,
        "order_type": order_type.value,
        "post_only": post_only,
        "product_id": product_id,
        "reduce_only": reduce_only,
        "side": side.value,
        "size": size,
        "time_in_force": time_in_force.value,
    }
    payload = {
        "dry_run_config_file": dry_run_config_path.as_posix(),
        "dry_run_execution_mode": dry_run_config.bot.rest.execution_mode.value,
        "issues": issues,
        "ledger_paths": {
            "dry_run": dry_run_config.ledger_path.as_posix(),
            "live": live_config.ledger_path.as_posix(),
        },
        "live_config_file": live_config_path.as_posix(),
        "live_execution_mode": live_config.bot.rest.execution_mode.value,
        "operator_id": operator_id,
        "order": order_payload,
        "reason": reason,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_CANARY_PLAN_SCHEMA_VERSION,
        "status": status.value,
        "steps": _steps(
            dry_run_config_file=dry_run_config_path.as_posix(),
            include_strategy_simulation=live_config.bot.strategies.schedule.enabled,
            leverage=leverage,
            limit_price=limit_price,
            live_config_file=live_config_path.as_posix(),
            margin_type=margin_type,
            operator_id=operator_id,
            order_type=order_type,
            post_only=post_only,
            product_id=product_id,
            reason=reason,
            reduce_only=reduce_only,
            side=side,
            size=size,
            time_in_force=time_in_force,
        ),
        "websocket_started": False,
        "writes_ledger": False,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary plan payload must normalize to an object")
    return normalized


def _config_issues(
    config: CoinbaseApplicationConfig,
    *,
    role: OperatorCanaryConfigRole,
    product_id: str,
    reduce_only: bool,
    side: OrderSide,
    order_type: OrderType,
    time_in_force: TimeInForce,
) -> list[dict[str, JsonValue]]:
    issues: list[dict[str, JsonValue]] = []
    expected_mode = ExecutionMode.LIVE if role == OperatorCanaryConfigRole.LIVE else ExecutionMode.DRY_RUN
    if config.bot.rest.execution_mode != expected_mode:
        issues.append(
            _issue(
                (
                    OperatorCanaryPlanIssue.LIVE_CONFIG_NOT_LIVE
                    if role == OperatorCanaryConfigRole.LIVE
                    else OperatorCanaryPlanIssue.DRY_RUN_CONFIG_NOT_DRY_RUN
                ),
                role=role,
                message=(
                    f"{role.value} config execution_mode must be {expected_mode.value}, "
                    f"got {config.bot.rest.execution_mode.value}"
                ),
            )
        )
    risk = effective_risk_policy_config(config.bot)
    if risk.allowed_products and product_id not in risk.allowed_products:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.PRODUCT_OUTSIDE_RISK_SCOPE,
                role=role,
                message=f"product_id {product_id!r} is outside configured risk allowed_products",
            )
        )
    if risk.allowed_sides and side not in risk.allowed_sides:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.SIDE_OUTSIDE_RISK_SCOPE,
                role=role,
                message=f"side {side.value!r} is outside configured risk allowed_sides",
            )
        )
    if risk.allowed_order_types and order_type not in risk.allowed_order_types:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.ORDER_TYPE_OUTSIDE_RISK_SCOPE,
                role=role,
                message=f"order_type {order_type.value!r} is outside configured risk allowed_order_types",
            )
        )
    if risk.allowed_time_in_force and time_in_force not in risk.allowed_time_in_force:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.TIME_IN_FORCE_OUTSIDE_RISK_SCOPE,
                role=role,
                message=f"time_in_force {time_in_force.value!r} is outside configured risk allowed_time_in_force",
            )
        )
    if risk.kill_switch_enabled:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.KILL_SWITCH_ENABLED,
                role=role,
                message="risk kill_switch_enabled would reject canary placement",
            )
        )
    if risk.require_reduce_only and not reduce_only:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.REDUCE_ONLY_REQUIRED,
                role=role,
                message="risk require_reduce_only is true but canary reduce_only is false",
            )
        )
    return issues


def _order_shape_issues(
    *,
    limit_price: str,
    order_type: OrderType,
    post_only: bool,
    size: str,
) -> list[dict[str, JsonValue]]:
    issues: list[dict[str, JsonValue]] = []
    if order_type != OrderType.LIMIT:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.UNSUPPORTED_ORDER_TYPE,
                role=None,
                message="controlled canary placement requires a limit order",
            )
        )
    if not post_only:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.UNSUPPORTED_POST_ONLY,
                role=None,
                message="controlled canary placement requires post_only=true",
            )
        )
    if not _positive_decimal(size):
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.NON_POSITIVE_SIZE,
                role=None,
                message="controlled canary size must be a positive decimal",
            )
        )
    if not _positive_decimal(limit_price):
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.NON_POSITIVE_LIMIT_PRICE,
                role=None,
                message="controlled canary limit_price must be a positive decimal",
            )
        )
    return issues


def _product_metadata_issues(
    product_catalog: ProductCatalog | None,
    *,
    limit_price: str,
    product_id: str,
    risk: Any,
    size: str,
) -> list[dict[str, JsonValue]]:
    if product_catalog is None or not product_catalog.values():
        return []
    product = product_catalog.get(product_id)
    if product is None:
        return [
            _issue(
                OperatorCanaryPlanIssue.PRODUCT_METADATA_MISSING,
                role=OperatorCanaryConfigRole.LIVE,
                message=(
                    f"live ledger has product metadata, but none for product_id {product_id!r}; "
                    "run live no-order preflight for this product before canary placement"
                ),
            )
        ]

    issues: list[dict[str, JsonValue]] = []
    price = _positive_decimal_or_none(limit_price)
    order_size = _positive_decimal_or_none(size)
    if price is None or order_size is None:
        return issues

    if not product.price_is_valid(price):
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.PRICE_INCREMENT_INVALID,
                role=OperatorCanaryConfigRole.LIVE,
                message=(
                    f"limit_price {limit_price!r} does not satisfy product price increment "
                    f"{product.price_increment}"
                ),
            )
        )
    if not product.size_is_valid(order_size):
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.SIZE_INCREMENT_INVALID,
                role=OperatorCanaryConfigRole.LIVE,
                message=(
                    f"size {size!r} does not satisfy product size rules "
                    f"min={product.base_min_size}, max={product.base_max_size}, "
                    f"increment={product.base_increment}"
                ),
            )
        )
    notional = product.notional(order_size, price)
    if notional is None:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.PRODUCT_NOTIONAL_INVALID,
                role=OperatorCanaryConfigRole.LIVE,
                message="canary notional could not be calculated from product metadata",
            )
        )
        return issues

    if not product.notional_is_valid(order_size, price):
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.PRODUCT_NOTIONAL_INVALID,
                role=OperatorCanaryConfigRole.LIVE,
                message=(
                    f"notional {notional} does not satisfy product quote notional rules "
                    f"min={product.quote_min_size}, max={product.quote_max_size}"
                ),
            )
        )
    max_order_notional = getattr(risk, "max_order_notional", None)
    if max_order_notional is not None and notional > max_order_notional:
        issues.append(
            _issue(
                OperatorCanaryPlanIssue.NOTIONAL_ABOVE_RISK_LIMIT,
                role=OperatorCanaryConfigRole.LIVE,
                message=(
                    f"canary notional {notional} exceeds configured max_order_notional "
                    f"{max_order_notional}"
                ),
            )
        )
    return issues


def _steps(
    *,
    dry_run_config_file: str,
    include_strategy_simulation: bool,
    leverage: str | None,
    limit_price: str,
    live_config_file: str,
    margin_type: MarginType | None,
    operator_id: str,
    order_type: OrderType,
    post_only: bool,
    product_id: str,
    reason: str,
    reduce_only: bool,
    side: OrderSide,
    size: str,
    time_in_force: TimeInForce,
) -> list[dict[str, JsonValue]]:
    dry_place = _operator_place_argv(
        config_file=dry_run_config_file,
        leverage=leverage,
        limit_price=limit_price,
        margin_type=margin_type,
        operator_id=operator_id,
        order_type=order_type,
        post_only=post_only,
        product_id=product_id,
        reason=f"{reason} dry-run",
        reduce_only=reduce_only,
        side=side,
        size=size,
        time_in_force=time_in_force,
    )
    live_place = _operator_place_argv(
        config_file=live_config_file,
        leverage=leverage,
        limit_price=limit_price,
        margin_type=margin_type,
        operator_id=operator_id,
        order_type=order_type,
        post_only=post_only,
        product_id=product_id,
        reason=f"{reason} live",
        reduce_only=reduce_only,
        side=side,
        size=size,
        time_in_force=time_in_force,
    )
    steps = [
        _step(
            OperatorCanaryPlanStep.DRY_RUN_PLACE_ORDER,
            dry_place,
            calls_order_endpoint=True,
            description="Submit the canary intent through the dry-run gateway and executor.",
            live_order_endpoint=False,
            writes_ledger=True,
        ),
        _step(
            OperatorCanaryPlanStep.DRY_RUN_OPEN_ORDERS,
            _argv(dry_run_config_file, "--operator-open-orders", "--operator-open-orders-product-id", product_id),
            description="Inspect tracked dry-run open orders after the dry-run canary.",
        ),
        _step(
            OperatorCanaryPlanStep.DRY_RUN_CANCEL_ALL_OPEN_ORDERS,
            _argv(
                dry_run_config_file,
                "--operator-cancel-all-open-orders",
                "--operator-id",
                operator_id,
                "--operator-cancel-product-id",
                product_id,
                "--operator-cancel-reason",
                f"{reason} dry-run cleanup",
            ),
            calls_order_endpoint=True,
            description="Clean up any tracked dry-run canary order before live testing.",
            live_order_endpoint=False,
            writes_ledger=True,
        ),
        _step(
            OperatorCanaryPlanStep.DRY_RUN_LEDGER_HEALTH,
            _argv(dry_run_config_file, "--ledger-health", "--ledger-health-fail-on-attention"),
            description="Verify dry-run canary evidence before touching live endpoints.",
        ),
        _step(
            OperatorCanaryPlanStep.READINESS,
            _argv(live_config_file, "--readiness", "--readiness-fail-on-attention"),
            description="Verify live config, credentials, risk policy, and local state without writing.",
        ),
        _step(
            OperatorCanaryPlanStep.LIVE_NO_ORDER_PREFLIGHT,
            _argv(
                live_config_file,
                "--live-no-order-preflight",
                "--live-no-order-preflight-feed-seconds",
                "10",
                "--live-no-order-preflight-fail-on-attention",
            ),
            description="Run aggregate live no-order checks and append compact preflight evidence.",
            writes_ledger=True,
        ),
        _step(
            OperatorCanaryPlanStep.LIVE_RUNTIME_GATE,
            _argv(live_config_file, "--live-runtime-gate", "--live-runtime-gate-fail-on-attention"),
            description="Verify live runtime admission gates without starting runtime tasks.",
        ),
        _step(
            OperatorCanaryPlanStep.LIVE_PLACE_ORDER,
            live_place,
            calls_order_endpoint=True,
            description="Submit exactly one live post-only canary order.",
            live_order_endpoint=True,
            writes_ledger=True,
        ),
        _step(
            OperatorCanaryPlanStep.LIVE_OPEN_ORDERS,
            _argv(live_config_file, "--operator-open-orders", "--operator-open-orders-product-id", product_id),
            description="Inspect the replayed tracked open order state after live placement.",
        ),
        _step(
            OperatorCanaryPlanStep.LIVE_CANCEL_ORDER,
            _argv(
                live_config_file,
                "--operator-cancel-order",
                "--operator-id",
                operator_id,
                "--operator-cancel-exchange-order-id",
                EXCHANGE_ORDER_ID_PLACEHOLDER,
                "--operator-cancel-reason",
                f"{reason} live cleanup",
            ),
            calls_order_endpoint=True,
            description="Cancel the live canary by exchange order ID from the live placement receipt.",
            live_order_endpoint=True,
            writes_ledger=True,
        ),
        _step(
            OperatorCanaryPlanStep.LIVE_CANARY_EVIDENCE,
            _argv(
                live_config_file,
                "--operator-canary-evidence",
                "--operator-canary-evidence-exchange-order-id",
                EXCHANGE_ORDER_ID_PLACEHOLDER,
                "--operator-canary-evidence-product-id",
                product_id,
                "--operator-canary-evidence-fail-on-attention",
            ),
            description="Replay compact post-canary evidence after live cleanup.",
        ),
        _step(
            OperatorCanaryPlanStep.SOURCE_OF_TRUTH,
            _argv(live_config_file, "--source-of-truth"),
            description="Replay the ledger and inspect source-of-truth state after cleanup.",
        ),
        _step(
            OperatorCanaryPlanStep.LEDGER_HEALTH,
            _argv(live_config_file, "--ledger-health", "--ledger-health-fail-on-attention"),
            description="Verify final ledger health after live canary cleanup.",
        ),
    ]
    if include_strategy_simulation:
        strategy_step = _step(
            OperatorCanaryPlanStep.STRATEGY_SIMULATION,
            _argv(
                live_config_file,
                "--strategy-simulate",
                "--strategy-simulate-record-result",
                "--strategy-simulate-fail-on-attention",
            ),
            description="Record no-order strategy qualification evidence for the current config.",
            writes_ledger=True,
        )
        insert_at = next(
            index
            for index, step in enumerate(steps)
            if step["step"] == OperatorCanaryPlanStep.LIVE_RUNTIME_GATE.value
        )
        steps.insert(insert_at, strategy_step)
    return steps


def _operator_place_argv(
    *,
    config_file: str,
    leverage: str | None,
    limit_price: str,
    margin_type: MarginType | None,
    operator_id: str,
    order_type: OrderType,
    post_only: bool,
    product_id: str,
    reason: str,
    reduce_only: bool,
    side: OrderSide,
    size: str,
    time_in_force: TimeInForce,
) -> list[str]:
    argv = _argv(
        config_file,
        "--operator-place-order",
        "--operator-id",
        operator_id,
        "--operator-place-product-id",
        product_id,
        "--operator-place-side",
        side.value,
        "--operator-place-size",
        size,
        "--operator-place-limit-price",
        limit_price,
        "--operator-place-order-type",
        order_type.value,
        "--operator-place-time-in-force",
        time_in_force.value,
        "--operator-place-reason",
        reason,
    )
    if leverage is not None:
        argv.extend(["--operator-place-leverage", leverage])
    if margin_type is not None:
        argv.extend(["--operator-place-margin-type", margin_type.value])
    if post_only:
        argv.append("--operator-place-post-only")
    if reduce_only:
        argv.append("--operator-place-reduce-only")
    return argv


def _argv(config_file: str, *args: str) -> list[str]:
    return ["python", "-m", "app.main", "--config-file", config_file, *args]


def _step(
    step: OperatorCanaryPlanStep,
    argv: list[str],
    *,
    calls_order_endpoint: bool = False,
    description: str,
    live_order_endpoint: bool = False,
    writes_ledger: bool = False,
) -> dict[str, JsonValue]:
    payload = {
        "argv": argv,
        "calls_order_endpoint": calls_order_endpoint,
        "description": description,
        "live_order_endpoint": live_order_endpoint,
        "step": step.value,
        "stop_on_attention": True,
        "writes_ledger": writes_ledger,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary plan step must normalize to an object")
    return normalized


def _issue(
    issue: OperatorCanaryPlanIssue,
    *,
    message: str,
    role: OperatorCanaryConfigRole | None,
) -> dict[str, JsonValue]:
    payload = {
        "issue": issue.value,
        "message": message,
        "role": role.value if role is not None else None,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary plan issue must normalize to an object")
    return normalized


def _positive_decimal(value: str) -> bool:
    return _positive_decimal_or_none(value) is not None


def _positive_decimal_or_none(value: str) -> Decimal | None:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if parsed <= Decimal("0"):
        return None
    return parsed


def _load_json_mapping(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"config file must contain a JSON object: {path}")
    return raw


def _ensure_mapping(parent: dict[str, Any], key: str, parent_name: str) -> dict[str, Any]:
    existing = parent.setdefault(key, {})
    if not isinstance(existing, dict):
        raise ValueError(f"{parent_name}.{key} must be a JSON object")
    return existing


def _set_schedule_disabled(
    schedule: dict[str, Any],
    path: str,
    disabled_paths: list[str],
) -> None:
    schedule["enabled"] = False
    schedule["run_on_start"] = False
    disabled_paths.append(f"{path}.enabled")
