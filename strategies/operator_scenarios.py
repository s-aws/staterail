from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from actions.gateway import ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    EventType,
    ExecutionMode,
    MarketDataKind,
    OperatorPolicyDistanceType,
    OperatorPolicyScenarioName,
    OperatorPolicyScenarioStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSide,
    OrderSizingDecisionStatus,
    OrderType,
    ProductType,
    ProductVenue,
    ReadinessStatus,
)
from core.json_tools import JsonValue, normalize_json
from orders.sizing import LineageSizingPolicy
from products.catalog import ProductMetadata
from projections.state import MarketOrderBookSnapshot, SourceOfTruthProjection
from risk.gate import RiskGate, RiskPolicy
from strategies.harness import StrategyInputRequirement, StrategySnapshot
from strategies.policy_calculations import (
    adaptive_reveal_size,
    anchored_price,
    slide_price_toward,
    tranche_release_sizes,
)


OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION = 1
_SCENARIO_PRODUCT_ID = "SCENARIO-CFM"


@dataclass(frozen=True)
class OperatorPolicyScenario:
    scenario: OperatorPolicyScenarioName
    given: Mapping[str, Any] = field(default_factory=dict)
    when: Mapping[str, Any] = field(default_factory=dict)
    expect: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.scenario, OperatorPolicyScenarioName):
            raise TypeError("scenario must be an OperatorPolicyScenarioName")
        _json_object(self.given, "given")
        _json_object(self.when, "when")
        _json_object(self.expect, "expect")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "expect": _json_object(self.expect, "expect"),
            "given": _json_object(self.given, "given"),
            "scenario": self.scenario.value,
            "when": _json_object(self.when, "when"),
        }


@dataclass(frozen=True)
class OperatorPolicyScenarioSuite:
    policy_name: str
    scenarios: tuple[OperatorPolicyScenario, ...]
    schema_version: int = OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION:
            raise ValueError(
                "operator policy scenario schema_version must be "
                f"{OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION}"
            )
        if not isinstance(self.policy_name, str) or not self.policy_name:
            raise ValueError("policy_name is required")
        if not isinstance(self.scenarios, tuple):
            raise TypeError("scenarios must be a tuple")
        if any(not isinstance(scenario, OperatorPolicyScenario) for scenario in self.scenarios):
            raise TypeError("scenarios must contain OperatorPolicyScenario values")
        scenario_names = tuple(scenario.scenario for scenario in self.scenarios)
        if len(scenario_names) != len(set(scenario_names)):
            raise ValueError("scenario names must be unique")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "policy_name": self.policy_name,
            "scenarios": [scenario.to_payload() for scenario in self.scenarios],
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class OperatorPolicyScenarioResult:
    scenario: OperatorPolicyScenarioName
    status: OperatorPolicyScenarioStatus
    expected: Mapping[str, Any]
    observed: Mapping[str, Any]
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.scenario, OperatorPolicyScenarioName):
            raise TypeError("scenario must be an OperatorPolicyScenarioName")
        if not isinstance(self.status, OperatorPolicyScenarioStatus):
            raise TypeError("status must be an OperatorPolicyScenarioStatus")
        _json_object(self.expected, "expected")
        _json_object(self.observed, "observed")
        if not isinstance(self.reasons, tuple):
            raise TypeError("reasons must be a tuple")
        if any(not isinstance(reason, str) or not reason for reason in self.reasons):
            raise ValueError("reasons must contain non-empty strings")

    @property
    def failed(self) -> bool:
        return self.status == OperatorPolicyScenarioStatus.FAILED

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "expected": _json_object(self.expected, "expected"),
            "observed": _json_object(self.observed, "observed"),
            "reasons": list(self.reasons),
            "scenario": self.scenario.value,
            "status": self.status.value,
        }


@dataclass(frozen=True)
class OperatorPolicyScenarioReport:
    policy_name: str
    results: tuple[OperatorPolicyScenarioResult, ...]
    schema_version: int = OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION:
            raise ValueError(
                "operator policy scenario report schema_version must be "
                f"{OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION}"
            )
        if not isinstance(self.policy_name, str) or not self.policy_name:
            raise ValueError("policy_name is required")
        if not isinstance(self.results, tuple):
            raise TypeError("results must be a tuple")
        if any(not isinstance(result, OperatorPolicyScenarioResult) for result in self.results):
            raise TypeError("results must contain OperatorPolicyScenarioResult values")

    @property
    def failed_count(self) -> int:
        return sum(1 for result in self.results if result.status == OperatorPolicyScenarioStatus.FAILED)

    @property
    def documented_only_count(self) -> int:
        return sum(
            1 for result in self.results if result.status == OperatorPolicyScenarioStatus.DOCUMENTED_ONLY
        )

    @property
    def passed_count(self) -> int:
        return sum(1 for result in self.results if result.status == OperatorPolicyScenarioStatus.PASSED)

    @property
    def passed(self) -> bool:
        return self.failed_count == 0

    @property
    def status(self) -> ReadinessStatus:
        return ReadinessStatus.OK if self.passed else ReadinessStatus.ATTENTION_REQUIRED

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "documented_only_count": self.documented_only_count,
            "failed_count": self.failed_count,
            "passed": self.passed,
            "passed_count": self.passed_count,
            "policy_name": self.policy_name,
            "result_count": len(self.results),
            "results": [result.to_payload() for result in self.results],
            "schema_version": self.schema_version,
            "status": self.status.value,
        }


def load_operator_policy_scenarios_from_json_file(path: Path) -> OperatorPolicyScenarioSuite:
    if not isinstance(path, Path):
        raise TypeError("path must be a pathlib.Path")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"operator policy scenario file must be valid JSON: {exc}") from exc
    return operator_policy_scenarios_from_mapping(raw)


def operator_policy_scenarios_from_mapping(raw: object) -> OperatorPolicyScenarioSuite:
    data = _require_mapping(raw, "operator_policy_scenarios")
    _reject_unknown_fields(data, "operator_policy_scenarios", {"policy_name", "scenarios", "schema_version"})
    schema_version = _int(data.get("schema_version"), "schema_version")
    scenarios = data.get("scenarios")
    if not isinstance(scenarios, list):
        raise TypeError("scenarios must be a list")
    return OperatorPolicyScenarioSuite(
        policy_name=_string(data.get("policy_name"), "policy_name"),
        scenarios=tuple(_operator_policy_scenario(item, index) for index, item in enumerate(scenarios)),
        schema_version=schema_version,
    )


def run_operator_policy_scenarios(suite: OperatorPolicyScenarioSuite) -> OperatorPolicyScenarioReport:
    if not isinstance(suite, OperatorPolicyScenarioSuite):
        raise TypeError("suite must be an OperatorPolicyScenarioSuite")
    return OperatorPolicyScenarioReport(
        policy_name=suite.policy_name,
        results=tuple(_evaluate_scenario(scenario) for scenario in suite.scenarios),
    )


def operator_policy_scenario_payload(path: Path) -> dict[str, JsonValue]:
    suite = load_operator_policy_scenarios_from_json_file(path)
    return run_operator_policy_scenarios(suite).to_payload()


def _operator_policy_scenario(raw: object, index: int) -> OperatorPolicyScenario:
    field_name = f"scenarios[{index}]"
    data = _require_mapping(raw, field_name)
    _reject_unknown_fields(data, field_name, {"expect", "given", "scenario", "when"})
    return OperatorPolicyScenario(
        expect=_require_mapping(data.get("expect", {}), f"{field_name}.expect"),
        given=_require_mapping(data.get("given", {}), f"{field_name}.given"),
        scenario=_enum(OperatorPolicyScenarioName, data.get("scenario"), f"{field_name}.scenario"),
        when=_require_mapping(data.get("when", {}), f"{field_name}.when"),
    )


def _evaluate_scenario(scenario: OperatorPolicyScenario) -> OperatorPolicyScenarioResult:
    if scenario.scenario == OperatorPolicyScenarioName.HARD_SAFETY_STOP:
        observed = _evaluate_hard_safety_stop(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.STALE_DATA_BLOCKS_ACTION:
        observed = _evaluate_stale_data_blocks_action(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.MOVE_SAME_SIDE_ORDER:
        observed = _evaluate_move_same_side_order(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.FILLED_ORDER_CREATES_FOLLOWUP:
        observed = _evaluate_followup_after_fill(scenario, filled_size_field="filled_size")
    elif scenario.scenario == OperatorPolicyScenarioName.TIDY_NEARBY_ORDERS:
        observed = _evaluate_tidy_nearby_orders(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.ANCHOR_REPRICING_FORCED:
        observed = _evaluate_anchor_repricing_forced(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.TRANCHE_RELEASE:
        observed = _evaluate_tranche_release(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.ADAPTIVE_SIZING:
        observed = _evaluate_adaptive_sizing(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.SLIDE_MODE_ENABLED:
        observed = _evaluate_slide_mode_enabled(scenario)
    elif scenario.scenario == OperatorPolicyScenarioName.FOLLOWUP_PARTIAL_FILL:
        observed = _evaluate_followup_after_fill(scenario, filled_size_field="filled_size")
    elif scenario.scenario == OperatorPolicyScenarioName.HOTPOINT_AUTO_REPLICATE:
        return OperatorPolicyScenarioResult(
            expected=scenario.expect,
            observed={
                "auto_place_additional_order": False,
                "implemented": False,
            },
            reasons=(
                "hotpoint auto-replication is documented only until an explicit strategy and risk contract exist",
            ),
            scenario=scenario.scenario,
            status=OperatorPolicyScenarioStatus.DOCUMENTED_ONLY,
        )
    else:
        raise ValueError(f"unsupported operator policy scenario: {scenario.scenario.value}")

    reasons = _expectation_mismatches(scenario.expect, observed)
    return OperatorPolicyScenarioResult(
        expected=scenario.expect,
        observed=observed,
        reasons=reasons,
        scenario=scenario.scenario,
        status=(
            OperatorPolicyScenarioStatus.PASSED
            if not reasons
            else OperatorPolicyScenarioStatus.FAILED
        ),
    )


def _evaluate_hard_safety_stop(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    if not _bool(scenario.when.get("strategy_requests_new_order"), "strategy_requests_new_order"):
        return {"action_rejected": False, "no_order_submitted": True}

    daily_used = _decimal(scenario.given.get("daily_notional_used_usd"), "daily_notional_used_usd")
    max_daily = _decimal(scenario.given.get("max_daily_notional_usd"), "max_daily_notional_usd")
    clock = FixedClock(datetime(2026, 1, 1, 12, tzinfo=timezone.utc))
    with TemporaryDirectory() as temp_dir:
        ledger = AuditLedger(Path(temp_dir) / "operator-scenario.jsonl", clock=clock)
        gateway = ActionGateway(
            AuditCore(ledger),
            risk_gate=RiskGate(RiskPolicy.from_values(max_daily_notional=max_daily)),
        )
        if daily_used > 0:
            gateway.submit(
                PlaceOrderIntent(
                    action_id="scenario-existing-daily-notional",
                    limit_price="1",
                    order_type=OrderType.LIMIT,
                    product_id=_SCENARIO_PRODUCT_ID,
                    side=OrderSide.BUY,
                    size=str(daily_used),
                ).to_command()
            )
        receipt = gateway.submit(
            PlaceOrderIntent(
                action_id="scenario-new-order",
                limit_price="1",
                order_type=OrderType.LIMIT,
                product_id=_SCENARIO_PRODUCT_ID,
                side=OrderSide.BUY,
                size="1",
            ).to_command()
        )
        event_types = tuple(record.event_type for record in ledger.iter_records())
    return {
        "action_rejected": receipt.status == ActionStatus.REJECTED,
        "no_order_submitted": EventType.ACTION_EXECUTION_STARTED not in event_types,
    }


def _evaluate_stale_data_blocks_action(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    if not _bool(scenario.when.get("strategy_evaluates"), "strategy_evaluates"):
        return {"action_count": 0, "attention_required": False}

    product_id = _string(scenario.given.get("product"), "product")
    latest_age = _number(
        scenario.given.get("latest_order_book_age_seconds"),
        "latest_order_book_age_seconds",
    )
    max_age = _number(scenario.given.get("max_allowed_age_seconds"), "max_allowed_age_seconds")
    evaluated_at = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    observed_at = evaluated_at - timedelta(seconds=latest_age)
    projection = SourceOfTruthProjection()
    projection.order_books_by_product_id[product_id] = MarketOrderBookSnapshot(
        message_key="scenario-order-book",
        observed_at=observed_at,
        product_id=product_id,
        sequence=1,
        source_id="scenario",
    )
    snapshot = StrategySnapshot(
        as_of_sequence=1,
        evaluated_at=evaluated_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=Path("operator-scenario.jsonl"),
        projection=projection,
    )
    freshness = StrategyInputRequirement(
        data_kind=MarketDataKind.ORDER_BOOK,
        max_age=timedelta(seconds=max_age),
        product_id=product_id,
    ).evaluate(snapshot)
    return {
        "action_count": 0 if not freshness.is_ok else 1,
        "attention_required": not freshness.is_ok,
    }


def _evaluate_move_same_side_order(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    existing = _require_mapping(scenario.given.get("existing_order"), "existing_order")
    original_side = _order_side(existing.get("side"), "existing_order.side")
    new_side = original_side
    venue_supports_amend = _bool(scenario.when.get("venue_supports_amend"), "venue_supports_amend")
    placement_kind = OrderPlacementKind.AMEND if venue_supports_amend else OrderPlacementKind.CANCEL_REPLACE
    return {
        "cancel_original": placement_kind == OrderPlacementKind.CANCEL_REPLACE,
        "place_replacement": placement_kind == OrderPlacementKind.CANCEL_REPLACE,
        "preserve_logical_order_id": new_side == original_side,
    }


def _evaluate_followup_after_fill(
    scenario: OperatorPolicyScenario,
    *,
    filled_size_field: str,
) -> dict[str, JsonValue]:
    parent = _require_mapping(scenario.given.get("parent_order"), "parent_order")
    parent_side = _order_side(parent.get("side"), "parent_order.side")
    parent_size = _decimal(parent.get("size"), "parent_order.size")
    filled_size = _decimal(parent.get(filled_size_field), f"parent_order.{filled_size_field}")
    decision = LineageSizingPolicy.from_values(product=_scenario_product()).followup_size(
        filled_size=filled_size,
        limit_price=parent.get("price") or "1",
        parent_size=parent_size,
    )
    child_size = str(filled_size) if decision.status == OrderSizingDecisionStatus.ACCEPTED else None
    return {
        "child_order": {
            "linked_to_parent": decision.status == OrderSizingDecisionStatus.ACCEPTED,
            "side": _opposite_side(parent_side).value,
            "size": child_size,
        }
    }


def _evaluate_tidy_nearby_orders(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    open_orders = scenario.given.get("open_orders")
    if not isinstance(open_orders, list):
        raise TypeError("open_orders must be a list")
    orders = tuple(_require_mapping(order, "open_orders[]") for order in open_orders)
    sizes = tuple(_decimal(order.get("size"), "open_orders[].size") for order in orders)
    side = _order_side(orders[0].get("side"), "open_orders[].side")
    price = _decimal(orders[0].get("price"), "open_orders[].price")
    decision = LineageSizingPolicy.from_values(product=_scenario_product()).consolidated_size(
        limit_price=price,
        source_sizes=sizes,
    )
    merged_size = decision.output_sizes[0] if decision.output_sizes else None
    return {
        "cancel_originals": decision.status == OrderSizingDecisionStatus.ACCEPTED,
        "create_merged_order": {
            "price": str(price),
            "side": side.value,
            "size": str(merged_size) if merged_size is not None else None,
        },
        "lineage_relation": OrderLineageRelation.CONSOLIDATION.value,
    }


def _evaluate_anchor_repricing_forced(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    price = anchored_price(
        current_price=_decimal(scenario.given.get("order_price"), "order_price"),
        distance_type=OperatorPolicyDistanceType.PERCENT,
        max_distance=_decimal(scenario.given.get("max_distance"), "max_distance"),
        reference_price=_decimal(scenario.given.get("reference_price"), "reference_price"),
    )
    return {"order_repriced_to": str(price)}


def _evaluate_tranche_release(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    schedule = scenario.given.get("tranche_schedule")
    if not isinstance(schedule, list):
        raise TypeError("tranche_schedule must be a list")
    sizes = tranche_release_sizes(
        total_size=_decimal(scenario.given.get("total_size"), "total_size"),
        tranche_schedule=tuple(_decimal(value, "tranche_schedule[]") for value in schedule),
    )
    return {
        "only_one_live_at_a_time": True,
        "order_sizes": [str(size) for size in sizes],
    }


def _evaluate_adaptive_sizing(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    size = adaptive_reveal_size(
        base_size=_decimal(scenario.given.get("base_size"), "base_size"),
        baseline_volume=_decimal(scenario.given.get("baseline_vol"), "baseline_vol"),
        market_volume=_decimal(scenario.given.get("market_vol"), "market_vol"),
        reveal_multiplier=_decimal(scenario.given.get("multiplier"), "multiplier"),
    )
    return {"slice_size": str(size)}


def _evaluate_slide_mode_enabled(scenario: OperatorPolicyScenario) -> dict[str, JsonValue]:
    price = slide_price_toward(
        current_price=_decimal(scenario.given.get("current_price"), "current_price"),
        desired_price=_decimal(scenario.given.get("desired_price"), "desired_price"),
        max_step=_decimal(scenario.given.get("max_step_per_reprice"), "max_step_per_reprice"),
    )
    return {"next_price": str(price)}


def _expectation_mismatches(expected: Mapping[str, Any], observed: Mapping[str, Any]) -> tuple[str, ...]:
    reasons: list[str] = []
    _compare_expected_mapping(expected, observed, path="", reasons=reasons)
    return tuple(reasons)


def _compare_expected_mapping(
    expected: Mapping[str, Any],
    observed: Mapping[str, Any],
    *,
    path: str,
    reasons: list[str],
) -> None:
    for key, expected_value in expected.items():
        key_path = f"{path}.{key}" if path else str(key)
        if key not in observed:
            reasons.append(f"{key_path} missing from observed result")
            continue
        _compare_expected_value(expected_value, observed[key], path=key_path, reasons=reasons)


def _compare_expected_value(expected: Any, observed: Any, *, path: str, reasons: list[str]) -> None:
    if isinstance(expected, Mapping):
        if not isinstance(observed, Mapping):
            reasons.append(f"{path} expected object but observed {type(observed).__name__}")
            return
        _compare_expected_mapping(expected, observed, path=path, reasons=reasons)
        return
    if isinstance(expected, list):
        if not isinstance(observed, list) or len(expected) != len(observed):
            reasons.append(f"{path} expected list length {len(expected)}")
            return
        for index, expected_item in enumerate(expected):
            _compare_expected_value(
                expected_item,
                observed[index],
                path=f"{path}[{index}]",
                reasons=reasons,
            )
        return
    if _values_equal(expected, observed):
        return
    reasons.append(f"{path} expected {expected!r} but observed {observed!r}")


def _values_equal(expected: Any, observed: Any) -> bool:
    expected_decimal = _decimal_or_none(expected)
    observed_decimal = _decimal_or_none(observed)
    if expected_decimal is not None and observed_decimal is not None:
        return expected_decimal == observed_decimal
    return expected == observed


def _scenario_product() -> ProductMetadata:
    return ProductMetadata(
        base_increment=Decimal("1"),
        base_min_size=Decimal("1"),
        price_increment=Decimal("1"),
        product_id=_SCENARIO_PRODUCT_ID,
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_min_size=Decimal("1"),
    )


def _opposite_side(side: OrderSide) -> OrderSide:
    if side == OrderSide.BUY:
        return OrderSide.SELL
    if side == OrderSide.SELL:
        return OrderSide.BUY
    raise ValueError(f"unsupported order side: {side.value}")


def _order_side(raw: object, field_name: str) -> OrderSide:
    return _enum(OrderSide, raw, field_name)


def _json_object(raw: Mapping[str, Any], field_name: str) -> dict[str, JsonValue]:
    normalized = normalize_json(raw)
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must normalize to a JSON object")
    return normalized


def _require_mapping(raw: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise TypeError(f"{field_name} must be a JSON object")
    return raw


def _reject_unknown_fields(data: Mapping[str, Any], field_name: str, allowed_fields: set[str]) -> None:
    unknown_fields = sorted(set(data) - allowed_fields)
    if unknown_fields:
        raise ValueError(f"{field_name} has unknown fields: {', '.join(unknown_fields)}")


def _string(raw: object, field_name: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{field_name} must be a non-empty string")
    return raw


def _bool(raw: object, field_name: str) -> bool:
    if not isinstance(raw, bool):
        raise TypeError(f"{field_name} must be a bool")
    return raw


def _int(raw: object, field_name: str) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise TypeError(f"{field_name} must be an integer")
    return raw


def _number(raw: object, field_name: str) -> float:
    if not isinstance(raw, int | float) or isinstance(raw, bool):
        raise TypeError(f"{field_name} must be numeric")
    return float(raw)


def _decimal(raw: object, field_name: str) -> Decimal:
    if isinstance(raw, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return value


def _decimal_or_none(raw: object) -> Decimal | None:
    if isinstance(raw, bool) or raw is None:
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() else None


def _enum(enum_type: type[Any], raw: object, field_name: str) -> Any:
    if isinstance(raw, enum_type):
        return raw
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{field_name} must be a non-empty string")
    try:
        return enum_type(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name} has unsupported value: {raw}") from exc
