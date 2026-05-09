from __future__ import annotations

import argparse
import asyncio
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from core.enums import (
    MarginType,
    MarketDataKind,
    OperatorPolicyDistanceType,
    OperatorPolicyReferencePriceSource,
    OperatorPolicyScenarioName,
    OperatorPolicyScenarioStatus,
    OperatorPolicySizingStrategy,
    OperatorPolicyVenue,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSide,
    ReadinessStatus,
    OrderType,
    TimeInForce,
)
from strategies import (
    OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION,
    OPERATOR_POLICY_SCHEMA_VERSION,
    load_operator_policy_from_json_file,
    load_operator_policy_scenarios_from_json_file,
    operator_policy_scenarios_from_mapping,
    run_operator_policy_scenarios,
)
from strategies.operator_policy import operator_policy_from_mapping


def test_checked_in_conservative_cfm_operator_policy_loads_runtime_controls():
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    risk_config = policy.to_risk_policy_config()
    requirements = policy.strategy_input_requirements()
    fragment = policy.runtime_config_fragment()

    assert policy.schema_version == OPERATOR_POLICY_SCHEMA_VERSION
    assert policy.policy_name == "conservative_cfm_policy_v0"
    assert policy.scope.products == ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    assert policy.scope.venue == OperatorPolicyVenue.COINBASE_CFM
    assert policy.scope.live_orders_allowed is True
    assert policy.risk_limits.allowed_sides == (OrderSide.BUY, OrderSide.SELL)
    assert policy.risk_limits.max_daily_notional_usd == Decimal("400")
    assert policy.order_behavior.default_leverage == Decimal("1")
    assert policy.order_behavior.default_margin_type == MarginType.CROSS
    assert policy.order_behavior.default_order_type == OrderType.LIMIT
    assert policy.order_behavior.post_only is True
    assert policy.order_behavior.time_in_force == TimeInForce.GOOD_UNTIL_CANCELLED
    assert policy.staged_or_hidden_release.allow_release is True
    assert risk_config.allowed_products == ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    assert risk_config.allowed_order_types == (OrderType.LIMIT,)
    assert risk_config.allowed_sides == (OrderSide.BUY, OrderSide.SELL)
    assert risk_config.allowed_time_in_force == (TimeInForce.GOOD_UNTIL_CANCELLED,)
    assert risk_config.allowed_lineage_relations == (
        OrderLineageRelation.ROOT,
        OrderLineageRelation.FOLLOWUP_AFTER_FILL,
        OrderLineageRelation.SPLIT_CHILD,
        OrderLineageRelation.CONSOLIDATION,
    )
    assert risk_config.allowed_placement_kinds == (
        OrderPlacementKind.INITIAL,
        OrderPlacementKind.AMEND,
        OrderPlacementKind.CANCEL_REPLACE,
        OrderPlacementKind.STAGED_RELEASE,
        OrderPlacementKind.RELEASE,
    )
    assert risk_config.max_daily_notional == Decimal("400")
    assert risk_config.max_open_orders == 4
    assert risk_config.max_order_notional == Decimal("200")
    assert risk_config.max_visible_notional == Decimal("200")
    assert risk_config.require_post_only is True
    assert risk_config.require_reduce_only is True
    assert risk_config.require_staged_release_above_visible_limit is True
    assert risk_config.kill_switch_enabled is True
    assert len(requirements) == 2
    assert {requirement.product_id for requirement in requirements} == {
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    }
    assert all(requirement.data_kind == MarketDataKind.ORDER_BOOK for requirement in requirements)
    assert all(requirement.max_age.total_seconds() == 5 for requirement in requirements)
    assert fragment["bot"]["feed"]["min_live_sources"] == 2
    assert fragment["bot"]["strategies"]["allow_live_execution"] is True
    assert fragment["bot"]["risk"]["kill_switch_enabled"] is True
    assert "kill_switch_enabled blocks new order placement" in policy.review_notes()[0]


def test_operator_policy_payload_is_json_normalized():
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    payload = policy.to_payload()

    assert payload["schema_version"] == OPERATOR_POLICY_SCHEMA_VERSION
    assert payload["runtime_config_fragment"]["bot"]["risk"]["max_order_notional"] == "200"
    json.dumps(payload, sort_keys=True)


def test_operator_policy_can_allow_staging_without_release_placement():
    raw = json.loads(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json").read_text(encoding="utf-8")
    )
    raw["staged_or_hidden_release"]["allow_release"] = False

    policy = operator_policy_from_mapping(raw)
    risk_config = policy.to_risk_policy_config()

    assert policy.staged_or_hidden_release.allow_release is False
    assert OrderPlacementKind.STAGED_RELEASE in risk_config.allowed_placement_kinds
    assert OrderPlacementKind.RELEASE not in risk_config.allowed_placement_kinds
    assert "release placement is blocked" in policy.review_notes()[-1]


def test_checked_in_stealth_operator_policy_loads_extended_constraints():
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json")
    )

    assert policy.policy_name == "stealth_orders_manager_policy_v1"
    assert policy.scope.products == ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    assert policy.market_data_requirements.require_redundant_feeds is False
    assert policy.anchor_repricing is not None
    assert policy.anchor_repricing.reference_price_source == OperatorPolicyReferencePriceSource.MIDPOINT
    assert policy.anchor_repricing.distance_type == OperatorPolicyDistanceType.PERCENT
    assert policy.anchor_repricing.target_distance == Decimal("0.01")
    assert policy.anchor_repricing.max_distance == Decimal("0.05")
    assert policy.anchor_repricing.min_reprice_interval.total_seconds() == 30
    assert policy.anchor_repricing.max_reprices_per_hour == 20
    assert policy.anchor_repricing.post_only_required is True
    assert policy.sizing is not None
    assert policy.sizing.strategy == OperatorPolicySizingStrategy.FIXED
    assert policy.sizing.tranche_schedule == (
        Decimal("0.25"),
        Decimal("0.50"),
        Decimal("0.75"),
        Decimal("1.0"),
    )
    assert policy.sizing.iceberg_mode is True
    assert policy.sizing.adaptive_base_size is None
    assert policy.sizing.adaptive_reveal_multiplier == Decimal("0.1")
    assert policy.sizing.adaptive_max_reveal_percentage == Decimal("0.5")
    assert policy.max_order_replacements == 11
    assert policy.target_movement == Decimal("0.002")
    assert policy.target_movement_type == OperatorPolicyDistanceType.PERCENT
    assert policy.allow_partial_fills is True
    assert policy.enable_hotpoint_replication is False
    assert policy.runtime_config_fragment()["bot"]["feed"]["min_live_sources"] == 1


def test_operator_policy_rejects_post_only_non_gtc_policy():
    raw = json.loads(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json").read_text(encoding="utf-8")
    )
    raw["order_behavior"]["time_in_force"] = TimeInForce.IMMEDIATE_OR_CANCEL.value

    with pytest.raises(ValueError, match="post_only requires good_until_cancelled"):
        operator_policy_from_mapping(raw)


def test_operator_policy_rejects_alias_time_in_force_value():
    raw = json.loads(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json").read_text(encoding="utf-8")
    )
    raw["order_behavior"]["time_in_force"] = "good_til_cancelled"

    with pytest.raises(ValueError, match="unsupported value"):
        operator_policy_from_mapping(raw)


def test_operator_policy_rejects_anchor_repricing_distance_inversion():
    raw = json.loads(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json").read_text(encoding="utf-8")
    )
    raw["anchor_repricing"]["max_distance"] = "0.001"

    with pytest.raises(ValueError, match="max_distance"):
        operator_policy_from_mapping(raw)


def test_operator_policy_rejects_non_cumulative_tranche_schedule():
    raw = json.loads(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json").read_text(encoding="utf-8")
    )
    raw["sizing"]["tranche_schedule"] = ["0.25", "0.50", "0.50", "1.0"]

    with pytest.raises(ValueError, match="strictly increasing"):
        operator_policy_from_mapping(raw)


def test_checked_in_conservative_cfm_operator_scenarios_are_well_formed():
    payload = json.loads(
        Path("docs/examples/operator-scenarios.conservative-cfm-v0.json").read_text(encoding="utf-8")
    )
    scenarios = payload["scenarios"]

    assert payload["schema_version"] == 1
    assert payload["policy_name"] == "conservative_cfm_policy_v0"
    assert [scenario["scenario"] for scenario in scenarios] == [
        "hard_safety_stop",
        "stale_data_blocks_action",
        "move_same_side_order",
        "filled_order_creates_followup",
        "tidy_nearby_orders",
        "anchor_repricing_forced",
        "tranche_release",
        "adaptive_sizing",
        "slide_mode_enabled",
        "followup_partial_fill",
        "hotpoint_auto_replicate",
    ]
    assert scenarios[0]["expect"]["no_order_submitted"] is True
    assert scenarios[4]["expect"]["lineage_relation"] == "consolidation"
    assert scenarios[7]["expect"]["slice_size"] == "0.4"
    assert scenarios[10]["expect"]["auto_place_additional_order"] is True


def test_checked_in_conservative_cfm_operator_scenarios_are_executable():
    suite = load_operator_policy_scenarios_from_json_file(
        Path("docs/examples/operator-scenarios.conservative-cfm-v0.json")
    )

    report = run_operator_policy_scenarios(suite)
    results = {result.scenario: result for result in report.results}
    payload = report.to_payload()

    assert suite.schema_version == OPERATOR_POLICY_SCENARIO_SCHEMA_VERSION
    assert report.status == ReadinessStatus.OK
    assert report.passed is True
    assert report.passed_count == 10
    assert report.failed_count == 0
    assert report.documented_only_count == 1
    assert results[OperatorPolicyScenarioName.HARD_SAFETY_STOP].status == (
        OperatorPolicyScenarioStatus.PASSED
    )
    assert results[OperatorPolicyScenarioName.ADAPTIVE_SIZING].observed["slice_size"] == "0.4"
    assert results[OperatorPolicyScenarioName.HOTPOINT_AUTO_REPLICATE].status == (
        OperatorPolicyScenarioStatus.DOCUMENTED_ONLY
    )
    assert payload["documented_only_count"] == 1
    assert payload["results"][-1]["status"] == OperatorPolicyScenarioStatus.DOCUMENTED_ONLY.value


def test_operator_policy_scenarios_report_expectation_mismatches():
    raw = json.loads(
        Path("docs/examples/operator-scenarios.conservative-cfm-v0.json").read_text(encoding="utf-8")
    )
    raw["scenarios"] = [
        scenario
        for scenario in raw["scenarios"]
        if scenario["scenario"] == OperatorPolicyScenarioName.ADAPTIVE_SIZING.value
    ]
    raw["scenarios"][0]["expect"]["slice_size"] = "999"
    suite = operator_policy_scenarios_from_mapping(raw)

    report = run_operator_policy_scenarios(suite)

    assert report.status == ReadinessStatus.ATTENTION_REQUIRED
    assert report.failed_count == 1
    assert report.results[0].status == OperatorPolicyScenarioStatus.FAILED
    assert report.results[0].reasons == ("slice_size expected '999' but observed '0.4'",)


def test_operator_policy_scenarios_reject_unknown_scenario_names():
    raw = json.loads(
        Path("docs/examples/operator-scenarios.conservative-cfm-v0.json").read_text(encoding="utf-8")
    )
    raw["scenarios"][0]["scenario"] = "unknown_scenario"

    with pytest.raises(ValueError, match="unsupported value"):
        operator_policy_scenarios_from_mapping(raw)


def test_cli_operator_policy_scenarios_runs_checked_in_fixture(capsys, monkeypatch):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                operator_policy_scenarios_file="docs/examples/operator-scenarios.conservative-cfm-v0.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["passed"] is True
    assert payload["documented_only_count"] == 1


def test_cli_operator_policy_scenarios_returns_attention_on_failure(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    raw = json.loads(
        Path("docs/examples/operator-scenarios.conservative-cfm-v0.json").read_text(encoding="utf-8")
    )
    raw["scenarios"] = [
        scenario
        for scenario in raw["scenarios"]
        if scenario["scenario"] == OperatorPolicyScenarioName.ADAPTIVE_SIZING.value
    ]
    raw["scenarios"][0]["expect"]["slice_size"] = "999"
    scenario_path = workspace_tmp_path / "operator-scenarios.json"
    scenario_path.write_text(json.dumps(raw), encoding="utf-8")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                operator_policy_scenarios_file=str(scenario_path),
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["failed_count"] == 1
