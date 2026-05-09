from __future__ import annotations

import json
import subprocess
import sys
from decimal import Decimal

import pytest

from app.config_loading import load_coinbase_application_config_from_json_file
from app.readiness import readiness_payload
from config.assembly import effective_risk_policy_config, effective_strategy_market_data_requirements
from core.enums import MarginType, MarketDataKind, OrderPlacementKind, ReadinessStatus
from strategies import PASSIVE_MARKET_MAKING_STRATEGY_ID, STAGED_RELEASE_MANAGER_STRATEGY_ID
from tools.config_template import ConfigTemplateError, render_config_template


def test_config_template_renders_cfm_live_template_for_operator_preflight(workspace_tmp_path):
    target = workspace_tmp_path / "config.local.json"
    ledger_path = workspace_tmp_path / "cfm-live-audit.jsonl"

    result = render_config_template(
        "docs/examples/config.cfm-live.json",
        target,
        ledger_path=ledger_path,
        replacements={"REPLACE_WITH_CFM_PRODUCT_IDS": "BIT-29MAY26-CDE"},
    )
    config = load_coinbase_application_config_from_json_file(target)

    assert result.replacement_count == 7
    assert result.ledger_path_overridden is True
    assert result.unresolved_placeholders == ()
    assert config.ledger_path == ledger_path
    assert config.bot.risk.allowed_products == ("BIT-29MAY26-CDE",)
    assert config.bot.product_catalog.product_ids == ("BIT-29MAY26-CDE",)
    assert config.bot.reconciliation.exchange_state_policy.position_product_ids == ("BIT-29MAY26-CDE",)
    assert {
        product_id
        for source in config.bot.websocket_sources
        for product_id in source.product_ids
    } == {"BIT-29MAY26-CDE"}
    readiness = readiness_payload(
        config,
        jwt_factory_configured=True,
        live_trading_approved=True,
        token_provider_configured=True,
    )

    assert readiness["status"] == ReadinessStatus.OK.value
    assert not ledger_path.exists()


def test_config_template_rejects_unresolved_placeholders(workspace_tmp_path):
    with pytest.raises(ConfigTemplateError, match="unresolved placeholder"):
        render_config_template(
            "docs/examples/config.cfm-live.json",
            workspace_tmp_path / "config.local.json",
            replacements={},
        )


def test_config_template_splices_json_list_replacements_for_multi_product_scope(workspace_tmp_path):
    target = workspace_tmp_path / "config.local.json"

    result = render_config_template(
        "docs/examples/config.cfm-live.json",
        target,
        replacements={
            "REPLACE_WITH_CFM_PRODUCT_IDS": [
                "SHB-26JUN26-CDE",
                "AVA-29MAY26-CDE",
            ]
        },
    )
    config = load_coinbase_application_config_from_json_file(target)

    assert result.replacement_count == 7
    assert config.bot.risk.allowed_products == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert config.bot.product_catalog.product_ids == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert config.bot.reconciliation.exchange_state_policy.position_product_ids == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert {
        product_id
        for source in config.bot.websocket_sources
        for product_id in source.product_ids
    } == {"SHB-26JUN26-CDE", "AVA-29MAY26-CDE"}


def test_config_template_renders_cfm_policy_probe_template(workspace_tmp_path):
    target = workspace_tmp_path / "config.policy-probe.json"
    ledger_path = workspace_tmp_path / "cfm-policy-probe-audit.jsonl"

    result = render_config_template(
        "docs/examples/config.cfm-policy-probe.json",
        target,
        ledger_path=ledger_path,
        replacements={
            "REPLACE_WITH_CFM_PRODUCT_IDS": [
                "SHB-26JUN26-CDE",
                "AVA-29MAY26-CDE",
            ]
        },
    )
    config = load_coinbase_application_config_from_json_file(target)
    risk = effective_risk_policy_config(config.bot)
    requirements = effective_strategy_market_data_requirements(config.bot)
    readiness = readiness_payload(
        config,
        jwt_factory_configured=True,
        live_trading_approved=True,
        token_provider_configured=True,
    )

    assert result.replacement_count == 8
    assert result.ledger_path_overridden is True
    assert result.unresolved_placeholders == ()
    assert config.ledger_path == ledger_path
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.strategies.strategy_ids == ("policy-probe",)
    assert config.bot.strategies.operator_policy is not None
    assert config.bot.strategies.operator_policy.scope.products == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert config.bot.strategies.operator_policy.order_behavior.default_leverage == Decimal("1")
    assert config.bot.strategies.operator_policy.order_behavior.default_margin_type == MarginType.CROSS
    assert risk.allowed_products == ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    assert risk.kill_switch_enabled is True
    assert {requirement.product_id for requirement in requirements} == {
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    }
    assert all(requirement.data_kind == MarketDataKind.ORDER_BOOK for requirement in requirements)
    assert all(requirement.max_age.total_seconds() == 60 for requirement in requirements)
    assert readiness["status"] == ReadinessStatus.OK.value
    assert not ledger_path.exists()


def test_config_template_renders_cfm_passive_market_making_template(workspace_tmp_path):
    target = workspace_tmp_path / "config.passive-mm.json"
    ledger_path = workspace_tmp_path / "cfm-passive-mm-audit.jsonl"

    result = render_config_template(
        "docs/examples/config.cfm-passive-market-making.json",
        target,
        ledger_path=ledger_path,
        replacements={
            "REPLACE_WITH_CFM_PRODUCT_IDS": [
                "SHB-26JUN26-CDE",
                "AVA-29MAY26-CDE",
            ]
        },
    )
    config = load_coinbase_application_config_from_json_file(target)
    risk = effective_risk_policy_config(config.bot)
    requirements = effective_strategy_market_data_requirements(config.bot)
    readiness = readiness_payload(
        config,
        jwt_factory_configured=True,
        live_trading_approved=True,
        token_provider_configured=True,
    )

    assert result.replacement_count == 8
    assert result.ledger_path_overridden is True
    assert result.unresolved_placeholders == ()
    assert config.ledger_path == ledger_path
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.strategies.strategy_ids == (PASSIVE_MARKET_MAKING_STRATEGY_ID,)
    assert config.bot.strategies.strategy_parameters == {
        PASSIVE_MARKET_MAKING_STRATEGY_ID: {
            "half_spread_bps": "50",
            "max_products_per_evaluation": 2,
            "max_staged_release_count_per_side": 1,
            "target_notional_usd": "5",
        }
    }
    assert config.bot.strategies.operator_policy is not None
    assert config.bot.strategies.operator_policy.scope.products == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert risk.allowed_products == ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    assert risk.kill_switch_enabled is False
    assert OrderPlacementKind.STAGED_RELEASE in risk.allowed_placement_kinds
    assert OrderPlacementKind.RELEASE not in risk.allowed_placement_kinds
    assert {requirement.product_id for requirement in requirements} == {
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    }
    assert all(requirement.data_kind == MarketDataKind.ORDER_BOOK for requirement in requirements)
    assert all(requirement.max_age.total_seconds() == 60 for requirement in requirements)
    assert readiness["status"] == ReadinessStatus.OK.value
    assert not ledger_path.exists()


def test_config_template_renders_cfm_passive_market_making_release_template(workspace_tmp_path):
    target = workspace_tmp_path / "config.passive-mm-release.json"
    ledger_path = workspace_tmp_path / "cfm-passive-mm-audit.jsonl"

    result = render_config_template(
        "docs/examples/config.cfm-passive-market-making-release.json",
        target,
        ledger_path=ledger_path,
        replacements={
            "REPLACE_WITH_CFM_PRODUCT_IDS": [
                "SHB-26JUN26-CDE",
                "AVA-29MAY26-CDE",
            ]
        },
    )
    config = load_coinbase_application_config_from_json_file(target)
    risk = effective_risk_policy_config(config.bot)
    requirements = effective_strategy_market_data_requirements(config.bot)
    readiness = readiness_payload(
        config,
        jwt_factory_configured=True,
        live_trading_approved=True,
        token_provider_configured=True,
    )

    assert result.replacement_count == 8
    assert result.ledger_path_overridden is True
    assert result.unresolved_placeholders == ()
    assert config.ledger_path == ledger_path
    assert config.bot.strategies.schedule.enabled is True
    assert config.bot.strategies.allow_live_execution is True
    assert config.bot.strategies.strategy_ids == (STAGED_RELEASE_MANAGER_STRATEGY_ID,)
    assert config.bot.strategies.strategy_parameters == {
        STAGED_RELEASE_MANAGER_STRATEGY_ID: {
            "allow_live_overlap": False,
            "max_releases_per_evaluation": 1,
        }
    }
    assert config.bot.strategies.operator_policy is not None
    assert config.bot.strategies.operator_policy.scope.products == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert risk.allowed_products == ("SHB-26JUN26-CDE", "AVA-29MAY26-CDE")
    assert risk.kill_switch_enabled is False
    assert OrderPlacementKind.RELEASE in risk.allowed_placement_kinds
    assert OrderPlacementKind.STAGED_RELEASE in risk.allowed_placement_kinds
    assert {requirement.product_id for requirement in requirements} == {
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    }
    assert all(requirement.data_kind == MarketDataKind.ORDER_BOOK for requirement in requirements)
    assert all(requirement.max_age.total_seconds() == 60 for requirement in requirements)
    assert readiness["status"] == ReadinessStatus.OK.value
    assert not ledger_path.exists()


def test_config_template_cli_outputs_render_summary(workspace_tmp_path):
    target = workspace_tmp_path / "config.local.json"
    ledger_path = workspace_tmp_path / "cfm-live-audit.jsonl"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.config_template",
            "docs/examples/config.cfm-live.json",
            str(target),
            "--set-json",
            'REPLACE_WITH_CFM_PRODUCT_IDS=["SHB-26JUN26-CDE","AVA-29MAY26-CDE"]',
            "--ledger-path",
            str(ledger_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    payload = json.loads(completed.stdout)

    assert completed.returncode == 0
    assert payload["ledger_path_overridden"] is True
    assert payload["replacement_count"] == 7
    assert payload["unresolved_placeholders"] == []
    config = load_coinbase_application_config_from_json_file(target)

    assert config.ledger_path == ledger_path
    assert config.bot.risk.allowed_products == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
