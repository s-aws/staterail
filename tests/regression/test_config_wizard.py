from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from app.config_loading import load_coinbase_application_config_from_json_file
from core.enums import ConfigWizardProfile, ExecutionMode
from strategies import PASSIVE_MARKET_MAKING_STRATEGY_ID
from tools.config_wizard import ConfigWizardError, main, run_config_wizard


def test_config_wizard_noninteractive_renders_valid_cfm_passive_market_making_config(workspace_tmp_path):
    target = workspace_tmp_path / "config.local.json"
    ledger_path = workspace_tmp_path / "operator-cfm-audit.jsonl"

    result = run_config_wizard(
        force=True,
        ledger_path=ledger_path,
        no_input=True,
        products="SHB-26JUN26-CDE, AVA-29MAY26-CDE",
        profile=ConfigWizardProfile.COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING,
        target_path=target,
    )
    config = load_coinbase_application_config_from_json_file(target)

    assert result.profile == ConfigWizardProfile.COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING
    assert result.config_validated is True
    assert result.secrets_written is False
    assert result.render_result.unresolved_placeholders == ()
    assert config.ledger_path == ledger_path
    assert config.bot.rest.execution_mode == ExecutionMode.LIVE
    assert config.bot.strategies.strategy_ids == (PASSIVE_MARKET_MAKING_STRATEGY_ID,)
    assert config.bot.strategies.operator_policy is not None
    assert config.bot.strategies.operator_policy.staged_or_hidden_release is not None
    assert config.bot.strategies.operator_policy.staged_or_hidden_release.allow_release is False
    assert config.bot.risk.allowed_products == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )
    assert any("--live-runtime-gate" in command for command in result.next_commands)
    assert not ledger_path.exists()


def test_config_wizard_interactive_prompts_for_profile_paths_and_products(workspace_tmp_path):
    target = workspace_tmp_path / "config.local.json"
    ledger_path = workspace_tmp_path / "operator-cfm-audit.jsonl"
    prompts: list[str] = []
    answers = iter(
        [
            "4",
            str(target),
            str(ledger_path),
            "SHB-26JUN26-CDE,AVA-29MAY26-CDE",
        ]
    )

    def input_reader(prompt: str) -> str:
        prompts.append(prompt)
        return next(answers)

    output: list[str] = []
    result = run_config_wizard(
        force=True,
        input_reader=input_reader,
        output_writer=output.append,
    )
    config = load_coinbase_application_config_from_json_file(target)

    assert result.profile == ConfigWizardProfile.COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING
    assert prompts == [
        "Select profile number or id: ",
        "Target config path [config.local.json]: ",
        "Ledger path [data/operator-cfm-passive-market-making-audit.jsonl]: ",
        "Product IDs, comma-separated: ",
    ]
    assert output[0] == "Available profiles:"
    assert config.ledger_path == ledger_path
    assert config.bot.product_catalog.product_ids == (
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    )


def test_config_wizard_requires_products_for_cfm_profiles_in_no_input_mode(workspace_tmp_path):
    with pytest.raises(ConfigWizardError, match="--products is required"):
        run_config_wizard(
            force=True,
            ledger_path=workspace_tmp_path / "audit.jsonl",
            no_input=True,
            profile=ConfigWizardProfile.COINBASE_CFM_NO_ORDER,
            target_path=workspace_tmp_path / "config.local.json",
        )


def test_config_wizard_rejects_duplicate_products(workspace_tmp_path):
    with pytest.raises(ConfigWizardError, match="duplicate product id"):
        run_config_wizard(
            force=True,
            ledger_path=workspace_tmp_path / "audit.jsonl",
            no_input=True,
            products="SHB-26JUN26-CDE,SHB-26JUN26-CDE",
            profile=ConfigWizardProfile.COINBASE_CFM_NO_ORDER,
            target_path=workspace_tmp_path / "config.local.json",
        )


def test_config_wizard_cli_lists_profiles_as_json(capsys):
    exit_code = main(["--list-profiles", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [profile["profile"] for profile in payload["profiles"]] == [
        ConfigWizardProfile.DRY_RUN.value,
        ConfigWizardProfile.COINBASE_CFM_NO_ORDER.value,
        ConfigWizardProfile.COINBASE_CFM_POLICY_PROBE.value,
        ConfigWizardProfile.COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING.value,
        ConfigWizardProfile.COINBASE_CFM_STAGED_RELEASE_MANAGER.value,
    ]
    assert payload["profiles"][0]["requires_products"] is False
    assert payload["profiles"][1]["requires_products"] is True


def test_config_wizard_cli_prints_json_summary(workspace_tmp_path, capsys):
    target = workspace_tmp_path / "config.local.json"
    ledger_path = workspace_tmp_path / "audit.jsonl"

    exit_code = main(
        [
            "--profile",
            ConfigWizardProfile.COINBASE_CFM_NO_ORDER.value,
            "--target",
            str(target),
            "--ledger-path",
            str(ledger_path),
            "--products",
            "SHB-26JUN26-CDE,AVA-29MAY26-CDE",
            "--force",
            "--no-input",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["profile"] == ConfigWizardProfile.COINBASE_CFM_NO_ORDER.value
    assert payload["config_validated"] is True
    assert payload["secrets_written"] is False
    assert Path(payload["target_path"]) == target
    assert payload["render"]["unresolved_placeholders"] == []


def test_config_wizard_module_cli_help_runs():
    completed = subprocess.run(
        [sys.executable, "-m", "tools.config_wizard", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "Interactively create" in completed.stdout
    assert "--list-profiles" in completed.stdout
