from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from core.enums import StrategyWizardTemplate
from tools.strategy_wizard import StrategyWizardError, main, run_strategy_wizard, scaffold_strategy_package


def test_strategy_wizard_scaffolds_external_metadata_only_package(workspace_tmp_path, monkeypatch):
    target = workspace_tmp_path / "my-strategy"

    result = scaffold_strategy_package(
        force=True,
        name="My Strategy",
        target_path=target,
        template=StrategyWizardTemplate.METADATA_ONLY,
    )
    pyproject = (target / "pyproject.toml").read_text(encoding="utf-8")
    scenario = json.loads(
        (target / "examples" / "strategy-scenario.my-strategy.json").read_text(encoding="utf-8")
    )
    config = json.loads((target / "examples" / "config.my-strategy.dry-run.json").read_text(encoding="utf-8"))

    assert result.strategy_id == "my-strategy"
    assert result.module_name == "my_strategy"
    assert result.strategy_class_name == "MyStrategyStrategy"
    assert result.template == StrategyWizardTemplate.METADATA_ONLY
    assert "my-strategy = \"my_strategy:build_strategy\"" in pyproject
    assert scenario["strategy_ids"] == ["my-strategy"]
    assert scenario["expectations"]["intent_count"] == 0
    assert config["bot"]["strategies"]["enabled"] is True
    assert config["bot"]["strategies"]["run_on_start"] is True
    assert config["bot"]["strategies"]["strategy_ids"] == ["my-strategy"]
    assert any(path.name == "py.typed" for path in result.files_written)
    assert any("strategy-scenario.my-strategy.json" in command for command in result.next_commands)

    monkeypatch.syspath_prepend(str(target / "src"))
    module = importlib.import_module("my_strategy")
    strategy = module.build_strategy()

    assert strategy.strategy_id == "my-strategy"


def test_strategy_wizard_generated_package_tests_pass(workspace_tmp_path):
    target = workspace_tmp_path / "metadata-strategy"
    scaffold_strategy_package(
        force=True,
        name="metadata strategy",
        target_path=target,
        template=StrategyWizardTemplate.METADATA_ONLY,
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = f"{target / 'src'}{os.pathsep}{Path.cwd()}"

    completed = subprocess.run(
        [sys.executable, "-m", "pytest", str(target / "tests"), "-v"],
        capture_output=True,
        cwd=Path.cwd(),
        env=env,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_strategy_wizard_scaffolds_current_market_data_template(workspace_tmp_path):
    target = workspace_tmp_path / "market-data-strategy"

    result = scaffold_strategy_package(
        force=True,
        name="market data strategy",
        target_path=target,
        template=StrategyWizardTemplate.CURRENT_MARKET_DATA,
    )
    pyproject = (target / "pyproject.toml").read_text(encoding="utf-8")
    strategy_text = (target / "src" / "market_data_strategy" / "strategy.py").read_text(encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONPATH"] = f"{target / 'src'}{os.pathsep}{Path.cwd()}"

    completed = subprocess.run(
        [sys.executable, "-m", "pytest", str(target / "tests"), "-v"],
        capture_output=True,
        cwd=Path.cwd(),
        env=env,
        text=True,
        check=False,
    )

    assert result.template == StrategyWizardTemplate.CURRENT_MARKET_DATA
    assert "staterail>=0.1.2" in pyproject
    assert "latest_ticker(product_id)" in strategy_text
    assert "order_book(product_id)" in strategy_text
    assert "market_trades_for_product(product_id)" in strategy_text
    assert "trade_window" not in strategy_text
    assert "market_window_stats" not in strategy_text
    assert "candles" not in strategy_text
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_strategy_wizard_scaffolds_market_window_stats_template(workspace_tmp_path):
    target = workspace_tmp_path / "window-stats-strategy"

    result = scaffold_strategy_package(
        force=True,
        name="window stats strategy",
        target_path=target,
        template=StrategyWizardTemplate.MARKET_WINDOW_STATS,
    )
    strategy_text = (target / "src" / "window_stats_strategy" / "strategy.py").read_text(encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONPATH"] = f"{target / 'src'}{os.pathsep}{Path.cwd()}"

    completed = subprocess.run(
        [sys.executable, "-m", "pytest", str(target / "tests"), "-v"],
        capture_output=True,
        cwd=Path.cwd(),
        env=env,
        text=True,
        check=False,
    )

    assert result.template == StrategyWizardTemplate.MARKET_WINDOW_STATS
    assert "market_window_stats(product_id, lookback=lookback)" in strategy_text
    assert "order_book_window_stats(product_id, lookback=lookback, levels=levels)" in strategy_text
    assert "PlaceOrderIntent" not in strategy_text
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_strategy_wizard_interactive_uses_safe_defaults(workspace_tmp_path):
    target = workspace_tmp_path / "interactive-strategy"
    prompts: list[str] = []
    answers = iter(["Interactive Strategy", str(target), ""])

    def input_reader(prompt: str) -> str:
        prompts.append(prompt)
        return next(answers)

    output: list[str] = []
    result = run_strategy_wizard(
        force=True,
        input_reader=input_reader,
        output_writer=output.append,
    )

    assert result.strategy_id == "interactive-strategy"
    assert result.template == StrategyWizardTemplate.METADATA_ONLY
    assert prompts == [
        "Strategy/package name: ",
        "Target directory [../interactive-strategy]: ",
        "Select template number or id [metadata_only]: ",
    ]
    assert output[0] == f"Target directory: {target.as_posix()}"
    assert output[1] == "Available templates:"
    assert (target / "src" / "interactive_strategy" / "strategy.py").exists()


def test_strategy_wizard_rejects_nonempty_target_without_force(workspace_tmp_path):
    target = workspace_tmp_path / "existing"
    target.mkdir()
    (target / "README.md").write_text("already here\n", encoding="utf-8")

    with pytest.raises(StrategyWizardError, match="not empty"):
        scaffold_strategy_package(name="existing", target_path=target)


def test_strategy_wizard_no_input_requires_name(workspace_tmp_path):
    with pytest.raises(StrategyWizardError, match="--name is required"):
        run_strategy_wizard(no_input=True, target_path=workspace_tmp_path / "strategy")


def test_strategy_wizard_cli_lists_templates_as_json(capsys):
    exit_code = main(["--list-templates", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [template["template"] for template in payload["templates"]] == [
        StrategyWizardTemplate.CUSTOM.value,
        StrategyWizardTemplate.CURRENT_MARKET_DATA.value,
        StrategyWizardTemplate.MARKET_WINDOW_STATS.value,
        StrategyWizardTemplate.METADATA_ONLY.value,
        StrategyWizardTemplate.NOOP.value,
    ]


def test_strategy_wizard_cli_creates_package_and_prints_json(workspace_tmp_path, capsys):
    target = workspace_tmp_path / "cli-strategy"

    exit_code = main(
        [
            "--name",
            "cli strategy",
            "--target",
            str(target),
            "--template",
            StrategyWizardTemplate.NOOP.value,
            "--force",
            "--no-input",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["strategy_id"] == "cli-strategy"
    assert payload["template"] == StrategyWizardTemplate.NOOP.value
    assert Path(payload["target_path"]) == target
    assert (target / "src" / "cli_strategy" / "strategy.py").exists()


def test_strategy_wizard_module_cli_help_runs():
    completed = subprocess.run(
        [sys.executable, "-m", "tools.strategy_wizard", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "Create an external StateRail strategy package scaffold" in completed.stdout
    assert "--list-templates" in completed.stdout
