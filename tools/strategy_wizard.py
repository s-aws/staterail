from __future__ import annotations

import argparse
import json
import keyword
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from textwrap import indent

from core.enums import StrategyWizardTemplate
from core.json_tools import JsonValue, normalize_json
from strategies.registry import STRATEGY_ENTRY_POINT_GROUP


InputReader = Callable[[str], str]
OutputWriter = Callable[[str], None]


class StrategyWizardError(RuntimeError):
    pass


@dataclass(frozen=True)
class StrategyScaffoldResult:
    distribution_name: str
    files_written: tuple[Path, ...]
    force: bool
    module_name: str
    next_commands: tuple[str, ...]
    strategy_class_name: str
    strategy_id: str
    target_path: Path
    template: StrategyWizardTemplate

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "distribution_name": self.distribution_name,
            "files_written": [path.as_posix() for path in self.files_written],
            "force": self.force,
            "module_name": self.module_name,
            "next_commands": list(self.next_commands),
            "strategy_class_name": self.strategy_class_name,
            "strategy_id": self.strategy_id,
            "target_path": self.target_path.as_posix(),
            "template": self.template.value,
        }


def scaffold_strategy_package(
    *,
    force: bool = False,
    name: str,
    package_name: str | None = None,
    strategy_id: str | None = None,
    target_path: Path | str | None = None,
    template: StrategyWizardTemplate | str = StrategyWizardTemplate.METADATA_ONLY,
) -> StrategyScaffoldResult:
    resolved_template = _template_from_value(template)
    resolved_strategy_id = _strategy_id(strategy_id or name)
    distribution_name = _distribution_name(name)
    module_name = _module_name(package_name or distribution_name)
    class_name = _class_name(resolved_strategy_id)
    target = Path(target_path) if target_path is not None else Path("..") / distribution_name

    if target.exists() and any(target.iterdir()) and not force:
        raise StrategyWizardError(f"target directory is not empty: {target}")
    if target.exists() and not target.is_dir():
        raise StrategyWizardError(f"target path exists but is not a directory: {target}")

    files = _strategy_package_files(
        class_name=class_name,
        distribution_name=distribution_name,
        module_name=module_name,
        strategy_id=resolved_strategy_id,
        target=target,
        template=resolved_template,
    )
    written_paths: list[Path] = []
    for relative_path, content in files.items():
        path = target / relative_path
        if path.exists() and not force:
            raise StrategyWizardError(f"target file already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
        written_paths.append(path)

    next_commands = (
        f"python -m pip install -e {target.as_posix()}",
        f"python -m pytest {target.joinpath('tests').as_posix()} -v",
        f"python -m app.main --config-file {target.joinpath('examples', f'config.{resolved_strategy_id}.dry-run.json').as_posix()} --max-cycles 1",
        f"python -m app.main --config-file {target.joinpath('examples', f'config.{resolved_strategy_id}.dry-run.json').as_posix()} --strategy-simulate --strategy-simulate-fail-on-attention",
        f"python -m app.main --config-file docs\\examples\\config.dry-run.json --ledger-path test_runtime\\{resolved_strategy_id}-scenario.jsonl --strategy-scenario-file {target.joinpath('examples', f'strategy-scenario.{resolved_strategy_id}.json').as_posix()}",
    )
    return StrategyScaffoldResult(
        distribution_name=distribution_name,
        files_written=tuple(written_paths),
        force=force,
        module_name=module_name,
        next_commands=next_commands,
        strategy_class_name=class_name,
        strategy_id=resolved_strategy_id,
        target_path=target,
        template=resolved_template,
    )


def run_strategy_wizard(
    *,
    force: bool = False,
    input_reader: InputReader = input,
    name: str | None = None,
    no_input: bool = False,
    output_writer: OutputWriter | None = None,
    package_name: str | None = None,
    strategy_id: str | None = None,
    target_path: Path | str | None = None,
    template: StrategyWizardTemplate | str | None = None,
) -> StrategyScaffoldResult:
    writer = output_writer or (lambda message: print(message))
    resolved_name = _resolve_name(name, input_reader=input_reader, no_input=no_input)
    default_target = Path("..") / _distribution_name(resolved_name)
    resolved_target = _resolve_target(
        target_path,
        default=default_target,
        input_reader=input_reader,
        no_input=no_input,
        output_writer=writer,
    )
    resolved_template = _resolve_template(
        template,
        input_reader=input_reader,
        no_input=no_input,
        output_writer=writer,
    )
    return scaffold_strategy_package(
        force=force,
        name=resolved_name,
        package_name=package_name,
        strategy_id=strategy_id,
        target_path=resolved_target,
        template=resolved_template,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create an external StateRail strategy package scaffold."
    )
    parser.add_argument("--name", default=None, help="Human strategy/package name.")
    parser.add_argument("--target", default=None, help="Target directory for the generated package.")
    parser.add_argument("--package-name", default=None, help="Optional Python module name override.")
    parser.add_argument("--strategy-id", default=None, help="Optional strategy_id override.")
    parser.add_argument(
        "--template",
        choices=[template.value for template in StrategyWizardTemplate],
        default=None,
        help="Safe starting template. Omit for interactive selection.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite generated files.")
    parser.add_argument(
        "--no-input",
        action="store_true",
        help="Fail instead of prompting when required values are missing.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--list-templates", action="store_true", help="List available templates and exit.")
    args = parser.parse_args(argv)

    if args.list_templates:
        payload = _templates_payload()
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_templates()
        return 0

    try:
        result = run_strategy_wizard(
            force=args.force,
            name=args.name,
            no_input=args.no_input,
            package_name=args.package_name,
            strategy_id=args.strategy_id,
            target_path=args.target,
            template=args.template,
        )
    except StrategyWizardError as exc:
        print(f"Strategy wizard failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result.to_payload(), indent=2, sort_keys=True))
    else:
        _print_summary(result)
    return 0


def _strategy_package_files(
    *,
    class_name: str,
    distribution_name: str,
    module_name: str,
    strategy_id: str,
    target: Path,
    template: StrategyWizardTemplate,
) -> dict[Path, str]:
    strategy_scenario_name = f"strategy-scenario.{strategy_id}.json"
    config_name = f"config.{strategy_id}.dry-run.json"
    return {
        Path("pyproject.toml"): _pyproject_text(
            distribution_name=distribution_name,
            module_name=module_name,
            strategy_id=strategy_id,
        ),
        Path("README.md"): _readme_text(
            config_name=config_name,
            distribution_name=distribution_name,
            strategy_id=strategy_id,
            strategy_scenario_name=strategy_scenario_name,
            target=target,
            template=template,
        ),
        Path("src") / module_name / "__init__.py": _init_text(class_name=class_name, strategy_id=strategy_id),
        Path("src") / module_name / "strategy.py": _strategy_text(
            class_name=class_name,
            strategy_id=strategy_id,
            template=template,
        ),
        Path("src") / module_name / "py.typed": "",
        Path("tests") / "test_strategy.py": _test_strategy_text(
            class_name=class_name,
            module_name=module_name,
            strategy_id=strategy_id,
            template=template,
        ),
        Path("examples") / strategy_scenario_name: _json_text(_scenario_payload(strategy_id)),
        Path("examples") / config_name: _json_text(_config_payload(strategy_id)),
    }


def _pyproject_text(*, distribution_name: str, module_name: str, strategy_id: str) -> str:
    return f"""[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "{distribution_name}"
version = "0.1.0"
description = "StateRail strategy package for {strategy_id}."
requires-python = ">=3.11"
dependencies = ["staterail>=0.1.2"]

[project.entry-points."{STRATEGY_ENTRY_POINT_GROUP}"]
{strategy_id} = "{module_name}:build_strategy"

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
"*" = ["py.typed"]
"""


def _init_text(*, class_name: str, strategy_id: str) -> str:
    return f'''from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from {__name_for_relative_strategy_module()} import {class_name}


STRATEGY_ID = "{strategy_id}"


def build_strategy(*, parameters: Mapping[str, Any] | None = None) -> {class_name}:
    return {class_name}.from_parameters(parameters)


__all__ = ["STRATEGY_ID", "{class_name}", "build_strategy"]
'''


def _strategy_text(*, class_name: str, strategy_id: str, template: StrategyWizardTemplate) -> str:
    body = _strategy_evaluate_body(template)
    extra_imports = _strategy_extra_imports(template)
    return f'''from __future__ import annotations

from collections.abc import Mapping
{extra_imports}from typing import Any

from strategies import StrategyDecision, StrategySnapshot


class {class_name}:
    def __init__(self, *, parameters: Mapping[str, Any] | None = None) -> None:
        self._parameters = dict(parameters or {{}})

    @classmethod
    def from_parameters(cls, parameters: Mapping[str, Any] | None = None) -> "{class_name}":
        return cls(parameters=parameters)

    @property
    def strategy_id(self) -> str:
        return "{strategy_id}"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
{indent(body, "        ")}
'''


def _strategy_extra_imports(template: StrategyWizardTemplate) -> str:
    if template == StrategyWizardTemplate.MARKET_WINDOW_STATS:
        return "from datetime import timedelta\n"
    return ""


def _strategy_evaluate_body(template: StrategyWizardTemplate) -> str:
    if template == StrategyWizardTemplate.NOOP:
        return '''del snapshot
return StrategyDecision()'''
    if template == StrategyWizardTemplate.CURRENT_MARKET_DATA:
        return '''product_id = self._parameters.get("product_id")
if not isinstance(product_id, str) or not product_id:
    return StrategyDecision(
        metadata={
            "status": "missing_product_id",
            "template": "current_market_data",
        }
    )

ticker = snapshot.projection.latest_ticker(product_id)
book = snapshot.projection.order_book(product_id)
trades = snapshot.projection.market_trades_for_product(product_id)
latest_trade = trades[-1] if trades else None
return StrategyDecision(
    metadata={
        "has_order_book": book is not None,
        "has_ticker": ticker is not None,
        "latest_trade_sequence": getattr(latest_trade, "sequence", None),
        "order_book_sequence": getattr(book, "sequence", None),
        "product_id": product_id,
        "status": "ok",
        "template": "current_market_data",
        "ticker_sequence": getattr(ticker, "sequence", None),
        "trade_count": len(trades),
    }
)'''
    if template == StrategyWizardTemplate.MARKET_WINDOW_STATS:
        return '''product_id = self._parameters.get("product_id")
if not isinstance(product_id, str) or not product_id:
    return StrategyDecision(
        metadata={
            "status": "missing_product_id",
            "template": "market_window_stats",
        }
    )

lookback_seconds = self._parameters.get("lookback_seconds", 300)
try:
    lookback = timedelta(seconds=float(lookback_seconds))
except (TypeError, ValueError):
    return StrategyDecision(
        metadata={
            "status": "invalid_lookback_seconds",
            "template": "market_window_stats",
        }
    )

book_levels = self._parameters.get("book_levels", 1)
try:
    levels = int(book_levels)
except (TypeError, ValueError):
    return StrategyDecision(
        metadata={
            "status": "invalid_book_levels",
            "template": "market_window_stats",
        }
    )

market_stats = snapshot.market_window_stats(product_id, lookback=lookback)
book_stats = snapshot.order_book_window_stats(product_id, lookback=lookback, levels=levels)
return StrategyDecision(
    metadata={
        "book_window": book_stats.to_payload(),
        "market_window": market_stats.to_payload(),
        "product_id": product_id,
        "status": "ok",
        "template": "market_window_stats",
    }
)'''
    if template == StrategyWizardTemplate.METADATA_ONLY:
        return '''open_orders = tuple(snapshot.projection.open_orders)
return StrategyDecision(
    metadata={
        "as_of_sequence": snapshot.as_of_sequence,
        "open_order_count": len(open_orders),
        "template": "metadata_only",
    }
)'''
    if template == StrategyWizardTemplate.CUSTOM:
        return '''return StrategyDecision(
    metadata={
        "as_of_sequence": snapshot.as_of_sequence,
        "template": "custom",
    }
)'''
    raise ValueError(f"unsupported strategy wizard template: {template.value}")


def _test_strategy_text(
    *,
    class_name: str,
    module_name: str,
    strategy_id: str,
    template: StrategyWizardTemplate,
) -> str:
    if template == StrategyWizardTemplate.CURRENT_MARKET_DATA:
        return f'''from __future__ import annotations

from types import SimpleNamespace

from strategies import StrategyDecision

from {module_name} import STRATEGY_ID, {class_name}, build_strategy


class ProjectionFixture:
    open_orders = ()

    def latest_ticker(self, product_id: str):
        assert product_id == "BTC-USD"
        return SimpleNamespace(sequence=10)

    def order_book(self, product_id: str):
        assert product_id == "BTC-USD"
        return SimpleNamespace(sequence=11)

    def market_trades_for_product(self, product_id: str):
        assert product_id == "BTC-USD"
        return (SimpleNamespace(sequence=12), SimpleNamespace(sequence=13))


def test_strategy_entry_point_factory_returns_expected_strategy() -> None:
    strategy = build_strategy()

    assert strategy.strategy_id == "{strategy_id}"
    assert STRATEGY_ID == "{strategy_id}"
    assert isinstance(strategy, {class_name})


def test_strategy_evaluate_reads_current_market_data_without_order_intents() -> None:
    strategy = build_strategy(parameters={{"product_id": "BTC-USD"}})
    snapshot = SimpleNamespace(
        as_of_sequence=13,
        projection=ProjectionFixture(),
    )

    decision = strategy.evaluate(snapshot)  # type: ignore[arg-type]

    assert isinstance(decision, StrategyDecision)
    assert decision.intents == ()
    assert decision.metadata["template"] == "current_market_data"
    assert decision.metadata["has_ticker"] is True
    assert decision.metadata["has_order_book"] is True
    assert decision.metadata["trade_count"] == 2
    assert decision.metadata["latest_trade_sequence"] == 13
'''
    if template == StrategyWizardTemplate.MARKET_WINDOW_STATS:
        return f'''from __future__ import annotations

from datetime import timedelta

from strategies import StrategyDecision

from {module_name} import STRATEGY_ID, {class_name}, build_strategy


class WindowResult:
    def __init__(self, *, name: str) -> None:
        self._name = name

    def to_payload(self) -> dict[str, object]:
        return {{"status": "ok", "window": self._name}}


class SnapshotFixture:
    as_of_sequence = 13

    def market_window_stats(self, product_id: str, *, lookback: timedelta):
        assert product_id == "BTC-USD"
        assert lookback == timedelta(minutes=5)
        return WindowResult(name="market")

    def order_book_window_stats(self, product_id: str, *, lookback: timedelta, levels: int):
        assert product_id == "BTC-USD"
        assert lookback == timedelta(minutes=5)
        assert levels == 1
        return WindowResult(name="book")


def test_strategy_entry_point_factory_returns_expected_strategy() -> None:
    strategy = build_strategy()

    assert strategy.strategy_id == "{strategy_id}"
    assert STRATEGY_ID == "{strategy_id}"
    assert isinstance(strategy, {class_name})


def test_strategy_evaluate_reads_window_stats_without_order_intents() -> None:
    strategy = build_strategy(parameters={{"product_id": "BTC-USD"}})

    decision = strategy.evaluate(SnapshotFixture())  # type: ignore[arg-type]

    assert isinstance(decision, StrategyDecision)
    assert decision.intents == ()
    assert decision.metadata["template"] == "market_window_stats"
    assert decision.metadata["market_window"]["window"] == "market"
    assert decision.metadata["book_window"]["window"] == "book"
'''
    return f'''from __future__ import annotations

from types import SimpleNamespace

from strategies import StrategyDecision

from {module_name} import STRATEGY_ID, {class_name}, build_strategy


def test_strategy_entry_point_factory_returns_expected_strategy() -> None:
    strategy = build_strategy()

    assert strategy.strategy_id == "{strategy_id}"
    assert STRATEGY_ID == "{strategy_id}"
    assert isinstance(strategy, {class_name})


def test_strategy_evaluate_returns_no_order_intents_by_default() -> None:
    strategy = build_strategy(parameters={{"example": "value"}})
    snapshot = SimpleNamespace(
        as_of_sequence=0,
        projection=SimpleNamespace(open_orders=()),
    )

    decision = strategy.evaluate(snapshot)  # type: ignore[arg-type]

    assert isinstance(decision, StrategyDecision)
    assert decision.intents == ()
'''


def _readme_text(
    *,
    config_name: str,
    distribution_name: str,
    strategy_id: str,
    strategy_scenario_name: str,
    target: Path,
    template: StrategyWizardTemplate,
) -> str:
    return f"""# {distribution_name}

External StateRail strategy package scaffold.

Strategy ID: `{strategy_id}`
Template: `{template.value}`

This package intentionally starts without live trading logic. Add behavior only after writing scenario fixtures and proving simulation output through StateRail's gateway preview.

## Install

```powershell
python -m pip install -e {target.as_posix()}
```

## Test

```powershell
python -m pytest {target.joinpath("tests").as_posix()} -v
```

## Run Scenario Fixture

```powershell
python -m app.main --config-file docs\\examples\\config.dry-run.json --ledger-path test_runtime\\{strategy_id}-scenario.jsonl --strategy-scenario-file {target.joinpath("examples", strategy_scenario_name).as_posix()}
```

## Run Simulation

```powershell
python -m app.main --config-file {target.joinpath("examples", config_name).as_posix()} --max-cycles 1
python -m app.main --config-file {target.joinpath("examples", config_name).as_posix()} --strategy-simulate --strategy-simulate-fail-on-attention
```

Do not call exchange clients, append to the ledger, or maintain a separate source-of-truth store from strategy code.
"""


def _scenario_payload(strategy_id: str) -> dict[str, JsonValue]:
    return {
        "events": [],
        "execution_mode": "dry_run",
        "expectations": {
            "accepted_action_count": 0,
            "action_previews": [],
            "completed_count": 1,
            "failed_count": 0,
            "intent_count": 0,
            "rejected_action_count": 0,
            "status": "ok",
        },
        "name": f"{strategy_id}-empty-ledger",
        "schema_version": 1,
        "strategy_ids": [strategy_id],
    }


def _config_payload(strategy_id: str) -> dict[str, JsonValue]:
    return {
        "bot": {
            "audit_anchor": {
                "enabled": False,
                "interval_seconds": 86400,
                "run_on_start": False,
            },
            "audit_archive": {
                "enabled": False,
                "interval_seconds": 86400,
                "run_on_start": False,
            },
            "product_catalog": {
                "enabled": False,
                "interval_seconds": 3600,
                "product_ids": ["BTC-USD"],
                "run_on_start": False,
            },
            "reconciliation": {
                "exchange_state": {
                    "enabled": False,
                    "interval_seconds": 60,
                    "run_on_start": False,
                },
                "fills": {
                    "enabled": False,
                    "interval_seconds": 30,
                    "run_on_start": False,
                },
                "order_recovery": {
                    "enabled": False,
                    "interval_seconds": 30,
                    "run_on_start": False,
                },
                "watchdog": {
                    "enabled": False,
                    "interval_seconds": 5,
                    "run_on_start": False,
                },
            },
            "rest": {"execution_mode": "dry_run"},
            "risk": {
                "allowed_order_types": ["limit"],
                "allowed_products": ["BTC-USD"],
                "kill_switch_enabled": False,
                "max_order_notional": "1000",
                "max_order_size": "1",
            },
            "strategies": {
                "allow_live_execution": False,
                "enabled": True,
                "interval_seconds": 5,
                "run_on_start": True,
                "strategy_ids": [strategy_id],
            },
            "trigger_polling": {
                "enabled": False,
                "interval_seconds": 1,
                "run_on_start": False,
            },
        },
        "ledger_path": f"test_runtime/{strategy_id}-audit.jsonl",
    }


def _resolve_name(
    name: str | None,
    *,
    input_reader: InputReader,
    no_input: bool,
) -> str:
    if name is not None and name.strip():
        return name.strip()
    if no_input:
        raise StrategyWizardError("--name is required with --no-input")
    raw_name = input_reader("Strategy/package name: ").strip()
    if not raw_name:
        raise StrategyWizardError("strategy/package name is required")
    return raw_name


def _resolve_target(
    target_path: Path | str | None,
    *,
    default: Path,
    input_reader: InputReader,
    no_input: bool,
    output_writer: OutputWriter,
) -> Path:
    if target_path is not None:
        return Path(target_path)
    if no_input:
        return default
    raw_target = input_reader(f"Target directory [{default.as_posix()}]: ").strip()
    target = Path(raw_target) if raw_target else default
    output_writer(f"Target directory: {target.as_posix()}")
    return target


def _resolve_template(
    template: StrategyWizardTemplate | str | None,
    *,
    input_reader: InputReader,
    no_input: bool,
    output_writer: OutputWriter,
) -> StrategyWizardTemplate:
    if template is not None:
        return _template_from_value(template)
    if no_input:
        return StrategyWizardTemplate.METADATA_ONLY
    _print_templates(output_writer)
    raw_value = input_reader("Select template number or id [metadata_only]: ").strip()
    if not raw_value:
        return StrategyWizardTemplate.METADATA_ONLY
    if raw_value.isdigit():
        templates = tuple(StrategyWizardTemplate)
        index = int(raw_value)
        if index < 1 or index > len(templates):
            raise StrategyWizardError(f"template number must be between 1 and {len(templates)}")
        return templates[index - 1]
    return _template_from_value(raw_value)


def _template_from_value(value: StrategyWizardTemplate | str) -> StrategyWizardTemplate:
    if isinstance(value, StrategyWizardTemplate):
        return value
    try:
        return StrategyWizardTemplate(value)
    except ValueError as exc:
        raise StrategyWizardError(f"unknown template: {value}") from exc


def _distribution_name(value: str) -> str:
    slug = _slug(value, separator="-")
    if not slug:
        raise StrategyWizardError("name must contain at least one ASCII letter or digit")
    if slug == "staterail":
        raise StrategyWizardError("strategy package name must not be staterail")
    return slug


def _strategy_id(value: str) -> str:
    slug = _slug(value, separator="-")
    if not slug:
        raise StrategyWizardError("strategy_id must contain at least one ASCII letter or digit")
    return slug


def _module_name(value: str) -> str:
    module = _slug(value, separator="_")
    if not module:
        raise StrategyWizardError("package name must contain at least one ASCII letter or digit")
    if module[0].isdigit():
        module = f"strategy_{module}"
    if keyword.iskeyword(module):
        module = f"{module}_strategy"
    return module


def _class_name(strategy_id: str) -> str:
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", strategy_id) if part]
    if not parts:
        raise StrategyWizardError("strategy_id must contain at least one ASCII letter or digit")
    return "".join(part[:1].upper() + part[1:] for part in parts) + "Strategy"


def _slug(value: str, *, separator: str) -> str:
    return re.sub(r"[^a-z0-9]+", separator, value.lower()).strip(separator)


def _json_text(value: Mapping[str, JsonValue]) -> str:
    return json.dumps(normalize_json(value), indent=2, sort_keys=True) + "\n"


def _templates_payload() -> dict[str, JsonValue]:
    return {
        "templates": [
            {
                "description": _template_description(template),
                "template": template.value,
            }
            for template in StrategyWizardTemplate
        ]
    }


def _print_templates(output_writer: OutputWriter | None = None) -> None:
    writer = output_writer or (lambda message: print(message))
    writer("Available templates:")
    for index, template in enumerate(StrategyWizardTemplate, start=1):
        writer(f"  {index}. {template.value} - {_template_description(template)}")


def _template_description(template: StrategyWizardTemplate) -> str:
    if template == StrategyWizardTemplate.CUSTOM:
        return "empty evaluate() structure returning metadata only until custom logic is added"
    if template == StrategyWizardTemplate.CURRENT_MARKET_DATA:
        return "reads current replayed ticker, order-book, and accepted-trade projection state without emitting order intents"
    if template == StrategyWizardTemplate.MARKET_WINDOW_STATS:
        return "reads replayed trade-window and order-book-window statistics without emitting order intents"
    if template == StrategyWizardTemplate.METADATA_ONLY:
        return "reads replayed projection metadata and emits no order intents"
    if template == StrategyWizardTemplate.NOOP:
        return "emits no intents and no metadata"
    raise ValueError(f"unsupported strategy wizard template: {template.value}")


def _print_summary(result: StrategyScaffoldResult) -> None:
    print(f"Created strategy package: {result.target_path.as_posix()}")
    print(f"Strategy ID: {result.strategy_id}")
    print(f"Template: {result.template.value}")
    print(f"Entry point group: {STRATEGY_ENTRY_POINT_GROUP}")
    print("Files written:")
    for path in result.files_written:
        print(f"  {path.as_posix()}")
    print("")
    print("Next commands:")
    for command in result.next_commands:
        print(f"  {command}")
    print("")
    print("No trading logic was generated. Add behavior only after scenario tests and simulation pass.")


def __name_for_relative_strategy_module() -> str:
    return ".strategy"


if __name__ == "__main__":
    raise SystemExit(main())
