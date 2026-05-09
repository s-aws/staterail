from __future__ import annotations

from pathlib import Path

from core.json_tools import JsonValue
from strategies import operator_policy_scenario_payload


def operator_policy_scenarios_payload(scenario_file: Path) -> dict[str, JsonValue]:
    if not isinstance(scenario_file, Path):
        raise TypeError("scenario_file must be a pathlib.Path")
    return operator_policy_scenario_payload(scenario_file)
