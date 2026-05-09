from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.json_tools import JsonValue, normalize_json
from strategies.simulation import StrategySimulationEvaluation, StrategySimulationReport


MetadataPath = str | tuple[str | int, ...]


def strategy_evaluation(
    report: StrategySimulationReport,
    strategy_id: str,
) -> StrategySimulationEvaluation:
    if not isinstance(report, StrategySimulationReport):
        raise TypeError("report must be a StrategySimulationReport")
    if not isinstance(strategy_id, str) or not strategy_id:
        raise TypeError("strategy_id must be a non-empty string")
    matches = tuple(evaluation for evaluation in report.evaluations if evaluation.strategy_id == strategy_id)
    if not matches:
        raise AssertionError(f"strategy evaluation not found: {strategy_id}")
    if len(matches) > 1:
        raise AssertionError(f"strategy evaluation is not unique: {strategy_id}")
    return matches[0]


def strategy_metadata(
    report: StrategySimulationReport,
    strategy_id: str,
) -> dict[str, JsonValue]:
    return _object_payload(strategy_evaluation(report, strategy_id).metadata, "metadata")


def assert_strategy_metadata_contains(
    report: StrategySimulationReport,
    strategy_id: str,
    expected: Mapping[str, Any],
) -> dict[str, JsonValue]:
    observed = strategy_metadata(report, strategy_id)
    expected_payload = _object_payload(expected, "expected")
    failures: list[str] = []
    _append_subset_failures(
        failures,
        expected=expected_payload,
        observed=observed,
        path="metadata",
    )
    if failures:
        joined = "; ".join(failures)
        raise AssertionError(f"strategy metadata mismatch for {strategy_id}: {joined}")
    return observed


def assert_strategy_metadata_path(
    report: StrategySimulationReport,
    strategy_id: str,
    path: MetadataPath,
    expected: Any,
) -> JsonValue:
    observed_metadata = strategy_metadata(report, strategy_id)
    path_parts = _path_parts(path)
    observed = _lookup_path(observed_metadata, path_parts)
    expected_payload = normalize_json(expected)
    if observed != expected_payload:
        path_text = _path_text(path_parts)
        raise AssertionError(
            f"strategy metadata mismatch for {strategy_id} at {path_text}: "
            f"expected {expected_payload!r}, observed {observed!r}"
        )
    return observed


def _append_subset_failures(
    failures: list[str],
    *,
    expected: Mapping[str, JsonValue],
    observed: Mapping[str, JsonValue],
    path: str,
) -> None:
    for key, expected_value in expected.items():
        key_path = f"{path}.{key}"
        if key not in observed:
            failures.append(f"{key_path} missing")
            continue
        observed_value = observed[key]
        if isinstance(expected_value, Mapping):
            if isinstance(observed_value, Mapping):
                _append_subset_failures(
                    failures,
                    expected=expected_value,
                    observed=observed_value,
                    path=key_path,
                )
                continue
            failures.append(f"{key_path} expected object, observed {observed_value!r}")
            continue
        if observed_value != expected_value:
            failures.append(f"{key_path} expected {expected_value!r}, observed {observed_value!r}")


def _lookup_path(payload: JsonValue, path_parts: tuple[str | int, ...]) -> JsonValue:
    current = payload
    traversed: list[str | int] = []
    for part in path_parts:
        traversed.append(part)
        if isinstance(part, str):
            if not isinstance(current, Mapping) or part not in current:
                raise AssertionError(f"metadata path missing: {_path_text(tuple(traversed))}")
            current = current[part]
            continue
        if not isinstance(current, list) or part < 0 or part >= len(current):
            raise AssertionError(f"metadata path missing: {_path_text(tuple(traversed))}")
        current = current[part]
    return current


def _path_parts(path: MetadataPath) -> tuple[str | int, ...]:
    if isinstance(path, str):
        if not path:
            raise ValueError("path must not be empty")
        return tuple(part for part in path.split(".") if part)
    if not isinstance(path, tuple) or not path:
        raise TypeError("path must be a non-empty string or tuple")
    for part in path:
        if isinstance(part, bool) or not isinstance(part, (str, int)) or part == "":
            raise TypeError("path parts must be non-empty strings or integers")
    return path


def _path_text(path_parts: tuple[str | int, ...]) -> str:
    rendered = "metadata"
    for part in path_parts:
        if isinstance(part, int):
            rendered += f"[{part}]"
        else:
            rendered += f".{part}"
    return rendered


def _object_payload(payload: Mapping[str, Any], field_name: str) -> dict[str, JsonValue]:
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must normalize to a JSON object")
    return normalized
