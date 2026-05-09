from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.json_tools import JsonValue, normalize_json


class ConfigTemplateError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConfigTemplateRenderResult:
    source_path: Path
    target_path: Path
    replacement_count: int
    ledger_path_overridden: bool
    unresolved_placeholders: tuple[str, ...]

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "ledger_path_overridden": self.ledger_path_overridden,
            "replacement_count": self.replacement_count,
            "source_path": self.source_path.as_posix(),
            "target_path": self.target_path.as_posix(),
            "unresolved_placeholders": list(self.unresolved_placeholders),
        }


def render_config_template(
    source_path: Path | str,
    target_path: Path | str,
    *,
    replacements: Mapping[str, JsonValue],
    force: bool = False,
    ledger_path: Path | str | None = None,
    require_no_unresolved_placeholders: bool = True,
) -> ConfigTemplateRenderResult:
    source = Path(source_path)
    target = Path(target_path)
    if not source.exists() or not source.is_file():
        raise ConfigTemplateError(f"source template must be an existing file: {source}")
    if target.exists() and not force:
        raise ConfigTemplateError(f"target already exists: {target}")
    for key, value in replacements.items():
        if not isinstance(key, str) or not key:
            raise ConfigTemplateError("replacement keys must be non-empty strings")
        normalize_json(value)
        if value == "":
            raise ConfigTemplateError("replacement values must not be empty strings")

    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigTemplateError(f"source template must be valid JSON: {source}") from exc

    rendered, replacement_count = _replace_placeholders(raw, replacements)
    if ledger_path is not None:
        if not isinstance(rendered, dict):
            raise ConfigTemplateError("rendered config must be a JSON object to set ledger_path")
        rendered["ledger_path"] = Path(ledger_path).as_posix()

    normalized = normalize_json(rendered)
    unresolved = tuple(_unresolved_placeholders(normalized))
    if require_no_unresolved_placeholders and unresolved:
        joined = ", ".join(unresolved)
        raise ConfigTemplateError(f"unresolved placeholder value(s): {joined}")

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(normalized, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ConfigTemplateRenderResult(
        replacement_count=replacement_count,
        ledger_path_overridden=ledger_path is not None,
        source_path=source,
        target_path=target,
        unresolved_placeholders=unresolved,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render a checked-in JSON config template into a local operator config."
    )
    parser.add_argument("source", help="JSON template file to render.")
    parser.add_argument("target", help="Target JSON config file to write.")
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        dest="replacement_pairs",
        metavar="PLACEHOLDER=VALUE",
        help="Placeholder replacement. Repeat for multiple placeholders.",
    )
    parser.add_argument(
        "--set-json",
        action="append",
        default=[],
        dest="json_replacement_pairs",
        metavar="PLACEHOLDER=JSON",
        help=(
            "JSON placeholder replacement. Lists splice into template lists when the "
            "placeholder is a list item. Repeat for multiple placeholders."
        ),
    )
    parser.add_argument(
        "--ledger-path",
        default=None,
        help="Optional ledger_path value to write into the rendered config.",
    )
    parser.add_argument(
        "--allow-unresolved-placeholders",
        action="store_true",
        help="Write the config even when REPLACE_WITH_ placeholder values remain.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the target file if it already exists.",
    )
    args = parser.parse_args(argv)

    try:
        replacements = _replacement_mapping(args.replacement_pairs, args.json_replacement_pairs)
        result = render_config_template(
            args.source,
            args.target,
            force=args.force,
            ledger_path=args.ledger_path,
            replacements=replacements,
            require_no_unresolved_placeholders=not args.allow_unresolved_placeholders,
        )
    except ConfigTemplateError as exc:
        print(f"Config template render failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result.to_payload(), indent=2, sort_keys=True))
    return 0


def _replacement_mapping(
    string_pairs: Sequence[str],
    json_pairs: Sequence[str] = (),
) -> dict[str, JsonValue]:
    replacements: dict[str, JsonValue] = {}
    for pair in string_pairs:
        key, separator, value = pair.partition("=")
        if not separator or not key or not value:
            raise ConfigTemplateError("--set values must use PLACEHOLDER=VALUE")
        if key in replacements:
            raise ConfigTemplateError(f"duplicate replacement key: {key}")
        replacements[key] = value
    for pair in json_pairs:
        key, separator, raw_value = pair.partition("=")
        if not separator or not key or not raw_value:
            raise ConfigTemplateError("--set-json values must use PLACEHOLDER=JSON")
        if key in replacements:
            raise ConfigTemplateError(f"duplicate replacement key: {key}")
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ConfigTemplateError(f"--set-json value for {key} must be valid JSON") from exc
        replacements[key] = normalize_json(parsed)
    return replacements


def _replace_placeholders(value: Any, replacements: Mapping[str, JsonValue]) -> tuple[Any, int]:
    if isinstance(value, dict):
        rendered: dict[str, Any] = {}
        count = 0
        for key, item in value.items():
            rendered_item, item_count = _replace_placeholders(item, replacements)
            rendered[key] = rendered_item
            count += item_count
        return rendered, count
    if isinstance(value, list):
        rendered_list: list[Any] = []
        count = 0
        for item in value:
            rendered_item, item_count = _replace_placeholders(item, replacements)
            if item_count and isinstance(rendered_item, list) and _is_placeholder_string(item):
                rendered_list.extend(rendered_item)
            else:
                rendered_list.append(rendered_item)
            count += item_count
        return rendered_list, count
    if isinstance(value, str) and value in replacements:
        return replacements[value], 1
    return value, 0


def _is_placeholder_string(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("REPLACE_WITH_")


def _unresolved_placeholders(value: JsonValue, path: str = "$") -> list[str]:
    if isinstance(value, dict):
        unresolved: list[str] = []
        for key, item in value.items():
            unresolved.extend(_unresolved_placeholders(item, f"{path}.{key}"))
        return unresolved
    if isinstance(value, list):
        unresolved = []
        for index, item in enumerate(value):
            unresolved.extend(_unresolved_placeholders(item, f"{path}[{index}]"))
        return unresolved
    if isinstance(value, str) and value.startswith("REPLACE_WITH_"):
        return [f"{path}={value}"]
    return []


if __name__ == "__main__":
    raise SystemExit(main())
