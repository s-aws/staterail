from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from app.config_loading import load_coinbase_application_config_from_json_file
from core.enums import ConfigWizardProfile
from core.errors import ConfigError
from core.json_tools import JsonValue, normalize_json
from tools.config_template import ConfigTemplateError, ConfigTemplateRenderResult, render_config_template


InputReader = Callable[[str], str]
OutputWriter = Callable[[str], None]

CFM_PRODUCT_PLACEHOLDER = "REPLACE_WITH_CFM_PRODUCT_IDS"


class ConfigWizardError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConfigWizardProfileSpec:
    default_ledger_path: Path
    default_target_path: Path
    description: str
    next_commands: tuple[str, ...]
    product_placeholder: str | None
    profile: ConfigWizardProfile
    template_path: Path


@dataclass(frozen=True)
class ConfigWizardResult:
    config_validated: bool
    force: bool
    ledger_path: Path
    next_commands: tuple[str, ...]
    profile: ConfigWizardProfile
    profile_description: str
    render_result: ConfigTemplateRenderResult
    secrets_written: bool
    target_path: Path
    template_path: Path

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "config_validated": self.config_validated,
            "force": self.force,
            "ledger_path": self.ledger_path.as_posix(),
            "next_commands": list(self.next_commands),
            "profile": self.profile.value,
            "profile_description": self.profile_description,
            "render": self.render_result.to_payload(),
            "secrets_written": self.secrets_written,
            "target_path": self.target_path.as_posix(),
            "template_path": self.template_path.as_posix(),
        }


PROFILE_SPECS: dict[ConfigWizardProfile, ConfigWizardProfileSpec] = {
    ConfigWizardProfile.DRY_RUN: ConfigWizardProfileSpec(
        default_ledger_path=Path("data/operator-dry-run-audit.jsonl"),
        default_target_path=Path("config.local.json"),
        description="Local dry-run runtime with no Coinbase credentials or live order capability.",
        next_commands=(
            "python -m app.main --config-file {target} --max-cycles 1",
            "python -m app.main --config-file {target} --ledger-health --ledger-health-fail-on-attention",
        ),
        product_placeholder=None,
        profile=ConfigWizardProfile.DRY_RUN,
        template_path=Path("docs/examples/config.dry-run.json"),
    ),
    ConfigWizardProfile.COINBASE_CFM_NO_ORDER: ConfigWizardProfileSpec(
        default_ledger_path=Path("data/operator-cfm-no-order-audit.jsonl"),
        default_target_path=Path("config.local.json"),
        description="Coinbase CFM live-mode no-order checks with product catalog, feeds, and exchange-state smoke.",
        next_commands=(
            "python -m app.main --config-file {target} --readiness --readiness-fail-on-attention",
            "python -m app.main --config-file {target} --live-no-order-preflight "
            "--live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention",
            "python -m app.main --config-file {target} --ledger-health --ledger-health-fail-on-attention",
        ),
        product_placeholder=CFM_PRODUCT_PLACEHOLDER,
        profile=ConfigWizardProfile.COINBASE_CFM_NO_ORDER,
        template_path=Path("docs/examples/config.cfm-live.json"),
    ),
    ConfigWizardProfile.COINBASE_CFM_POLICY_PROBE: ConfigWizardProfileSpec(
        default_ledger_path=Path("data/operator-cfm-policy-probe-audit.jsonl"),
        default_target_path=Path("config.local.json"),
        description="Coinbase CFM no-order policy visibility check through the strategy harness.",
        next_commands=(
            "python -m app.main --config-file {target} --live-no-order-preflight "
            "--live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention",
            "python -m app.main --config-file {target} --strategy-simulate --strategy-simulate-record-result "
            "--strategy-simulate-fail-on-attention",
            "python -m app.main --config-file {target} --live-runtime-gate --live-runtime-gate-fail-on-attention",
        ),
        product_placeholder=CFM_PRODUCT_PLACEHOLDER,
        profile=ConfigWizardProfile.COINBASE_CFM_POLICY_PROBE,
        template_path=Path("docs/examples/config.cfm-policy-probe.json"),
    ),
    ConfigWizardProfile.COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING: ConfigWizardProfileSpec(
        default_ledger_path=Path("data/operator-cfm-passive-market-making-audit.jsonl"),
        default_target_path=Path("config.local.json"),
        description="Coinbase CFM staged-only passive market-making; release stays disabled.",
        next_commands=(
            "python -m app.main --config-file {target} --live-no-order-preflight "
            "--live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention",
            "python -m app.main --config-file {target} --strategy-simulate --strategy-simulate-record-result "
            "--strategy-simulate-fail-on-attention",
            "python -m app.main --config-file {target} --live-runtime-gate --live-runtime-gate-fail-on-attention",
            "python -m app.main --config-file {target} --stop-after-task strategies.evaluate "
            "--stop-after-task-count 1 --runtime-fail-on-attention",
        ),
        product_placeholder=CFM_PRODUCT_PLACEHOLDER,
        profile=ConfigWizardProfile.COINBASE_CFM_STAGED_PASSIVE_MARKET_MAKING,
        template_path=Path("docs/examples/config.cfm-passive-market-making.json"),
    ),
    ConfigWizardProfile.COINBASE_CFM_STAGED_RELEASE_MANAGER: ConfigWizardProfileSpec(
        default_ledger_path=Path("data/operator-cfm-staged-release-audit.jsonl"),
        default_target_path=Path("config.local.json"),
        description="Advanced Coinbase CFM staged-release manager; only use after staged placements are reviewed.",
        next_commands=(
            "python -m app.main --config-file {target} --strategy-simulate --strategy-simulate-record-result "
            "--strategy-simulate-fail-on-attention",
            "python -m app.main --config-file {target} --live-runtime-gate --live-runtime-gate-fail-on-attention",
            "python -m app.main --config-file {target} --stop-after-task strategies.evaluate "
            "--stop-after-task-count 1 --runtime-fail-on-attention",
        ),
        product_placeholder=CFM_PRODUCT_PLACEHOLDER,
        profile=ConfigWizardProfile.COINBASE_CFM_STAGED_RELEASE_MANAGER,
        template_path=Path("docs/examples/config.cfm-passive-market-making-release.json"),
    ),
}


def run_config_wizard(
    *,
    force: bool = False,
    input_reader: InputReader = input,
    ledger_path: Path | str | None = None,
    no_input: bool = False,
    output_writer: OutputWriter | None = None,
    products: Sequence[str] | str | None = None,
    profile: ConfigWizardProfile | str | None = None,
    target_path: Path | str | None = None,
) -> ConfigWizardResult:
    writer = output_writer or (lambda message: print(message))
    selected_profile = _resolve_profile(profile, no_input=no_input, input_reader=input_reader, output_writer=writer)
    spec = PROFILE_SPECS[selected_profile]
    resolved_target = _resolve_path(
        target_path,
        default=spec.default_target_path,
        label="Target config path",
        no_input=no_input,
        input_reader=input_reader,
        output_writer=writer,
    )
    resolved_ledger = _resolve_path(
        ledger_path,
        default=spec.default_ledger_path,
        label="Ledger path",
        no_input=no_input,
        input_reader=input_reader,
        output_writer=writer,
    )
    replacements: dict[str, JsonValue] = {}
    if spec.product_placeholder is not None:
        replacements[spec.product_placeholder] = _resolve_products(
            products,
            no_input=no_input,
            input_reader=input_reader,
            output_writer=writer,
        )

    try:
        render_result = render_config_template(
            spec.template_path,
            resolved_target,
            force=force,
            ledger_path=resolved_ledger,
            replacements=replacements,
        )
        load_coinbase_application_config_from_json_file(resolved_target)
    except (ConfigError, ConfigTemplateError) as exc:
        raise ConfigWizardError(str(exc)) from exc

    commands = tuple(command.format(target=resolved_target.as_posix()) for command in spec.next_commands)
    return ConfigWizardResult(
        config_validated=True,
        force=force,
        ledger_path=resolved_ledger,
        next_commands=commands,
        profile=selected_profile,
        profile_description=spec.description,
        render_result=render_result,
        secrets_written=False,
        target_path=resolved_target,
        template_path=spec.template_path,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Interactively create a validated local StateRail config from checked templates."
    )
    parser.add_argument(
        "--profile",
        choices=[profile.value for profile in ConfigWizardProfile],
        default=None,
        help="Config profile to render. Omit for interactive selection.",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Target config path. Defaults to the selected profile's local config path.",
    )
    parser.add_argument(
        "--ledger-path",
        default=None,
        help="Ledger path to write into the rendered config.",
    )
    parser.add_argument(
        "--products",
        default=None,
        help="Comma-separated product IDs for profiles that require a product scope.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the target config if it already exists.",
    )
    parser.add_argument(
        "--no-input",
        action="store_true",
        help="Fail instead of prompting when required values are missing.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of the operator summary.",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="List available wizard profiles and exit.",
    )
    args = parser.parse_args(argv)

    if args.list_profiles:
        payload = _profiles_payload()
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_profiles()
        return 0

    try:
        result = run_config_wizard(
            force=args.force,
            ledger_path=args.ledger_path,
            no_input=args.no_input,
            products=args.products,
            profile=args.profile,
            target_path=args.target,
        )
    except ConfigWizardError as exc:
        print(f"Config wizard failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result.to_payload(), indent=2, sort_keys=True))
    else:
        _print_summary(result)
    return 0


def _resolve_profile(
    profile: ConfigWizardProfile | str | None,
    *,
    input_reader: InputReader,
    no_input: bool,
    output_writer: OutputWriter,
) -> ConfigWizardProfile:
    if profile is not None:
        return _profile_from_value(profile)
    if no_input:
        raise ConfigWizardError("--profile is required with --no-input")

    _print_profiles(output_writer)
    raw_value = input_reader("Select profile number or id: ").strip()
    if not raw_value:
        raise ConfigWizardError("profile selection is required")
    if raw_value.isdigit():
        index = int(raw_value)
        profiles = tuple(PROFILE_SPECS)
        if index < 1 or index > len(profiles):
            raise ConfigWizardError(f"profile number must be between 1 and {len(profiles)}")
        return profiles[index - 1]
    return _profile_from_value(raw_value)


def _profile_from_value(value: ConfigWizardProfile | str) -> ConfigWizardProfile:
    if isinstance(value, ConfigWizardProfile):
        return value
    try:
        return ConfigWizardProfile(value)
    except ValueError as exc:
        raise ConfigWizardError(f"unknown profile: {value}") from exc


def _resolve_path(
    value: Path | str | None,
    *,
    default: Path,
    input_reader: InputReader,
    label: str,
    no_input: bool,
    output_writer: OutputWriter,
) -> Path:
    if value is not None:
        return Path(value)
    if no_input:
        return default
    raw_value = input_reader(f"{label} [{default.as_posix()}]: ").strip()
    resolved = Path(raw_value) if raw_value else default
    output_writer(f"{label}: {resolved.as_posix()}")
    return resolved


def _resolve_products(
    value: Sequence[str] | str | None,
    *,
    input_reader: InputReader,
    no_input: bool,
    output_writer: OutputWriter,
) -> list[JsonValue]:
    if value is not None:
        return _parse_products(value)
    if no_input:
        raise ConfigWizardError("--products is required for the selected profile with --no-input")
    raw_value = input_reader("Product IDs, comma-separated: ").strip()
    products = _parse_products(raw_value)
    output_writer(f"Product IDs: {', '.join(str(product) for product in products)}")
    return products


def _parse_products(value: Sequence[str] | str) -> list[JsonValue]:
    if isinstance(value, str):
        raw_products = value.split(",")
    else:
        raw_products = list(value)
    products: list[str] = []
    seen: set[str] = set()
    for raw_product in raw_products:
        product = str(raw_product).strip()
        if not product:
            continue
        if product in seen:
            raise ConfigWizardError(f"duplicate product id: {product}")
        products.append(product)
        seen.add(product)
    if not products:
        raise ConfigWizardError("at least one product id is required")
    normalized = normalize_json(products)
    if not isinstance(normalized, list):
        raise TypeError("product ids must normalize to a list")
    return normalized


def _profiles_payload() -> dict[str, JsonValue]:
    return {
        "profiles": [
            {
                "default_ledger_path": spec.default_ledger_path.as_posix(),
                "default_target_path": spec.default_target_path.as_posix(),
                "description": spec.description,
                "profile": spec.profile.value,
                "requires_products": spec.product_placeholder is not None,
                "template_path": spec.template_path.as_posix(),
            }
            for spec in PROFILE_SPECS.values()
        ]
    }


def _print_profiles(output_writer: OutputWriter | None = None) -> None:
    writer = output_writer or (lambda message: print(message))
    writer("Available profiles:")
    for index, spec in enumerate(PROFILE_SPECS.values(), start=1):
        requires_products = " requires product IDs" if spec.product_placeholder is not None else ""
        writer(f"  {index}. {spec.profile.value} - {spec.description}{requires_products}")


def _print_summary(result: ConfigWizardResult) -> None:
    print(f"Created {result.target_path.as_posix()}")
    print(f"Profile: {result.profile.value}")
    print(f"Template: {result.template_path.as_posix()}")
    print(f"Ledger: {result.ledger_path.as_posix()}")
    print("Secrets written: no")
    print("Config validation: ok")
    print("")
    print("Next commands:")
    for command in result.next_commands:
        print(f"  {command}")
    print("")
    print("Set Coinbase credentials in the operator shell only; the wizard does not write secrets.")


if __name__ == "__main__":
    raise SystemExit(main())
