from __future__ import annotations

import importlib
import tomllib
from collections.abc import Callable
from pathlib import Path


def test_pyproject_exposes_expected_console_scripts():
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert metadata["project"]["name"] == "staterail"
    assert metadata["project"]["description"] == "Event-sourced execution infrastructure for auditable exchange workflows."

    scripts = metadata["project"]["scripts"]

    assert scripts == {
        "staterail": "app.main:main",
        "staterail-config-template": "tools.config_template:main",
        "staterail-config-wizard": "tools.config_wizard:main",
        "staterail-strategy-wizard": "tools.strategy_wizard:main",
    }
    for target in scripts.values():
        assert _target_callable(target)


def test_pyproject_dev_extra_contains_regression_dependencies():
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    dev_dependencies = set(metadata["project"]["optional-dependencies"]["dev"])

    assert {"pytest", "pytest-asyncio", "respx"}.issubset(dev_dependencies)


def test_pyproject_declares_apache_license_metadata():
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert metadata["project"]["license"] == "Apache-2.0"
    assert Path("LICENSE").exists()


def test_pyproject_uses_explicit_flat_layout_package_discovery():
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    package_discovery = metadata["tool"]["setuptools"]["packages"]["find"]

    assert "app*" in package_discovery["include"]
    assert "core*" in package_discovery["include"]
    assert "exchanges*" in package_discovery["include"]
    assert "strategies*" in package_discovery["include"]
    assert "data*" in package_discovery["exclude"]
    assert "pytest_tmp*" in package_discovery["exclude"]
    assert "test_runtime*" in package_discovery["exclude"]
    assert "tests*" in package_discovery["exclude"]


def test_packages_expose_pep561_type_markers():
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    package_data = metadata["tool"]["setuptools"]["package-data"]

    assert package_data["*"] == ["py.typed"]
    for package_root in (
        "actions",
        "app",
        "audit",
        "config",
        "core",
        "exchanges",
        "feeds",
        "hooks",
        "orders",
        "products",
        "projections",
        "reconciliation",
        "risk",
        "runtime",
        "strategies",
        "tools",
        "triggers",
    ):
        assert Path(package_root, "py.typed").exists()


def _target_callable(target: str) -> bool:
    module_name, separator, attribute_name = target.partition(":")
    if not separator:
        raise ValueError(f"Invalid entry point target: {target}")
    module = importlib.import_module(module_name)
    attribute = getattr(module, attribute_name)
    return isinstance(attribute, Callable)
