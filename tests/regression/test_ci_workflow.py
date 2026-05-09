from __future__ import annotations

from pathlib import Path


def test_regression_workflow_uses_windows_and_read_only_permissions():
    workflow = Path(".github/workflows/regression.yml").read_text(encoding="utf-8")

    assert "permissions:\n  contents: read\n" in workflow
    assert "runs-on: windows-latest" in workflow
    assert "uses: actions/checkout@v6" in workflow
    assert "uses: actions/setup-python@v6" in workflow
    assert 'python-version: "3.13"' in workflow
    assert 'python -m pip install -e ".[dev]"' in workflow
    assert "pytest tests/regression/ -v" in workflow
    assert "upload-artifact" not in workflow
    assert "release-validation" not in workflow
