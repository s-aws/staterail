from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Iterator

import pytest


@pytest.fixture
def workspace_tmp_path() -> Iterator[Path]:
    base = Path.cwd() / "test_runtime"
    path = base / uuid.uuid4().hex
    path.mkdir(parents=True, exist_ok=False)
    try:
        yield path
    finally:
        target = path.resolve()
        root = base.resolve()
        if target == root or root not in target.parents:
            raise RuntimeError(f"Refusing to remove path outside test runtime: {target}")
        shutil.rmtree(target, ignore_errors=True)
