from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


JsonValue = str | int | float | bool | None | dict[str, "JsonValue"] | list["JsonValue"]


def normalize_json(value: Any) -> JsonValue:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        timestamp = value
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc).isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return normalize_json(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): normalize_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [normalize_json(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON payload floats must be finite")
        return value
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"Unsupported JSON payload type: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        normalize_json(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )

