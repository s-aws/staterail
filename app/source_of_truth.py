from __future__ import annotations

from pathlib import Path

from app.ledger_view import load_verified_ledger_view
from core.json_tools import JsonValue, normalize_json


def source_of_truth_payload(path: str | Path) -> dict[str, JsonValue]:
    view = load_verified_ledger_view(path)
    payload = {
        "audit_anchor_count": view.audit_anchor_count,
        "audit_archive_count": view.audit_archive_count,
        "audit_checkpoint_count": view.audit_checkpoint_count,
        "ledger": {
            "last_hash": view.state.last_hash,
            "ledger_path": view.ledger_path.as_posix(),
            "next_sequence": view.state.next_sequence,
            "record_count": len(view.records),
            "verified": True,
        },
        "projection": view.projection.to_payload(),
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("Source-of-truth payload must normalize to an object")
    return normalized
