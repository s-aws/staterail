from __future__ import annotations

import hashlib
from pathlib import Path

from app.ledger_view import load_verified_ledger_view
from core.enums import DigestAlgorithm
from core.json_tools import JsonValue, canonical_json, normalize_json


def ledger_export_payload(path: str | Path) -> dict[str, JsonValue]:
    view = load_verified_ledger_view(path)
    records = [record.to_dict() for record in view.records]
    payload = {
        "audit_anchor_count": view.audit_anchor_count,
        "audit_archive_count": view.audit_archive_count,
        "audit_checkpoint_count": view.audit_checkpoint_count,
        "digest_algorithm": DigestAlgorithm.SHA256,
        "export_digest": _sha256(canonical_json(records)),
        "ledger": {
            "last_hash": view.state.last_hash,
            "ledger_path": view.ledger_path.as_posix(),
            "next_sequence": view.state.next_sequence,
            "record_count": len(view.records),
            "verified": True,
        },
        "records": records,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("Ledger export payload must normalize to an object")
    return normalized


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
