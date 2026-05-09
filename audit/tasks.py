from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from audit.anchors import LedgerAnchorStore, publish_recorded_ledger_checkpoint_anchor
from audit.archives import LedgerArchiveStore, publish_ledger_archive
from audit.checkpoints import record_ledger_checkpoint
from core.clock import Clock
from core.json_tools import JsonValue, normalize_json


@dataclass(frozen=True)
class AuditAnchorTask:
    ledger_path: Path
    store: LedgerAnchorStore
    clock: Clock | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_path, Path):
            raise TypeError("ledger_path must be a pathlib.Path")

    def run(self) -> dict[str, JsonValue]:
        checkpoint = record_ledger_checkpoint(self.ledger_path, clock=self.clock)
        anchor = publish_recorded_ledger_checkpoint_anchor(
            self.ledger_path,
            checkpoint,
            self.store,
            clock=self.clock,
        )
        payload = {
            "anchor_record_sequence": anchor.audit_record_sequence,
            "artifact_uri": anchor.receipt.artifact_uri,
            "checkpoint_record_sequence": checkpoint.audit_record_sequence,
            "checkpoint_through_sequence": checkpoint.checkpoint.through_sequence,
            "store_type": anchor.receipt.store_type,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Audit anchor task result must normalize to a JSON object")
        return normalized


@dataclass(frozen=True)
class AuditArchiveTask:
    ledger_path: Path
    store: LedgerArchiveStore
    clock: Clock | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_path, Path):
            raise TypeError("ledger_path must be a pathlib.Path")

    def run(self) -> dict[str, JsonValue]:
        archive = publish_ledger_archive(
            self.ledger_path,
            self.store,
            clock=self.clock,
        )
        payload = {
            "archive_record_sequence": archive.audit_record_sequence,
            "artifact_uri": archive.receipt.artifact_uri,
            "record_count": archive.receipt.record_count,
            "store_type": archive.receipt.store_type,
            "through_sequence": archive.receipt.through_sequence,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Audit archive task result must normalize to a JSON object")
        return normalized
