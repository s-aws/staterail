from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from audit.anchors import verify_recorded_ledger_anchor_receipts
from audit.archives import verify_recorded_ledger_archive_receipts
from audit.checkpoints import verify_recorded_ledger_checkpoints
from audit.ledger import AuditLedger, AuditRecord, LedgerState
from projections.state import SourceOfTruthProjection


@dataclass(frozen=True)
class VerifiedLedgerView:
    ledger_path: Path
    state: LedgerState
    records: tuple[AuditRecord, ...]
    audit_anchor_count: int
    audit_archive_count: int
    audit_checkpoint_count: int
    projection: SourceOfTruthProjection


def load_verified_ledger_view(path: str | Path) -> VerifiedLedgerView:
    ledger_path = Path(path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger does not exist: {ledger_path}")

    ledger = AuditLedger(ledger_path)
    audit_checkpoint_count = verify_recorded_ledger_checkpoints(ledger)
    audit_anchor_count = verify_recorded_ledger_anchor_receipts(ledger)
    audit_archive_count = verify_recorded_ledger_archive_receipts(ledger)
    snapshot = ledger.snapshot()
    state = snapshot.state
    records = snapshot.records
    projection = SourceOfTruthProjection.from_records(records)
    return VerifiedLedgerView(
        audit_anchor_count=audit_anchor_count,
        audit_archive_count=audit_archive_count,
        audit_checkpoint_count=audit_checkpoint_count,
        ledger_path=ledger_path,
        projection=projection,
        records=records,
        state=state,
    )
