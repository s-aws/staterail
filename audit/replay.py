from __future__ import annotations

from collections.abc import Callable

from audit.ledger import AuditLedger, AuditRecord


class ReplayEngine:
    def __init__(self, ledger: AuditLedger) -> None:
        self._ledger = ledger

    def replay(
        self,
        handler: Callable[[AuditRecord], None],
        *,
        from_sequence: int = 1,
    ) -> int:
        replayed = 0
        for record in self._ledger.iter_records():
            if record.sequence < from_sequence:
                continue
            handler(record)
            replayed += 1
        return replayed

