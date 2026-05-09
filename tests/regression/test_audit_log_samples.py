from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from app.ledger_health import ledger_health_payload
from app.ledger_summary import ledger_summary_payload
from audit.ledger import AuditLedger


SAMPLE_DIR = Path("docs/examples/audit-log-samples")


def test_action_handling_audit_log_sample_is_replayable():
    ledger_path = SAMPLE_DIR / "action-handling.jsonl"
    summary = ledger_summary_payload(ledger_path)

    assert summary["verified"] is True
    assert summary["action_count"] == 6
    assert summary["failed_action_count"] == 2
    assert summary["error_count"] == 1
    _assert_index_ranges(ledger_path, SAMPLE_DIR / "action-handling.index.json")


def test_runtime_replay_recovery_audit_log_sample_is_replayable():
    ledger_path = SAMPLE_DIR / "runtime-replay-recovery.jsonl"
    index = _load_index(SAMPLE_DIR / "runtime-replay-recovery.index.json")
    summary = ledger_summary_payload(ledger_path)
    health = ledger_health_payload(ledger_path)
    event_counts = Counter(record.event_type.value for record in AuditLedger(ledger_path).iter_records())

    for key, expected in index["expected_summary"].items():
        assert summary[key] == expected
    for key, expected in index["expected_event_counts"].items():
        assert event_counts[key] == expected

    assert health["verified"] == index["expected_ledger_health"]["verified"]
    assert health["status"] == index["expected_ledger_health"]["status"]
    reconciliation_check = next(check for check in health["checks"] if check["name"] == "reconciliation")
    assert reconciliation_check["status"] == index["expected_ledger_health"]["reconciliation_check_status"]
    _assert_index_ranges(ledger_path, SAMPLE_DIR / "runtime-replay-recovery.index.json")


def _assert_index_ranges(ledger_path: Path, index_path: Path) -> None:
    records = AuditLedger(ledger_path).iter_records()
    records_by_sequence = {record.sequence: record for record in records}
    index = _load_index(index_path)

    for case in index["cases"]:
        start = case["sequence_start"]
        end = case["sequence_end"]
        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start <= end
        observed_events = {
            records_by_sequence[sequence].event_type.value
            for sequence in range(start, end + 1)
        }
        assert set(case["primary_events"]).issubset(observed_events)


def _load_index(index_path: Path) -> dict[str, object]:
    return json.loads(index_path.read_text(encoding="utf-8"))
