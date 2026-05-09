from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from audit.anchors import LocalFileLedgerAnchorStore, verify_recorded_ledger_anchor_receipts
from audit.ledger import AuditLedger
from audit.tasks import AuditAnchorTask
from config.assembly import (
    CoinbaseBotConfig,
    ReconciliationRuntimeConfig,
    TaskScheduleConfig,
    assemble_coinbase_runtime,
)
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import AnchorStoreType, EventType, RuntimeTask


def test_audit_anchor_task_records_checkpoint_and_anchor(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger_path = workspace_tmp_path / "audit.jsonl"
    ledger = AuditLedger(ledger_path, clock=clock)
    ledger.append(EventType.ACTION_REQUESTED, {"client_order_id": "order-1"})
    task = AuditAnchorTask(
        ledger_path,
        LocalFileLedgerAnchorStore(workspace_tmp_path / "anchors"),
        clock=clock,
    )

    result = task.run()
    records = AuditLedger(ledger_path, clock=clock).iter_records()

    assert result["checkpoint_record_sequence"] == 2
    assert result["anchor_record_sequence"] == 3
    assert result["store_type"] == AnchorStoreType.LOCAL_FILE.value
    assert Path(result["artifact_uri"]).exists()
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.AUDIT_CHECKPOINT,
        EventType.AUDIT_ANCHOR_PUBLISHED,
    ]
    assert verify_recorded_ledger_anchor_receipts(AuditLedger(ledger_path, clock=clock)) == 1


def test_assembly_runs_audit_anchor_schedule_with_injected_store(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path, clock=clock))
    config = CoinbaseBotConfig(
        audit_anchor_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.AUDIT_ANCHOR,
            interval=timedelta(hours=24),
            enabled=True,
        ),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
    )
    store = LocalFileLedgerAnchorStore(workspace_tmp_path / "anchors")

    assembly = assemble_coinbase_runtime(
        audit_anchor_store=store,
        clock=clock,
        config=config,
        core=core,
    )
    completed_cycles = asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = AuditLedger(ledger_path, clock=clock).iter_records()

    assert completed_cycles == 1
    assert assembly.audit_anchor_task is not None
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.AUDIT_CHECKPOINT,
        EventType.AUDIT_ANCHOR_PUBLISHED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[1].payload["task_id"] == RuntimeTask.AUDIT_ANCHOR.value
    assert records[4].payload["result"]["anchor_record_sequence"] == 4
    assert verify_recorded_ledger_anchor_receipts(AuditLedger(ledger_path, clock=clock)) == 1
