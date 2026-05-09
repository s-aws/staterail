from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, RuntimeComponent, RuntimeStopReason, RuntimeTask
from projections.state import SourceOfTruthProjection
from runtime.orchestrator import RuntimeOrchestrator, ScheduledRuntimeTask


class MutableClock:
    def __init__(self, current_time: datetime) -> None:
        self.current_time = current_time

    def now(self) -> datetime:
        return self.current_time

    def advance(self, seconds: float) -> None:
        self.current_time += timedelta(seconds=seconds)


def test_runtime_orchestrator_audits_task_lifecycle_and_projection_state(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    calls: list[RuntimeTask] = []

    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=30),
                handler=lambda: calls.append(RuntimeTask.WATCHDOG) or {"findings": 0},
            ),
            ScheduledRuntimeTask(
                task_id=RuntimeTask.ORDER_RECOVERY,
                interval=timedelta(seconds=60),
                handler=lambda: calls.append(RuntimeTask.ORDER_RECOVERY) or (),
            ),
        ),
    )

    completed_cycles = asyncio.run(orchestrator.run(max_cycles=2))
    projection = SourceOfTruthProjection.from_ledger(ledger)
    records = ledger.iter_records()

    assert completed_cycles == 2
    assert calls == [RuntimeTask.WATCHDOG, RuntimeTask.ORDER_RECOVERY]
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[0].payload["component"] == RuntimeComponent.ORCHESTRATOR.value
    assert records[-1].payload["reason"] == RuntimeStopReason.MAX_CYCLES.value
    assert projection.system_stops[0].component == RuntimeComponent.ORCHESTRATOR
    assert projection.system_stops[0].completed_cycles == 2
    assert projection.system_stops[0].reason == RuntimeStopReason.MAX_CYCLES
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].started_count == 1
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].completed_count == 1
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].last_result == {"findings": 0}
    assert projection.runtime_tasks[RuntimeTask.ORDER_RECOVERY].completed_count == 1


def test_runtime_orchestrator_audits_startup_metadata(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=30),
                handler=lambda: None,
            ),
        ),
        startup_metadata={"config_fingerprint": "abc123"},
    )

    assert asyncio.run(orchestrator.run(max_cycles=1)) == 1
    projection = SourceOfTruthProjection.from_ledger(ledger)
    start_record = ledger.iter_records()[0]

    assert start_record.payload["startup_metadata"] == {"config_fingerprint": "abc123"}
    assert projection.system_starts[0].component == RuntimeComponent.ORCHESTRATOR
    assert projection.system_starts[0].startup_metadata == {"config_fingerprint": "abc123"}


def test_runtime_orchestrator_sleeps_until_task_is_due(workspace_tmp_path):
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    sleep_calls: list[float] = []
    calls: list[datetime] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        clock.advance(delay)

    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.FILL_RECONCILIATION,
                interval=timedelta(seconds=15),
                handler=lambda: calls.append(clock.now()) or (),
                run_on_start=False,
            ),
        ),
        clock=clock,
        sleep=fake_sleep,
    )

    assert asyncio.run(orchestrator.run(max_cycles=1)) == 1

    assert sleep_calls == [15.0]
    assert calls == [datetime(2026, 1, 1, 0, 0, 15, tzinfo=timezone.utc)]


def test_runtime_orchestrator_logs_task_errors_and_continues(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    calls: list[RuntimeTask] = []

    def failing_handler() -> None:
        calls.append(RuntimeTask.WATCHDOG)
        raise RuntimeError("watchdog failed")

    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=30),
                handler=failing_handler,
            ),
            ScheduledRuntimeTask(
                task_id=RuntimeTask.EXCHANGE_STATE_RECONCILIATION,
                interval=timedelta(seconds=30),
                handler=lambda: calls.append(RuntimeTask.EXCHANGE_STATE_RECONCILIATION) or {"drift_count": 0},
            ),
        ),
    )

    assert asyncio.run(orchestrator.run(max_cycles=2)) == 2
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert calls == [RuntimeTask.WATCHDOG, RuntimeTask.EXCHANGE_STATE_RECONCILIATION]
    assert projection.error_count == 1
    error_record = next(record for record in ledger.iter_records() if record.event_type == EventType.ERROR)
    assert error_record.payload["error_category"] == ErrorCategory.RUNTIME_TASK.value
    assert error_record.payload["error_code"] == ErrorCode.RUNTIME_TASK_FAILED.value
    assert error_record.payload["error"]["context"]["task_id"] == RuntimeTask.WATCHDOG.value
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].started_count == 1
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].completed_count == 0
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].last_error_sequence == 3
    assert projection.runtime_tasks[RuntimeTask.EXCHANGE_STATE_RECONCILIATION].completed_count == 1


def test_runtime_orchestrator_can_stop_after_target_task_completion(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    calls: list[RuntimeTask] = []

    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                interval=timedelta(seconds=30),
                handler=lambda: calls.append(RuntimeTask.PRODUCT_CATALOG_REFRESH) or {"product_count": 1},
            ),
            ScheduledRuntimeTask(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=30),
                handler=lambda: calls.append(RuntimeTask.WATCHDOG) or (),
            ),
            ScheduledRuntimeTask(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=30),
                handler=lambda: calls.append(RuntimeTask.STRATEGY_EVALUATION) or {"strategy_count": 1},
            ),
        ),
    )

    completed_cycles = asyncio.run(
        orchestrator.run_until(stop_after_task=RuntimeTask.STRATEGY_EVALUATION)
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    records = ledger.iter_records()

    assert completed_cycles == 3
    assert calls == [
        RuntimeTask.PRODUCT_CATALOG_REFRESH,
        RuntimeTask.WATCHDOG,
        RuntimeTask.STRATEGY_EVALUATION,
    ]
    assert records[-1].payload["reason"] == RuntimeStopReason.TASK_COMPLETION_TARGET.value
    assert projection.system_stops[0].reason == RuntimeStopReason.TASK_COMPLETION_TARGET
    assert projection.runtime_tasks[RuntimeTask.STRATEGY_EVALUATION].completed_count == 1


def test_runtime_orchestrator_rejects_unknown_stop_after_task(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=30),
                handler=lambda: None,
            ),
        ),
    )

    with pytest.raises(ValueError, match="not scheduled"):
        asyncio.run(orchestrator.run_until(stop_after_task=RuntimeTask.STRATEGY_EVALUATION))


def test_runtime_orchestrator_awaits_async_handlers(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    async def async_handler() -> dict[str, int]:
        await asyncio.sleep(0)
        return {"position_snapshots": 2}

    orchestrator = RuntimeOrchestrator(
        core,
        (
            ScheduledRuntimeTask(
                task_id=RuntimeTask.EXCHANGE_STATE_RECONCILIATION,
                interval=timedelta(seconds=30),
                handler=async_handler,
            ),
        ),
    )

    assert asyncio.run(orchestrator.run(max_cycles=1)) == 1
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.runtime_tasks[
        RuntimeTask.EXCHANGE_STATE_RECONCILIATION
    ].last_result == {"position_snapshots": 2}


def test_runtime_orchestrator_rejects_duplicate_task_ids(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    task = ScheduledRuntimeTask(
        task_id=RuntimeTask.WATCHDOG,
        interval=timedelta(seconds=30),
        handler=lambda: None,
    )

    with pytest.raises(ValueError, match="unique"):
        RuntimeOrchestrator(core, (task, task))
