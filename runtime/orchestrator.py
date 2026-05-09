from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, RuntimeComponent, RuntimeStopReason, RuntimeTask
from core.errors import exception_to_error_payload
from core.json_tools import JsonValue, normalize_json


Sleep = Callable[[float], Awaitable[None]]
TaskHandler = Callable[[], Any]


@dataclass(frozen=True)
class ScheduledRuntimeTask:
    task_id: RuntimeTask
    interval: timedelta
    handler: TaskHandler
    run_on_start: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, RuntimeTask):
            raise TypeError("task_id must be a RuntimeTask")
        if self.interval <= timedelta(0):
            raise ValueError("interval must be positive")


class RuntimeOrchestrator:
    def __init__(
        self,
        core: AuditCore,
        tasks: tuple[ScheduledRuntimeTask, ...],
        *,
        clock: Clock | None = None,
        sleep: Sleep | None = None,
        startup_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not tasks:
            raise ValueError("At least one runtime task is required")
        if len({task.task_id for task in tasks}) != len(tasks):
            raise ValueError("Runtime task ids must be unique")
        self._core = core
        self._tasks = tasks
        self._clock = clock or SystemClock()
        self._sleep = sleep
        self._stop_requested = False
        self._startup_metadata = _metadata_payload(startup_metadata)

    def stop(self) -> None:
        self._stop_requested = True

    async def run(self, *, max_cycles: int | None = None) -> int:
        if max_cycles is not None and max_cycles <= 0:
            raise ValueError("max_cycles must be positive")
        return await self.run_until(max_cycles=max_cycles)

    async def run_until(
        self,
        *,
        max_cycles: int | None = None,
        stop_after_task: RuntimeTask | None = None,
        stop_after_task_count: int = 1,
    ) -> int:
        if max_cycles is not None and max_cycles <= 0:
            raise ValueError("max_cycles must be positive")
        if stop_after_task is not None and not isinstance(stop_after_task, RuntimeTask):
            raise TypeError("stop_after_task must be a RuntimeTask")
        if stop_after_task_count <= 0:
            raise ValueError("stop_after_task_count must be positive")
        if stop_after_task is not None and stop_after_task not in {task.task_id for task in self._tasks}:
            raise ValueError(f"stop_after_task is not scheduled: {stop_after_task.value}")

        started_at = _utc(self._clock.now())
        self._core.emit(EventType.SYSTEM_STARTED, self._system_started_payload(started_at))

        completed_cycles = 0
        stop_reason = RuntimeStopReason.STOP_REQUESTED
        next_due = self._initial_due_times(started_at)
        task_completion_counts: dict[RuntimeTask, int] = {}
        target_reached = False
        try:
            while not self._stop_requested:
                if target_reached:
                    break
                if max_cycles is not None and completed_cycles >= max_cycles:
                    stop_reason = RuntimeStopReason.MAX_CYCLES
                    break

                due_tasks = self._due_tasks(next_due)
                if not due_tasks:
                    await self._sleep_until_next_due(next_due)
                    continue

                for task in due_tasks:
                    if self._stop_requested:
                        break
                    if max_cycles is not None and completed_cycles >= max_cycles:
                        stop_reason = RuntimeStopReason.MAX_CYCLES
                        break
                    await self._run_task(task)
                    completed_cycles += 1
                    task_completion_counts[task.task_id] = task_completion_counts.get(task.task_id, 0) + 1
                    next_due[task.task_id] = _utc(self._clock.now()) + task.interval
                    if (
                        stop_after_task is not None
                        and task.task_id == stop_after_task
                        and task_completion_counts[task.task_id] >= stop_after_task_count
                    ):
                        stop_reason = RuntimeStopReason.TASK_COMPLETION_TARGET
                        target_reached = True
                        break
        finally:
            self._core.emit(
                EventType.SYSTEM_STOPPED,
                {
                    "component": RuntimeComponent.ORCHESTRATOR.value,
                    "completed_cycles": completed_cycles,
                    "reason": stop_reason.value,
                    "stopped_at": self._clock.now(),
                },
            )
        return completed_cycles

    def _initial_due_times(self, started_at: datetime) -> dict[RuntimeTask, datetime]:
        due_times: dict[RuntimeTask, datetime] = {}
        for task in self._tasks:
            if task.run_on_start:
                due_times[task.task_id] = started_at
            else:
                due_times[task.task_id] = started_at + task.interval
        return due_times

    def _system_started_payload(self, started_at: datetime) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": started_at,
            "task_count": len(self._tasks),
        }
        if self._startup_metadata:
            payload["startup_metadata"] = self._startup_metadata
        return payload

    def _due_tasks(self, next_due: Mapping[RuntimeTask, datetime]) -> tuple[ScheduledRuntimeTask, ...]:
        now = _utc(self._clock.now())
        return tuple(task for task in self._tasks if now >= next_due[task.task_id])

    async def _sleep_until_next_due(self, next_due: Mapping[RuntimeTask, datetime]) -> None:
        now = _utc(self._clock.now())
        next_time = min(next_due.values())
        delay = max(0.0, (next_time - now).total_seconds())
        sleep = self._sleep
        if sleep is None:
            import asyncio

            sleep = asyncio.sleep
        await sleep(delay)

    async def _run_task(self, task: ScheduledRuntimeTask) -> None:
        started_record = self._core.emit(
            EventType.RUNTIME_TASK_STARTED,
            {
                "interval_seconds": task.interval.total_seconds(),
                "started_at": self._clock.now(),
                "task_id": task.task_id.value,
            },
        )
        try:
            result = task.handler()
            if inspect.isawaitable(result):
                result = await result
            self._core.emit(
                EventType.RUNTIME_TASK_COMPLETED,
                {
                    "completed_at": self._clock.now(),
                    "result": _result_payload(result),
                    "started_sequence": started_record.sequence,
                    "task_id": task.task_id.value,
                },
            )
        except Exception as exc:
            self._core.emit(
                EventType.ERROR,
                exception_to_error_payload(
                    exc,
                    category=ErrorCategory.RUNTIME_TASK,
                    context={
                        "started_sequence": started_record.sequence,
                        "task_id": task.task_id.value,
                    },
                    error_code=ErrorCode.RUNTIME_TASK_FAILED,
                ),
            )


def _result_payload(value: object) -> JsonValue:
    try:
        return normalize_json(value)
    except (TypeError, ValueError):
        return {"result_type": value.__class__.__name__}


def _metadata_payload(value: Mapping[str, Any] | None) -> dict[str, JsonValue]:
    if value is None:
        return {}
    normalized = normalize_json(value)
    if not isinstance(normalized, dict):
        raise TypeError("startup_metadata must normalize to a JSON object")
    return normalized


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
