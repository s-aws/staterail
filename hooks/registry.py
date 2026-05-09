from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import RLock
from typing import Any

from audit.ledger import AuditRecord
from core.enums import ErrorCategory, ErrorCode, EventType, HookPoint
from core.errors import error_event_payload
from core.json_tools import JsonValue


@dataclass(frozen=True)
class HookContext:
    point: HookPoint
    event_type: EventType
    payload: Mapping[str, Any]
    record: AuditRecord | None = None


@dataclass(frozen=True)
class HookFailure:
    point: HookPoint
    hook_name: str
    exception_type: str
    message: str
    record_sequence: int | None

    def to_payload(self) -> dict[str, JsonValue]:
        return error_event_payload(
            category=ErrorCategory.HOOK,
            context={
                "hook_name": self.hook_name,
                "point": self.point.value,
                "record_sequence": self.record_sequence,
            },
            error_code=ErrorCode.HOOK_FAILED,
            exception_type=self.exception_type,
            message=self.message,
        )


Hook = Callable[[HookContext], None]


class HookRegistry:
    def __init__(self) -> None:
        self._hooks: dict[HookPoint, list[Hook]] = {point: [] for point in HookPoint}
        self._lock = RLock()

    def register(self, point: HookPoint, hook: Hook) -> None:
        if not isinstance(point, HookPoint):
            raise TypeError("point must be a HookPoint")
        if not callable(hook):
            raise TypeError("hook must be callable")
        with self._lock:
            if hook in self._hooks[point]:
                raise ValueError(f"hook is already registered for {point.value}")
            self._hooks[point].append(hook)

    def run(self, point: HookPoint, context: HookContext) -> list[HookFailure]:
        if not isinstance(point, HookPoint):
            raise TypeError("point must be a HookPoint")

        failures: list[HookFailure] = []
        with self._lock:
            hooks = tuple(self._hooks[point])

        for hook in hooks:
            try:
                hook(context)
            except Exception as exc:  # Hook errors are data; the core keeps moving and audits them.
                failures.append(
                    HookFailure(
                        point=point,
                        hook_name=getattr(hook, "__name__", hook.__class__.__name__),
                        exception_type=exc.__class__.__name__,
                        message=str(exc),
                        record_sequence=context.record.sequence if context.record else None,
                    )
                )
        return failures

    def snapshot(self) -> dict[HookPoint, tuple[str, ...]]:
        with self._lock:
            return {
                point: tuple(getattr(hook, "__name__", hook.__class__.__name__) for hook in hooks)
                for point, hooks in self._hooks.items()
            }
