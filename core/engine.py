from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from audit.ledger import AuditLedger, AuditRecord
from core.enums import EventType, HookPoint
from core.json_tools import normalize_json
from hooks.registry import HookContext, HookFailure, HookRegistry
from triggers.rules import TriggerEngine


class AuditCore:
    """Small immutable center: append facts, run extensions, audit failures."""

    def __init__(
        self,
        ledger: AuditLedger,
        hooks: HookRegistry | None = None,
        triggers: TriggerEngine | None = None,
    ) -> None:
        self._ledger = ledger
        self._hooks = hooks or HookRegistry()
        self._triggers = triggers
        self._logging_failure = False
        if self._triggers is not None:
            self._triggers.seed_fired_from_records(self._ledger.iter_records())

    @property
    def ledger(self) -> AuditLedger:
        return self._ledger

    def emit(self, event_type: EventType, payload: Mapping[str, Any] | None = None) -> AuditRecord:
        return self._emit(event_type, payload or {}, run_extensions=True)

    def emit_due_time_triggers(self) -> tuple[AuditRecord, ...]:
        if self._triggers is None:
            return ()
        return tuple(
            self._emit(EventType.TRIGGER_FIRED, decision.to_payload(), run_extensions=False)
            for decision in self._triggers.evaluate_time()
        )

    def _emit(
        self,
        event_type: EventType,
        payload: Mapping[str, Any],
        *,
        run_extensions: bool,
    ) -> AuditRecord:
        if not isinstance(event_type, EventType):
            raise TypeError("event_type must be an EventType")

        hook_payload = _immutable_payload(payload)
        failures: list[HookFailure] = []
        if run_extensions:
            failures.extend(
                self._hooks.run(
                    HookPoint.BEFORE_APPEND,
                    HookContext(point=HookPoint.BEFORE_APPEND, event_type=event_type, payload=hook_payload),
                )
            )
            self._emit_before_trigger_records(event_type)

        record = self._ledger.append(event_type, payload)

        if run_extensions:
            context = HookContext(
                point=HookPoint.AFTER_APPEND,
                event_type=event_type,
                payload=_immutable_payload(record.payload if isinstance(record.payload, Mapping) else {}),
                record=_immutable_record(record),
            )
            failures.extend(self._hooks.run(HookPoint.AFTER_APPEND, context))
            self._emit_trigger_records(record)
            self._emit_hook_failures(failures)

        return record

    def _emit_before_trigger_records(self, event_type: EventType) -> None:
        if self._triggers is None:
            return

        next_sequence = self._ledger.verify().next_sequence
        for decision in self._triggers.evaluate_before_append(event_type, next_sequence=next_sequence):
            self._emit(EventType.TRIGGER_FIRED, decision.to_payload(), run_extensions=False)

    def _emit_trigger_records(self, record: AuditRecord) -> None:
        if self._triggers is None:
            return

        for decision in self._triggers.evaluate(record):
            self._emit(EventType.TRIGGER_FIRED, decision.to_payload(), run_extensions=False)

    def _emit_hook_failures(self, failures: list[HookFailure]) -> None:
        if not failures or self._logging_failure:
            return

        self._logging_failure = True
        try:
            for failure in failures:
                self._emit(EventType.ERROR, failure.to_payload(), run_extensions=False)
        finally:
            self._logging_failure = False


def _immutable_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("payload must normalize to a JSON object")
    return _freeze_json(normalized)


def _immutable_record(record: AuditRecord) -> AuditRecord:
    return replace(record, payload=_freeze_json(record.payload))


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value
