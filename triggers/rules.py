from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock

from audit.ledger import AuditRecord
from core.clock import Clock, SystemClock
from core.enums import EventType, TriggerRelation
from core.json_tools import JsonValue


@dataclass(frozen=True)
class TriggerDecision:
    trigger_id: str
    relation: TriggerRelation
    matched_event_type: EventType | None = None
    matched_sequence: int | None = None
    target_time: datetime | None = None

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "matched_event_type": self.matched_event_type.value if self.matched_event_type is not None else None,
            "matched_sequence": self.matched_sequence,
            "relation": self.relation.value,
            "target_time": self.target_time,
            "trigger_id": self.trigger_id,
        }


@dataclass(frozen=True)
class TimeTrigger:
    trigger_id: str
    relation: TriggerRelation
    target_time: datetime
    tolerance: timedelta = timedelta(seconds=1)
    repeatable: bool = False

    def __post_init__(self) -> None:
        if not self.trigger_id:
            raise ValueError("trigger_id is required")
        if not isinstance(self.relation, TriggerRelation):
            raise TypeError("relation must be a TriggerRelation")
        if not isinstance(self.target_time, datetime):
            raise TypeError("target_time must be a datetime")
        if self.tolerance <= timedelta(0):
            raise ValueError("tolerance must be positive")
        if not isinstance(self.repeatable, bool):
            raise TypeError("repeatable must be a bool")

    def evaluate(self, record: AuditRecord, now: datetime) -> TriggerDecision | None:
        current_time = _utc(now)
        target = _utc(self.target_time)

        if self.relation == TriggerRelation.BEFORE and current_time < target:
            return self._decision(record)
        if self.relation == TriggerRelation.ON and abs(current_time - target) <= self.tolerance:
            return self._decision(record)
        if self.relation == TriggerRelation.AFTER and current_time > target:
            return self._decision(record)
        return None

    def evaluate_time(self, now: datetime) -> TriggerDecision | None:
        current_time = _utc(now)
        target = _utc(self.target_time)

        if self.relation == TriggerRelation.BEFORE and current_time < target:
            return self._time_decision()
        if self.relation == TriggerRelation.ON and abs(current_time - target) <= self.tolerance:
            return self._time_decision()
        if self.relation == TriggerRelation.AFTER and current_time > target:
            return self._time_decision()
        return None

    def _decision(self, record: AuditRecord) -> TriggerDecision:
        return TriggerDecision(
            trigger_id=self.trigger_id,
            relation=self.relation,
            matched_event_type=record.event_type,
            matched_sequence=record.sequence,
            target_time=self.target_time,
        )

    def _time_decision(self) -> TriggerDecision:
        return TriggerDecision(
            trigger_id=self.trigger_id,
            relation=self.relation,
            target_time=self.target_time,
        )


@dataclass(frozen=True)
class MessageTrigger:
    trigger_id: str
    relation: TriggerRelation
    event_type: EventType | None = None
    repeatable: bool = True

    def __post_init__(self) -> None:
        if not self.trigger_id:
            raise ValueError("trigger_id is required")
        if not isinstance(self.relation, TriggerRelation):
            raise TypeError("relation must be a TriggerRelation")
        if self.event_type is not None and not isinstance(self.event_type, EventType):
            raise TypeError("event_type must be an EventType")
        if not isinstance(self.repeatable, bool):
            raise TypeError("repeatable must be a bool")

    def evaluate_before_append(self, event_type: EventType, matched_sequence: int) -> TriggerDecision | None:
        if self.relation != TriggerRelation.BEFORE:
            return None
        if not self.matches_event_type(event_type):
            return None
        return TriggerDecision(
            trigger_id=self.trigger_id,
            relation=self.relation,
            matched_event_type=event_type,
            matched_sequence=matched_sequence,
        )

    def evaluate(self, record: AuditRecord, now: datetime) -> TriggerDecision | None:
        del now
        if self.relation == TriggerRelation.BEFORE:
            return None
        if not self.matches_event_type(record.event_type):
            return None
        return TriggerDecision(
            trigger_id=self.trigger_id,
            relation=self.relation,
            matched_event_type=record.event_type,
            matched_sequence=record.sequence,
        )

    def matches_event_type(self, event_type: EventType) -> bool:
        return self.event_type is None or event_type == self.event_type


class TriggerEngine:
    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self._rules: list[TimeTrigger | MessageTrigger] = []
        self._fired_keys: set[str] = set()
        self._seeded_trigger_ids: set[str] = set()
        self._lock = RLock()

    def register(self, rule: TimeTrigger | MessageTrigger) -> None:
        if not isinstance(rule, (TimeTrigger, MessageTrigger)):
            raise TypeError("rule must be a TimeTrigger or MessageTrigger")
        with self._lock:
            if any(existing.trigger_id == rule.trigger_id for existing in self._rules):
                raise ValueError(f"trigger_id is already registered: {rule.trigger_id}")
            self._rules.append(rule)
            self._seed_rule_if_needed_locked(rule)

    def seed_fired_from_records(self, records: Iterable[AuditRecord]) -> None:
        with self._lock:
            for record in records:
                if record.event_type != EventType.TRIGGER_FIRED:
                    continue
                trigger_id = _string_or_none(_payload_dict(record.payload).get("trigger_id"))
                if trigger_id is not None:
                    self._seeded_trigger_ids.add(trigger_id)

            for rule in self._rules:
                self._seed_rule_if_needed_locked(rule)

    def evaluate(self, record: AuditRecord) -> tuple[TriggerDecision, ...]:
        with self._lock:
            now = self._clock.now()
            decisions: list[TriggerDecision] = []

            for rule in tuple(self._rules):
                if isinstance(rule, TimeTrigger):
                    continue
                decision = rule.evaluate(record, now)
                if decision is None:
                    continue

                fired_key = self._fired_key(rule, record)
                if fired_key in self._fired_keys:
                    continue

                self._fired_keys.add(fired_key)
                decisions.append(decision)

            return tuple(decisions)

    def evaluate_before_append(self, event_type: EventType, *, next_sequence: int) -> tuple[TriggerDecision, ...]:
        with self._lock:
            matched_rules = [
                rule
                for rule in tuple(self._rules)
                if isinstance(rule, MessageTrigger)
                and rule.relation == TriggerRelation.BEFORE
                and rule.matches_event_type(event_type)
            ]
            matched_sequence = next_sequence + len(matched_rules)
            decisions: list[TriggerDecision] = []

            for rule in matched_rules:
                decision = rule.evaluate_before_append(event_type, matched_sequence)
                if decision is None:
                    continue

                fired_key = self._before_fired_key(rule, decision)
                if fired_key in self._fired_keys:
                    continue

                self._fired_keys.add(fired_key)
                decisions.append(decision)

            return tuple(decisions)

    def evaluate_time(self) -> tuple[TriggerDecision, ...]:
        with self._lock:
            now = self._clock.now()
            decisions: list[TriggerDecision] = []

            for rule in tuple(self._rules):
                if not isinstance(rule, TimeTrigger):
                    continue
                decision = rule.evaluate_time(now)
                if decision is None:
                    continue

                fired_key = self._time_fired_key(rule, now)
                if fired_key in self._fired_keys:
                    continue

                self._fired_keys.add(fired_key)
                decisions.append(decision)

            return tuple(decisions)

    def _fired_key(self, rule: TimeTrigger | MessageTrigger, record: AuditRecord) -> str:
        if rule.repeatable:
            return f"{rule.trigger_id}:{record.sequence}:{record.record_hash}"
        return rule.trigger_id

    def _before_fired_key(self, rule: MessageTrigger, decision: TriggerDecision) -> str:
        if rule.repeatable:
            return f"{rule.trigger_id}:before:{decision.matched_sequence}"
        return rule.trigger_id

    def _time_fired_key(self, rule: TimeTrigger, now: datetime) -> str:
        if rule.repeatable:
            return f"{rule.trigger_id}:time:{_utc(now).isoformat()}"
        return rule.trigger_id

    def _seed_rule_if_needed_locked(self, rule: TimeTrigger | MessageTrigger) -> None:
        if rule.repeatable:
            return
        if rule.trigger_id in self._seeded_trigger_ids:
            self._fired_keys.add(rule.trigger_id)


def _payload_dict(payload: JsonValue) -> Mapping[str, JsonValue]:
    if isinstance(payload, dict):
        return payload
    return {}


def _string_or_none(value: JsonValue) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
