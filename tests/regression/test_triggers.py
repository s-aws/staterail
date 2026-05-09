from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import EventType, TriggerRelation
from triggers.rules import MessageTrigger, TimeTrigger, TriggerEngine


def test_time_trigger_fires_once_when_relation_matches(workspace_tmp_path):
    target = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(target))
    triggers = TriggerEngine(clock=FixedClock(target))
    triggers.register(TimeTrigger("noon", TriggerRelation.ON, target))
    core = AuditCore(ledger, triggers=triggers)

    core.emit_due_time_triggers()
    core.emit_due_time_triggers()

    trigger_records = [
        record for record in ledger.iter_records() if record.event_type == EventType.TRIGGER_FIRED
    ]
    assert len(trigger_records) == 1
    assert trigger_records[0].payload["trigger_id"] == "noon"
    assert trigger_records[0].payload["target_time"] == target.isoformat()


def test_time_trigger_can_fire_without_a_matched_event(workspace_tmp_path):
    target = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(target))
    triggers = TriggerEngine(clock=FixedClock(target + timedelta(seconds=5)))
    triggers.register(TimeTrigger("after-noon", TriggerRelation.AFTER, target))
    core = AuditCore(ledger, triggers=triggers)

    fired_records = core.emit_due_time_triggers()
    second_poll = core.emit_due_time_triggers()

    assert len(fired_records) == 1
    assert second_poll == ()
    assert fired_records[0].event_type == EventType.TRIGGER_FIRED
    assert fired_records[0].payload["trigger_id"] == "after-noon"
    assert fired_records[0].payload["matched_event_type"] is None
    assert fired_records[0].payload["matched_sequence"] is None
    assert fired_records[0].payload["target_time"] == target.isoformat()


def test_non_repeatable_time_trigger_does_not_refire_after_restart(workspace_tmp_path):
    target = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock = FixedClock(target + timedelta(seconds=5))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    triggers = TriggerEngine(clock=clock)
    triggers.register(TimeTrigger("after-noon", TriggerRelation.AFTER, target))
    core = AuditCore(ledger, triggers=triggers)

    assert len(core.emit_due_time_triggers()) == 1

    restarted_triggers = TriggerEngine(clock=clock)
    restarted_triggers.register(TimeTrigger("after-noon", TriggerRelation.AFTER, target))
    restarted_core = AuditCore(AuditLedger(ledger.path, clock=clock), triggers=restarted_triggers)

    assert restarted_core.emit_due_time_triggers() == ()
    assert [
        record.event_type for record in ledger.iter_records() if record.event_type == EventType.TRIGGER_FIRED
    ] == [EventType.TRIGGER_FIRED]


def test_repeatable_time_trigger_remains_active_after_restart(workspace_tmp_path):
    target = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock = FixedClock(target + timedelta(seconds=5))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    triggers = TriggerEngine(clock=clock)
    triggers.register(TimeTrigger("after-noon", TriggerRelation.AFTER, target, repeatable=True))
    core = AuditCore(ledger, triggers=triggers)

    assert len(core.emit_due_time_triggers()) == 1

    restarted_triggers = TriggerEngine(clock=clock)
    restarted_triggers.register(TimeTrigger("after-noon", TriggerRelation.AFTER, target, repeatable=True))
    restarted_core = AuditCore(AuditLedger(ledger.path, clock=clock), triggers=restarted_triggers)

    assert len(restarted_core.emit_due_time_triggers()) == 1
    assert [
        record.event_type for record in ledger.iter_records() if record.event_type == EventType.TRIGGER_FIRED
    ] == [EventType.TRIGGER_FIRED, EventType.TRIGGER_FIRED]


def test_message_trigger_fires_for_matching_event_type(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    triggers = TriggerEngine()
    triggers.register(
        MessageTrigger(
            trigger_id="accepted-data",
            relation=TriggerRelation.ON,
            event_type=EventType.DATA_ACCEPTED,
        )
    )
    core = AuditCore(ledger, triggers=triggers)

    core.emit(EventType.DATA_RECEIVED, {"message_key": "BTC-PERP:1"})
    core.emit(EventType.DATA_ACCEPTED, {"message_key": "BTC-PERP:1"})

    trigger_records = [
        record for record in ledger.iter_records() if record.event_type == EventType.TRIGGER_FIRED
    ]
    assert len(trigger_records) == 1
    assert trigger_records[0].payload["matched_event_type"] == EventType.DATA_ACCEPTED.value


def test_non_repeatable_message_trigger_does_not_refire_after_restart(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    triggers = TriggerEngine()
    triggers.register(
        MessageTrigger(
            trigger_id="accepted-data",
            relation=TriggerRelation.ON,
            event_type=EventType.DATA_ACCEPTED,
            repeatable=False,
        )
    )
    core = AuditCore(ledger, triggers=triggers)

    core.emit(EventType.DATA_ACCEPTED, {"message_key": "BTC-PERP:1"})

    restarted_triggers = TriggerEngine()
    restarted_triggers.register(
        MessageTrigger(
            trigger_id="accepted-data",
            relation=TriggerRelation.ON,
            event_type=EventType.DATA_ACCEPTED,
            repeatable=False,
        )
    )
    restarted_core = AuditCore(AuditLedger(ledger.path), triggers=restarted_triggers)
    restarted_core.emit(EventType.DATA_ACCEPTED, {"message_key": "BTC-PERP:2"})

    assert [
        record.event_type for record in ledger.iter_records() if record.event_type == EventType.TRIGGER_FIRED
    ] == [EventType.TRIGGER_FIRED]


def test_before_message_trigger_is_audited_before_matched_event(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    triggers = TriggerEngine()
    triggers.register(
        MessageTrigger(
            trigger_id="before-accepted-data",
            relation=TriggerRelation.BEFORE,
            event_type=EventType.DATA_ACCEPTED,
        )
    )
    core = AuditCore(ledger, triggers=triggers)

    core.emit(EventType.DATA_RECEIVED, {"message_key": "BTC-PERP:1"})
    core.emit(EventType.DATA_ACCEPTED, {"message_key": "BTC-PERP:1"})
    records = ledger.iter_records()

    assert [record.event_type for record in records] == [
        EventType.DATA_RECEIVED,
        EventType.TRIGGER_FIRED,
        EventType.DATA_ACCEPTED,
    ]
    assert records[1].payload["relation"] == TriggerRelation.BEFORE.value
    assert records[1].payload["matched_event_type"] == EventType.DATA_ACCEPTED.value
    assert records[1].payload["matched_sequence"] == records[2].sequence


def test_message_trigger_without_event_type_matches_any_message(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    triggers = TriggerEngine()
    triggers.register(MessageTrigger(trigger_id="any-message", relation=TriggerRelation.ON))
    core = AuditCore(ledger, triggers=triggers)

    core.emit(EventType.ACTION_REQUESTED, {"action_id": "action-1"})

    records = ledger.iter_records()
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.TRIGGER_FIRED,
    ]
    assert records[1].payload["matched_event_type"] == EventType.ACTION_REQUESTED.value


def test_trigger_engine_rejects_duplicate_or_invalid_rules():
    triggers = TriggerEngine()
    triggers.register(MessageTrigger(trigger_id="unique", relation=TriggerRelation.ON))

    with pytest.raises(ValueError, match="already registered"):
        triggers.register(MessageTrigger(trigger_id="unique", relation=TriggerRelation.AFTER))

    with pytest.raises(TypeError, match="TimeTrigger or MessageTrigger"):
        triggers.register("not-a-trigger")


def test_trigger_rules_require_typed_fields():
    target = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="trigger_id"):
        MessageTrigger(trigger_id="", relation=TriggerRelation.ON)

    with pytest.raises(TypeError, match="TriggerRelation"):
        MessageTrigger(trigger_id="bad-relation", relation="on")

    with pytest.raises(TypeError, match="EventType"):
        MessageTrigger(trigger_id="bad-event-type", relation=TriggerRelation.ON, event_type="data.accepted")

    with pytest.raises(ValueError, match="tolerance"):
        TimeTrigger("bad-tolerance", TriggerRelation.ON, target, tolerance=timedelta(0))
