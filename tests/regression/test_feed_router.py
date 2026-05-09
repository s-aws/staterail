from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, FeedStatus
from feeds.router import FeedMessage, MessageDeduplicator, RedundantFeedRouter
from projections.state import SourceOfTruthProjection


def test_redundant_feed_router_audits_received_unique_and_duplicate_messages(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)
    message = FeedMessage(
        source_id="coinbase-primary",
        message_key="BTC-PERP:book:100",
        event_type=EventType.DATA_RECEIVED,
        payload={"sequence": 100},
    )

    assert router.ingest(message) is True
    assert router.ingest(message) is False

    event_types = [record.event_type for record in ledger.iter_records()]
    assert event_types == [
        EventType.DATA_RECEIVED,
        EventType.DATA_ACCEPTED,
        EventType.DATA_RECEIVED,
        EventType.DATA_DUPLICATE,
    ]


def test_redundant_feed_router_marks_stale_sources_and_audits_degradation(workspace_tmp_path):
    first_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(first_time)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core, clock=FixedClock(first_time + timedelta(minutes=1)))
    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="ETH-PERP:ticker:1",
            event_type=EventType.DATA_RECEIVED,
            payload={"price": "1000"},
            received_at=first_time,
        )
    )

    states = router.audit_health()

    assert states[0].status == FeedStatus.STALE
    assert ledger.iter_records()[-1].event_type == EventType.FEED_DEGRADED


def test_redundant_feed_router_audits_missing_expected_sources(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(
        core,
        expected_source_ids=("coinbase-primary", "coinbase-secondary"),
        min_live_sources=2,
    )

    states = router.audit_health()

    assert {state.source_id for state in states} == {"coinbase-primary", "coinbase-secondary"}
    assert all(state.status == FeedStatus.DISCONNECTED for state in states)
    degraded = ledger.iter_records()[-1]
    assert degraded.event_type == EventType.FEED_DEGRADED
    assert degraded.payload["live_count"] == 0
    assert degraded.payload["min_live_sources"] == 2
    assert degraded.payload["disconnected_sources"] == ["coinbase-primary", "coinbase-secondary"]
    assert degraded.payload["stale_sources"] == []


def test_redundant_feed_router_rejects_unexpected_source_when_expected_sources_are_configured(
    workspace_tmp_path,
):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core, expected_source_ids=("coinbase-primary",))

    accepted = router.ingest(
        FeedMessage(
            source_id="coinbase-secondary",
            message_key="ETH-PERP:ticker:1",
            event_type=EventType.DATA_RECEIVED,
            payload={"price": "1000"},
        )
    )
    records = ledger.iter_records()

    assert accepted is False
    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.FEED_SOURCE.value
    assert records[0].payload["error_code"] == ErrorCode.FEED_UNEXPECTED_SOURCE.value
    assert records[0].payload["observed_source_id"] == "coinbase-secondary"
    assert records[0].payload["expected_source_ids"] == ["coinbase-primary"]


def test_redundant_feed_router_rejects_invalid_health_configuration(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))

    with pytest.raises(ValueError, match="stale_after"):
        RedundantFeedRouter(core, stale_after=timedelta(0))

    with pytest.raises(ValueError, match="expected_source_ids"):
        RedundantFeedRouter(core, expected_source_ids=("coinbase-primary", "coinbase-primary"))

    with pytest.raises(ValueError, match="max_entries"):
        MessageDeduplicator(max_entries=0)


def test_feed_message_requires_typed_fields():
    with pytest.raises(ValueError, match="source_id"):
        FeedMessage("", "BTC-PERP:book:100", EventType.DATA_RECEIVED, {})

    with pytest.raises(ValueError, match="message_key"):
        FeedMessage("coinbase-primary", "", EventType.DATA_RECEIVED, {})

    with pytest.raises(TypeError, match="EventType"):
        FeedMessage("coinbase-primary", "BTC-PERP:book:100", "data.received", {})

    with pytest.raises(TypeError, match="mapping"):
        FeedMessage("coinbase-primary", "BTC-PERP:book:100", EventType.DATA_RECEIVED, [])


def test_redundant_feed_router_audits_heartbeats_as_feed_health(workspace_tmp_path):
    received_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core, clock=FixedClock(received_at))

    accepted = router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:heartbeats:1",
            event_type=EventType.FEED_HEARTBEAT,
            payload={"sequence": 1},
            received_at=received_at,
        )
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert accepted is True
    assert [record.event_type for record in ledger.iter_records()] == [EventType.FEED_HEARTBEAT]
    assert projection.accepted_data_count == 0
    assert projection.feed_sources["coinbase-primary"].last_seen == received_at.isoformat()


def test_redundant_feed_router_seeds_duplicate_detection_from_ledger(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)
    message_key = "BTC-PERP:book:100"
    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key=message_key,
            event_type=EventType.DATA_RECEIVED,
            payload={"sequence": 100},
        )
    )

    restarted_core = AuditCore(AuditLedger(ledger.path))
    restarted_router = RedundantFeedRouter.from_ledger(restarted_core)
    accepted = restarted_router.ingest(
        FeedMessage(
            source_id="coinbase-secondary",
            message_key=message_key,
            event_type=EventType.DATA_RECEIVED,
            payload={"sequence": 100},
        )
    )
    event_types = [record.event_type for record in restarted_core.ledger.iter_records()]

    assert accepted is False
    assert event_types.count(EventType.DATA_ACCEPTED) == 1
    assert event_types[-1] == EventType.DATA_DUPLICATE
