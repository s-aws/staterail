from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, FeedStopReason
from feeds.router import FeedMessage, RedundantFeedRouter
from feeds.supervisor import FeedSupervisor, ReconnectPolicy


class ScriptedFeedSource:
    def __init__(self, source_id: str, scripts: list[list[FeedMessage | Exception]]) -> None:
        self._source_id = source_id
        self._scripts = scripts

    @property
    def source_id(self) -> str:
        return self._source_id

    async def stream(self) -> AsyncIterator[FeedMessage]:
        script = self._scripts.pop(0) if self._scripts else []
        for item in script:
            await asyncio.sleep(0)
            if isinstance(item, Exception):
                raise item
            yield item


async def no_sleep(delay_seconds: float) -> None:
    assert delay_seconds >= 0


def test_reconnect_policy_rejects_invalid_values():
    with pytest.raises(ValueError, match="initial_delay_seconds"):
        ReconnectPolicy(initial_delay_seconds=-1)

    with pytest.raises(ValueError, match="max_delay_seconds"):
        ReconnectPolicy(max_delay_seconds=-1)

    with pytest.raises(ValueError, match="multiplier"):
        ReconnectPolicy(multiplier=0)


def test_feed_supervisor_runs_redundant_sources_without_duplicate_accepts(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)
    message_key = "BTC-PERP:book:100"
    supervisor = FeedSupervisor(
        core,
        router,
        [
            ScriptedFeedSource(
                "coinbase-primary",
                [[FeedMessage("coinbase-primary", message_key, EventType.DATA_RECEIVED, {"sequence": 100})]],
            ),
            ScriptedFeedSource(
                "coinbase-secondary",
                [[FeedMessage("coinbase-secondary", message_key, EventType.DATA_RECEIVED, {"sequence": 100})]],
            ),
        ],
    )

    asyncio.run(supervisor.run(max_attempts_per_source=1))

    event_types = [record.event_type for record in ledger.iter_records()]
    assert event_types.count(EventType.FEED_CONNECTED) == 2
    assert event_types.count(EventType.DATA_ACCEPTED) == 1
    assert event_types.count(EventType.DATA_DUPLICATE) == 1
    assert event_types.count(EventType.FEED_DISCONNECTED) == 2


def test_feed_supervisor_logs_errors_and_reconnects(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)
    source = ScriptedFeedSource(
        "coinbase-primary",
        [
            [RuntimeError("socket dropped")],
            [FeedMessage("coinbase-primary", "ETH-PERP:ticker:1", EventType.DATA_RECEIVED, {"price": "1000"})],
        ],
    )
    supervisor = FeedSupervisor(
        core,
        router,
        [source],
        reconnect_policy=ReconnectPolicy(initial_delay_seconds=0, max_delay_seconds=0),
        sleep=no_sleep,
    )

    asyncio.run(supervisor.run(max_attempts_per_source=2))

    records = ledger.iter_records()
    event_types = [record.event_type for record in records]
    assert EventType.ERROR in event_types
    assert EventType.FEED_RECONNECT_SCHEDULED in event_types
    assert EventType.DATA_ACCEPTED in event_types
    assert any(record.payload.get("exception_type") == "RuntimeError" for record in records)
    error_record = next(record for record in records if record.event_type == EventType.ERROR)
    assert error_record.payload["error_category"] == ErrorCategory.FEED_SOURCE.value
    assert error_record.payload["error_code"] == ErrorCode.FEED_SOURCE_FAILED.value
    assert error_record.payload["retryable"] is True


def test_feed_supervisor_reconnects_source_after_sequence_anomaly(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)
    source = ScriptedFeedSource(
        "coinbase-primary",
        [
            [
                FeedMessage(
                    "coinbase-primary",
                    "coinbase:sequence-gap:connection:l2_data:1:3",
                    EventType.DATA_SEQUENCE_GAP,
                    {
                        "channel": "l2_data",
                        "gap_size": 1,
                        "observed_sequence": 3,
                        "previous_sequence": 1,
                        "track_key": "connection:l2_data",
                    },
                )
            ],
            [FeedMessage("coinbase-primary", "ETH-PERP:ticker:4", EventType.DATA_RECEIVED, {"price": "1000"})],
        ],
    )
    supervisor = FeedSupervisor(
        core,
        router,
        [source],
        reconnect_policy=ReconnectPolicy(initial_delay_seconds=0, max_delay_seconds=0),
        sleep=no_sleep,
    )

    asyncio.run(supervisor.run(max_attempts_per_source=2))

    records = ledger.iter_records()
    event_types = [record.event_type for record in records]
    disconnect = next(
        record
        for record in records
        if record.event_type == EventType.FEED_DISCONNECTED
        and record.payload["reason"] == FeedStopReason.SEQUENCE_ANOMALY.value
    )

    assert disconnect.payload["source_id"] == "coinbase-primary"
    assert EventType.FEED_RECONNECT_SCHEDULED in event_types
    assert event_types.count(EventType.DATA_ACCEPTED) == 2


def test_feed_supervisor_rejects_source_id_mismatch(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)
    supervisor = FeedSupervisor(
        core,
        router,
        [
            ScriptedFeedSource(
                "coinbase-primary",
                [
                    [
                        FeedMessage(
                            "coinbase-secondary",
                            "SOL-PERP:ticker:1",
                            EventType.DATA_RECEIVED,
                            {"price": "50"},
                        ),
                        FeedMessage(
                            "coinbase-primary",
                            "SOL-PERP:ticker:2",
                            EventType.DATA_RECEIVED,
                            {"price": "51"},
                        ),
                    ],
                ],
            )
        ],
    )

    asyncio.run(supervisor.run(max_attempts_per_source=1))

    records = ledger.iter_records()
    assert [record.event_type for record in records].count(EventType.DATA_RECEIVED) == 0
    assert any(
        record.event_type == EventType.ERROR
        and record.payload["reason"] == FeedStopReason.SOURCE_MISMATCH.value
        and record.payload["error_code"] == ErrorCode.FEED_SOURCE_MISMATCH.value
        for record in records
    )


def test_feed_supervisor_rejects_duplicate_source_ids(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)

    with pytest.raises(ValueError, match="unique"):
        FeedSupervisor(
            core,
            router,
            [
                ScriptedFeedSource("coinbase-primary", [[]]),
                ScriptedFeedSource("coinbase-primary", [[]]),
            ],
        )


def test_feed_supervisor_rejects_sources_not_expected_by_router(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core, expected_source_ids=("coinbase-primary",))

    with pytest.raises(ValueError, match="not expected"):
        FeedSupervisor(
            core,
            router,
            [ScriptedFeedSource("coinbase-secondary", [[]])],
        )
