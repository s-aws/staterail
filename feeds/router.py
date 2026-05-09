from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any

from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, FeedStatus
from core.errors import error_event_payload
from core.json_tools import JsonValue, normalize_json
from core.order_update_contract import validate_exchange_order_update


@dataclass(frozen=True)
class FeedMessage:
    source_id: str
    message_key: str
    event_type: EventType
    payload: Mapping[str, Any]
    received_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id is required")
        if not self.message_key:
            raise ValueError("message_key is required")
        if not isinstance(self.event_type, EventType):
            raise TypeError("event_type must be an EventType")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")
        if self.received_at is not None and not isinstance(self.received_at, datetime):
            raise TypeError("received_at must be a datetime")


@dataclass(frozen=True)
class FeedSourceState:
    source_id: str
    status: FeedStatus
    last_seen: datetime | None


class MessageDeduplicator:
    def __init__(self, max_entries: int = 100_000) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._lock = RLock()

    def mark_seen(self, message_key: str) -> bool:
        with self._lock:
            if message_key in self._seen:
                self._seen.move_to_end(message_key)
                return True

            self._seen[message_key] = None
            if len(self._seen) > self._max_entries:
                self._seen.popitem(last=False)
            return False

    def seed_seen(self, message_keys: Iterable[str]) -> None:
        with self._lock:
            for message_key in message_keys:
                if message_key in self._seen:
                    self._seen.move_to_end(message_key)
                    continue
                self._seen[message_key] = None
                if len(self._seen) > self._max_entries:
                    self._seen.popitem(last=False)

    @property
    def seen_count(self) -> int:
        with self._lock:
            return len(self._seen)


class RedundantFeedRouter:
    def __init__(
        self,
        core: AuditCore,
        *,
        clock: Clock | None = None,
        stale_after: timedelta = timedelta(seconds=30),
        min_live_sources: int = 1,
        deduplicator: MessageDeduplicator | None = None,
        expected_source_ids: Iterable[str] = (),
    ) -> None:
        if min_live_sources <= 0:
            raise ValueError("min_live_sources must be positive")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        expected_source_id_tuple = tuple(expected_source_ids)
        if len(expected_source_id_tuple) != len(set(expected_source_id_tuple)):
            raise ValueError("expected_source_ids must be unique")
        self._core = core
        self._clock = clock or SystemClock()
        self._stale_after = stale_after
        self._min_live_sources = min_live_sources
        self._deduplicator = deduplicator or MessageDeduplicator()
        self._expected_source_ids = frozenset(expected_source_id_tuple)
        self._sources: dict[str, FeedSourceState] = {
            source_id: FeedSourceState(source_id=source_id, status=FeedStatus.DISCONNECTED, last_seen=None)
            for source_id in expected_source_id_tuple
        }
        self._lock = RLock()

    @classmethod
    def from_ledger(
        cls,
        core: AuditCore,
        *,
        clock: Clock | None = None,
        stale_after: timedelta = timedelta(seconds=30),
        min_live_sources: int = 1,
        max_deduplication_entries: int = 100_000,
        expected_source_ids: Iterable[str] = (),
    ) -> "RedundantFeedRouter":
        from projections.state import SourceOfTruthProjection

        projection = SourceOfTruthProjection.from_ledger(core.ledger)
        deduplicator = MessageDeduplicator(max_entries=max_deduplication_entries)
        deduplicator.seed_seen(
            message.message_key
            for message in projection.data_messages.values()
            if message.accepted
        )
        return cls(
            core,
            clock=clock,
            deduplicator=deduplicator,
            expected_source_ids=expected_source_ids,
            min_live_sources=min_live_sources,
            stale_after=stale_after,
        )

    def ingest(self, message: FeedMessage) -> bool:
        if not isinstance(message.event_type, EventType):
            raise TypeError("message.event_type must be an EventType")

        received_at = _utc(message.received_at or self._clock.now())
        with self._lock:
            if not self._allows_source(message.source_id):
                self._core.emit(
                    EventType.ERROR,
                    error_event_payload(
                        category=ErrorCategory.FEED_SOURCE,
                        context={
                            "expected_source_ids": tuple(sorted(self._expected_source_ids)),
                            "message_key": message.message_key,
                            "observed_source_id": message.source_id,
                        },
                        error_code=ErrorCode.FEED_UNEXPECTED_SOURCE,
                        message="Feed router received a message from an unexpected source_id",
                    ),
                )
                return False

            self.mark_connected(message.source_id, seen_at=received_at)

            normalized_payload = normalize_json(message.payload)
            if message.event_type == EventType.EXCHANGE_ORDER_UPDATE:
                payload_dict = normalized_payload if isinstance(normalized_payload, dict) else {}
                order_payload = payload_dict.get("order")
                order = order_payload if isinstance(order_payload, dict) else {}
                contract = validate_exchange_order_update(order)
                if not contract.valid:
                    self._core.emit(
                        EventType.ERROR,
                        error_event_payload(
                            category=ErrorCategory.FEED_SOURCE,
                            context={
                                "event_type": message.event_type.value,
                                "invalid_fields": list(contract.invalid_fields),
                                "message_key": message.message_key,
                                "missing_fields": list(contract.missing_fields),
                                "order_field_present": isinstance(order_payload, dict),
                                "raw_payload": normalized_payload,
                                "source_id": message.source_id,
                            },
                            error_code=ErrorCode.EXCHANGE_ORDER_UPDATE_INVALID,
                            message="Feed router rejected an exchange order update that failed contract validation",
                        ),
                    )
                    return False

            receipt = {
                "message_event_type": message.event_type.value,
                "message_key": message.message_key,
                "payload": normalized_payload,
                "received_at": received_at.isoformat(),
                "source_id": message.source_id,
            }
            if message.event_type == EventType.FEED_HEARTBEAT:
                self._core.emit(EventType.FEED_HEARTBEAT, receipt)
                self.audit_health()
                return True

            received_record = self._core.emit(EventType.DATA_RECEIVED, receipt)

            is_duplicate = self._deduplicator.mark_seen(message.message_key)
            if is_duplicate:
                self._core.emit(
                    EventType.DATA_DUPLICATE,
                    {
                        "message_key": message.message_key,
                        "received_sequence": received_record.sequence,
                        "source_id": message.source_id,
                    },
                )
                return False

            self._core.emit(
                EventType.DATA_ACCEPTED,
                {
                    "message_event_type": message.event_type.value,
                    "message_key": message.message_key,
                    "received_sequence": received_record.sequence,
                    "source_id": message.source_id,
                },
            )
            self.audit_health()
            return True

    def mark_connected(self, source_id: str, *, seen_at: datetime | None = None) -> None:
        with self._lock:
            self._assert_source_allowed(source_id)
            self._sources[source_id] = FeedSourceState(
                source_id=source_id,
                status=FeedStatus.CONNECTED,
                last_seen=_utc(seen_at or self._clock.now()),
            )

    def register_source(self, source_id: str) -> None:
        with self._lock:
            self._assert_source_allowed(source_id)
            if source_id not in self._sources:
                self._sources[source_id] = FeedSourceState(
                    source_id=source_id,
                    status=FeedStatus.DISCONNECTED,
                    last_seen=None,
                )

    def mark_disconnected(self, source_id: str) -> None:
        with self._lock:
            self._assert_source_allowed(source_id)
            previous = self._sources.get(source_id)
            self._sources[source_id] = FeedSourceState(
                source_id=source_id,
                status=FeedStatus.DISCONNECTED,
                last_seen=previous.last_seen if previous else None,
            )

    def audit_health(self) -> tuple[FeedSourceState, ...]:
        with self._lock:
            states = self.source_states()
            live_count = sum(1 for state in states if state.status == FeedStatus.CONNECTED)
            if live_count < self._min_live_sources:
                connected_sources = tuple(state.source_id for state in states if state.status == FeedStatus.CONNECTED)
                disconnected_sources = tuple(
                    state.source_id for state in states if state.status == FeedStatus.DISCONNECTED
                )
                stale_sources = tuple(state.source_id for state in states if state.status == FeedStatus.STALE)
                self._core.emit(
                    EventType.FEED_DEGRADED,
                    {
                        "connected_sources": connected_sources,
                        "disconnected_sources": disconnected_sources,
                        "live_count": live_count,
                        "min_live_sources": self._min_live_sources,
                        "stale_sources": stale_sources,
                    },
                )
            return states

    def source_states(self) -> tuple[FeedSourceState, ...]:
        now = _utc(self._clock.now())
        states: list[FeedSourceState] = []
        for source_id, state in self._sources.items():
            if state.status == FeedStatus.DISCONNECTED:
                status = FeedStatus.DISCONNECTED
            elif state.last_seen is None:
                status = FeedStatus.DISCONNECTED
            elif now - state.last_seen > self._stale_after:
                status = FeedStatus.STALE
            else:
                status = FeedStatus.CONNECTED
            states.append(FeedSourceState(source_id=source_id, status=status, last_seen=state.last_seen))
        return tuple(states)

    def _allows_source(self, source_id: str) -> bool:
        return not self._expected_source_ids or source_id in self._expected_source_ids

    def _assert_source_allowed(self, source_id: str) -> None:
        if not self._allows_source(source_id):
            raise ValueError(f"source_id is not expected: {source_id}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
