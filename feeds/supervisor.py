from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Protocol

from core.engine import AuditCore
from core.enums import ErrorCategory, ErrorCode, EventType, FeedStopReason
from core.errors import error_event_payload, exception_to_error_payload
from feeds.router import FeedMessage, RedundantFeedRouter


SEQUENCE_ANOMALY_EVENT_TYPES = frozenset(
    {
        EventType.DATA_OUT_OF_ORDER,
        EventType.DATA_SEQUENCE_GAP,
    }
)


class AsyncFeedSource(Protocol):
    @property
    def source_id(self) -> str:
        ...

    def stream(self) -> AsyncIterator[FeedMessage]:
        ...


Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class ReconnectPolicy:
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.initial_delay_seconds < 0:
            raise ValueError("initial_delay_seconds must not be negative")
        if self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must not be negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")

    def delay_for_attempt(self, completed_attempts: int) -> float:
        delay = self.initial_delay_seconds * self.multiplier ** max(completed_attempts - 1, 0)
        return min(delay, self.max_delay_seconds)


class FeedSupervisor:
    def __init__(
        self,
        core: AuditCore,
        router: RedundantFeedRouter,
        sources: Iterable[AsyncFeedSource],
        *,
        reconnect_policy: ReconnectPolicy | None = None,
        sleep: Sleep | None = None,
    ) -> None:
        self._core = core
        self._router = router
        self._sources = tuple(sources)
        if not self._sources:
            raise ValueError("At least one feed source is required")
        source_ids = tuple(source.source_id for source in self._sources)
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("Feed source ids must be unique")
        for source_id in source_ids:
            self._router.register_source(source_id)
        self._reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._sleep = sleep or asyncio.sleep
        self._stop_requested = False

    def stop(self) -> None:
        self._stop_requested = True

    async def run(self, *, max_attempts_per_source: int | None = None) -> None:
        tasks = [
            asyncio.create_task(
                self._run_source(source, max_attempts=max_attempts_per_source),
                name=f"feed-supervisor:{source.source_id}",
            )
            for source in self._sources
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_source(self, source: AsyncFeedSource, *, max_attempts: int | None) -> None:
        attempts = 0
        while not self._stop_requested and (max_attempts is None or attempts < max_attempts):
            attempts += 1
            self._router.mark_connected(source.source_id)
            self._core.emit(
                EventType.FEED_CONNECTED,
                {
                    "attempt": attempts,
                    "source_id": source.source_id,
                },
            )

            reason = FeedStopReason.STREAM_ENDED
            try:
                async for message in source.stream():
                    if self._stop_requested:
                        reason = FeedStopReason.STOP_REQUESTED
                        break
                    if message.source_id != source.source_id:
                        reason = FeedStopReason.SOURCE_MISMATCH
                        self._core.emit(
                            EventType.ERROR,
                            error_event_payload(
                                category=ErrorCategory.FEED_SOURCE,
                                context={
                                    "expected_source_id": source.source_id,
                                    "message_key": message.message_key,
                                    "observed_source_id": message.source_id,
                                    "reason": reason.value,
                                },
                                error_code=ErrorCode.FEED_SOURCE_MISMATCH,
                                message="Feed source emitted a message with a mismatched source_id",
                            ),
                        )
                        break
                    self._router.ingest(message)
                    if message.event_type in SEQUENCE_ANOMALY_EVENT_TYPES:
                        reason = FeedStopReason.SEQUENCE_ANOMALY
                        break
            except asyncio.CancelledError:
                reason = FeedStopReason.CANCELLED
                self._audit_disconnected(source.source_id, attempts, reason)
                raise
            except Exception as exc:
                reason = FeedStopReason.ERROR
                self._core.emit(
                    EventType.ERROR,
                    exception_to_error_payload(
                        exc,
                        category=ErrorCategory.FEED_SOURCE,
                        context={
                            "attempt": attempts,
                            "source_id": source.source_id,
                        },
                        retryable=True,
                    ),
                )
            finally:
                if reason != FeedStopReason.CANCELLED:
                    self._audit_disconnected(source.source_id, attempts, reason)

            if self._stop_requested or (max_attempts is not None and attempts >= max_attempts):
                break

            self._router.audit_health()

            delay = self._reconnect_policy.delay_for_attempt(attempts)
            self._core.emit(
                EventType.FEED_RECONNECT_SCHEDULED,
                {
                    "attempt": attempts + 1,
                    "delay_seconds": delay,
                    "source_id": source.source_id,
                },
            )
            await self._sleep(delay)

    def _audit_disconnected(self, source_id: str, attempt: int, reason: FeedStopReason) -> None:
        self._router.mark_disconnected(source_id)
        self._core.emit(
            EventType.FEED_DISCONNECTED,
            {
                "attempt": attempt,
                "reason": reason.value,
                "source_id": source_id,
            },
        )
