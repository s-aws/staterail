from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


@dataclass(frozen=True)
class FixedClock:
    current_time: datetime

    def now(self) -> datetime:
        if self.current_time.tzinfo is None:
            return self.current_time.replace(tzinfo=timezone.utc)
        return self.current_time.astimezone(timezone.utc)

