from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from audit.ledger import AuditRecord
from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import (
    EventType,
    ExecutionMode,
    ExecutionStatus,
    ReconciliationIssue,
)
from core.json_tools import JsonValue
from projections.state import OrderSnapshot, SourceOfTruthProjection


@dataclass(frozen=True)
class ReconciliationPolicy:
    execution_result_timeout: timedelta = timedelta(seconds=30)
    user_confirmation_timeout: timedelta = timedelta(seconds=30)
    execution_modes: tuple[ExecutionMode, ...] = (ExecutionMode.LIVE,)

    def __post_init__(self) -> None:
        if self.execution_result_timeout <= timedelta(0):
            raise ValueError("execution_result_timeout must be positive")
        if self.user_confirmation_timeout <= timedelta(0):
            raise ValueError("user_confirmation_timeout must be positive")
        if not self.execution_modes:
            raise ValueError("execution_modes must not be empty")
        for mode in self.execution_modes:
            if not isinstance(mode, ExecutionMode):
                raise TypeError("execution_modes must contain ExecutionMode values")


@dataclass(frozen=True)
class ReconciliationFinding:
    action_id: str
    reason: ReconciliationIssue
    observed_at: datetime
    timeout_seconds: float
    elapsed_seconds: float
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    executed_at: datetime | None = None
    executed_sequence: int | None = None
    execution_started_at: datetime | None = None
    execution_started_sequence: int | None = None
    product_id: str | None = None

    def to_payload(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {
            "action_id": self.action_id,
            "client_order_id": self.client_order_id,
            "elapsed_seconds": self.elapsed_seconds,
            "exchange_order_id": self.exchange_order_id,
            "observed_at": _utc(self.observed_at).isoformat(),
            "product_id": self.product_id,
            "reason": self.reason.value,
            "timeout_seconds": self.timeout_seconds,
        }
        if self.executed_at is not None:
            payload["executed_at"] = _utc(self.executed_at).isoformat()
        if self.executed_sequence is not None:
            payload["executed_sequence"] = self.executed_sequence
        if self.execution_started_at is not None:
            payload["execution_started_at"] = _utc(self.execution_started_at).isoformat()
        if self.execution_started_sequence is not None:
            payload["execution_started_sequence"] = self.execution_started_sequence
        return payload


class ReconciliationWatchdog:
    def __init__(
        self,
        core: AuditCore,
        *,
        clock: Clock | None = None,
        policy: ReconciliationPolicy | None = None,
    ) -> None:
        self._core = core
        self._clock = clock or SystemClock()
        self._policy = policy or ReconciliationPolicy()

    def audit(self) -> tuple[ReconciliationFinding, ...]:
        records = self._core.ledger.iter_records()
        projection = SourceOfTruthProjection.from_records(records)
        observed_at = _utc(self._clock.now())
        sequence_times = {record.sequence: record.occurred_at for record in records}

        findings: list[ReconciliationFinding] = []
        for order in projection.orders_by_action_id.values():
            findings.extend(
                finding
                for finding in (
                    self._missing_execution_result(
                        order=order,
                        observed_at=observed_at,
                        sequence_times=sequence_times,
                        projection=projection,
                    ),
                    self._missing_user_confirmation(
                        order=order,
                        observed_at=observed_at,
                        sequence_times=sequence_times,
                        projection=projection,
                    ),
                )
                if finding is not None
            )

        for finding in findings:
            self._core.emit(EventType.RECONCILIATION_MISMATCH, finding.to_payload())
        return tuple(findings)

    def _missing_execution_result(
        self,
        *,
        order: OrderSnapshot,
        observed_at: datetime,
        sequence_times: dict[int, datetime],
        projection: SourceOfTruthProjection,
    ) -> ReconciliationFinding | None:
        if order.execution_started_sequence is None:
            return None
        if order.executed_sequence is not None:
            return None
        if projection.has_reconciliation_mismatch(
            action_id=order.action_id,
            reason=ReconciliationIssue.MISSING_EXECUTION_RESULT,
        ):
            return None

        execution_started_at = sequence_times.get(order.execution_started_sequence)
        if execution_started_at is None:
            return None
        elapsed = observed_at - _utc(execution_started_at)
        if elapsed < self._policy.execution_result_timeout:
            return None

        return ReconciliationFinding(
            action_id=order.action_id,
            client_order_id=order.client_order_id,
            elapsed_seconds=elapsed.total_seconds(),
            exchange_order_id=order.exchange_order_id,
            execution_started_at=execution_started_at,
            execution_started_sequence=order.execution_started_sequence,
            observed_at=observed_at,
            product_id=order.product_id,
            reason=ReconciliationIssue.MISSING_EXECUTION_RESULT,
            timeout_seconds=self._policy.execution_result_timeout.total_seconds(),
        )

    def _missing_user_confirmation(
        self,
        *,
        order: OrderSnapshot,
        observed_at: datetime,
        sequence_times: dict[int, datetime],
        projection: SourceOfTruthProjection,
    ) -> ReconciliationFinding | None:
        if order.execution_mode not in self._policy.execution_modes:
            return None
        if order.execution_status != ExecutionStatus.ACCEPTED:
            return None
        if order.executed_sequence is None:
            return None
        if order.last_exchange_update:
            return None
        if projection.has_reconciliation_mismatch(
            action_id=order.action_id,
            reason=ReconciliationIssue.MISSING_USER_CONFIRMATION,
        ):
            return None

        executed_at = sequence_times.get(order.executed_sequence)
        if executed_at is None:
            return None
        elapsed = observed_at - _utc(executed_at)
        if elapsed < self._policy.user_confirmation_timeout:
            return None

        return ReconciliationFinding(
            action_id=order.action_id,
            client_order_id=order.client_order_id,
            elapsed_seconds=elapsed.total_seconds(),
            executed_at=executed_at,
            executed_sequence=order.executed_sequence,
            exchange_order_id=order.exchange_order_id,
            observed_at=observed_at,
            product_id=order.product_id,
            reason=ReconciliationIssue.MISSING_USER_CONFIRMATION,
            timeout_seconds=self._policy.user_confirmation_timeout.total_seconds(),
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
