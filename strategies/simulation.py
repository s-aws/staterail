from __future__ import annotations

import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionCommand, ActionGateway, ActionPreview
from audit.ledger import AuditLedger, AuditRecord
from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ErrorCategory,
    ErrorCode,
    ExecutionMode,
    StrategyEvaluationStatus,
    StrategySimulationStatus,
)
from core.errors import (
    StrategyContractError,
    StrategyInputUnavailableError,
    exception_to_error_payload,
)
from core.json_tools import JsonValue, canonical_json, normalize_json
from products.catalog import ProductCatalog
from projections.state import SourceOfTruthProjection
from risk.gate import RiskGate
from strategies.harness import (
    Strategy,
    StrategyDecision,
    StrategyInputFreshness,
    StrategyInputRequirement,
    StrategySnapshot,
    strategy_decision_commands,
)

if TYPE_CHECKING:
    from strategies.operator_policy import OperatorPolicy


@dataclass(frozen=True)
class StrategySimulationActionPreview:
    command: ActionCommand
    preview: ActionPreview

    def __post_init__(self) -> None:
        if not isinstance(self.command, ActionCommand):
            raise TypeError("command must be an ActionCommand")
        if not isinstance(self.preview, ActionPreview):
            raise TypeError("preview must be an ActionPreview")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "command": self.command.to_payload(),
            "preview": self.preview.to_payload(),
        }


@dataclass(frozen=True)
class StrategySimulationEvaluation:
    strategy_id: str
    status: StrategyEvaluationStatus
    as_of_sequence: int
    evaluated_at: datetime
    action_previews: tuple[StrategySimulationActionPreview, ...] = ()
    error: Mapping[str, Any] | None = None
    input_freshness: tuple[StrategyInputFreshness, ...] = ()
    intent_count: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.strategy_id:
            raise ValueError("strategy_id is required")
        if not isinstance(self.status, StrategyEvaluationStatus):
            raise TypeError("status must be a StrategyEvaluationStatus")
        if self.as_of_sequence < 0:
            raise ValueError("as_of_sequence must not be negative")
        if not isinstance(self.evaluated_at, datetime):
            raise TypeError("evaluated_at must be a datetime")
        if self.intent_count < 0:
            raise ValueError("intent_count must not be negative")
        if not isinstance(self.action_previews, tuple):
            raise TypeError("action_previews must be a tuple")
        if not isinstance(self.input_freshness, tuple):
            raise TypeError("input_freshness must be a tuple")
        for freshness in self.input_freshness:
            if not isinstance(freshness, StrategyInputFreshness):
                raise TypeError("input_freshness must contain StrategyInputFreshness values")
        _metadata_payload(self.metadata)
        if self.error is not None:
            _metadata_payload(self.error)

    @property
    def accepted_action_count(self) -> int:
        return sum(1 for preview in self.action_previews if preview.preview.status == ActionStatus.ACCEPTED)

    @property
    def rejected_action_count(self) -> int:
        return sum(1 for preview in self.action_previews if preview.preview.status == ActionStatus.REJECTED)

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "accepted_action_count": self.accepted_action_count,
            "action_previews": [preview.to_payload() for preview in self.action_previews],
            "as_of_sequence": self.as_of_sequence,
            "error": self.error,
            "evaluated_at": self.evaluated_at,
            "intent_count": self.intent_count,
            "input_freshness": [freshness.to_payload() for freshness in self.input_freshness],
            "metadata": _metadata_payload(self.metadata),
            "rejected_action_count": self.rejected_action_count,
            "status": self.status.value,
            "strategy_id": self.strategy_id,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Strategy simulation evaluation payload must normalize to an object")
        return normalized


@dataclass(frozen=True)
class StrategySimulationReport:
    ledger_path: Path
    ledger_last_hash: str | None
    ledger_record_count: int
    as_of_sequence: int
    evaluated_at: datetime
    execution_mode: ExecutionMode
    evaluations: tuple[StrategySimulationEvaluation, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_path, Path):
            raise TypeError("ledger_path must be a pathlib.Path")
        if self.ledger_record_count < 0:
            raise ValueError("ledger_record_count must not be negative")
        if self.as_of_sequence < 0:
            raise ValueError("as_of_sequence must not be negative")
        if not isinstance(self.evaluated_at, datetime):
            raise TypeError("evaluated_at must be a datetime")
        if not isinstance(self.execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")
        if not isinstance(self.evaluations, tuple):
            raise TypeError("evaluations must be a tuple")

    @property
    def completed_count(self) -> int:
        return sum(1 for evaluation in self.evaluations if evaluation.status == StrategyEvaluationStatus.COMPLETED)

    @property
    def failed_count(self) -> int:
        return sum(1 for evaluation in self.evaluations if evaluation.status == StrategyEvaluationStatus.FAILED)

    @property
    def accepted_action_count(self) -> int:
        return sum(evaluation.accepted_action_count for evaluation in self.evaluations)

    @property
    def rejected_action_count(self) -> int:
        return sum(evaluation.rejected_action_count for evaluation in self.evaluations)

    @property
    def intent_count(self) -> int:
        return sum(evaluation.intent_count for evaluation in self.evaluations)

    @property
    def status(self) -> StrategySimulationStatus:
        if self.failed_count > 0 or self.rejected_action_count > 0:
            return StrategySimulationStatus.ATTENTION_REQUIRED
        return StrategySimulationStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "accepted_action_count": self.accepted_action_count,
            "as_of_sequence": self.as_of_sequence,
            "completed_count": self.completed_count,
            "evaluated_at": self.evaluated_at,
            "evaluations": [evaluation.to_payload() for evaluation in self.evaluations],
            "execution_mode": self.execution_mode.value,
            "failed_count": self.failed_count,
            "intent_count": self.intent_count,
            "ledger": {
                "last_hash": self.ledger_last_hash,
                "ledger_path": self.ledger_path.as_posix(),
                "record_count": self.ledger_record_count,
                "verified": True,
            },
            "read_only": True,
            "rejected_action_count": self.rejected_action_count,
            "status": self.status.value,
            "strategy_count": len(self.evaluations),
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Strategy simulation report payload must normalize to an object")
        return normalized


def simulate_strategies(
    *,
    execution_mode: ExecutionMode,
    ledger_last_hash: str | None,
    ledger_path: Path,
    ledger_record_count: int,
    market_data_requirements: tuple[StrategyInputRequirement, ...] = (),
    projection: SourceOfTruthProjection,
    strategies: tuple[Strategy, ...],
    clock: Clock | None = None,
    operator_policy: OperatorPolicy | None = None,
    product_catalog: ProductCatalog | None = None,
    risk_gate: RiskGate | None = None,
) -> StrategySimulationReport:
    if not strategies:
        raise ValueError("at least one strategy is required")
    if not isinstance(execution_mode, ExecutionMode):
        raise TypeError("execution_mode must be an ExecutionMode")
    if not isinstance(ledger_path, Path):
        raise TypeError("ledger_path must be a pathlib.Path")
    if ledger_record_count < 0:
        raise ValueError("ledger_record_count must not be negative")
    if not isinstance(projection, SourceOfTruthProjection):
        raise TypeError("projection must be a SourceOfTruthProjection")
    if not isinstance(market_data_requirements, tuple):
        raise TypeError("market_data_requirements must be a tuple")
    for requirement in market_data_requirements:
        if not isinstance(requirement, StrategyInputRequirement):
            raise TypeError("market_data_requirements must contain StrategyInputRequirement values")

    resolved_clock = clock or SystemClock()
    evaluated_at = resolved_clock.now()
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=evaluated_at,
        execution_mode=execution_mode,
        ledger_path=ledger_path,
        operator_policy=operator_policy,
        product_catalog=product_catalog,
        projection=projection,
        metadata={"simulation": True},
    )
    baseline_records = AuditLedger(ledger_path, clock=resolved_clock).snapshot().records
    evaluations = tuple(
        _simulate_strategy(
            baseline_records,
            resolved_clock,
            ledger_path,
            market_data_requirements,
            risk_gate,
            snapshot,
            strategy,
            strategy_index=index,
        )
        for index, strategy in enumerate(strategies)
    )
    return StrategySimulationReport(
        as_of_sequence=projection.last_sequence,
        evaluated_at=evaluated_at,
        evaluations=evaluations,
        execution_mode=execution_mode,
        ledger_last_hash=ledger_last_hash,
        ledger_path=ledger_path,
        ledger_record_count=ledger_record_count,
    )


def _simulate_strategy(
    baseline_records: tuple[AuditRecord, ...],
    clock: Clock,
    ledger_path: Path,
    market_data_requirements: tuple[StrategyInputRequirement, ...],
    risk_gate: RiskGate | None,
    snapshot: StrategySnapshot,
    strategy: Strategy,
    *,
    strategy_index: int,
) -> StrategySimulationEvaluation:
    strategy_id = strategy.strategy_id
    if not isinstance(strategy_id, str) or not strategy_id:
        raise ValueError("strategy_id must be a non-empty string")
    input_freshness = tuple(requirement.evaluate(snapshot) for requirement in market_data_requirements)
    try:
        stale_or_missing_inputs = tuple(freshness for freshness in input_freshness if not freshness.is_ok)
        if stale_or_missing_inputs:
            raise StrategyInputUnavailableError(
                "strategy input requirements are not satisfied",
                context={
                    "input_freshness": [freshness.to_payload() for freshness in input_freshness],
                    "strategy_id": strategy_id,
                },
            )
        decision = strategy.evaluate(snapshot)
        if not isinstance(decision, StrategyDecision):
            raise StrategyContractError(
                "strategy evaluate must return a StrategyDecision",
                context={
                    "observed_type": decision.__class__.__name__,
                    "strategy_id": strategy_id,
                    "strategy_index": strategy_index,
                },
            )
        action_previews = _preview_commands_in_order(
            baseline_records=baseline_records,
            clock=clock,
            commands=strategy_decision_commands(strategy_id, decision),
            ledger_path=ledger_path,
            risk_gate=risk_gate,
        )
        return StrategySimulationEvaluation(
            action_previews=action_previews,
            as_of_sequence=snapshot.as_of_sequence,
            evaluated_at=snapshot.evaluated_at,
            input_freshness=input_freshness,
            intent_count=len(decision.intents),
            metadata=decision.metadata,
            status=StrategyEvaluationStatus.COMPLETED,
            strategy_id=strategy_id,
        )
    except Exception as exc:
        error_code = (
            ErrorCode.STRATEGY_CONTRACT_FAILED
            if isinstance(exc, StrategyContractError)
            else ErrorCode.STRATEGY_INPUT_UNAVAILABLE
            if isinstance(exc, StrategyInputUnavailableError)
            else ErrorCode.STRATEGY_EVALUATION_FAILED
        )
        return StrategySimulationEvaluation(
            as_of_sequence=snapshot.as_of_sequence,
            error=exception_to_error_payload(
                exc,
                category=ErrorCategory.STRATEGY,
                context={
                    "simulation": True,
                    "strategy_id": strategy_id,
                    "strategy_index": strategy_index,
                },
                error_code=error_code,
            ),
            evaluated_at=snapshot.evaluated_at,
            input_freshness=input_freshness,
            status=StrategyEvaluationStatus.FAILED,
            strategy_id=strategy_id,
        )


def _preview_commands_in_order(
    *,
    baseline_records: tuple[AuditRecord, ...],
    clock: Clock,
    commands: tuple[ActionCommand, ...],
    ledger_path: Path,
    risk_gate: RiskGate | None,
) -> tuple[StrategySimulationActionPreview, ...]:
    if not commands:
        return ()
    with tempfile.TemporaryDirectory(prefix="strategy-preview-") as temp_dir:
        temp_ledger_path = Path(temp_dir) / ledger_path.name
        _write_baseline_records(temp_ledger_path, baseline_records)
        temp_ledger = AuditLedger(temp_ledger_path, clock=clock)
        temp_gateway = ActionGateway(AuditCore(temp_ledger), risk_gate=risk_gate)
        previews: list[StrategySimulationActionPreview] = []
        for command in commands:
            projection = SourceOfTruthProjection.from_ledger(temp_ledger)
            preview = temp_gateway.preview(command, projection=projection)
            previews.append(StrategySimulationActionPreview(command=command, preview=preview))
            if preview.status != ActionStatus.ACCEPTED:
                break
            receipt = temp_gateway.submit_and_execute(command, DryRunExecutor())
            if receipt.status not in {ActionStatus.ACCEPTED, ActionStatus.EXECUTED}:
                break
        return tuple(previews)


def _write_baseline_records(path: Path, records: tuple[AuditRecord, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        return
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(canonical_json(record.to_dict()))
            handle.write("\n")


def _metadata_payload(metadata: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(metadata)
    if not isinstance(normalized, dict):
        raise TypeError("metadata must normalize to a JSON object")
    return normalized
