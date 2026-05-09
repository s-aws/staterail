from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.enums import (
    ExecutionMode,
    StrategyEvaluationStatus,
    StrategyMarketDataStatus,
)
from strategies import (
    assert_strategy_metadata_contains,
    assert_strategy_metadata_path,
    strategy_evaluation,
    strategy_metadata,
)
from strategies.simulation import StrategySimulationEvaluation, StrategySimulationReport


def test_strategy_metadata_assertions_match_nested_subset_and_paths():
    report = _simulation_report(
        StrategySimulationEvaluation(
            as_of_sequence=3,
            evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            metadata={
                "book": {
                    "depth": {"status": StrategyMarketDataStatus.INSUFFICIENT_DATA.value},
                    "status": StrategyMarketDataStatus.STALE.value,
                },
                "diagnostics": [
                    {"status": StrategyMarketDataStatus.MISSING.value},
                ],
            },
            status=StrategyEvaluationStatus.COMPLETED,
            strategy_id="example",
        )
    )

    observed = assert_strategy_metadata_contains(
        report,
        "example",
        {
            "book": {
                "depth": {"status": StrategyMarketDataStatus.INSUFFICIENT_DATA.value},
            },
        },
    )

    assert observed == strategy_metadata(report, "example")
    assert strategy_evaluation(report, "example").status == StrategyEvaluationStatus.COMPLETED
    assert assert_strategy_metadata_path(
        report,
        "example",
        "book.status",
        StrategyMarketDataStatus.STALE.value,
    ) == StrategyMarketDataStatus.STALE.value
    assert assert_strategy_metadata_path(
        report,
        "example",
        ("diagnostics", 0, "status"),
        StrategyMarketDataStatus.MISSING.value,
    ) == StrategyMarketDataStatus.MISSING.value


def test_strategy_metadata_assertions_report_actionable_failures():
    report = _simulation_report(
        StrategySimulationEvaluation(
            as_of_sequence=3,
            evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            metadata={"book": {"status": StrategyMarketDataStatus.STALE.value}},
            status=StrategyEvaluationStatus.COMPLETED,
            strategy_id="example",
        )
    )

    with pytest.raises(AssertionError, match="metadata.book.depth missing"):
        assert_strategy_metadata_contains(
            report,
            "example",
            {"book": {"depth": {"status": StrategyMarketDataStatus.INSUFFICIENT_DATA.value}}},
        )

    with pytest.raises(AssertionError, match="metadata.book.status"):
        assert_strategy_metadata_path(
            report,
            "example",
            "book.status",
            StrategyMarketDataStatus.OK.value,
        )

    with pytest.raises(AssertionError, match="strategy evaluation not found"):
        strategy_evaluation(report, "missing")


def test_strategy_evaluation_assertion_rejects_duplicate_strategy_ids():
    report = _simulation_report(
        StrategySimulationEvaluation(
            as_of_sequence=3,
            evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=StrategyEvaluationStatus.COMPLETED,
            strategy_id="duplicate",
        ),
        StrategySimulationEvaluation(
            as_of_sequence=3,
            evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=StrategyEvaluationStatus.COMPLETED,
            strategy_id="duplicate",
        ),
    )

    with pytest.raises(AssertionError, match="not unique"):
        strategy_evaluation(report, "duplicate")


def _simulation_report(
    *evaluations: StrategySimulationEvaluation,
) -> StrategySimulationReport:
    return StrategySimulationReport(
        as_of_sequence=3,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        evaluations=evaluations,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=None,
        ledger_path=Path("scenario.jsonl"),
        ledger_record_count=3,
    )
