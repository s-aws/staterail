from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionGateway, CancelOrderIntent, PlaceOrderIntent
from app.bootstrap import CoinbaseApplicationConfig, build_coinbase_application, default_coinbase_application_config
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from app.ledger_health import ledger_health_payload
from app.ledger_summary import ledger_summary_payload
from app.strategy_simulation import strategy_simulation_payload
from app.strategy_simulation_gate import (
    enforce_live_strategy_simulation_gate,
    record_strategy_simulation_result,
    strategy_simulation_gate_payload,
)
from audit.ledger import AuditLedger
from config.assembly import CoinbaseBotConfig, CoinbaseRestApiConfig, StrategyRuntimeConfig, TaskScheduleConfig
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ErrorCode,
    EventType,
    ExecutionMode,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    MarketDataKind,
    OrderLineageRelation,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    ReadinessStatus,
    RiskRule,
    RuntimeTask,
    StrategyEvaluationStatus,
    StrategySimulationGateIssue,
    StrategyInputStatus,
    StrategySimulationStatus,
)
from projections.state import SourceOfTruthProjection
from products.catalog import ProductCatalog, ProductMetadata
from risk.gate import RiskGate
from strategies import (
    StrategyDecision,
    StrategyInputRequirement,
    StrategySnapshot,
    load_operator_policy_from_json_file,
    simulate_strategies,
    strategy_consolidation_intent,
)


class StaticOrderStrategy:
    @property
    def strategy_id(self) -> str:
        return "static-order"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id=f"static-order-{snapshot.as_of_sequence}",
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    size="0.01",
                    limit_price="50000",
                ),
            ),
            metadata={"source_sequence": snapshot.as_of_sequence},
        )


class InvalidReturnStrategy:
    @property
    def strategy_id(self) -> str:
        return "invalid-return"

    def evaluate(self, snapshot: StrategySnapshot):
        del snapshot
        return {"intents": []}


class RejectedOrderStrategy:
    @property
    def strategy_id(self) -> str:
        return "rejected-order"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id=f"rejected-order-{snapshot.as_of_sequence}",
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    size="1",
                    limit_price="50000",
                ),
            ),
        )


class DuplicateClientIdentityStrategy:
    @property
    def strategy_id(self) -> str:
        return "duplicate-client"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        del snapshot
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id="first-action",
                    idempotency_key="same-client-order",
                    limit_price="50000",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
                PlaceOrderIntent(
                    action_id="second-action",
                    idempotency_key="same-client-order",
                    limit_price="50001",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
            )
        )


class CapturingNoopStrategy:
    def __init__(self) -> None:
        self.snapshots: list[StrategySnapshot] = []

    @property
    def strategy_id(self) -> str:
        return "capturing-noop"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        self.snapshots.append(snapshot)
        return StrategyDecision()


class ConsolidationPlanStrategy:
    @property
    def strategy_id(self) -> str:
        return "consolidation-plan"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        source_ids = ("source-a", "source-b")
        cancels = tuple(
            CancelOrderIntent(
                action_id=f"cancel-{source_id}",
                exchange_order_id=snapshot.projection.orders_by_action_id[source_id].exchange_order_id,
            )
            for source_id in source_ids
        )
        consolidation = strategy_consolidation_intent(
            self.strategy_id,
            "tidy",
            snapshot,
            source_ids,
        )
        return StrategyDecision(intents=(*cancels, consolidation))


def test_strategy_simulation_previews_intents_without_appending(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    core.emit(EventType.SYSTEM_STARTED, {"component": "test"})
    projection = SourceOfTruthProjection.from_ledger(ledger)
    before_records = ledger.iter_records()

    report = simulate_strategies(
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=before_records[-1].record_hash,
        ledger_path=ledger.path,
        ledger_record_count=len(before_records),
        projection=projection,
        strategies=(StaticOrderStrategy(),),
    )
    payload = report.to_payload()

    assert report.completed_count == 1
    assert report.accepted_action_count == 1
    assert payload["read_only"] is True
    assert payload["status"] == StrategySimulationStatus.OK.value
    assert payload["ledger"]["record_count"] == len(before_records)
    assert payload["evaluations"][0]["status"] == StrategyEvaluationStatus.COMPLETED.value
    assert payload["evaluations"][0]["metadata"] == {"source_sequence": 1}
    assert payload["evaluations"][0]["action_previews"][0]["preview"]["status"] == ActionStatus.ACCEPTED.value
    assert ledger.iter_records() == before_records


def test_strategy_simulation_previews_ordered_cancel_then_consolidate_plan(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    gateway = ActionGateway(AuditCore(ledger))
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-a",
            limit_price="100",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size="0.05",
        ).to_command(),
        DryRunExecutor(),
    )
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-b",
            limit_price="100",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size="0.05",
        ).to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    before_records = ledger.iter_records()
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    operator_policy = replace(
        operator_policy,
        risk_limits=replace(operator_policy.risk_limits, kill_switch_enabled=False),
    )
    product_catalog = ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),))

    report = simulate_strategies(
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=before_records[-1].record_hash,
        ledger_path=ledger.path,
        ledger_record_count=len(before_records),
        operator_policy=operator_policy,
        product_catalog=product_catalog,
        projection=projection,
        risk_gate=RiskGate(operator_policy.to_risk_policy_config().to_policy(product_catalog=product_catalog)),
        strategies=(ConsolidationPlanStrategy(),),
    )
    previews = report.evaluations[0].action_previews
    consolidation_payload = previews[2].command.to_payload()["payload"]
    consolidation_risk = previews[2].preview.risk_evaluation
    assert consolidation_risk is not None
    open_orders_check = next(
        check for check in consolidation_risk.checks if check.rule == RiskRule.MAX_OPEN_ORDERS
    )

    assert report.completed_count == 1
    assert report.accepted_action_count == 3
    assert [preview.preview.status for preview in previews] == [
        ActionStatus.ACCEPTED,
        ActionStatus.ACCEPTED,
        ActionStatus.ACCEPTED,
    ]
    assert consolidation_payload["lineage_relation"] == OrderLineageRelation.CONSOLIDATION.value
    assert consolidation_payload["source_order_ids"] == ["source-a", "source-b"]
    assert open_orders_check.observed == "1"
    assert ledger.iter_records() == before_records


def test_strategy_simulation_exposes_operator_policy_on_snapshot(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    strategy = CapturingNoopStrategy()
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )

    report = simulate_strategies(
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=None,
        ledger_path=ledger.path,
        ledger_record_count=0,
        operator_policy=operator_policy,
        projection=SourceOfTruthProjection.from_ledger(ledger),
        strategies=(strategy,),
    )

    assert report.completed_count == 1
    assert strategy.snapshots[0].operator_policy is operator_policy


def test_strategy_simulation_reports_strategy_contract_failures_without_appending(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    before_records = ledger.iter_records()

    report = simulate_strategies(
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=None,
        ledger_path=ledger.path,
        ledger_record_count=0,
        projection=SourceOfTruthProjection.from_ledger(ledger),
        strategies=(InvalidReturnStrategy(),),
    )
    payload = report.to_payload()

    assert report.failed_count == 1
    assert payload["status"] == StrategySimulationStatus.ATTENTION_REQUIRED.value
    assert payload["evaluations"][0]["status"] == StrategyEvaluationStatus.FAILED.value
    assert payload["evaluations"][0]["error"]["error_code"] == ErrorCode.STRATEGY_CONTRACT_FAILED.value
    assert ledger.iter_records() == before_records


def test_strategy_simulation_blocks_missing_required_market_data_without_appending(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    before_records = ledger.iter_records()

    report = simulate_strategies(
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=None,
        ledger_path=ledger.path,
        ledger_record_count=0,
        market_data_requirements=(
            StrategyInputRequirement(
                data_kind=MarketDataKind.TICKER,
                max_age=timedelta(seconds=5),
                product_id="BTC-USD",
            ),
        ),
        projection=SourceOfTruthProjection.from_ledger(ledger),
        strategies=(StaticOrderStrategy(),),
    )
    payload = report.to_payload()

    assert report.failed_count == 1
    assert report.accepted_action_count == 0
    assert payload["status"] == StrategySimulationStatus.ATTENTION_REQUIRED.value
    assert payload["evaluations"][0]["error"]["error_code"] == ErrorCode.STRATEGY_INPUT_UNAVAILABLE.value
    assert payload["evaluations"][0]["input_freshness"][0]["status"] == StrategyInputStatus.MISSING.value
    assert payload["evaluations"][0]["action_previews"] == []
    assert ledger.iter_records() == before_records


def test_strategy_simulation_rejects_duplicate_decision_identities_without_previews(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    before_records = ledger.iter_records()

    report = simulate_strategies(
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_last_hash=None,
        ledger_path=ledger.path,
        ledger_record_count=0,
        projection=SourceOfTruthProjection.from_ledger(ledger),
        strategies=(DuplicateClientIdentityStrategy(),),
    )
    payload = report.to_payload()

    assert report.failed_count == 1
    assert report.accepted_action_count == 0
    assert payload["evaluations"][0]["error"]["error_code"] == ErrorCode.STRATEGY_CONTRACT_FAILED.value
    assert payload["evaluations"][0]["error"]["error"]["context"]["duplicate_client_order_ids"] == [
        "same-client-order"
    ]
    assert payload["evaluations"][0]["action_previews"] == []
    assert ledger.iter_records() == before_records


def test_strategy_simulation_payload_selects_configured_static_strategy(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    before_records = AuditLedger(ledger_path).iter_records()
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            strategies=StrategyRuntimeConfig(strategy_ids=("static-order",)),
        ),
    )

    payload = strategy_simulation_payload(
        config,
        strategies=(StaticOrderStrategy(),),
    )

    assert payload["strategy_count"] == 1
    assert payload["status"] == StrategySimulationStatus.OK.value
    assert payload["evaluations"][0]["strategy_id"] == "static-order"
    assert payload["evaluations"][0]["action_previews"][0]["preview"]["status"] == ActionStatus.ACCEPTED.value
    assert AuditLedger(ledger_path).iter_records() == before_records


def test_cli_strategy_simulation_is_read_only(workspace_tmp_path, capsys, monkeypatch):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "strategy-sim-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    before_records = AuditLedger(ledger_path).iter_records()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(ledger_path),
                max_cycles=99,
                strategy_simulate=True,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["read_only"] is True
    assert payload["status"] == StrategySimulationStatus.OK.value
    assert payload["strategy_count"] == 1
    assert payload["evaluations"][0]["strategy_id"] == "noop"
    assert AuditLedger(ledger_path).iter_records() == before_records


def test_cli_strategy_simulation_can_record_qualification_result(workspace_tmp_path, capsys, monkeypatch):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "strategy-sim-recorded-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    before_records = AuditLedger(ledger_path).iter_records()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(ledger_path),
                max_cycles=99,
                strategy_simulate=True,
                strategy_simulate_record_result=True,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    records = AuditLedger(ledger_path).iter_records()
    summary = ledger_summary_payload(ledger_path)
    health = ledger_health_payload(ledger_path)
    checks = {check["name"]: check for check in health["checks"]}

    assert exit_code == 0
    assert payload["read_only"] is True
    assert payload["status"] == StrategySimulationStatus.OK.value
    assert payload["strategy_simulation_result_sequence"] == records[-1].sequence
    assert records[:-1] == before_records
    assert records[-1].event_type == EventType.STRATEGY_SIMULATION_RESULT
    assert records[-1].payload["read_only"] is True
    assert records[-1].payload["runtime_tasks_started"] is False
    assert records[-1].payload["strategy_tasks_started"] is False
    assert records[-1].payload["order_endpoint_called"] is False
    assert summary["strategy_simulation_result_count"] == 1
    assert summary["latest_strategy_simulation_sequence"] == records[-1].sequence
    assert checks[LedgerHealthCheckName.STRATEGY_SIMULATION_CONTRACT.value]["status"] == (
        LedgerHealthStatus.OK.value
    )


def test_live_strategy_simulation_gate_requires_matching_clean_result(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "strategy-sim-gate-audit.jsonl"
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
            strategies=StrategyRuntimeConfig(
                allow_live_execution=True,
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.STRATEGY_EVALUATION,
                    interval=timedelta(seconds=1),
                    enabled=True,
                    run_on_start=False,
                ),
                strategy_ids=("noop",),
            ),
        ),
    )
    AuditCore(AuditLedger(ledger_path)).emit(EventType.SYSTEM_STARTED, {"component": "test"})
    payload = strategy_simulation_payload(config)
    record_strategy_simulation_result(config, payload)

    gate = strategy_simulation_gate_payload(config)

    assert gate["status"] == ReadinessStatus.OK.value
    assert gate["matching_result"]["strategy_ids"] == ["noop"]
    enforce_live_strategy_simulation_gate(config)

    changed_config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
            strategies=StrategyRuntimeConfig(
                allow_live_execution=True,
                schedule=config.bot.strategies.schedule,
                strategy_ids=("other",),
            ),
        ),
    )
    changed_gate = strategy_simulation_gate_payload(changed_config)

    assert changed_gate["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert StrategySimulationGateIssue.CONFIG_FINGERPRINT_MISMATCH.value in changed_gate["attention_reasons"]
    assert StrategySimulationGateIssue.STRATEGY_IDS_MISMATCH.value in changed_gate["attention_reasons"]


def test_ledger_health_reports_malformed_strategy_simulation_result(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "malformed-strategy-simulation.jsonl"
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.STRATEGY_SIMULATION_RESULT,
        {
            "config_fingerprint": "fingerprint-1",
            "fingerprint_algorithm": "sha256",
            "ledger_path": ledger_path.as_posix(),
            "read_only": False,
            "schema_version": 999,
            "status": StrategySimulationStatus.OK.value,
            "strategy_ids": ["noop"],
        },
    )

    payload = ledger_health_payload(ledger_path)
    checks = {check["name"]: check for check in payload["checks"]}
    check = checks[LedgerHealthCheckName.STRATEGY_SIMULATION_CONTRACT.value]

    assert payload["status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert check["status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert check["count"] == 1
    assert "schema_version" in check["details"]["anomalies"][0]["invalid_fields"]
    assert "read_only" in check["details"]["anomalies"][0]["invalid_fields"]


def test_cli_strategy_simulation_can_fail_on_attention(workspace_tmp_path, capsys, monkeypatch):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "strategy-sim-rejection-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    before_records = AuditLedger(ledger_path).iter_records()

    def fake_configured_strategies(strategy_ids, *, static_strategies=(), strategy_parameters=None):
        assert strategy_ids == ("noop",)
        assert static_strategies == ()
        assert strategy_parameters == {}
        return (RejectedOrderStrategy(),)

    monkeypatch.setattr(
        "app.strategy_simulation.configured_strategies",
        fake_configured_strategies,
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(ledger_path),
                max_cycles=99,
                strategy_simulate=True,
                strategy_simulate_fail_on_attention=True,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == StrategySimulationStatus.ATTENTION_REQUIRED.value
    assert payload["failed_count"] == 0
    assert payload["rejected_action_count"] == 1
    assert AuditLedger(ledger_path).iter_records() == before_records


def _strategy_product(product_id: str) -> ProductMetadata:
    return ProductMetadata(
        base_increment=Decimal("0.01"),
        base_max_size=Decimal("1"),
        base_min_size=Decimal("0.01"),
        price_increment=Decimal("0.01"),
        product_id=product_id,
        product_type=ProductType.FUTURE if product_id.endswith("-CDE") else ProductType.SPOT,
        product_venue=ProductVenue.FCM if product_id.endswith("-CDE") else ProductVenue.CBE,
        quote_max_size=Decimal("1000"),
        quote_min_size=Decimal("1"),
    )
