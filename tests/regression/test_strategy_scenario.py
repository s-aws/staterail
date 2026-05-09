from __future__ import annotations

import argparse
import asyncio
import json
import os

from actions.gateway import PlaceOrderIntent
from app.bootstrap import CoinbaseApplicationConfig
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from app.strategy_scenario import strategy_scenario_payload
from audit.ledger import AuditLedger
from config.assembly import CoinbaseBotConfig, StrategyRuntimeConfig
from core.engine import AuditCore
from core.enums import (
    ActionRejectionReason,
    ActionStatus,
    ActionType,
    EventType,
    ExecutionMode,
    OrderLineageRelation,
    OrderPlacementStatus,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyManagerSkipReason,
    StrategySimulationStatus,
)
from projections.state import SourceOfTruthProjection
from risk.gate import RiskGate, RiskPolicy
from strategies import (
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    STRATEGY_SCENARIO_SCHEMA_VERSION,
    StrategyDecision,
    StrategyScenario,
    StrategyScenarioEvent,
    StrategyScenarioExpectedActionPreview,
    StrategyScenarioExpectations,
    StrategySnapshot,
    load_strategy_scenario_from_json_file,
    run_strategy_scenario,
    scenario_fill,
    scenario_open_order_import,
    scenario_order_book,
    scenario_product_snapshot,
    scenario_staged_order,
    scenario_trade,
)


class TickerOrderStrategy:
    @property
    def strategy_id(self) -> str:
        return "ticker-order"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        ticker = snapshot.projection.latest_ticker("BTC-USD")
        if ticker is None or ticker.last_price is None:
            return StrategyDecision()
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id=f"ticker-order-{snapshot.as_of_sequence}",
                    limit_price=ticker.last_price,
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
            )
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
                    limit_price="50000",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
            )
        )


def test_strategy_scenario_replays_events_and_validates_expected_preview(workspace_tmp_path):
    scenario = StrategyScenario(
        name="ticker-entry",
        events=_ticker_events(),
        expectations=StrategyScenarioExpectations(
            accepted_action_count=1,
            action_previews=(
                StrategyScenarioExpectedActionPreview(
                    action_id="ticker-order-2",
                    status=ActionStatus.ACCEPTED,
                    strategy_id="ticker-order",
                ),
            ),
            completed_count=1,
            failed_count=0,
            intent_count=1,
            rejected_action_count=0,
            status=StrategySimulationStatus.OK,
        ),
    )

    result = run_strategy_scenario(
        ledger_path=workspace_tmp_path / "scenario.jsonl",
        scenario=scenario,
        strategies=(TickerOrderStrategy(),),
    )
    payload = result.to_payload()

    assert result.passed is True
    assert result.simulation.accepted_action_count == 1
    assert result.simulation.ledger_record_count == 2
    assert AuditLedger(workspace_tmp_path / "scenario.jsonl").verify().next_sequence == 3
    assert payload["passed"] is True
    assert payload["simulation"]["evaluations"][0]["action_previews"][0]["preview"]["status"] == (
        ActionStatus.ACCEPTED.value
    )


def test_strategy_scenario_fixture_builders_seed_market_and_product_state(workspace_tmp_path):
    events = (
        scenario_product_snapshot(
            product_id="AVA-29MAY26-CDE",
            product_type=ProductType.FUTURE,
            product_venue=ProductVenue.FCM,
            base_increment="1",
            base_min_size="1",
            contract_size="1",
            price_increment="0.01",
            quote_min_size="1",
        ),
        *scenario_order_book(
            ask="101",
            ask_size="3",
            bid="99",
            bid_size="2",
            product_id="AVA-29MAY26-CDE",
            received_sequence=2,
            sequence_num=10,
        ),
        *scenario_trade(
            price="100",
            product_id="AVA-29MAY26-CDE",
            received_sequence=4,
            sequence_num=11,
            side=OrderSide.BUY,
            size="1",
            trade_id="trade-ava-1",
        ),
    )

    projection = _projection_from_scenario_events(workspace_tmp_path, events)

    product = projection.exchange_products_by_product_id["AVA-29MAY26-CDE"]
    book = projection.order_book("AVA-29MAY26-CDE")
    trades = projection.market_trades_for_product("AVA-29MAY26-CDE")

    assert product.product_type == ProductType.FUTURE
    assert product.product_venue == ProductVenue.FCM
    assert book is not None
    assert book.best_bid_price == "99"
    assert book.best_bid_size == "2"
    assert book.best_ask_price == "101"
    assert book.best_ask_size == "3"
    assert trades[0].trade_id == "trade-ava-1"
    assert trades[0].side == OrderSide.BUY
    assert trades[0].price == "100"


def test_strategy_scenario_fixture_builders_seed_order_lifecycle_state(workspace_tmp_path):
    events = (
        *scenario_staged_order(
            action_id="stage-action-1",
            limit_price="100",
            logical_order_id="logical-stage-1",
            product_id="AVA-29MAY26-CDE",
            side=OrderSide.SELL,
            size="2",
        ),
        *scenario_open_order_import(
            client_order_id="client-live-1",
            exchange_order_id="exchange-live-1",
            limit_price="99",
            product_id="AVA-29MAY26-CDE",
            received_sequence=5,
            sequence_num=20,
            side=OrderSide.BUY,
            size="1",
        ),
        scenario_fill(
            fill_id="fill-live-1",
            order_id="exchange-live-1",
            price="99",
            product_id="AVA-29MAY26-CDE",
            side=OrderSide.BUY,
            size="0.25",
            trade_id="trade-live-1",
        ),
    )

    projection = _projection_from_scenario_events(workspace_tmp_path, events)

    logical_order = projection.logical_orders_by_id["logical-stage-1"]
    staged_placement = projection.placements_by_id["stage-action-1"]
    live_order = projection.orders_by_exchange_order_id["exchange-live-1"]

    assert logical_order.created_by_action_id == "stage-action-1"
    assert staged_placement.placement_status == OrderPlacementStatus.STAGED
    assert staged_placement.logical_order_id == "logical-stage-1"
    assert live_order.client_order_id == "client-live-1"
    assert live_order.fill_ids == ["fill-live-1"]
    assert live_order.filled_size == "0.25"
    assert projection.fills_by_id["fill-live-1"].trade_id == "trade-live-1"


def test_strategy_scenario_can_expect_risk_rejected_previews(workspace_tmp_path):
    scenario = StrategyScenario(
        name="kill-switch-rejection",
        expectations=StrategyScenarioExpectations(
            accepted_action_count=0,
            action_previews=(
                StrategyScenarioExpectedActionPreview(
                    action_id="static-order-0",
                    rejection_reason=ActionRejectionReason.RISK_CHECK_FAILED,
                    status=ActionStatus.REJECTED,
                    strategy_id="static-order",
                ),
            ),
            completed_count=1,
            failed_count=0,
            intent_count=1,
            rejected_action_count=1,
            status=StrategySimulationStatus.ATTENTION_REQUIRED,
        ),
    )

    result = run_strategy_scenario(
        ledger_path=workspace_tmp_path / "risk-rejection.jsonl",
        risk_gate=RiskGate(RiskPolicy.from_values(kill_switch_enabled=True)),
        scenario=scenario,
        strategies=(StaticOrderStrategy(),),
    )

    assert result.passed is True
    assert result.simulation.rejected_action_count == 1
    assert result.expectation_failures == ()


def test_strategy_scenario_reports_expectation_failures_and_refuses_existing_ledger(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "existing.jsonl"
    AuditLedger(ledger_path).append(EventType.SYSTEM_STARTED, {"component": "test"})
    scenario = StrategyScenario(
        name="wrong-expectation",
        expectations=StrategyScenarioExpectations(
            action_previews=(
                StrategyScenarioExpectedActionPreview(
                    action_id="static-order-0",
                    command_payload={"product_id": "ETH-USD"},
                    status=ActionStatus.ACCEPTED,
                    strategy_id="static-order",
                ),
            ),
            failed_count=99,
            status=StrategySimulationStatus.OK,
        ),
    )

    try:
        run_strategy_scenario(
            ledger_path=ledger_path,
            scenario=scenario,
            strategies=(StaticOrderStrategy(),),
        )
        raise AssertionError("run_strategy_scenario should reject non-empty ledgers")
    except ValueError as exc:
        assert "empty" in str(exc)

    result = run_strategy_scenario(
        ledger_path=workspace_tmp_path / "expectation-failure.jsonl",
        risk_gate=RiskGate(RiskPolicy.from_values(kill_switch_enabled=True)),
        scenario=scenario,
        strategies=(StaticOrderStrategy(),),
    )

    assert result.passed is False
    assert {failure.check for failure in result.expectation_failures} == {
        "action_preview_command_payload",
        "action_preview_status",
        "failed_count",
        "status",
    }


def test_strategy_scenario_loads_json_fixture_and_runs_configured_strategy(workspace_tmp_path):
    scenario_path = workspace_tmp_path / "scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "schema_version": STRATEGY_SCENARIO_SCHEMA_VERSION,
                "name": "static-order-empty-ledger",
                "execution_mode": ExecutionMode.DRY_RUN.value,
                "events": [],
                "expectations": {
                    "status": StrategySimulationStatus.OK.value,
                    "accepted_action_count": 1,
                    "rejected_action_count": 0,
                    "completed_count": 1,
                    "failed_count": 0,
                    "intent_count": 1,
                    "action_previews": [
                        {
                            "action_id": "static-order-0",
                            "status": ActionStatus.ACCEPTED.value,
                            "strategy_id": "static-order",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    loaded_scenario = load_strategy_scenario_from_json_file(scenario_path)

    payload = strategy_scenario_payload(
        CoinbaseApplicationConfig(
            ledger_path=workspace_tmp_path / "fixture-ledger.jsonl",
            bot=CoinbaseBotConfig(
                strategies=StrategyRuntimeConfig(strategy_ids=("static-order",)),
            ),
        ),
        scenario_file=scenario_path,
        strategies=(StaticOrderStrategy(),),
    )

    assert loaded_scenario.name == "static-order-empty-ledger"
    assert payload["passed"] is True
    assert payload["simulation"]["accepted_action_count"] == 1
    assert payload["scenario"]["schema_version"] == STRATEGY_SCENARIO_SCHEMA_VERSION


def test_strategy_scenario_loads_static_intent_strategy_from_json(workspace_tmp_path):
    scenario_path = workspace_tmp_path / "static-intent-scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "schema_version": STRATEGY_SCENARIO_SCHEMA_VERSION,
                "name": "static-intent-order",
                "execution_mode": ExecutionMode.DRY_RUN.value,
                "strategy_ids": ["static-intent-fixture"],
                "static_strategies": [
                    {
                        "strategy_id": "static-intent-fixture",
                        "metadata": {"purpose": "regression"},
                        "intents": [
                            {
                                "action_type": "order.place",
                                "action_id": "static-intent-fixture-entry-1",
                                "product_id": "BTC-USD",
                                "side": OrderSide.BUY.value,
                                "order_type": OrderType.LIMIT.value,
                                "size": "0.01",
                                "limit_price": "50000",
                                "placement_kind": OrderPlacementKind.STAGED_RELEASE.value,
                            }
                        ],
                    }
                ],
                "expectations": {
                    "status": StrategySimulationStatus.OK.value,
                    "accepted_action_count": 1,
                    "rejected_action_count": 0,
                    "completed_count": 1,
                    "failed_count": 0,
                    "intent_count": 1,
                    "action_previews": [
                        {
                            "action_id": "static-intent-fixture-entry-1",
                            "status": ActionStatus.ACCEPTED.value,
                            "strategy_id": "static-intent-fixture",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    loaded_scenario = load_strategy_scenario_from_json_file(scenario_path)

    payload = strategy_scenario_payload(
        CoinbaseApplicationConfig(ledger_path=workspace_tmp_path / "static-intent-ledger.jsonl"),
        scenario_file=scenario_path,
    )

    assert loaded_scenario.strategy_ids == ("static-intent-fixture",)
    assert loaded_scenario.static_strategies[0].strategy_id == "static-intent-fixture"
    assert payload["passed"] is True
    assert payload["scenario"]["static_strategies"][0]["intent_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 1


def test_cli_strategy_scenario_runs_checked_in_noop_fixture(workspace_tmp_path, capsys, monkeypatch):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.noop.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "noop-empty-ledger"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["intent_count"] == 0


def test_cli_strategy_scenario_runs_checked_in_staged_order_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-staged-order-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.staged-order.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "staged-order-preview"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 1


def test_cli_strategy_scenario_runs_checked_in_staged_release_manager_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-staged-release-manager-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.staged-release-manager.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.staged-release-manager.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    previews = payload["simulation"]["evaluations"][0]["action_previews"]
    command_payload = previews[0]["command"]["payload"]

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "staged-release-manager-releases-existing-stage"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 1
    assert command_payload["logical_order_id"] == "logical-stage-shb-1"
    assert command_payload["placement_kind"] == OrderPlacementKind.RELEASE.value
    assert command_payload["metadata"]["staged_release"]["release_of_placement_id"] == "stage-shb-1"


def test_cli_strategy_scenario_runs_checked_in_staged_release_manager_blocked_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-staged-release-manager-blocked-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.staged-release-manager.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file=(
                    "docs/examples/strategy-scenario.staged-release-manager-blocked.json"
                ),
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "staged-release-manager-skips-crossing-stage"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 0
    assert payload["simulation"]["intent_count"] == 0
    assert payload["simulation"]["evaluations"][0]["metadata"]["skipped_staged_placements"] == [
        {
            "best_ask_price": "99.50",
            "best_bid_price": "98",
            "limit_price": "100",
            "matched": False,
            "placement_id": "stage-shb-crossing-1",
            "reason": StrategyManagerSkipReason.RELEASE_CONDITIONS_NOT_MATCHED.value,
            "side": OrderSide.BUY.value,
        }
    ]


def test_cli_strategy_scenario_runs_checked_in_followup_manager_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-followup-manager-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.staged-release-manager.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.followup-on-fill-manager.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    previews = payload["simulation"]["evaluations"][0]["action_previews"]
    command_payload = previews[0]["command"]["payload"]

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "followup-on-fill-manager-creates-child"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 1
    assert command_payload["lineage_relation"] == OrderLineageRelation.FOLLOWUP_AFTER_FILL.value
    assert command_payload["parent_order_id"] == "parent-action"
    assert command_payload["root_order_id"] == "parent-action"
    assert command_payload["source_order_ids"] == ["parent-action"]
    assert command_payload["metadata"]["followup_after_fill"]["fill_id"] == "fill-1"


def test_cli_strategy_scenario_runs_checked_in_consolidation_manager_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-consolidation-manager-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.staged-release-manager.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.consolidation-manager.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    previews = payload["simulation"]["evaluations"][0]["action_previews"]
    consolidation_payload = previews[2]["command"]["payload"]

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "consolidation-manager-tidies-duplicate-orders"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 3
    assert [preview["command"]["action_type"] for preview in previews] == [
        ActionType.CANCEL_ORDER.value,
        ActionType.CANCEL_ORDER.value,
        ActionType.PLACE_ORDER.value,
    ]
    assert previews[0]["command"]["payload"]["exchange_order_id"] == "exchange-source-a"
    assert previews[1]["command"]["payload"]["exchange_order_id"] == "exchange-source-b"
    assert consolidation_payload["lineage_relation"] == OrderLineageRelation.CONSOLIDATION.value
    assert consolidation_payload["product_id"] == "SHB-26JUN26-CDE"
    assert consolidation_payload["side"] == OrderSide.SELL.value
    assert consolidation_payload["size"] == "0.10"
    assert consolidation_payload["source_order_ids"] == ["source-a", "source-b"]


def test_cli_strategy_scenario_runs_checked_in_anchor_repricing_manager_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-anchor-repricing-manager-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.anchor-repricing-manager.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.anchor-repricing-manager.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    previews = payload["simulation"]["evaluations"][0]["action_previews"]
    cancel_payload = previews[0]["command"]["payload"]
    replacement_payload = previews[1]["command"]["payload"]

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "anchor-repricing-manager-moves-drifted-order"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 2
    assert [preview["command"]["action_type"] for preview in previews] == [
        ActionType.CANCEL_ORDER.value,
        ActionType.PLACE_ORDER.value,
    ]
    assert payload["simulation"]["evaluations"][0]["strategy_id"] == (
        ANCHOR_REPRICING_MANAGER_STRATEGY_ID
    )
    assert cancel_payload["client_order_id"] == "client-source-anchor"
    assert cancel_payload["exchange_order_id"] == "exchange-source-anchor"
    assert replacement_payload["logical_order_id"] == "logical-anchor"
    assert replacement_payload["limit_price"] == "99.75"
    assert replacement_payload["placement_kind"] == OrderPlacementKind.CANCEL_REPLACE.value
    assert replacement_payload["side"] == OrderSide.BUY.value
    assert replacement_payload["metadata"]["anchor_repricing"]["source_action_id"] == "source-anchor"
    assert replacement_payload["metadata"]["anchor_repricing"]["target_price"] == "99.75"


def test_cli_strategy_scenario_runs_checked_in_passive_market_making_fixture(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    ledger_path = workspace_tmp_path / "checked-in-passive-market-making-scenario.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.passive-market-making.dry-run.json",
                ledger_path=str(ledger_path),
                strategy_scenario_file="docs/examples/strategy-scenario.passive-market-making.json",
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    previews = payload["simulation"]["evaluations"][0]["action_previews"]
    buy_payload = previews[0]["command"]["payload"]
    sell_payload = previews[1]["command"]["payload"]

    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["scenario"]["name"] == "passive-market-making-stages-hidden-quotes"
    assert payload["simulation"]["strategy_count"] == 1
    assert payload["simulation"]["accepted_action_count"] == 2
    assert [preview["command"]["action_type"] for preview in previews] == [
        ActionType.PLACE_ORDER.value,
        ActionType.PLACE_ORDER.value,
    ]
    assert buy_payload["placement_kind"] == OrderPlacementKind.STAGED_RELEASE.value
    assert buy_payload["side"] == OrderSide.BUY.value
    assert buy_payload["limit_price"] == "99.50"
    assert buy_payload["leverage"] == "1"
    assert buy_payload["margin_type"] == "cross"
    assert sell_payload["placement_kind"] == OrderPlacementKind.STAGED_RELEASE.value
    assert sell_payload["side"] == OrderSide.SELL.value
    assert sell_payload["limit_price"] == "100.50"
    assert sell_payload["leverage"] == "1"
    assert sell_payload["margin_type"] == "cross"
    assert buy_payload["metadata"]["passive_market_making"]["midpoint"] == "100"
    assert sell_payload["metadata"]["passive_market_making"]["midpoint"] == "100"


def test_cli_strategy_scenario_requires_explicit_ledger_path():
    try:
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file="docs/examples/config.dry-run.json",
                    ledger_path=None,
                    strategy_scenario_file="docs/examples/strategy-scenario.noop.json",
                )
            )
        )
        raise AssertionError("strategy scenario CLI should require an explicit ledger path")
    except ValueError as exc:
        assert "--ledger-path" in str(exc)


def test_cli_strategy_scenario_returns_attention_exit_code_on_expectation_failure(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    for key in list(os.environ):
        if key.startswith("STATERAIL_"):
            monkeypatch.delenv(key, raising=False)
    scenario_path = workspace_tmp_path / "failing-scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "schema_version": STRATEGY_SCENARIO_SCHEMA_VERSION,
                "name": "noop-failing-expectation",
                "events": [],
                "expectations": {
                    "status": StrategySimulationStatus.OK.value,
                    "completed_count": 99,
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(workspace_tmp_path / "failing-scenario.jsonl"),
                strategy_scenario_file=str(scenario_path),
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["passed"] is False
    assert payload["expectation_failures"][0]["check"] == "completed_count"


def _projection_from_scenario_events(
    workspace_tmp_path,
    events: tuple[StrategyScenarioEvent, ...],
) -> SourceOfTruthProjection:
    ledger = AuditLedger(workspace_tmp_path / "builder-events.jsonl")
    core = AuditCore(ledger)
    for event in events:
        core.emit(event.event_type, event.payload)
    return SourceOfTruthProjection.from_ledger(ledger)


def _ticker_events() -> tuple[StrategyScenarioEvent, ...]:
    return (
        StrategyScenarioEvent(
            event_type=EventType.DATA_RECEIVED,
            payload={
                "message_event_type": EventType.DATA_RECEIVED.value,
                "message_key": "coinbase:ticker:1",
                "payload": {
                    "channel": "ticker",
                    "raw": {
                        "channel": "ticker",
                        "events": [
                            {
                                "tickers": [
                                    {
                                        "price": "50000",
                                        "product_id": "BTC-USD",
                                    }
                                ]
                            }
                        ],
                        "sequence_num": 1,
                    },
                    "sequence_num": 1,
                },
                "source_id": "coinbase-primary",
            },
        ),
        StrategyScenarioEvent(
            event_type=EventType.DATA_ACCEPTED,
            payload={
                "message_event_type": EventType.DATA_RECEIVED.value,
                "message_key": "coinbase:ticker:1",
                "received_sequence": 1,
                "source_id": "coinbase-primary",
            },
        ),
    )
