from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionGateway, CancelOrderIntent, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.errors import StrategyContractError, StrategyInputUnavailableError
from core.enums import (
    ActionRejectionReason,
    ActionStatus,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    IncrementRoundingMode,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    MarketDataKind,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSizingDecisionStatus,
    OrderSide,
    OrderType,
    OperatorPolicyPermission,
    ProductType,
    ProductVenue,
    ProductRuleCheckStatus,
    ProductRuleFailure,
    RiskCheckStatus,
    ScheduledSliceStatus,
    StrategyHelperStatus,
    StrategyEvaluationStatus,
    StrategyInputStatus,
    StrategyMarketDataStatus,
)
from feeds.router import FeedMessage, RedundantFeedRouter
from app.ledger_health import ledger_health
from orders.sizing import OrderSizingDecision
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import (
    MarketOrderBookSnapshot,
    OrderSnapshot,
    PositionSnapshot,
    SourceOfTruthProjection,
)
from risk.gate import RiskGate, RiskPolicy
from strategies import (
    NoOpStrategy,
    StrategyDecision,
    StrategyEvaluationTask,
    StrategyInputRequirement,
    StrategySnapshot,
    load_operator_policy_from_json_file,
    select_strategies,
    strategy_action_id,
    strategy_client_order_id,
    strategy_consolidation_intent,
    strategy_decision_commands,
    strategy_followup_after_fill_intent,
    strategy_release_staged_placement_intent,
    strategy_split_order_intents,
    strategy_staged_release_intents,
)


class StaticOrderStrategy:
    def __init__(self) -> None:
        self.snapshots: list[StrategySnapshot] = []

    @property
    def strategy_id(self) -> str:
        return "static-order"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        self.snapshots.append(snapshot)
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id="strategy-action-1",
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    size="0.01",
                    limit_price="50000",
                    requested_by="ignored-by-harness",
                ),
            ),
            metadata={"observed_sequence": snapshot.as_of_sequence},
        )


class DuplicateActionIdStrategy:
    @property
    def strategy_id(self) -> str:
        return "duplicate-action"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        del snapshot
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id="duplicate-action-id",
                    limit_price="50000",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
                PlaceOrderIntent(
                    action_id="duplicate-action-id",
                    limit_price="50001",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
            )
        )


class TwoOrderStrategy:
    @property
    def strategy_id(self) -> str:
        return "two-order"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        del snapshot
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id="first-strategy-action",
                    limit_price="50000",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
                PlaceOrderIntent(
                    action_id="second-strategy-action",
                    limit_price="50001",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
            )
        )


class FailingStrategy:
    @property
    def strategy_id(self) -> str:
        return "failing"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        del snapshot
        raise RuntimeError("strategy failed")


class InvalidReturnStrategy:
    @property
    def strategy_id(self) -> str:
        return "invalid-return"

    def evaluate(self, snapshot: StrategySnapshot):
        del snapshot
        return {"intents": []}


class TradeWindowMetadataStrategy:
    @property
    def strategy_id(self) -> str:
        return "trade-window-metadata"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        window = snapshot.trade_window("AVA-29MAY26-CDE", lookback=timedelta(minutes=5))
        return StrategyDecision(
            metadata={
                "projection_retention": snapshot.projection.to_payload()["market_trade_retention"],
                "trade_window": window.to_payload(),
            }
        )


class OrderBookWindowMetadataStrategy:
    @property
    def strategy_id(self) -> str:
        return "order-book-window-metadata"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        window = snapshot.order_book_sample_window(
            "AVA-29MAY26-CDE",
            lookback=timedelta(minutes=5),
        )
        return StrategyDecision(
            metadata={
                "order_book_window": window.to_payload(),
                "projection_retention": snapshot.projection.to_payload()[
                    "market_order_book_sample_retention"
                ],
            }
        )


def test_strategy_task_audits_evaluation_and_submits_intents_through_gateway(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    strategy = StaticOrderStrategy()
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        operator_policy=operator_policy,
        strategies=(strategy,),
    )

    result = task.run()
    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert result["completed_count"] == 1
    assert result["failed_count"] == 0
    assert result["submitted_action_count"] == 1
    assert [record.event_type for record in records] == [
        EventType.STRATEGY_EVALUATION_STARTED,
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTED,
        EventType.ORDER_PLACEMENT_RECORDED,
        EventType.STRATEGY_EVALUATION_COMPLETED,
    ]
    assert records[1].payload["requested_by"] == "strategy:static-order"
    assert records[-1].payload["strategy_id"] == "static-order"
    assert records[-1].payload["started_sequence"] == records[0].sequence
    assert records[-1].payload["action_receipts"][0]["status"] == ActionStatus.EXECUTED.value
    assert strategy.snapshots[0].as_of_sequence == 0
    assert strategy.snapshots[0].execution_mode == ExecutionMode.DRY_RUN
    assert strategy.snapshots[0].operator_policy is operator_policy
    assert projection.strategy_evaluations[0].strategy_id == "static-order"
    assert projection.strategy_evaluations[0].status == StrategyEvaluationStatus.COMPLETED
    assert projection.strategy_evaluations[0].action_ids == ["strategy-action-1"]
    assert projection.actions["strategy-action-1"].status == ActionStatus.EXECUTED


def test_strategy_task_stops_after_failed_action_receipt(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(
            core,
            risk_gate=RiskGate(RiskPolicy.from_values(kill_switch_enabled=True)),
        ),
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        strategies=(TwoOrderStrategy(),),
    )

    result = task.run()
    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    failed_payload = records[-1].payload
    action_receipt = failed_payload["action_receipts"][0]

    assert result["completed_count"] == 0
    assert result["failed_count"] == 1
    assert result["submitted_action_count"] == 1
    assert [record.event_type for record in records] == [
        EventType.STRATEGY_EVALUATION_STARTED,
        EventType.ACTION_REQUESTED,
        EventType.ACTION_REJECTED,
        EventType.ERROR,
        EventType.STRATEGY_EVALUATION_FAILED,
    ]
    assert records[3].payload["error_code"] == ErrorCode.STRATEGY_ACTION_FAILED.value
    assert records[3].payload["action_id"] == "first-strategy-action"
    assert action_receipt["action_id"] == "first-strategy-action"
    assert action_receipt["status"] == ActionStatus.REJECTED.value
    assert action_receipt["rejection_reason"] == ActionRejectionReason.RISK_CHECK_FAILED.value
    assert failed_payload["intent_count"] == 2
    assert failed_payload["submitted_action_count"] == 1
    assert "second-strategy-action" not in projection.actions


def test_strategy_snapshot_reports_market_data_freshness(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:ticker:1",
            "payload": {
                "channel": "ticker",
                "raw": {
                    "channel": "ticker",
                    "events": [{"tickers": [{"price": "50000", "product_id": "BTC-USD"}]}],
                    "sequence_num": 1,
                },
                "sequence_num": 1,
            },
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:ticker:1",
            "received_sequence": received.sequence,
            "source_id": "coinbase-primary",
        },
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at + timedelta(seconds=5),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        projection=projection,
    )

    fresh = snapshot.market_data_freshness(
        data_kind=MarketDataKind.TICKER,
        max_age=timedelta(seconds=10),
        product_id="BTC-USD",
    )
    stale = snapshot.market_data_freshness(
        data_kind=MarketDataKind.TICKER,
        max_age=timedelta(seconds=1),
        product_id="BTC-USD",
    )
    missing = snapshot.market_data_freshness(
        data_kind=MarketDataKind.TRADE,
        max_age=timedelta(seconds=10),
        product_id="ETH-USD",
    )

    assert fresh.is_ok is True
    assert fresh.status == StrategyInputStatus.OK
    assert fresh.age_seconds == 5
    assert fresh.observed_at == observed_at
    assert fresh.sequence == 2
    assert fresh.to_payload()["status"] == StrategyInputStatus.OK.value
    assert stale.status == StrategyInputStatus.STALE
    assert stale.is_ok is False
    assert missing.status == StrategyInputStatus.MISSING
    assert missing.sequence is None


def test_strategy_snapshot_plans_staged_release_sizes_from_policy_and_catalog(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=operator_policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    decision = snapshot.plan_staged_release_sizes(
        product_id="SHB-26JUN26-CDE",
        total_size="0.25",
        limit_price="100",
    )

    assert decision.status == OrderSizingDecisionStatus.ACCEPTED
    assert decision.output_sizes == (Decimal("0.25"),)
    assert decision.to_payload()["output_sizes"] == ["0.25"]


def test_strategy_snapshot_plans_staged_release_sizes_with_explicit_visible_cap(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((_strategy_product("BTC-USD"),)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    decision = snapshot.plan_staged_release_sizes(
        product_id="BTC-USD",
        total_size="0.25",
        limit_price="100",
        max_visible_notional="20",
    )

    assert decision.status == OrderSizingDecisionStatus.ACCEPTED
    assert decision.output_sizes == (Decimal("0.20"), Decimal("0.05"))


def test_strategy_snapshot_validates_product_rules_and_increment_proposals(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    product = _strategy_product("BTC-USD")
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((product,)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    size_ok = snapshot.validate_order_size(product_id="BTC-USD", size="0.25")
    size_bad_increment = snapshot.validate_order_size(product_id="BTC-USD", size="0.255")
    size_too_large = snapshot.validate_order_size(product_id="BTC-USD", size="2")
    price_bad_increment = snapshot.validate_limit_price(product_id="BTC-USD", price="100.001")
    notional_ok = snapshot.validate_notional(product_id="BTC-USD", size="0.01", price="100")
    notional_too_small = snapshot.validate_notional(product_id="BTC-USD", size="0.01", price="50")
    price_down = snapshot.price_tick_proposal(
        product_id="BTC-USD",
        price="100.003",
        mode=IncrementRoundingMode.DOWN,
    )
    price_up = snapshot.price_tick_proposal(
        product_id="BTC-USD",
        price="100.003",
        mode=IncrementRoundingMode.UP,
    )
    size_nearest = snapshot.size_increment_proposal(
        product_id="BTC-USD",
        size="0.255",
        mode=IncrementRoundingMode.NEAREST,
    )
    size_down_rejected = snapshot.size_increment_proposal(
        product_id="BTC-USD",
        size="0.004",
        mode=IncrementRoundingMode.DOWN,
    )
    missing_catalog_snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    assert size_ok.status == ProductRuleCheckStatus.ACCEPTED
    assert size_ok.failures == ()
    assert size_bad_increment.status == ProductRuleCheckStatus.REJECTED
    assert size_bad_increment.failures == (ProductRuleFailure.SIZE_INCREMENT,)
    assert size_too_large.failures == (ProductRuleFailure.SIZE_ABOVE_MAX,)
    assert price_bad_increment.failures == (ProductRuleFailure.PRICE_INCREMENT,)
    assert notional_ok.status == ProductRuleCheckStatus.ACCEPTED
    assert notional_ok.notional == Decimal("1.00")
    assert notional_too_small.failures == (ProductRuleFailure.NOTIONAL_BELOW_MIN,)
    assert price_down.proposed_value == Decimal("100.00")
    assert price_down.changed is True
    assert price_up.proposed_value == Decimal("100.01")
    assert size_nearest.proposed_value == Decimal("0.26")
    assert size_down_rejected.status == ProductRuleCheckStatus.REJECTED
    assert size_down_rejected.failures == (ProductRuleFailure.VALUE_NOT_POSITIVE,)
    assert size_down_rejected.proposed_value == Decimal("0.00")
    assert size_bad_increment.to_payload()["failures"] == ["size_increment"]
    assert price_down.to_payload()["mode"] == IncrementRoundingMode.DOWN.value
    with pytest.raises(StrategyInputUnavailableError, match="product catalog"):
        missing_catalog_snapshot.validate_order_size(product_id="BTC-USD", size="0.25")


def test_strategy_snapshot_builds_quote_pair_intents_and_ladder_plan(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    product_id = "AVA-29MAY26-CDE"
    projection = SourceOfTruthProjection.from_ledger(ledger)
    projection.order_books_by_product_id[product_id] = MarketOrderBookSnapshot(
        ask_levels={"101": "5"},
        best_ask_price="101",
        best_ask_size="5",
        best_bid_price="99",
        best_bid_size="4",
        bid_levels={"99": "4"},
        message_key="book-1",
        observed_at=now,
        product_id=product_id,
        sequence=1,
    )
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=1,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product(product_id),)),
        projection=projection,
    )

    ladder = snapshot.ladder_plan(
        anchor_price="100",
        levels=3,
        product_id=product_id,
        side=OrderSide.BUY,
        size_per_level="1",
        step_bps="100",
    )
    first_intents = snapshot.quote_pair_intents(
        product_id=product_id,
        size="1",
        spread_bps="100",
        strategy_id="example",
        max_release_count=1,
    )
    second_intents = snapshot.quote_pair_intents(
        product_id=product_id,
        size="1",
        spread_bps="100",
        strategy_id="example",
        max_release_count=1,
    )
    commands = strategy_decision_commands("example", StrategyDecision(intents=first_intents))

    assert ladder.status == ProductRuleCheckStatus.ACCEPTED
    assert [row.price for row in ladder.rows] == [
        Decimal("100.00"),
        Decimal("99.00"),
        Decimal("98.00"),
    ]
    assert all(row.notional == row.price for row in ladder.rows)
    assert len(first_intents) == 2
    assert [intent.side for intent in first_intents] == [OrderSide.BUY, OrderSide.SELL]
    assert [intent.limit_price for intent in first_intents] == ["99.50", "100.50"]
    assert all(intent.placement_kind == OrderPlacementKind.STAGED_RELEASE for intent in first_intents)
    assert all(intent.post_only is True for intent in first_intents)
    assert all(intent.reduce_only is True for intent in first_intents)
    assert all(intent.leverage == "1" for intent in first_intents)
    assert all(intent.metadata["quote_pair"]["midpoint"] == "100" for intent in first_intents)
    assert [intent.action_id for intent in first_intents] == [
        intent.action_id for intent in second_intents
    ]
    assert len({intent.logical_order_id for intent in first_intents}) == 2
    assert all(command.requested_by == "strategy:example" for command in commands)


def test_strategy_snapshot_quote_pair_intents_fail_closed(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    product_id = "AVA-29MAY26-CDE"
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    missing_book_snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product(product_id),)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )
    missing_policy_snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((_strategy_product(product_id),)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    with pytest.raises(StrategyInputUnavailableError, match="order book midpoint"):
        missing_book_snapshot.quote_pair_intents(
            product_id=product_id,
            size="1",
            spread_bps="100",
            strategy_id="example",
        )
    with pytest.raises(StrategyContractError, match="operator policy"):
        missing_policy_snapshot.quote_pair_intents(
            product_id=product_id,
            size="1",
            spread_bps="100",
            strategy_id="example",
        )


def test_strategy_snapshot_plans_scheduled_slices_from_action_history(workspace_tmp_path):
    now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(now))
    gateway = ActionGateway(AuditCore(ledger))
    product_id = "BTC-USD"

    def snapshot(at: datetime) -> StrategySnapshot:
        projection = SourceOfTruthProjection.from_ledger(ledger)
        return StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            product_catalog=ProductCatalog((_strategy_product(product_id),)),
            projection=projection,
        )

    first = snapshot(now).scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="twap-entry",
        slices=3,
        strategy_id="example",
        total_size="0.6",
    )
    assert first.status == ScheduledSliceStatus.DUE
    assert first.slice_index == 1
    assert first.slice_size == Decimal("0.2")
    assert first.suggested_action_id is not None
    assert first.suggested_client_order_id is not None

    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id=first.suggested_action_id,
            idempotency_key=first.suggested_client_order_id,
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id=product_id,
            side=OrderSide.BUY,
            size=str(first.slice_size),
        ).to_command(),
        DryRunExecutor(),
    )

    not_due = snapshot(now + timedelta(minutes=5)).scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="twap-entry",
        slices=3,
        strategy_id="example",
        total_size="0.6",
    )
    due = snapshot(now + timedelta(minutes=11)).scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="twap-entry",
        slices=3,
        strategy_id="example",
        total_size="0.6",
    )

    assert not_due.status == ScheduledSliceStatus.NOT_DUE
    assert not_due.completed_slice_count == 1
    assert not_due.slice_index == 2
    assert not_due.due_in_seconds == 300
    assert due.status == ScheduledSliceStatus.DUE
    assert due.completed_action_ids == (first.suggested_action_id,)
    assert due.slice_index == 2
    assert due.suggested_action_id != first.suggested_action_id
    assert due.to_payload()["status"] == ScheduledSliceStatus.DUE.value
    assert due.suggested_action_id is not None
    assert due.suggested_client_order_id is not None

    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id=due.suggested_action_id,
            idempotency_key=due.suggested_client_order_id,
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id=product_id,
            side=OrderSide.BUY,
            size=str(due.slice_size),
        ).to_command(),
        DryRunExecutor(),
    )
    third = snapshot(now + timedelta(minutes=11)).scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="twap-entry",
        slices=3,
        strategy_id="example",
        total_size="0.6",
    )
    assert third.status == ScheduledSliceStatus.DUE
    assert third.slice_index == 3
    assert third.suggested_action_id is not None
    assert third.suggested_client_order_id is not None

    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id=third.suggested_action_id,
            idempotency_key=third.suggested_client_order_id,
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id=product_id,
            side=OrderSide.BUY,
            size=str(third.slice_size),
        ).to_command(),
        DryRunExecutor(),
    )
    complete = snapshot(now + timedelta(minutes=11)).scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="twap-entry",
        slices=3,
        strategy_id="example",
        total_size="0.6",
    )
    assert complete.status == ScheduledSliceStatus.COMPLETE
    assert complete.completed_slice_count == 3
    assert complete.remaining_slice_count == 0


def test_strategy_snapshot_scheduled_slice_plan_reports_blocked_states(
    workspace_tmp_path,
):
    now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(now))
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(RiskPolicy.from_values(kill_switch_enabled=True)),
    )
    product_id = "BTC-USD"
    base_snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((_strategy_product(product_id),)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )
    blocked_size = base_snapshot.scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        slices=3,
        strategy_id="example",
        total_size="0.01",
    )
    rejected_slice = base_snapshot.scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="blocked",
        slices=1,
        strategy_id="example",
        total_size="0.1",
    )
    assert rejected_slice.suggested_action_id is not None

    gateway.submit(
        PlaceOrderIntent(
            action_id=rejected_slice.suggested_action_id,
            idempotency_key=rejected_slice.suggested_client_order_id,
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id=product_id,
            side=OrderSide.BUY,
            size=str(rejected_slice.slice_size),
        ).to_command()
    )
    blocked_snapshot = StrategySnapshot(
        as_of_sequence=SourceOfTruthProjection.from_ledger(ledger).last_sequence,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((_strategy_product(product_id),)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )
    blocked_action = blocked_snapshot.scheduled_slice_plan(
        interval=timedelta(minutes=10),
        product_id=product_id,
        schedule_id="blocked",
        slices=1,
        strategy_id="example",
        total_size="0.1",
    )

    assert blocked_size.status == ScheduledSliceStatus.BLOCKED
    assert ProductRuleFailure.SIZE_BELOW_MIN in blocked_size.size_failures
    assert blocked_action.status == ScheduledSliceStatus.BLOCKED
    assert blocked_action.reasons == ("slice 1 action is rejected",)


def test_strategy_snapshot_reports_product_exposure_and_order_capacity(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    product = ProductMetadata(
        base_increment=Decimal("1"),
        base_min_size=Decimal("1"),
        contract_size=Decimal("10"),
        price_increment=Decimal("0.01"),
        product_id="AVA-29MAY26-CDE",
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_max_size=Decimal("100000000"),
        quote_min_size=Decimal("0"),
    )
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    policy = replace(
        policy,
        risk_limits=replace(policy.risk_limits, kill_switch_enabled=False),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    projection.record_occurred_at_by_sequence[10] = now
    projection.record_occurred_at_by_sequence[11] = now
    projection.orders_by_action_id["open-buy"] = OrderSnapshot(
        accepted_sequence=10,
        action_id="open-buy",
        lifecycle_status=OrderLifecycleStatus.OPEN,
        limit_price="2",
        product_id="AVA-29MAY26-CDE",
        side=OrderSide.BUY,
        size="1",
    )
    projection.orders_by_action_id["filled-sell"] = OrderSnapshot(
        accepted_sequence=11,
        action_id="filled-sell",
        lifecycle_status=OrderLifecycleStatus.FILLED,
        limit_price="3",
        product_id="AVA-29MAY26-CDE",
        side=OrderSide.SELL,
        size="2",
    )
    projection.positions_by_product_id["AVA-29MAY26-CDE"] = PositionSnapshot(
        fill_count=2,
        gross_buy_notional="4",
        gross_buy_size="2",
        gross_sell_notional="6",
        gross_sell_size="2",
        net_size="0",
        product_id="AVA-29MAY26-CDE",
    )
    snapshot = StrategySnapshot(
        as_of_sequence=11,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((product,)),
        projection=projection,
    )

    exposure = snapshot.product_exposure("AVA-29MAY26-CDE")
    capacity = snapshot.order_capacity("AVA-29MAY26-CDE", side=OrderSide.BUY)
    blocked_capacity = snapshot.order_capacity("UNKNOWN-CDE", side=OrderSide.BUY)
    missing_policy_snapshot = StrategySnapshot(
        as_of_sequence=11,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((product,)),
        projection=projection,
    )

    assert exposure.status == StrategyHelperStatus.OK
    assert exposure.open_order_count == 1
    assert exposure.open_buy_order_count == 1
    assert exposure.open_sell_order_count == 0
    assert exposure.open_order_notional == Decimal("20")
    assert exposure.open_buy_notional == Decimal("20")
    assert exposure.net_size == Decimal("0")
    assert exposure.gross_buy_size == Decimal("2")
    assert exposure.gross_sell_size == Decimal("2")
    assert exposure.fill_count == 2
    assert exposure.to_payload()["open_order_notional"] == "20"
    assert capacity.status == RiskCheckStatus.PASS
    assert capacity.product_allowed is True
    assert capacity.side_allowed is True
    assert capacity.kill_switch_enabled is False
    assert capacity.daily_notional_used == Decimal("80")
    assert capacity.remaining_daily_notional == Decimal("320")
    assert capacity.open_order_count == 1
    assert capacity.product_open_order_count == 1
    assert capacity.remaining_open_order_slots == 3
    assert capacity.remaining_max_order_notional == Decimal("200")
    assert capacity.to_payload()["status"] == RiskCheckStatus.PASS.value
    assert blocked_capacity.status == RiskCheckStatus.FAIL
    assert blocked_capacity.product_allowed is False
    assert (
        missing_policy_snapshot.order_capacity("AVA-29MAY26-CDE", side=OrderSide.BUY).status
        == RiskCheckStatus.FAIL
    )


def test_strategy_snapshot_plans_staged_release_sizes_with_future_contract_size(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    product = ProductMetadata(
        base_increment=Decimal("1"),
        base_min_size=Decimal("1"),
        contract_size=Decimal("10"),
        price_increment=Decimal("0.01"),
        product_id="AVA-29MAY26-CDE",
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_max_size=Decimal("100000000"),
        quote_min_size=Decimal("0"),
    )
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((product,)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    decision = snapshot.plan_staged_release_sizes(
        product_id="AVA-29MAY26-CDE",
        total_size="3",
        limit_price="2",
        max_visible_notional="25",
    )

    assert decision.status == OrderSizingDecisionStatus.ACCEPTED
    assert decision.output_sizes == (Decimal("1"), Decimal("1"), Decimal("1"))


def test_strategy_snapshot_rejects_future_min_contract_above_visible_cap(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    product = ProductMetadata(
        base_increment=Decimal("1"),
        base_min_size=Decimal("1"),
        contract_size=Decimal("10000"),
        price_increment=Decimal("0.00001"),
        product_id="SHB-26JUN26-CDE",
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_max_size=Decimal("100000000"),
        quote_min_size=Decimal("0"),
    )
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        product_catalog=ProductCatalog((product,)),
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    decision = snapshot.plan_staged_release_sizes(
        product_id="SHB-26JUN26-CDE",
        total_size="1",
        limit_price="0.00636",
        max_visible_notional="10",
    )

    assert decision.status == OrderSizingDecisionStatus.REJECTED
    assert decision.reasons == ("max_visible_notional is below minimum valid release size",)


def test_strategy_snapshot_staged_release_planning_reports_input_and_policy_errors(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    missing_catalog_snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        projection=projection,
    )
    out_of_scope_snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=operator_policy,
        product_catalog=ProductCatalog((_strategy_product("BTC-USD"),)),
        projection=projection,
    )

    with pytest.raises(StrategyInputUnavailableError, match="product catalog"):
        missing_catalog_snapshot.plan_staged_release_sizes(
            product_id="BTC-USD",
            total_size="0.25",
            limit_price="100",
            max_visible_notional="20",
        )
    with pytest.raises(StrategyContractError, match="outside operator policy scope"):
        out_of_scope_snapshot.plan_staged_release_sizes(
            product_id="BTC-USD",
            total_size="0.25",
            limit_price="100",
        )


def test_strategy_task_blocks_stale_or_missing_required_market_data(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    strategy = StaticOrderStrategy()
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        market_data_requirements=(
            StrategyInputRequirement(
                data_kind=MarketDataKind.TICKER,
                max_age=timedelta(seconds=5),
                product_id="BTC-USD",
            ),
        ),
        strategies=(strategy,),
    )

    result = task.run()
    records = ledger.iter_records()

    assert result["failed_count"] == 1
    assert result["submitted_action_count"] == 0
    assert strategy.snapshots == []
    assert EventType.ACTION_REQUESTED not in [record.event_type for record in records]
    assert records[1].event_type == EventType.ERROR
    assert records[1].payload["error_code"] == ErrorCode.STRATEGY_INPUT_UNAVAILABLE.value
    assert records[2].event_type == EventType.STRATEGY_EVALUATION_FAILED
    assert records[2].payload["input_freshness"][0]["status"] == StrategyInputStatus.MISSING.value


def test_strategy_task_applies_market_trade_retention_cap_to_snapshots(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core, clock=clock)
    for sequence in range(1, 4):
        router.ingest(
            FeedMessage(
                source_id="coinbase-primary",
                message_key=f"coinbase:market_trades:{sequence}",
                event_type=EventType.DATA_RECEIVED,
                payload={
                    "channel": "market_trades",
                    "raw": {
                        "channel": "market_trades",
                        "events": [
                            {
                                "trades": [
                                    {
                                        "price": str(100 + sequence),
                                        "product_id": "AVA-29MAY26-CDE",
                                        "side": OrderSide.BUY.value,
                                        "size": "1",
                                        "time": f"2026-01-01T12:00:0{sequence}Z",
                                        "trade_id": f"trade-{sequence}",
                                    }
                                ],
                                "type": "update",
                            }
                        ],
                        "sequence_num": sequence,
                        "timestamp": f"2026-01-01T12:00:0{sequence}Z",
                    },
                    "sequence_num": sequence,
                    "timestamp": f"2026-01-01T12:00:0{sequence}Z",
                },
            )
        )
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        max_market_trades_per_product=2,
        strategies=(TradeWindowMetadataStrategy(),),
    )

    result = task.run()
    records = ledger.iter_records()
    started = next(
        record
        for record in records
        if record.event_type == EventType.STRATEGY_EVALUATION_STARTED
    )
    completed = next(
        record
        for record in records
        if record.event_type == EventType.STRATEGY_EVALUATION_COMPLETED
    )
    metadata = completed.payload["metadata"]

    assert result["completed_count"] == 1
    assert result["submitted_action_count"] == 0
    assert started.payload["max_market_trades_per_product"] == 2
    assert metadata["trade_window"]["status"] == StrategyMarketDataStatus.OK.value
    assert metadata["trade_window"]["trade_ids"] == ["trade-2", "trade-3"]
    assert metadata["projection_retention"] == {
        "dropped_by_product_id": {"AVA-29MAY26-CDE": 1},
        "dropped_count": 1,
        "max_market_trades_per_product": 2,
    }


def test_strategy_task_applies_order_book_sample_retention_cap_to_snapshots(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core, clock=clock)
    for sequence, bid, ask in (
        (1, "99", "101"),
        (2, "100", "102"),
        (3, "101", "103"),
    ):
        router.ingest(
            FeedMessage(
                source_id="coinbase-primary",
                message_key=f"coinbase:l2_data:{sequence}",
                event_type=EventType.DATA_RECEIVED,
                payload={
                    "channel": "l2_data",
                    "raw": {
                        "channel": "l2_data",
                        "events": [
                            {
                                "product_id": "AVA-29MAY26-CDE",
                                "type": "snapshot",
                                "updates": [
                                    {"new_quantity": "2", "price_level": bid, "side": "bid"},
                                    {
                                        "new_quantity": "1",
                                        "price_level": str(int(bid) - 1),
                                        "side": "bid",
                                    },
                                    {"new_quantity": "3", "price_level": ask, "side": "offer"},
                                    {
                                        "new_quantity": "1",
                                        "price_level": str(int(ask) + 1),
                                        "side": "offer",
                                    },
                                ],
                            }
                        ],
                        "sequence_num": sequence,
                        "timestamp": f"2026-01-01T12:00:0{sequence}Z",
                    },
                    "sequence_num": sequence,
                    "timestamp": f"2026-01-01T12:00:0{sequence}Z",
                },
            )
        )
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        max_order_book_sample_depth_per_side=1,
        max_order_book_samples_per_product=2,
        order_book_sample_product_ids=("AVA-29MAY26-CDE",),
        strategies=(OrderBookWindowMetadataStrategy(),),
    )

    result = task.run()
    records = ledger.iter_records()
    started = next(
        record
        for record in records
        if record.event_type == EventType.STRATEGY_EVALUATION_STARTED
    )
    completed = next(
        record
        for record in records
        if record.event_type == EventType.STRATEGY_EVALUATION_COMPLETED
    )
    metadata = completed.payload["metadata"]

    assert result["completed_count"] == 1
    assert result["submitted_action_count"] == 0
    assert started.payload["max_order_book_sample_depth_per_side"] == 1
    assert started.payload["max_order_book_samples_per_product"] == 2
    assert started.payload["order_book_sample_product_ids"] == ["AVA-29MAY26-CDE"]
    assert metadata["order_book_window"]["status"] == StrategyMarketDataStatus.OK.value
    assert metadata["order_book_window"]["sample_count"] == 2
    assert metadata["order_book_window"]["sample_sequences"] == [4, 6]
    assert metadata["projection_retention"] == {
        "dropped_by_product_id": {"AVA-29MAY26-CDE": 1},
        "dropped_count": 1,
        "max_order_book_sample_depth_per_side": 1,
        "max_order_book_samples_per_product": 2,
        "order_book_sample_product_ids": ["AVA-29MAY26-CDE"],
        "scope_skipped_by_product_id": {},
        "scope_skipped_count": 0,
    }


def test_strategy_task_rejects_duplicate_decision_actions_before_gateway(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        strategies=(DuplicateActionIdStrategy(),),
    )

    result = task.run()
    records = ledger.iter_records()

    assert result["failed_count"] == 1
    assert result["submitted_action_count"] == 0
    assert EventType.ACTION_REQUESTED not in [record.event_type for record in records]
    assert records[1].event_type == EventType.ERROR
    assert records[1].payload["error_code"] == ErrorCode.STRATEGY_CONTRACT_FAILED.value
    assert records[1].payload["error"]["context"]["duplicate_action_ids"] == ["duplicate-action-id"]
    assert records[2].event_type == EventType.STRATEGY_EVALUATION_FAILED
    assert records[2].payload["action_receipts"] == []


def test_strategy_task_audits_strategy_errors_without_raising(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        strategies=(FailingStrategy(),),
    )

    result = task.run()
    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    health = ledger_health(ledger.path)
    checks = {check.name: check for check in health.checks}

    assert result["completed_count"] == 0
    assert result["failed_count"] == 1
    assert [record.event_type for record in records] == [
        EventType.STRATEGY_EVALUATION_STARTED,
        EventType.ERROR,
        EventType.STRATEGY_EVALUATION_FAILED,
    ]
    assert records[1].payload["error_category"] == ErrorCategory.STRATEGY.value
    assert records[1].payload["error_code"] == ErrorCode.STRATEGY_EVALUATION_FAILED.value
    assert records[2].payload["error_sequence"] == records[1].sequence
    assert projection.strategy_evaluations[0].status == StrategyEvaluationStatus.FAILED
    assert projection.strategy_evaluations[0].error_sequence == records[1].sequence
    assert checks[LedgerHealthCheckName.STRATEGY_CONTRACT].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.ERROR_EVENTS].count == 1


def test_strategy_task_stops_current_cycle_after_failed_strategy(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    second_strategy = StaticOrderStrategy()
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        strategies=(FailingStrategy(), second_strategy),
    )

    result = task.run()
    records = ledger.iter_records()

    assert result["completed_count"] == 0
    assert result["failed_count"] == 1
    assert result["strategy_count"] == 1
    assert result["submitted_action_count"] == 0
    assert second_strategy.snapshots == []
    assert [record.event_type for record in records] == [
        EventType.STRATEGY_EVALUATION_STARTED,
        EventType.ERROR,
        EventType.STRATEGY_EVALUATION_FAILED,
    ]


def test_strategy_task_reports_contract_errors_for_invalid_return_values(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        strategies=(InvalidReturnStrategy(),),
    )

    result = task.run()
    error_record = next(record for record in ledger.iter_records() if record.event_type == EventType.ERROR)

    assert result["failed_count"] == 1
    assert error_record.payload["error_code"] == ErrorCode.STRATEGY_CONTRACT_FAILED.value
    assert error_record.payload["exception_type"] == "StrategyContractError"
    assert error_record.payload["error"]["context"]["strategy_id"] == "invalid-return"


def test_ledger_health_reports_unclosed_strategy_evaluation(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    core.emit(
        EventType.STRATEGY_EVALUATION_STARTED,
        {
            "as_of_sequence": 0,
            "execution_mode": ExecutionMode.DRY_RUN.value,
            "strategy_id": "interrupted",
        },
    )

    health = ledger_health(ledger.path)
    checks = {check.name: check for check in health.checks}
    strategy_check = checks[LedgerHealthCheckName.STRATEGY_CONTRACT]

    assert strategy_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert strategy_check.count == 1
    assert strategy_check.details["anomalies"][0]["strategy_id"] == "interrupted"
    assert strategy_check.details["anomalies"][0]["closed"] is False


def test_ledger_health_reports_strategy_closure_payload_contract_mismatch(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    started = core.emit(
        EventType.STRATEGY_EVALUATION_STARTED,
        {
            "as_of_sequence": 0,
            "execution_mode": ExecutionMode.DRY_RUN.value,
            "strategy_id": "broken",
        },
    )
    core.emit(
        EventType.STRATEGY_EVALUATION_COMPLETED,
        {
            "action_receipts": [],
            "as_of_sequence": 0,
            "input_freshness": [
                {
                    "age_seconds": None,
                    "data_kind": MarketDataKind.TICKER.value,
                    "is_ok": True,
                    "max_age_seconds": 0,
                    "product_id": "BTC-USD",
                    "sequence": None,
                    "status": StrategyInputStatus.STALE.value,
                }
            ],
            "intent_count": -1,
            "metadata": {},
            "started_sequence": started.sequence,
            "status": StrategyEvaluationStatus.FAILED.value,
            "strategy_id": "broken",
            "submitted_action_count": 1,
        },
    )

    health = ledger_health(ledger.path)
    checks = {check.name: check for check in health.checks}
    strategy_check = checks[LedgerHealthCheckName.STRATEGY_CONTRACT]
    payload_anomalies = [
        anomaly
        for anomaly in strategy_check.details["anomalies"]
        if anomaly.get("issue") == "strategy_closure_payload_contract"
    ]

    assert strategy_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert {anomaly["field"] for anomaly in payload_anomalies} == {
        "input_freshness[0].is_ok",
        "input_freshness[0].max_age_seconds",
        "intent_count",
        "status",
        "submitted_action_count",
    }


def test_strategy_selection_requires_explicit_known_unique_ids():
    with pytest.raises(ValueError, match="must not be empty"):
        select_strategies((NoOpStrategy(),), ())

    with pytest.raises(ValueError, match="unknown"):
        select_strategies((NoOpStrategy(),), ("missing",))

    with pytest.raises(ValueError, match="unique"):
        select_strategies((NoOpStrategy(), NoOpStrategy()), ("noop",))


def test_strategy_decision_requires_tuple_intents():
    with pytest.raises(TypeError, match="tuple"):
        StrategyDecision(intents=[])


def test_strategy_decision_commands_reject_duplicate_place_order_client_identity():
    decision = StrategyDecision(
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

    with pytest.raises(StrategyContractError, match="client identities") as exc_info:
        strategy_decision_commands("duplicate-client", decision)

    assert exc_info.value.context["duplicate_client_order_ids"] == ["same-client-order"]


def test_strategy_staged_release_intents_use_sizing_outputs_and_stable_identities():
    base_intent = PlaceOrderIntent(
        action_id="ignored-base-action",
        idempotency_key="ignored-base-client-order",
        limit_price="100.0",
        order_type=OrderType.LIMIT,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        size="9",
    )
    decision = OrderSizingDecision(
        status=OrderSizingDecisionStatus.ACCEPTED,
        lineage_relation=OrderLineageRelation.ROOT,
        product_id="BTC-USD",
        requested_sizes=(Decimal("5"),),
        output_sizes=(Decimal("2.00"), Decimal("2.00"), Decimal("1.00")),
        limit_price=Decimal("100"),
    )

    intents = strategy_staged_release_intents(
        "Example Strategy",
        "entry",
        base_intent,
        decision,
        {"product_id": "BTC-USD"},
    )
    repeated_intents = strategy_staged_release_intents(
        "Example Strategy",
        "entry",
        base_intent,
        decision,
        {"product_id": "BTC-USD"},
    )
    commands = strategy_decision_commands(
        "Example Strategy",
        StrategyDecision(intents=intents),
    )

    assert tuple(intent.action_id for intent in intents) == tuple(
        intent.action_id for intent in repeated_intents
    )
    assert tuple(intent.size for intent in intents) == ("2.00", "2.00", "1.00")
    assert {intent.placement_kind for intent in intents} == {OrderPlacementKind.STAGED_RELEASE}
    assert all(intent.limit_price == "100" for intent in intents)
    assert all(intent.idempotency_key is not None for intent in intents)
    assert len({intent.action_id for intent in intents}) == 3
    assert len({intent.idempotency_key for intent in intents}) == 3
    assert all(command.payload["placement_kind"] == OrderPlacementKind.STAGED_RELEASE.value for command in commands)
    assert all(command.requested_by == "strategy:Example Strategy" for command in commands)


def test_strategy_staged_release_intents_reject_invalid_sizing_decisions():
    base_intent = PlaceOrderIntent(
        action_id="base-action",
        limit_price="100",
        order_type=OrderType.LIMIT,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        size="1",
    )
    rejected_decision = OrderSizingDecision(
        status=OrderSizingDecisionStatus.REJECTED,
        lineage_relation=OrderLineageRelation.ROOT,
        product_id="BTC-USD",
        requested_sizes=(Decimal("5"),),
        output_sizes=(),
        limit_price=Decimal("100"),
        reasons=("test rejection",),
    )
    mismatched_product_decision = OrderSizingDecision(
        status=OrderSizingDecisionStatus.ACCEPTED,
        lineage_relation=OrderLineageRelation.ROOT,
        product_id="ETH-USD",
        requested_sizes=(Decimal("5"),),
        output_sizes=(Decimal("1"),),
        limit_price=Decimal("100"),
    )
    mismatched_price_decision = OrderSizingDecision(
        status=OrderSizingDecisionStatus.ACCEPTED,
        lineage_relation=OrderLineageRelation.ROOT,
        product_id="BTC-USD",
        requested_sizes=(Decimal("5"),),
        output_sizes=(Decimal("1"),),
        limit_price=Decimal("101"),
    )

    with pytest.raises(StrategyContractError, match="accepted staged release"):
        strategy_staged_release_intents("example", "entry", base_intent, rejected_decision)
    with pytest.raises(StrategyContractError, match="product_id"):
        strategy_staged_release_intents("example", "entry", base_intent, mismatched_product_decision)
    with pytest.raises(StrategyContractError, match="limit_price"):
        strategy_staged_release_intents("example", "entry", base_intent, mismatched_price_decision)


def test_strategy_release_staged_placement_intent_uses_source_of_truth_staged_chunk(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    staged_intent = PlaceOrderIntent(
        action_id="stage-action",
        idempotency_key="stage-client-order",
        limit_price="100",
        logical_order_id="stage-logical",
        order_type=OrderType.LIMIT,
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
        post_only=True,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        size="0.10",
    )

    staged_receipt = gateway.submit_and_execute(staged_intent.to_command(), DryRunExecutor())
    projection = SourceOfTruthProjection.from_ledger(ledger)
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        projection=projection,
    )

    release_intent = strategy_release_staged_placement_intent(
        "Example Strategy",
        "entry",
        snapshot,
        "stage-action",
        {"product_id": "BTC-USD"},
    )
    repeated_release_intent = strategy_release_staged_placement_intent(
        "Example Strategy",
        "entry",
        snapshot,
        "stage-action",
        {"product_id": "BTC-USD"},
    )
    commands = strategy_decision_commands(
        "Example Strategy",
        StrategyDecision(intents=(release_intent,)),
    )
    release_receipt = gateway.submit_and_execute(commands[0], DryRunExecutor())
    released_projection = SourceOfTruthProjection.from_ledger(ledger)
    release_placement = released_projection.placements_by_id[release_intent.action_id]

    assert staged_receipt.status == ActionStatus.ACCEPTED
    assert release_receipt.status == ActionStatus.EXECUTED
    assert release_intent.action_id == repeated_release_intent.action_id
    assert release_intent.idempotency_key != staged_intent.idempotency_key
    assert release_intent.logical_order_id == "stage-logical"
    assert release_intent.placement_kind == OrderPlacementKind.RELEASE
    assert release_intent.post_only is True
    assert release_intent.metadata["staged_release"]["release_of_placement_id"] == "stage-action"
    assert commands[0].requested_by == "strategy:Example Strategy"
    assert release_placement.placement_kind == OrderPlacementKind.RELEASE
    assert release_placement.payload["metadata"]["staged_release"]["release_of_action_id"] == "stage-action"
    assert released_projection.orders_by_action_id[release_intent.action_id].lifecycle_status == (
        OrderLifecycleStatus.OPEN
    )


def test_strategy_release_staged_placement_intent_rejects_missing_or_unsafe_releases(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    gateway = ActionGateway(core)

    def snapshot() -> StrategySnapshot:
        projection = SourceOfTruthProjection.from_ledger(ledger)
        return StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            projection=projection,
        )

    with pytest.raises(StrategyInputUnavailableError, match="staged placement"):
        strategy_release_staged_placement_intent("example", "entry", snapshot(), "missing-stage")

    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="stage-one",
            limit_price="100",
            logical_order_id="shared-logical",
            order_type=OrderType.LIMIT,
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="0.10",
        ).to_command(),
        DryRunExecutor(),
    )
    release_one = strategy_release_staged_placement_intent(
        "example",
        "entry",
        snapshot(),
        "stage-one",
    )
    gateway.submit_and_execute(release_one.to_command(), DryRunExecutor())

    with pytest.raises(StrategyContractError, match="already has"):
        strategy_release_staged_placement_intent("example", "entry", snapshot(), "stage-one")

    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="stage-two",
            limit_price="100",
            logical_order_id="shared-logical",
            order_type=OrderType.LIMIT,
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="0.10",
        ).to_command(),
        DryRunExecutor(),
    )

    with pytest.raises(StrategyContractError, match="overlap"):
        strategy_release_staged_placement_intent("example", "entry", snapshot(), "stage-two")


def test_strategy_followup_after_fill_intent_uses_replayed_fill_policy_and_product_rules(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="parent-action",
            idempotency_key="parent-client-order",
            limit_price="100",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.BUY,
            size="0.2",
        ).to_command(),
        DryRunExecutor(),
    )
    parent_projection = SourceOfTruthProjection.from_ledger(ledger)
    exchange_order_id = parent_projection.orders_by_action_id["parent-action"].exchange_order_id
    assert exchange_order_id is not None
    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "order_id": exchange_order_id,
            "price": "100",
            "product_id": "SHB-26JUN26-CDE",
            "side": "BUY",
            "size": "0.1",
            "trade_id": "trade-1",
        },
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )

    intent = strategy_followup_after_fill_intent("example", "entry", snapshot, "fill-1")
    commands = strategy_decision_commands("example", StrategyDecision(intents=(intent,)))
    receipt = gateway.submit_and_execute(commands[0], DryRunExecutor())
    after_followup = SourceOfTruthProjection.from_ledger(ledger)
    child = after_followup.logical_orders_by_id[intent.logical_order_id]

    assert intent.lineage_relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL
    assert intent.parent_order_id == "parent-action"
    assert intent.root_order_id == "parent-action"
    assert intent.source_order_ids == ("parent-action",)
    assert intent.side == OrderSide.SELL
    assert intent.size == "0.1"
    assert intent.limit_price == "100"
    assert intent.post_only is True
    assert intent.reduce_only is True
    assert receipt.status == ActionStatus.EXECUTED
    assert child.lineage_relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL
    assert child.parent_order_id == "parent-action"
    assert after_followup.logical_orders_by_id["parent-action"].child_order_ids == [
        intent.logical_order_id
    ]

    repeated_snapshot = StrategySnapshot(
        as_of_sequence=after_followup.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=after_followup,
    )
    with pytest.raises(StrategyContractError, match="already exists"):
        strategy_followup_after_fill_intent("example", "entry", repeated_snapshot, "fill-1")


def test_strategy_followup_after_fill_intent_rejects_small_or_incomplete_inputs(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="parent-action",
            idempotency_key="parent-client-order",
            limit_price="100",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.BUY,
            size="0.2",
        ).to_command(),
        DryRunExecutor(),
    )
    parent_projection = SourceOfTruthProjection.from_ledger(ledger)
    exchange_order_id = parent_projection.orders_by_action_id["parent-action"].exchange_order_id
    assert exchange_order_id is not None
    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-small",
            "order_id": exchange_order_id,
            "price": "100",
            "product_id": "SHB-26JUN26-CDE",
            "side": "BUY",
            "size": "0.01",
        },
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )

    with pytest.raises(StrategyContractError, match="below configured minimum"):
        strategy_followup_after_fill_intent("example", "entry", snapshot, "fill-small")

    missing_catalog_snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        projection=projection,
    )
    with pytest.raises(StrategyInputUnavailableError, match="product catalog"):
        strategy_followup_after_fill_intent(
            "example",
            "entry",
            missing_catalog_snapshot,
            "fill-small",
        )


def test_strategy_split_order_intents_cancel_source_then_create_children(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    source_intent = PlaceOrderIntent(
        action_id="source-action",
        idempotency_key="source-client-order",
        limit_price="100",
        logical_order_id="source-logical",
        order_type=OrderType.LIMIT,
        post_only=True,
        product_id="SHB-26JUN26-CDE",
        side=OrderSide.BUY,
        size="0.20",
    )
    source_receipt = gateway.submit_and_execute(source_intent.to_command(), DryRunExecutor())
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )

    intents = strategy_split_order_intents(
        "Example Strategy",
        "split",
        snapshot,
        "source-logical",
        2,
        {"product_id": "SHB-26JUN26-CDE"},
    )
    repeated_intents = strategy_split_order_intents(
        "Example Strategy",
        "split",
        snapshot,
        "source-logical",
        2,
        {"product_id": "SHB-26JUN26-CDE"},
    )
    commands = strategy_decision_commands("Example Strategy", StrategyDecision(intents=intents))
    receipts = tuple(gateway.submit_and_execute(command, DryRunExecutor()) for command in commands)
    after_split = SourceOfTruthProjection.from_ledger(ledger)
    cancel_intent = intents[0]
    child_intents = intents[1:]

    assert source_receipt.status == ActionStatus.EXECUTED
    assert isinstance(cancel_intent, CancelOrderIntent)
    assert all(isinstance(intent, PlaceOrderIntent) for intent in child_intents)
    assert tuple(intent.action_id for intent in intents) == tuple(
        intent.action_id for intent in repeated_intents
    )
    assert [receipt.status for receipt in receipts] == [
        ActionStatus.EXECUTED,
        ActionStatus.EXECUTED,
        ActionStatus.EXECUTED,
    ]
    assert after_split.orders_by_action_id["source-action"].lifecycle_status == (
        OrderLifecycleStatus.CANCELLED
    )
    assert after_split.orders_by_action_id["source-action"].cancel_action_ids == [
        cancel_intent.action_id
    ]
    assert tuple(intent.size for intent in child_intents) == ("0.10", "0.10")
    assert len({intent.action_id for intent in child_intents}) == 2
    assert len({intent.idempotency_key for intent in child_intents}) == 2
    assert all(intent.lineage_relation == OrderLineageRelation.SPLIT_CHILD for intent in child_intents)
    assert all(intent.parent_order_id == "source-logical" for intent in child_intents)
    assert all(intent.root_order_id == "source-logical" for intent in child_intents)
    assert all(intent.source_order_ids == ("source-logical",) for intent in child_intents)
    assert all(command.requested_by == "strategy:Example Strategy" for command in commands)

    for index, intent in enumerate(child_intents, start=1):
        logical_order = after_split.logical_orders_by_id[intent.logical_order_id]
        placement = after_split.placements_by_id[intent.action_id]

        assert logical_order.lineage_relation == OrderLineageRelation.SPLIT_CHILD
        assert logical_order.parent_order_id == "source-logical"
        assert logical_order.root_order_id == "source-logical"
        assert logical_order.source_order_ids == ("source-logical",)
        assert placement.logical_order_id == intent.logical_order_id
        assert placement.placement_kind == OrderPlacementKind.INITIAL
        assert placement.payload["metadata"]["split_child"]["child_index"] == index
        assert placement.payload["metadata"]["split_child"]["source_action_id"] == "source-action"


def test_strategy_split_order_intents_rejects_partial_fill_and_disabled_policy(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-action",
            idempotency_key="source-client-order",
            limit_price="100",
            logical_order_id="source-logical",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.BUY,
            size="0.20",
        ).to_command(),
        DryRunExecutor(),
    )
    placed_projection = SourceOfTruthProjection.from_ledger(ledger)
    exchange_order_id = placed_projection.orders_by_action_id["source-action"].exchange_order_id
    assert exchange_order_id is not None
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    disabled_policy = replace(
        policy,
        lineage=replace(policy.lineage, split_orders=OperatorPolicyPermission.DISABLED),
    )
    base_snapshot = StrategySnapshot(
        as_of_sequence=placed_projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=disabled_policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=placed_projection,
    )

    with pytest.raises(StrategyContractError, match="does not allow split"):
        strategy_split_order_intents("example", "split", base_snapshot, "source-logical", 2)

    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "order_id": exchange_order_id,
            "price": "100",
            "product_id": "SHB-26JUN26-CDE",
            "side": "BUY",
            "size": "0.01",
            "trade_id": "trade-1",
        },
    )
    filled_projection = SourceOfTruthProjection.from_ledger(ledger)
    filled_snapshot = StrategySnapshot(
        as_of_sequence=filled_projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=filled_projection,
    )

    with pytest.raises(StrategyContractError, match="partially filled"):
        strategy_split_order_intents("example", "split", filled_snapshot, "source-logical", 2)


def test_strategy_consolidation_intent_uses_source_orders_and_product_rules(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-b",
            limit_price="100",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size="0.2",
        ).to_command(),
        DryRunExecutor(),
    )
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-a",
            limit_price="100.00",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size="0.1",
        ).to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )

    intent = strategy_consolidation_intent(
        "example",
        "tidy",
        snapshot,
        ("source-b", "source-a"),
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
    )
    commands = strategy_decision_commands("example", StrategyDecision(intents=(intent,)))
    receipt = gateway.submit(commands[0])
    after_consolidation = SourceOfTruthProjection.from_ledger(ledger)
    consolidated = after_consolidation.logical_orders_by_id[intent.logical_order_id]

    assert intent.lineage_relation == OrderLineageRelation.CONSOLIDATION
    assert intent.placement_kind == OrderPlacementKind.STAGED_RELEASE
    assert intent.source_order_ids == ("source-a", "source-b")
    assert intent.side == OrderSide.SELL
    assert intent.size == "0.3"
    assert intent.limit_price is not None
    assert Decimal(intent.limit_price) == Decimal("100")
    assert intent.metadata["consolidation"]["source_order_ids"] == ["source-a", "source-b"]
    assert receipt.status == ActionStatus.ACCEPTED
    assert consolidated.lineage_relation == OrderLineageRelation.CONSOLIDATION
    assert consolidated.source_order_ids == ("source-a", "source-b")


def test_strategy_consolidation_intent_rejects_unsafe_or_incomplete_inputs(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-a",
            limit_price="100",
            order_type=OrderType.LIMIT,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size="0.1",
        ).to_command(),
        DryRunExecutor(),
    )
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="source-b",
            limit_price="101",
            order_type=OrderType.LIMIT,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size="0.2",
        ).to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )

    with pytest.raises(StrategyContractError, match="share limit_price"):
        strategy_consolidation_intent("example", "tidy", snapshot, ("source-a", "source-b"))

    with pytest.raises(StrategyInputUnavailableError, match="source logical orders"):
        strategy_consolidation_intent(
            "example",
            "tidy",
            snapshot,
            ("source-a", "missing-source"),
            limit_price="100",
        )

    missing_catalog_snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        projection=projection,
    )
    with pytest.raises(StrategyInputUnavailableError, match="product catalog"):
        strategy_consolidation_intent(
            "example",
            "tidy",
            missing_catalog_snapshot,
            ("source-a", "source-b"),
            limit_price="100",
        )


def test_strategy_identifier_helpers_are_stable_json_normalized_and_venue_safe():
    action_id = strategy_action_id(
        "Example Strategy",
        "entry order",
        {"product_id": "BTC-USD", "side": OrderSide.BUY},
        "50000",
    )

    assert action_id == strategy_action_id(
        "Example Strategy",
        "entry order",
        {"side": OrderSide.BUY, "product_id": "BTC-USD"},
        "50000",
    )
    assert action_id != strategy_action_id(
        "Example Strategy",
        "entry order",
        {"product_id": "BTC-USD", "side": OrderSide.SELL},
        "50000",
    )
    assert action_id.startswith("act-example-strategy-entry-order-")
    assert len(action_id) <= 64
    assert set(action_id) <= set("abcdefghijklmnopqrstuvwxyz0123456789-")

    client_order_id = strategy_client_order_id("Example Strategy", "entry order", "BTC-USD")

    assert client_order_id.startswith("coid-example-strategy-entry-order-")
    assert client_order_id != action_id
    assert len(client_order_id) <= 64


def test_strategy_identifier_helpers_reject_empty_or_non_json_parts():
    with pytest.raises(ValueError, match="strategy_id"):
        strategy_action_id("", "entry")

    with pytest.raises(ValueError, match="purpose"):
        strategy_action_id("example", "")

    with pytest.raises(TypeError, match="Unsupported JSON"):
        strategy_action_id("example", "entry", object())


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
