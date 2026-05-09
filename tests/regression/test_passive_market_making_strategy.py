from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionGateway
from app.ledger_health import ledger_health
from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    EventType,
    ExecutionMode,
    LedgerHealthStatus,
    MarginType,
    OrderPlacementKind,
    OrderLineageRelation,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyManagerGate,
    StrategyManagerSkipReason,
    TimeInForce,
)
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from risk.gate import RiskGate, RiskPolicy
from strategies import (
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    PassiveMarketMakingStrategy,
    StrategyEvaluationTask,
    StrategySnapshot,
    configured_strategies,
    load_operator_policy_from_json_file,
    strategy_decision_commands,
    strategy_release_staged_placement_intent,
)


def test_passive_market_making_stages_bid_and_ask_without_execution(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = _operator_policy()
    _accept_order_book(core, "SHB-26JUN26-CDE")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    strategy = PassiveMarketMakingStrategy()

    decision = strategy.evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=policy)
    )
    commands = strategy_decision_commands(PASSIVE_MARKET_MAKING_STRATEGY_ID, decision)
    receipts = tuple(gateway.submit_and_execute(command, DryRunExecutor()) for command in commands)
    after_staging = SourceOfTruthProjection.from_ledger(ledger)
    second_decision = strategy.evaluate(
        _snapshot(ledger, projection=after_staging, observed_at=observed_at, policy=policy)
    )

    assert len(decision.intents) == 2
    assert [intent.side for intent in decision.intents] == [OrderSide.BUY, OrderSide.SELL]
    assert [intent.placement_kind for intent in decision.intents] == [
        OrderPlacementKind.STAGED_RELEASE,
        OrderPlacementKind.STAGED_RELEASE,
    ]
    assert [Decimal(intent.limit_price) for intent in decision.intents if intent.limit_price is not None] == [
        Decimal("99.50"),
        Decimal("100.50"),
    ]
    assert {intent.leverage for intent in decision.intents} == {"1"}
    assert {intent.margin_type for intent in decision.intents} == {MarginType.CROSS}
    assert {intent.size for intent in decision.intents} == {"0.05"}
    assert [receipt.status for receipt in receipts] == [ActionStatus.ACCEPTED, ActionStatus.ACCEPTED]
    assert [placement.placement_kind for placement in after_staging.unreleased_staged_order_placements] == [
        OrderPlacementKind.STAGED_RELEASE,
        OrderPlacementKind.STAGED_RELEASE,
    ]
    assert after_staging.open_orders == ()
    assert second_decision.intents == ()
    assert {skip["reason"] for skip in second_decision.metadata["skipped_sides"]} == {
        StrategyManagerSkipReason.ACTIVE_QUOTE_EXISTS.value
    }


def test_passive_market_making_uses_future_contract_size_for_quote_size(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    policy = _operator_policy()
    product = _strategy_product("SHB-26JUN26-CDE", contract_size=Decimal("9"))
    _accept_order_book(core, "SHB-26JUN26-CDE")
    projection = SourceOfTruthProjection.from_ledger(ledger)

    decision = PassiveMarketMakingStrategy().evaluate(
        _snapshot(
            ledger,
            product_catalog=ProductCatalog((product,)),
            projection=projection,
            observed_at=observed_at,
            policy=policy,
        )
    )

    assert len(decision.intents) == 2
    assert {intent.size for intent in decision.intents} == {"0.01"}


def test_passive_market_making_task_stages_quotes_through_dry_run_gateway(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(observed_at)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    policy = _operator_policy()
    product_catalog = ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),))
    _accept_order_book(core, "SHB-26JUN26-CDE")
    task = StrategyEvaluationTask(
        core,
        action_gateway=ActionGateway(core),
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        operator_policy=policy,
        product_catalog=product_catalog,
        strategies=(PassiveMarketMakingStrategy(),),
    )

    result = task.run()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    health = ledger_health(ledger.path)
    action_receipts = result["evaluations"][0]["action_receipts"]
    staged_action_ids = tuple(receipt["action_id"] for receipt in action_receipts)

    assert result["completed_count"] == 1
    assert result["failed_count"] == 0
    assert result["submitted_action_count"] == 2
    assert [receipt["status"] for receipt in action_receipts] == [
        ActionStatus.ACCEPTED.value,
        ActionStatus.ACCEPTED.value,
    ]
    assert tuple(
        placement.placement_id for placement in projection.unreleased_staged_order_placements
    ) == staged_action_ids
    assert [placement.placement_kind for placement in projection.unreleased_staged_order_placements] == [
        OrderPlacementKind.STAGED_RELEASE,
        OrderPlacementKind.STAGED_RELEASE,
    ]
    assert projection.open_orders == ()
    assert health.status == LedgerHealthStatus.OK


def test_passive_market_making_staged_quotes_pass_cfm_leverage_risk(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    product = _strategy_product("SHB-26JUN26-CDE")
    product_catalog = ProductCatalog((product,))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(
        core,
        risk_gate=RiskGate(
            RiskPolicy.from_values(
                allowed_lineage_relations=(OrderLineageRelation.ROOT,),
                allowed_order_types=(OrderType.LIMIT,),
                allowed_placement_kinds=(OrderPlacementKind.STAGED_RELEASE,),
                allowed_products=("SHB-26JUN26-CDE",),
                allowed_sides=(OrderSide.BUY, OrderSide.SELL),
                allowed_time_in_force=(TimeInForce.GOOD_UNTIL_CANCELLED,),
                max_leverage="3",
                product_catalog=product_catalog,
                require_post_only=True,
            )
        ),
    )
    policy = _operator_policy()
    _accept_order_book(core, "SHB-26JUN26-CDE")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    decision = PassiveMarketMakingStrategy().evaluate(
        _snapshot(
            ledger,
            product_catalog=product_catalog,
            projection=projection,
            observed_at=observed_at,
            policy=policy,
        )
    )

    receipts = tuple(
        gateway.submit_and_execute(command, DryRunExecutor())
        for command in strategy_decision_commands(PASSIVE_MARKET_MAKING_STRATEGY_ID, decision)
    )

    assert [receipt.status for receipt in receipts] == [ActionStatus.ACCEPTED, ActionStatus.ACCEPTED]


def test_passive_market_making_blocks_released_open_quote_by_projection_link(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = _operator_policy()
    _accept_order_book(core, "SHB-26JUN26-CDE")
    initial_projection = SourceOfTruthProjection.from_ledger(ledger)
    strategy = PassiveMarketMakingStrategy()

    initial_decision = strategy.evaluate(
        _snapshot(ledger, projection=initial_projection, observed_at=observed_at, policy=policy)
    )
    buy_stage_intent = initial_decision.intents[0]
    gateway.submit_and_execute(
        strategy_decision_commands(PASSIVE_MARKET_MAKING_STRATEGY_ID, initial_decision)[0],
        DryRunExecutor(),
    )
    after_staging = SourceOfTruthProjection.from_ledger(ledger)
    release_intent = strategy_release_staged_placement_intent(
        PASSIVE_MARKET_MAKING_STRATEGY_ID,
        "quote",
        _snapshot(ledger, projection=after_staging, observed_at=observed_at, policy=policy),
        buy_stage_intent.action_id,
    )
    release_receipt = gateway.submit_and_execute(release_intent.to_command(), DryRunExecutor())
    after_release = SourceOfTruthProjection.from_ledger(ledger)

    next_decision = strategy.evaluate(
        _snapshot(ledger, projection=after_release, observed_at=observed_at, policy=policy)
    )

    assert release_receipt.status == ActionStatus.EXECUTED
    assert after_release.passive_market_making_quotes[0].released is True
    assert after_release.open_orders[0].action_id == release_intent.action_id
    assert [intent.side for intent in next_decision.intents] == [OrderSide.SELL]
    assert next_decision.metadata["skipped_sides"] == [
        {
            "product_id": "SHB-26JUN26-CDE",
            "reason": StrategyManagerSkipReason.ACTIVE_QUOTE_EXISTS.value,
            "side": OrderSide.BUY.value,
        }
    ]


def test_passive_market_making_blocks_without_required_policy_inputs(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    _accept_order_book(core, "SHB-26JUN26-CDE")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = _operator_policy()
    unsafe_policy = replace(
        policy,
        order_behavior=replace(policy.order_behavior, post_only=False),
    )
    unstaged_policy = replace(
        policy,
        staged_or_hidden_release=replace(policy.staged_or_hidden_release, enabled=False),
    )

    missing_policy_decision = PassiveMarketMakingStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            projection=projection,
        )
    )
    missing_catalog_decision = PassiveMarketMakingStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            operator_policy=policy,
            projection=projection,
        )
    )
    unsafe_policy_decision = PassiveMarketMakingStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=unsafe_policy)
    )
    unstaged_policy_decision = PassiveMarketMakingStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=unstaged_policy)
    )

    assert missing_policy_decision.intents == ()
    assert missing_policy_decision.metadata["passive_market_making_gate"] == (
        StrategyManagerGate.OPERATOR_POLICY_MISSING.value
    )
    assert missing_catalog_decision.intents == ()
    assert missing_catalog_decision.metadata["passive_market_making_gate"] == (
        StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
    )
    assert unsafe_policy_decision.intents == ()
    assert unsafe_policy_decision.metadata["passive_market_making_gate"] == (
        StrategyManagerGate.PASSIVE_MARKET_MAKING_POLICY_UNSAFE.value
    )
    assert unstaged_policy_decision.intents == ()
    assert unstaged_policy_decision.metadata["passive_market_making_gate"] == (
        StrategyManagerGate.STAGED_RELEASE_DISABLED.value
    )


def test_passive_market_making_blocks_without_fresh_order_book(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    projection = SourceOfTruthProjection.from_ledger(ledger)

    decision = PassiveMarketMakingStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=_operator_policy())
    )

    assert decision.intents == ()
    assert decision.metadata["skipped_products"][0]["reason"] == (
        StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value
    )


def test_passive_market_making_is_available_as_builtin_strategy():
    strategies = configured_strategies((PASSIVE_MARKET_MAKING_STRATEGY_ID,))

    assert [strategy.strategy_id for strategy in strategies] == [
        PASSIVE_MARKET_MAKING_STRATEGY_ID
    ]


def _operator_policy():
    return load_operator_policy_from_json_file(Path("docs/examples/operator-policy.conservative-cfm-v0.json"))


def _snapshot(
    ledger: AuditLedger,
    *,
    product_catalog: ProductCatalog | None = None,
    projection: SourceOfTruthProjection,
    observed_at: datetime,
    policy,
) -> StrategySnapshot:
    return StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=product_catalog or ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )


def _strategy_product(product_id: str, *, contract_size: Decimal | None = None) -> ProductMetadata:
    return ProductMetadata(
        base_increment=Decimal("0.01"),
        base_max_size=Decimal("1"),
        base_min_size=Decimal("0.01"),
        contract_size=contract_size,
        price_increment=Decimal("0.01"),
        product_id=product_id,
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_max_size=Decimal("1000"),
        quote_min_size=Decimal("1"),
    )


def _accept_order_book(core: AuditCore, product_id: str) -> None:
    received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": f"coinbase:l2_data:{product_id}:1",
            "payload": {
                "channel": "l2_data",
                "raw": {
                    "channel": "l2_data",
                    "events": [
                        {
                            "product_id": product_id,
                            "type": "snapshot",
                            "updates": [
                                {"new_quantity": "2", "price_level": "99", "side": "bid"},
                                {"new_quantity": "3", "price_level": "101", "side": "offer"},
                            ],
                        }
                    ],
                    "sequence_num": 1,
                    "timestamp": "2026-01-01T00:00:00Z",
                },
                "sequence_num": 1,
                "timestamp": "2026-01-01T00:00:00Z",
            },
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": f"coinbase:l2_data:{product_id}:1",
            "received_sequence": received.sequence,
            "source_id": "coinbase-primary",
        },
    )
