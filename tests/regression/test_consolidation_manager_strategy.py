from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import (
    ActionType,
    ActionStatus,
    ExecutionMode,
    MarketDataKind,
    OperatorPolicyPermission,
    OrderLifecycleStatus,
    OrderLineageRelation,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyManagerGate,
    StrategyManagerSkipReason,
    StrategyInputStatus,
)
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from strategies import (
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    ConsolidationManagerStrategy,
    StrategySnapshot,
    configured_strategies,
    load_operator_policy_from_json_file,
    strategy_decision_commands,
)


def test_consolidation_manager_cancels_duplicate_orders_and_places_merge(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    _create_open_order(gateway, action_id="source-a", price="100", size="0.05")
    _create_open_order(gateway, action_id="source-b", price="100.00", size="0.05")
    _create_open_order(gateway, action_id="source-c", price="101", size="0.05")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = _operator_policy_without_order_book_requirement()
    strategy = ConsolidationManagerStrategy()

    decision = strategy.evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=policy)
    )
    commands = strategy_decision_commands(CONSOLIDATION_MANAGER_STRATEGY_ID, decision)
    receipts = tuple(gateway.submit_and_execute(command, DryRunExecutor()) for command in commands)
    after_consolidation = SourceOfTruthProjection.from_ledger(ledger)
    second_decision = strategy.evaluate(
        _snapshot(ledger, projection=after_consolidation, observed_at=observed_at, policy=policy)
    )

    assert [command.action_type for command in commands] == [
        ActionType.CANCEL_ORDER,
        ActionType.CANCEL_ORDER,
        ActionType.PLACE_ORDER,
    ]
    assert [receipt.status for receipt in receipts] == [
        ActionStatus.EXECUTED,
        ActionStatus.EXECUTED,
        ActionStatus.EXECUTED,
    ]
    assert decision.metadata["created_consolidations"] == [
        {
            "cancel_action_ids": [commands[0].action_id, commands[1].action_id],
            "limit_price": "100",
            "placement_action_id": commands[2].action_id,
            "product_id": "SHB-26JUN26-CDE",
            "side": "sell",
            "source_order_ids": ["source-a", "source-b"],
        }
    ]
    assert after_consolidation.orders_by_action_id["source-a"].lifecycle_status == (
        OrderLifecycleStatus.CANCELLED
    )
    assert after_consolidation.orders_by_action_id["source-b"].lifecycle_status == (
        OrderLifecycleStatus.CANCELLED
    )
    assert after_consolidation.orders_by_action_id["source-c"].lifecycle_status == (
        OrderLifecycleStatus.OPEN
    )
    consolidated = after_consolidation.orders_by_action_id[commands[2].action_id]
    logical = after_consolidation.logical_orders_by_id[commands[2].action_id]
    assert consolidated.lifecycle_status == OrderLifecycleStatus.OPEN
    assert logical.lineage_relation == OrderLineageRelation.CONSOLIDATION
    assert logical.source_order_ids == ("source-a", "source-b")
    assert Decimal(logical.size) == Decimal("0.10")
    assert second_decision.intents == ()


def test_consolidation_manager_blocks_without_required_policy_inputs(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    _create_open_order(gateway, action_id="source-a", price="100", size="0.05")
    _create_open_order(gateway, action_id="source-b", price="100", size="0.05")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = _operator_policy_without_order_book_requirement()
    disabled_policy = replace(
        policy,
        lineage=replace(policy.lineage, merge_orders=OperatorPolicyPermission.DISABLED),
    )

    missing_policy_decision = ConsolidationManagerStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            projection=projection,
        )
    )
    missing_catalog_decision = ConsolidationManagerStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            operator_policy=policy,
            projection=projection,
        )
    )
    disabled_policy_decision = ConsolidationManagerStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=disabled_policy)
    )

    assert missing_policy_decision.intents == ()
    assert missing_policy_decision.metadata["consolidation_gate"] == (
        StrategyManagerGate.OPERATOR_POLICY_MISSING.value
    )
    assert missing_catalog_decision.intents == ()
    assert missing_catalog_decision.metadata["consolidation_gate"] == (
        StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
    )
    assert disabled_policy_decision.intents == ()
    assert disabled_policy_decision.metadata["consolidation_gate"] == (
        StrategyManagerGate.CONSOLIDATION_DISABLED.value
    )


def test_consolidation_manager_blocks_without_fresh_required_order_book(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    _create_open_order(gateway, action_id="source-a", price="100", size="0.05")
    _create_open_order(gateway, action_id="source-b", price="100", size="0.05")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(Path("docs/examples/operator-policy.conservative-cfm-v0.json"))

    decision = ConsolidationManagerStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=policy)
    )

    assert decision.intents == ()
    assert decision.metadata["skipped_groups"] == [
        {
            "freshness": {
                "age_seconds": None,
                "data_kind": MarketDataKind.ORDER_BOOK.value,
                "is_ok": False,
                "max_age_seconds": 5.0,
                "observed_at": None,
                "product_id": "SHB-26JUN26-CDE",
                "sequence": None,
                "status": StrategyInputStatus.MISSING.value,
            },
            "product_id": "SHB-26JUN26-CDE",
            "reason": StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value,
            "source_order_ids": ["source-a", "source-b"],
        }
    ]


def test_consolidation_manager_is_available_as_builtin_strategy():
    strategies = configured_strategies((CONSOLIDATION_MANAGER_STRATEGY_ID,))

    assert [strategy.strategy_id for strategy in strategies] == [
        CONSOLIDATION_MANAGER_STRATEGY_ID
    ]


def _create_open_order(
    gateway: ActionGateway,
    *,
    action_id: str,
    price: str,
    size: str,
) -> None:
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id=action_id,
            limit_price=price,
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.SELL,
            size=size,
        ).to_command(),
        DryRunExecutor(),
    )


def _operator_policy_without_order_book_requirement():
    policy = load_operator_policy_from_json_file(Path("docs/examples/operator-policy.conservative-cfm-v0.json"))
    return replace(
        policy,
        market_data_requirements=replace(
            policy.market_data_requirements,
            require_order_book=False,
        ),
    )


def _snapshot(
    ledger: AuditLedger,
    *,
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
        product_catalog=ProductCatalog((_strategy_product("SHB-26JUN26-CDE"),)),
        projection=projection,
    )


def _strategy_product(product_id: str) -> ProductMetadata:
    return ProductMetadata(
        base_increment=Decimal("0.01"),
        base_max_size=Decimal("1"),
        base_min_size=Decimal("0.01"),
        price_increment=Decimal("0.01"),
        product_id=product_id,
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_max_size=Decimal("1000"),
        quote_min_size=Decimal("1"),
    )
