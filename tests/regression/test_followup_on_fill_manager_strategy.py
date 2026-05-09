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
    ActionStatus,
    EventType,
    ExecutionMode,
    OrderLineageRelation,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyManagerGate,
    StrategyManagerSkipReason,
)
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from strategies import (
    FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,
    FollowupOnFillManagerStrategy,
    StrategySnapshot,
    configured_strategies,
    load_operator_policy_from_json_file,
    strategy_decision_commands,
)


def test_followup_on_fill_manager_creates_oldest_policy_followup_once(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    _create_filled_parent(core, gateway)
    projection = SourceOfTruthProjection.from_ledger(ledger)
    strategy = FollowupOnFillManagerStrategy()

    first_decision = strategy.evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=policy)
    )
    commands = strategy_decision_commands(FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID, first_decision)
    receipt = gateway.submit_and_execute(commands[0], DryRunExecutor())
    after_followup = SourceOfTruthProjection.from_ledger(ledger)
    second_decision = strategy.evaluate(
        _snapshot(ledger, projection=after_followup, observed_at=observed_at, policy=policy)
    )

    assert first_decision.metadata["created_followup_fill_ids"] == ["fill-1"]
    assert len(first_decision.intents) == 1
    assert first_decision.intents[0].side == OrderSide.SELL
    assert first_decision.intents[0].lineage_relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL
    assert receipt.status == ActionStatus.EXECUTED
    assert second_decision.intents == ()
    assert second_decision.metadata["skipped_fills"] == [
        {
            "fill_id": "fill-1",
            "message": "followup action already exists for fill",
            "reason": StrategyManagerSkipReason.STRATEGY_CONTRACT_ERROR.value,
        }
    ]


def test_followup_on_fill_manager_blocks_without_policy_or_catalog(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    _create_filled_parent(core, gateway)
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )

    missing_policy_decision = FollowupOnFillManagerStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            projection=projection,
        )
    )
    missing_catalog_decision = FollowupOnFillManagerStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            operator_policy=policy,
            projection=projection,
        )
    )
    disabled_followup_decision = FollowupOnFillManagerStrategy().evaluate(
        _snapshot(
            ledger,
            projection=projection,
            observed_at=observed_at,
            policy=replace(
                policy,
                partial_fills=replace(policy.partial_fills, followup_enabled=False),
            ),
        )
    )

    assert missing_policy_decision.intents == ()
    assert missing_policy_decision.metadata["followup_gate"] == (
        StrategyManagerGate.OPERATOR_POLICY_MISSING.value
    )
    assert missing_catalog_decision.intents == ()
    assert missing_catalog_decision.metadata["followup_gate"] == (
        StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
    )
    assert disabled_followup_decision.intents == ()
    assert disabled_followup_decision.metadata["followup_gate"] == (
        StrategyManagerGate.FOLLOWUP_DISABLED.value
    )


def test_followup_on_fill_manager_is_available_as_builtin_strategy():
    strategies = configured_strategies((FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID,))

    assert [strategy.strategy_id for strategy in strategies] == [
        FOLLOWUP_ON_FILL_MANAGER_STRATEGY_ID
    ]


def _create_filled_parent(core: AuditCore, gateway: ActionGateway) -> None:
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
    projection = SourceOfTruthProjection.from_ledger(core.ledger)
    exchange_order_id = projection.orders_by_action_id["parent-action"].exchange_order_id
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
