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
    ActionType,
    EventType,
    ExecutionMode,
    OperatorPolicyPermission,
    OrderLifecycleStatus,
    OrderPlacementKind,
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
    ANCHOR_REPRICING_MANAGER_STRATEGY_ID,
    AnchorRepricingManagerStrategy,
    StrategySnapshot,
    configured_strategies,
    load_operator_policy_from_json_file,
    strategy_decision_commands,
)


def test_anchor_repricing_manager_cancel_replaces_open_order_outside_anchor_band(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    _create_open_order(gateway, action_id="source-order", price="90")
    _accept_order_book(core, "SHB-26JUN26-CDE", bid="104", ask="106")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = _operator_policy()

    decision = AnchorRepricingManagerStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=policy)
    )
    commands = strategy_decision_commands(ANCHOR_REPRICING_MANAGER_STRATEGY_ID, decision)
    receipts = tuple(gateway.submit_and_execute(command, DryRunExecutor()) for command in commands)
    after_move = SourceOfTruthProjection.from_ledger(ledger)

    assert [command.action_type for command in commands] == [
        ActionType.CANCEL_ORDER,
        ActionType.PLACE_ORDER,
    ]
    assert [receipt.status for receipt in receipts] == [
        ActionStatus.EXECUTED,
        ActionStatus.EXECUTED,
    ]
    assert decision.metadata["created_moves"] == [
        {
            "cancel_action_id": commands[0].action_id,
            "current_price": "90",
            "logical_order_id": "logical-source",
            "placement_action_id": commands[1].action_id,
            "product_id": "SHB-26JUN26-CDE",
            "reference_price": "105",
            "side": "buy",
            "source_action_id": "source-order",
            "target_price": "99.75",
        }
    ]
    assert after_move.orders_by_action_id["source-order"].lifecycle_status == (
        OrderLifecycleStatus.CANCELLED
    )
    replacement = after_move.orders_by_action_id[commands[1].action_id]
    assert replacement.lifecycle_status == OrderLifecycleStatus.OPEN
    assert replacement.limit_price == "99.75"
    assert replacement.side == OrderSide.BUY
    assert after_move.placements_by_id[commands[1].action_id].placement_kind == (
        OrderPlacementKind.CANCEL_REPLACE
    )
    assert after_move.logical_orders_by_id["logical-source"].placement_ids == [
        "source-order",
        commands[1].action_id,
    ]


def test_anchor_repricing_manager_respects_cooldown_after_move(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = _operator_policy()
    _create_open_order(gateway, action_id="source-order", price="90")
    _accept_order_book(core, "SHB-26JUN26-CDE", bid="104", ask="106")
    first_decision = AnchorRepricingManagerStrategy().evaluate(
        _snapshot(
            ledger,
            projection=SourceOfTruthProjection.from_ledger(ledger),
            observed_at=observed_at,
            policy=policy,
        )
    )
    for command in strategy_decision_commands(ANCHOR_REPRICING_MANAGER_STRATEGY_ID, first_decision):
        gateway.submit_and_execute(command, DryRunExecutor())
    _accept_order_book(core, "SHB-26JUN26-CDE", bid="105", ask="130", sequence=2)

    second_decision = AnchorRepricingManagerStrategy().evaluate(
        _snapshot(
            ledger,
            projection=SourceOfTruthProjection.from_ledger(ledger),
            observed_at=observed_at,
            policy=policy,
        )
    )

    assert second_decision.intents == ()
    assert second_decision.metadata["skipped_orders"] == [
        {
            "cooldown_seconds": 30.0,
            "latest_move_at": "2026-01-01T00:00:00+00:00",
            "logical_order_id": "logical-source",
            "message": "anchor repricing cooldown is active",
            "reason": StrategyManagerSkipReason.REPRICE_COOLDOWN_ACTIVE.value,
            "source_action_id": first_decision.intents[1].action_id,
        }
    ]


def test_anchor_repricing_manager_blocks_without_required_policy_inputs(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    gateway = ActionGateway(AuditCore(ledger))
    _create_open_order(gateway, action_id="source-order", price="90")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    policy = _operator_policy()
    disabled_anchor = replace(policy, anchor_repricing=replace(policy.anchor_repricing, enabled=False))
    disabled_move = replace(
        policy,
        lineage=replace(policy.lineage, move_same_side_orders=OperatorPolicyPermission.DISABLED),
    )

    missing_policy_decision = AnchorRepricingManagerStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            projection=projection,
        )
    )
    missing_catalog_decision = AnchorRepricingManagerStrategy().evaluate(
        StrategySnapshot(
            as_of_sequence=projection.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            operator_policy=policy,
            projection=projection,
        )
    )
    disabled_anchor_decision = AnchorRepricingManagerStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=disabled_anchor)
    )
    disabled_move_decision = AnchorRepricingManagerStrategy().evaluate(
        _snapshot(ledger, projection=projection, observed_at=observed_at, policy=disabled_move)
    )

    assert missing_policy_decision.intents == ()
    assert missing_policy_decision.metadata["anchor_repricing_gate"] == (
        StrategyManagerGate.OPERATOR_POLICY_MISSING.value
    )
    assert missing_catalog_decision.intents == ()
    assert missing_catalog_decision.metadata["anchor_repricing_gate"] == (
        StrategyManagerGate.PRODUCT_CATALOG_MISSING.value
    )
    assert disabled_anchor_decision.intents == ()
    assert disabled_anchor_decision.metadata["anchor_repricing_gate"] == (
        StrategyManagerGate.ANCHOR_REPRICING_DISABLED.value
    )
    assert disabled_move_decision.intents == ()
    assert disabled_move_decision.metadata["anchor_repricing_gate"] == (
        StrategyManagerGate.ANCHOR_REPRICING_POLICY_UNSAFE.value
    )


def test_anchor_repricing_manager_is_available_as_builtin_strategy():
    strategies = configured_strategies((ANCHOR_REPRICING_MANAGER_STRATEGY_ID,))

    assert [strategy.strategy_id for strategy in strategies] == [
        ANCHOR_REPRICING_MANAGER_STRATEGY_ID
    ]


def _create_open_order(
    gateway: ActionGateway,
    *,
    action_id: str,
    price: str,
) -> None:
    gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id=action_id,
            idempotency_key=f"{action_id}-client-order",
            limit_price=price,
            logical_order_id="logical-source",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.BUY,
            size="0.05",
        ).to_command(),
        DryRunExecutor(),
    )


def _accept_order_book(
    core: AuditCore,
    product_id: str,
    *,
    ask: str,
    bid: str,
    sequence: int = 1,
) -> None:
    received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": f"coinbase:l2_data:{product_id}:{sequence}",
            "payload": {
                "channel": "l2_data",
                "raw": {
                    "channel": "l2_data",
                    "events": [
                        {
                            "product_id": product_id,
                            "type": "snapshot",
                            "updates": [
                                {"new_quantity": "2", "price_level": bid, "side": "bid"},
                                {"new_quantity": "3", "price_level": ask, "side": "offer"},
                            ],
                        }
                    ],
                    "sequence_num": sequence,
                    "timestamp": "2026-01-01T00:00:00Z",
                },
                "sequence_num": sequence,
                "timestamp": "2026-01-01T00:00:00Z",
            },
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": f"coinbase:l2_data:{product_id}:{sequence}",
            "received_sequence": received.sequence,
            "source_id": "coinbase-primary",
        },
    )


def _operator_policy():
    return load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.stealth-orders-manager-v1.json")
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
