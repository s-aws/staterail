from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from actions.dry_run import DryRunExecutor
from actions.gateway import ActionGateway, PlaceOrderIntent
from app.ledger_health import ledger_health
from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    EventType,
    ExecutionMode,
    LedgerHealthStatus,
    OrderLifecycleStatus,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    StrategyManagerGate,
    StrategyManagerSkipReason,
)
from projections.state import SourceOfTruthProjection
from strategies import (
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
    StagedReleaseManagerStrategy,
    StrategyEvaluationTask,
    StrategySnapshot,
    configured_strategies,
    load_operator_policy_from_json_file,
    strategy_decision_commands,
)


def test_staged_release_manager_releases_oldest_fresh_policy_scoped_stage(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    _accept_order_book(core, "SHB-26JUN26-CDE")
    gateway.submit_and_execute(
        _staged_intent("stage-one", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    gateway.submit_and_execute(
        _staged_intent("stage-two", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        projection=projection,
    )

    decision = StagedReleaseManagerStrategy().evaluate(snapshot)
    commands = strategy_decision_commands(STAGED_RELEASE_MANAGER_STRATEGY_ID, decision)
    receipt = gateway.submit_and_execute(commands[0], DryRunExecutor())
    released_projection = SourceOfTruthProjection.from_ledger(ledger)
    release_action_id = decision.intents[0].action_id

    assert decision.metadata["released_staged_placement_ids"] == ["stage-one"]
    assert len(decision.intents) == 1
    assert decision.intents[0].placement_kind == OrderPlacementKind.RELEASE
    assert decision.intents[0].logical_order_id == "logical-stage-one"
    assert receipt.status == ActionStatus.EXECUTED
    assert released_projection.placements_by_id[release_action_id].placement_kind == (
        OrderPlacementKind.RELEASE
    )
    assert released_projection.orders_by_action_id[release_action_id].lifecycle_status == (
        OrderLifecycleStatus.OPEN
    )


def test_staged_release_manager_ignores_released_stages_on_later_evaluations(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    _accept_order_book(core, "SHB-26JUN26-CDE")
    gateway.submit_and_execute(
        _staged_intent("stage-one", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    gateway.submit_and_execute(
        _staged_intent("stage-two", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    strategy = StagedReleaseManagerStrategy()

    first_decision = strategy.evaluate(
        StrategySnapshot(
            as_of_sequence=SourceOfTruthProjection.from_ledger(ledger).last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            operator_policy=policy,
            projection=SourceOfTruthProjection.from_ledger(ledger),
        )
    )
    gateway.submit_and_execute(
        strategy_decision_commands(STAGED_RELEASE_MANAGER_STRATEGY_ID, first_decision)[0],
        DryRunExecutor(),
    )
    after_first_release = SourceOfTruthProjection.from_ledger(ledger)

    second_decision = strategy.evaluate(
        StrategySnapshot(
            as_of_sequence=after_first_release.last_sequence,
            evaluated_at=observed_at,
            execution_mode=ExecutionMode.DRY_RUN,
            ledger_path=ledger.path,
            operator_policy=policy,
            projection=after_first_release,
        )
    )

    assert after_first_release.released_staged_placement_ids == ("stage-one",)
    assert [placement.placement_id for placement in after_first_release.unreleased_staged_order_placements] == [
        "stage-two"
    ]
    assert second_decision.metadata["released_staged_placement_ids"] == ["stage-two"]
    assert second_decision.metadata["skipped_staged_placements"] == []


def test_staged_release_manager_blocks_without_fresh_order_book_or_policy(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    gateway.submit_and_execute(
        _staged_intent("stage-one", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    missing_book_snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        projection=projection,
    )
    missing_policy_snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        projection=projection,
    )

    missing_book_decision = StagedReleaseManagerStrategy().evaluate(missing_book_snapshot)
    missing_policy_decision = StagedReleaseManagerStrategy().evaluate(missing_policy_snapshot)

    assert missing_book_decision.intents == ()
    assert missing_book_decision.metadata["skipped_staged_placements"][0]["reason"] == (
        StrategyManagerSkipReason.ORDER_BOOK_NOT_FRESH.value
    )
    assert missing_policy_decision.intents == ()
    assert missing_policy_decision.metadata["release_gate"] == (
        StrategyManagerGate.OPERATOR_POLICY_MISSING.value
    )


def test_staged_release_manager_blocks_when_policy_disallows_release(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    staging_only_policy = replace(
        policy,
        staged_or_hidden_release=replace(policy.staged_or_hidden_release, allow_release=False),
    )
    _accept_order_book(core, "SHB-26JUN26-CDE")
    gateway.submit_and_execute(
        _staged_intent("stage-one", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=staging_only_policy,
        projection=projection,
    )

    decision = StagedReleaseManagerStrategy().evaluate(snapshot)

    assert decision.intents == ()
    assert decision.metadata["release_gate"] == StrategyManagerGate.RELEASE_DISABLED.value
    assert decision.metadata["released_staged_placement_ids"] == []
    assert decision.metadata["skipped_staged_placements"] == []


def test_staged_release_manager_blocks_when_release_conditions_no_longer_match(
    workspace_tmp_path,
):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedClock(observed_at))
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    _accept_order_book(core, "SHB-26JUN26-CDE", bid_price="98", ask_price="99.50")
    gateway.submit_and_execute(
        _staged_intent(
            "stage-one",
            limit_price="100",
            product_id="SHB-26JUN26-CDE",
            side=OrderSide.BUY,
        ).to_command(),
        DryRunExecutor(),
    )
    projection = SourceOfTruthProjection.from_ledger(ledger)
    snapshot = StrategySnapshot(
        as_of_sequence=projection.last_sequence,
        evaluated_at=observed_at,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        projection=projection,
    )

    decision = StagedReleaseManagerStrategy().evaluate(snapshot)

    assert decision.intents == ()
    assert decision.metadata["released_staged_placement_ids"] == []
    assert decision.metadata["skipped_staged_placements"] == [
        {
            "best_ask_price": "99.50",
            "best_bid_price": "98",
            "limit_price": "100",
            "matched": False,
            "placement_id": "stage-one",
            "reason": StrategyManagerSkipReason.RELEASE_CONDITIONS_NOT_MATCHED.value,
            "side": OrderSide.BUY.value,
        }
    ]


def test_staged_release_manager_task_releases_stage_through_dry_run_gateway(workspace_tmp_path):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(observed_at)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    core = AuditCore(ledger)
    gateway = ActionGateway(core)
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    _accept_order_book(core, "SHB-26JUN26-CDE")
    staged_receipt = gateway.submit_and_execute(
        _staged_intent("stage-one", product_id="SHB-26JUN26-CDE").to_command(),
        DryRunExecutor(),
    )
    task = StrategyEvaluationTask(
        core,
        action_gateway=gateway,
        clock=clock,
        execution_mode=ExecutionMode.DRY_RUN,
        executor=DryRunExecutor(),
        operator_policy=policy,
        strategies=(StagedReleaseManagerStrategy(),),
    )

    result = task.run()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    health = ledger_health(ledger.path)
    release_action_id = result["evaluations"][0]["action_receipts"][0]["action_id"]
    release_placement = projection.placements_by_id[release_action_id]
    release_order = projection.orders_by_action_id[release_action_id]

    assert staged_receipt.status == ActionStatus.ACCEPTED
    assert result["completed_count"] == 1
    assert result["failed_count"] == 0
    assert result["submitted_action_count"] == 1
    assert projection.released_staged_placement_ids == ("stage-one",)
    assert projection.unreleased_staged_order_placements == ()
    assert projection.release_placement_for_staged_placement("stage-one") == release_placement
    assert release_placement.placement_kind == OrderPlacementKind.RELEASE
    assert release_placement.release_of_placement_id == "stage-one"
    assert release_order.lifecycle_status == OrderLifecycleStatus.OPEN
    assert health.status == LedgerHealthStatus.OK


def test_staged_release_manager_is_available_as_builtin_strategy():
    strategies = configured_strategies((STAGED_RELEASE_MANAGER_STRATEGY_ID,))

    assert [strategy.strategy_id for strategy in strategies] == [STAGED_RELEASE_MANAGER_STRATEGY_ID]


def _staged_intent(
    action_id: str,
    *,
    limit_price: str = "100",
    product_id: str,
    side: OrderSide = OrderSide.BUY,
) -> PlaceOrderIntent:
    return PlaceOrderIntent(
        action_id=action_id,
        idempotency_key=f"{action_id}-client-order",
        limit_price=limit_price,
        logical_order_id=f"logical-{action_id}",
        order_type=OrderType.LIMIT,
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
        post_only=True,
        product_id=product_id,
        side=side,
        size="0.01",
    )


def _accept_order_book(
    core: AuditCore,
    product_id: str,
    *,
    ask_price: str = "101",
    bid_price: str = "99",
) -> None:
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
                                {"new_quantity": "2", "price_level": bid_price, "side": "bid"},
                                {"new_quantity": "3", "price_level": ask_price, "side": "offer"},
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
