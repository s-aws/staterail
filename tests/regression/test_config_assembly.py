from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from actions.dry_run import DryRunExecutor
from actions.gateway import PlaceOrderIntent
from actions.venue_guard import ProductVenueRestrictedExecutor
from audit.ledger import AuditLedger
from audit.archives import create_worm_ledger_archive_receipt
from config.assembly import (
    AuditAnchorStoreConfig,
    AuditArchiveStoreConfig,
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
    CoinbaseWebSocketSourceConfig,
    FeedRuntimeConfig,
    MessageTriggerConfig,
    ProductCatalogRuntimeConfig,
    ReconciliationRuntimeConfig,
    RiskPolicyConfig,
    StrategyRuntimeConfig,
    TaskScheduleConfig,
    TimeTriggerConfig,
    assemble_coinbase_runtime,
    trigger_engine_from_config,
)
from core.engine import AuditCore
from core.clock import FixedClock
from core.enums import (
    ActionFailureReason,
    AnchorImmutabilityMode,
    AnchorStoreType,
    ActionStatus,
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    ErrorCode,
    EventType,
    ExchangeLookupStatus,
    ExecutionMode,
    ExecutionStatus,
    HttpMethod,
    LedgerAnchorStoreProvider,
    MarginType,
    MarketDataKind,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    RiskCheckStatus,
    RiskRule,
    RuntimeTask,
    StrategyInputStatus,
    TriggerRelation,
)
from exchanges.coinbase.advanced_trade_rest import (
    CoinbaseAdvancedTradeRestExecutor,
    CoinbaseRestRetryPolicy,
    HttpResponse,
)
from exchanges.coinbase.auth import static_token_provider
from feeds import FeedMessage
from feeds.supervisor import ReconnectPolicy
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from reconciliation.positions import ExchangeStateReconciliationPolicy
from strategies import (
    CONSOLIDATION_MANAGER_STRATEGY_ID,
    PASSIVE_MARKET_MAKING_STRATEGY_ID,
    STAGED_RELEASE_MANAGER_STRATEGY_ID,
    ConsolidationManagerStrategy,
    PassiveMarketMakingStrategy,
    StagedReleaseManagerStrategy,
    StrategyDecision,
    StrategyInputRequirement,
    load_operator_policy_from_json_file,
)
from triggers.rules import TimeTrigger, TriggerEngine


class PluginStrategy:
    @property
    def strategy_id(self) -> str:
        return "plugin"

    def evaluate(self, snapshot: object) -> StrategyDecision:
        del snapshot
        return StrategyDecision()


class OrderPlacingStrategy:
    @property
    def strategy_id(self) -> str:
        return "order-placing"

    def evaluate(self, snapshot: object) -> StrategyDecision:
        del snapshot
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id="blocked-action",
                    limit_price="50000",
                    order_type=OrderType.LIMIT,
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    size="0.01",
                ),
            )
        )


def test_coinbase_bot_config_defaults_to_dry_run_watchdog_only(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig()

    assembly = assemble_coinbase_runtime(config=config, core=core)

    assert isinstance(assembly.rest_executor, DryRunExecutor)
    assert assembly.order_lookup_client is None
    assert assembly.fill_lookup_client is None
    assert assembly.account_lookup_client is None
    assert assembly.websocket_configs == ()
    assert assembly.rest_config.execution_mode == ExecutionMode.DRY_RUN
    assert config.feed.min_live_sources == 1
    assert config.feed.stale_after == timedelta(seconds=30)
    assert config.feed_health_schedule.enabled is True
    assert config.product_catalog.schedule.enabled is False
    assert config.strategies.schedule.enabled is False
    assert config.trigger_polling_schedule.enabled is False
    assert config.risk.kill_switch_enabled is False


def test_strategy_schedule_runs_configured_noop_strategy(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=("noop",),
        ),
    )

    assembly = assemble_coinbase_runtime(config=config, core=core)
    completed_cycles = asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = core.ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(core.ledger)

    assert completed_cycles == 1
    assert assembly.strategy_task is not None
    assert [strategy.strategy_id for strategy in assembly.strategies] == ["noop"]
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.STRATEGY_EVALUATION_STARTED,
        EventType.STRATEGY_EVALUATION_COMPLETED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[1].payload["task_id"] == RuntimeTask.STRATEGY_EVALUATION.value
    assert records[4].payload["result"]["strategy_count"] == 1
    assert projection.strategy_evaluations[0].strategy_id == "noop"


def test_strategy_schedule_blocks_configured_missing_market_data_requirement(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        strategies=StrategyRuntimeConfig(
            market_data_requirements=(
                StrategyInputRequirement(
                    data_kind=MarketDataKind.TICKER,
                    max_age=timedelta(seconds=5),
                    product_id="BTC-USD",
                ),
            ),
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=("order-placing",),
        ),
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        strategies=(OrderPlacingStrategy(),),
    )
    asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = core.ledger.iter_records()

    assert EventType.ACTION_REQUESTED not in [record.event_type for record in records]
    assert any(
        record.event_type == EventType.ERROR
        and record.payload["error_code"] == ErrorCode.STRATEGY_INPUT_UNAVAILABLE.value
        for record in records
    )
    failed = next(record for record in records if record.event_type == EventType.STRATEGY_EVALUATION_FAILED)
    assert failed.payload["input_freshness"][0]["status"] == StrategyInputStatus.MISSING.value


def test_strategy_schedule_applies_operator_policy_market_data_requirements(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    config = CoinbaseBotConfig(
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        strategies=StrategyRuntimeConfig(
            operator_policy=operator_policy,
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=("noop",),
        ),
    )

    assembly = assemble_coinbase_runtime(config=config, core=core)
    asyncio.run(assembly.orchestrator.run(max_cycles=1))
    failed = next(
        record
        for record in core.ledger.iter_records()
        if record.event_type == EventType.STRATEGY_EVALUATION_FAILED
    )

    assert len(failed.payload["input_freshness"]) == 2
    assert {item["product_id"] for item in failed.payload["input_freshness"]} == {
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    }
    assert all(
        item["status"] == StrategyInputStatus.MISSING.value
        for item in failed.payload["input_freshness"]
    )


def test_strategy_schedule_requires_configured_strategy_ids(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            )
        )
    )

    with pytest.raises(ValueError, match="strategy_ids"):
        assemble_coinbase_runtime(config=config, core=core)


def test_strategy_schedule_can_select_entry_point_strategies(monkeypatch, workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    loaded_strategy_ids: list[tuple[str, ...]] = []

    def fake_strategy_selector(
        strategy_ids: tuple[str, ...],
        *,
        static_strategies=(),
        strategy_parameters=None,
    ):
        assert static_strategies == ()
        assert strategy_parameters == {}
        loaded_strategy_ids.append(strategy_ids)
        return (PluginStrategy(),)

    monkeypatch.setattr("config.assembly.configured_strategies", fake_strategy_selector)
    config = CoinbaseBotConfig(
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=("plugin",),
        ),
    )

    assembly = assemble_coinbase_runtime(config=config, core=core)

    assert loaded_strategy_ids == [("plugin",)]
    assert [strategy.strategy_id for strategy in assembly.strategies] == ["plugin"]


def test_strategy_schedule_applies_builtin_strategy_parameters(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=(PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            strategy_parameters={
                PASSIVE_MARKET_MAKING_STRATEGY_ID: {
                    "half_spread_bps": "25",
                    "max_products_per_evaluation": 1,
                    "max_staged_release_count_per_side": 2,
                    "target_notional_usd": "7.50",
                }
            },
        )
    )

    assembly = assemble_coinbase_runtime(config=config, core=core)
    strategy = assembly.strategies[0]

    assert isinstance(strategy, PassiveMarketMakingStrategy)
    assert strategy.half_spread_bps == Decimal("25")
    assert strategy.max_products_per_evaluation == 1
    assert strategy.max_staged_release_count_per_side == 2
    assert strategy.target_notional_usd == Decimal("7.50")


def test_strategy_schedule_applies_staged_release_manager_parameters(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=(STAGED_RELEASE_MANAGER_STRATEGY_ID,),
            strategy_parameters={
                STAGED_RELEASE_MANAGER_STRATEGY_ID: {
                    "allow_live_overlap": True,
                    "max_releases_per_evaluation": 2,
                }
            },
        )
    )

    assembly = assemble_coinbase_runtime(config=config, core=core)
    strategy = assembly.strategies[0]

    assert isinstance(strategy, StagedReleaseManagerStrategy)
    assert strategy.allow_live_overlap is True
    assert strategy.max_releases_per_evaluation == 2


def test_strategy_schedule_applies_other_manager_strategy_parameters(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=(CONSOLIDATION_MANAGER_STRATEGY_ID,),
            strategy_parameters={
                CONSOLIDATION_MANAGER_STRATEGY_ID: {
                    "max_consolidations_per_evaluation": 2,
                    "max_source_orders_per_consolidation": 3,
                }
            },
        )
    )

    assembly = assemble_coinbase_runtime(config=config, core=core)
    strategy = assembly.strategies[0]

    assert isinstance(strategy, ConsolidationManagerStrategy)
    assert strategy.max_consolidations_per_evaluation == 2
    assert strategy.max_source_orders_per_consolidation == 3


def test_strategy_runtime_config_rejects_unselected_strategy_parameters():
    with pytest.raises(ValueError, match="unselected strategy_id"):
        StrategyRuntimeConfig(
            strategy_ids=("noop",),
            strategy_parameters={PASSIVE_MARKET_MAKING_STRATEGY_ID: {"target_notional_usd": "5"}},
        )


def test_strategy_schedule_requires_explicit_live_strategy_allowance(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
        strategies=StrategyRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=("noop",),
        ),
    )

    with pytest.raises(ValueError, match="allow_live_execution"):
        assemble_coinbase_runtime(
            config=config,
            core=core,
            token_provider=static_token_provider("test-token"),
        )


def test_strategy_schedule_allows_live_when_explicitly_configured(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        strategies=StrategyRuntimeConfig(
            allow_live_execution=True,
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.STRATEGY_EVALUATION,
                interval=timedelta(seconds=1),
                enabled=True,
            ),
            strategy_ids=("noop",),
        ),
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        token_provider=static_token_provider("test-token"),
    )

    assert assembly.strategy_task is not None
    assert assembly.rest_config.execution_mode == ExecutionMode.LIVE


def test_configured_risk_policy_builds_runtime_action_gateway(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        risk=RiskPolicyConfig(
            allowed_order_types=(OrderType.LIMIT,),
            allowed_products=("BTC-USD",),
            max_order_size=Decimal("1"),
            kill_switch_enabled=True,
        )
    )
    assembly = assemble_coinbase_runtime(config=config, core=core)

    receipt = assembly.action_gateway.submit(
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="50000",
        ).to_command()
    )
    records = core.ledger.iter_records()

    assert receipt.status == ActionStatus.REJECTED
    assert records[-1].event_type == EventType.ACTION_REJECTED
    assert records[-1].payload["risk_evaluation"]["checks"][0]["rule"] == RiskRule.KILL_SWITCH.value


def test_operator_policy_extends_runtime_action_gateway_risk_controls(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    operator_policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    config = CoinbaseBotConfig(
        strategies=StrategyRuntimeConfig(operator_policy=operator_policy)
    )
    assembly = assemble_coinbase_runtime(config=config, core=core)

    receipt = assembly.action_gateway.submit(
        PlaceOrderIntent(
            action_id="action-1",
            limit_price="10",
            order_type=OrderType.LIMIT,
            post_only=True,
            product_id="SHB-26JUN26-CDE",
            reduce_only=True,
            side=OrderSide.BUY,
            size="1",
        ).to_command()
    )
    records = core.ledger.iter_records()

    assert receipt.status == ActionStatus.REJECTED
    assert records[-1].event_type == EventType.ACTION_REJECTED
    assert records[-1].payload["risk_evaluation"]["checks"][0]["rule"] == RiskRule.KILL_SWITCH.value


def test_runtime_risk_gate_uses_injected_product_catalog(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        risk=RiskPolicyConfig(
            allowed_order_types=(OrderType.LIMIT,),
            allowed_products=("BTC-USD",),
        )
    )
    catalog = ProductCatalog(
        (
            ProductMetadata(
                product_id="BTC-USD",
                product_type=ProductType.SPOT,
                trading_disabled=True,
            ),
        )
    )
    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        product_catalog=catalog,
    )

    receipt = assembly.action_gateway.submit(
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="50000",
        ).to_command()
    )
    records = core.ledger.iter_records()

    assert receipt.status == ActionStatus.REJECTED
    failed_rules = {
        check["rule"]
        for check in records[-1].payload["risk_evaluation"]["checks"]
        if check["status"] == RiskCheckStatus.FAIL.value
    }
    assert RiskRule.PRODUCT_TRADABLE.value in failed_rules


def test_trigger_polling_schedule_runs_due_time_triggers(workspace_tmp_path):
    now = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    clock = FixedClock(now)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    triggers = TriggerEngine(clock=clock)
    triggers.register(TimeTrigger("after-noon", TriggerRelation.AFTER, now - timedelta(seconds=5)))
    core = AuditCore(ledger, triggers=triggers)
    config = CoinbaseBotConfig(
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        trigger_polling_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.TRIGGER_POLLING,
            interval=timedelta(seconds=1),
            enabled=True,
        ),
    )

    assembly = assemble_coinbase_runtime(config=config, core=core, clock=clock)
    completed_cycles = asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = ledger.iter_records()

    assert completed_cycles == 1
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.TRIGGER_FIRED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[1].payload["task_id"] == RuntimeTask.TRIGGER_POLLING.value
    assert records[2].payload["trigger_id"] == "after-noon"


def test_configured_trigger_rules_build_runtime_trigger_engine(workspace_tmp_path):
    now = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    clock = FixedClock(now)
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock))
    config = CoinbaseBotConfig(
        trigger_polling_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.TRIGGER_POLLING,
            interval=timedelta(seconds=1),
            enabled=True,
        ),
        trigger_rules=(
            TimeTriggerConfig(
                trigger_id="after-noon",
                relation=TriggerRelation.AFTER,
                target_time=now - timedelta(seconds=5),
            ),
            MessageTriggerConfig(
                trigger_id="after-error",
                relation=TriggerRelation.AFTER,
                event_type=EventType.ERROR,
            ),
        ),
    )
    triggers = trigger_engine_from_config(config, clock=clock)
    assert triggers is not None
    core = AuditCore(core.ledger, triggers=triggers)

    core.emit_due_time_triggers()
    core.emit(EventType.ERROR, {"message": "operator attention"})
    records = core.ledger.iter_records()
    trigger_records = [record for record in records if record.event_type == EventType.TRIGGER_FIRED]

    assert [record.payload["trigger_id"] for record in trigger_records] == [
        "after-noon",
        "after-error",
    ]


def test_live_config_assembles_rest_clients_websockets_and_runtime_tasks(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        rest=CoinbaseRestApiConfig(
            execution_mode=ExecutionMode.LIVE,
            retail_portfolio_id="portfolio-1",
            perpetual_portfolio_uuid="perp-portfolio-1",
        ),
        reconciliation=ReconciliationRuntimeConfig(
            order_recovery_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.ORDER_RECOVERY,
                interval=timedelta(seconds=10),
            ),
            fill_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.FILL_RECONCILIATION,
                interval=timedelta(seconds=20),
            ),
            exchange_state_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.EXCHANGE_STATE_RECONCILIATION,
                interval=timedelta(seconds=30),
            ),
            exchange_state_policy=ExchangeStateReconciliationPolicy(position_size_tolerance="0.001"),
        ),
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-primary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-user-primary",
                channels=(CoinbaseWebSocketChannel.USER,),
                endpoint=CoinbaseWebSocketEndpoint.USER_ORDER_DATA,
                product_ids=("BTC-USD",),
            ),
        ),
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        jwt_factory=lambda payload: f"jwt-for-{payload['channel']}",
        token_provider=static_token_provider("test-token"),
    )

    assert isinstance(assembly.rest_executor, CoinbaseAdvancedTradeRestExecutor)
    assert assembly.order_lookup_client is not None
    assert assembly.fill_lookup_client is not None
    assert assembly.account_lookup_client is not None
    assert assembly.exchange_state_reconciliation is not None
    assert assembly.websocket_configs[0].endpoint == CoinbaseWebSocketEndpoint.MARKET_DATA
    assert assembly.websocket_configs[1].endpoint == CoinbaseWebSocketEndpoint.USER_ORDER_DATA
    assert assembly.websocket_configs[1].subscription_messages()[1]["jwt"] == "jwt-for-user"
    assert assembly.feed_router is not None
    assert assembly.feed_supervisor is not None
    assert len(assembly.websocket_feed_sources) == 2
    assert any(task.task_id == RuntimeTask.FEED_HEALTH for task in assembly.orchestrator._tasks)


def test_live_execution_rejects_unsupported_product_venue_before_http(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    catalog = ProductCatalog(
        (
            ProductMetadata(
                product_id="BTC-PERP-INTX",
                product_type=ProductType.FUTURE,
                product_venue=ProductVenue.INTX,
            ),
        )
    )
    transport = FakePostTransport(
        [
            HttpResponse(
                status_code=200,
                body={"success": True, "success_response": {"order_id": "exchange-1"}},
            )
        ]
    )
    config = CoinbaseBotConfig(rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE))

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        product_catalog=catalog,
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )
    receipt = assembly.action_gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="action-intx-1",
            product_id="BTC-PERP-INTX",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="50000",
            margin_type=MarginType.CROSS,
        ).to_command(),
        assembly.rest_executor,
    )
    failed_record = _only_record(core, EventType.ACTION_EXECUTION_FAILED)
    execution_result = failed_record.payload["execution_result"]

    assert isinstance(assembly.rest_executor, ProductVenueRestrictedExecutor)
    assert receipt.status == ActionStatus.FAILED
    assert receipt.failure_reason == ActionFailureReason.EXECUTION_REJECTED
    assert transport.posts == []
    assert failed_record.payload["failure_reason"] == ActionFailureReason.EXECUTION_REJECTED.value
    assert execution_result["status"] == ExecutionStatus.REJECTED.value
    assert execution_result["error_code"] == ErrorCode.UNSUPPORTED_PRODUCT_VENUE.value
    assert execution_result["raw_response"]["product_venue"] == ProductVenue.INTX.value
    assert execution_result["raw_response"]["allowed_product_venues"] == [
        ProductVenue.CBE.value,
        ProductVenue.FCM.value,
    ]


def test_live_execution_allows_cfm_product_venue_to_reach_http_transport(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    catalog = ProductCatalog(
        (
            ProductMetadata(
                product_id="BIT-29MAY26-CDE",
                product_type=ProductType.FUTURE,
                product_venue=ProductVenue.FCM,
            ),
        )
    )
    transport = FakePostTransport(
        [
            HttpResponse(
                status_code=200,
                body={"success": True, "success_response": {"order_id": "exchange-cfm-1"}},
            )
        ]
    )
    config = CoinbaseBotConfig(rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE))

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        product_catalog=catalog,
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )
    receipt = assembly.action_gateway.submit_and_execute(
        PlaceOrderIntent(
            action_id="action-cfm-1",
            product_id="BIT-29MAY26-CDE",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="1",
            limit_price="50000",
            margin_type=MarginType.CROSS,
        ).to_command(),
        assembly.rest_executor,
    )
    executed_record = _only_record(core, EventType.ACTION_EXECUTED)
    execution_result = executed_record.payload["execution_result"]

    assert isinstance(assembly.rest_executor, ProductVenueRestrictedExecutor)
    assert receipt.status == ActionStatus.EXECUTED
    assert len(transport.posts) == 1
    assert transport.posts[0]["json_body"]["product_id"] == "BIT-29MAY26-CDE"
    assert transport.posts[0]["json_body"]["margin_type"] == "CROSS"
    assert execution_result["status"] == ExecutionStatus.ACCEPTED.value
    assert execution_result["exchange_order_id"] == "exchange-cfm-1"


def test_exchange_state_policy_inherits_missing_portfolio_ids_from_rest_config(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        rest=CoinbaseRestApiConfig(
            retail_portfolio_id="rest-retail-portfolio",
            perpetual_portfolio_uuid="rest-perp-portfolio",
        ),
        reconciliation=ReconciliationRuntimeConfig(
            exchange_state_policy=ExchangeStateReconciliationPolicy(
                retail_portfolio_id="policy-retail-portfolio",
                position_product_ids=("policy-product",),
            )
        ),
    )

    assembly = assemble_coinbase_runtime(config=config, core=core, token_provider=static_token_provider("test-token"))

    assert assembly.exchange_state_reconciliation is not None
    policy = assembly.exchange_state_reconciliation._policy
    assert policy.retail_portfolio_id == "policy-retail-portfolio"
    assert policy.perpetual_portfolio_uuid == "rest-perp-portfolio"
    assert policy.position_product_ids == ("policy-product",)


def test_exchange_state_policy_derives_product_scope_from_runtime_config(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        risk=RiskPolicyConfig(allowed_products=("BTC-USD", "ETH-USD")),
        product_catalog=ProductCatalogRuntimeConfig(product_ids=("ETH-USD", "SOL-USD")),
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-primary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("SOL-USD", "DOGE-USD"),
            ),
        ),
    )

    assembly = assemble_coinbase_runtime(config=config, core=core, token_provider=static_token_provider("test-token"))

    assert assembly.exchange_state_reconciliation is not None
    assert assembly.exchange_state_reconciliation._policy.position_product_ids == (
        "BTC-USD",
        "ETH-USD",
        "SOL-USD",
        "DOGE-USD",
    )


def test_assembly_runs_configured_websocket_sources_through_feed_supervisor(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    message_key = "BTC-USD:level2:101"
    config = CoinbaseBotConfig(
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-primary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-secondary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
        )
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        websocket_source_factory=lambda source_config: ScriptedFeedSource(
            source_config.source_id,
            [
                FeedMessage(
                    source_config.source_id,
                    message_key,
                    EventType.DATA_RECEIVED,
                    {"sequence": 101},
                )
            ],
        ),
    )

    assert assembly.feed_supervisor is not None
    asyncio.run(assembly.feed_supervisor.run(max_attempts_per_source=1))
    event_types = [record.event_type for record in core.ledger.iter_records()]

    assert event_types.count(EventType.DATA_ACCEPTED) == 1
    assert event_types.count(EventType.DATA_DUPLICATE) == 1
    assert event_types.count(EventType.FEED_CONNECTED) == 2


def test_assembly_uses_configured_feed_runtime_policy(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        feed=FeedRuntimeConfig(
            min_live_sources=2,
            reconnect_policy=ReconnectPolicy(
                initial_delay_seconds=0,
                max_delay_seconds=0,
                multiplier=1,
            ),
            stale_after=timedelta(seconds=5),
        ),
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-primary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-secondary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
        ),
    )
    sleep_calls: list[float] = []

    async def fake_sleep(delay_seconds: float) -> None:
        sleep_calls.append(delay_seconds)

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        sleep=fake_sleep,
        websocket_source_factory=lambda source_config: ScriptedFeedSource(source_config.source_id, []),
    )

    assert assembly.feed_router is not None
    assert assembly.feed_supervisor is not None
    asyncio.run(assembly.feed_supervisor.run(max_attempts_per_source=2))

    records = core.ledger.iter_records()
    reconnect_records = [
        record for record in records if record.event_type == EventType.FEED_RECONNECT_SCHEDULED
    ]
    degraded_records = [record for record in records if record.event_type == EventType.FEED_DEGRADED]
    assert reconnect_records
    assert all(record.payload["delay_seconds"] == 0 for record in reconnect_records)
    assert sleep_calls == [0, 0]
    assert all(record.payload["min_live_sources"] == 2 for record in degraded_records)


def test_feed_health_schedule_audits_stale_sources_during_runtime(workspace_tmp_path):
    now = datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
    clock = FixedClock(now)
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock))
    config = CoinbaseBotConfig(
        feed=FeedRuntimeConfig(stale_after=timedelta(seconds=5)),
        feed_health_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.FEED_HEALTH,
            interval=timedelta(seconds=1),
            enabled=True,
            run_on_start=True,
        ),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-primary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
        ),
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        clock=clock,
        websocket_source_factory=lambda source_config: ScriptedFeedSource(source_config.source_id, []),
    )
    assert assembly.feed_router is not None
    assembly.feed_router.mark_connected(
        "coinbase-market-primary",
        seen_at=now - timedelta(seconds=10),
    )

    completed_cycles = asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = core.ledger.iter_records()
    degraded_records = [record for record in records if record.event_type == EventType.FEED_DEGRADED]

    assert completed_cycles == 1
    assert records[1].payload["task_id"] == RuntimeTask.FEED_HEALTH.value
    assert degraded_records
    assert degraded_records[0].payload["live_count"] == 0
    assert degraded_records[0].payload["stale_sources"] == ["coinbase-market-primary"]


def test_assembly_rejects_websocket_source_factory_id_mismatch(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-market-primary",
                channels=(CoinbaseWebSocketChannel.LEVEL2,),
                endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                product_ids=("BTC-USD",),
            ),
        )
    )

    with pytest.raises(ValueError, match="source_id"):
        assemble_coinbase_runtime(
            config=config,
            core=core,
            websocket_source_factory=lambda source_config: ScriptedFeedSource("wrong-source", []),
        )


def test_assembly_wraps_rest_clients_with_audited_retry_transport(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    sleep_calls: list[float] = []
    transport = FakeGetTransport(
        [
            HttpResponse(status_code=500, body={"message": "try again"}),
            HttpResponse(status_code=200, body={"order": {"order_id": "exchange-1", "status": "OPEN"}}),
        ]
    )
    config = CoinbaseBotConfig(
        rest=CoinbaseRestApiConfig(retry_policy=CoinbaseRestRetryPolicy(max_attempts=2, initial_delay_seconds=0)),
        reconciliation=ReconciliationRuntimeConfig(
            order_recovery_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.ORDER_RECOVERY,
                interval=timedelta(seconds=10),
            )
        ),
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        rest_retry_sleep=sleep_calls.append,
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )

    assert assembly.order_lookup_client is not None
    assert assembly.order_lookup_client.get_order("exchange-1").status == ExchangeLookupStatus.FOUND
    assert len(transport.gets) == 2
    assert sleep_calls == [0]
    assert core.ledger.iter_records()[0].event_type == EventType.EXCHANGE_REQUEST_RETRY


def test_assembly_wraps_rest_executor_with_audited_retry_transport(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    sleep_calls: list[float] = []
    transport = FakePostTransport(
        [
            HttpResponse(status_code=503, body={"message": "orders unavailable"}),
            HttpResponse(
                status_code=200,
                body={"success": True, "success_response": {"order_id": "exchange-1"}},
            ),
        ]
    )
    config = CoinbaseBotConfig(
        rest=CoinbaseRestApiConfig(
            execution_mode=ExecutionMode.LIVE,
            retry_policy=CoinbaseRestRetryPolicy(max_attempts=2, initial_delay_seconds=0),
        )
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        rest_retry_sleep=sleep_calls.append,
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )
    result = assembly.rest_executor.execute(
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="50000",
        ).to_command()
    )
    retry = SourceOfTruthProjection.from_ledger(core.ledger).exchange_request_retries[0]

    assert result.status == ExecutionStatus.ACCEPTED
    assert result.exchange_order_id == "exchange-1"
    assert len(transport.posts) == 2
    assert sleep_calls == [0]
    assert retry.method == HttpMethod.POST
    assert retry.payload["has_json_body"] is True


def test_config_rejects_string_enums():
    with pytest.raises(TypeError, match="ExecutionMode"):
        CoinbaseRestApiConfig(execution_mode="live")

    with pytest.raises(TypeError, match="LedgerAnchorStoreProvider"):
        AuditAnchorStoreConfig(provider="local_file", local_anchor_dir=Path("anchors"))

    with pytest.raises(TypeError, match="FeedRuntimeConfig"):
        CoinbaseBotConfig(feed="feed-config")

    with pytest.raises(TypeError, match="RuntimeTask"):
        TaskScheduleConfig(task_id="reconciliation.watchdog", interval=timedelta(seconds=1))

    with pytest.raises(TypeError, match="CoinbaseWebSocketChannel"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-market-primary",
            channels=("level2",),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
            product_ids=("BTC-USD",),
        )

    with pytest.raises(TypeError, match="TriggerRelation"):
        TimeTriggerConfig(
            trigger_id="bad-trigger",
            relation="after",
            target_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    with pytest.raises(TypeError, match="TriggerRuleType"):
        MessageTriggerConfig(
            trigger_id="bad-trigger-type",
            relation=TriggerRelation.ON,
            rule_type="message",
        )

    with pytest.raises(TypeError, match="OrderType"):
        RiskPolicyConfig(allowed_order_types=("limit",))

    with pytest.raises(ValueError, match="allowed_products"):
        RiskPolicyConfig(allowed_products=("BTC-USD", "BTC-USD"))

    with pytest.raises(ValueError, match="max_order_size"):
        RiskPolicyConfig(max_order_size=Decimal("0"))


def test_feed_runtime_config_requires_valid_values():
    with pytest.raises(ValueError, match="min_live_sources"):
        FeedRuntimeConfig(min_live_sources=0)

    with pytest.raises(TypeError, match="ReconnectPolicy"):
        FeedRuntimeConfig(reconnect_policy="not-policy")

    with pytest.raises(ValueError, match="stale_after"):
        FeedRuntimeConfig(stale_after=timedelta(0))


def test_audit_anchor_store_config_requires_provider_specific_fields():
    local_config = AuditAnchorStoreConfig(
        provider=LedgerAnchorStoreProvider.LOCAL_FILE,
        local_anchor_dir=Path("anchors"),
    )
    s3_config = AuditAnchorStoreConfig(
        provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
        s3_bucket="audit-bucket",
        s3_immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
        s3_retention_period=timedelta(days=365),
    )

    assert local_config.local_anchor_dir == Path("anchors")
    assert s3_config.s3_bucket == "audit-bucket"

    with pytest.raises(TypeError, match="local_anchor_dir"):
        AuditAnchorStoreConfig(provider=LedgerAnchorStoreProvider.LOCAL_FILE)

    with pytest.raises(ValueError, match="local_anchor_dir"):
        AuditAnchorStoreConfig(
            provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
            local_anchor_dir=Path("anchors"),
            s3_bucket="audit-bucket",
            s3_immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            s3_retention_period=timedelta(days=365),
        )

    with pytest.raises(ValueError, match="s3_retention_period"):
        AuditAnchorStoreConfig(
            provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
            s3_bucket="audit-bucket",
            s3_immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            s3_retention_period=timedelta(0),
        )


def test_audit_archive_store_config_requires_s3_object_lock_fields():
    config = AuditArchiveStoreConfig(
        provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
        s3_bucket="audit-bucket",
        s3_immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
        s3_retention_period=timedelta(days=2555),
    )

    assert config.s3_key_prefix == "audit-ledger-archives"

    with pytest.raises(ValueError, match="aws_s3_object_lock"):
        AuditArchiveStoreConfig(
            provider=LedgerAnchorStoreProvider.LOCAL_FILE,
            s3_bucket="audit-bucket",
            s3_immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            s3_retention_period=timedelta(days=2555),
        )

    with pytest.raises(TypeError, match="s3_immutability_mode"):
        AuditArchiveStoreConfig(
            provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
            s3_bucket="audit-bucket",
            s3_immutability_mode="compliance",
            s3_retention_period=timedelta(days=2555),
        )


def test_config_rejects_invalid_schedule_binding():
    with pytest.raises(ValueError, match=RuntimeTask.AUDIT_ANCHOR.value):
        CoinbaseBotConfig(
            audit_anchor_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
            )
        )

    with pytest.raises(ValueError, match=RuntimeTask.AUDIT_ARCHIVE.value):
        CoinbaseBotConfig(
            audit_archive_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
            )
        )

    with pytest.raises(ValueError, match=RuntimeTask.WATCHDOG.value):
        ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.FILL_RECONCILIATION,
                interval=timedelta(seconds=5),
            )
        )

    with pytest.raises(ValueError, match=RuntimeTask.FEED_HEALTH.value):
        CoinbaseBotConfig(
            feed_health_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
            )
        )

    with pytest.raises(ValueError, match="trigger_ids"):
        CoinbaseBotConfig(
            trigger_rules=(
                MessageTriggerConfig("duplicate", TriggerRelation.ON),
                TimeTriggerConfig(
                    "duplicate",
                    TriggerRelation.AFTER,
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
            )
        )


def test_assembly_requires_token_provider_for_enabled_rest_tasks(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        reconciliation=ReconciliationRuntimeConfig(
            fill_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.FILL_RECONCILIATION,
                interval=timedelta(seconds=10),
            )
        )
    )

    with pytest.raises(ValueError, match="token_provider"):
        assemble_coinbase_runtime(config=config, core=core)


def test_assembly_runs_configured_product_catalog_refresh_schedule(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        product_catalog=ProductCatalogRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                interval=timedelta(hours=1),
                enabled=True,
            ),
            product_ids=("BTC-USD",),
        ),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
    )
    client = FakeProductCatalogClient([_product_payload("BTC-USD")])

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        product_catalog_client=client,
    )

    asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = core.ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(core.ledger)

    assert assembly.product_catalog_refresh_task is not None
    assert assembly.product_catalog is not None
    assert assembly.product_catalog.get("BTC-USD") is not None
    assert client.calls == [{"get_tradability_status": True, "product_ids": ("BTC-USD",)}]
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[1].payload["task_id"] == RuntimeTask.PRODUCT_CATALOG_REFRESH.value
    assert records[2].payload["product_ids"] == ["BTC-USD"]
    assert records[3].payload["result"]["snapshot_sequence"] == 3
    assert projection.exchange_product_count == 1
    assert projection.exchange_products_by_product_id["BTC-USD"].product_type == ProductType.SPOT


def test_assembly_seeds_product_catalog_from_replayed_snapshot_before_refresh(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    core.emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "product_count": 1,
            "product_ids": ["BTC-USD"],
            "products": [
                ProductMetadata.from_coinbase_payload(
                    {**_product_payload("BTC-USD"), "trading_disabled": True}
                ).to_payload()
            ],
        },
    )
    config = CoinbaseBotConfig(
        product_catalog=ProductCatalogRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                interval=timedelta(hours=1),
                enabled=True,
            ),
            product_ids=("BTC-USD",),
        )
    )

    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        product_catalog_client=FakeProductCatalogClient([_product_payload("BTC-USD")]),
    )

    assert assembly.product_catalog is not None
    product = assembly.product_catalog.get("BTC-USD")
    assert product is not None
    assert product.trading_disabled is True


def test_assembly_requires_token_provider_for_product_catalog_refresh_without_injected_client(
    workspace_tmp_path,
):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        product_catalog=ProductCatalogRuntimeConfig(
            schedule=TaskScheduleConfig(
                task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                interval=timedelta(hours=1),
                enabled=True,
            )
        )
    )

    with pytest.raises(ValueError, match="token_provider"):
        assemble_coinbase_runtime(config=config, core=core)


def test_assembly_requires_anchor_store_for_enabled_audit_anchor_schedule(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        audit_anchor_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.AUDIT_ANCHOR,
            interval=timedelta(hours=24),
            enabled=True,
        ),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
    )

    with pytest.raises(ValueError, match=RuntimeTask.AUDIT_ANCHOR.value):
        assemble_coinbase_runtime(config=config, core=core)


def test_assembly_runs_configured_audit_archive_schedule(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        audit_archive_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.AUDIT_ARCHIVE,
            interval=timedelta(hours=24),
            enabled=True,
        ),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
    )
    assembly = assemble_coinbase_runtime(
        config=config,
        core=core,
        audit_archive_store=InMemoryArchiveStore(),
    )

    asyncio.run(assembly.orchestrator.run(max_cycles=1))
    records = core.ledger.iter_records()

    assert assembly.audit_archive_task is not None
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.AUDIT_LEDGER_ARCHIVED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[1].payload["task_id"] == RuntimeTask.AUDIT_ARCHIVE.value
    assert records[3].payload["result"]["store_type"] == AnchorStoreType.WORM_OBJECT.value


def test_assembly_requires_archive_store_for_enabled_audit_archive_schedule(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(
        audit_archive_schedule=TaskScheduleConfig(
            task_id=RuntimeTask.AUDIT_ARCHIVE,
            interval=timedelta(hours=24),
            enabled=True,
        ),
        reconciliation=ReconciliationRuntimeConfig(
            watchdog_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.WATCHDOG,
                interval=timedelta(seconds=5),
                enabled=False,
            )
        ),
    )

    with pytest.raises(ValueError, match=RuntimeTask.AUDIT_ARCHIVE.value):
        assemble_coinbase_runtime(config=config, core=core)


def test_assembly_requires_token_provider_for_live_execution(workspace_tmp_path):
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))
    config = CoinbaseBotConfig(rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE))

    with pytest.raises(ValueError, match="live REST execution"):
        assemble_coinbase_runtime(config=config, core=core)


def test_user_websocket_requires_user_endpoint_and_jwt_factory(workspace_tmp_path):
    with pytest.raises(ValueError, match="separately"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-user-primary",
            channels=(CoinbaseWebSocketChannel.USER, CoinbaseWebSocketChannel.LEVEL2),
            endpoint=CoinbaseWebSocketEndpoint.USER_ORDER_DATA,
            product_ids=("BTC-USD",),
        )

    with pytest.raises(ValueError, match="USER_ORDER_DATA"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-user-primary",
            channels=(CoinbaseWebSocketChannel.USER,),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
            product_ids=("BTC-USD",),
        )

    config = CoinbaseBotConfig(
        websocket_sources=(
            CoinbaseWebSocketSourceConfig(
                source_id="coinbase-user-primary",
                channels=(CoinbaseWebSocketChannel.USER,),
                endpoint=CoinbaseWebSocketEndpoint.USER_ORDER_DATA,
                product_ids=("BTC-USD",),
            ),
        )
    )
    core = AuditCore(AuditLedger(workspace_tmp_path / "audit.jsonl"))

    with pytest.raises(ValueError, match="jwt_factory"):
        assemble_coinbase_runtime(config=config, core=core)


def test_websocket_config_rejects_missing_product_scope():
    with pytest.raises(ValueError, match="product_ids"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-market-primary",
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
        )


def test_websocket_config_rejects_duplicate_channels_and_products():
    with pytest.raises(ValueError, match="channels"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-market-primary",
            channels=(CoinbaseWebSocketChannel.LEVEL2, CoinbaseWebSocketChannel.LEVEL2),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
            product_ids=("BTC-USD",),
        )

    with pytest.raises(ValueError, match="product_ids"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-market-primary",
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
            product_ids=("BTC-USD", "BTC-USD"),
        )

    with pytest.raises(TypeError, match="product_ids"):
        CoinbaseWebSocketSourceConfig(
            source_id="coinbase-market-primary",
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
            product_ids=("BTC-USD", ""),
        )


class FakeGetTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self._responses = responses
        self.gets: list[dict[str, object]] = []

    def get(self, url: str, *, headers: dict[str, str], query_params: object = None) -> HttpResponse:
        self.gets.append({"headers": headers, "query_params": query_params, "url": url})
        return self._responses.pop(0)

    def post(self, url: str, *, headers: dict[str, str], json_body: dict[str, object]) -> HttpResponse:
        raise AssertionError("unexpected POST")


class FakePostTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self._responses = responses
        self.posts: list[dict[str, object]] = []

    def get(self, url: str, *, headers: dict[str, str], query_params: object = None) -> HttpResponse:
        raise AssertionError("unexpected GET")

    def post(self, url: str, *, headers: dict[str, str], json_body: dict[str, object]) -> HttpResponse:
        self.posts.append({"headers": headers, "json_body": json_body, "url": url})
        return self._responses.pop(0)


class FakeProductCatalogClient:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self._catalog = ProductCatalog.from_coinbase_payloads(payloads)
        self.calls: list[dict[str, object]] = []

    def list_products(
        self,
        *,
        product_ids: tuple[str, ...] = (),
        get_tradability_status: bool = True,
    ) -> ProductCatalog:
        self.calls.append(
            {
                "get_tradability_status": get_tradability_status,
                "product_ids": product_ids,
            }
        )
        return self._catalog


def _product_payload(product_id: str) -> dict[str, object]:
    return {
        "base_increment": "0.0001",
        "base_max_size": "10",
        "base_min_size": "0.0001",
        "cancel_only": False,
        "is_disabled": False,
        "limit_only": False,
        "price_increment": "0.1",
        "product_id": product_id,
        "product_type": ProductType.SPOT.value,
        "product_venue": "CBE",
        "quote_max_size": "1000000",
        "quote_min_size": "10",
        "trading_disabled": False,
        "view_only": False,
    }


def _only_record(core: AuditCore, event_type: EventType):
    records = [record for record in core.ledger.iter_records() if record.event_type == event_type]
    assert len(records) == 1
    return records[0]


class InMemoryArchiveStore:
    def publish(self, artifact, *, clock=None):
        published_at = (clock.now() if clock is not None else datetime(2026, 1, 1, tzinfo=timezone.utc))
        return create_worm_ledger_archive_receipt(
            artifact_digest=artifact.artifact_digest,
            artifact_uri=f"memory://{artifact.artifact_name}",
            clock=FixedClock(published_at),
            immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
            record_count=artifact.record_count,
            retention_until=published_at + timedelta(days=1),
            store_metadata={
                "object_content_verified": True,
                "object_sha256": artifact.artifact_digest,
            },
            through_hash=artifact.through_hash,
            through_sequence=artifact.through_sequence,
            version_id="memory-version-1",
        )


class ScriptedFeedSource:
    def __init__(self, source_id: str, messages: list[FeedMessage]) -> None:
        self._source_id = source_id
        self._messages = messages

    @property
    def source_id(self) -> str:
        return self._source_id

    async def stream(self) -> AsyncIterator[FeedMessage]:
        for message in self._messages:
            yield message
