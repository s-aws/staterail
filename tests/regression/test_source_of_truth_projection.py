from __future__ import annotations

from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    AnchorImmutabilityMode,
    AnchorStoreType,
    DigestAlgorithm,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    FeedStatus,
    FeedStopReason,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    PreflightStep,
    ProductType,
    ProductVenue,
    ReadinessStatus,
    StrategyEvaluationStatus,
    StrategySimulationStatus,
    TriggerRelation,
)
from feeds.router import FeedMessage, RedundantFeedRouter
from orders.lineage import LogicalOrderRecord, OrderPlacementRecord
from projections.state import SourceOfTruthProjection


def test_projection_rebuilds_action_lifecycle_from_ledger(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(EventType.ACTION_REQUESTED, {"action_id": "action-1", "product_id": "BTC-PERP-INTX"})
    core.emit(EventType.ACTION_ACCEPTED, {"action_id": "action-1"})
    core.emit(EventType.ACTION_EXECUTED, {"action_id": "action-1", "exchange_order_id": "order-1"})

    projection = SourceOfTruthProjection.from_ledger(AuditLedger(ledger.path))

    action = projection.actions["action-1"]
    assert action.status == ActionStatus.EXECUTED
    assert action.requested_sequence == 1
    assert action.accepted_sequence == 2
    assert action.executed_sequence == 3
    assert projection.last_sequence == 3
    assert projection.last_record_hash == ledger.verify().last_hash
    assert projection.to_payload()["actions"]["action-1"]["status"] == ActionStatus.EXECUTED.value


def test_projection_rebuilds_feed_and_data_state_from_ledger(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    router = RedundantFeedRouter(core)

    core.emit(EventType.FEED_CONNECTED, {"source_id": "coinbase-primary", "attempt": 1})
    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:level2:1",
            event_type=EventType.DATA_RECEIVED,
            payload={"sequence_num": 1},
        )
    )
    router.ingest(
        FeedMessage(
            source_id="coinbase-secondary",
            message_key="coinbase:level2:1",
            event_type=EventType.DATA_RECEIVED,
            payload={"sequence_num": 1},
        )
    )
    core.emit(
        EventType.FEED_DISCONNECTED,
        {
            "reason": FeedStopReason.STREAM_ENDED.value,
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.FEED_RECONNECT_SCHEDULED,
        {
            "attempt": 2,
            "delay_seconds": 1.5,
            "source_id": "coinbase-primary",
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.accepted_data_count == 1
    assert projection.duplicate_data_count == 1
    assert projection.data_messages["coinbase:level2:1"].accepted
    assert projection.feed_sources["coinbase-primary"].status == FeedStatus.DISCONNECTED
    assert projection.feed_sources["coinbase-primary"].connected_count == 1
    assert projection.feed_sources["coinbase-primary"].disconnected_count == 1
    assert projection.feed_sources["coinbase-primary"].reconnect_scheduled_count == 1
    assert projection.feed_sources["coinbase-primary"].last_reconnect_attempt == 2
    assert projection.feed_sources["coinbase-primary"].last_reconnect_delay_seconds == 1.5
    assert projection.feed_sources["coinbase-primary"].last_reconnect_scheduled_sequence == 7


def test_projection_tracks_accepted_market_tickers_without_duplicate_overwrite(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    router = RedundantFeedRouter(AuditCore(ledger))
    raw_ticker = {
        "channel": "ticker",
        "events": [
            {
                "tickers": [
                    {
                        "best_ask": "50100",
                        "best_ask_quantity": "0.8",
                        "best_bid": "49900",
                        "best_bid_quantity": "1.2",
                        "price": "50000",
                        "product_id": "BTC-USD",
                        "time": "2026-01-01T00:00:00Z",
                    }
                ]
            }
        ],
        "sequence_num": 0,
        "timestamp": "2026-01-01T00:00:00Z",
    }

    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:ticker:0",
            event_type=EventType.DATA_RECEIVED,
            payload={
                "channel": "ticker",
                "raw": raw_ticker,
                "sequence_num": 0,
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
    )
    duplicate_raw = {
        **raw_ticker,
        "events": [{"tickers": [{"price": "99999", "product_id": "BTC-USD"}]}],
    }
    router.ingest(
        FeedMessage(
            source_id="coinbase-secondary",
            message_key="coinbase:ticker:0",
            event_type=EventType.DATA_RECEIVED,
            payload={
                "channel": "ticker",
                "raw": duplicate_raw,
                "sequence_num": 0,
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    ticker = projection.latest_ticker("BTC-USD")

    assert ticker is not None
    assert projection.duplicate_data_count == 1
    assert ticker.last_price == "50000"
    assert ticker.bid_price == "49900"
    assert ticker.bid_size == "1.2"
    assert ticker.ask_price == "50100"
    assert ticker.ask_size == "0.8"
    assert ticker.exchange_sequence == 0
    assert ticker.source_id == "coinbase-primary"
    assert projection.to_payload()["market_tickers"]["BTC-USD"]["last_price"] == "50000"


def test_projection_tracks_order_book_and_trade_market_data(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    router = RedundantFeedRouter(AuditCore(ledger))

    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:l2_data:1",
            event_type=EventType.DATA_RECEIVED,
            payload={
                "channel": "l2_data",
                "raw": {
                    "channel": "l2_data",
                    "events": [
                        {
                            "product_id": "BIT-29MAY26-CDE",
                            "type": "snapshot",
                            "updates": [
                                {"new_quantity": "2", "price_level": "100", "side": "bid"},
                                {"new_quantity": "1", "price_level": "99", "side": "bid"},
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
        )
    )
    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:l2_data:2",
            event_type=EventType.DATA_RECEIVED,
            payload={
                "channel": "l2_data",
                "raw": {
                    "channel": "l2_data",
                    "events": [
                        {
                            "product_id": "BIT-29MAY26-CDE",
                            "type": "update",
                            "updates": [
                                {"new_quantity": "0", "price_level": "100", "side": "bid"},
                                {"new_quantity": "4", "price_level": "102", "side": "ask"},
                            ],
                        }
                    ],
                    "sequence_num": 2,
                    "timestamp": "2026-01-01T00:00:01Z",
                },
                "sequence_num": 2,
                "timestamp": "2026-01-01T00:00:01Z",
            },
        )
    )
    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:market_trades:3",
            event_type=EventType.DATA_RECEIVED,
            payload={
                "channel": "market_trades",
                "raw": {
                    "channel": "market_trades",
                    "events": [
                        {
                            "trades": [
                                {
                                    "price": "101",
                                    "product_id": "BIT-29MAY26-CDE",
                                    "side": "BUY",
                                    "size": "0.5",
                                    "time": "2026-01-01T00:00:02Z",
                                    "trade_id": "trade-1",
                                }
                            ],
                            "type": "update",
                        }
                    ],
                    "sequence_num": 3,
                    "timestamp": "2026-01-01T00:00:02Z",
                },
                "sequence_num": 3,
                "timestamp": "2026-01-01T00:00:02Z",
            },
        )
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    book = projection.order_book("BIT-29MAY26-CDE")
    trades = projection.market_trades_for_product("BIT-29MAY26-CDE")

    assert book is not None
    assert book.best_bid_price == "99"
    assert book.best_bid_size == "1"
    assert book.best_ask_price == "101"
    assert book.best_ask_size == "3"
    assert "100" not in book.bid_levels
    assert book.ask_levels["102"] == "4"
    assert book.update_count == 5
    assert projection.market_trade_count == 1
    assert len(trades) == 1
    assert trades[0].trade_id == "trade-1"
    assert trades[0].side == OrderSide.BUY
    assert trades[0].price == "101"
    assert projection.to_payload()["market_order_books"]["BIT-29MAY26-CDE"]["best_bid_price"] == "99"
    assert projection.to_payload()["market_trades"]["trade-1"]["side"] == OrderSide.BUY.value


def test_projection_replaces_order_book_levels_on_new_snapshot(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    router = RedundantFeedRouter(AuditCore(ledger))

    for sequence, bid, ask in (
        (1, "100", "101"),
        (2, "105", "106"),
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
                                "product_id": "BIT-29MAY26-CDE",
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
            )
        )

    book = SourceOfTruthProjection.from_ledger(ledger).order_book("BIT-29MAY26-CDE")

    assert book is not None
    assert book.bid_levels == {"105": "2"}
    assert book.ask_levels == {"106": "3"}
    assert book.best_bid_price == "105"
    assert book.best_ask_price == "106"


def test_projection_tracks_errors_triggers_and_sequence_anomalies(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_SEQUENCE_GAP.value,
            "message_key": "coinbase:sequence-gap:ticker:10:13",
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.TRIGGER_FIRED,
        {
            "matched_event_type": EventType.DATA_SEQUENCE_GAP.value,
            "matched_sequence": 1,
            "relation": TriggerRelation.ON.value,
            "trigger_id": "gap-alert",
        },
    )
    core.emit(
        EventType.FEED_DEGRADED,
        {
            "connected_sources": [],
            "disconnected_sources": ["coinbase-primary"],
            "live_count": 0,
            "min_live_sources": 1,
            "stale_sources": ["coinbase-secondary"],
        },
    )
    core.emit(
        EventType.ERROR,
        {
            "error_category": ErrorCategory.RUNTIME_TASK.value,
            "error_code": ErrorCode.RUNTIME_TASK_FAILED.value,
            "exception_type": "RuntimeError",
            "message": "operator intervention required",
            "retryable": False,
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.sequence_gap_sequences == [1]
    assert projection.trigger_sequences == [2]
    assert projection.trigger_firings[0].trigger_id == "gap-alert"
    assert projection.trigger_firings[0].relation == TriggerRelation.ON
    assert projection.trigger_firings[0].matched_event_type == EventType.DATA_SEQUENCE_GAP
    assert projection.trigger_firings[0].matched_sequence == 1
    assert projection.feed_degraded_sequences == [3]
    assert projection.feed_degraded_count == 1
    assert projection.feed_degradations[0].disconnected_sources == ("coinbase-primary",)
    assert projection.feed_degradations[0].stale_sources == ("coinbase-secondary",)
    assert projection.feed_degradations[0].live_count == 0
    assert projection.feed_degradations[0].min_live_sources == 1
    assert projection.error_sequences == [4]
    assert projection.error_count == 1
    assert projection.errors[0].category == ErrorCategory.RUNTIME_TASK
    assert projection.errors[0].code == ErrorCode.RUNTIME_TASK_FAILED
    assert projection.errors[0].exception_type == "RuntimeError"
    assert projection.errors[0].message == "operator intervention required"
    assert projection.errors[0].retryable is False
    assert projection.to_payload()["errors"][0]["category"] == ErrorCategory.RUNTIME_TASK.value


def test_projection_counts_unique_sequence_anomalies_once(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    message_key = "coinbase:sequence-gap:l2_data:BIT-29MAY26-CDE:1:3"

    received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_SEQUENCE_GAP.value,
            "message_key": message_key,
            "source_id": "coinbase-cfm-market-primary",
        },
    )
    core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_SEQUENCE_GAP.value,
            "message_key": message_key,
            "received_sequence": received.sequence,
            "source_id": "coinbase-cfm-market-primary",
        },
    )
    duplicate_received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_SEQUENCE_GAP.value,
            "message_key": message_key,
            "source_id": "coinbase-cfm-market-secondary",
        },
    )
    core.emit(
        EventType.DATA_DUPLICATE,
        {
            "message_key": message_key,
            "received_sequence": duplicate_received.sequence,
            "source_id": "coinbase-cfm-market-secondary",
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.sequence_gap_sequences == [received.sequence]


def test_projection_tracks_audit_anchor_receipts(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.AUDIT_CHECKPOINT,
        {
            "checkpoint_hash": "checkpoint-hash",
            "created_at": "2026-01-01T00:00:00+00:00",
            "digest_algorithm": DigestAlgorithm.SHA256.value,
            "ledger_path": ledger.path.as_posix(),
            "record_count": 0,
            "records_digest": "records-digest",
            "through_hash": "through-hash",
            "through_sequence": 0,
        },
    )
    core.emit(
        EventType.AUDIT_ANCHOR_PUBLISHED,
        {
            "artifact_uri": "anchors/checkpoint.json",
            "checkpoint_hash": "abc123",
            "checkpoint_through_sequence": 10,
            "immutability_mode": AnchorImmutabilityMode.COMPLIANCE.value,
            "retention_until": "2033-01-01T00:00:00+00:00",
            "store_metadata": {"provider": "regression"},
            "store_type": AnchorStoreType.WORM_OBJECT.value,
            "version_id": "version-1",
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.audit_checkpoint_count == 1
    assert projection.audit_checkpoints[0].checkpoint_hash == "checkpoint-hash"
    assert projection.audit_checkpoints[0].digest_algorithm == DigestAlgorithm.SHA256
    assert projection.audit_checkpoints[0].record_count == 0
    assert projection.audit_checkpoints[0].through_sequence == 0
    assert projection.audit_anchor_count == 1
    assert projection.audit_anchors[0].artifact_uri == "anchors/checkpoint.json"
    assert projection.audit_anchors[0].checkpoint_hash == "abc123"
    assert projection.audit_anchors[0].checkpoint_through_sequence == 10
    assert projection.audit_anchors[0].immutability_mode == AnchorImmutabilityMode.COMPLIANCE
    assert projection.audit_anchors[0].retention_until == "2033-01-01T00:00:00+00:00"
    assert projection.audit_anchors[0].store_metadata == {"provider": "regression"}
    assert projection.audit_anchors[0].store_type == AnchorStoreType.WORM_OBJECT
    assert projection.audit_anchors[0].version_id == "version-1"


def test_projection_tracks_exchange_product_snapshots(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "product_count": 1,
            "product_ids": ["BTC-USD"],
            "products": [
                {
                    "cancel_only": False,
                    "is_disabled": False,
                    "limit_only": False,
                    "post_only": False,
                    "product_id": "BTC-USD",
                    "product_type": ProductType.SPOT.value,
                    "product_venue": ProductVenue.CBE.value,
                    "trading_disabled": False,
                    "tradable_for_new_orders": True,
                    "view_only": False,
                }
            ],
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert projection.exchange_product_snapshot_count == 1
    assert projection.exchange_product_snapshot_sequences == [1]
    assert projection.exchange_product_count == 1
    product = projection.exchange_products_by_product_id["BTC-USD"]
    assert product.product_type == ProductType.SPOT
    assert product.product_venue == ProductVenue.CBE
    assert product.tradable_for_new_orders is True
    assert projection.to_payload()["exchange_products"]["BTC-USD"]["product_type"] == ProductType.SPOT.value


def test_projection_tracks_live_preflight_results(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.LIVE_PREFLIGHT_RESULT,
        {
            "completed_step_names": [
                PreflightStep.READINESS.value,
                PreflightStep.PRODUCT_CATALOG_SMOKE.value,
                PreflightStep.FEED_SMOKE.value,
                PreflightStep.EXCHANGE_STATE_SMOKE.value,
            ],
            "config_fingerprint": "fingerprint-1",
            "fingerprint_algorithm": "sha256",
            "order_endpoint_called": False,
            "runtime_tasks_started": False,
            "schema_version": 1,
            "skipped_step_names": [],
            "status": ReadinessStatus.OK.value,
            "strategy_tasks_started": False,
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert len(projection.live_preflight_results) == 1
    assert projection.live_preflight_results[0].status == ReadinessStatus.OK
    assert projection.live_preflight_results[0].completed_step_names == (
        PreflightStep.READINESS.value,
        PreflightStep.PRODUCT_CATALOG_SMOKE.value,
        PreflightStep.FEED_SMOKE.value,
        PreflightStep.EXCHANGE_STATE_SMOKE.value,
    )
    assert projection.live_preflight_results[0].config_fingerprint == "fingerprint-1"
    assert projection.to_payload()["live_preflight_results"][0]["status"] == ReadinessStatus.OK.value


def test_projection_tracks_runtime_health_check_results(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.RUNTIME_HEALTH_CHECK_RESULT,
        {
            "attention_check_count": 1,
            "attention_checks": [LedgerHealthCheckName.ERROR_EVENTS.value],
            "checked_health_status": LedgerHealthStatus.ATTENTION_REQUIRED.value,
            "checked_through_sequence": 8,
            "ledger_path": ledger.path.as_posix(),
            "record_count": 8,
            "schema_version": 1,
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert len(projection.runtime_health_check_results) == 1
    result = projection.runtime_health_check_results[0]
    assert result.checked_health_status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert result.checked_through_sequence == 8
    assert result.attention_check_count == 1
    assert result.attention_checks == (LedgerHealthCheckName.ERROR_EVENTS.value,)
    assert projection.to_payload()["runtime_health_check_results"][0][
        "checked_health_status"
    ] == LedgerHealthStatus.ATTENTION_REQUIRED.value


def test_projection_tracks_ledger_health_acknowledgements(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.OPERATOR_LEDGER_HEALTH_ACKNOWLEDGED,
        {
            "acknowledged_by": "operator-1",
            "acknowledged_health_status": LedgerHealthStatus.ATTENTION_REQUIRED.value,
            "acknowledged_through_hash": "abc123",
            "acknowledged_through_sequence": 7,
            "attention_check_count": 1,
            "ledger_health_attention_digest": "digest-1",
            "reason": "reviewed",
            "schema_version": 1,
        },
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert len(projection.ledger_health_acknowledgements) == 1
    acknowledgement = projection.ledger_health_acknowledgements[0]
    assert acknowledgement.acknowledged_by == "operator-1"
    assert acknowledgement.acknowledged_health_status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert acknowledgement.acknowledged_through_sequence == 7
    assert acknowledgement.ledger_health_attention_digest == "digest-1"
    assert projection.to_payload()["ledger_health_acknowledgements"][0][
        "acknowledged_health_status"
    ] == LedgerHealthStatus.ATTENTION_REQUIRED.value


def test_projection_tracks_strategy_simulation_results(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.STRATEGY_SIMULATION_RESULT,
        {
            "accepted_action_count": 1,
            "as_of_sequence": 3,
            "completed_count": 1,
            "config_fingerprint": "fingerprint-1",
            "evaluated_at": "2026-01-01T00:00:00+00:00",
            "evaluation_statuses": [
                {
                    "accepted_action_count": 1,
                    "intent_count": 1,
                    "rejected_action_count": 0,
                    "status": StrategyEvaluationStatus.COMPLETED.value,
                    "strategy_id": "example",
                }
            ],
            "execution_mode": ExecutionMode.LIVE.value,
            "failed_count": 0,
            "fingerprint_algorithm": "sha256",
            "intent_count": 1,
            "ledger_path": ledger.path.as_posix(),
            "order_endpoint_called": False,
            "read_only": True,
            "rejected_action_count": 0,
            "runtime_tasks_started": False,
            "schema_version": 1,
            "simulated_ledger": {
                "last_hash": None,
                "ledger_path": ledger.path.as_posix(),
                "record_count": 0,
                "verified": True,
            },
            "status": StrategySimulationStatus.OK.value,
            "strategy_count": 1,
            "strategy_ids": ["example"],
            "strategy_tasks_started": False,
        },
    )

    projection = SourceOfTruthProjection.from_ledger(AuditLedger(ledger.path))

    assert len(projection.strategy_simulation_results) == 1
    assert projection.strategy_simulation_results[0].status == StrategySimulationStatus.OK
    assert projection.strategy_simulation_results[0].accepted_action_count == 1
    assert projection.strategy_simulation_results[0].strategy_ids == ("example",)
    assert projection.to_payload()["strategy_simulation_results"][0]["status"] == (
        StrategySimulationStatus.OK.value
    )


def test_projection_tracks_logical_order_lineage_and_placements(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)

    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="logical-parent",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
            created_by_action_id="create-parent-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            placement_id="placement-parent-1",
            logical_order_id="logical-parent",
            placement_kind=OrderPlacementKind.INITIAL,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
            action_id="place-parent-1",
            venue_client_order_id="client-parent-1",
            exchange_order_id="exchange-parent-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="logical-child",
            root_order_id="logical-parent",
            parent_order_id="logical-parent",
            lineage_relation=OrderLineageRelation.FOLLOWUP_AFTER_FILL,
            product_id="BTC-USD",
            side=OrderSide.SELL,
            size="1",
            limit_price="110",
            source_order_ids=("logical-parent",),
            created_by_action_id="create-child-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            placement_id="stage-child-1",
            logical_order_id="logical-child",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.SELL,
            size="1",
            limit_price="110",
            action_id="stage-child-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            placement_id="release-child-1",
            logical_order_id="logical-child",
            placement_kind=OrderPlacementKind.RELEASE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.SELL,
            size="1",
            limit_price="110",
            action_id="release-child-1",
            exchange_order_id="exchange-release-child-1",
            metadata={
                "staged_release": {
                    "release_of_action_id": "stage-child-1",
                    "release_of_placement_id": "stage-child-1",
                },
            },
            venue_client_order_id="client-release-child-1",
        ).to_payload(),
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    parent = projection.logical_orders_by_id["logical-parent"]
    child = projection.logical_orders_by_id["logical-child"]
    placement = projection.placements_by_id["placement-parent-1"]
    staged_placement = projection.placements_by_id["stage-child-1"]
    release_placement = projection.placements_by_id["release-child-1"]

    assert parent.lineage_relation == OrderLineageRelation.ROOT
    assert parent.child_order_ids == ["logical-child"]
    assert parent.placement_ids == ["placement-parent-1"]
    assert child.root_order_id == "logical-parent"
    assert child.parent_order_id == "logical-parent"
    assert child.source_order_ids == ("logical-parent",)
    assert placement.placement_status == OrderPlacementStatus.ACCEPTED
    assert placement.placement_kind == OrderPlacementKind.INITIAL
    assert projection.logical_order_id_by_action_id["create-parent-1"] == "logical-parent"
    assert projection.logical_order_id_by_action_id["create-child-1"] == "logical-child"
    assert projection.placement_ids_by_action_id["place-parent-1"] == ["placement-parent-1"]
    assert projection.placement_ids_by_action_id["stage-child-1"] == ["stage-child-1"]
    assert projection.staged_order_placements == (staged_placement,)
    assert projection.unreleased_staged_order_placements == ()
    assert projection.released_staged_placement_ids == ("stage-child-1",)
    assert release_placement.release_of_action_id == "stage-child-1"
    assert release_placement.release_of_placement_id == "stage-child-1"
    assert projection.release_placement_for_staged_placement("stage-child-1") == release_placement
    assert projection.release_placement_for_staged_placement("missing-stage") is None
    assert projection.placements_for_action("stage-child-1") == (staged_placement,)
    assert projection.placements_for_logical_order("logical-child") == (
        staged_placement,
        release_placement,
    )
    assert projection.latest_placement_for_logical_order("logical-child") == release_placement
    assert projection.to_payload()["logical_orders"]["logical-child"]["lineage_relation"] == (
        OrderLineageRelation.FOLLOWUP_AFTER_FILL.value
    )
    assert projection.to_payload()["placement_ids_by_action_id"]["place-parent-1"] == ["placement-parent-1"]
    assert projection.to_payload()["release_placement_id_by_staged_placement_id"] == {
        "stage-child-1": "release-child-1"
    }
    assert projection.to_payload()["unreleased_staged_placement_ids"] == []


def test_projection_exposes_passive_market_making_quotes_from_staged_placements(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    passive_metadata = {
        "passive_market_making": {
            "ask_price": "101",
            "bid_price": "99",
            "half_spread_bps": "50",
            "midpoint": "100",
            "product_id": "BTC-USD",
            "side": OrderSide.BUY.value,
        },
        "staged_release": {
            "chunk_count": 1,
            "chunk_index": 1,
            "size": "0.05",
        },
    }

    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="passive-quote-logical",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="0.05",
            limit_price="99",
            created_by_action_id="passive-stage-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="passive-stage-1",
            limit_price="99",
            logical_order_id="passive-quote-logical",
            metadata=passive_metadata,
            placement_id="passive-stage-1",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="0.05",
        ).to_payload(),
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    quote = projection.passive_market_making_quotes[0]
    payload = projection.to_payload()

    assert projection.passive_market_making_quotes == (quote,)
    assert projection.unreleased_passive_market_making_quotes == (quote,)
    assert quote.action_id == "passive-stage-1"
    assert quote.ask_price == "101"
    assert quote.bid_price == "99"
    assert quote.half_spread_bps == "50"
    assert quote.limit_price == "99"
    assert quote.midpoint == "100"
    assert quote.placement_id == "passive-stage-1"
    assert quote.product_id == "BTC-USD"
    assert quote.released is False
    assert quote.release_placement_id is None
    assert quote.side == OrderSide.BUY
    assert payload["passive_market_making_quotes"]["passive-stage-1"]["released"] is False
    assert payload["passive_market_making_quotes"]["passive-stage-1"]["side"] == OrderSide.BUY.value
    assert payload["unreleased_passive_market_making_quote_ids"] == ["passive-stage-1"]

    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="passive-release-1",
            exchange_order_id="exchange-passive-release-1",
            limit_price="99",
            logical_order_id="passive-quote-logical",
            metadata={
                "staged_release": {
                    "release_of_action_id": "passive-stage-1",
                    "release_of_placement_id": "passive-stage-1",
                },
            },
            placement_id="passive-release-1",
            placement_kind=OrderPlacementKind.RELEASE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="0.05",
            venue_client_order_id="client-passive-release-1",
        ).to_payload(),
    )

    released_projection = SourceOfTruthProjection.from_ledger(ledger)
    released_quote = released_projection.passive_market_making_quotes[0]
    released_payload = released_projection.to_payload()

    assert released_quote.released is True
    assert released_quote.release_placement_id == "passive-release-1"
    assert released_projection.unreleased_passive_market_making_quotes == ()
    assert released_payload["passive_market_making_quotes"]["passive-stage-1"]["released"] is True
    assert released_payload["unreleased_passive_market_making_quote_ids"] == []
