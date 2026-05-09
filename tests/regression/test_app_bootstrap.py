from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from app.bootstrap import (
    CoinbaseApplicationConfig,
    build_coinbase_application,
    default_coinbase_application_config,
)
from app.config_fingerprint import (
    APPLICATION_CONFIG_SCHEMA_VERSION,
    CONFIG_FINGERPRINT_ALGORITHM,
    application_config_startup_metadata,
    application_config_fingerprint,
    application_config_snapshot,
)
from app.credentials import (
    COINBASE_API_KEY_NAME_ENV,
    COINBASE_API_PRIVATE_KEY_ENV,
    COINBASE_SDK_API_KEY_ENV,
    COINBASE_SDK_API_SECRET_ENV,
)
from app.ledger_health import ledger_health
from app.ledger_summary import summarize_ledger
from app.live_safety import LIVE_TRADING_APPROVAL_ENV
from app.main import (
    ATTENTION_REQUIRED_EXIT_CODE,
    _runtime_max_cycles_from_args,
    _runtime_stop_after_task_count_from_args,
    _runtime_stop_after_task_from_args,
    _validate_runtime_stop_after_task,
    run_from_args,
)
from actions.dry_run import DryRunExecutor
from actions.gateway import ActionGateway, PlaceOrderIntent
from audit.anchors import LedgerAnchorError, LocalFileLedgerAnchorStore, publish_recorded_ledger_checkpoint_anchor
from audit.archives import publish_ledger_archive
from audit.checkpoints import record_ledger_checkpoint
from audit.s3_object_lock import (
    S3ObjectLockAnchorConfig,
    S3ObjectLockLedgerArchiveConfig,
    S3ObjectLockLedgerArchiveStore,
    S3ObjectLockLedgerAnchorStore,
)
from core.engine import AuditCore
from audit.ledger import AuditLedger
from config.assembly import (
    AuditAnchorStoreConfig,
    AuditArchiveStoreConfig,
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
    CoinbaseWebSocketSourceConfig,
    MessageTriggerConfig,
    ProductCatalogRuntimeConfig,
    ReconciliationRuntimeConfig,
    TaskScheduleConfig,
)
from core.enums import (
    ActionFailureReason,
    ActionStatus,
    ActionType,
    AnchorImmutabilityMode,
    AnchorStoreType,
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    DigestAlgorithm,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExecutionMode,
    ExecutionStatus,
    ExchangeLookupStatus,
    ExchangeOrderStatus,
    FeedStopReason,
    LedgerHealthCheckName,
    LedgerHealthStatus,
    LedgerAnchorStoreProvider,
    OrderSide,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderType,
    ProductType,
    ProductVenue,
    ReadinessCheckName,
    ReadinessStatus,
    ReconciliationIssue,
    RuntimeComponent,
    RuntimeStopReason,
    RuntimeTask,
    TriggerRelation,
)
from feeds import FeedMessage, RedundantFeedRouter
from orders.lineage import (
    LogicalOrderRecord,
    ManualAssociationApproval,
    OrderPlacementRecord,
    manual_association_metadata,
)
from projections.state import SourceOfTruthProjection
from tools.config_template import render_config_template


class CliFakeS3ObjectLockClient:
    def __init__(self) -> None:
        self.get_bucket_versioning_calls: list[dict[str, Any]] = []
        self.get_object_calls: list[dict[str, Any]] = []
        self.get_object_lock_configuration_calls: list[dict[str, Any]] = []
        self.put_object_calls: list[dict[str, Any]] = []
        self.get_object_retention_calls: list[dict[str, Any]] = []
        self._uploaded_body: bytes | None = None

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        self.get_bucket_versioning_calls.append(kwargs)
        return {"Status": "Enabled"}

    def get_object_lock_configuration(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_lock_configuration_calls.append(kwargs)
        return {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_object_calls.append(kwargs)
        self._uploaded_body = kwargs["Body"]
        return {"ETag": '"cli-etag"', "VersionId": "cli-version-1"}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_calls.append(kwargs)
        return {"Body": BytesIO(self._uploaded_body or b"")}

    def get_object_retention(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_retention_calls.append(kwargs)
        mode = self.put_object_calls[-1]["ObjectLockMode"]
        return {
            "Retention": {
                "Mode": mode,
                "RetainUntilDate": datetime(2035, 1, 1, tzinfo=timezone.utc),
            }
        }


def test_default_application_builds_dry_run_runtime_and_replays_cycle(workspace_tmp_path):
    config = default_coinbase_application_config(ledger_path=workspace_tmp_path / "audit.jsonl")
    application = build_coinbase_application(config)

    result = asyncio.run(application.run(max_cycles=1))
    projection = SourceOfTruthProjection.from_ledger(application.ledger)
    records = application.ledger.iter_records()

    assert result.completed_cycles == 1
    assert result.ledger_path == workspace_tmp_path / "audit.jsonl"
    assert isinstance(application.assembly.rest_executor, DryRunExecutor)
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert projection.runtime_tasks[RuntimeTask.WATCHDOG].completed_count == 1


def test_application_stop_requests_graceful_runtime_stop(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "stop-audit.jsonl"
    application_holder: dict[str, object] = {}

    async def stop_during_sleep(delay_seconds: float) -> None:
        del delay_seconds
        application = application_holder["application"]
        application.stop()

    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=True,
                    run_on_start=False,
                )
            )
        ),
    )
    application = build_coinbase_application(config, sleep=stop_during_sleep)
    application_holder["application"] = application

    result = asyncio.run(application.run(max_cycles=None))
    records = AuditLedger(ledger_path).iter_records()

    assert result.completed_cycles == 0
    assert records[-1].event_type == EventType.SYSTEM_STOPPED
    assert records[-1].payload["reason"] == RuntimeStopReason.STOP_REQUESTED.value


def test_application_exposes_single_audited_action_gateway(workspace_tmp_path):
    application = build_coinbase_application(
        default_coinbase_application_config(ledger_path=workspace_tmp_path / "actions.jsonl")
    )

    receipt = application.submit_and_execute_action(
        PlaceOrderIntent(
            action_id="action-1",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            size="0.01",
            limit_price="50000",
        ).to_command()
    )
    records = application.ledger.iter_records()

    assert isinstance(application.assembly.action_gateway, ActionGateway)
    assert receipt.status == ActionStatus.EXECUTED
    assert [record.event_type for record in records] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_ACCEPTED,
        EventType.ORDER_LOGICAL_CREATED,
        EventType.ACTION_EXECUTION_STARTED,
        EventType.ACTION_EXECUTED,
        EventType.ORDER_PLACEMENT_RECORDED,
    ]
    executed_record = next(record for record in records if record.event_type == EventType.ACTION_EXECUTED)
    assert executed_record.payload["execution_result"]["status"] == ExecutionStatus.ACCEPTED.value


def test_application_builds_configured_local_audit_anchor_store(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            audit_anchor_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.AUDIT_ANCHOR,
                interval=timedelta(seconds=1),
                enabled=True,
                run_on_start=True,
            ),
            audit_anchor_store=AuditAnchorStoreConfig(
                provider=LedgerAnchorStoreProvider.LOCAL_FILE,
                local_anchor_dir=workspace_tmp_path / "anchors",
            ),
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=1),
                    enabled=False,
                )
            ),
        ),
    )

    application = build_coinbase_application(config)
    result = asyncio.run(application.run(max_cycles=1))
    records = AuditLedger(ledger_path).iter_records()
    summary = summarize_ledger(ledger_path)

    assert result.completed_cycles == 1
    assert application.assembly.audit_anchor_task is not None
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.AUDIT_CHECKPOINT,
        EventType.AUDIT_ANCHOR_PUBLISHED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert records[1].payload["task_id"] == RuntimeTask.AUDIT_ANCHOR.value
    assert Path(records[3].payload["artifact_uri"]).exists()
    assert summary.audit_checkpoint_count == 1
    assert summary.audit_anchor_count == 1


def test_application_builds_configured_s3_audit_archive_store(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "archive-config-audit.jsonl"
    client = CliFakeS3ObjectLockClient()
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            audit_archive_schedule=TaskScheduleConfig(
                task_id=RuntimeTask.AUDIT_ARCHIVE,
                interval=timedelta(hours=24),
                enabled=True,
            ),
            audit_archive_store=AuditArchiveStoreConfig(
                provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
                s3_bucket="audit-bucket",
                s3_expected_bucket_owner="123456789012",
                s3_immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
                s3_key_prefix="staterail/ledger-archives",
                s3_retention_period=timedelta(days=2555),
            ),
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=False,
                )
            ),
        ),
    )
    application = build_coinbase_application(
        config,
        s3_archive_store_factory=lambda archive_config: S3ObjectLockLedgerArchiveStore(
            archive_config,
            s3_client=client,
        ),
    )

    result = asyncio.run(application.run(max_cycles=1))
    records = application.ledger.iter_records()
    summary = summarize_ledger(ledger_path)

    assert result.completed_cycles == 1
    assert application.assembly.audit_archive_task is not None
    assert [record.event_type for record in records] == [
        EventType.SYSTEM_STARTED,
        EventType.RUNTIME_TASK_STARTED,
        EventType.AUDIT_LEDGER_ARCHIVED,
        EventType.RUNTIME_TASK_COMPLETED,
        EventType.SYSTEM_STOPPED,
    ]
    assert client.put_object_calls[0]["Bucket"] == "audit-bucket"
    assert client.put_object_calls[0]["ExpectedBucketOwner"] == "123456789012"
    assert client.put_object_calls[0]["Key"].startswith("staterail/ledger-archives/")
    assert records[3].payload["result"]["through_sequence"] == 2
    assert summary.audit_archive_count == 1


def test_ledger_summary_verifies_and_replays_source_of_truth(workspace_tmp_path):
    config = default_coinbase_application_config(ledger_path=workspace_tmp_path / "audit.jsonl")
    application = build_coinbase_application(
        config,
        websocket_source_factory=lambda source_config: IdleFeedSource(source_config.source_id),
    )

    asyncio.run(application.run(max_cycles=1))
    summary = summarize_ledger(config.ledger_path)
    payload = summary.to_payload()

    assert summary.verified is True
    assert summary.record_count == 4
    assert summary.last_sequence == 4
    assert summary.next_sequence == 5
    assert summary.runtime_task_count == 1
    assert summary.system_start_count == 1
    assert summary.system_stop_count == 1
    assert summary.error_count == 0
    assert summary.execution_unknown_order_count == 0
    assert summary.failed_action_count == 0
    assert summary.market_order_book_count == 0
    assert summary.market_ticker_count == 0
    assert summary.market_trade_count == 0
    assert summary.passive_market_making_quote_count == 0
    assert summary.passive_market_making_released_quote_count == 0
    assert summary.passive_market_making_unreleased_quote_count == 0
    assert summary.runtime_health_check_result_count == 0
    assert summary.latest_runtime_health_check_sequence is None
    assert summary.latest_runtime_health_check_status is None
    assert summary.latest_config_fingerprint == application_config_fingerprint(config)
    assert payload["ledger_path"] == config.ledger_path.as_posix()
    assert payload["latest_config_fingerprint_algorithm"] == CONFIG_FINGERPRINT_ALGORITHM
    assert payload["passive_market_making_quote_count"] == 0


def test_ledger_summary_reports_market_data_projection_counts(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    router = RedundantFeedRouter(AuditCore(ledger))

    router.ingest(
        FeedMessage(
            source_id="coinbase-primary",
            message_key="coinbase:ticker:1",
            event_type=EventType.DATA_RECEIVED,
            payload={
                "channel": "ticker",
                "raw": {
                    "channel": "ticker",
                    "events": [{"tickers": [{"price": "50000", "product_id": "BTC-USD"}]}],
                    "sequence_num": 1,
                },
                "sequence_num": 1,
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
                            "product_id": "BTC-USD",
                            "updates": [{"new_quantity": "1", "price_level": "49900", "side": "bid"}],
                        }
                    ],
                    "sequence_num": 2,
                },
                "sequence_num": 2,
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
                                    "price": "50000",
                                    "product_id": "BTC-USD",
                                    "size": "0.1",
                                    "trade_id": "trade-1",
                                }
                            ]
                        }
                    ],
                    "sequence_num": 3,
                },
                "sequence_num": 3,
            },
        )
    )

    summary = summarize_ledger(ledger.path)
    payload = summary.to_payload()

    assert summary.market_order_book_count == 1
    assert summary.market_ticker_count == 1
    assert summary.market_trade_count == 1
    assert payload["market_order_book_count"] == 1
    assert payload["market_ticker_count"] == 1
    assert payload["market_trade_count"] == 1


def test_ledger_health_reports_clean_verified_ledger(workspace_tmp_path):
    config = default_coinbase_application_config(ledger_path=workspace_tmp_path / "audit.jsonl")
    application = build_coinbase_application(config)

    asyncio.run(application.run(max_cycles=1))
    health = ledger_health(config.ledger_path)
    payload = health.to_payload()
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.OK
    assert health.verified is True
    assert health.record_count == 4
    assert checks[LedgerHealthCheckName.AUDIT_INTEGRITY].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.AUDIT_INTEGRITY].count == 4
    assert checks[LedgerHealthCheckName.STARTUP_CONFIG_CONTRACT].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.STARTUP_CONFIG_CONTRACT].details["system_start_count"] == 1
    assert checks[LedgerHealthCheckName.SYSTEM_LIFECYCLE_CONTRACT].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].details["unanchored_checkpoint_count"] == 0
    assert checks[LedgerHealthCheckName.EXECUTION_UNCERTAINTY].status == LedgerHealthStatus.OK
    assert payload["status"] == LedgerHealthStatus.OK.value


def test_ledger_health_reports_startup_config_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = default_coinbase_application_config(ledger_path=ledger_path)
    tampered_metadata = application_config_startup_metadata(config)
    application_config = tampered_metadata["application_config"]
    if not isinstance(application_config, dict):
        raise TypeError("application config metadata must be a dict")
    application_config["fingerprint"] = "0" * 64
    core = AuditCore(AuditLedger(ledger_path))
    tampered = core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "startup_metadata": tampered_metadata,
            "task_count": 1,
        },
    )
    missing = core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "task_count": 1,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    startup_check = checks[LedgerHealthCheckName.STARTUP_CONFIG_CONTRACT]
    anomalies = startup_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert startup_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert startup_check.count == 2
    assert startup_check.details["anomaly_count"] == 2
    assert startup_check.details["system_start_count"] == 2
    assert anomalies[0]["sequence"] == tampered.sequence
    assert anomalies[0]["application_config_present"] is True
    assert anomalies[0]["fingerprint"] == "0" * 64
    assert anomalies[0]["calculated_fingerprint"] == application_config_fingerprint(config)
    assert anomalies[0]["fingerprint_matches"] is False
    assert anomalies[0]["fingerprint_algorithm_valid"] is True
    assert anomalies[0]["snapshot_schema_version_matches"] is True
    assert anomalies[1]["sequence"] == missing.sequence
    assert anomalies[1]["application_config_present"] is False
    assert anomalies[1]["snapshot_present"] is False
    assert anomalies[1]["fingerprint_present"] is False


def test_ledger_health_reports_system_lifecycle_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    invalid_start = core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": "not-a-component",
            "started_at": None,
            "task_count": 0,
        },
    )
    orphan_stop = core.emit(
        EventType.SYSTEM_STOPPED,
        {
            "completed_cycles": -1,
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "reason": "not-a-stop-reason",
        },
    )
    core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": "2026-01-01T00:00:00+00:00",
            "task_count": 1,
        },
    )
    overlapping_start = core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": "2026-01-01T00:00:01+00:00",
            "task_count": 1,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lifecycle_check = checks[LedgerHealthCheckName.SYSTEM_LIFECYCLE_CONTRACT]
    anomalies = lifecycle_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lifecycle_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lifecycle_check.count == 3
    assert lifecycle_check.details["anomaly_count"] == 3
    assert anomalies[0]["sequence"] == invalid_start.sequence
    assert anomalies[0]["component_valid"] is False
    assert anomalies[0]["started_at_present"] is False
    assert anomalies[0]["task_count_valid"] is False
    assert anomalies[1]["sequence"] == orphan_stop.sequence
    assert anomalies[1]["active_start_found"] is False
    assert anomalies[1]["completed_cycles_valid"] is False
    assert anomalies[1]["reason_valid"] is False
    assert anomalies[2]["sequence"] == overlapping_start.sequence
    assert anomalies[2]["already_started"] is True


def test_ledger_health_accepts_valid_feed_lifecycle_contract(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    core.emit(
        EventType.FEED_CONNECTED,
        {
            "attempt": 1,
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.FEED_HEARTBEAT,
        {
            "message_event_type": EventType.FEED_HEARTBEAT.value,
            "message_key": "coinbase:heartbeat:1",
            "payload": {"sequence": 1},
            "received_at": "2026-01-01T00:00:00+00:00",
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.FEED_DISCONNECTED,
        {
            "attempt": 1,
            "reason": FeedStopReason.STREAM_ENDED.value,
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.FEED_RECONNECT_SCHEDULED,
        {
            "attempt": 2,
            "delay_seconds": 0.5,
            "source_id": "coinbase-primary",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.FEED_LIFECYCLE_CONTRACT].status == LedgerHealthStatus.OK


def test_ledger_health_reports_feed_lifecycle_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    invalid_connected = core.emit(
        EventType.FEED_CONNECTED,
        {
            "attempt": 0,
            "source_id": "",
        },
    )
    orphan_disconnect = core.emit(
        EventType.FEED_DISCONNECTED,
        {
            "attempt": 3,
            "reason": "not-a-stop-reason",
            "source_id": "coinbase-primary",
        },
    )
    bad_reconnect = core.emit(
        EventType.FEED_RECONNECT_SCHEDULED,
        {
            "attempt": 1,
            "delay_seconds": -1,
            "source_id": "coinbase-primary",
        },
    )
    bad_heartbeat = core.emit(
        EventType.FEED_HEARTBEAT,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "",
            "payload": {},
            "source_id": "coinbase-primary",
        },
    )
    bad_degradation = core.emit(
        EventType.FEED_DEGRADED,
        {
            "connected_sources": ["coinbase-primary"],
            "disconnected_sources": ["coinbase-primary"],
            "live_count": 2,
            "min_live_sources": 1,
            "stale_sources": [],
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    feed_check = checks[LedgerHealthCheckName.FEED_LIFECYCLE_CONTRACT]
    anomalies = feed_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert feed_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert feed_check.count == 5
    assert feed_check.details["anomaly_count"] == 5
    assert anomalies[0]["sequence"] == invalid_connected.sequence
    assert anomalies[0]["source_id_present"] is False
    assert anomalies[0]["attempt_valid"] is False
    assert anomalies[1]["sequence"] == orphan_disconnect.sequence
    assert anomalies[1]["active_connection_found"] is False
    assert anomalies[1]["reason_valid"] is False
    assert anomalies[2]["sequence"] == bad_reconnect.sequence
    assert anomalies[2]["attempt_follows_disconnect"] is False
    assert anomalies[2]["delay_valid"] is False
    assert anomalies[3]["sequence"] == bad_heartbeat.sequence
    assert anomalies[3]["message_event_type_valid"] is False
    assert anomalies[3]["message_key_present"] is False
    assert anomalies[4]["sequence"] == bad_degradation.sequence
    assert anomalies[4]["live_count_matches_connected_sources"] is False
    assert anomalies[4]["degraded_condition_valid"] is False
    assert anomalies[4]["source_sets_disjoint"] is False


def test_ledger_health_accepts_valid_order_lineage_contract(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="root-order",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            placement_id="root-placement-1",
            logical_order_id="root-order",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
        ).to_payload(),
    )
    requested = core.emit(
        EventType.ACTION_REQUESTED,
        {
            "action_id": "place-root-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "payload": {
                "limit_price": "100",
                "order_type": OrderType.LIMIT.value,
                "product_id": "BTC-USD",
                "side": OrderSide.BUY.value,
                "size": "2",
            },
        },
    )
    assert requested.sequence == 3
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="place-root-1",
            exchange_order_id="exchange-root-1",
            limit_price="101",
            logical_order_id="root-order",
            placement_id="root-placement-2",
            placement_kind=OrderPlacementKind.CANCEL_REPLACE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1.5",
            venue_client_order_id="client-root-2",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="followup-order",
            root_order_id="root-order",
            parent_order_id="root-order",
            lineage_relation=OrderLineageRelation.FOLLOWUP_AFTER_FILL,
            product_id="BTC-USD",
            side=OrderSide.SELL,
            size="1",
            limit_price="110",
            source_order_ids=("root-order",),
        ).to_payload(),
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT].details["logical_order_count"] == 2
    assert checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT].details["placement_count"] == 2


def test_ledger_health_reports_manual_association_without_operator_approval(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="source-root",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="manual-valid",
            root_order_id="source-root",
            lineage_relation=OrderLineageRelation.MANUAL_ASSOCIATION,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
            source_order_ids=("source-root",),
            metadata=manual_association_metadata(
                ManualAssociationApproval(
                    approved_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    approved_by="operator-1",
                    reason="Attach an externally discovered order to the source root.",
                )
            ),
        ).to_payload(),
    )
    bad_manual = core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        {
            "lineage_relation": OrderLineageRelation.MANUAL_ASSOCIATION.value,
            "logical_order_id": "manual-missing-approval",
            "metadata": {},
            "parent_order_id": None,
            "product_id": "BTC-USD",
            "root_order_id": "source-root",
            "schema_version": 1,
            "side": OrderSide.BUY.value,
            "size": "1",
            "source_order_ids": ["source-root"],
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lineage_check = checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    anomalies = lineage_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.count == 1
    assert anomalies[0]["sequence"] == bad_manual.sequence
    assert anomalies[0]["lineage_relation"] == OrderLineageRelation.MANUAL_ASSOCIATION.value
    assert anomalies[0]["manual_association_approval_valid"] is False
    assert anomalies[0]["source_references_found"] is True


def test_ledger_health_reports_missing_order_lineage_placement_after_execution(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    requested = core.emit(
        EventType.ACTION_REQUESTED,
        {
            "action_id": "place-root-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "idempotency_key": "client-root-1",
            "payload": {
                "limit_price": "100",
                "order_type": OrderType.LIMIT.value,
                "product_id": "BTC-USD",
                "side": OrderSide.BUY.value,
                "size": "2",
            },
        },
    )
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": "place-root-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "requested_sequence": requested.sequence,
        },
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="place-root-1",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
            created_by_action_id="place-root-1",
        ).to_payload(),
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": "place-root-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "requested_sequence": requested.sequence,
        },
    )
    executed = core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": "place-root-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "execution_started_sequence": execution_started.sequence,
            "execution_result": {
                "action_id": "place-root-1",
                "action_type": ActionType.PLACE_ORDER.value,
                "client_order_id": "client-root-1",
                "error_category": None,
                "error_code": None,
                "error_message": None,
                "exchange_order_id": "exchange-root-1",
                "mode": ExecutionMode.DRY_RUN.value,
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.ACCEPTED.value,
            },
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lineage_check = checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    anomalies = lineage_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.details["accepted_execution_without_placement_count"] == 1
    assert lineage_check.count == 1
    assert anomalies[0]["sequence"] == executed.sequence
    assert anomalies[0]["event_type"] == EventType.ACTION_EXECUTED.value
    assert anomalies[0]["placement_record_found"] is False
    assert anomalies[0]["exchange_order_id"] == "exchange-root-1"


def test_ledger_health_reports_executed_staged_release_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    requested = core.emit(
        EventType.ACTION_REQUESTED,
        {
            "action_id": "stage-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "idempotency_key": "stage-1",
            "payload": {
                "limit_price": "100",
                "logical_order_id": "stage-logical",
                "order_type": OrderType.LIMIT.value,
                "placement_kind": OrderPlacementKind.STAGED_RELEASE.value,
                "product_id": "BTC-USD",
                "side": OrderSide.BUY.value,
                "size": "2",
            },
        },
    )
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": "stage-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "requested_sequence": requested.sequence,
        },
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="stage-logical",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
            created_by_action_id="stage-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="stage-1",
            limit_price="100",
            logical_order_id="stage-logical",
            placement_id="stage-1",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
        ).to_payload(),
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": "stage-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "requested_sequence": requested.sequence,
        },
    )
    executed = core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": "stage-1",
            "action_type": ActionType.PLACE_ORDER.value,
            "execution_started_sequence": execution_started.sequence,
            "execution_result": {
                "action_id": "stage-1",
                "action_type": ActionType.PLACE_ORDER.value,
                "client_order_id": "stage-client-1",
                "error_category": None,
                "error_code": None,
                "error_message": None,
                "exchange_order_id": "exchange-stage-1",
                "mode": ExecutionMode.DRY_RUN.value,
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.ACCEPTED.value,
            },
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lineage_check = checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    anomalies = lineage_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.count == 1
    assert lineage_check.details["accepted_execution_without_placement_count"] == 0
    assert lineage_check.details["staged_release_execution_count"] == 1
    assert anomalies[0]["sequence"] == executed.sequence
    assert anomalies[0]["event_type"] == EventType.ACTION_EXECUTED.value
    assert anomalies[0]["placement_kind"] == OrderPlacementKind.STAGED_RELEASE.value
    assert anomalies[0]["staged_release_executed"] is True
    assert anomalies[0]["exchange_order_id"] == "exchange-stage-1"


def test_ledger_health_accepts_valid_release_placement_contract(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    _emit_place_order_request(
        core,
        action_id="stage-1",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="stage-logical",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
            created_by_action_id="stage-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="stage-1",
            limit_price="100",
            logical_order_id="stage-logical",
            placement_id="stage-1",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
        ).to_payload(),
    )
    _emit_place_order_request(
        core,
        action_id="release-1",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.RELEASE,
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="release-1",
            exchange_order_id="exchange-release-1",
            limit_price="100",
            logical_order_id="stage-logical",
            metadata={
                "staged_release": {
                    "release_of_action_id": "stage-1",
                    "release_of_placement_id": "stage-1",
                },
            },
            placement_id="release-1",
            placement_kind=OrderPlacementKind.RELEASE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2.0",
            venue_client_order_id="client-release-1",
        ).to_payload(),
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lineage_check = checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]

    assert health.status == LedgerHealthStatus.OK
    assert lineage_check.status == LedgerHealthStatus.OK
    assert lineage_check.details["placement_count"] == 2
    assert lineage_check.details["release_placement_count"] == 1


def test_ledger_health_summarizes_passive_market_making_quotes(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    _emit_place_order_request(
        core,
        action_id="stage-1",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="stage-logical",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="99",
            created_by_action_id="stage-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="stage-1",
            limit_price="99",
            logical_order_id="stage-logical",
            metadata={
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
                    "size": "2",
                },
            },
            placement_id="stage-1",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
        ).to_payload(),
    )

    health = ledger_health(ledger_path)
    summary = summarize_ledger(ledger_path)
    lineage_check = {
        check.name: check for check in health.checks
    }[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    quote_summary = lineage_check.details["passive_market_making_quotes"][0]

    assert health.status == LedgerHealthStatus.OK
    assert lineage_check.details["passive_market_making_quote_count"] == 1
    assert lineage_check.details["passive_market_making_unreleased_quote_count"] == 1
    assert lineage_check.details["passive_market_making_released_quote_count"] == 0
    assert summary.passive_market_making_quote_count == 1
    assert summary.passive_market_making_unreleased_quote_count == 1
    assert summary.passive_market_making_released_quote_count == 0
    assert quote_summary["placement_id"] == "stage-1"
    assert quote_summary["released"] is False
    assert quote_summary["bid_price"] == "99"
    assert quote_summary["ask_price"] == "101"
    assert quote_summary["midpoint"] == "100"

    _emit_place_order_request(
        core,
        action_id="release-1",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.RELEASE,
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="release-1",
            exchange_order_id="exchange-release-1",
            limit_price="99",
            logical_order_id="stage-logical",
            metadata={
                "staged_release": {
                    "release_of_action_id": "stage-1",
                    "release_of_placement_id": "stage-1",
                },
            },
            placement_id="release-1",
            placement_kind=OrderPlacementKind.RELEASE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2.0",
            venue_client_order_id="client-release-1",
        ).to_payload(),
    )

    released_health = ledger_health(ledger_path)
    released_summary = summarize_ledger(ledger_path)
    released_lineage_check = {
        check.name: check for check in released_health.checks
    }[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    released_quote_summary = released_lineage_check.details["passive_market_making_quotes"][0]

    assert released_health.status == LedgerHealthStatus.OK
    assert released_lineage_check.details["passive_market_making_quote_count"] == 1
    assert released_lineage_check.details["passive_market_making_unreleased_quote_count"] == 0
    assert released_lineage_check.details["passive_market_making_released_quote_count"] == 1
    assert released_summary.passive_market_making_quote_count == 1
    assert released_summary.passive_market_making_unreleased_quote_count == 0
    assert released_summary.passive_market_making_released_quote_count == 1
    assert released_quote_summary["released"] is True
    assert released_quote_summary["release_placement_id"] == "release-1"


def test_ledger_health_reports_passive_market_making_quote_contract_mismatch(
    workspace_tmp_path,
):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    _emit_place_order_request(
        core,
        action_id="stage-bad",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="stage-logical",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="99",
            created_by_action_id="stage-bad",
        ).to_payload(),
    )
    staged = core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="stage-bad",
            limit_price="99",
            logical_order_id="stage-logical",
            metadata={
                "passive_market_making": {
                    "ask_price": "101",
                    "bid_price": "102",
                    "half_spread_bps": "0",
                    "midpoint": "99",
                    "product_id": "ETH-USD",
                    "side": OrderSide.SELL.value,
                },
                "staged_release": {
                    "chunk_count": 1,
                    "chunk_index": 1,
                    "size": "2",
                },
            },
            placement_id="stage-bad",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
        ).to_payload(),
    )

    health = ledger_health(ledger_path)
    lineage_check = {
        check.name: check for check in health.checks
    }[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    anomaly = lineage_check.details["anomalies"][0]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.count == 1
    assert lineage_check.details["passive_market_making_quote_count"] == 1
    assert lineage_check.details["passive_market_making_quote_contract_anomaly_count"] == 1
    assert anomaly["sequence"] == staged.sequence
    assert anomaly["placement_id"] == "stage-bad"
    assert anomaly["passive_market_making_quote_contract_valid"] is False
    assert anomaly["placement_is_staged_release"] is True
    assert anomaly["product_matches_metadata"] is False
    assert anomaly["side_matches_metadata"] is False
    assert anomaly["price_order_valid"] is False
    assert anomaly["half_spread_bps_valid"] is False
    assert anomaly["limit_price_matches_passive_side_price"] is False


def test_ledger_health_reports_release_placement_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    _emit_place_order_request(
        core,
        action_id="stage-1",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
    )
    core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        LogicalOrderRecord(
            logical_order_id="stage-logical",
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            limit_price="100",
            created_by_action_id="stage-1",
        ).to_payload(),
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="stage-1",
            limit_price="100",
            logical_order_id="stage-logical",
            placement_id="stage-1",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            placement_status=OrderPlacementStatus.STAGED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
        ).to_payload(),
    )
    _emit_place_order_request(
        core,
        action_id="release-valid",
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.RELEASE,
    )
    core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id="release-valid",
            exchange_order_id="exchange-release-valid",
            limit_price="100",
            logical_order_id="stage-logical",
            metadata={
                "staged_release": {
                    "release_of_action_id": "stage-1",
                    "release_of_placement_id": "stage-1",
                },
            },
            placement_id="release-valid",
            placement_kind=OrderPlacementKind.RELEASE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            venue_client_order_id="client-release-valid",
        ).to_payload(),
    )
    missing_metadata = _emit_release_placement(
        core,
        action_id="release-missing-metadata",
        metadata={},
    )
    duplicate_release = _emit_release_placement(
        core,
        action_id="release-duplicate",
        metadata={
            "staged_release": {
                "release_of_action_id": "stage-1",
                "release_of_placement_id": "stage-1",
            },
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lineage_check = checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    anomalies = lineage_check.details["anomalies"]
    anomalies_by_placement_id = {anomaly["placement_id"]: anomaly for anomaly in anomalies}

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.count == 2
    assert lineage_check.details["release_placement_count"] == 3
    assert anomalies_by_placement_id["release-missing-metadata"]["sequence"] == missing_metadata.sequence
    assert anomalies_by_placement_id["release-missing-metadata"]["release_contract_valid"] is False
    assert anomalies_by_placement_id["release-missing-metadata"]["release_contract"]["release_of_placement_id"] is None
    assert anomalies_by_placement_id["release-duplicate"]["sequence"] == duplicate_release.sequence
    assert anomalies_by_placement_id["release-duplicate"]["release_contract"]["duplicate_release"] is True
    assert (
        anomalies_by_placement_id["release-duplicate"]["release_contract"]["already_released_by_placement_id"]
        == "release-valid"
    )


def _emit_place_order_request(
    core: AuditCore,
    *,
    action_id: str,
    logical_order_id: str,
    placement_kind: OrderPlacementKind,
) -> None:
    core.emit(
        EventType.ACTION_REQUESTED,
        {
            "action_id": action_id,
            "action_type": ActionType.PLACE_ORDER.value,
            "idempotency_key": action_id,
            "payload": {
                "limit_price": "100",
                "logical_order_id": logical_order_id,
                "order_type": OrderType.LIMIT.value,
                "placement_kind": placement_kind.value,
                "product_id": "BTC-USD",
                "side": OrderSide.BUY.value,
                "size": "2",
            },
        },
    )


def _emit_release_placement(
    core: AuditCore,
    *,
    action_id: str,
    metadata: dict[str, Any],
) -> Any:
    _emit_place_order_request(
        core,
        action_id=action_id,
        logical_order_id="stage-logical",
        placement_kind=OrderPlacementKind.RELEASE,
    )
    return core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        OrderPlacementRecord(
            action_id=action_id,
            exchange_order_id=f"exchange-{action_id}",
            limit_price="100",
            logical_order_id="stage-logical",
            metadata=metadata,
            placement_id=action_id,
            placement_kind=OrderPlacementKind.RELEASE,
            placement_status=OrderPlacementStatus.ACCEPTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="2",
            venue_client_order_id=f"client-{action_id}",
        ).to_payload(),
    )


def test_ledger_health_reports_order_lineage_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    bad_root = core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        {
            "lineage_relation": OrderLineageRelation.ROOT.value,
            "logical_order_id": "bad-root",
            "parent_order_id": "should-not-exist",
            "product_id": "BTC-USD",
            "root_order_id": "other-root",
            "schema_version": 1,
            "side": OrderSide.BUY.value,
            "size": "1",
            "source_order_ids": ["source-1"],
        },
    )
    orphan_followup = core.emit(
        EventType.ORDER_LOGICAL_CREATED,
        {
            "lineage_relation": OrderLineageRelation.FOLLOWUP_AFTER_FILL.value,
            "logical_order_id": "orphan-followup",
            "parent_order_id": "missing-parent",
            "product_id": "BTC-USD",
            "root_order_id": "missing-parent",
            "schema_version": 1,
            "side": OrderSide.SELL.value,
            "size": "1",
            "source_order_ids": ["missing-parent"],
        },
    )
    bad_placement = core.emit(
        EventType.ORDER_PLACEMENT_RECORDED,
        {
            "logical_order_id": "orphan-followup",
            "placement_id": "bad-placement",
            "placement_kind": OrderPlacementKind.CANCEL_REPLACE.value,
            "placement_status": OrderPlacementStatus.SUBMITTED.value,
            "product_id": "ETH-USD",
            "schema_version": 1,
            "side": OrderSide.BUY.value,
            "size": "0",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lineage_check = checks[LedgerHealthCheckName.ORDER_LINEAGE_CONTRACT]
    anomalies = lineage_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lineage_check.count == 3
    assert lineage_check.details["anomaly_count"] == 3
    assert anomalies[0]["sequence"] == bad_root.sequence
    assert anomalies[0]["relation_shape_valid"] is False
    assert anomalies[0]["root_self_reference"] is False
    assert anomalies[1]["sequence"] == orphan_followup.sequence
    assert anomalies[1]["parent_reference_found"] is False
    assert anomalies[1]["source_references_found"] is False
    assert anomalies[2]["sequence"] == bad_placement.sequence
    assert anomalies[2]["logical_order_found"] is True
    assert anomalies[2]["prior_placement_required"] is True
    assert anomalies[2]["prior_placement_found"] is False
    assert anomalies[2]["side_matches_logical_order"] is False
    assert anomalies[2]["size_valid"] is False


def test_ledger_health_reports_runtime_task_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    watchdog_start = core.emit(
        EventType.RUNTIME_TASK_STARTED,
        {
            "interval_seconds": 30,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "task_id": RuntimeTask.WATCHDOG.value,
        },
    )
    bad_completion = core.emit(
        EventType.RUNTIME_TASK_COMPLETED,
        {
            "completed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "result": {},
            "started_sequence": watchdog_start.sequence + 100,
            "task_id": RuntimeTask.WATCHDOG.value,
        },
    )
    orphan_start = core.emit(
        EventType.RUNTIME_TASK_STARTED,
        {
            "interval_seconds": 60,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "task_id": RuntimeTask.ORDER_RECOVERY.value,
        },
    )
    failed_start = core.emit(
        EventType.RUNTIME_TASK_STARTED,
        {
            "interval_seconds": 15,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "task_id": RuntimeTask.FEED_HEALTH.value,
        },
    )
    core.emit(
        EventType.ERROR,
        {
            "error_category": ErrorCategory.RUNTIME_TASK.value,
            "message": "feed health failed",
            "started_sequence": failed_start.sequence,
            "task_id": RuntimeTask.FEED_HEALTH.value,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    task_check = checks[LedgerHealthCheckName.RUNTIME_TASK_CONTRACT]
    anomalies = task_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert task_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert task_check.count == 3
    assert task_check.details["anomaly_count"] == 3
    assert anomalies[0]["event_type"] == EventType.RUNTIME_TASK_COMPLETED.value
    assert anomalies[0]["sequence"] == bad_completion.sequence
    assert anomalies[0]["started_reference_found"] is False
    assert anomalies[0]["task_id"] == RuntimeTask.WATCHDOG.value
    assert anomalies[1]["event_type"] == EventType.RUNTIME_TASK_STARTED.value
    assert anomalies[1]["sequence"] == watchdog_start.sequence
    assert anomalies[1]["closed"] is False
    assert anomalies[2]["event_type"] == EventType.RUNTIME_TASK_STARTED.value
    assert anomalies[2]["sequence"] == orphan_start.sequence
    assert anomalies[2]["task_id"] == RuntimeTask.ORDER_RECOVERY.value


def test_ledger_health_accepts_valid_data_flow_with_duplicate(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    first_received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:market:1",
            "payload": {"channel": "ticker", "sequence_num": 1},
            "received_at": "2026-01-01T00:00:00+00:00",
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:market:1",
            "received_sequence": first_received.sequence,
            "source_id": "coinbase-primary",
        },
    )
    second_received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:market:1",
            "payload": {"channel": "ticker", "sequence_num": 1},
            "received_at": "2026-01-01T00:00:01+00:00",
            "source_id": "coinbase-secondary",
        },
    )
    core.emit(
        EventType.DATA_DUPLICATE,
        {
            "message_key": "coinbase:market:1",
            "received_sequence": second_received.sequence,
            "source_id": "coinbase-secondary",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.DATA_FLOW_CONTRACT].status == LedgerHealthStatus.OK


def test_ledger_health_reports_data_flow_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    invalid_received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": "not-an-event-type",
            "message_key": "bad-received",
            "payload": {},
            "source_id": "coinbase-primary",
        },
    )
    orphan_accepted = core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "orphan-accepted",
            "received_sequence": invalid_received.sequence + 100,
            "source_id": "coinbase-primary",
        },
    )
    good_received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:market:2",
            "payload": {"channel": "ticker", "sequence_num": 2},
            "source_id": "coinbase-primary",
        },
    )
    mismatched_accepted = core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "wrong-key",
            "received_sequence": good_received.sequence,
            "source_id": "coinbase-primary",
        },
    )
    bad_duplicate = core.emit(
        EventType.DATA_DUPLICATE,
        {
            "message_key": "never-accepted",
            "received_sequence": good_received.sequence,
            "source_id": "coinbase-primary",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    data_check = checks[LedgerHealthCheckName.DATA_FLOW_CONTRACT]
    anomalies = data_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert data_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert data_check.count == 4
    assert data_check.details["anomaly_count"] == 4
    assert anomalies[0]["event_type"] == EventType.DATA_RECEIVED.value
    assert anomalies[0]["sequence"] == invalid_received.sequence
    assert anomalies[0]["message_event_type_valid"] is False
    assert anomalies[1]["event_type"] == EventType.DATA_ACCEPTED.value
    assert anomalies[1]["sequence"] == orphan_accepted.sequence
    assert anomalies[1]["received_reference_found"] is False
    assert anomalies[2]["event_type"] == EventType.DATA_ACCEPTED.value
    assert anomalies[2]["sequence"] == mismatched_accepted.sequence
    assert anomalies[2]["message_key_matches"] is False
    assert anomalies[2]["received_message_key"] == "coinbase:market:2"
    assert anomalies[3]["event_type"] == EventType.DATA_DUPLICATE.value
    assert anomalies[3]["sequence"] == bad_duplicate.sequence
    assert anomalies[3]["duplicate_has_prior_accept"] is False
    assert anomalies[3]["message_key_matches"] is False


def test_ledger_health_accepts_valid_trigger_contract(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    matched = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:market:trigger-valid",
            "payload": {},
            "source_id": "coinbase-primary",
        },
    )
    core.emit(
        EventType.TRIGGER_FIRED,
        {
            "matched_event_type": EventType.DATA_RECEIVED.value,
            "matched_sequence": matched.sequence,
            "relation": TriggerRelation.ON.value,
            "trigger_id": "on-market-data",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.TRIGGER_CONTRACT].status == LedgerHealthStatus.OK


def test_ledger_health_reports_trigger_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    matched = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.DATA_RECEIVED.value,
            "message_key": "coinbase:market:trigger-bad",
            "payload": {},
            "source_id": "coinbase-primary",
        },
    )
    bad_identity = core.emit(
        EventType.TRIGGER_FIRED,
        {
            "matched_event_type": EventType.DATA_ACCEPTED.value,
            "matched_sequence": matched.sequence,
            "relation": "not-a-relation",
            "trigger_id": "",
        },
    )
    orphan = core.emit(
        EventType.TRIGGER_FIRED,
        {
            "matched_event_type": EventType.DATA_RECEIVED.value,
            "matched_sequence": matched.sequence + 100,
            "relation": TriggerRelation.ON.value,
            "trigger_id": "orphan-trigger",
        },
    )
    partial = core.emit(
        EventType.TRIGGER_FIRED,
        {
            "matched_event_type": EventType.DATA_RECEIVED.value,
            "matched_sequence": None,
            "relation": TriggerRelation.ON.value,
            "trigger_id": "partial-trigger",
        },
    )
    bad_before = core.emit(
        EventType.TRIGGER_FIRED,
        {
            "matched_event_type": EventType.DATA_RECEIVED.value,
            "matched_sequence": matched.sequence,
            "relation": TriggerRelation.BEFORE.value,
            "trigger_id": "late-before-trigger",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    trigger_check = checks[LedgerHealthCheckName.TRIGGER_CONTRACT]
    anomalies = trigger_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert trigger_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert trigger_check.count == 4
    assert trigger_check.details["anomaly_count"] == 4
    assert anomalies[0]["sequence"] == bad_identity.sequence
    assert anomalies[0]["trigger_id_present"] is False
    assert anomalies[0]["relation_valid"] is False
    assert anomalies[0]["matched_event_type_matches"] is False
    assert anomalies[1]["sequence"] == orphan.sequence
    assert anomalies[1]["matched_reference_found"] is False
    assert anomalies[2]["sequence"] == partial.sequence
    assert anomalies[2]["matched_pair_consistent"] is False
    assert anomalies[3]["sequence"] == bad_before.sequence
    assert anomalies[3]["relation_order_valid"] is False


def test_ledger_health_reports_unanchored_checkpoints(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    checkpoint = record_ledger_checkpoint(ledger_path)

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].count == 1
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].details["latest_checkpoint_hash"] == (
        checkpoint.checkpoint.checkpoint_hash
    )
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].details["unanchored_checkpoint_hashes"] == [
        checkpoint.checkpoint.checkpoint_hash
    ]


def test_ledger_health_reports_anchor_freshness_policy_breach(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    checkpoint = record_ledger_checkpoint(ledger_path)
    anchor = publish_recorded_ledger_checkpoint_anchor(
        ledger_path,
        checkpoint,
        LocalFileLedgerAnchorStore(workspace_tmp_path / "anchors"),
    )
    application.core.emit(EventType.ACTION_REQUESTED, {"client_order_id": "order-after-anchor"})

    health = ledger_health(ledger_path, max_records_after_anchor=2)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert checks[LedgerHealthCheckName.ANCHOR_COVERAGE].status == LedgerHealthStatus.OK
    assert checks[LedgerHealthCheckName.ANCHOR_FRESHNESS].status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert checks[LedgerHealthCheckName.ANCHOR_FRESHNESS].count == 3
    assert checks[LedgerHealthCheckName.ANCHOR_FRESHNESS].details["latest_anchor_record_sequence"] == (
        anchor.audit_record_sequence
    )
    assert checks[LedgerHealthCheckName.ANCHOR_FRESHNESS].details["records_after_latest_anchor_checkpoint"] == 3


def test_ledger_health_reports_archive_freshness_policy_breach(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))

    health = ledger_health(ledger_path, max_records_after_archive=0)
    checks = {check.name: check for check in health.checks}
    freshness = checks[LedgerHealthCheckName.ARCHIVE_FRESHNESS]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert freshness.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert freshness.details["archive_count"] == 0
    assert freshness.details["records_after_latest_archive"] == len(application.ledger.iter_records())


def test_ledger_health_reports_missing_enabled_product_catalog_snapshot(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                    run_on_start=True,
                ),
                product_ids=("BTC-USD",),
            )
        ),
    )
    core = AuditCore(AuditLedger(ledger_path))

    core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "startup_metadata": application_config_startup_metadata(config),
            "task_count": 1,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    freshness = checks[LedgerHealthCheckName.PRODUCT_CATALOG_FRESHNESS]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert freshness.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert freshness.details["enabled"] is True
    assert freshness.details["configured_product_ids"] == ["BTC-USD"]
    assert freshness.details["missing_snapshot"] is True
    assert freshness.details["missing_startup_snapshot"] is True


def test_ledger_health_accepts_enabled_product_catalog_after_snapshot(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                    run_on_start=True,
                ),
                product_ids=("BTC-USD",),
            )
        ),
    )
    core = AuditCore(AuditLedger(ledger_path))

    core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "startup_metadata": application_config_startup_metadata(config),
            "task_count": 1,
        },
    )
    core.emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "product_count": 1,
            "product_ids": ["BTC-USD"],
            "products": [
                {
                    "product_id": "BTC-USD",
                    "product_type": ProductType.SPOT.value,
                    "product_venue": ProductVenue.CBE.value,
                    "tradable_for_new_orders": True,
                }
            ],
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    freshness = checks[LedgerHealthCheckName.PRODUCT_CATALOG_FRESHNESS]

    assert health.status == LedgerHealthStatus.OK
    assert freshness.status == LedgerHealthStatus.OK
    assert freshness.details["latest_product_snapshot_sequence"] == 2
    assert freshness.details["product_count"] == 1


def test_ledger_health_product_catalog_freshness_ignores_feed_smoke_starts(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                    run_on_start=True,
                ),
                product_ids=("BTC-USD",),
            )
        ),
    )
    core = AuditCore(AuditLedger(ledger_path))

    core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "startup_metadata": application_config_startup_metadata(config),
            "task_count": 1,
        },
    )
    core.emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "product_count": 1,
            "product_ids": ["BTC-USD"],
            "products": [
                {
                    "product_id": "BTC-USD",
                    "product_type": ProductType.SPOT.value,
                    "product_venue": ProductVenue.CBE.value,
                    "tradable_for_new_orders": True,
                }
            ],
        },
    )
    core.emit(
        EventType.SYSTEM_STOPPED,
        {
            "component": RuntimeComponent.ORCHESTRATOR.value,
            "completed_cycles": 1,
            "reason": RuntimeStopReason.MAX_CYCLES.value,
            "stopped_at": datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        },
    )
    core.emit(
        EventType.SYSTEM_STARTED,
        {
            "component": RuntimeComponent.FEED_SMOKE.value,
            "started_at": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
            "startup_metadata": application_config_startup_metadata(config),
            "task_count": 2,
        },
    )
    core.emit(
        EventType.SYSTEM_STOPPED,
        {
            "component": RuntimeComponent.FEED_SMOKE.value,
            "completed_cycles": 0,
            "reason": RuntimeStopReason.STOP_REQUESTED.value,
            "stopped_at": datetime(2026, 1, 1, 0, 1, 1, tzinfo=timezone.utc),
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    freshness = checks[LedgerHealthCheckName.PRODUCT_CATALOG_FRESHNESS]

    assert health.status == LedgerHealthStatus.OK
    assert freshness.status == LedgerHealthStatus.OK
    assert freshness.details["latest_start_sequence"] == 1
    assert freshness.details["latest_product_snapshot_sequence"] == 2
    assert freshness.details["missing_startup_snapshot"] is False


def test_ledger_health_reports_live_accepted_orders_outside_supported_venues(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    core.emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "product_count": 2,
            "product_ids": ["BIT-29MAY26-CDE", "BTC-PERP-INTX"],
            "products": [
                {
                    "product_id": "BIT-29MAY26-CDE",
                    "product_type": ProductType.FUTURE.value,
                    "product_venue": ProductVenue.FCM.value,
                    "tradable_for_new_orders": True,
                },
                {
                    "product_id": "BTC-PERP-INTX",
                    "product_type": ProductType.FUTURE.value,
                    "product_venue": ProductVenue.INTX.value,
                    "tradable_for_new_orders": True,
                },
            ],
        },
    )
    _emit_live_accepted_order(core, action_id="live-cfm-1", product_id="BIT-29MAY26-CDE")
    _emit_live_accepted_order(core, action_id="live-intx-1", product_id="BTC-PERP-INTX")
    _emit_live_accepted_order(core, action_id="live-missing-1", product_id="ETH-USD")

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    venue_check = checks[LedgerHealthCheckName.LIVE_EXECUTION_VENUE]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert venue_check.count == 2
    assert venue_check.details["allowed_product_venues"] == [ProductVenue.CBE.value, ProductVenue.FCM.value]
    assert venue_check.details["unsupported_product_venue_count"] == 1
    assert venue_check.details["unsupported_product_venue_orders"][0]["action_id"] == "live-intx-1"
    assert venue_check.details["unsupported_product_venue_orders"][0]["product_venue"] == ProductVenue.INTX.value
    assert venue_check.details["missing_product_metadata_count"] == 1
    assert venue_check.details["missing_product_metadata_orders"][0]["action_id"] == "live-missing-1"


def test_ledger_health_reports_action_execution_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    command = PlaceOrderIntent(
        action_id="contract-place-1",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="1",
        limit_price="50000",
    ).to_command()
    requested = core.emit(EventType.ACTION_REQUESTED, command.to_payload())
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    executed = core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "execution_result": {
                "action_id": "different-action",
                "action_type": ActionType.CANCEL_ORDER.value,
                "client_order_id": command.action_id,
                "exchange_order_id": "exchange-contract-1",
                "mode": ExecutionMode.DRY_RUN.value,
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.ACCEPTED.value,
            },
            "execution_started_sequence": execution_started.sequence,
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    contract_check = checks[LedgerHealthCheckName.ACTION_EXECUTION_CONTRACT]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert contract_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert contract_check.count == 1
    assert contract_check.details["mismatch_count"] == 1
    assert contract_check.details["mismatches"] == [
        {
            "action_id": command.action_id,
            "action_id_matches": False,
            "action_type": ActionType.PLACE_ORDER.value,
            "action_type_matches": False,
            "allowed_statuses": [
                ExecutionStatus.ACCEPTED.value,
                ExecutionStatus.REJECTED.value,
                ExecutionStatus.FAILED.value,
            ],
            "client_order_id_matches": True,
            "expected_client_order_id": command.action_id,
            "mode_valid": True,
            "result_action_id": "different-action",
            "result_action_type": ActionType.CANCEL_ORDER.value,
            "result_client_order_id": command.action_id,
            "result_mode": ExecutionMode.DRY_RUN.value,
            "result_status": ExecutionStatus.ACCEPTED.value,
            "sequence": executed.sequence,
            "status_matches_action_type": True,
        }
    ]


def test_ledger_health_reports_action_execution_client_order_id_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    command = PlaceOrderIntent(
        action_id="contract-place-1",
        idempotency_key="expected-client-order-id",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="1",
        limit_price="50000",
    ).to_command()
    requested = core.emit(EventType.ACTION_REQUESTED, command.to_payload())
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    executed = core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "execution_result": {
                "action_id": command.action_id,
                "action_type": command.action_type.value,
                "client_order_id": "unexpected-client-order-id",
                "exchange_order_id": "exchange-contract-1",
                "mode": ExecutionMode.DRY_RUN.value,
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.ACCEPTED.value,
            },
            "execution_started_sequence": execution_started.sequence,
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    contract_check = checks[LedgerHealthCheckName.ACTION_EXECUTION_CONTRACT]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert contract_check.count == 1
    assert contract_check.details["mismatches"] == [
        {
            "action_id": command.action_id,
            "action_id_matches": True,
            "action_type": ActionType.PLACE_ORDER.value,
            "action_type_matches": True,
            "allowed_statuses": [
                ExecutionStatus.ACCEPTED.value,
                ExecutionStatus.REJECTED.value,
                ExecutionStatus.FAILED.value,
            ],
            "client_order_id_matches": False,
            "expected_client_order_id": "expected-client-order-id",
            "mode_valid": True,
            "result_action_id": command.action_id,
            "result_action_type": ActionType.PLACE_ORDER.value,
            "result_client_order_id": "unexpected-client-order-id",
            "result_mode": ExecutionMode.DRY_RUN.value,
            "result_status": ExecutionStatus.ACCEPTED.value,
            "sequence": executed.sequence,
            "status_matches_action_type": True,
        }
    ]


def test_ledger_health_reports_action_execution_status_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    command = PlaceOrderIntent(
        action_id="contract-place-1",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="1",
        limit_price="50000",
    ).to_command()
    requested = core.emit(EventType.ACTION_REQUESTED, command.to_payload())
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    executed = core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "execution_result": {
                "action_id": command.action_id,
                "action_type": command.action_type.value,
                "client_order_id": command.action_id,
                "exchange_order_id": "exchange-contract-1",
                "mode": ExecutionMode.DRY_RUN.value,
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.CANCELLED.value,
            },
            "execution_started_sequence": execution_started.sequence,
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    contract_check = checks[LedgerHealthCheckName.ACTION_EXECUTION_CONTRACT]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert contract_check.count == 1
    assert contract_check.details["mismatches"] == [
        {
            "action_id": command.action_id,
            "action_id_matches": True,
            "action_type": ActionType.PLACE_ORDER.value,
            "action_type_matches": True,
            "allowed_statuses": [
                ExecutionStatus.ACCEPTED.value,
                ExecutionStatus.REJECTED.value,
                ExecutionStatus.FAILED.value,
            ],
            "client_order_id_matches": True,
            "expected_client_order_id": command.action_id,
            "mode_valid": True,
            "result_action_id": command.action_id,
            "result_action_type": ActionType.PLACE_ORDER.value,
            "result_client_order_id": command.action_id,
            "result_mode": ExecutionMode.DRY_RUN.value,
            "result_status": ExecutionStatus.CANCELLED.value,
            "sequence": executed.sequence,
            "status_matches_action_type": False,
        }
    ]


def test_ledger_health_reports_action_execution_missing_mode_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    command = PlaceOrderIntent(
        action_id="contract-place-1",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="1",
        limit_price="50000",
    ).to_command()
    requested = core.emit(EventType.ACTION_REQUESTED, command.to_payload())
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    executed = core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "execution_result": {
                "action_id": command.action_id,
                "action_type": command.action_type.value,
                "client_order_id": command.action_id,
                "exchange_order_id": "exchange-contract-1",
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.ACCEPTED.value,
            },
            "execution_started_sequence": execution_started.sequence,
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    contract_check = checks[LedgerHealthCheckName.ACTION_EXECUTION_CONTRACT]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert contract_check.count == 1
    assert contract_check.details["mismatches"] == [
        {
            "action_id": command.action_id,
            "action_id_matches": True,
            "action_type": ActionType.PLACE_ORDER.value,
            "action_type_matches": True,
            "allowed_statuses": [
                ExecutionStatus.ACCEPTED.value,
                ExecutionStatus.REJECTED.value,
                ExecutionStatus.FAILED.value,
            ],
            "client_order_id_matches": True,
            "expected_client_order_id": command.action_id,
            "mode_valid": False,
            "result_action_id": command.action_id,
            "result_action_type": ActionType.PLACE_ORDER.value,
            "result_client_order_id": command.action_id,
            "result_mode": None,
            "result_status": ExecutionStatus.ACCEPTED.value,
            "sequence": executed.sequence,
            "status_matches_action_type": True,
        }
    ]


def test_ledger_health_reports_action_lifecycle_reference_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    command = PlaceOrderIntent(
        action_id="lifecycle-place-1",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="1",
        limit_price="50000",
    ).to_command()
    requested = core.emit(EventType.ACTION_REQUESTED, command.to_payload())
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence + 100,
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    lifecycle_check = checks[LedgerHealthCheckName.ACTION_LIFECYCLE_CONTRACT]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lifecycle_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert lifecycle_check.count == 1
    assert lifecycle_check.details["anomaly_count"] == 1
    assert lifecycle_check.details["anomalies"] == [
        {
            "action_id": command.action_id,
            "action_id_matches": False,
            "action_type": ActionType.PLACE_ORDER.value,
            "action_type_matches": False,
            "event_type": EventType.ACTION_EXECUTION_STARTED.value,
            "field": "accepted_sequence",
            "field_present": True,
            "reference_found": False,
            "referenced_action_id": None,
            "referenced_action_type": None,
            "referenced_sequence": accepted.sequence + 100,
            "sequence": execution_started.sequence,
        }
    ]


def test_ledger_health_reports_order_identity_contract_collision(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))

    first = PlaceOrderIntent(
        action_id="identity-place-1",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="50000",
        idempotency_key="shared-client-order-id",
    ).to_command()
    first_requested = core.emit(EventType.ACTION_REQUESTED, first.to_payload())
    core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": first.action_id,
            "action_type": first.action_type.value,
            "requested_sequence": first_requested.sequence,
        },
    )
    second = PlaceOrderIntent(
        action_id="identity-place-2",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="0.01",
        limit_price="50000",
        idempotency_key="shared-client-order-id",
    ).to_command()
    second_requested = core.emit(EventType.ACTION_REQUESTED, second.to_payload())
    second_accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": second.action_id,
            "action_type": second.action_type.value,
            "requested_sequence": second_requested.sequence,
        },
    )
    _emit_live_accepted_order(
        core,
        action_id="identity-place-3",
        product_id="BTC-USD",
        exchange_order_id="shared-exchange-order-id",
    )
    _emit_live_accepted_order(
        core,
        action_id="identity-place-4",
        product_id="BTC-USD",
        exchange_order_id="shared-exchange-order-id",
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    identity_check = checks[LedgerHealthCheckName.ORDER_IDENTITY_CONTRACT]
    collisions = identity_check.details["collisions"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert identity_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert identity_check.count == 2
    assert identity_check.details["collision_count"] == 2
    assert collisions[0] == {
        "event_type": EventType.ACTION_ACCEPTED.value,
        "existing_action_id": "identity-place-1",
        "identifier": "shared-client-order-id",
        "identifier_type": "client_order_id",
        "observed_action_id": "identity-place-2",
        "sequence": second_accepted.sequence,
    }
    assert collisions[1]["event_type"] == EventType.ACTION_EXECUTED.value
    assert collisions[1]["existing_action_id"] == "identity-place-3"
    assert collisions[1]["identifier"] == "shared-exchange-order-id"
    assert collisions[1]["identifier_type"] == "exchange_order_id"
    assert collisions[1]["observed_action_id"] == "identity-place-4"


def test_ledger_health_reports_order_update_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    received = core.emit(
        EventType.DATA_RECEIVED,
        {
            "message_event_type": EventType.EXCHANGE_ORDER_UPDATE.value,
            "message_key": "bad-user-update",
            "payload": {
                "order": {
                    "order_id": "exchange-1",
                    "status": "NOT_A_STATUS",
                }
            },
            "source_id": "coinbase-primary",
        },
    )
    accepted = core.emit(
        EventType.DATA_ACCEPTED,
        {
            "message_event_type": EventType.EXCHANGE_ORDER_UPDATE.value,
            "message_key": "bad-user-update",
            "received_sequence": received.sequence,
            "source_id": "coinbase-primary",
        },
    )
    recovery = core.emit(
        EventType.RECONCILIATION_RECOVERY,
        {
            "action_id": "place-1",
            "lookup_status": ExchangeLookupStatus.FOUND.value,
            "order_update": {
                "client_order_id": "client-1",
                "status": ExchangeOrderStatus.UNKNOWN.value,
            },
            "reason": ReconciliationIssue.MISSING_USER_CONFIRMATION.value,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    order_update_check = checks[LedgerHealthCheckName.ORDER_UPDATE_CONTRACT]
    anomalies = order_update_check.details["anomalies"]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert order_update_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert order_update_check.count == 2
    assert order_update_check.details["anomaly_count"] == 2
    assert anomalies[0]["event_type"] == EventType.DATA_ACCEPTED.value
    assert anomalies[0]["sequence"] == accepted.sequence
    assert anomalies[0]["message_key"] == "bad-user-update"
    assert set(anomalies[0]["missing_fields"]) == {"product_id"}
    assert set(anomalies[0]["invalid_fields"]) == {"status"}
    assert anomalies[1]["event_type"] == EventType.RECONCILIATION_RECOVERY.value
    assert anomalies[1]["sequence"] == recovery.sequence
    assert anomalies[1]["action_id"] == "place-1"
    assert set(anomalies[1]["missing_fields"]) == {"product_id"}
    assert set(anomalies[1]["invalid_fields"]) == {"status"}


def test_ledger_health_reports_exchange_fill_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    core.emit(
        EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        {
            "product_count": 1,
            "product_ids": ["BTC-USD"],
            "products": [
                {
                    "product_id": "BTC-USD",
                    "product_type": ProductType.SPOT.value,
                    "product_venue": ProductVenue.CBE.value,
                    "tradable_for_new_orders": True,
                }
            ],
        },
    )
    _emit_live_accepted_order(core, action_id="fill-place-1", product_id="BTC-USD")
    fill = core.emit(
        EventType.EXCHANGE_FILL,
        {
            "action_id": "other-action",
            "client_order_id": "other-client",
            "commission": "1.25",
            "fill_id": "fill-1",
            "order_id": "exchange-fill-place-1",
            "price": "100000",
            "product_id": "ETH-USD",
            "side": "SELL",
            "size": "0.01",
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    fill_check = checks[LedgerHealthCheckName.FILL_CONTRACT]
    anomaly = fill_check.details["anomalies"][0]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert fill_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert fill_check.count == 1
    assert fill_check.details["anomaly_count"] == 1
    assert anomaly["sequence"] == fill.sequence
    assert anomaly["fill_id"] == "fill-1"
    assert anomaly["fill_id_present"] is True
    assert anomaly["order_id"] == "exchange-fill-place-1"
    assert anomaly["order_found"] is True
    assert anomaly["action_id_matches"] is False
    assert anomaly["expected_action_id"] == "fill-place-1"
    assert anomaly["client_order_id_matches"] is False
    assert anomaly["expected_client_order_id"] == "fill-place-1"
    assert anomaly["product_id_matches"] is False
    assert anomaly["expected_product_id"] == "BTC-USD"
    assert anomaly["side_matches"] is False
    assert anomaly["expected_side"] == OrderSide.BUY.value
    assert anomaly["side"] == OrderSide.SELL.value


def test_ledger_health_reports_exchange_state_contract_mismatch(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))
    balance = core.emit(
        EventType.EXCHANGE_BALANCE_SNAPSHOT,
        {
            "account_id": "account-1",
            "available": "10",
        },
    )
    position = core.emit(
        EventType.EXCHANGE_POSITION_SNAPSHOT,
        {
            "net_size": "not-decimal",
            "product_id": "BTC-PERP",
            "venue": ProductVenue.FCM.value,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    state_check = checks[LedgerHealthCheckName.EXCHANGE_STATE_CONTRACT]
    anomalies = {anomaly["event_type"]: anomaly for anomaly in state_check.details["anomalies"]}

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert state_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert state_check.count == 2
    assert state_check.details["anomaly_count"] == 2
    assert anomalies[EventType.EXCHANGE_BALANCE_SNAPSHOT.value]["sequence"] == balance.sequence
    assert anomalies[EventType.EXCHANGE_BALANCE_SNAPSHOT.value]["account_id"] == "account-1"
    assert set(anomalies[EventType.EXCHANGE_BALANCE_SNAPSHOT.value]["missing_fields"]) == {"currency", "venue"}
    assert anomalies[EventType.EXCHANGE_POSITION_SNAPSHOT.value]["sequence"] == position.sequence
    assert anomalies[EventType.EXCHANGE_POSITION_SNAPSHOT.value]["product_id"] == "BTC-PERP"
    assert set(anomalies[EventType.EXCHANGE_POSITION_SNAPSHOT.value]["invalid_fields"]) == {"net_size"}


def test_ledger_health_reports_attention_for_unresolved_risks(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "audit.jsonl"
    core = AuditCore(AuditLedger(ledger_path))

    core.emit(
        EventType.ERROR,
        {
            "error_category": ErrorCategory.RUNTIME_TASK.value,
            "message": "operator attention needed",
            "retryable": True,
        },
    )
    core.emit(
        EventType.ACTION_EXECUTION_FAILED,
        {
            "action_id": "place-1",
            "failure_reason": ActionFailureReason.EXECUTOR_ERROR.value,
        },
    )
    core.emit(
        EventType.RECONCILIATION_MISMATCH,
        {
            "action_id": "place-1",
            "reason": ReconciliationIssue.MISSING_EXECUTION_RESULT.value,
        },
    )
    core.emit(
        EventType.RECONCILIATION_DRIFT,
        {
            "drift_key": "position_size_drift:FCM:BIT-29MAY26-CDE:1:2",
            "issue": ReconciliationIssue.POSITION_SIZE_DRIFT.value,
            "product_id": "BIT-29MAY26-CDE",
            "venue": ProductVenue.FCM.value,
        },
    )
    core.emit(
        EventType.FEED_DEGRADED,
        {
            "connected_sources": [],
            "disconnected_sources": ["coinbase-primary"],
            "live_count": 0,
            "min_live_sources": 1,
            "stale_sources": [],
        },
    )
    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert checks[LedgerHealthCheckName.ERROR_EVENTS].count == 1
    assert checks[LedgerHealthCheckName.ERROR_EVENTS].details["by_category"] == {
        ErrorCategory.RUNTIME_TASK.value: 1
    }
    assert checks[LedgerHealthCheckName.ERROR_EVENTS].details["projection_parse_issue_count"] == 0
    assert checks[LedgerHealthCheckName.ERROR_EVENTS].details["retryable_count"] == 1
    assert checks[LedgerHealthCheckName.EXECUTION_UNCERTAINTY].count == 1
    assert checks[LedgerHealthCheckName.EXECUTION_UNCERTAINTY].details["failed_action_count"] == 1
    assert checks[LedgerHealthCheckName.FEED_DEGRADATION].count == 1
    assert checks[LedgerHealthCheckName.FEED_DEGRADATION].details["latest_disconnected_sources"] == [
        "coinbase-primary"
    ]
    assert checks[LedgerHealthCheckName.RECONCILIATION].count == 2
    assert checks[LedgerHealthCheckName.RECONCILIATION].details["exchange_state_drift_count"] == 1
    assert checks[LedgerHealthCheckName.RECONCILIATION].details["unresolved_mismatch_count"] == 1


def test_application_startup_audits_config_snapshot_and_fingerprint(workspace_tmp_path):
    config = CoinbaseApplicationConfig(
        ledger_path=workspace_tmp_path / "audit.jsonl",
        bot=CoinbaseBotConfig(
            websocket_sources=(
                CoinbaseWebSocketSourceConfig(
                    source_id="coinbase-market-primary",
                    channels=(CoinbaseWebSocketChannel.LEVEL2,),
                    endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
                    product_ids=("BTC-USD",),
                ),
            )
        ),
    )
    application = build_coinbase_application(config)

    asyncio.run(application.run(max_cycles=1))
    projection = SourceOfTruthProjection.from_ledger(application.ledger)
    startup_metadata = projection.system_starts[0].startup_metadata
    application_config = startup_metadata["application_config"]

    assert application_config["fingerprint"] == application_config_fingerprint(config)
    assert application_config["fingerprint_algorithm"] == CONFIG_FINGERPRINT_ALGORITHM
    assert application_config["schema_version"] == APPLICATION_CONFIG_SCHEMA_VERSION
    assert application_config["snapshot"] == application_config_snapshot(config)
    assert application_config["snapshot"]["bot"]["feed"]["min_live_sources"] == 1
    assert application_config["snapshot"]["bot"]["feed"]["stale_after_seconds"] == 30.0
    assert application_config["snapshot"]["bot"]["websocket_sources"][0]["channels"] == ["level2"]


def test_application_builds_configured_trigger_rules(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "configured-trigger-audit.jsonl"
    application = build_coinbase_application(
        CoinbaseApplicationConfig(
            ledger_path=ledger_path,
            bot=CoinbaseBotConfig(
                trigger_rules=(
                    MessageTriggerConfig(
                        trigger_id="on-data-accepted",
                        relation=TriggerRelation.ON,
                        event_type=EventType.DATA_ACCEPTED,
                    ),
                ),
            ),
        )
    )

    application.core.emit(EventType.DATA_ACCEPTED, {"message_key": "BTC-USD:1"})
    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [
        EventType.DATA_ACCEPTED,
        EventType.TRIGGER_FIRED,
    ]
    assert records[1].payload["trigger_id"] == "on-data-accepted"


def test_application_config_requires_path_and_bot_config():
    with pytest.raises(TypeError, match="pathlib.Path"):
        CoinbaseApplicationConfig(ledger_path="audit.jsonl")

    with pytest.raises(TypeError, match="CoinbaseApplicationConfig"):
        build_coinbase_application("not-config")


def test_application_build_audits_runtime_assembly_config_errors(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "build-error-audit.jsonl"
    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE)),
    )

    with pytest.raises(ValueError, match="token_provider"):
        build_coinbase_application(config)

    records = AuditLedger(ledger_path).iter_records()
    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["error_category"] == ErrorCategory.CONFIG.value
    assert records[0].payload["stage"] == "runtime_assembly"


def test_application_run_audits_unexpected_runtime_boundary_errors(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "run-error-audit.jsonl"

    async def failing_sleep(delay_seconds: float) -> None:
        raise RuntimeError(f"sleep failed after {delay_seconds}")

    config = CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            reconciliation=ReconciliationRuntimeConfig(
                watchdog_schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.WATCHDOG,
                    interval=timedelta(seconds=5),
                    enabled=True,
                    run_on_start=False,
                )
            )
        ),
    )
    application = build_coinbase_application(config, sleep=failing_sleep)

    with pytest.raises(RuntimeError, match="sleep failed"):
        asyncio.run(application.run(max_cycles=1))

    records = AuditLedger(ledger_path).iter_records()
    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_category"] == ErrorCategory.UNEXPECTED.value
    assert records[-1].payload["error_code"] == ErrorCode.UNEXPECTED_EXCEPTION.value
    assert records[-1].payload["stage"] == "application_run"


def test_cli_wrapper_runs_default_dry_run_cycle(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-audit.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                max_cycles=1,
            )
        )
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert ledger_path.exists()
    assert "completed_cycles=1" in output
    assert f"ledger_path={ledger_path}" in output


def test_cli_runtime_fail_on_attention_checks_ledger_health_after_run(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-runtime-health-audit.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                max_cycles=1,
                runtime_fail_on_attention=True,
            )
        )
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "completed_cycles=1" in output
    assert f"ledger_path={ledger_path}" in output
    assert f"runtime_health_status={LedgerHealthStatus.OK.value}" in output
    records = AuditLedger(ledger_path).iter_records()
    assert records[-1].event_type == EventType.RUNTIME_HEALTH_CHECK_RESULT
    assert records[-1].payload["checked_health_status"] == LedgerHealthStatus.OK.value
    assert records[-1].payload["attention_check_count"] == 0
    summary = summarize_ledger(ledger_path)
    assert summary.runtime_health_check_result_count == 1
    assert summary.latest_runtime_health_check_sequence == records[-1].sequence
    assert summary.latest_runtime_health_check_status == LedgerHealthStatus.OK.value


def test_cli_runtime_fail_on_attention_returns_attention_exit_code(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-runtime-attention-audit.jsonl"

    def fake_ledger_health_payload(path):
        assert path == ledger_path
        return {
            "checks": [
                {
                    "name": LedgerHealthCheckName.ERROR_EVENTS.value,
                    "status": LedgerHealthStatus.ATTENTION_REQUIRED.value,
                }
            ],
            "last_sequence": 3,
            "record_count": 3,
            "status": LedgerHealthStatus.ATTENTION_REQUIRED.value,
        }

    monkeypatch.setattr("app.main.ledger_health_payload", fake_ledger_health_payload)

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                max_cycles=1,
                runtime_fail_on_attention=True,
            )
        )
    )
    output = capsys.readouterr().out

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert "completed_cycles=1" in output
    assert f"runtime_health_status={LedgerHealthStatus.ATTENTION_REQUIRED.value}" in output
    records = AuditLedger(ledger_path).iter_records()
    assert records[-1].event_type == EventType.RUNTIME_HEALTH_CHECK_RESULT
    assert records[-1].payload["attention_check_count"] == 1
    assert records[-1].payload["attention_checks"] == [LedgerHealthCheckName.ERROR_EVENTS.value]
    assert records[-1].payload["checked_health_status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert records[-1].payload["checked_through_sequence"] == 3
    summary = summarize_ledger(ledger_path)
    assert summary.runtime_health_check_result_count == 1
    assert summary.latest_runtime_health_check_sequence == records[-1].sequence
    assert summary.latest_runtime_health_check_status == LedgerHealthStatus.ATTENTION_REQUIRED.value


def test_ledger_health_reports_malformed_runtime_health_check_result(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "malformed-runtime-health-result.jsonl"
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.RUNTIME_HEALTH_CHECK_RESULT,
        {
            "attention_check_count": 1,
            "attention_checks": [],
            "checked_health_status": LedgerHealthStatus.OK.value,
            "checked_through_sequence": -1,
            "record_count": -1,
            "schema_version": 99,
        },
    )

    health = ledger_health(ledger_path)
    checks = {check.name: check for check in health.checks}
    check = checks[LedgerHealthCheckName.RUNTIME_HEALTH_CHECK_RESULT_CONTRACT]

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert check.count == 1
    anomaly = check.details["anomalies"][0]
    assert anomaly["sequence"] == 1
    assert "ledger_path" in anomaly["missing_fields"]
    assert "schema_version" in anomaly["invalid_fields"]
    assert "checked_health_status" in anomaly["invalid_fields"]
    assert "attention_check_count" in anomaly["invalid_fields"]


def test_cli_run_forever_uses_unbounded_runtime_cycles():
    assert _runtime_max_cycles_from_args(argparse.Namespace(max_cycles=7, run_forever=False)) == 7
    assert _runtime_max_cycles_from_args(argparse.Namespace(max_cycles=7, run_forever=True)) is None
    assert (
        _runtime_max_cycles_from_args(
            argparse.Namespace(max_cycles=None, run_forever=False, stop_after_task=None)
        )
        == 1
    )


def test_cli_stop_after_task_uses_unbounded_runtime_by_default():
    args = argparse.Namespace(
        max_cycles=None,
        run_forever=False,
        stop_after_task=RuntimeTask.STRATEGY_EVALUATION.value,
        stop_after_task_count=2,
    )

    assert _runtime_max_cycles_from_args(args) is None
    assert _runtime_stop_after_task_from_args(args) == RuntimeTask.STRATEGY_EVALUATION
    assert _runtime_stop_after_task_count_from_args(args) == 2


def test_cli_stop_after_task_validation_requires_enabled_schedule():
    config = default_coinbase_application_config()

    _validate_runtime_stop_after_task(config, RuntimeTask.WATCHDOG)
    with pytest.raises(ValueError, match="not enabled"):
        _validate_runtime_stop_after_task(config, RuntimeTask.STRATEGY_EVALUATION)


def test_cli_stop_after_task_count_must_be_positive():
    with pytest.raises(ValueError, match="positive"):
        _runtime_stop_after_task_count_from_args(argparse.Namespace(stop_after_task_count=0))


def test_cli_ledger_summary_verifies_without_running_runtime(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-summary-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                ledger_summary=True,
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()

    assert exit_code == 0
    assert payload["verified"] is True
    assert payload["record_count"] == record_count
    assert payload["latest_config_fingerprint"] == application_config_fingerprint(application.config)
    assert len(replayed_records) == record_count
    assert "completed_cycles=" not in output


def test_cli_ledger_health_verifies_without_running_runtime(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-health-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_health=True,
                ledger_path=str(ledger_path),
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()

    assert exit_code == 0
    assert payload["verified"] is True
    assert payload["status"] == LedgerHealthStatus.OK.value
    assert payload["record_count"] == record_count
    assert len(replayed_records) == record_count
    assert "completed_cycles=" not in output


def test_cli_ledger_health_can_fail_on_attention(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-health-attention-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_ledger_checkpoint(ledger_path)

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_health=True,
                ledger_health_fail_on_attention=True,
                ledger_path=str(ledger_path),
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == LedgerHealthStatus.ATTENTION_REQUIRED.value
    assert "completed_cycles=" not in output


def test_cli_ledger_health_can_verify_s3_object_lock_receipts(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-health-s3-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    checkpoint = record_ledger_checkpoint(ledger_path)
    client = CliFakeS3ObjectLockClient()
    publish_recorded_ledger_checkpoint_anchor(
        ledger_path,
        checkpoint,
        S3ObjectLockLedgerAnchorStore(
            S3ObjectLockAnchorConfig(
                bucket="audit-bucket",
                immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
                retention_period=timedelta(days=2555),
            ),
            s3_client=client,
        ),
    )
    verified_artifact_uris: list[str] = []

    def verify_receipt(receipt):
        verified_artifact_uris.append(receipt.artifact_uri)
        return {
            "artifact_uri": receipt.artifact_uri,
            "object_content_verified": True,
            "retention_verified": True,
        }

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_health=True,
                ledger_health_max_records_after_anchor=None,
                ledger_health_verify_s3_anchors=True,
                ledger_path=str(ledger_path),
                max_cycles=99,
            ),
            s3_anchor_receipt_verifier=verify_receipt,
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}

    assert exit_code == 0
    assert payload["status"] == LedgerHealthStatus.OK.value
    assert checks[LedgerHealthCheckName.ANCHOR_REMOTE_VERIFICATION.value]["details"]["enabled"] is True
    assert checks[LedgerHealthCheckName.ANCHOR_REMOTE_VERIFICATION.value]["details"]["verified_count"] == 1
    assert verified_artifact_uris == [checks[LedgerHealthCheckName.ANCHOR_REMOTE_VERIFICATION.value]["details"]["verified"][0]["artifact_uri"]]
    assert "completed_cycles=" not in output


def test_cli_ledger_health_can_verify_s3_object_lock_archives(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-health-s3-archive-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    client = CliFakeS3ObjectLockClient()
    publish_ledger_archive(
        ledger_path,
        S3ObjectLockLedgerArchiveStore(
            config=S3ObjectLockLedgerArchiveConfig(
                bucket="audit-bucket",
                immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
                retention_period=timedelta(days=2555),
            ),
            s3_client=client,
        ),
    )
    verified_artifact_uris: list[str] = []

    def verify_receipt(receipt):
        verified_artifact_uris.append(receipt.artifact_uri)
        return {
            "artifact_uri": receipt.artifact_uri,
            "object_content_verified": True,
            "retention_verified": True,
        }

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_health=True,
                ledger_health_max_records_after_anchor=None,
                ledger_health_max_records_after_archive=None,
                ledger_health_verify_s3_anchors=False,
                ledger_health_verify_s3_archives=True,
                ledger_path=str(ledger_path),
                max_cycles=99,
            ),
            s3_archive_receipt_verifier=verify_receipt,
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    checks = {check["name"]: check for check in payload["checks"]}
    remote_check = checks[LedgerHealthCheckName.ARCHIVE_REMOTE_VERIFICATION.value]

    assert exit_code == 0
    assert payload["status"] == LedgerHealthStatus.OK.value
    assert remote_check["details"]["enabled"] is True
    assert remote_check["details"]["verified_count"] == 1
    assert verified_artifact_uris == [remote_check["details"]["verified"][0]["artifact_uri"]]
    assert "completed_cycles=" not in output


def test_ledger_health_reports_s3_anchor_remote_verification_failure(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "health-s3-failure-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    checkpoint = record_ledger_checkpoint(ledger_path)
    publish_recorded_ledger_checkpoint_anchor(
        ledger_path,
        checkpoint,
        S3ObjectLockLedgerAnchorStore(
            S3ObjectLockAnchorConfig(
                bucket="audit-bucket",
                immutability_mode=AnchorImmutabilityMode.COMPLIANCE,
                retention_period=timedelta(days=2555),
            ),
            s3_client=CliFakeS3ObjectLockClient(),
        ),
    )

    def failing_verifier(receipt):
        raise LedgerAnchorError(f"remote verification failed for {receipt.artifact_uri}")

    health = ledger_health(ledger_path, anchor_receipt_verifier=failing_verifier)
    checks = {check.name: check for check in health.checks}

    assert health.status == LedgerHealthStatus.ATTENTION_REQUIRED
    remote_check = checks[LedgerHealthCheckName.ANCHOR_REMOTE_VERIFICATION]
    assert remote_check.status == LedgerHealthStatus.ATTENTION_REQUIRED
    assert remote_check.count == 1
    assert remote_check.details["failure_count"] == 1
    assert remote_check.details["failures"][0]["exception_type"] == "LedgerAnchorError"


def test_cli_ledger_export_verifies_and_prints_records_without_running_runtime(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-export-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_export=True,
                ledger_path=str(ledger_path),
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()

    assert exit_code == 0
    assert payload["ledger"]["verified"] is True
    assert payload["ledger"]["record_count"] == record_count
    assert payload["digest_algorithm"] == DigestAlgorithm.SHA256.value
    assert len(payload["export_digest"]) == 64
    assert payload["records"][0]["event_type"] == EventType.SYSTEM_STARTED.value
    assert payload["records"][-1]["event_type"] == EventType.SYSTEM_STOPPED.value
    assert len(payload["records"]) == record_count
    assert len(replayed_records) == record_count
    assert "completed_cycles=" not in output


def test_cli_source_of_truth_replays_projection_without_running_runtime(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-source-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_path=str(ledger_path),
                max_cycles=99,
                source_of_truth=True,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()

    assert exit_code == 0
    assert payload["ledger"]["verified"] is True
    assert payload["ledger"]["record_count"] == record_count
    assert payload["projection"]["runtime_tasks"][RuntimeTask.WATCHDOG.value]["completed_count"] == 1
    assert len(replayed_records) == record_count
    assert "completed_cycles=" not in output


def test_cli_ledger_checkpoint_appends_checkpoint_without_running_runtime(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-checkpoint-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_checkpoint=True,
                ledger_path=str(ledger_path),
                ledger_summary=False,
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()
    summary = summarize_ledger(ledger_path)

    assert exit_code == 0
    assert payload["audit_record_sequence"] == record_count + 1
    assert payload["checkpoint"]["record_count"] == record_count
    assert payload["checkpoint"]["through_sequence"] == record_count
    assert replayed_records[-1].event_type == EventType.AUDIT_CHECKPOINT
    assert len(replayed_records) == record_count + 1
    assert summary.audit_checkpoint_count == 1
    assert "completed_cycles" not in output


def test_cli_ledger_anchor_writes_artifact_and_audits_receipt_without_running_runtime(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-anchor-audit.jsonl"
    anchor_dir = workspace_tmp_path / "anchors"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_anchor_dir=str(anchor_dir),
                ledger_checkpoint=False,
                ledger_path=str(ledger_path),
                ledger_summary=False,
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()
    summary = summarize_ledger(ledger_path)

    assert exit_code == 0
    assert payload["checkpoint"]["audit_record_sequence"] == record_count + 1
    assert payload["anchor"]["audit_record_sequence"] == record_count + 2
    assert Path(payload["anchor"]["receipt"]["artifact_uri"]).exists()
    assert replayed_records[-2].event_type == EventType.AUDIT_CHECKPOINT
    assert replayed_records[-1].event_type == EventType.AUDIT_ANCHOR_PUBLISHED
    assert summary.audit_checkpoint_count == 1
    assert summary.audit_anchor_count == 1
    assert "completed_cycles" not in output


def test_cli_ledger_anchor_latest_checkpoint_does_not_append_new_checkpoint(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-anchor-latest-audit.jsonl"
    anchor_dir = workspace_tmp_path / "anchors"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    checkpoint = record_ledger_checkpoint(ledger_path)
    record_count = len(AuditLedger(ledger_path).iter_records())

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_anchor_dir=str(anchor_dir),
                ledger_anchor_latest_checkpoint=True,
                ledger_checkpoint=False,
                ledger_path=str(ledger_path),
                ledger_summary=False,
                max_cycles=99,
            )
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()
    summary = summarize_ledger(ledger_path)

    assert exit_code == 0
    assert payload["checkpoint"]["audit_record_sequence"] == checkpoint.audit_record_sequence
    assert payload["checkpoint"]["checkpoint"]["checkpoint_hash"] == checkpoint.checkpoint.checkpoint_hash
    assert payload["anchor"]["audit_record_sequence"] == record_count + 1
    assert Path(payload["anchor"]["receipt"]["artifact_uri"]).exists()
    assert replayed_records[-2].event_type == EventType.AUDIT_CHECKPOINT
    assert replayed_records[-1].event_type == EventType.AUDIT_ANCHOR_PUBLISHED
    assert summary.audit_checkpoint_count == 1
    assert summary.audit_anchor_count == 1
    assert "completed_cycles" not in output


def test_cli_ledger_s3_anchor_writes_object_lock_receipt_without_running_runtime(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-s3-anchor-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())
    client = CliFakeS3ObjectLockClient()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_anchor_dir=None,
                ledger_anchor_s3_bucket="audit-bucket",
                ledger_anchor_s3_expected_bucket_owner="123456789012",
                ledger_anchor_s3_mode=AnchorImmutabilityMode.COMPLIANCE.value,
                ledger_anchor_s3_prefix="staterail/anchors",
                ledger_anchor_s3_retention_days=2555,
                ledger_checkpoint=False,
                ledger_path=str(ledger_path),
                ledger_summary=False,
                max_cycles=99,
            ),
            s3_anchor_store_factory=lambda config: S3ObjectLockLedgerAnchorStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()
    summary = summarize_ledger(ledger_path)

    assert exit_code == 0
    assert payload["checkpoint"]["audit_record_sequence"] == record_count + 1
    assert payload["anchor"]["audit_record_sequence"] == record_count + 2
    assert payload["anchor"]["receipt"]["artifact_uri"].startswith("s3://audit-bucket/staterail/anchors/")
    assert payload["anchor"]["receipt"]["immutability_mode"] == AnchorImmutabilityMode.COMPLIANCE.value
    assert payload["anchor"]["receipt"]["store_metadata"]["provider"] == "aws_s3_object_lock"
    assert payload["anchor"]["receipt"]["store_type"] == AnchorStoreType.WORM_OBJECT.value
    assert payload["anchor"]["receipt"]["version_id"] == "cli-version-1"
    assert client.get_bucket_versioning_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.get_object_lock_configuration_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.put_object_calls[0]["Bucket"] == "audit-bucket"
    assert client.put_object_calls[0]["ExpectedBucketOwner"] == "123456789012"
    assert client.put_object_calls[0]["ObjectLockMode"] == "COMPLIANCE"
    assert client.get_object_calls[0]["Bucket"] == "audit-bucket"
    assert client.get_object_calls[0]["ExpectedBucketOwner"] == "123456789012"
    assert client.get_object_calls[0]["VersionId"] == "cli-version-1"
    assert replayed_records[-2].event_type == EventType.AUDIT_CHECKPOINT
    assert replayed_records[-1].event_type == EventType.AUDIT_ANCHOR_PUBLISHED
    assert summary.audit_checkpoint_count == 1
    assert summary.audit_anchor_count == 1
    assert "completed_cycles" not in output


def test_cli_ledger_s3_archive_writes_object_lock_receipt_without_running_runtime(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cli-s3-archive-audit.jsonl"
    application = build_coinbase_application(default_coinbase_application_config(ledger_path=ledger_path))
    asyncio.run(application.run(max_cycles=1))
    record_count = len(application.ledger.iter_records())
    client = CliFakeS3ObjectLockClient()

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=None,
                ledger_archive_s3_bucket="audit-bucket",
                ledger_archive_s3_expected_bucket_owner="123456789012",
                ledger_archive_s3_mode=AnchorImmutabilityMode.COMPLIANCE.value,
                ledger_archive_s3_prefix="staterail/ledger-archives",
                ledger_archive_s3_retention_days=2555,
                ledger_path=str(ledger_path),
                max_cycles=99,
            ),
            s3_archive_store_factory=lambda config: S3ObjectLockLedgerArchiveStore(
                config,
                s3_client=client,
            ),
        )
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    replayed_records = AuditLedger(ledger_path).iter_records()
    summary = summarize_ledger(ledger_path)

    assert exit_code == 0
    assert payload["archive"]["audit_record_sequence"] == record_count + 1
    assert payload["archive"]["receipt"]["artifact_uri"].startswith(
        "s3://audit-bucket/staterail/ledger-archives/"
    )
    assert payload["archive"]["receipt"]["immutability_mode"] == AnchorImmutabilityMode.COMPLIANCE.value
    assert payload["archive"]["receipt"]["record_count"] == record_count
    assert payload["archive"]["receipt"]["store_metadata"]["provider"] == "aws_s3_object_lock"
    assert payload["archive"]["receipt"]["store_type"] == AnchorStoreType.WORM_OBJECT.value
    assert payload["archive"]["receipt"]["through_sequence"] == record_count
    assert payload["archive"]["receipt"]["version_id"] == "cli-version-1"
    assert client.get_bucket_versioning_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.get_object_lock_configuration_calls == [
        {"Bucket": "audit-bucket", "ExpectedBucketOwner": "123456789012"}
    ]
    assert client.put_object_calls[0]["Bucket"] == "audit-bucket"
    assert client.put_object_calls[0]["ContentType"] == "application/x-ndjson"
    assert client.put_object_calls[0]["ExpectedBucketOwner"] == "123456789012"
    assert client.put_object_calls[0]["Key"].startswith("staterail/ledger-archives/")
    assert client.put_object_calls[0]["ObjectLockMode"] == "COMPLIANCE"
    assert client.get_object_calls[0]["VersionId"] == "cli-version-1"
    assert replayed_records[-1].event_type == EventType.AUDIT_LEDGER_ARCHIVED
    assert summary.audit_archive_count == 1
    assert "completed_cycles" not in output


def test_cli_rejects_latest_checkpoint_anchor_without_anchor_target(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)

    with pytest.raises(ValueError, match="requires an anchor target"):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_anchor_dir=None,
                    ledger_anchor_latest_checkpoint=True,
                    ledger_checkpoint=False,
                    ledger_path=str(workspace_tmp_path / "audit.jsonl"),
                    max_cycles=99,
                )
            )
        )


def test_cli_rejects_latest_checkpoint_anchor_with_checkpoint_command(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)

    with pytest.raises(ValueError, match="cannot be combined with --ledger-checkpoint"):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_anchor_dir=str(workspace_tmp_path / "anchors"),
                    ledger_anchor_latest_checkpoint=True,
                    ledger_checkpoint=True,
                    ledger_path=str(workspace_tmp_path / "audit.jsonl"),
                    max_cycles=99,
                )
            )
        )


def test_cli_rejects_combined_local_and_s3_anchor_targets(workspace_tmp_path, monkeypatch):
    _clear_coinbase_env(monkeypatch)

    with pytest.raises(ValueError, match="cannot be combined"):
        asyncio.run(
            run_from_args(
                argparse.Namespace(
                    config_file=None,
                    ledger_anchor_dir=str(workspace_tmp_path / "anchors"),
                    ledger_anchor_s3_bucket="audit-bucket",
                    ledger_anchor_s3_expected_bucket_owner=None,
                    ledger_anchor_s3_mode=AnchorImmutabilityMode.GOVERNANCE.value,
                    ledger_anchor_s3_prefix="audit-anchors",
                    ledger_anchor_s3_retention_days=1,
                    ledger_checkpoint=False,
                    ledger_path=str(workspace_tmp_path / "audit.jsonl"),
                    max_cycles=99,
                )
            )
        )


def test_cli_wrapper_loads_json_config_file(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "config-audit.jsonl"
    config_path = workspace_tmp_path / "bot-config.json"
    config_path.write_text(
        json.dumps({"ledger_path": str(ledger_path)}),
        encoding="utf-8",
    )

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                max_cycles=1,
            )
        )
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert ledger_path.exists()
    assert f"ledger_path={ledger_path}" in output


def test_cli_runs_checked_in_dry_run_example_with_ledger_override(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "example-dry-run-audit.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.dry-run.json",
                ledger_path=str(ledger_path),
                max_cycles=1,
            )
        )
    )
    output = capsys.readouterr().out
    records = AuditLedger(ledger_path).iter_records()
    event_types = [record.event_type for record in records]

    assert exit_code == 0
    assert ledger_path.exists()
    assert f"ledger_path={ledger_path}" in output
    assert EventType.SYSTEM_STARTED in event_types
    assert EventType.RUNTIME_TASK_STARTED in event_types
    assert EventType.RUNTIME_TASK_COMPLETED in event_types
    assert EventType.SYSTEM_STOPPED in event_types


def test_cli_readiness_checks_checked_in_cfm_live_example_without_creating_ledger(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "cfm-live-readiness.jsonl"

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file="docs/examples/config.cfm-live.json",
                ledger_path=str(ledger_path),
                max_cycles=99,
                readiness=True,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)
    checks = {check["name"]: check for check in payload["checks"]}

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert checks[ReadinessCheckName.CONFIG_PLACEHOLDERS.value]["status"] == (
        ReadinessStatus.ATTENTION_REQUIRED.value
    )
    assert checks[ReadinessCheckName.CREDENTIALS.value]["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert checks[ReadinessCheckName.LIVE_TRADING_APPROVAL.value]["status"] == (
        ReadinessStatus.ATTENTION_REQUIRED.value
    )
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES.value]["status"] == ReadinessStatus.OK.value
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES.value]["details"]["source_count"] == 4
    assert checks[ReadinessCheckName.WEBSOCKET_SOURCES.value]["details"]["single_source_scope_count"] == 0
    assert not ledger_path.exists()


def test_cli_readiness_accepts_cfm_live_example_after_operator_values(
    workspace_tmp_path,
    capsys,
    monkeypatch,
):
    _clear_coinbase_env(monkeypatch)
    config_path = workspace_tmp_path / "cfm-live-ready.json"
    ledger_path = workspace_tmp_path / "cfm-live-ready.jsonl"
    _write_cfm_live_config_with_operator_values(config_path, ledger_path=ledger_path)
    monkeypatch.setenv(COINBASE_API_KEY_NAME_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_API_PRIVATE_KEY_ENV, "private-key")
    monkeypatch.setenv(LIVE_TRADING_APPROVAL_ENV, "true")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                max_cycles=99,
                readiness=True,
                readiness_fail_on_attention=True,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == ReadinessStatus.OK.value
    assert all(check["status"] == ReadinessStatus.OK.value for check in payload["checks"])
    assert not ledger_path.exists()


def test_application_config_rejects_directory_path(workspace_tmp_path):
    with pytest.raises(ValueError, match="file name"):
        CoinbaseApplicationConfig(ledger_path=Path())


def test_ledger_summary_requires_existing_ledger(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "missing.jsonl"

    with pytest.raises(FileNotFoundError, match="Ledger does not exist"):
        summarize_ledger(ledger_path)

    assert not ledger_path.exists()


def _emit_live_accepted_order(
    core: AuditCore,
    *,
    action_id: str,
    product_id: str,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
) -> None:
    resolved_client_order_id = client_order_id or action_id
    command = PlaceOrderIntent(
        action_id=action_id,
        product_id=product_id,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        size="1",
        limit_price="50000",
        idempotency_key=client_order_id,
    ).to_command()
    requested = core.emit(EventType.ACTION_REQUESTED, command.to_payload())
    accepted = core.emit(
        EventType.ACTION_ACCEPTED,
        {
            "action_id": action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    execution_started = core.emit(
        EventType.ACTION_EXECUTION_STARTED,
        {
            "accepted_sequence": accepted.sequence,
            "action_id": action_id,
            "action_type": command.action_type.value,
            "requested_sequence": requested.sequence,
        },
    )
    core.emit(
        EventType.ACTION_EXECUTED,
        {
            "action_id": action_id,
            "action_type": command.action_type.value,
            "execution_result": {
                "action_id": action_id,
                "action_type": command.action_type.value,
                "client_order_id": resolved_client_order_id,
                "exchange_order_id": exchange_order_id or f"exchange-{action_id}",
                "mode": ExecutionMode.LIVE.value,
                "raw_response": {},
                "retryable": False,
                "status": ExecutionStatus.ACCEPTED.value,
            },
            "execution_started_sequence": execution_started.sequence,
            "requested_sequence": requested.sequence,
        },
    )


def _clear_coinbase_env(monkeypatch) -> None:
    for key in list(os.environ):
        if (
            key.startswith("STATERAIL_")
            or key.startswith("COINBASE_BOT_")
            or key in {COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV}
        ):
            monkeypatch.delenv(key, raising=False)


def _write_cfm_live_config_with_operator_values(path: Path, *, ledger_path: Path) -> None:
    render_config_template(
        "docs/examples/config.cfm-live.json",
        path,
        ledger_path=ledger_path,
        replacements={"REPLACE_WITH_CFM_PRODUCT_IDS": "BIT-29MAY26-CDE"},
    )


class IdleFeedSource:
    def __init__(self, source_id: str) -> None:
        self._source_id = source_id

    @property
    def source_id(self) -> str:
        return self._source_id

    async def stream(self) -> AsyncIterator[FeedMessage]:
        await asyncio.Event().wait()
        if False:
            yield FeedMessage(self._source_id, "never", EventType.DATA_RECEIVED, {})
