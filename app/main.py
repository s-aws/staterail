from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from app.audit_archive_store import (
    S3ArchiveStoreFactory,
    s3_object_lock_archive_config_from_store_config,
)
from app.audit_anchor_store import (
    S3AnchorStoreFactory,
    ledger_anchor_store_from_config,
    s3_object_lock_anchor_config_from_store_config,
)
from app.bootstrap import (
    CoinbaseApplicationConfig,
    build_coinbase_application,
    default_coinbase_application_config,
)
from app.config_loading import (
    has_coinbase_application_env,
    load_coinbase_application_config_from_env,
    load_coinbase_application_config_from_json_file,
)
from app.credentials import load_coinbase_runtime_credentials_from_env
from app.exchange_state_smoke import exchange_state_smoke_payload
from app.feed_smoke import feed_smoke_payload
from app.ledger_health_acknowledgement import acknowledge_ledger_health
from app.ledger_health import AnchorReceiptVerifier, ArchiveReceiptVerifier, ledger_health_payload
from app.ledger_export import ledger_export_payload
from app.ledger_summary import ledger_summary_payload
from app.live_no_order_preflight import live_no_order_preflight_payload
from app.live_runtime_gate import enforce_live_runtime_gate, live_runtime_gate_payload
from app.live_safety import live_trading_approved_from_env
from app.product_catalog_smoke import product_catalog_smoke_payload
from app.readiness import ReadinessCheckResult, readiness_payload
from app.source_of_truth import source_of_truth_payload
from app.operator_policy_scenarios import operator_policy_scenarios_payload
from app.operator_actions import (
    operator_cancel_all_open_orders_payload,
    operator_cancel_order_payload,
    operator_canary_evidence_payload,
    operator_lookup_order_payload,
    operator_open_orders_payload,
    operator_place_order_payload,
    record_operator_canary_evidence_result,
)
from app.operator_canary import (
    operator_canary_plan_payload,
    render_operator_canary_dry_run_config,
)
from app.strategy_scenario import strategy_scenario_payload
from app.strategy_simulation_gate import (
    record_strategy_simulation_result,
)
from app.strategy_simulation import strategy_simulation_payload
from app.venue_contract import venue_contract_report_payload
from audit.anchors import LedgerAnchorReceipt, LedgerAnchorStore, publish_recorded_ledger_checkpoint_anchor
from audit.archives import LedgerArchiveReceipt, LedgerArchiveStore, publish_ledger_archive
from audit.checkpoints import latest_recorded_ledger_checkpoint, record_ledger_checkpoint
from audit.ledger import AuditLedger
from audit.s3_object_lock import (
    S3ObjectLockAnchorConfig,
    S3ObjectLockLedgerArchiveConfig,
    S3ObjectLockLedgerArchiveStore,
    S3ObjectLockLedgerAnchorStore,
    verify_s3_object_lock_anchor_receipt,
    verify_s3_object_lock_ledger_archive_receipt,
)
from config.assembly import AuditAnchorStoreConfig, WebSocketSourceFactory
from core.engine import AuditCore
from core.enums import (
    AnchorImmutabilityMode,
    ErrorCategory,
    ErrorCode,
    EventType,
    LedgerHealthStatus,
    LedgerAnchorStoreProvider,
    MarginType,
    OrderSide,
    OrderType,
    ReadinessCheckName,
    ReadinessRequirement,
    ReadinessStatus,
    RuntimeTask,
    StrategySimulationStatus,
    StrategyHelperStatus,
    TimeInForce,
    VenueContractRequirementSet,
)
from core.errors import exception_to_error_payload
from core.errors import ConfigError
from core.json_tools import JsonValue
from exchanges.coinbase.advanced_trade_rest import HttpTransport
from products.replay import product_catalog_from_projection
from products.tasks import ProductCatalogLookup
from projections.state import SourceOfTruthProjection


ATTENTION_REQUIRED_EXIT_CODE = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the StateRail runtime.")
    parser.add_argument(
        "--config-file",
        default=None,
        help="Optional JSON config file to load before CLI overrides.",
    )
    parser.add_argument(
        "--ledger-path",
        default=None,
        help="Path to the append-only audit ledger JSONL file.",
    )
    parser.add_argument(
        "--max-cycles",
        default=None,
        type=int,
        help=(
            "Maximum runtime task cycles to execute. Defaults to 1 unless "
            "--run-forever or --stop-after-task is used."
        ),
    )
    parser.add_argument(
        "--run-forever",
        action="store_true",
        help="Run runtime tasks continuously until the process is stopped.",
    )
    parser.add_argument(
        "--runtime-fail-on-attention",
        action="store_true",
        help=(
            "Run ledger health after runtime stops and return an attention exit code "
            "when the resulting ledger health is not ok."
        ),
    )
    parser.add_argument(
        "--stop-after-task",
        choices=tuple(task.value for task in RuntimeTask),
        default=None,
        help=(
            "Run until the selected scheduled task has executed the requested number "
            "of times. Useful for bounded live strategy checks such as strategies.evaluate."
        ),
    )
    parser.add_argument(
        "--stop-after-task-count",
        default=1,
        type=int,
        help="Number of selected task executions required by --stop-after-task.",
    )
    parser.add_argument(
        "--readiness",
        action="store_true",
        help="Print a read-only runtime readiness report without running the bot.",
    )
    parser.add_argument(
        "--readiness-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when the readiness report requires attention.",
    )
    parser.add_argument(
        "--readiness-allow-config-fingerprint-mismatch",
        action="store_true",
        help=(
            "Allow the readiness config-fingerprint check to pass when the existing ledger's "
            "latest startup fingerprint differs from the current config. Intended for explicit "
            "operator-mode transitions on the same ledger; all other readiness checks still apply."
        ),
    )
    parser.add_argument(
        "--ledger-export",
        action="store_true",
        help="Verify and export all ledger records without running the bot.",
    )
    parser.add_argument(
        "--ledger-summary",
        action="store_true",
        help="Verify and summarize the ledger without running the bot.",
    )
    parser.add_argument(
        "--ledger-health",
        action="store_true",
        help="Verify the ledger and print trust posture checks without running the bot.",
    )
    parser.add_argument(
        "--ledger-health-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when ledger health requires attention.",
    )
    parser.add_argument(
        "--ledger-health-acknowledge",
        action="store_true",
        help=(
            "Append an operator acknowledgement for the current ledger-health attention state "
            "without starting runtime tasks."
        ),
    )
    parser.add_argument(
        "--ledger-health-acknowledged-by",
        default=None,
        help="Operator identifier required with --ledger-health-acknowledge.",
    )
    parser.add_argument(
        "--ledger-health-acknowledgement-reason",
        default=None,
        help="Operator review reason required with --ledger-health-acknowledge.",
    )
    parser.add_argument(
        "--ledger-health-max-records-after-anchor",
        default=None,
        type=int,
        help="Optional health policy limit for ledger records after the latest anchored checkpoint.",
    )
    parser.add_argument(
        "--ledger-health-verify-s3-anchors",
        action="store_true",
        help="Read and verify S3 Object Lock anchor object versions and retention during ledger health.",
    )
    parser.add_argument(
        "--ledger-health-max-records-after-archive",
        default=None,
        type=int,
        help="Optional health policy limit for ledger records after the latest archived ledger prefix.",
    )
    parser.add_argument(
        "--ledger-health-verify-s3-archives",
        action="store_true",
        help="Read and verify S3 Object Lock ledger archive object versions and retention during ledger health.",
    )
    parser.add_argument(
        "--source-of-truth",
        action="store_true",
        help="Verify the ledger and print the replayed source-of-truth projection without running the bot.",
    )
    parser.add_argument(
        "--venue-contract-report",
        action="store_true",
        help="Print a read-only venue capability contract report without running the bot.",
    )
    parser.add_argument(
        "--venue-contract-venue",
        default=None,
        help="Venue to inspect with --venue-contract-report, such as CBE, FCM, INTX, or coinbase_cfm.",
    )
    parser.add_argument(
        "--venue-contract-requirement-set",
        choices=[requirement_set.value for requirement_set in VenueContractRequirementSet],
        default=VenueContractRequirementSet.LIVE_ORDER_ROUTING.value,
        help="Requirement set to check with --venue-contract-report.",
    )
    parser.add_argument(
        "--venue-contract-fail-on-missing",
        action="store_true",
        help="Return a non-zero exit code when the venue contract report is not ok.",
    )
    parser.add_argument(
        "--strategy-simulate",
        action="store_true",
        help="Verify the ledger and run configured strategies in read-only simulation mode.",
    )
    parser.add_argument(
        "--strategy-simulate-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when strategy simulation fails or previews rejected actions.",
    )
    parser.add_argument(
        "--strategy-simulate-record-result",
        action="store_true",
        help=(
            "Append compact strategy simulation qualification evidence after --strategy-simulate. "
            "The simulation itself remains read-only and does not execute orders."
        ),
    )
    parser.add_argument(
        "--strategy-scenario-file",
        default=None,
        help=(
            "Run a typed strategy scenario fixture against configured strategies. "
            "Requires --ledger-path for the temporary scenario ledger."
        ),
    )
    parser.add_argument(
        "--operator-policy-scenarios-file",
        default=None,
        help=(
            "Run a checked operator-policy scenario fixture without starting runtime "
            "or writing to the ledger."
        ),
    )
    parser.add_argument(
        "--operator-open-orders",
        action="store_true",
        help="List tracked open orders from the verified source-of-truth projection without writing to the ledger.",
    )
    parser.add_argument(
        "--operator-open-orders-product-id",
        default=None,
        help="Optional product filter for --operator-open-orders.",
    )
    parser.add_argument(
        "--operator-lookup-order",
        action="store_true",
        help=(
            "Look up one exchange order directly through the configured venue lookup client "
            "and record the returned exchange order update in the ledger."
        ),
    )
    parser.add_argument(
        "--operator-lookup-exchange-order-id",
        default=None,
        help="Exchange order ID to inspect with --operator-lookup-order.",
    )
    parser.add_argument(
        "--operator-lookup-reason",
        default=None,
        help="Operator reason recorded with --operator-lookup-order.",
    )
    parser.add_argument(
        "--operator-canary-plan",
        action="store_true",
        help=(
            "Print a read-only controlled canary command plan using the existing "
            "operator place, inspect, cancel, replay, and health commands."
        ),
    )
    parser.add_argument(
        "--operator-canary-dry-run-config-file",
        default=None,
        help=(
            "Dry-run config file used by --operator-canary-plan before the live "
            "config from --config-file is used."
        ),
    )
    parser.add_argument(
        "--operator-canary-evidence",
        action="store_true",
        help=(
            "Replay the ledger and print compact post-canary evidence for one operator "
            "place/cancel lifecycle without writing to the ledger."
        ),
    )
    parser.add_argument(
        "--operator-canary-evidence-action-id",
        default=None,
        help="Optional place action ID to inspect with --operator-canary-evidence.",
    )
    parser.add_argument(
        "--operator-canary-evidence-exchange-order-id",
        default=None,
        help="Optional exchange order ID to inspect with --operator-canary-evidence.",
    )
    parser.add_argument(
        "--operator-canary-evidence-product-id",
        default=None,
        help="Optional product filter for --operator-canary-evidence.",
    )
    parser.add_argument(
        "--operator-canary-evidence-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when post-canary evidence requires attention.",
    )
    parser.add_argument(
        "--operator-canary-evidence-record-result",
        action="store_true",
        help=(
            "Append the compact post-canary evidence result to the audit ledger. "
            "Without this flag, --operator-canary-evidence remains read-only."
        ),
    )
    parser.add_argument(
        "--operator-canary-render-dry-run-config",
        action="store_true",
        help=(
            "Render an isolated dry-run canary config from --config-file without "
            "starting runtime tasks, writing the ledger, or calling order endpoints."
        ),
    )
    parser.add_argument(
        "--operator-canary-dry-run-ledger-path",
        default=None,
        help="Dry-run ledger path to write into the rendered canary config.",
    )
    parser.add_argument(
        "--operator-canary-dry-run-config-force",
        action="store_true",
        help="Overwrite the target --operator-canary-dry-run-config-file when rendering.",
    )
    parser.add_argument(
        "--operator-place-order",
        action="store_true",
        help=(
            "Submit one audited operator limit order through the action gateway and configured executor "
            "without starting scheduled runtime tasks."
        ),
    )
    parser.add_argument(
        "--operator-place-action-id",
        default=None,
        help="Optional explicit action_id for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-client-order-id",
        default=None,
        help="Optional explicit client order ID/idempotency key for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-product-id",
        default=None,
        help="Product ID for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-side",
        choices=[side.value for side in OrderSide],
        default=None,
        help="Order side for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-size",
        default=None,
        help="Order size for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-limit-price",
        default=None,
        help="Limit price for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-leverage",
        default=None,
        help="Optional leverage value for futures --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-order-type",
        choices=[order_type.value for order_type in OrderType],
        default=None,
        help="Order type for --operator-place-order. Operator placement currently accepts limit only.",
    )
    parser.add_argument(
        "--operator-place-time-in-force",
        choices=[time_in_force.value for time_in_force in TimeInForce],
        default=None,
        help="Time in force for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-margin-type",
        choices=[margin_type.value for margin_type in MarginType],
        default=None,
        help="Optional margin type for futures --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-post-only",
        action="store_true",
        help="Set post_only=true for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-reduce-only",
        action="store_true",
        help="Set reduce_only=true for --operator-place-order.",
    )
    parser.add_argument(
        "--operator-place-reason",
        default=None,
        help="Operator reason recorded in the audited place-order request metadata.",
    )
    parser.add_argument(
        "--operator-cancel-order",
        action="store_true",
        help=(
            "Submit an audited operator cancel through the action gateway and configured executor "
            "without starting scheduled runtime tasks."
        ),
    )
    parser.add_argument(
        "--operator-cancel-all-open-orders",
        action="store_true",
        help=(
            "Submit audited operator cancels for all currently tracked open orders through "
            "the action gateway and configured executor without starting scheduled runtime tasks."
        ),
    )
    parser.add_argument(
        "--operator-cancel-exchange-order-id",
        default=None,
        help="Exchange order ID to cancel with --operator-cancel-order.",
    )
    parser.add_argument(
        "--operator-cancel-client-order-id",
        default=None,
        help="Client order ID to cancel with --operator-cancel-order. Live Coinbase cancel requires exchange order ID.",
    )
    parser.add_argument(
        "--operator-cancel-action-id",
        default=None,
        help="Optional explicit action_id for --operator-cancel-order.",
    )
    parser.add_argument(
        "--operator-cancel-action-id-prefix",
        default=None,
        help="Optional action_id prefix for --operator-cancel-all-open-orders.",
    )
    parser.add_argument(
        "--operator-cancel-product-id",
        default=None,
        help="Optional product filter for --operator-cancel-all-open-orders.",
    )
    parser.add_argument(
        "--operator-cancel-allow-untracked",
        action="store_true",
        help="Allow a cancel request even when the current ledger projection has no matching open order.",
    )
    parser.add_argument(
        "--operator-cancel-reason",
        default=None,
        help="Operator reason recorded in the audited cancel request metadata.",
    )
    parser.add_argument(
        "--operator-id",
        default=None,
        help="Operator identifier required by operator write commands and canary planning.",
    )
    parser.add_argument(
        "--product-catalog-smoke",
        action="store_true",
        help=(
            "Fetch configured product metadata once, append an audited product snapshot, "
            "and exit without starting runtime, websocket, strategy, or order tasks."
        ),
    )
    parser.add_argument(
        "--product-catalog-smoke-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when product catalog smoke output requires attention.",
    )
    parser.add_argument(
        "--feed-smoke",
        action="store_true",
        help=(
            "Run configured websocket feeds for a bounded no-order smoke check, append normal feed audit events, "
            "and exit without starting runtime, strategy, or order tasks."
        ),
    )
    parser.add_argument(
        "--feed-smoke-seconds",
        default=10.0,
        type=float,
        help="Duration for --feed-smoke.",
    )
    parser.add_argument(
        "--feed-smoke-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when feed smoke output requires attention.",
    )
    parser.add_argument(
        "--exchange-state-smoke",
        action="store_true",
        help=(
            "Fetch account balances and CFM/eligible position state once, append audited exchange-state snapshots, "
            "and exit without starting runtime, websocket, strategy, or order tasks."
        ),
    )
    parser.add_argument(
        "--exchange-state-smoke-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when exchange-state smoke output requires attention.",
    )
    parser.add_argument(
        "--live-no-order-preflight",
        action="store_true",
        help=(
            "Run readiness, product-catalog smoke, feed smoke, and exchange-state smoke in order, "
            "stopping on attention without starting order, strategy, or live runtime tasks."
        ),
    )
    parser.add_argument(
        "--live-no-order-preflight-feed-seconds",
        default=10.0,
        type=float,
        help="Feed-smoke duration used by --live-no-order-preflight.",
    )
    parser.add_argument(
        "--live-no-order-preflight-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when live no-order preflight output requires attention.",
    )
    parser.add_argument(
        "--live-runtime-preflight-max-age-seconds",
        default=None,
        type=float,
        help=(
            "Optional maximum age for the required matching live no-order preflight "
            "when starting live runtime tasks."
        ),
    )
    parser.add_argument(
        "--live-runtime-gate",
        action="store_true",
        help=(
            "Print a read-only live runtime admission gate report without starting runtime tasks."
        ),
    )
    parser.add_argument(
        "--live-runtime-gate-fail-on-attention",
        action="store_true",
        help="Return a non-zero exit code when the live runtime gate report requires attention.",
    )
    parser.add_argument(
        "--live-runtime-strategy-simulation-max-age-seconds",
        default=None,
        type=float,
        help=(
            "Optional maximum age for the required matching strategy simulation qualification "
            "when starting live strategy runtime tasks."
        ),
    )
    parser.add_argument(
        "--ledger-checkpoint",
        action="store_true",
        help="Verify the ledger and append an audit checkpoint without running the bot.",
    )
    parser.add_argument(
        "--ledger-anchor-latest-checkpoint",
        action="store_true",
        help=(
            "Publish an anchor for the latest existing audit checkpoint instead of "
            "creating a new checkpoint first. Requires an anchor target."
        ),
    )
    parser.add_argument(
        "--ledger-anchor-dir",
        default=None,
        help="Verify, checkpoint, and publish a local checkpoint anchor artifact without running the bot.",
    )
    parser.add_argument(
        "--ledger-anchor-s3-bucket",
        default=None,
        help="Verify, checkpoint, and publish an AWS S3 Object Lock checkpoint anchor.",
    )
    parser.add_argument(
        "--ledger-anchor-s3-prefix",
        default="audit-anchors",
        help="S3 key prefix for Object Lock checkpoint anchors.",
    )
    parser.add_argument(
        "--ledger-anchor-s3-mode",
        choices=[mode.value for mode in AnchorImmutabilityMode],
        default=None,
        help="S3 Object Lock retention mode for checkpoint anchors.",
    )
    parser.add_argument(
        "--ledger-anchor-s3-retention-days",
        default=None,
        type=int,
        help="Positive Object Lock retention period, in days, for S3 checkpoint anchors.",
    )
    parser.add_argument(
        "--ledger-anchor-s3-expected-bucket-owner",
        default=None,
        help="Optional AWS account ID expected to own the S3 Object Lock bucket.",
    )
    parser.add_argument(
        "--ledger-archive-s3-bucket",
        default=None,
        help="Verify and upload the current ledger record prefix to AWS S3 Object Lock.",
    )
    parser.add_argument(
        "--ledger-archive-s3-prefix",
        default="audit-ledger-archives",
        help="S3 key prefix for Object Lock ledger archives.",
    )
    parser.add_argument(
        "--ledger-archive-s3-mode",
        choices=[mode.value for mode in AnchorImmutabilityMode],
        default=None,
        help="S3 Object Lock retention mode for ledger archives.",
    )
    parser.add_argument(
        "--ledger-archive-s3-retention-days",
        default=None,
        type=int,
        help="Positive Object Lock retention period, in days, for S3 ledger archives.",
    )
    parser.add_argument(
        "--ledger-archive-s3-expected-bucket-owner",
        default=None,
        help="Optional AWS account ID expected to own the S3 Object Lock archive bucket.",
    )
    return parser.parse_args()


async def run_from_args(
    args: argparse.Namespace,
    *,
    product_catalog_client: ProductCatalogLookup | None = None,
    s3_anchor_receipt_verifier: AnchorReceiptVerifier | None = None,
    s3_anchor_store_factory: S3AnchorStoreFactory | None = None,
    s3_archive_receipt_verifier: ArchiveReceiptVerifier | None = None,
    s3_archive_store_factory: S3ArchiveStoreFactory | None = None,
    transport: HttpTransport | None = None,
    websocket_source_factory: WebSocketSourceFactory | None = None,
) -> int:
    config = _config_from_args(args)

    ledger_anchor_dir = getattr(args, "ledger_anchor_dir", None)
    s3_anchor_config = _s3_anchor_config_from_args(args)
    s3_archive_config = _s3_archive_config_from_args(args)
    readiness_requested = getattr(args, "readiness", False)
    operator_policy_scenarios_file = getattr(args, "operator_policy_scenarios_file", None)
    operator_cancel_all_open_orders_requested = getattr(args, "operator_cancel_all_open_orders", False)
    operator_cancel_order_requested = getattr(args, "operator_cancel_order", False)
    operator_canary_evidence_requested = getattr(args, "operator_canary_evidence", False)
    operator_canary_evidence_record_requested = getattr(
        args,
        "operator_canary_evidence_record_result",
        False,
    )
    operator_canary_plan_requested = getattr(args, "operator_canary_plan", False)
    operator_canary_render_requested = getattr(args, "operator_canary_render_dry_run_config", False)
    operator_lookup_order_requested = getattr(args, "operator_lookup_order", False)
    operator_place_order_requested = getattr(args, "operator_place_order", False)
    venue_contract_report_requested = getattr(args, "venue_contract_report", False)
    strategy_scenario_file = getattr(args, "strategy_scenario_file", None)
    strategy_scenario_requested = strategy_scenario_file is not None
    product_catalog_smoke_requested = getattr(args, "product_catalog_smoke", False)
    feed_smoke_requested = getattr(args, "feed_smoke", False)
    exchange_state_smoke_requested = getattr(args, "exchange_state_smoke", False)
    live_no_order_preflight_requested = getattr(args, "live_no_order_preflight", False)
    ledger_health_acknowledge_requested = getattr(args, "ledger_health_acknowledge", False)
    strategy_simulate_record_result_requested = getattr(
        args,
        "strategy_simulate_record_result",
        False,
    )
    if ledger_health_acknowledge_requested:
        if not getattr(args, "ledger_health_acknowledged_by", None):
            raise ValueError("--ledger-health-acknowledged-by is required with --ledger-health-acknowledge")
        if not getattr(args, "ledger_health_acknowledgement_reason", None):
            raise ValueError(
                "--ledger-health-acknowledgement-reason is required with --ledger-health-acknowledge"
            )
    if strategy_simulate_record_result_requested and not getattr(args, "strategy_simulate", False):
        raise ValueError("--strategy-simulate-record-result requires --strategy-simulate")
    if operator_canary_evidence_record_requested and not operator_canary_evidence_requested:
        raise ValueError(
            "--operator-canary-evidence-record-result requires --operator-canary-evidence"
        )
    if strategy_scenario_requested and getattr(args, "ledger_path", None) is None:
        raise ValueError("--ledger-path is required with --strategy-scenario-file")
    if operator_cancel_order_requested and operator_cancel_all_open_orders_requested:
        raise ValueError("--operator-cancel-order and --operator-cancel-all-open-orders cannot be combined")
    if operator_place_order_requested and (operator_cancel_order_requested or operator_cancel_all_open_orders_requested):
        raise ValueError("--operator-place-order cannot be combined with operator cancel commands")
    if operator_canary_render_requested:
        if not getattr(args, "config_file", None):
            raise ValueError("--config-file is required with --operator-canary-render-dry-run-config")
        if not getattr(args, "operator_canary_dry_run_config_file", None):
            raise ValueError(
                "--operator-canary-dry-run-config-file is required with --operator-canary-render-dry-run-config"
            )
        if not getattr(args, "operator_canary_dry_run_ledger_path", None):
            raise ValueError(
                "--operator-canary-dry-run-ledger-path is required with --operator-canary-render-dry-run-config"
            )
        if (
            operator_canary_plan_requested
            or operator_place_order_requested
            or operator_cancel_order_requested
            or operator_cancel_all_open_orders_requested
            or operator_canary_evidence_requested
            or operator_lookup_order_requested
            or getattr(args, "operator_open_orders", False)
        ):
            raise ValueError(
                "--operator-canary-render-dry-run-config cannot be combined with operator plan, "
                "evidence, place, cancel, or open-order commands"
            )
    if operator_canary_plan_requested:
        if not getattr(args, "config_file", None):
            raise ValueError("--config-file is required with --operator-canary-plan")
        if not getattr(args, "operator_canary_dry_run_config_file", None):
            raise ValueError("--operator-canary-dry-run-config-file is required with --operator-canary-plan")
        if not getattr(args, "operator_id", None):
            raise ValueError("--operator-id is required with --operator-canary-plan")
        _require_cli_value(args, "operator_place_product_id", "--operator-place-product-id")
        _require_cli_value(args, "operator_place_side", "--operator-place-side")
        _require_cli_value(args, "operator_place_size", "--operator-place-size")
        _require_cli_value(args, "operator_place_limit_price", "--operator-place-limit-price")
        _require_cli_value(args, "operator_place_order_type", "--operator-place-order-type")
        _require_cli_value(args, "operator_place_time_in_force", "--operator-place-time-in-force")
        _require_cli_value(args, "operator_place_reason", "--operator-place-reason")
    operator_write_requested = (
        operator_place_order_requested
        or operator_cancel_order_requested
        or operator_cancel_all_open_orders_requested
    )
    if operator_lookup_order_requested:
        if not getattr(args, "operator_id", None):
            raise ValueError("--operator-id is required with --operator-lookup-order")
        _require_cli_value(args, "operator_lookup_exchange_order_id", "--operator-lookup-exchange-order-id")
        _require_cli_value(args, "operator_lookup_reason", "--operator-lookup-reason")
    if operator_write_requested:
        if not getattr(args, "operator_id", None):
            raise ValueError("--operator-id is required with operator write commands")
    if venue_contract_report_requested:
        _require_cli_value(args, "venue_contract_venue", "--venue-contract-venue")
    if operator_place_order_requested:
        _require_cli_value(args, "operator_place_product_id", "--operator-place-product-id")
        _require_cli_value(args, "operator_place_side", "--operator-place-side")
        _require_cli_value(args, "operator_place_size", "--operator-place-size")
        _require_cli_value(args, "operator_place_limit_price", "--operator-place-limit-price")
        _require_cli_value(args, "operator_place_order_type", "--operator-place-order-type")
        _require_cli_value(args, "operator_place_time_in_force", "--operator-place-time-in-force")
        _require_cli_value(args, "operator_place_reason", "--operator-place-reason")
        if _enum_value(OrderType, getattr(args, "operator_place_order_type"), "--operator-place-order-type") != OrderType.LIMIT:
            raise ValueError("--operator-place-order currently supports limit orders only")
    if operator_cancel_order_requested:
        if not (
            getattr(args, "operator_cancel_exchange_order_id", None)
            or getattr(args, "operator_cancel_client_order_id", None)
        ):
            raise ValueError(
                "--operator-cancel-exchange-order-id or --operator-cancel-client-order-id "
                "is required with --operator-cancel-order"
            )
    anchor_requested = ledger_anchor_dir is not None or (s3_anchor_config is not None and not readiness_requested)
    archive_requested = s3_archive_config is not None and not readiness_requested
    if ledger_anchor_dir is not None and s3_anchor_config is not None:
        raise ValueError("local and S3 checkpoint anchors cannot be combined")
    if archive_requested and (getattr(args, "ledger_checkpoint", False) or anchor_requested):
        raise ValueError("ledger archive cannot be combined with checkpoint or anchor commands")
    anchor_latest_checkpoint_requested = getattr(args, "ledger_anchor_latest_checkpoint", False)
    if anchor_latest_checkpoint_requested and not anchor_requested:
        raise ValueError("--ledger-anchor-latest-checkpoint requires an anchor target")
    if anchor_latest_checkpoint_requested and getattr(args, "ledger_checkpoint", False):
        raise ValueError("--ledger-anchor-latest-checkpoint cannot be combined with --ledger-checkpoint")
    read_only_commands = {
        "--ledger-export": getattr(args, "ledger_export", False),
        "--ledger-health": getattr(args, "ledger_health", False),
        "--ledger-summary": getattr(args, "ledger_summary", False),
        "--operator-canary-evidence": operator_canary_evidence_requested,
        "--operator-canary-plan": operator_canary_plan_requested,
        "--operator-open-orders": getattr(args, "operator_open_orders", False),
        "--live-runtime-gate": getattr(args, "live_runtime_gate", False),
        "--operator-policy-scenarios-file": operator_policy_scenarios_file is not None,
        "--readiness": readiness_requested,
        "--source-of-truth": getattr(args, "source_of_truth", False),
        "--strategy-simulate": getattr(args, "strategy_simulate", False),
        "--venue-contract-report": venue_contract_report_requested,
    }
    enabled_read_only_commands = [
        command for command, enabled in read_only_commands.items() if enabled
    ]
    if len(enabled_read_only_commands) > 1:
        raise ValueError(f"{', '.join(read_only_commands)} cannot be combined")
    if enabled_read_only_commands and (
        getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
    ):
        raise ValueError(
            f"{enabled_read_only_commands[0]} cannot be combined with acknowledgement, checkpoint, anchor, or archive commands"
        )
    if operator_canary_render_requested and (
        enabled_read_only_commands
        or strategy_scenario_requested
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or product_catalog_smoke_requested
        or feed_smoke_requested
        or exchange_state_smoke_requested
        or live_no_order_preflight_requested
        or getattr(args, "strategy_simulate", False)
    ):
        raise ValueError(
            "--operator-canary-render-dry-run-config cannot be combined with runtime, read-only, "
            "smoke, preflight, scenario, checkpoint, acknowledgement, anchor, or archive commands"
        )
    if strategy_scenario_requested and (
        enabled_read_only_commands
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or product_catalog_smoke_requested
        or feed_smoke_requested
        or exchange_state_smoke_requested
        or live_no_order_preflight_requested
    ):
        raise ValueError(
            "--strategy-scenario-file cannot be combined with read-only, smoke, preflight, checkpoint, anchor, or archive commands"
        )
    operator_cancel_requested = operator_cancel_order_requested or operator_cancel_all_open_orders_requested
    if operator_write_requested and (
        enabled_read_only_commands
        or strategy_scenario_requested
        or operator_lookup_order_requested
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or product_catalog_smoke_requested
        or feed_smoke_requested
        or exchange_state_smoke_requested
        or live_no_order_preflight_requested
    ):
        raise ValueError(
            "operator write commands cannot be combined with read-only, scenario, smoke, "
            "preflight, checkpoint, acknowledgement, anchor, or archive commands"
        )
    if operator_lookup_order_requested and (
        enabled_read_only_commands
        or strategy_scenario_requested
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or product_catalog_smoke_requested
        or feed_smoke_requested
        or exchange_state_smoke_requested
        or live_no_order_preflight_requested
    ):
        raise ValueError(
            "--operator-lookup-order cannot be combined with read-only, scenario, smoke, "
            "preflight, checkpoint, acknowledgement, anchor, or archive commands"
        )
    if product_catalog_smoke_requested and (
        enabled_read_only_commands
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or feed_smoke_requested
        or exchange_state_smoke_requested
        or live_no_order_preflight_requested
    ):
        raise ValueError(
            "--product-catalog-smoke cannot be combined with read-only, feed-smoke, exchange-state-smoke, live-no-order-preflight, checkpoint, anchor, or archive commands"
        )
    if feed_smoke_requested and (
        enabled_read_only_commands
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or exchange_state_smoke_requested
        or live_no_order_preflight_requested
    ):
        raise ValueError(
            "--feed-smoke cannot be combined with read-only, exchange-state-smoke, live-no-order-preflight, checkpoint, anchor, or archive commands"
        )
    if exchange_state_smoke_requested and (
        enabled_read_only_commands
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
        or live_no_order_preflight_requested
    ):
        raise ValueError(
            "--exchange-state-smoke cannot be combined with read-only, live-no-order-preflight, checkpoint, anchor, or archive commands"
        )
    if live_no_order_preflight_requested and (
        enabled_read_only_commands
        or getattr(args, "ledger_checkpoint", False)
        or ledger_health_acknowledge_requested
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
    ):
        raise ValueError(
            "--live-no-order-preflight cannot be combined with read-only, checkpoint, anchor, or archive commands"
        )
    if ledger_health_acknowledge_requested and (
        getattr(args, "ledger_checkpoint", False)
        or anchor_latest_checkpoint_requested
        or anchor_requested
        or archive_requested
    ):
        raise ValueError(
            "--ledger-health-acknowledge cannot be combined with checkpoint, anchor, or archive commands"
        )

    if getattr(args, "live_runtime_gate", False):
        preflight_max_age_seconds = getattr(
            args,
            "live_runtime_preflight_max_age_seconds",
            None,
        )
        strategy_simulation_max_age_seconds = getattr(
            args,
            "live_runtime_strategy_simulation_max_age_seconds",
            None,
        )
        payload = live_runtime_gate_payload(
            config,
            approved=live_trading_approved_from_env(),
            preflight_max_age=(
                timedelta(seconds=preflight_max_age_seconds)
                if preflight_max_age_seconds is not None
                else None
            ),
            strategy_simulation_max_age=(
                timedelta(seconds=strategy_simulation_max_age_seconds)
                if strategy_simulation_max_age_seconds is not None
                else None
            ),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "live_runtime_gate_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if readiness_requested:
        credentials = load_coinbase_runtime_credentials_from_env()
        live_trading_approved = live_trading_approved_from_env()
        extra_checks = _audit_store_readiness_checks(
            config,
            cli_s3_anchor_config=s3_anchor_config,
            cli_s3_archive_config=s3_archive_config,
            s3_anchor_store_factory=s3_anchor_store_factory,
            s3_archive_store_factory=s3_archive_store_factory,
        )
        payload = readiness_payload(
            config,
            allow_config_fingerprint_mismatch=getattr(
                args,
                "readiness_allow_config_fingerprint_mismatch",
                False,
            ),
            extra_checks=extra_checks,
            jwt_factory_configured=credentials.jwt_factory_configured,
            live_trading_approved=live_trading_approved,
            token_provider_configured=credentials.token_provider_configured,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "readiness_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if operator_canary_render_requested:
        payload = render_operator_canary_dry_run_config(
            force=getattr(args, "operator_canary_dry_run_config_force", False),
            ledger_path=Path(getattr(args, "operator_canary_dry_run_ledger_path")),
            source_config_file=Path(getattr(args, "config_file")),
            target_config_file=Path(getattr(args, "operator_canary_dry_run_config_file")),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if getattr(args, "ledger_export", False):
        print(json.dumps(ledger_export_payload(config.ledger_path), indent=2, sort_keys=True))
        return 0

    if getattr(args, "ledger_health", False):
        anchor_receipt_verifier = None
        if getattr(args, "ledger_health_verify_s3_anchors", False):
            anchor_receipt_verifier = s3_anchor_receipt_verifier or _verify_s3_anchor_receipt_payload
        archive_receipt_verifier = None
        if getattr(args, "ledger_health_verify_s3_archives", False):
            archive_receipt_verifier = s3_archive_receipt_verifier or _verify_s3_archive_receipt_payload
        payload = ledger_health_payload(
            config.ledger_path,
            anchor_receipt_verifier=anchor_receipt_verifier,
            archive_receipt_verifier=archive_receipt_verifier,
            max_records_after_anchor=getattr(
                args,
                "ledger_health_max_records_after_anchor",
                None,
            ),
            max_records_after_archive=getattr(
                args,
                "ledger_health_max_records_after_archive",
                None,
            ),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "ledger_health_fail_on_attention", False),
            ok_status=LedgerHealthStatus.OK.value,
        )

    if ledger_health_acknowledge_requested:
        payload = acknowledge_ledger_health(
            config.ledger_path,
            acknowledged_by=getattr(args, "ledger_health_acknowledged_by"),
            reason=getattr(args, "ledger_health_acknowledgement_reason"),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if getattr(args, "ledger_summary", False):
        print(json.dumps(ledger_summary_payload(config.ledger_path), indent=2, sort_keys=True))
        return 0

    if getattr(args, "source_of_truth", False):
        print(json.dumps(source_of_truth_payload(config.ledger_path), indent=2, sort_keys=True))
        return 0

    if venue_contract_report_requested:
        payload = venue_contract_report_payload(
            getattr(args, "venue_contract_venue"),
            requirement_set=_enum_value(
                VenueContractRequirementSet,
                getattr(args, "venue_contract_requirement_set"),
                "--venue-contract-requirement-set",
            ),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "venue_contract_fail_on_missing", False),
            ok_status=StrategyHelperStatus.OK.value,
        )

    if operator_canary_plan_requested:
        dry_run_config = load_coinbase_application_config_from_json_file(
            Path(getattr(args, "operator_canary_dry_run_config_file"))
        )
        payload = operator_canary_plan_payload(
            config,
            dry_run_config,
            dry_run_config_file=getattr(args, "operator_canary_dry_run_config_file"),
            leverage=getattr(args, "operator_place_leverage", None),
            limit_price=getattr(args, "operator_place_limit_price"),
            live_config_file=getattr(args, "config_file"),
            margin_type=(
                _enum_value(
                    MarginType,
                    getattr(args, "operator_place_margin_type"),
                    "--operator-place-margin-type",
                )
                if getattr(args, "operator_place_margin_type", None) is not None
                else None
            ),
            operator_id=getattr(args, "operator_id"),
            order_type=_enum_value(
                OrderType,
                getattr(args, "operator_place_order_type"),
                "--operator-place-order-type",
            ),
            post_only=getattr(args, "operator_place_post_only", False),
            product_id=getattr(args, "operator_place_product_id"),
            reason=getattr(args, "operator_place_reason"),
            reduce_only=getattr(args, "operator_place_reduce_only", False),
            side=_enum_value(
                OrderSide,
                getattr(args, "operator_place_side"),
                "--operator-place-side",
            ),
            size=getattr(args, "operator_place_size"),
            time_in_force=_enum_value(
                TimeInForce,
                getattr(args, "operator_place_time_in_force"),
                "--operator-place-time-in-force",
            ),
            product_catalog=_product_catalog_from_existing_ledger(config.ledger_path),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=True,
            ok_status=ReadinessStatus.OK.value,
        )

    if operator_canary_evidence_requested:
        payload = operator_canary_evidence_payload(
            config.ledger_path,
            action_id=getattr(args, "operator_canary_evidence_action_id", None),
            exchange_order_id=getattr(
                args,
                "operator_canary_evidence_exchange_order_id",
                None,
            ),
            product_id=getattr(args, "operator_canary_evidence_product_id", None),
        )
        if operator_canary_evidence_record_requested:
            record = record_operator_canary_evidence_result(config, payload)
            payload["operator_canary_evidence_result_sequence"] = record.sequence
            payload["writes_ledger"] = True
        else:
            payload["operator_canary_evidence_result_sequence"] = None
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "operator_canary_evidence_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if getattr(args, "operator_open_orders", False):
        payload = operator_open_orders_payload(
            config.ledger_path,
            product_id=getattr(args, "operator_open_orders_product_id", None),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if operator_policy_scenarios_file is not None:
        payload = operator_policy_scenarios_payload(Path(operator_policy_scenarios_file))
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=True,
            ok_status=ReadinessStatus.OK.value,
        )

    if getattr(args, "strategy_simulate", False):
        payload = strategy_simulation_payload(config)
        if strategy_simulate_record_result_requested:
            record = record_strategy_simulation_result(config, payload)
            payload["strategy_simulation_result_sequence"] = record.sequence
        else:
            payload["strategy_simulation_result_sequence"] = None
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "strategy_simulate_fail_on_attention", False),
            ok_status=StrategySimulationStatus.OK.value,
        )

    if strategy_scenario_requested:
        payload = strategy_scenario_payload(
            config,
            scenario_file=Path(strategy_scenario_file),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if payload["passed"] is True else ATTENTION_REQUIRED_EXIT_CODE

    if product_catalog_smoke_requested:
        try:
            credentials = load_coinbase_runtime_credentials_from_env()
        except Exception as exc:
            _audit_runtime_preflight_error(config.ledger_path, exc)
            raise
        payload = product_catalog_smoke_payload(
            config,
            jwt_factory=credentials.jwt_factory,
            product_catalog_client=product_catalog_client,
            token_provider=credentials.token_provider,
            transport=transport,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "product_catalog_smoke_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if feed_smoke_requested:
        try:
            credentials = load_coinbase_runtime_credentials_from_env()
        except Exception as exc:
            _audit_runtime_preflight_error(config.ledger_path, exc)
            raise
        payload = await feed_smoke_payload(
            config,
            duration=timedelta(seconds=getattr(args, "feed_smoke_seconds", 10.0)),
            jwt_factory=credentials.jwt_factory,
            token_provider=credentials.token_provider,
            transport=transport,
            websocket_source_factory=websocket_source_factory,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "feed_smoke_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if exchange_state_smoke_requested:
        try:
            credentials = load_coinbase_runtime_credentials_from_env()
        except Exception as exc:
            _audit_runtime_preflight_error(config.ledger_path, exc)
            raise
        payload = exchange_state_smoke_payload(
            config,
            jwt_factory=credentials.jwt_factory,
            token_provider=credentials.token_provider,
            transport=transport,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "exchange_state_smoke_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if live_no_order_preflight_requested:
        try:
            credentials = load_coinbase_runtime_credentials_from_env()
        except Exception as exc:
            _audit_runtime_preflight_error(config.ledger_path, exc)
            raise
        payload = await live_no_order_preflight_payload(
            config,
            allow_config_fingerprint_mismatch=getattr(
                args,
                "readiness_allow_config_fingerprint_mismatch",
                False,
            ),
            duration=timedelta(
                seconds=getattr(args, "live_no_order_preflight_feed_seconds", 10.0)
            ),
            jwt_factory=credentials.jwt_factory,
            live_trading_approved=live_trading_approved_from_env(),
            product_catalog_client=product_catalog_client,
            token_provider=credentials.token_provider,
            transport=transport,
            websocket_source_factory=websocket_source_factory,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=getattr(args, "live_no_order_preflight_fail_on_attention", False),
            ok_status=ReadinessStatus.OK.value,
        )

    if operator_lookup_order_requested:
        try:
            credentials = load_coinbase_runtime_credentials_from_env()
        except Exception as exc:
            _audit_runtime_preflight_error(config.ledger_path, exc)
            raise
        application = build_coinbase_application(
            config,
            jwt_factory=credentials.jwt_factory,
            s3_anchor_store_factory=s3_anchor_store_factory,
            s3_archive_store_factory=s3_archive_store_factory,
            token_provider=credentials.token_provider,
            transport=transport,
        )
        payload = operator_lookup_order_payload(
            config,
            application,
            exchange_order_id=getattr(args, "operator_lookup_exchange_order_id"),
            operator_id=getattr(args, "operator_id"),
            reason=getattr(args, "operator_lookup_reason"),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=True,
            ok_status=ReadinessStatus.OK.value,
        )

    if getattr(args, "ledger_checkpoint", False):
        checkpoint = record_ledger_checkpoint(config.ledger_path)
        if anchor_requested:
            store = _anchor_store_from_args(
                ledger_anchor_dir=ledger_anchor_dir,
                s3_anchor_config=s3_anchor_config,
                s3_anchor_store_factory=s3_anchor_store_factory,
            )
            anchor = publish_recorded_ledger_checkpoint_anchor(
                config.ledger_path,
                checkpoint,
                store,
            )
            print(
                json.dumps(
                    {
                        "anchor": anchor.to_payload(),
                        "checkpoint": checkpoint.to_payload(),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        print(json.dumps(checkpoint.to_payload(), indent=2, sort_keys=True))
        return 0

    if anchor_requested:
        checkpoint = (
            latest_recorded_ledger_checkpoint(config.ledger_path)
            if anchor_latest_checkpoint_requested
            else record_ledger_checkpoint(config.ledger_path)
        )
        store = _anchor_store_from_args(
            ledger_anchor_dir=ledger_anchor_dir,
            s3_anchor_config=s3_anchor_config,
            s3_anchor_store_factory=s3_anchor_store_factory,
        )
        anchor = publish_recorded_ledger_checkpoint_anchor(
            config.ledger_path,
            checkpoint,
            store,
        )
        print(
            json.dumps(
                {
                    "anchor": anchor.to_payload(),
                    "checkpoint": checkpoint.to_payload(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if archive_requested:
        store = _archive_store_from_args(
            s3_archive_config=s3_archive_config,
            s3_archive_store_factory=s3_archive_store_factory,
        )
        archive = publish_ledger_archive(config.ledger_path, store)
        print(json.dumps({"archive": archive.to_payload()}, indent=2, sort_keys=True))
        return 0

    if operator_write_requested:
        token_provider = None
        jwt_factory = None
        if config.bot.live_rest_execution_enabled():
            if not live_trading_approved_from_env():
                exc = ConfigError(
                    f"Missing required live runtime approval: {ReadinessRequirement.LIVE_TRADING_APPROVAL.value}",
                    context={"requirement": ReadinessRequirement.LIVE_TRADING_APPROVAL.value},
                )
                _audit_runtime_preflight_error(config.ledger_path, exc)
                raise exc
            try:
                credentials = load_coinbase_runtime_credentials_from_env()
            except Exception as exc:
                _audit_runtime_preflight_error(config.ledger_path, exc)
                raise
            token_provider = credentials.token_provider
            jwt_factory = credentials.jwt_factory
        application = build_coinbase_application(
            config,
            jwt_factory=jwt_factory,
            s3_anchor_store_factory=s3_anchor_store_factory,
            s3_archive_store_factory=s3_archive_store_factory,
            token_provider=token_provider,
            transport=transport,
        )
        if operator_place_order_requested:
            payload = operator_place_order_payload(
                config,
                application,
                action_id=getattr(args, "operator_place_action_id", None),
                client_order_id=getattr(args, "operator_place_client_order_id", None),
                limit_price=getattr(args, "operator_place_limit_price"),
                leverage=getattr(args, "operator_place_leverage", None),
                margin_type=(
                    _enum_value(
                        MarginType,
                        getattr(args, "operator_place_margin_type"),
                        "--operator-place-margin-type",
                    )
                    if getattr(args, "operator_place_margin_type", None) is not None
                    else None
                ),
                operator_id=getattr(args, "operator_id"),
                order_type=_enum_value(
                    OrderType,
                    getattr(args, "operator_place_order_type"),
                    "--operator-place-order-type",
                ),
                post_only=getattr(args, "operator_place_post_only", False),
                product_id=getattr(args, "operator_place_product_id"),
                reason=getattr(args, "operator_place_reason"),
                reduce_only=getattr(args, "operator_place_reduce_only", False),
                side=_enum_value(
                    OrderSide,
                    getattr(args, "operator_place_side"),
                    "--operator-place-side",
                ),
                size=getattr(args, "operator_place_size"),
                time_in_force=_enum_value(
                    TimeInForce,
                    getattr(args, "operator_place_time_in_force"),
                    "--operator-place-time-in-force",
                ),
            )
        elif operator_cancel_all_open_orders_requested:
            payload = operator_cancel_all_open_orders_payload(
                config,
                application,
                action_id_prefix=getattr(args, "operator_cancel_action_id_prefix", None),
                operator_id=getattr(args, "operator_id"),
                product_id=getattr(args, "operator_cancel_product_id", None),
                reason=getattr(args, "operator_cancel_reason", None),
            )
        else:
            payload = operator_cancel_order_payload(
                config,
                application,
                action_id=getattr(args, "operator_cancel_action_id", None),
                allow_untracked=getattr(args, "operator_cancel_allow_untracked", False),
                client_order_id=getattr(args, "operator_cancel_client_order_id", None),
                exchange_order_id=getattr(args, "operator_cancel_exchange_order_id", None),
                operator_id=getattr(args, "operator_id"),
                reason=getattr(args, "operator_cancel_reason", None),
            )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return _report_exit_code(
            payload,
            fail_on_attention=True,
            ok_status=ReadinessStatus.OK.value,
        )

    try:
        preflight_max_age_seconds = getattr(
            args,
            "live_runtime_preflight_max_age_seconds",
            None,
        )
        strategy_simulation_max_age_seconds = getattr(
            args,
            "live_runtime_strategy_simulation_max_age_seconds",
            None,
        )
        enforce_live_runtime_gate(
            config,
            approved=live_trading_approved_from_env(),
            preflight_max_age=(
                timedelta(seconds=preflight_max_age_seconds)
                if preflight_max_age_seconds is not None
                else None
            ),
            strategy_simulation_max_age=(
                timedelta(seconds=strategy_simulation_max_age_seconds)
                if strategy_simulation_max_age_seconds is not None
                else None
            ),
        )
        credentials = load_coinbase_runtime_credentials_from_env()
    except Exception as exc:
        _audit_runtime_preflight_error(config.ledger_path, exc)
        raise

    stop_after_task = _runtime_stop_after_task_from_args(args)
    _validate_runtime_stop_after_task(config, stop_after_task)

    application = build_coinbase_application(
        config,
        jwt_factory=credentials.jwt_factory,
        s3_anchor_store_factory=s3_anchor_store_factory,
        s3_archive_store_factory=s3_archive_store_factory,
        token_provider=credentials.token_provider,
    )
    result = await application.run(
        max_cycles=_runtime_max_cycles_from_args(args),
        stop_after_task=stop_after_task,
        stop_after_task_count=_runtime_stop_after_task_count_from_args(args),
    )
    print(f"completed_cycles={result.completed_cycles}")
    print(f"ledger_path={result.ledger_path}")
    if getattr(args, "runtime_fail_on_attention", False):
        health_payload = ledger_health_payload(config.ledger_path)
        _record_runtime_health_check_result(config, health_payload)
        print(f"runtime_health_status={health_payload.get('status')}")
        return _report_exit_code(
            health_payload,
            fail_on_attention=True,
            ok_status=LedgerHealthStatus.OK.value,
        )
    return 0


def _report_exit_code(
    payload: dict[str, JsonValue],
    *,
    fail_on_attention: bool,
    ok_status: str,
) -> int:
    if fail_on_attention and payload.get("status") != ok_status:
        return ATTENTION_REQUIRED_EXIT_CODE
    return 0


def _config_from_args(args: argparse.Namespace) -> CoinbaseApplicationConfig:
    config_file = getattr(args, "config_file", None)
    if config_file is not None:
        config = load_coinbase_application_config_from_json_file(Path(config_file))
    elif has_coinbase_application_env():
        config = load_coinbase_application_config_from_env()
    else:
        config = default_coinbase_application_config()

    ledger_path = getattr(args, "ledger_path", None)
    if ledger_path is not None:
        config = replace(config, ledger_path=Path(ledger_path))
    return config


def _require_cli_value(args: argparse.Namespace, attribute: str, option_name: str) -> None:
    value = getattr(args, attribute, None)
    if value is None or value == "":
        raise ValueError(f"{option_name} is required")


def _enum_value(enum_type, value: str, option_name: str):
    try:
        return enum_type(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise ValueError(f"{option_name} must be one of: {allowed}") from exc


def _anchor_store_from_args(
    *,
    ledger_anchor_dir: str | None,
    s3_anchor_config: S3ObjectLockAnchorConfig | None,
    s3_anchor_store_factory: S3AnchorStoreFactory | None,
) -> LedgerAnchorStore:
    if ledger_anchor_dir is not None:
        return ledger_anchor_store_from_config(
            AuditAnchorStoreConfig(
                provider=LedgerAnchorStoreProvider.LOCAL_FILE,
                local_anchor_dir=Path(ledger_anchor_dir),
            )
        )
    if s3_anchor_config is not None:
        return ledger_anchor_store_from_config(
            _audit_anchor_store_config_from_s3_config(s3_anchor_config),
            s3_anchor_store_factory=s3_anchor_store_factory,
        )
    raise ValueError("checkpoint anchor target is required")


def _archive_store_from_args(
    *,
    s3_archive_config: S3ObjectLockLedgerArchiveConfig | None,
    s3_archive_store_factory: S3ArchiveStoreFactory | None,
) -> LedgerArchiveStore:
    if s3_archive_config is not None:
        if s3_archive_store_factory is not None:
            return s3_archive_store_factory(s3_archive_config)
        return S3ObjectLockLedgerArchiveStore(s3_archive_config)
    raise ValueError("ledger archive target is required")


def _audit_store_readiness_checks(
    config: CoinbaseApplicationConfig,
    *,
    cli_s3_anchor_config: S3ObjectLockAnchorConfig | None,
    cli_s3_archive_config: S3ObjectLockLedgerArchiveConfig | None,
    s3_anchor_store_factory: S3AnchorStoreFactory | None,
    s3_archive_store_factory: S3ArchiveStoreFactory | None,
) -> tuple[ReadinessCheckResult, ...]:
    return (
        *_anchor_readiness_checks(
            config,
            cli_s3_anchor_config=cli_s3_anchor_config,
            s3_anchor_store_factory=s3_anchor_store_factory,
        ),
        *_archive_readiness_checks(
            config,
            cli_s3_archive_config=cli_s3_archive_config,
            s3_archive_store_factory=s3_archive_store_factory,
        ),
    )


def _product_catalog_from_existing_ledger(ledger_path: Path):
    if not ledger_path.exists() or ledger_path.is_dir():
        return None
    projection = SourceOfTruthProjection.from_ledger(AuditLedger(ledger_path))
    return product_catalog_from_projection(projection)


def _anchor_readiness_checks(
    config: CoinbaseApplicationConfig,
    *,
    cli_s3_anchor_config: S3ObjectLockAnchorConfig | None,
    s3_anchor_store_factory: S3AnchorStoreFactory | None,
) -> tuple[ReadinessCheckResult, ...]:
    if cli_s3_anchor_config is not None:
        return (
            _s3_anchor_readiness_check(
                cli_s3_anchor_config,
                s3_anchor_store_factory=s3_anchor_store_factory,
            ),
        )

    store_config = config.bot.audit_anchor_store
    if not config.bot.audit_anchor_schedule.enabled or store_config is None:
        return ()

    if store_config.provider == LedgerAnchorStoreProvider.LOCAL_FILE:
        return (_local_anchor_readiness_check(store_config),)

    if store_config.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        return (
            _s3_anchor_readiness_check(
                s3_object_lock_anchor_config_from_store_config(store_config),
                s3_anchor_store_factory=s3_anchor_store_factory,
            ),
        )

    raise ValueError(f"unsupported audit anchor store provider: {store_config.provider.value}")


def _archive_readiness_checks(
    config: CoinbaseApplicationConfig,
    *,
    cli_s3_archive_config: S3ObjectLockLedgerArchiveConfig | None,
    s3_archive_store_factory: S3ArchiveStoreFactory | None,
) -> tuple[ReadinessCheckResult, ...]:
    if cli_s3_archive_config is not None:
        return (
            _s3_archive_readiness_check(
                cli_s3_archive_config,
                s3_archive_store_factory=s3_archive_store_factory,
            ),
        )

    store_config = config.bot.audit_archive_store
    if not config.bot.audit_archive_schedule.enabled or store_config is None:
        return ()

    if store_config.provider == LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK:
        return (
            _s3_archive_readiness_check(
                s3_object_lock_archive_config_from_store_config(store_config),
                s3_archive_store_factory=s3_archive_store_factory,
            ),
        )

    raise ValueError(f"unsupported audit archive store provider: {store_config.provider.value}")


def _local_anchor_readiness_check(config: AuditAnchorStoreConfig) -> ReadinessCheckResult:
    anchor_dir = config.local_anchor_dir
    if anchor_dir is None:
        raise ValueError("local_anchor_dir is required")
    details: dict[str, JsonValue] = {
        "anchor_dir": anchor_dir.as_posix(),
        "exists": anchor_dir.exists(),
        "is_directory": anchor_dir.is_dir() if anchor_dir.exists() else None,
        "provider": LedgerAnchorStoreProvider.LOCAL_FILE.value,
        "write_attempted": False,
    }
    if anchor_dir.exists() and not anchor_dir.is_dir():
        return ReadinessCheckResult(
            count=1,
            details=details,
            name=ReadinessCheckName.ANCHOR_STORE,
            status=ReadinessStatus.ATTENTION_REQUIRED,
        )
    return ReadinessCheckResult(
        count=0,
        details=details,
        name=ReadinessCheckName.ANCHOR_STORE,
        status=ReadinessStatus.OK,
    )


def _s3_anchor_readiness_check(
    config: S3ObjectLockAnchorConfig,
    *,
    s3_anchor_store_factory: S3AnchorStoreFactory | None,
) -> ReadinessCheckResult:
    def verify_bucket_configuration() -> None:
        store = (
            s3_anchor_store_factory(config)
            if s3_anchor_store_factory is not None
            else S3ObjectLockLedgerAnchorStore(config)
        )
        store.verify_bucket_configuration()

    return _s3_object_lock_readiness_check(
        details=_s3_anchor_readiness_details(config),
        name=ReadinessCheckName.ANCHOR_STORE,
        verify_bucket_configuration=verify_bucket_configuration,
    )


def _s3_anchor_readiness_details(config: S3ObjectLockAnchorConfig) -> dict[str, JsonValue]:
    return {
        "bucket": config.bucket,
        "expected_bucket_owner": config.expected_bucket_owner,
        "immutability_mode": config.immutability_mode.value,
        "key_prefix": config.key_prefix,
        "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
        "retention_days": config.retention_period.days,
        "write_attempted": False,
    }


def _s3_archive_readiness_check(
    config: S3ObjectLockLedgerArchiveConfig,
    *,
    s3_archive_store_factory: S3ArchiveStoreFactory | None,
) -> ReadinessCheckResult:
    def verify_bucket_configuration() -> None:
        store = (
            s3_archive_store_factory(config)
            if s3_archive_store_factory is not None
            else S3ObjectLockLedgerArchiveStore(config)
        )
        store.verify_bucket_configuration()

    return _s3_object_lock_readiness_check(
        details=_s3_archive_readiness_details(config),
        name=ReadinessCheckName.ARCHIVE_STORE,
        verify_bucket_configuration=verify_bucket_configuration,
    )


def _s3_archive_readiness_details(
    config: S3ObjectLockLedgerArchiveConfig,
) -> dict[str, JsonValue]:
    return {
        "bucket": config.bucket,
        "expected_bucket_owner": config.expected_bucket_owner,
        "immutability_mode": config.immutability_mode.value,
        "key_prefix": config.key_prefix,
        "provider": LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK.value,
        "retention_days": config.retention_period.days,
        "write_attempted": False,
    }


def _s3_object_lock_readiness_check(
    *,
    details: dict[str, JsonValue],
    name: ReadinessCheckName,
    verify_bucket_configuration: Callable[[], None],
) -> ReadinessCheckResult:
    try:
        verify_bucket_configuration()
    except Exception as exc:
        details.update(
            {
                "bucket_configuration_verified": False,
                "exception_type": type(exc).__name__,
                "message": str(exc),
            }
        )
        return ReadinessCheckResult(
            count=1,
            details=details,
            name=name,
            status=ReadinessStatus.ATTENTION_REQUIRED,
        )

    details["bucket_configuration_verified"] = True
    return ReadinessCheckResult(
        count=0,
        details=details,
        name=name,
        status=ReadinessStatus.OK,
    )


def _audit_anchor_store_config_from_s3_config(config: S3ObjectLockAnchorConfig) -> AuditAnchorStoreConfig:
    return AuditAnchorStoreConfig(
        provider=LedgerAnchorStoreProvider.AWS_S3_OBJECT_LOCK,
        s3_bucket=config.bucket,
        s3_expected_bucket_owner=config.expected_bucket_owner,
        s3_immutability_mode=config.immutability_mode,
        s3_key_prefix=config.key_prefix,
        s3_retention_period=config.retention_period,
        s3_verify_bucket_configuration=config.verify_bucket_configuration,
    )


def _s3_anchor_config_from_args(args: argparse.Namespace) -> S3ObjectLockAnchorConfig | None:
    bucket = getattr(args, "ledger_anchor_s3_bucket", None)
    mode = getattr(args, "ledger_anchor_s3_mode", None)
    retention_days = getattr(args, "ledger_anchor_s3_retention_days", None)
    expected_bucket_owner = getattr(args, "ledger_anchor_s3_expected_bucket_owner", None)
    prefix = getattr(args, "ledger_anchor_s3_prefix", "audit-anchors")

    if bucket is None:
        if mode is not None or retention_days is not None or expected_bucket_owner is not None:
            raise ValueError("--ledger-anchor-s3-bucket is required with S3 anchor options")
        return None
    if mode is None:
        raise ValueError("--ledger-anchor-s3-mode is required with --ledger-anchor-s3-bucket")
    if retention_days is None:
        raise ValueError("--ledger-anchor-s3-retention-days is required with --ledger-anchor-s3-bucket")

    return S3ObjectLockAnchorConfig(
        bucket=bucket,
        expected_bucket_owner=expected_bucket_owner,
        immutability_mode=AnchorImmutabilityMode(mode),
        key_prefix=prefix,
        retention_period=timedelta(days=retention_days),
    )


def _s3_archive_config_from_args(args: argparse.Namespace) -> S3ObjectLockLedgerArchiveConfig | None:
    bucket = getattr(args, "ledger_archive_s3_bucket", None)
    mode = getattr(args, "ledger_archive_s3_mode", None)
    retention_days = getattr(args, "ledger_archive_s3_retention_days", None)
    expected_bucket_owner = getattr(args, "ledger_archive_s3_expected_bucket_owner", None)
    prefix = getattr(args, "ledger_archive_s3_prefix", "audit-ledger-archives")

    if bucket is None:
        if mode is not None or retention_days is not None or expected_bucket_owner is not None:
            raise ValueError("--ledger-archive-s3-bucket is required with S3 archive options")
        return None
    if mode is None:
        raise ValueError("--ledger-archive-s3-mode is required with --ledger-archive-s3-bucket")
    if retention_days is None:
        raise ValueError("--ledger-archive-s3-retention-days is required with --ledger-archive-s3-bucket")

    return S3ObjectLockLedgerArchiveConfig(
        bucket=bucket,
        expected_bucket_owner=expected_bucket_owner,
        immutability_mode=AnchorImmutabilityMode(mode),
        key_prefix=prefix,
        retention_period=timedelta(days=retention_days),
    )


def _audit_runtime_preflight_error(ledger_path: Path, exc: Exception) -> None:
    AuditCore(AuditLedger(ledger_path)).emit(
        EventType.ERROR,
        exception_to_error_payload(
            exc,
            category=ErrorCategory.CONFIG,
            context={"stage": "runtime_preflight"},
            error_code=ErrorCode.CONFIG_INVALID,
        ),
    )


def _runtime_max_cycles_from_args(args: argparse.Namespace) -> int | None:
    if getattr(args, "run_forever", False):
        return None
    max_cycles = getattr(args, "max_cycles", None)
    if max_cycles is not None:
        return max_cycles
    if _runtime_stop_after_task_from_args(args) is not None:
        return None
    return 1


def _runtime_stop_after_task_from_args(args: argparse.Namespace) -> RuntimeTask | None:
    value = getattr(args, "stop_after_task", None)
    if value is None:
        return None
    if isinstance(value, RuntimeTask):
        return value
    return RuntimeTask(value)


def _runtime_stop_after_task_count_from_args(args: argparse.Namespace) -> int:
    count = getattr(args, "stop_after_task_count", 1)
    if count <= 0:
        raise ValueError("--stop-after-task-count must be positive")
    return count


def _validate_runtime_stop_after_task(
    config: CoinbaseApplicationConfig,
    stop_after_task: RuntimeTask | None,
) -> None:
    if stop_after_task is None:
        return
    enabled_tasks = {schedule.task_id for schedule in config.bot.enabled_schedules()}
    if stop_after_task not in enabled_tasks:
        raise ValueError(f"--stop-after-task target is not enabled by config: {stop_after_task.value}")


def _record_runtime_health_check_result(
    config: CoinbaseApplicationConfig,
    health_payload: dict[str, JsonValue],
) -> None:
    checks = health_payload.get("checks")
    attention_checks: list[str] = []
    if isinstance(checks, list):
        for check in checks:
            if not isinstance(check, dict):
                continue
            if check.get("status") != LedgerHealthStatus.OK.value:
                name = check.get("name")
                if isinstance(name, str):
                    attention_checks.append(name)

    AuditCore(AuditLedger(config.ledger_path)).emit(
        EventType.RUNTIME_HEALTH_CHECK_RESULT,
        {
            "attention_check_count": len(attention_checks),
            "attention_checks": attention_checks,
            "checked_health_status": health_payload.get("status"),
            "checked_through_sequence": health_payload.get("last_sequence"),
            "ledger_path": str(config.ledger_path),
            "record_count": health_payload.get("record_count"),
            "schema_version": 1,
        },
    )


def _verify_s3_anchor_receipt_payload(receipt: LedgerAnchorReceipt) -> dict[str, JsonValue]:
    return verify_s3_object_lock_anchor_receipt(receipt).to_payload()


def _verify_s3_archive_receipt_payload(receipt: LedgerArchiveReceipt) -> dict[str, JsonValue]:
    return verify_s3_object_lock_ledger_archive_receipt(receipt).to_payload()


def main() -> int:
    return asyncio.run(run_from_args(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
