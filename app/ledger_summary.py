from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.ledger_config import latest_ledger_application_config
from app.ledger_view import load_verified_ledger_view
from core.enums import ActionStatus, OrderLifecycleStatus
from core.json_tools import JsonValue, normalize_json


@dataclass(frozen=True)
class LedgerSummary:
    ledger_path: Path
    verified: bool
    audit_anchor_count: int
    audit_archive_count: int
    audit_checkpoint_count: int
    next_sequence: int
    last_hash: str
    record_count: int
    last_sequence: int
    last_record_hash: str | None
    action_count: int
    accepted_data_count: int
    duplicate_data_count: int
    error_count: int
    exchange_balance_count: int
    exchange_position_count: int
    exchange_product_count: int
    exchange_product_snapshot_count: int
    exchange_request_retry_count: int
    execution_unknown_order_count: int
    failed_action_count: int
    feed_degraded_count: int
    feed_source_count: int
    fill_count: int
    market_order_book_count: int
    market_ticker_count: int
    market_trade_count: int
    live_preflight_result_count: int
    latest_live_preflight_sequence: int | None
    latest_strategy_simulation_sequence: int | None
    open_order_count: int
    order_count: int
    passive_market_making_quote_count: int
    passive_market_making_released_quote_count: int
    passive_market_making_unreleased_quote_count: int
    position_count: int
    reconciliation_drift_count: int
    reconciliation_mismatch_count: int
    reconciliation_recovery_count: int
    runtime_task_count: int
    strategy_evaluation_count: int
    strategy_simulation_result_count: int
    system_start_count: int
    system_stop_count: int
    trigger_count: int
    latest_config_fingerprint: str | None = None
    latest_config_fingerprint_algorithm: str | None = None
    latest_config_schema_version: int | None = None
    latest_runtime_health_check_sequence: int | None = None
    latest_runtime_health_check_status: str | None = None
    runtime_health_check_result_count: int = 0

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "accepted_data_count": self.accepted_data_count,
            "action_count": self.action_count,
            "audit_anchor_count": self.audit_anchor_count,
            "audit_archive_count": self.audit_archive_count,
            "audit_checkpoint_count": self.audit_checkpoint_count,
            "duplicate_data_count": self.duplicate_data_count,
            "error_count": self.error_count,
            "exchange_balance_count": self.exchange_balance_count,
            "exchange_position_count": self.exchange_position_count,
            "exchange_product_count": self.exchange_product_count,
            "exchange_product_snapshot_count": self.exchange_product_snapshot_count,
            "exchange_request_retry_count": self.exchange_request_retry_count,
            "execution_unknown_order_count": self.execution_unknown_order_count,
            "failed_action_count": self.failed_action_count,
            "feed_degraded_count": self.feed_degraded_count,
            "feed_source_count": self.feed_source_count,
            "fill_count": self.fill_count,
            "last_hash": self.last_hash,
            "last_record_hash": self.last_record_hash,
            "last_sequence": self.last_sequence,
            "latest_config_fingerprint": self.latest_config_fingerprint,
            "latest_config_fingerprint_algorithm": self.latest_config_fingerprint_algorithm,
            "latest_config_schema_version": self.latest_config_schema_version,
            "latest_live_preflight_sequence": self.latest_live_preflight_sequence,
            "latest_runtime_health_check_sequence": self.latest_runtime_health_check_sequence,
            "latest_runtime_health_check_status": self.latest_runtime_health_check_status,
            "latest_strategy_simulation_sequence": self.latest_strategy_simulation_sequence,
            "ledger_path": self.ledger_path.as_posix(),
            "live_preflight_result_count": self.live_preflight_result_count,
            "market_order_book_count": self.market_order_book_count,
            "market_ticker_count": self.market_ticker_count,
            "market_trade_count": self.market_trade_count,
            "next_sequence": self.next_sequence,
            "open_order_count": self.open_order_count,
            "order_count": self.order_count,
            "passive_market_making_quote_count": self.passive_market_making_quote_count,
            "passive_market_making_released_quote_count": (
                self.passive_market_making_released_quote_count
            ),
            "passive_market_making_unreleased_quote_count": (
                self.passive_market_making_unreleased_quote_count
            ),
            "position_count": self.position_count,
            "reconciliation_drift_count": self.reconciliation_drift_count,
            "reconciliation_mismatch_count": self.reconciliation_mismatch_count,
            "reconciliation_recovery_count": self.reconciliation_recovery_count,
            "record_count": self.record_count,
            "runtime_health_check_result_count": self.runtime_health_check_result_count,
            "runtime_task_count": self.runtime_task_count,
            "strategy_evaluation_count": self.strategy_evaluation_count,
            "strategy_simulation_result_count": self.strategy_simulation_result_count,
            "system_start_count": self.system_start_count,
            "system_stop_count": self.system_stop_count,
            "trigger_count": self.trigger_count,
            "verified": self.verified,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Ledger summary payload must normalize to an object")
        return normalized


def summarize_ledger(path: str | Path) -> LedgerSummary:
    view = load_verified_ledger_view(path)
    state = view.state
    records = view.records
    projection = view.projection
    latest_config = latest_ledger_application_config(projection)
    passive_quotes = projection.passive_market_making_quotes
    latest_runtime_health_check = (
        projection.runtime_health_check_results[-1]
        if projection.runtime_health_check_results
        else None
    )

    return LedgerSummary(
        accepted_data_count=projection.accepted_data_count,
        action_count=len(projection.actions),
        audit_anchor_count=view.audit_anchor_count,
        audit_archive_count=view.audit_archive_count,
        audit_checkpoint_count=view.audit_checkpoint_count,
        duplicate_data_count=projection.duplicate_data_count,
        error_count=projection.error_count,
        exchange_balance_count=len(projection.exchange_balances_by_account_id),
        exchange_position_count=len(projection.exchange_positions_by_venue_product),
        exchange_product_count=projection.exchange_product_count,
        exchange_product_snapshot_count=projection.exchange_product_snapshot_count,
        exchange_request_retry_count=len(projection.exchange_request_retries),
        execution_unknown_order_count=sum(
            1
            for order in projection.orders_by_action_id.values()
            if order.lifecycle_status == OrderLifecycleStatus.EXECUTION_UNKNOWN
        ),
        failed_action_count=sum(
            1 for action in projection.actions.values() if action.status == ActionStatus.FAILED
        ),
        feed_degraded_count=projection.feed_degraded_count,
        feed_source_count=len(projection.feed_sources),
        fill_count=projection.fill_count,
        last_hash=state.last_hash,
        last_record_hash=projection.last_record_hash,
        last_sequence=projection.last_sequence,
        latest_config_fingerprint=latest_config.fingerprint,
        latest_config_fingerprint_algorithm=latest_config.fingerprint_algorithm,
        latest_config_schema_version=latest_config.schema_version,
        ledger_path=view.ledger_path,
        latest_live_preflight_sequence=(
            projection.live_preflight_results[-1].sequence
            if projection.live_preflight_results
            else None
        ),
        latest_runtime_health_check_sequence=(
            latest_runtime_health_check.sequence
            if latest_runtime_health_check is not None
            else None
        ),
        latest_runtime_health_check_status=(
            latest_runtime_health_check.checked_health_status.value
            if latest_runtime_health_check is not None
            and latest_runtime_health_check.checked_health_status is not None
            else None
        ),
        latest_strategy_simulation_sequence=(
            projection.strategy_simulation_results[-1].sequence
            if projection.strategy_simulation_results
            else None
        ),
        live_preflight_result_count=len(projection.live_preflight_results),
        market_order_book_count=len(projection.order_books_by_product_id),
        market_ticker_count=len(projection.latest_tickers_by_product_id),
        market_trade_count=projection.market_trade_count,
        next_sequence=state.next_sequence,
        open_order_count=len(projection.open_orders),
        order_count=len(projection.orders_by_action_id),
        passive_market_making_quote_count=len(passive_quotes),
        passive_market_making_released_quote_count=sum(
            1 for quote in passive_quotes if quote.released
        ),
        passive_market_making_unreleased_quote_count=len(
            projection.unreleased_passive_market_making_quotes
        ),
        position_count=len(projection.positions_by_product_id),
        reconciliation_drift_count=projection.reconciliation_drift_count,
        reconciliation_mismatch_count=projection.reconciliation_mismatch_count,
        reconciliation_recovery_count=projection.reconciliation_recovery_count,
        record_count=len(records),
        runtime_health_check_result_count=len(projection.runtime_health_check_results),
        runtime_task_count=len(projection.runtime_tasks),
        strategy_evaluation_count=len(projection.strategy_evaluations),
        strategy_simulation_result_count=len(projection.strategy_simulation_results),
        system_start_count=len(projection.system_starts),
        system_stop_count=len(projection.system_stops),
        trigger_count=len(projection.trigger_sequences),
        verified=True,
    )


def ledger_summary_payload(path: str | Path) -> dict[str, JsonValue]:
    return summarize_ledger(path).to_payload()
