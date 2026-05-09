from __future__ import annotations

from datetime import datetime, timezone

from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ErrorCode,
    EventType,
    ExchangeLookupStatus,
    ProductVenue,
    ReconciliationIssue,
)
from exchanges.coinbase.advanced_trade_rest import CoinbaseAccountsLookupResult, CoinbasePositionsLookupResult
from projections.state import SourceOfTruthProjection
from reconciliation.positions import (
    ExchangeStateReconciliation,
    ExchangeStateReconciliationPolicy,
)


class FixedTestClock:
    def now(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeAccountLookupClient:
    def __init__(self, results: list[CoinbaseAccountsLookupResult]) -> None:
        self._results = results
        self.requests: list[dict[str, object]] = []

    def list_accounts(
        self,
        *,
        cursor: str | None = None,
        limit: int = 250,
        retail_portfolio_id: str | None = None,
    ) -> CoinbaseAccountsLookupResult:
        self.requests.append(
            {
                "cursor": cursor,
                "limit": limit,
                "retail_portfolio_id": retail_portfolio_id,
            }
        )
        return self._results.pop(0)


class FakePositionLookupClient:
    def __init__(
        self,
        *,
        cfm_result: CoinbasePositionsLookupResult,
        intx_result: CoinbasePositionsLookupResult | None = None,
    ) -> None:
        self._cfm_result = cfm_result
        self._intx_result = intx_result
        self.calls: list[str] = []

    def list_us_futures_positions(self) -> CoinbasePositionsLookupResult:
        self.calls.append("cfm")
        return self._cfm_result

    def list_perpetual_positions(self, portfolio_uuid: str) -> CoinbasePositionsLookupResult:
        self.calls.append(f"intx:{portfolio_uuid}")
        if self._intx_result is None:
            raise AssertionError("Unexpected intx lookup")
        return self._intx_result


def test_exchange_state_reconciliation_snapshots_balances_and_positions(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    account_client = FakeAccountLookupClient(
        [
            CoinbaseAccountsLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                accounts=(
                    {
                        "account_id": "account-1",
                        "available": "10",
                        "currency": "USDC",
                        "hold": "1",
                        "venue": ProductVenue.CBE.value,
                    },
                ),
            )
        ]
    )
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "0.01",
                    "product_id": "BTC-PERP-INTX",
                    "venue": ProductVenue.FCM.value,
                },
            ),
        )
    )

    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "order_id": "exchange-1",
            "price": "100000",
            "product_id": "BTC-PERP-INTX",
            "side": "BUY",
            "size": "0.01",
        },
    )
    result = ExchangeStateReconciliation(
        core,
        account_lookup_client=account_client,
        clock=FixedTestClock(),
        position_lookup_client=position_client,
    ).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert result.balance_snapshots == 1
    assert result.position_snapshots == 1
    assert result.drift_count == 0
    assert result.new_drift_record_count == 0
    assert projection.exchange_balances_by_account_id["account-1"].available == "10"
    assert projection.exchange_positions_by_venue_product[
        (ProductVenue.FCM, "BTC-PERP-INTX")
    ].net_size == "0.01"


def test_exchange_state_reconciliation_audits_position_size_drift(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "0.02",
                    "product_id": "BTC-PERP-INTX",
                    "venue": ProductVenue.FCM.value,
                },
            ),
        )
    )

    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "price": "100000",
            "product_id": "BTC-PERP-INTX",
            "side": "BUY",
            "size": "0.01",
        },
    )
    result = ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        position_lookup_client=position_client,
    ).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    drift = next(iter(projection.reconciliation_drifts.values()))

    assert result.drift_count == 1
    assert result.new_drift_record_count == 1
    assert ledger.iter_records()[-1].event_type == EventType.RECONCILIATION_DRIFT
    assert drift.issue == ReconciliationIssue.POSITION_SIZE_DRIFT
    assert drift.payload["bot_net_size"] == "0.01"
    assert drift.payload["exchange_net_size"] == "0.02"
    assert projection.reconciliation_drift_count == 1


def test_exchange_state_reconciliation_suppresses_duplicate_drift_after_restart(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "price": "100000",
            "product_id": "BTC-PERP-INTX",
            "side": "BUY",
            "size": "0.01",
        },
    )

    first_client = _drift_position_client()
    assert ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        position_lookup_client=first_client,
    ).reconcile().drift_count == 1

    restarted_core = AuditCore(AuditLedger(ledger.path, clock=FixedTestClock()))
    second_client = _drift_position_client()
    restarted_result = ExchangeStateReconciliation(
        restarted_core,
        clock=FixedTestClock(),
        position_lookup_client=second_client,
    ).reconcile()
    assert restarted_result.drift_count == 1
    assert restarted_result.new_drift_record_count == 0
    assert [
        record.event_type for record in ledger.iter_records() if record.event_type == EventType.RECONCILIATION_DRIFT
    ] == [EventType.RECONCILIATION_DRIFT]


def test_exchange_state_reconciliation_audits_missing_positions(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "price": "100000",
            "product_id": "BTC-PERP-INTX",
            "side": "BUY",
            "size": "0.01",
        },
    )
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "0.5",
                    "product_id": "ETH-PERP-INTX",
                    "venue": ProductVenue.FCM.value,
                },
            ),
        )
    )

    result = ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        position_lookup_client=position_client,
    ).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    issues = {drift.issue for drift in projection.reconciliation_drifts.values()}

    assert result.drift_count == 2
    assert result.new_drift_record_count == 2
    assert issues == {
        ReconciliationIssue.BOT_POSITION_MISSING,
        ReconciliationIssue.EXCHANGE_POSITION_MISSING,
    }


def test_exchange_state_reconciliation_scopes_drift_to_position_product_ids(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "price": "100000",
            "product_id": "ETH-PERP-INTX",
            "side": "BUY",
            "size": "0.01",
        },
    )
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "0.5",
                    "product_id": "BTC-PERP-INTX",
                    "venue": ProductVenue.FCM.value,
                },
                {
                    "net_size": "2",
                    "product_id": "DOGE-PERP-INTX",
                    "venue": ProductVenue.FCM.value,
                },
            ),
        )
    )

    result = ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        policy=ExchangeStateReconciliationPolicy(position_product_ids=("BTC-PERP-INTX",)),
        position_lookup_client=position_client,
    ).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    drift_products = [drift.product_id for drift in projection.reconciliation_drifts.values()]

    assert result.position_snapshots == 2
    assert result.drift_count == 1
    assert result.new_drift_record_count == 1
    assert drift_products == ["BTC-PERP-INTX"]


def test_exchange_state_reconciliation_logs_lookup_failures_without_false_drift(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    core.emit(
        EventType.EXCHANGE_FILL,
        {
            "fill_id": "fill-1",
            "price": "100000",
            "product_id": "BTC-PERP-INTX",
            "side": "BUY",
            "size": "0.01",
        },
    )
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FAILED,
            status_code=500,
            error_code="http_500",
            error_message="server error",
        )
    )

    result = ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        position_lookup_client=position_client,
    ).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert result.error_count == 1
    assert result.drift_count == 0
    assert result.new_drift_record_count == 0
    assert projection.error_count == 1
    assert projection.reconciliation_drift_count == 0


def test_exchange_state_reconciliation_logs_and_skips_invalid_balance_snapshot(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    account_client = FakeAccountLookupClient(
        [
            CoinbaseAccountsLookupResult(
                status=ExchangeLookupStatus.FOUND,
                status_code=200,
                accounts=(
                    {
                        "account_id": "account-1",
                        "available": "10",
                    },
                ),
            )
        ]
    )

    result = ExchangeStateReconciliation(
        core,
        account_lookup_client=account_client,
        clock=FixedTestClock(),
    ).reconcile()
    records = ledger.iter_records()

    assert result.balance_snapshots == 0
    assert result.error_count == 1
    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_code"] == ErrorCode.EXCHANGE_STATE_SNAPSHOT_INVALID.value
    assert records[-1].payload["error"]["context"]["event_type"] == EventType.EXCHANGE_BALANCE_SNAPSHOT.value
    assert set(records[-1].payload["error"]["context"]["missing_fields"]) == {"currency", "venue"}
    assert not any(record.event_type == EventType.EXCHANGE_BALANCE_SNAPSHOT for record in records)


def test_exchange_state_reconciliation_logs_and_skips_invalid_position_snapshot(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "not-decimal",
                },
            ),
        )
    )

    result = ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        position_lookup_client=position_client,
    ).reconcile()
    records = ledger.iter_records()

    assert result.position_snapshots == 0
    assert result.error_count == 1
    assert records[-1].event_type == EventType.ERROR
    assert records[-1].payload["error_code"] == ErrorCode.EXCHANGE_STATE_SNAPSHOT_INVALID.value
    assert records[-1].payload["error"]["context"]["event_type"] == EventType.EXCHANGE_POSITION_SNAPSHOT.value
    assert set(records[-1].payload["error"]["context"]["missing_fields"]) == {"product_id"}
    assert set(records[-1].payload["error"]["context"]["invalid_fields"]) == {"net_size"}
    assert not any(record.event_type == EventType.EXCHANGE_POSITION_SNAPSHOT for record in records)


def test_exchange_state_reconciliation_can_query_perpetual_positions(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=FixedTestClock())
    core = AuditCore(ledger)
    position_client = FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(),
        ),
        intx_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "0.01",
                    "product_id": "BTC-PERP-INTX",
                    "venue": ProductVenue.INTX.value,
                },
            ),
        ),
    )

    result = ExchangeStateReconciliation(
        core,
        clock=FixedTestClock(),
        policy=ExchangeStateReconciliationPolicy(perpetual_portfolio_uuid="portfolio-1"),
        position_lookup_client=position_client,
    ).reconcile()
    projection = SourceOfTruthProjection.from_ledger(ledger)

    assert position_client.calls == ["cfm", "intx:portfolio-1"]
    assert result.position_snapshots == 1
    assert projection.exchange_positions_by_venue_product[
        (ProductVenue.INTX, "BTC-PERP-INTX")
    ].net_size == "0.01"


def _drift_position_client() -> FakePositionLookupClient:
    return FakePositionLookupClient(
        cfm_result=CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=200,
            positions=(
                {
                    "net_size": "0.02",
                    "product_id": "BTC-PERP-INTX",
                    "venue": ProductVenue.FCM.value,
                },
            ),
        )
    )
