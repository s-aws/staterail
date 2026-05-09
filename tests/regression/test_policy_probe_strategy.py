from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from audit.ledger import AuditLedger
from core.enums import ExecutionMode, ProductType, ProductVenue
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from strategies import (
    POLICY_PROBE_STRATEGY_ID,
    PolicyProbeStrategy,
    StrategySnapshot,
    configured_strategies,
    load_operator_policy_from_json_file,
)


def test_policy_probe_strategy_reports_policy_and_product_metadata(workspace_tmp_path):
    policy = load_operator_policy_from_json_file(
        Path("docs/examples/operator-policy.conservative-cfm-v0.json")
    )
    catalog = ProductCatalog(
        (
            ProductMetadata(
                product_id="SHB-26JUN26-CDE",
                product_type=ProductType.FUTURE,
                product_venue=ProductVenue.FCM,
            ),
        )
    )
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=ledger.path,
        operator_policy=policy,
        product_catalog=catalog,
        projection=SourceOfTruthProjection.from_ledger(ledger),
    )

    decision = PolicyProbeStrategy().evaluate(snapshot)

    assert decision.intents == ()
    assert decision.metadata["operator_policy_configured"] is True
    assert decision.metadata["operator_policy"]["policy_name"] == "conservative_cfm_policy_v0"
    assert decision.metadata["operator_policy"]["products"] == [
        "SHB-26JUN26-CDE",
        "AVA-29MAY26-CDE",
    ]
    assert decision.metadata["policy_product_metadata"] == [
        {
            "present": True,
            "product_id": "SHB-26JUN26-CDE",
            "product_type": ProductType.FUTURE.value,
            "product_venue": ProductVenue.FCM.value,
            "tradable_for_new_orders": True,
        },
        {
            "present": False,
            "product_id": "AVA-29MAY26-CDE",
            "product_type": None,
            "product_venue": None,
            "tradable_for_new_orders": None,
        },
    ]


def test_policy_probe_strategy_is_available_as_builtin_strategy():
    strategies = configured_strategies((POLICY_PROBE_STRATEGY_ID,))

    assert [strategy.strategy_id for strategy in strategies] == [POLICY_PROBE_STRATEGY_ID]
