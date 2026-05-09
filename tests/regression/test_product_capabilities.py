from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from core.enums import (
    ExecutionMode,
    OperatorPolicyVenue,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyHelperStatus,
    TimeInForce,
)
from products.capabilities import product_capabilities, venue_capabilities
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from strategies import StrategySnapshot


def test_venue_capabilities_describe_current_coinbase_adapter_contract():
    cfm = venue_capabilities(OperatorPolicyVenue.COINBASE_CFM)
    spot = venue_capabilities(ProductVenue.CBE)
    intx = venue_capabilities(ProductVenue.INTX)
    unknown = venue_capabilities("unknown")

    assert cfm.is_ok
    assert cfm.product_venues == (ProductVenue.FCM,)
    assert cfm.supports_live_execution is True
    assert cfm.supports_place_orders is True
    assert cfm.supports_cancel_orders is True
    assert cfm.supports_amend is False
    assert cfm.supports_reduce_only is False
    assert cfm.supports_attached_orders is False
    assert cfm.supported_order_types == (OrderType.LIMIT, OrderType.MARKET)
    assert cfm.supported_time_in_force == (
        TimeInForce.GOOD_UNTIL_CANCELLED,
        TimeInForce.IMMEDIATE_OR_CANCEL,
        TimeInForce.FILL_OR_KILL,
    )
    assert spot.supports_live_execution is True
    assert intx.status == StrategyHelperStatus.OK
    assert intx.supports_live_execution is False
    assert unknown.status == StrategyHelperStatus.MISSING
    assert unknown.reason == "unknown_venue"


def test_product_capabilities_apply_product_metadata_to_venue_contract():
    catalog = ProductCatalog(
        (
            ProductMetadata(
                limit_only=True,
                product_id="AVA-29MAY26-CDE",
                product_type=ProductType.FUTURE,
                product_venue=ProductVenue.FCM,
            ),
            ProductMetadata(
                cancel_only=True,
                product_id="BTC-USD",
                product_type=ProductType.SPOT,
                product_venue=ProductVenue.CBE,
            ),
        )
    )

    future = product_capabilities(catalog, "AVA-29MAY26-CDE")
    cancel_only = product_capabilities(catalog, "BTC-USD")
    missing = product_capabilities(catalog, "MISSING-USD")
    no_catalog = product_capabilities(None, "BTC-USD")

    assert future.is_ok
    assert future.product_type == ProductType.FUTURE
    assert future.product_venue == ProductVenue.FCM
    assert future.supports_place_orders is True
    assert future.supports_cancel_orders is True
    assert future.supports_market_orders is False
    assert future.supported_order_types == (OrderType.LIMIT,)
    assert future.venue_capabilities is not None
    assert future.venue_capabilities.product_venues == (ProductVenue.FCM,)
    assert cancel_only.tradable_for_new_orders is False
    assert cancel_only.supports_place_orders is False
    assert cancel_only.supports_cancel_orders is True
    assert cancel_only.supported_order_types == ()
    assert missing.status == StrategyHelperStatus.MISSING
    assert missing.reason == "product_metadata_missing"
    assert no_catalog.status == StrategyHelperStatus.MISSING
    assert no_catalog.reason == "product_catalog_missing"


def test_strategy_snapshot_exposes_venue_and_product_capabilities():
    catalog = ProductCatalog(
        (
            ProductMetadata(
                product_id="BTC-USD",
                product_type=ProductType.SPOT,
                product_venue=ProductVenue.CBE,
            ),
        )
    )
    snapshot = StrategySnapshot(
        as_of_sequence=0,
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=Path("audit.jsonl"),
        product_catalog=catalog,
        projection=SourceOfTruthProjection(),
    )

    assert snapshot.venue_capabilities(ProductVenue.CBE).supports_post_only is True
    assert snapshot.product_capabilities("BTC-USD").supports_market_orders is True
    assert snapshot.product_capabilities("UNKNOWN-USD").status == StrategyHelperStatus.MISSING
