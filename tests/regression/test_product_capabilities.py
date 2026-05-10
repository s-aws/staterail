from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from core.enums import (
    ExecutionMode,
    OperatorPolicyVenue,
    OrderType,
    ProductType,
    ProductVenue,
    StrategyHelperStatus,
    TimeInForce,
    VenueCapabilityRequirement,
    VenueContractRequirementSet,
)
from products.capabilities import (
    CFM_LIVE_ORDER_ROUTING_REQUIREMENTS,
    LIVE_ORDER_ROUTING_REQUIREMENTS,
    product_capabilities,
    venue_capabilities,
    venue_contract_report,
)
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
    assert cfm.supports_product_metadata_lookup is True
    assert cfm.supports_market_data_websocket is True
    assert cfm.supports_user_order_websocket is True
    assert cfm.supports_place_orders is True
    assert cfm.supports_cancel_orders is True
    assert cfm.supports_order_lookup is True
    assert cfm.supports_fill_lookup is True
    assert cfm.supports_account_lookup is True
    assert cfm.supports_position_lookup is True
    assert cfm.supports_amend is False
    assert cfm.supports_reduce_only is False
    assert cfm.supports_attached_orders is False
    assert cfm.supported_order_types == (OrderType.LIMIT, OrderType.MARKET)
    assert cfm.supported_time_in_force == (
        TimeInForce.GOOD_UNTIL_CANCELLED,
        TimeInForce.IMMEDIATE_OR_CANCEL,
        TimeInForce.FILL_OR_KILL,
    )
    assert cfm.to_payload()["supports_fill_lookup"] is True
    assert cfm.to_payload()["supports_position_lookup"] is True
    assert spot.supports_live_execution is True
    assert spot.supports_product_metadata_lookup is True
    assert spot.supports_position_lookup is False
    assert intx.status == StrategyHelperStatus.OK
    assert intx.supports_live_execution is False
    assert intx.supports_product_metadata_lookup is True
    assert intx.supports_order_lookup is False
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


def test_venue_contract_report_checks_required_capabilities():
    spot = venue_contract_report(ProductVenue.CBE)
    cfm = venue_contract_report(
        OperatorPolicyVenue.COINBASE_CFM,
        requirements=CFM_LIVE_ORDER_ROUTING_REQUIREMENTS,
    )
    intx = venue_contract_report(ProductVenue.INTX)
    metadata_only = venue_contract_report(
        ProductVenue.INTX,
        requirements=(VenueCapabilityRequirement.PRODUCT_METADATA_LOOKUP,),
    )

    assert spot.is_ok
    assert spot.missing_requirements == ()
    assert [check.requirement for check in spot.checks] == list(LIVE_ORDER_ROUTING_REQUIREMENTS)
    assert VenueCapabilityRequirement.LIMIT_ORDERS in LIVE_ORDER_ROUTING_REQUIREMENTS
    assert VenueCapabilityRequirement.POST_ONLY in LIVE_ORDER_ROUTING_REQUIREMENTS
    assert (
        VenueCapabilityRequirement.GOOD_UNTIL_CANCELLED_TIME_IN_FORCE
        in LIVE_ORDER_ROUTING_REQUIREMENTS
    )
    assert cfm.is_ok
    assert cfm.missing_requirements == ()
    assert intx.status == StrategyHelperStatus.MISSING
    assert VenueCapabilityRequirement.PRODUCT_METADATA_LOOKUP not in intx.missing_requirements
    assert VenueCapabilityRequirement.LIVE_EXECUTION in intx.missing_requirements
    assert VenueCapabilityRequirement.LIMIT_ORDERS in intx.missing_requirements
    assert VenueCapabilityRequirement.POST_ONLY in intx.missing_requirements
    assert (
        VenueCapabilityRequirement.GOOD_UNTIL_CANCELLED_TIME_IN_FORCE
        in intx.missing_requirements
    )
    assert metadata_only.is_ok
    assert metadata_only.missing_requirements == ()
    assert metadata_only.to_payload()["checks"] == [
        {
            "requirement": VenueCapabilityRequirement.PRODUCT_METADATA_LOOKUP.value,
            "supported": True,
        }
    ]


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


def test_cli_venue_contract_report_prints_cfm_contract(capsys):
    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                venue_contract_fail_on_missing=True,
                venue_contract_report=True,
                venue_contract_requirement_set=VenueContractRequirementSet.CFM_LIVE_ORDER_ROUTING.value,
                venue_contract_venue=OperatorPolicyVenue.COINBASE_CFM.value,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == StrategyHelperStatus.OK.value
    assert payload["read_only"] is True
    assert payload["writes_ledger"] is False
    assert payload["requirement_set"] == VenueContractRequirementSet.CFM_LIVE_ORDER_ROUTING.value
    assert payload["venue"] == OperatorPolicyVenue.COINBASE_CFM.value
    assert payload["missing_requirements"] == []
    assert {
        "requirement": VenueCapabilityRequirement.POSITION_LOOKUP.value,
        "supported": True,
    } in payload["checks"]
    assert {
        "requirement": VenueCapabilityRequirement.LIMIT_ORDERS.value,
        "supported": True,
    } in payload["checks"]
    assert {
        "requirement": VenueCapabilityRequirement.POST_ONLY.value,
        "supported": True,
    } in payload["checks"]
    assert {
        "requirement": VenueCapabilityRequirement.GOOD_UNTIL_CANCELLED_TIME_IN_FORCE.value,
        "supported": True,
    } in payload["checks"]


def test_cli_venue_contract_report_can_fail_on_missing_requirements(capsys):
    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                venue_contract_fail_on_missing=True,
                venue_contract_report=True,
                venue_contract_requirement_set=VenueContractRequirementSet.LIVE_ORDER_ROUTING.value,
                venue_contract_venue=ProductVenue.INTX.value,
            )
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == StrategyHelperStatus.MISSING.value
    assert VenueCapabilityRequirement.LIVE_EXECUTION.value in payload["missing_requirements"]
