from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from core.enums import (
    OrderLineageRelation,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OrderSizingDecisionStatus,
    ProductType,
    ProductVenue,
)
from orders.lineage import (
    LogicalOrderRecord,
    ManualAssociationApproval,
    OrderPlacementRecord,
    manual_association_metadata,
)
from orders.sizing import LineageSizingPolicy
from products.catalog import ProductMetadata


def test_logical_order_record_defaults_root_to_logical_order_id():
    payload = LogicalOrderRecord(
        logical_order_id="logical-1",
        product_id="BTC-USD",
        side=OrderSide.BUY,
        size="1",
        metadata={"note": "staged"},
    ).to_payload()

    assert payload["logical_order_id"] == "logical-1"
    assert payload["root_order_id"] == "logical-1"
    assert payload["lineage_relation"] == OrderLineageRelation.ROOT.value
    assert payload["metadata"] == {"note": "staged"}


def test_logical_order_record_rejects_ambiguous_followup_lineage():
    with pytest.raises(ValueError, match="parent_order_id"):
        LogicalOrderRecord(
            logical_order_id="logical-child",
            root_order_id="logical-parent",
            lineage_relation=OrderLineageRelation.FOLLOWUP_AFTER_FILL,
            product_id="BTC-USD",
            side=OrderSide.SELL,
            size="1",
            source_order_ids=("logical-parent",),
        )


def test_logical_order_record_requires_operator_approval_for_manual_association():
    with pytest.raises(ValueError, match="operator approval"):
        LogicalOrderRecord(
            logical_order_id="manual-1",
            lineage_relation=OrderLineageRelation.MANUAL_ASSOCIATION,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
            source_order_ids=("source-1",),
        )


def test_logical_order_record_accepts_manual_association_approval_metadata():
    approval = ManualAssociationApproval(
        approved_by="operator-1",
        approved_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        reason="Attach externally discovered order to the current lineage root.",
    )

    payload = LogicalOrderRecord(
        logical_order_id="manual-1",
        lineage_relation=OrderLineageRelation.MANUAL_ASSOCIATION,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        size="1",
        source_order_ids=("source-1",),
        metadata=manual_association_metadata(approval),
    ).to_payload()

    approval_payload = payload["metadata"]["manual_association_approval"]
    assert approval_payload["approved_by"] == "operator-1"
    assert approval_payload["approved_at"] == "2026-01-01T00:00:00+00:00"


def test_order_placement_record_allows_staged_without_venue_ids():
    payload = OrderPlacementRecord(
        placement_id="placement-1",
        logical_order_id="logical-1",
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
        placement_status=OrderPlacementStatus.STAGED,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        size="1",
        limit_price="100",
    ).to_payload()

    assert payload["placement_status"] == OrderPlacementStatus.STAGED.value
    assert payload["venue_client_order_id"] is None
    assert payload["exchange_order_id"] is None


def test_order_placement_record_requires_venue_id_after_staging():
    with pytest.raises(ValueError, match="non-staged"):
        OrderPlacementRecord(
            placement_id="placement-1",
            logical_order_id="logical-1",
            placement_kind=OrderPlacementKind.INITIAL,
            placement_status=OrderPlacementStatus.SUBMITTED,
            product_id="BTC-USD",
            side=OrderSide.BUY,
            size="1",
        )


def test_lineage_sizing_policy_accepts_followup_when_product_and_partial_rules_pass():
    policy = LineageSizingPolicy.from_values(
        product=_lineage_product(),
        partial_followup_min_size="1",
        partial_followup_min_fraction="0.25",
    )

    decision = policy.followup_size(parent_size="8", filled_size="2", limit_price="100")

    assert decision.accepted
    assert decision.status == OrderSizingDecisionStatus.ACCEPTED
    assert decision.lineage_relation == OrderLineageRelation.FOLLOWUP_AFTER_FILL
    assert decision.output_sizes == (Decimal("2"),)
    assert decision.single_output_size() == "2"
    assert decision.to_payload()["status"] == OrderSizingDecisionStatus.ACCEPTED.value
    assert decision.to_payload()["output_sizes"] == ["2"]


def test_lineage_sizing_policy_rejects_followup_below_configured_partial_threshold():
    policy = LineageSizingPolicy.from_values(
        product=_lineage_product(),
        partial_followup_min_size="1",
        partial_followup_min_fraction="0.25",
    )

    decision = policy.followup_size(parent_size="10", filled_size="1", limit_price="100")

    assert not decision.accepted
    assert decision.status == OrderSizingDecisionStatus.REJECTED
    assert decision.output_sizes == ()
    assert decision.reasons == ("filled_size is below configured partial followup minimum fraction",)
    with pytest.raises(ValueError, match="accepted sizing decision"):
        decision.single_output_size()


def test_lineage_sizing_policy_rejects_split_that_requires_hidden_rounding():
    policy = LineageSizingPolicy.from_values(product=_lineage_product())

    decision = policy.split_sizes(total_size="1.5", child_count=2, limit_price="100")

    assert not decision.accepted
    assert decision.lineage_relation == OrderLineageRelation.SPLIT_CHILD
    assert decision.output_sizes == ()
    assert decision.reasons == ("output size violates product base size rules",)


def test_lineage_sizing_policy_accepts_consolidation_when_product_rules_pass():
    policy = LineageSizingPolicy.from_values(product=_lineage_product())

    decision = policy.consolidated_size(source_sizes=("1", "1.5"), limit_price="100")

    assert decision.accepted
    assert decision.lineage_relation == OrderLineageRelation.CONSOLIDATION
    assert decision.requested_sizes == (Decimal("1"), Decimal("1.5"))
    assert decision.output_sizes == (Decimal("2.5"),)
    assert decision.to_payload()["lineage_relation"] == OrderLineageRelation.CONSOLIDATION.value


def test_lineage_sizing_policy_plans_staged_release_chunks_under_visible_notional():
    policy = LineageSizingPolicy.from_values(product=_lineage_product())

    decision = policy.staged_release_sizes(
        total_size="5",
        limit_price="100",
        max_visible_notional="200",
    )

    assert decision.accepted
    assert decision.lineage_relation == OrderLineageRelation.ROOT
    assert decision.requested_sizes == (Decimal("5"),)
    assert decision.output_sizes == (Decimal("2"), Decimal("2"), Decimal("1"))
    assert all(size * Decimal("100") <= Decimal("200") for size in decision.output_sizes)


def test_lineage_sizing_policy_rebalances_final_staged_release_when_possible():
    policy = LineageSizingPolicy.from_values(product=_lineage_product(base_min_size="2"))

    decision = policy.staged_release_sizes(
        total_size="7",
        limit_price="100",
        max_visible_notional="300",
    )

    assert decision.accepted
    assert decision.output_sizes == (Decimal("3"), Decimal("2"), Decimal("2"))


def test_lineage_sizing_policy_caps_staged_release_chunks_by_product_base_max():
    policy = LineageSizingPolicy.from_values(product=_lineage_product(base_max_size="3"))

    decision = policy.staged_release_sizes(
        total_size="8",
        limit_price="100",
        max_visible_notional="1000",
    )

    assert decision.accepted
    assert decision.output_sizes == (Decimal("3"), Decimal("3"), Decimal("2"))


def test_lineage_sizing_policy_rejects_impossible_staged_release_remainder():
    policy = LineageSizingPolicy.from_values(product=_lineage_product(base_min_size="2"))

    decision = policy.staged_release_sizes(
        total_size="5",
        limit_price="100",
        max_visible_notional="200",
    )

    assert not decision.accepted
    assert decision.reasons == ("total_size cannot be split into valid staged releases",)


def test_lineage_sizing_policy_rejects_staged_release_visible_cap_below_minimum():
    policy = LineageSizingPolicy.from_values(product=_lineage_product(base_min_size="2"))

    decision = policy.staged_release_sizes(
        total_size="2",
        limit_price="100",
        max_visible_notional="100",
    )

    assert not decision.accepted
    assert decision.reasons == ("max_visible_notional is below minimum valid release size",)


def test_lineage_sizing_policy_rejects_staged_release_count_limit():
    policy = LineageSizingPolicy.from_values(product=_lineage_product())

    decision = policy.staged_release_sizes(
        total_size="5",
        limit_price="100",
        max_release_count=2,
        max_visible_notional="200",
    )

    assert not decision.accepted
    assert decision.reasons == ("staged release count exceeds configured maximum",)


def _lineage_product(
    *,
    base_increment: str = "0.5",
    base_min_size: str = "1",
    base_max_size: str = "10",
    quote_min_size: str = "50",
    quote_max_size: str = "2000",
) -> ProductMetadata:
    return ProductMetadata(
        product_id="BTC-USD",
        product_type=ProductType.SPOT,
        product_venue=ProductVenue.CBE,
        base_increment=Decimal(base_increment),
        base_min_size=Decimal(base_min_size),
        base_max_size=Decimal(base_max_size),
        quote_min_size=Decimal(quote_min_size),
        quote_max_size=Decimal(quote_max_size),
        price_increment=Decimal("0.01"),
    )
