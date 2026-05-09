from __future__ import annotations

from decimal import Decimal

import pytest

from core.enums import OperatorPolicyDistanceType
from strategies import (
    adaptive_reveal_size,
    anchored_price,
    slide_price_toward,
    tranche_release_sizes,
    visible_notional,
)


def test_tranche_release_sizes_derive_incremental_order_sizes():
    sizes = tranche_release_sizes(
        total_size="4",
        tranche_schedule=(
            Decimal("0.25"),
            Decimal("0.50"),
            Decimal("0.75"),
            Decimal("1.0"),
        ),
    )

    assert sizes == (
        Decimal("1.00"),
        Decimal("1.00"),
        Decimal("1.00"),
        Decimal("1.00"),
    )


def test_adaptive_reveal_size_matches_policy_example_and_honors_cap():
    output = adaptive_reveal_size(
        base_size="2",
        baseline_volume="50",
        market_volume="100",
        reveal_multiplier="0.1",
        max_reveal_percentage="0.5",
    )
    capped = adaptive_reveal_size(
        base_size="2",
        baseline_volume="1",
        market_volume="100",
        reveal_multiplier="1",
        max_reveal_percentage="0.5",
    )

    assert output == Decimal("0.4")
    assert capped == Decimal("1.0")


def test_slide_price_toward_moves_one_configured_step():
    assert slide_price_toward(
        current_price="100",
        desired_price="90",
        max_step="5",
    ) == Decimal("95")
    assert slide_price_toward(
        current_price="100",
        desired_price="103",
        max_step="5",
    ) == Decimal("103")


def test_anchored_price_clamps_to_percent_band_and_can_slide():
    assert anchored_price(
        current_price="100",
        distance_type=OperatorPolicyDistanceType.PERCENT,
        max_distance="0.05",
        reference_price="105",
    ) == Decimal("100")
    assert anchored_price(
        current_price="90",
        distance_type=OperatorPolicyDistanceType.PERCENT,
        max_distance="0.05",
        reference_price="105",
    ) == Decimal("99.75")
    assert anchored_price(
        current_price="90",
        distance_type=OperatorPolicyDistanceType.PERCENT,
        max_distance="0.05",
        reference_price="105",
        slide_mode=True,
        max_step_per_reprice="5",
    ) == Decimal("95")


def test_visible_notional_and_policy_calculations_reject_invalid_values():
    assert visible_notional(size="2", price="10") == Decimal("20")
    with pytest.raises(ValueError, match="strictly increasing"):
        tranche_release_sizes(
            total_size="4",
            tranche_schedule=(Decimal("0.50"), Decimal("0.50"), Decimal("1")),
        )
    with pytest.raises(ValueError, match="max_step_per_reprice"):
        anchored_price(
            current_price="90",
            distance_type=OperatorPolicyDistanceType.PERCENT,
            max_distance="0.05",
            reference_price="105",
            slide_mode=True,
        )
