from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from core.enums import (
    ExecutionMode,
    MarketSeriesMembershipRule,
    MarketSeriesTimeField,
    OrderLifecycleStatus,
    OrderSide,
    ProductType,
    ProductVenue,
    StrategyMarketDataStatus,
)
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import (
    MarketOrderBookSnapshot,
    MarketTradeSnapshot,
    OrderSnapshot,
    SourceOfTruthProjection,
)
from strategies import StrategySnapshot


def test_strategy_snapshot_market_data_helpers_return_replay_derived_state():
    now = datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
    projection = SourceOfTruthProjection()
    projection.order_books_by_product_id["AVA-29MAY26-CDE"] = MarketOrderBookSnapshot(
        ask_levels={"101": "4", "102": "6", "103": "8"},
        best_ask_price="101",
        best_ask_size="4",
        best_bid_price="99",
        best_bid_size="2",
        bid_levels={"99": "2", "98": "5", "97": "7"},
        message_key="book-1",
        observed_at=now - timedelta(seconds=10),
        product_id="AVA-29MAY26-CDE",
        sequence=10,
    )
    projection.order_book_samples_by_product_id["AVA-29MAY26-CDE"] = [
        MarketOrderBookSnapshot(
            ask_levels={"91": "4"},
            best_ask_price="91",
            best_ask_size="4",
            best_bid_price="89",
            best_bid_size="2",
            bid_levels={"89": "2"},
            message_key="book-old",
            observed_at=now - timedelta(minutes=10),
            product_id="AVA-29MAY26-CDE",
            sequence=9,
        ),
        MarketOrderBookSnapshot(
            ask_levels={"101": "4"},
            best_ask_price="101",
            best_ask_size="4",
            best_bid_price="99",
            best_bid_size="2",
            bid_levels={"99": "2"},
            message_key="book-1",
            observed_at=now - timedelta(seconds=60),
            product_id="AVA-29MAY26-CDE",
            sequence=11,
        ),
        MarketOrderBookSnapshot(
            ask_levels={"102": "6"},
            best_ask_price="102",
            best_ask_size="6",
            best_bid_price="100",
            best_bid_size="3",
            bid_levels={"100": "3"},
            message_key="book-2",
            observed_at=now - timedelta(seconds=30),
            product_id="AVA-29MAY26-CDE",
            sequence=12,
        ),
    ]
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="trade-old",
            observed_at=now - timedelta(minutes=10),
            price="90",
            product_id="AVA-29MAY26-CDE",
            sequence=9,
            size="1",
            trade_id="old",
        ),
    )
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="trade-1",
            observed_at=now - timedelta(seconds=60),
            price="100",
            product_id="AVA-29MAY26-CDE",
            sequence=11,
            side=OrderSide.BUY,
            size="2",
            trade_id="trade-1",
        ),
    )
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="trade-2",
            observed_at=now - timedelta(seconds=30),
            price="101",
            product_id="AVA-29MAY26-CDE",
            sequence=12,
            side=OrderSide.SELL,
            size="3",
            trade_id="trade-2",
        ),
    )
    projection.orders_by_action_id["open-buy"] = OrderSnapshot(
        action_id="open-buy",
        lifecycle_status=OrderLifecycleStatus.OPEN,
        product_id="AVA-29MAY26-CDE",
        side=OrderSide.BUY,
    )
    projection.orders_by_action_id["open-sell-other"] = OrderSnapshot(
        action_id="open-sell-other",
        lifecycle_status=OrderLifecycleStatus.OPEN,
        product_id="SHB-26JUN26-CDE",
        side=OrderSide.SELL,
    )
    product = ProductMetadata(
        contract_size=Decimal("10"),
        product_id="AVA-29MAY26-CDE",
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
    )
    snapshot = StrategySnapshot(
        as_of_sequence=12,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=Path("data/audit.jsonl"),
        product_catalog=ProductCatalog((product,)),
        projection=projection,
    )

    top = snapshot.best_bid_ask("AVA-29MAY26-CDE")
    mid = snapshot.midpoint("AVA-29MAY26-CDE")
    market_spread = snapshot.spread("AVA-29MAY26-CDE")
    book_stats = snapshot.order_book_stats("AVA-29MAY26-CDE", levels=2)
    near_book_stats = snapshot.order_book_stats("AVA-29MAY26-CDE", max_distance_bps="150")
    book_window = snapshot.order_book_sample_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )
    book_window_stats = snapshot.order_book_window_stats(
        "AVA-29MAY26-CDE",
        levels=1,
        lookback=timedelta(minutes=5),
    )
    retained_book_window_stats = snapshot.order_book_window_stats(
        "AVA-29MAY26-CDE",
        levels=1,
        lookback=timedelta(minutes=5),
        max_retained_samples=1,
    )
    retained_book_window = snapshot.order_book_sample_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
        max_retained_samples=1,
    )
    latest = snapshot.latest_trade("AVA-29MAY26-CDE")
    series_window = snapshot.market_series_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )
    window = snapshot.trade_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )
    stats = snapshot.market_window_stats(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )
    candles = snapshot.candles(
        "AVA-29MAY26-CDE",
        interval=timedelta(minutes=1),
        lookback=timedelta(minutes=2),
    )
    volume = snapshot.rolling_trade_volume(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )
    count = snapshot.rolling_trade_count(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )

    assert top.status == StrategyMarketDataStatus.OK
    assert top.bid_price == Decimal("99")
    assert top.ask_price == Decimal("101")
    assert mid.midpoint == Decimal("100")
    assert market_spread.spread == Decimal("2")
    assert market_spread.spread_bps == Decimal("200")
    assert book_stats.status == StrategyMarketDataStatus.OK
    assert book_stats.best_bid_price == Decimal("99")
    assert book_stats.best_ask_price == Decimal("101")
    assert book_stats.top_bid_size == Decimal("2")
    assert book_stats.top_ask_size == Decimal("4")
    assert book_stats.spread == Decimal("2")
    assert book_stats.spread_bps == Decimal("200")
    assert book_stats.midpoint == Decimal("100")
    assert book_stats.microprice == Decimal("598") / Decimal("6")
    assert book_stats.weighted_mid == Decimal("602") / Decimal("6")
    assert book_stats.bid_level_count == 2
    assert book_stats.ask_level_count == 2
    assert book_stats.bid_volume == Decimal("7")
    assert book_stats.ask_volume == Decimal("10")
    assert book_stats.bid_notional == Decimal("6880")
    assert book_stats.ask_notional == Decimal("10160")
    assert book_stats.book_imbalance == Decimal("-3") / Decimal("17")
    assert near_book_stats.status == StrategyMarketDataStatus.OK
    assert near_book_stats.bid_level_count == 1
    assert near_book_stats.ask_level_count == 1
    assert near_book_stats.bid_volume == Decimal("2")
    assert near_book_stats.ask_volume == Decimal("4")
    assert near_book_stats.bid_notional == Decimal("1980")
    assert near_book_stats.ask_notional == Decimal("4040")
    assert book_window.status == StrategyMarketDataStatus.OK
    assert book_window.sample_count == 2
    assert book_window.source_sequences == (11, 12)
    assert book_window.to_payload()["sample_sequences"] == [11, 12]
    assert retained_book_window.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert retained_book_window.sample_count == 1
    assert retained_book_window.retention_dropped_sample_count == 1
    assert book_window_stats.status == StrategyMarketDataStatus.OK
    assert book_window_stats.valid_stats_count == 2
    assert book_window_stats.average_spread == Decimal("2")
    assert book_window_stats.average_midpoint == Decimal("100.5")
    assert book_window_stats.average_bid_volume == Decimal("2.5")
    assert book_window_stats.average_ask_volume == Decimal("5")
    assert book_window_stats.average_book_imbalance == Decimal("-1") / Decimal("3")
    assert book_window_stats.average_spread_bps == (
        Decimal("200") + (Decimal("20000") / Decimal("101"))
    ) / Decimal("2")
    assert book_window_stats.to_payload()["valid_stats_count"] == 2
    assert retained_book_window_stats.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert retained_book_window_stats.valid_stats_count == 1
    assert latest.trade_id == "trade-2"
    assert latest.price == Decimal("101")
    assert latest.size == Decimal("3")
    assert latest.side == OrderSide.SELL
    assert series_window.product_id == "AVA-29MAY26-CDE"
    assert series_window.as_of == now
    assert series_window.window_start == now - timedelta(minutes=5)
    assert series_window.window_end == now
    assert series_window.membership_rule == MarketSeriesMembershipRule.START_INCLUSIVE_END_INCLUSIVE
    assert series_window.time_field == MarketSeriesTimeField.OBSERVED_AT
    assert series_window.to_payload()["membership_rule"] == (
        MarketSeriesMembershipRule.START_INCLUSIVE_END_INCLUSIVE.value
    )
    assert window.status == StrategyMarketDataStatus.OK
    assert window.membership_rule == MarketSeriesMembershipRule.START_INCLUSIVE_END_INCLUSIVE
    assert window.time_field == MarketSeriesTimeField.OBSERVED_AT
    assert tuple(trade.trade_id for trade in window.trades) == ("trade-1", "trade-2")
    assert window.available_trade_count == 3
    assert window.timestamped_trade_count == 3
    assert window.trade_count == 2
    assert window.first_sequence == 11
    assert window.last_sequence == 12
    assert window.source_sequences == (11, 12)
    assert stats.status == StrategyMarketDataStatus.OK
    assert stats.aggressor_status == StrategyMarketDataStatus.OK
    assert stats.open == Decimal("100")
    assert stats.high == Decimal("101")
    assert stats.low == Decimal("100")
    assert stats.close == Decimal("101")
    assert stats.base_volume == Decimal("5")
    assert stats.quote_volume == Decimal("503")
    assert stats.buy_aggressor_volume == Decimal("2")
    assert stats.sell_aggressor_volume == Decimal("3")
    assert stats.buy_aggressor_quote_volume == Decimal("200")
    assert stats.sell_aggressor_quote_volume == Decimal("303")
    assert stats.net_aggressor_volume == Decimal("-1")
    assert stats.net_aggressor_quote_volume == Decimal("-103")
    assert stats.aggressor_imbalance == Decimal("-0.2")
    assert stats.vwap == Decimal("100.6")
    assert stats.twap == Decimal("100.5")
    assert stats.realized_volatility == Decimal("0")
    assert stats.classified_trade_count == 2
    assert stats.unclassified_trade_count == 0
    assert volume.status == StrategyMarketDataStatus.OK
    assert volume.trade_count == 2
    assert volume.valid_trade_count == 2
    assert volume.base_volume == Decimal("5")
    assert volume.quote_volume == Decimal("503")
    assert volume.first_sequence == 11
    assert volume.last_sequence == 12
    assert count.status == StrategyMarketDataStatus.OK
    assert count.trade_count == 2
    assert [order.action_id for order in snapshot.open_orders(product_id="AVA-29MAY26-CDE")] == ["open-buy"]
    assert [order.action_id for order in snapshot.open_orders(side=OrderSide.SELL)] == ["open-sell-other"]
    assert snapshot.product_rules("AVA-29MAY26-CDE") is product
    assert snapshot.notional(product_id="AVA-29MAY26-CDE", size="2", price="100") == Decimal("2000")
    assert window.to_payload()["trade_ids"] == ["trade-1", "trade-2"]
    assert window.to_payload()["source_sequences"] == [11, 12]
    assert window.to_payload()["time_field"] == MarketSeriesTimeField.OBSERVED_AT.value
    assert stats.to_payload()["aggressor_imbalance"] == "-0.2"
    assert stats.to_payload()["membership_rule"] == (
        MarketSeriesMembershipRule.START_INCLUSIVE_END_INCLUSIVE.value
    )
    assert stats.to_payload()["twap"] == "100.5"
    assert candles.status == StrategyMarketDataStatus.OK
    assert candles.membership_rule == MarketSeriesMembershipRule.FIXED_BUCKETS_FINAL_END_INCLUSIVE
    assert candles.time_field == MarketSeriesTimeField.OBSERVED_AT
    assert candles.aggressor_status == StrategyMarketDataStatus.OK
    assert candles.candle_count == 2
    assert candles.complete_candle_count == 1
    assert candles.empty_candle_count == 1
    assert candles.trade_count == 2
    assert candles.source_sequences == (11, 12)
    assert candles.candles[0].status == StrategyMarketDataStatus.MISSING
    assert candles.candles[0].membership_rule == MarketSeriesMembershipRule.START_INCLUSIVE_END_EXCLUSIVE
    assert candles.candles[0].trade_count == 0
    assert candles.candles[1].status == StrategyMarketDataStatus.OK
    assert candles.candles[1].membership_rule == MarketSeriesMembershipRule.START_INCLUSIVE_END_INCLUSIVE
    assert candles.candles[1].start == now - timedelta(minutes=1)
    assert candles.candles[1].end == now
    assert candles.candles[1].open == Decimal("100")
    assert candles.candles[1].high == Decimal("101")
    assert candles.candles[1].low == Decimal("100")
    assert candles.candles[1].close == Decimal("101")
    assert candles.candles[1].base_volume == Decimal("5")
    assert candles.candles[1].quote_volume == Decimal("503")
    assert candles.candles[1].buy_aggressor_volume == Decimal("2")
    assert candles.candles[1].sell_aggressor_volume == Decimal("3")
    assert candles.candles[1].aggressor_imbalance == Decimal("-0.2")
    assert candles.candles[1].source_sequences == (11, 12)
    assert candles.to_payload()["membership_rule"] == (
        MarketSeriesMembershipRule.FIXED_BUCKETS_FINAL_END_INCLUSIVE.value
    )
    assert candles.to_payload()["candles"][1]["close"] == "101"
    assert book_stats.to_payload()["book_imbalance"] == str(Decimal("-3") / Decimal("17"))
    assert volume.to_payload()["base_volume"] == "5"
    assert volume.to_payload()["status"] == StrategyMarketDataStatus.OK.value


def test_strategy_snapshot_trade_windows_apply_explicit_retention_limit():
    now = datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
    projection = SourceOfTruthProjection()
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="trade-dropped",
            observed_at=now - timedelta(seconds=90),
            price="90",
            product_id="AVA-29MAY26-CDE",
            sequence=99,
            side=OrderSide.BUY,
            size="10",
            trade_id="dropped",
        ),
    )
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="trade-1",
            observed_at=now - timedelta(seconds=60),
            price="100",
            product_id="AVA-29MAY26-CDE",
            sequence=11,
            side=OrderSide.BUY,
            size="2",
            trade_id="trade-1",
        ),
    )
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="trade-2",
            observed_at=now - timedelta(seconds=30),
            price="101",
            product_id="AVA-29MAY26-CDE",
            sequence=12,
            side=OrderSide.SELL,
            size="3",
            trade_id="trade-2",
        ),
    )
    snapshot = StrategySnapshot(
        as_of_sequence=99,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=Path("data/audit.jsonl"),
        projection=projection,
    )

    window = snapshot.trade_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
        max_retained_trades=2,
    )
    series_window = snapshot.market_series_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
        max_retained_trades=2,
    )
    stats = snapshot.market_window_stats(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
        max_retained_trades=2,
    )
    candles = snapshot.candles(
        "AVA-29MAY26-CDE",
        interval=timedelta(minutes=1),
        lookback=timedelta(minutes=2),
        max_retained_trades=1,
    )
    volume = snapshot.rolling_trade_volume(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
        max_retained_trades=2,
    )
    count = snapshot.rolling_trade_count(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
        max_retained_trades=2,
    )

    assert series_window.retention_limit == 2
    assert series_window.to_payload()["retention_limit"] == 2

    assert window.status == StrategyMarketDataStatus.OK
    assert tuple(trade.trade_id for trade in window.trades) == ("trade-1", "trade-2")
    assert window.available_trade_count == 3
    assert window.timestamped_trade_count == 3
    assert window.trade_count == 2
    assert window.retention_limit == 2
    assert window.retention_dropped_trade_count == 1
    assert window.source_sequences == (11, 12)
    assert window.to_payload()["retention_limit"] == 2
    assert window.to_payload()["retention_dropped_trade_count"] == 1

    assert stats.status == StrategyMarketDataStatus.OK
    assert stats.base_volume == Decimal("5")
    assert stats.quote_volume == Decimal("503")
    assert stats.retention_limit == 2
    assert stats.retention_dropped_trade_count == 1
    assert stats.source_sequences == (11, 12)
    assert stats.to_payload()["retention_limit"] == 2

    assert volume.status == StrategyMarketDataStatus.OK
    assert volume.base_volume == Decimal("5")
    assert volume.quote_volume == Decimal("503")
    assert volume.retention_limit == 2
    assert volume.retention_dropped_trade_count == 1
    assert volume.to_payload()["retention_dropped_trade_count"] == 1

    assert count.status == StrategyMarketDataStatus.OK
    assert count.trade_count == 2
    assert count.retention_limit == 2
    assert count.retention_dropped_trade_count == 1

    assert candles.status == StrategyMarketDataStatus.OK
    assert candles.trade_count == 1
    assert candles.retention_limit == 1
    assert candles.retention_dropped_trade_count == 2
    assert candles.source_sequences == (12,)
    assert candles.candles[0].status == StrategyMarketDataStatus.MISSING
    assert candles.candles[1].status == StrategyMarketDataStatus.OK
    assert candles.candles[1].open == Decimal("101")
    assert candles.to_payload()["retention_dropped_trade_count"] == 2

    with pytest.raises(ValueError, match="max_retained_trades must be positive"):
        snapshot.trade_window(
            "AVA-29MAY26-CDE",
            lookback=timedelta(minutes=5),
            max_retained_trades=0,
        )
    with pytest.raises(TypeError, match="max_retained_trades must be an integer"):
        snapshot.market_window_stats(
            "AVA-29MAY26-CDE",
            lookback=timedelta(minutes=5),
            max_retained_trades=True,
        )


def test_strategy_snapshot_market_data_helpers_report_missing_stale_and_insufficient_data():
    now = datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
    projection = SourceOfTruthProjection()
    projection.order_books_by_product_id["AVA-29MAY26-CDE"] = MarketOrderBookSnapshot(
        best_bid_price="99",
        best_bid_size="1",
        message_key="partial-book",
        observed_at=now,
        product_id="AVA-29MAY26-CDE",
        sequence=1,
    )
    projection.order_book_samples_by_product_id["AVA-29MAY26-CDE"] = [
        MarketOrderBookSnapshot(
            best_bid_price="99",
            best_bid_size="1",
            message_key="partial-book",
            observed_at=now,
            product_id="AVA-29MAY26-CDE",
            sequence=1,
        )
    ]
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="old-trade",
            observed_at=now - timedelta(minutes=10),
            price="100",
            product_id="AVA-29MAY26-CDE",
            sequence=2,
            size="1",
            trade_id="old-trade",
        ),
    )
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="invalid-trade",
            observed_at=now - timedelta(seconds=30),
            price="101",
            product_id="SHB-26JUN26-CDE",
            sequence=3,
            size=None,
            trade_id="invalid-trade",
        ),
    )
    _add_trade(
        projection,
        MarketTradeSnapshot(
            message_key="no-side-trade",
            observed_at=now - timedelta(seconds=20),
            price="10",
            product_id="NO-SIDE-CDE",
            sequence=4,
            size="2",
            trade_id="no-side-trade",
        ),
    )
    snapshot = StrategySnapshot(
        as_of_sequence=3,
        evaluated_at=now,
        execution_mode=ExecutionMode.DRY_RUN,
        ledger_path=Path("data/audit.jsonl"),
        projection=projection,
    )

    assert snapshot.best_bid_ask("MISSING-USD").status == StrategyMarketDataStatus.MISSING
    assert (
        snapshot.order_book_stats("MISSING-USD", levels=1).status
        == StrategyMarketDataStatus.MISSING
    )
    assert snapshot.midpoint("AVA-29MAY26-CDE").status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert snapshot.spread("AVA-29MAY26-CDE").status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    partial_book_stats = snapshot.order_book_stats("AVA-29MAY26-CDE", levels=1)
    assert partial_book_stats.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert partial_book_stats.best_bid_price == Decimal("99")
    assert partial_book_stats.best_ask_price is None
    assert (
        snapshot.order_book_sample_window("MISSING-USD", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.MISSING
    )
    assert (
        snapshot.order_book_window_stats(
            "MISSING-USD",
            levels=1,
            lookback=timedelta(minutes=5),
        ).status
        == StrategyMarketDataStatus.MISSING
    )
    partial_book_window = snapshot.order_book_sample_window(
        "AVA-29MAY26-CDE",
        lookback=timedelta(minutes=5),
    )
    assert partial_book_window.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert partial_book_window.sample_count == 1
    partial_book_window_stats = snapshot.order_book_window_stats(
        "AVA-29MAY26-CDE",
        levels=1,
        lookback=timedelta(minutes=5),
    )
    assert partial_book_window_stats.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert partial_book_window_stats.sample_count == 1
    assert partial_book_window_stats.valid_stats_count == 0
    with pytest.raises(ValueError, match="either levels or max_distance_bps"):
        snapshot.order_book_stats("AVA-29MAY26-CDE")
    with pytest.raises(ValueError, match="either levels or max_distance_bps"):
        snapshot.order_book_window_stats(
            "AVA-29MAY26-CDE",
            lookback=timedelta(minutes=5),
        )
    with pytest.raises(ValueError, match="mutually exclusive"):
        snapshot.order_book_stats("AVA-29MAY26-CDE", levels=1, max_distance_bps="50")
    with pytest.raises(ValueError, match="max_retained_samples must be positive"):
        snapshot.order_book_sample_window(
            "AVA-29MAY26-CDE",
            lookback=timedelta(minutes=5),
            max_retained_samples=0,
        )
    with pytest.raises(TypeError, match="min_samples must be an integer"):
        snapshot.order_book_sample_window(
            "AVA-29MAY26-CDE",
            lookback=timedelta(minutes=5),
            min_samples=True,
        )
    assert snapshot.latest_trade("MISSING-USD").status == StrategyMarketDataStatus.MISSING
    assert (
        snapshot.rolling_trade_volume("MISSING-USD", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.MISSING
    )
    assert (
        snapshot.trade_window("MISSING-USD", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.MISSING
    )
    assert (
        snapshot.trade_window("AVA-29MAY26-CDE", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.STALE
    )
    assert (
        snapshot.trade_window("SHB-26JUN26-CDE", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.OK
    )
    assert (
        snapshot.rolling_trade_volume("AVA-29MAY26-CDE", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.STALE
    )
    assert (
        snapshot.rolling_trade_volume("SHB-26JUN26-CDE", lookback=timedelta(minutes=5)).status
        == StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    invalid_stats = snapshot.market_window_stats("SHB-26JUN26-CDE", lookback=timedelta(minutes=5))
    assert invalid_stats.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert invalid_stats.open == Decimal("101")
    assert invalid_stats.valid_price_count == 1
    assert invalid_stats.valid_volume_count == 0
    invalid_candles = snapshot.candles(
        "SHB-26JUN26-CDE",
        interval=timedelta(minutes=1),
        lookback=timedelta(minutes=2),
    )
    assert invalid_candles.status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert invalid_candles.candle_count == 2
    assert invalid_candles.complete_candle_count == 0
    assert invalid_candles.empty_candle_count == 1
    assert invalid_candles.candles[1].status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert invalid_candles.candles[1].open == Decimal("101")
    assert invalid_candles.candles[1].valid_price_count == 1
    assert invalid_candles.candles[1].valid_volume_count == 0
    missing_candles = snapshot.candles(
        "MISSING-USD",
        interval=timedelta(minutes=1),
        lookback=timedelta(minutes=2),
    )
    assert missing_candles.status == StrategyMarketDataStatus.MISSING
    assert missing_candles.candle_count == 2
    assert missing_candles.empty_candle_count == 2
    assert all(candle.status == StrategyMarketDataStatus.MISSING for candle in missing_candles.candles)
    with pytest.raises(ValueError, match="integer multiple"):
        snapshot.candles(
            "SHB-26JUN26-CDE",
            interval=timedelta(minutes=2),
            lookback=timedelta(minutes=3),
        )
    no_side_stats = snapshot.market_window_stats("NO-SIDE-CDE", lookback=timedelta(minutes=5))
    assert no_side_stats.status == StrategyMarketDataStatus.OK
    assert no_side_stats.aggressor_status == StrategyMarketDataStatus.INSUFFICIENT_DATA
    assert no_side_stats.base_volume == Decimal("2")
    assert no_side_stats.quote_volume == Decimal("20")
    assert no_side_stats.buy_aggressor_volume is None
    assert no_side_stats.unclassified_trade_count == 1
    invalid_count = snapshot.rolling_trade_count("SHB-26JUN26-CDE", lookback=timedelta(minutes=5))
    assert invalid_count.status == StrategyMarketDataStatus.OK
    assert invalid_count.trade_count == 1
    assert snapshot.product_rules("AVA-29MAY26-CDE") is None


def _add_trade(projection: SourceOfTruthProjection, trade: MarketTradeSnapshot) -> None:
    projection.market_trades_by_id[trade.trade_id] = trade
    projection.market_trade_ids_by_product_id.setdefault(trade.product_id, []).append(trade.trade_id)
