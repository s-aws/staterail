from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import OrderSide, StrategyMarketDataStatus
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import (
    MarketOrderBookSnapshot,
    MarketTradeSnapshot,
    SourceOfTruthProjection,
)


AmountInput = Decimal | str | int | float


@dataclass(frozen=True)
class BestBidAsk:
    product_id: str
    status: StrategyMarketDataStatus
    ask_price: Decimal | None = None
    ask_size: Decimal | None = None
    bid_price: Decimal | None = None
    bid_size: Decimal | None = None
    observed_at: datetime | None = None
    sequence: int | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "ask_price": self.ask_price,
                "ask_size": self.ask_size,
                "bid_price": self.bid_price,
                "bid_size": self.bid_size,
                "is_ok": self.is_ok,
                "observed_at": self.observed_at,
                "product_id": self.product_id,
                "sequence": self.sequence,
                "status": self.status,
            }
        )


@dataclass(frozen=True)
class MarketMidpoint:
    product_id: str
    status: StrategyMarketDataStatus
    midpoint: Decimal | None = None
    ask_price: Decimal | None = None
    bid_price: Decimal | None = None
    observed_at: datetime | None = None
    sequence: int | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "ask_price": self.ask_price,
                "bid_price": self.bid_price,
                "is_ok": self.is_ok,
                "midpoint": self.midpoint,
                "observed_at": self.observed_at,
                "product_id": self.product_id,
                "sequence": self.sequence,
                "status": self.status,
            }
        )


@dataclass(frozen=True)
class MarketSpread:
    product_id: str
    status: StrategyMarketDataStatus
    spread: Decimal | None = None
    spread_bps: Decimal | None = None
    ask_price: Decimal | None = None
    bid_price: Decimal | None = None
    observed_at: datetime | None = None
    sequence: int | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "ask_price": self.ask_price,
                "bid_price": self.bid_price,
                "is_ok": self.is_ok,
                "observed_at": self.observed_at,
                "product_id": self.product_id,
                "sequence": self.sequence,
                "spread": self.spread,
                "spread_bps": self.spread_bps,
                "status": self.status,
            }
        )


@dataclass(frozen=True)
class MarketOrderBookStats:
    product_id: str
    status: StrategyMarketDataStatus
    levels: int | None = None
    max_distance_bps: Decimal | None = None
    best_bid_price: Decimal | None = None
    best_ask_price: Decimal | None = None
    top_bid_size: Decimal | None = None
    top_ask_size: Decimal | None = None
    spread: Decimal | None = None
    spread_bps: Decimal | None = None
    midpoint: Decimal | None = None
    microprice: Decimal | None = None
    weighted_mid: Decimal | None = None
    bid_volume: Decimal = Decimal("0")
    ask_volume: Decimal = Decimal("0")
    bid_notional: Decimal = Decimal("0")
    ask_notional: Decimal = Decimal("0")
    bid_level_count: int = 0
    ask_level_count: int = 0
    book_imbalance: Decimal | None = None
    observed_at: datetime | None = None
    sequence: int | None = None
    source_id: str | None = None
    update_count: int = 0

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "ask_level_count": self.ask_level_count,
                "ask_notional": self.ask_notional,
                "ask_volume": self.ask_volume,
                "best_ask_price": self.best_ask_price,
                "best_bid_price": self.best_bid_price,
                "bid_level_count": self.bid_level_count,
                "bid_notional": self.bid_notional,
                "bid_volume": self.bid_volume,
                "book_imbalance": self.book_imbalance,
                "is_ok": self.is_ok,
                "levels": self.levels,
                "max_distance_bps": self.max_distance_bps,
                "microprice": self.microprice,
                "midpoint": self.midpoint,
                "observed_at": self.observed_at,
                "product_id": self.product_id,
                "sequence": self.sequence,
                "source_id": self.source_id,
                "spread": self.spread,
                "spread_bps": self.spread_bps,
                "status": self.status,
                "top_ask_size": self.top_ask_size,
                "top_bid_size": self.top_bid_size,
                "update_count": self.update_count,
                "weighted_mid": self.weighted_mid,
            }
        )


@dataclass(frozen=True)
class LatestMarketTrade:
    product_id: str
    status: StrategyMarketDataStatus
    trade_id: str | None = None
    price: Decimal | None = None
    side: OrderSide | None = None
    size: Decimal | None = None
    observed_at: datetime | None = None
    sequence: int | None = None
    trade_time: str | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "is_ok": self.is_ok,
                "observed_at": self.observed_at,
                "price": self.price,
                "product_id": self.product_id,
                "sequence": self.sequence,
                "side": self.side,
                "size": self.size,
                "status": self.status,
                "trade_id": self.trade_id,
                "trade_time": self.trade_time,
            }
        )


@dataclass(frozen=True)
class TradeWindow:
    product_id: str
    lookback: timedelta
    status: StrategyMarketDataStatus
    trades: tuple[MarketTradeSnapshot, ...] = ()
    available_trade_count: int = 0
    timestamped_trade_count: int = 0
    trade_count: int = 0
    window_end: datetime | None = None
    window_start: datetime | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None
    source_sequences: tuple[int, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "available_trade_count": self.available_trade_count,
                "first_observed_at": self.first_observed_at,
                "first_sequence": self.first_sequence,
                "is_ok": self.is_ok,
                "last_observed_at": self.last_observed_at,
                "last_sequence": self.last_sequence,
                "lookback_seconds": self.lookback.total_seconds(),
                "product_id": self.product_id,
                "source_sequences": self.source_sequences,
                "status": self.status,
                "timestamped_trade_count": self.timestamped_trade_count,
                "trade_count": self.trade_count,
                "trade_ids": tuple(trade.trade_id for trade in self.trades),
                "window_end": self.window_end,
                "window_start": self.window_start,
            }
        )


@dataclass(frozen=True)
class MarketWindowStats:
    product_id: str
    lookback: timedelta
    status: StrategyMarketDataStatus
    aggressor_status: StrategyMarketDataStatus = StrategyMarketDataStatus.INSUFFICIENT_DATA
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    base_volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    buy_aggressor_volume: Decimal | None = None
    sell_aggressor_volume: Decimal | None = None
    buy_aggressor_quote_volume: Decimal | None = None
    sell_aggressor_quote_volume: Decimal | None = None
    net_aggressor_volume: Decimal | None = None
    net_aggressor_quote_volume: Decimal | None = None
    aggressor_imbalance: Decimal | None = None
    vwap: Decimal | None = None
    twap: Decimal | None = None
    realized_volatility: Decimal | None = None
    trade_count: int = 0
    valid_price_count: int = 0
    valid_volume_count: int = 0
    invalid_trade_count: int = 0
    classified_trade_count: int = 0
    unclassified_trade_count: int = 0
    window_end: datetime | None = None
    window_start: datetime | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None
    source_sequences: tuple[int, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    @property
    def aggressor_is_ok(self) -> bool:
        return self.aggressor_status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "aggressor_imbalance": self.aggressor_imbalance,
                "aggressor_is_ok": self.aggressor_is_ok,
                "aggressor_status": self.aggressor_status,
                "base_volume": self.base_volume,
                "buy_aggressor_quote_volume": self.buy_aggressor_quote_volume,
                "buy_aggressor_volume": self.buy_aggressor_volume,
                "classified_trade_count": self.classified_trade_count,
                "close": self.close,
                "first_observed_at": self.first_observed_at,
                "first_sequence": self.first_sequence,
                "high": self.high,
                "invalid_trade_count": self.invalid_trade_count,
                "is_ok": self.is_ok,
                "last_observed_at": self.last_observed_at,
                "last_sequence": self.last_sequence,
                "lookback_seconds": self.lookback.total_seconds(),
                "low": self.low,
                "net_aggressor_quote_volume": self.net_aggressor_quote_volume,
                "net_aggressor_volume": self.net_aggressor_volume,
                "open": self.open,
                "product_id": self.product_id,
                "quote_volume": self.quote_volume,
                "realized_volatility": self.realized_volatility,
                "sell_aggressor_quote_volume": self.sell_aggressor_quote_volume,
                "sell_aggressor_volume": self.sell_aggressor_volume,
                "source_sequences": self.source_sequences,
                "status": self.status,
                "trade_count": self.trade_count,
                "twap": self.twap,
                "unclassified_trade_count": self.unclassified_trade_count,
                "valid_price_count": self.valid_price_count,
                "valid_volume_count": self.valid_volume_count,
                "vwap": self.vwap,
                "window_end": self.window_end,
                "window_start": self.window_start,
            }
        )


@dataclass(frozen=True)
class MarketCandle:
    product_id: str
    start: datetime
    end: datetime
    status: StrategyMarketDataStatus
    aggressor_status: StrategyMarketDataStatus = StrategyMarketDataStatus.INSUFFICIENT_DATA
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    base_volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    buy_aggressor_volume: Decimal | None = None
    sell_aggressor_volume: Decimal | None = None
    buy_aggressor_quote_volume: Decimal | None = None
    sell_aggressor_quote_volume: Decimal | None = None
    net_aggressor_volume: Decimal | None = None
    net_aggressor_quote_volume: Decimal | None = None
    aggressor_imbalance: Decimal | None = None
    trade_count: int = 0
    valid_price_count: int = 0
    valid_volume_count: int = 0
    invalid_trade_count: int = 0
    classified_trade_count: int = 0
    unclassified_trade_count: int = 0
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None
    source_sequences: tuple[int, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    @property
    def aggressor_is_ok(self) -> bool:
        return self.aggressor_status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "aggressor_imbalance": self.aggressor_imbalance,
                "aggressor_is_ok": self.aggressor_is_ok,
                "aggressor_status": self.aggressor_status,
                "base_volume": self.base_volume,
                "buy_aggressor_quote_volume": self.buy_aggressor_quote_volume,
                "buy_aggressor_volume": self.buy_aggressor_volume,
                "classified_trade_count": self.classified_trade_count,
                "close": self.close,
                "end": self.end,
                "first_observed_at": self.first_observed_at,
                "first_sequence": self.first_sequence,
                "high": self.high,
                "invalid_trade_count": self.invalid_trade_count,
                "is_ok": self.is_ok,
                "last_observed_at": self.last_observed_at,
                "last_sequence": self.last_sequence,
                "low": self.low,
                "net_aggressor_quote_volume": self.net_aggressor_quote_volume,
                "net_aggressor_volume": self.net_aggressor_volume,
                "open": self.open,
                "product_id": self.product_id,
                "quote_volume": self.quote_volume,
                "sell_aggressor_quote_volume": self.sell_aggressor_quote_volume,
                "sell_aggressor_volume": self.sell_aggressor_volume,
                "source_sequences": self.source_sequences,
                "start": self.start,
                "status": self.status,
                "trade_count": self.trade_count,
                "unclassified_trade_count": self.unclassified_trade_count,
                "valid_price_count": self.valid_price_count,
                "valid_volume_count": self.valid_volume_count,
            }
        )


@dataclass(frozen=True)
class MarketCandles:
    product_id: str
    interval: timedelta
    lookback: timedelta
    status: StrategyMarketDataStatus
    candles: tuple[MarketCandle, ...] = ()
    aggressor_status: StrategyMarketDataStatus = StrategyMarketDataStatus.INSUFFICIENT_DATA
    candle_count: int = 0
    complete_candle_count: int = 0
    empty_candle_count: int = 0
    trade_count: int = 0
    window_end: datetime | None = None
    window_start: datetime | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None
    source_sequences: tuple[int, ...] = ()

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    @property
    def aggressor_is_ok(self) -> bool:
        return self.aggressor_status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "aggressor_is_ok": self.aggressor_is_ok,
                "aggressor_status": self.aggressor_status,
                "candle_count": self.candle_count,
                "candles": tuple(candle.to_payload() for candle in self.candles),
                "complete_candle_count": self.complete_candle_count,
                "empty_candle_count": self.empty_candle_count,
                "first_observed_at": self.first_observed_at,
                "first_sequence": self.first_sequence,
                "interval_seconds": self.interval.total_seconds(),
                "is_ok": self.is_ok,
                "last_observed_at": self.last_observed_at,
                "last_sequence": self.last_sequence,
                "lookback_seconds": self.lookback.total_seconds(),
                "product_id": self.product_id,
                "source_sequences": self.source_sequences,
                "status": self.status,
                "trade_count": self.trade_count,
                "window_end": self.window_end,
                "window_start": self.window_start,
            }
        )


@dataclass(frozen=True)
class RollingTradeVolume:
    product_id: str
    lookback: timedelta
    status: StrategyMarketDataStatus
    base_volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    trade_count: int = 0
    valid_trade_count: int = 0
    invalid_trade_count: int = 0
    window_end: datetime | None = None
    window_start: datetime | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "base_volume": self.base_volume,
                "first_observed_at": self.first_observed_at,
                "first_sequence": self.first_sequence,
                "invalid_trade_count": self.invalid_trade_count,
                "is_ok": self.is_ok,
                "last_observed_at": self.last_observed_at,
                "last_sequence": self.last_sequence,
                "lookback_seconds": self.lookback.total_seconds(),
                "product_id": self.product_id,
                "quote_volume": self.quote_volume,
                "status": self.status,
                "trade_count": self.trade_count,
                "valid_trade_count": self.valid_trade_count,
                "window_end": self.window_end,
                "window_start": self.window_start,
            }
        )


@dataclass(frozen=True)
class RollingTradeCount:
    product_id: str
    lookback: timedelta
    status: StrategyMarketDataStatus
    trade_count: int = 0
    window_end: datetime | None = None
    window_start: datetime | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == StrategyMarketDataStatus.OK

    def to_payload(self) -> dict[str, JsonValue]:
        return _payload(
            {
                "first_observed_at": self.first_observed_at,
                "first_sequence": self.first_sequence,
                "is_ok": self.is_ok,
                "last_observed_at": self.last_observed_at,
                "last_sequence": self.last_sequence,
                "lookback_seconds": self.lookback.total_seconds(),
                "product_id": self.product_id,
                "status": self.status,
                "trade_count": self.trade_count,
                "window_end": self.window_end,
                "window_start": self.window_start,
            }
        )


def best_bid_ask(projection: SourceOfTruthProjection, product_id: str) -> BestBidAsk:
    _validate_projection(projection)
    _validate_product_id(product_id)
    book = projection.order_book(product_id)
    if book is None:
        return BestBidAsk(product_id=product_id, status=StrategyMarketDataStatus.MISSING)
    return best_bid_ask_from_book(book)


def best_bid_ask_from_book(book: MarketOrderBookSnapshot) -> BestBidAsk:
    if not isinstance(book, MarketOrderBookSnapshot):
        raise TypeError("book must be a MarketOrderBookSnapshot")
    bid_price = _decimal_or_none(book.best_bid_price)
    bid_size = _decimal_or_none(book.best_bid_size)
    ask_price = _decimal_or_none(book.best_ask_price)
    ask_size = _decimal_or_none(book.best_ask_size)
    status = (
        StrategyMarketDataStatus.OK
        if bid_price is not None and ask_price is not None
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    return BestBidAsk(
        ask_price=ask_price,
        ask_size=ask_size,
        bid_price=bid_price,
        bid_size=bid_size,
        observed_at=book.observed_at,
        product_id=book.product_id,
        sequence=book.sequence,
        status=status,
    )


def midpoint(projection: SourceOfTruthProjection, product_id: str) -> MarketMidpoint:
    top = best_bid_ask(projection, product_id)
    if not top.is_ok or top.bid_price is None or top.ask_price is None:
        return MarketMidpoint(
            ask_price=top.ask_price,
            bid_price=top.bid_price,
            observed_at=top.observed_at,
            product_id=product_id,
            sequence=top.sequence,
            status=top.status,
        )
    return MarketMidpoint(
        ask_price=top.ask_price,
        bid_price=top.bid_price,
        midpoint=(top.bid_price + top.ask_price) / Decimal("2"),
        observed_at=top.observed_at,
        product_id=product_id,
        sequence=top.sequence,
        status=StrategyMarketDataStatus.OK,
    )


def spread(projection: SourceOfTruthProjection, product_id: str) -> MarketSpread:
    top = best_bid_ask(projection, product_id)
    if not top.is_ok or top.bid_price is None or top.ask_price is None:
        return MarketSpread(
            ask_price=top.ask_price,
            bid_price=top.bid_price,
            observed_at=top.observed_at,
            product_id=product_id,
            sequence=top.sequence,
            status=top.status,
        )

    value = top.ask_price - top.bid_price
    mid = (top.bid_price + top.ask_price) / Decimal("2")
    bps = (value / mid) * Decimal("10000") if mid > 0 else None
    return MarketSpread(
        ask_price=top.ask_price,
        bid_price=top.bid_price,
        observed_at=top.observed_at,
        product_id=product_id,
        sequence=top.sequence,
        spread=value,
        spread_bps=bps,
        status=StrategyMarketDataStatus.OK,
    )


def order_book_stats(
    projection: SourceOfTruthProjection,
    product_id: str,
    *,
    levels: int | None = None,
    max_distance_bps: AmountInput | None = None,
    product_catalog: ProductCatalog | None = None,
) -> MarketOrderBookStats:
    _validate_projection(projection)
    _validate_product_id(product_id)
    resolved_levels, resolved_distance = _order_book_depth_filter(
        levels=levels,
        max_distance_bps=max_distance_bps,
    )
    _validate_product_catalog(product_catalog)
    book = projection.order_book(product_id)
    if book is None:
        return MarketOrderBookStats(
            levels=resolved_levels,
            max_distance_bps=resolved_distance,
            product_id=product_id,
            status=StrategyMarketDataStatus.MISSING,
        )
    return order_book_stats_from_book(
        book,
        levels=levels,
        max_distance_bps=max_distance_bps,
        product_catalog=product_catalog,
    )


def order_book_stats_from_book(
    book: MarketOrderBookSnapshot,
    *,
    levels: int | None = None,
    max_distance_bps: AmountInput | None = None,
    product_catalog: ProductCatalog | None = None,
) -> MarketOrderBookStats:
    if not isinstance(book, MarketOrderBookSnapshot):
        raise TypeError("book must be a MarketOrderBookSnapshot")
    resolved_levels, resolved_distance = _order_book_depth_filter(
        levels=levels,
        max_distance_bps=max_distance_bps,
    )
    _validate_product_catalog(product_catalog)
    product = product_catalog.get(book.product_id) if product_catalog is not None else None

    bid_levels = _book_side_levels(
        book.bid_levels,
        fallback_price=book.best_bid_price,
        fallback_size=book.best_bid_size,
        reverse=True,
    )
    ask_levels = _book_side_levels(
        book.ask_levels,
        fallback_price=book.best_ask_price,
        fallback_size=book.best_ask_size,
        reverse=False,
    )
    if not bid_levels or not ask_levels:
        return MarketOrderBookStats(
            best_ask_price=(ask_levels[0][0] if ask_levels else None),
            best_bid_price=(bid_levels[0][0] if bid_levels else None),
            levels=resolved_levels,
            max_distance_bps=resolved_distance,
            observed_at=book.observed_at,
            product_id=book.product_id,
            sequence=book.sequence,
            source_id=book.source_id,
            status=StrategyMarketDataStatus.INSUFFICIENT_DATA,
            top_ask_size=(ask_levels[0][1] if ask_levels else None),
            top_bid_size=(bid_levels[0][1] if bid_levels else None),
            update_count=book.update_count,
        )

    best_bid_price, top_bid_size = bid_levels[0]
    best_ask_price, top_ask_size = ask_levels[0]
    midpoint_value = (best_bid_price + best_ask_price) / Decimal("2")
    spread_value = best_ask_price - best_bid_price
    selected_bid_levels = _selected_book_levels(
        bid_levels,
        limit_levels=resolved_levels,
        max_distance_bps=resolved_distance,
        midpoint=midpoint_value,
    )
    selected_ask_levels = _selected_book_levels(
        ask_levels,
        limit_levels=resolved_levels,
        max_distance_bps=resolved_distance,
        midpoint=midpoint_value,
    )
    bid_volume, bid_notional = _book_depth_totals(selected_bid_levels, product=product)
    ask_volume, ask_notional = _book_depth_totals(selected_ask_levels, product=product)
    top_size_total = top_bid_size + top_ask_size
    selected_size_total = bid_volume + ask_volume
    status = (
        StrategyMarketDataStatus.OK
        if selected_bid_levels and selected_ask_levels and selected_size_total > 0
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )

    return MarketOrderBookStats(
        ask_level_count=len(selected_ask_levels),
        ask_notional=ask_notional,
        ask_volume=ask_volume,
        best_ask_price=best_ask_price,
        best_bid_price=best_bid_price,
        bid_level_count=len(selected_bid_levels),
        bid_notional=bid_notional,
        bid_volume=bid_volume,
        book_imbalance=(
            (bid_volume - ask_volume) / selected_size_total
            if selected_size_total > 0
            else None
        ),
        levels=resolved_levels,
        max_distance_bps=resolved_distance,
        microprice=(
            ((best_bid_price * top_ask_size) + (best_ask_price * top_bid_size)) / top_size_total
            if top_size_total > 0
            else None
        ),
        midpoint=midpoint_value,
        observed_at=book.observed_at,
        product_id=book.product_id,
        sequence=book.sequence,
        source_id=book.source_id,
        spread=spread_value,
        spread_bps=((spread_value / midpoint_value) * Decimal("10000") if midpoint_value > 0 else None),
        status=status,
        top_ask_size=top_ask_size,
        top_bid_size=top_bid_size,
        update_count=book.update_count,
        weighted_mid=(
            ((best_bid_price * top_bid_size) + (best_ask_price * top_ask_size)) / top_size_total
            if top_size_total > 0
            else None
        ),
    )


def latest_trade(projection: SourceOfTruthProjection, product_id: str) -> LatestMarketTrade:
    _validate_projection(projection)
    _validate_product_id(product_id)
    trades = projection.market_trades_for_product(product_id)
    if not trades:
        return LatestMarketTrade(product_id=product_id, status=StrategyMarketDataStatus.MISSING)
    return latest_trade_from_trades(product_id=product_id, trades=trades)


def latest_trade_from_trades(
    *,
    product_id: str,
    trades: tuple[MarketTradeSnapshot, ...],
) -> LatestMarketTrade:
    _validate_product_id(product_id)
    if not isinstance(trades, tuple):
        raise TypeError("trades must be a tuple")
    if not trades:
        return LatestMarketTrade(product_id=product_id, status=StrategyMarketDataStatus.MISSING)
    trade = max(trades, key=lambda item: item.sequence)
    price = _decimal_or_none(trade.price)
    size = _decimal_or_none(trade.size)
    status = (
        StrategyMarketDataStatus.OK
        if price is not None and size is not None
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    return LatestMarketTrade(
        observed_at=trade.observed_at,
        price=price,
        product_id=product_id,
        sequence=trade.sequence,
        side=trade.side,
        size=size,
        status=status,
        trade_id=trade.trade_id,
        trade_time=trade.trade_time,
    )


def trade_window(
    projection: SourceOfTruthProjection,
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
) -> TradeWindow:
    _validate_projection(projection)
    _validate_product_id(product_id)
    _validate_datetime(as_of, "as_of")
    _validate_lookback(lookback)
    return trade_window_from_trades(
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
        trades=projection.market_trades_for_product(product_id),
    )


def trade_window_from_trades(
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
    trades: tuple[MarketTradeSnapshot, ...],
) -> TradeWindow:
    _validate_datetime(as_of, "as_of")
    _validate_lookback(lookback)
    _validate_product_id(product_id)
    _validate_trade_tuple(trades)

    window_start = as_of - lookback
    empty = {
        "lookback": lookback,
        "product_id": product_id,
        "window_end": as_of,
        "window_start": window_start,
    }
    if not trades:
        return TradeWindow(status=StrategyMarketDataStatus.MISSING, **empty)

    timestamped_trades = tuple(
        trade for trade in trades if isinstance(trade.observed_at, datetime)
    )
    if not timestamped_trades:
        return TradeWindow(
            available_trade_count=len(trades),
            status=StrategyMarketDataStatus.INSUFFICIENT_DATA,
            **empty,
        )

    in_window = tuple(
        trade
        for trade in timestamped_trades
        if trade.observed_at is not None and window_start <= trade.observed_at <= as_of
    )
    if not in_window:
        return TradeWindow(
            available_trade_count=len(trades),
            status=StrategyMarketDataStatus.STALE,
            timestamped_trade_count=len(timestamped_trades),
            **empty,
        )

    ordered = tuple(sorted(in_window, key=lambda trade: trade.sequence))
    return TradeWindow(
        available_trade_count=len(trades),
        first_observed_at=ordered[0].observed_at,
        first_sequence=ordered[0].sequence,
        last_observed_at=ordered[-1].observed_at,
        last_sequence=ordered[-1].sequence,
        lookback=lookback,
        product_id=product_id,
        source_sequences=tuple(trade.sequence for trade in ordered),
        status=StrategyMarketDataStatus.OK,
        timestamped_trade_count=len(timestamped_trades),
        trade_count=len(ordered),
        trades=ordered,
        window_end=as_of,
        window_start=window_start,
    )


def rolling_trade_volume(
    projection: SourceOfTruthProjection,
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
) -> RollingTradeVolume:
    window = trade_window(
        projection,
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
    )
    return rolling_trade_volume_from_window(window)


def market_window_stats(
    projection: SourceOfTruthProjection,
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
) -> MarketWindowStats:
    window = trade_window(
        projection,
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
    )
    return market_window_stats_from_window(window)


def market_window_stats_from_trades(
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
    trades: tuple[MarketTradeSnapshot, ...],
) -> MarketWindowStats:
    window = trade_window_from_trades(
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
        trades=trades,
    )
    return market_window_stats_from_window(window)


def market_window_stats_from_window(window: TradeWindow) -> MarketWindowStats:
    if not isinstance(window, TradeWindow):
        raise TypeError("window must be a TradeWindow")
    empty = {
        "lookback": window.lookback,
        "product_id": window.product_id,
        "trade_count": window.trade_count,
        "window_end": window.window_end,
        "window_start": window.window_start,
    }
    if not window.is_ok:
        return MarketWindowStats(status=window.status, **empty)

    price_points: list[tuple[datetime, int, Decimal]] = []
    base_volume = Decimal("0")
    quote_volume = Decimal("0")
    buy_volume = Decimal("0")
    sell_volume = Decimal("0")
    buy_quote_volume = Decimal("0")
    sell_quote_volume = Decimal("0")
    valid_volume_count = 0
    invalid_trade_count = 0
    classified_trade_count = 0
    unclassified_trade_count = 0

    for trade in window.trades:
        price = _positive_decimal_or_none(trade.price)
        size = _positive_decimal_or_none(trade.size)
        if price is not None and isinstance(trade.observed_at, datetime):
            price_points.append((trade.observed_at, trade.sequence, price))
        if size is None:
            invalid_trade_count += 1
        else:
            base_volume += size
            valid_volume_count += 1
            if price is not None:
                quote_value = size * price
                quote_volume += quote_value
            else:
                quote_value = None

            if trade.side is None:
                unclassified_trade_count += 1
            elif trade.side == OrderSide.BUY:
                classified_trade_count += 1
                buy_volume += size
                if quote_value is not None:
                    buy_quote_volume += quote_value
            elif trade.side == OrderSide.SELL:
                classified_trade_count += 1
                sell_volume += size
                if quote_value is not None:
                    sell_quote_volume += quote_value
            else:
                unclassified_trade_count += 1

        if size is None and trade.side is None:
            unclassified_trade_count += 1
        elif size is None and trade.side in {OrderSide.BUY, OrderSide.SELL}:
            classified_trade_count += 1

    ordered_prices = tuple(price for _, _, price in sorted(price_points))
    status = (
        StrategyMarketDataStatus.OK
        if ordered_prices and base_volume > 0 and quote_volume > 0
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    aggressor_status = (
        StrategyMarketDataStatus.OK
        if classified_trade_count > 0 and (buy_volume + sell_volume) > 0
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    total_aggressor_volume = buy_volume + sell_volume
    net_aggressor_volume = buy_volume - sell_volume
    net_aggressor_quote_volume = buy_quote_volume - sell_quote_volume
    return MarketWindowStats(
        aggressor_imbalance=(
            net_aggressor_volume / total_aggressor_volume
            if total_aggressor_volume > 0
            else None
        ),
        aggressor_status=aggressor_status,
        base_volume=base_volume,
        buy_aggressor_quote_volume=(buy_quote_volume if aggressor_status == StrategyMarketDataStatus.OK else None),
        buy_aggressor_volume=(buy_volume if aggressor_status == StrategyMarketDataStatus.OK else None),
        classified_trade_count=classified_trade_count,
        close=(ordered_prices[-1] if ordered_prices else None),
        first_observed_at=window.first_observed_at,
        first_sequence=window.first_sequence,
        high=(max(ordered_prices) if ordered_prices else None),
        invalid_trade_count=invalid_trade_count,
        last_observed_at=window.last_observed_at,
        last_sequence=window.last_sequence,
        lookback=window.lookback,
        low=(min(ordered_prices) if ordered_prices else None),
        net_aggressor_quote_volume=(
            net_aggressor_quote_volume if aggressor_status == StrategyMarketDataStatus.OK else None
        ),
        net_aggressor_volume=(
            net_aggressor_volume if aggressor_status == StrategyMarketDataStatus.OK else None
        ),
        open=(ordered_prices[0] if ordered_prices else None),
        product_id=window.product_id,
        quote_volume=quote_volume,
        realized_volatility=_realized_volatility(ordered_prices),
        sell_aggressor_quote_volume=(sell_quote_volume if aggressor_status == StrategyMarketDataStatus.OK else None),
        sell_aggressor_volume=(sell_volume if aggressor_status == StrategyMarketDataStatus.OK else None),
        source_sequences=window.source_sequences,
        status=status,
        trade_count=window.trade_count,
        twap=_twap(price_points, window_end=window.window_end),
        unclassified_trade_count=unclassified_trade_count,
        valid_price_count=len(ordered_prices),
        valid_volume_count=valid_volume_count,
        vwap=(quote_volume / base_volume if base_volume > 0 and quote_volume > 0 else None),
        window_end=window.window_end,
        window_start=window.window_start,
    )


def candles(
    projection: SourceOfTruthProjection,
    *,
    as_of: datetime,
    interval: timedelta,
    lookback: timedelta,
    product_id: str,
) -> MarketCandles:
    window = trade_window(
        projection,
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
    )
    return candles_from_window(window, interval=interval)


def candles_from_trades(
    *,
    as_of: datetime,
    interval: timedelta,
    lookback: timedelta,
    product_id: str,
    trades: tuple[MarketTradeSnapshot, ...],
) -> MarketCandles:
    window = trade_window_from_trades(
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
        trades=trades,
    )
    return candles_from_window(window, interval=interval)


def candles_from_window(window: TradeWindow, *, interval: timedelta) -> MarketCandles:
    if not isinstance(window, TradeWindow):
        raise TypeError("window must be a TradeWindow")
    _validate_interval(interval)
    _validate_candle_lookback(window.lookback, interval)
    if not isinstance(window.window_start, datetime) or not isinstance(window.window_end, datetime):
        raise ValueError("window must include window_start and window_end datetimes")

    candle_count = _bucket_count(window.lookback, interval)
    bucket_candles: list[MarketCandle] = []
    for index in range(candle_count):
        start = window.window_start + (interval * index)
        end = start + interval
        bucket_trades = _bucket_trades(
            window.trades,
            start=start,
            end=end,
            include_end=index == candle_count - 1,
        )
        if bucket_trades:
            bucket_window = _trade_window_for_bucket(
                bucket_trades,
                product_id=window.product_id,
                start=start,
                end=end,
                interval=interval,
            )
            bucket_candles.append(_candle_from_stats(market_window_stats_from_window(bucket_window)))
        else:
            bucket_candles.append(
                MarketCandle(
                    end=end,
                    product_id=window.product_id,
                    start=start,
                    status=StrategyMarketDataStatus.MISSING,
                )
            )

    complete_candle_count = sum(1 for candle in bucket_candles if candle.is_ok)
    empty_candle_count = sum(1 for candle in bucket_candles if candle.trade_count == 0)
    aggressor_status = (
        StrategyMarketDataStatus.OK
        if any(candle.aggressor_is_ok for candle in bucket_candles)
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    if complete_candle_count > 0:
        status = StrategyMarketDataStatus.OK
    elif window.is_ok:
        status = StrategyMarketDataStatus.INSUFFICIENT_DATA
    else:
        status = window.status

    return MarketCandles(
        aggressor_status=aggressor_status,
        candle_count=len(bucket_candles),
        candles=tuple(bucket_candles),
        complete_candle_count=complete_candle_count,
        empty_candle_count=empty_candle_count,
        first_observed_at=window.first_observed_at,
        first_sequence=window.first_sequence,
        interval=interval,
        last_observed_at=window.last_observed_at,
        last_sequence=window.last_sequence,
        lookback=window.lookback,
        product_id=window.product_id,
        source_sequences=window.source_sequences,
        status=status,
        trade_count=window.trade_count,
        window_end=window.window_end,
        window_start=window.window_start,
    )


def rolling_trade_count(
    projection: SourceOfTruthProjection,
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
) -> RollingTradeCount:
    window = trade_window(
        projection,
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
    )
    return RollingTradeCount(
        first_observed_at=window.first_observed_at,
        first_sequence=window.first_sequence,
        last_observed_at=window.last_observed_at,
        last_sequence=window.last_sequence,
        lookback=window.lookback,
        product_id=window.product_id,
        status=window.status,
        trade_count=window.trade_count,
        window_end=window.window_end,
        window_start=window.window_start,
    )


def rolling_trade_volume_from_trades(
    *,
    as_of: datetime,
    lookback: timedelta,
    product_id: str,
    trades: tuple[MarketTradeSnapshot, ...],
) -> RollingTradeVolume:
    window = trade_window_from_trades(
        as_of=as_of,
        lookback=lookback,
        product_id=product_id,
        trades=trades,
    )
    return rolling_trade_volume_from_window(window)


def rolling_trade_volume_from_window(window: TradeWindow) -> RollingTradeVolume:
    if not isinstance(window, TradeWindow):
        raise TypeError("window must be a TradeWindow")
    empty = {
        "lookback": window.lookback,
        "product_id": window.product_id,
        "window_end": window.window_end,
        "window_start": window.window_start,
    }
    if not window.is_ok:
        return RollingTradeVolume(status=window.status, **empty)

    base_volume = Decimal("0")
    quote_volume = Decimal("0")
    valid_trade_count = 0
    invalid_trade_count = 0
    for trade in window.trades:
        size = _positive_decimal_or_none(trade.size)
        price = _positive_decimal_or_none(trade.price)
        if size is None:
            invalid_trade_count += 1
            continue
        base_volume += size
        if price is not None:
            quote_volume += size * price
        valid_trade_count += 1

    status = (
        StrategyMarketDataStatus.OK
        if valid_trade_count > 0
        else StrategyMarketDataStatus.INSUFFICIENT_DATA
    )
    return RollingTradeVolume(
        base_volume=base_volume,
        first_observed_at=window.first_observed_at,
        first_sequence=window.first_sequence,
        invalid_trade_count=invalid_trade_count,
        last_observed_at=window.last_observed_at,
        last_sequence=window.last_sequence,
        lookback=window.lookback,
        product_id=window.product_id,
        quote_volume=quote_volume,
        status=status,
        trade_count=window.trade_count,
        valid_trade_count=valid_trade_count,
        window_end=window.window_end,
        window_start=window.window_start,
    )


def _validate_projection(projection: SourceOfTruthProjection) -> None:
    if not isinstance(projection, SourceOfTruthProjection):
        raise TypeError("projection must be a SourceOfTruthProjection")


def _validate_product_id(product_id: str) -> None:
    if not isinstance(product_id, str) or not product_id:
        raise ValueError("product_id must be a non-empty string")


def _validate_datetime(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")


def _validate_lookback(value: timedelta) -> None:
    if not isinstance(value, timedelta):
        raise TypeError("lookback must be a datetime.timedelta")
    if value <= timedelta(0):
        raise ValueError("lookback must be positive")


def _validate_interval(value: timedelta) -> None:
    if not isinstance(value, timedelta):
        raise TypeError("interval must be a datetime.timedelta")
    if value <= timedelta(0):
        raise ValueError("interval must be positive")


def _validate_candle_lookback(lookback: timedelta, interval: timedelta) -> None:
    lookback_units = _timedelta_microseconds(lookback)
    interval_units = _timedelta_microseconds(interval)
    if lookback_units % interval_units != 0:
        raise ValueError("lookback must be an integer multiple of interval")


def _validate_trade_tuple(trades: tuple[MarketTradeSnapshot, ...]) -> None:
    if not isinstance(trades, tuple):
        raise TypeError("trades must be a tuple")
    if any(not isinstance(trade, MarketTradeSnapshot) for trade in trades):
        raise TypeError("trades must contain MarketTradeSnapshot values")


def _validate_product_catalog(product_catalog: ProductCatalog | None) -> None:
    if product_catalog is not None and not isinstance(product_catalog, ProductCatalog):
        raise TypeError("product_catalog must be a ProductCatalog when provided")


def _order_book_depth_filter(
    *,
    levels: int | None,
    max_distance_bps: AmountInput | None,
) -> tuple[int | None, Decimal | None]:
    if levels is None and max_distance_bps is None:
        raise ValueError("either levels or max_distance_bps is required")
    if levels is not None and max_distance_bps is not None:
        raise ValueError("levels and max_distance_bps are mutually exclusive")
    if levels is not None:
        if isinstance(levels, bool) or not isinstance(levels, int):
            raise TypeError("levels must be an integer when provided")
        if levels <= 0:
            raise ValueError("levels must be positive")
        return levels, None
    distance = _positive_decimal_or_none(max_distance_bps)
    if distance is None:
        raise ValueError("max_distance_bps must be positive")
    return None, distance


def _book_side_levels(
    levels: Mapping[str, str],
    *,
    fallback_price: str | None,
    fallback_size: str | None,
    reverse: bool,
) -> tuple[tuple[Decimal, Decimal], ...]:
    parsed = tuple(
        (price, size)
        for raw_price, raw_size in levels.items()
        if (price := _positive_decimal_or_none(raw_price)) is not None
        and (size := _positive_decimal_or_none(raw_size)) is not None
    )
    if not parsed:
        fallback = _book_level_or_none(fallback_price, fallback_size)
        if fallback is not None:
            return (fallback,)
    return tuple(sorted(parsed, key=lambda item: item[0], reverse=reverse))


def _book_level_or_none(
    raw_price: str | None,
    raw_size: str | None,
) -> tuple[Decimal, Decimal] | None:
    price = _positive_decimal_or_none(raw_price)
    size = _positive_decimal_or_none(raw_size)
    if price is None or size is None:
        return None
    return price, size


def _selected_book_levels(
    book_levels: tuple[tuple[Decimal, Decimal], ...],
    *,
    limit_levels: int | None,
    max_distance_bps: Decimal | None,
    midpoint: Decimal,
) -> tuple[tuple[Decimal, Decimal], ...]:
    if limit_levels is not None:
        return book_levels[:limit_levels]
    if max_distance_bps is None or midpoint <= 0:
        return ()
    return tuple(
        (price, size)
        for price, size in book_levels
        if (abs(price - midpoint) / midpoint) * Decimal("10000") <= max_distance_bps
    )


def _book_depth_totals(
    levels: tuple[tuple[Decimal, Decimal], ...],
    *,
    product: ProductMetadata | None,
) -> tuple[Decimal, Decimal]:
    volume = Decimal("0")
    notional = Decimal("0")
    for price, size in levels:
        volume += size
        notional_value = product.notional(size, price) if product is not None else size * price
        if notional_value is not None:
            notional += notional_value
    return volume, notional


def _bucket_count(lookback: timedelta, interval: timedelta) -> int:
    return _timedelta_microseconds(lookback) // _timedelta_microseconds(interval)


def _bucket_trades(
    trades: tuple[MarketTradeSnapshot, ...],
    *,
    start: datetime,
    end: datetime,
    include_end: bool,
) -> tuple[MarketTradeSnapshot, ...]:
    if include_end:
        selected = tuple(
            trade
            for trade in trades
            if isinstance(trade.observed_at, datetime) and start <= trade.observed_at <= end
        )
    else:
        selected = tuple(
            trade
            for trade in trades
            if isinstance(trade.observed_at, datetime) and start <= trade.observed_at < end
        )
    return tuple(sorted(selected, key=lambda trade: trade.sequence))


def _trade_window_for_bucket(
    trades: tuple[MarketTradeSnapshot, ...],
    *,
    product_id: str,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> TradeWindow:
    return TradeWindow(
        available_trade_count=len(trades),
        first_observed_at=trades[0].observed_at,
        first_sequence=trades[0].sequence,
        last_observed_at=trades[-1].observed_at,
        last_sequence=trades[-1].sequence,
        lookback=interval,
        product_id=product_id,
        source_sequences=tuple(trade.sequence for trade in trades),
        status=StrategyMarketDataStatus.OK,
        timestamped_trade_count=len(trades),
        trade_count=len(trades),
        trades=trades,
        window_end=end,
        window_start=start,
    )


def _candle_from_stats(stats: MarketWindowStats) -> MarketCandle:
    if not isinstance(stats.window_start, datetime) or not isinstance(stats.window_end, datetime):
        raise ValueError("stats must include window_start and window_end datetimes")
    return MarketCandle(
        aggressor_imbalance=stats.aggressor_imbalance,
        aggressor_status=stats.aggressor_status,
        base_volume=stats.base_volume,
        buy_aggressor_quote_volume=stats.buy_aggressor_quote_volume,
        buy_aggressor_volume=stats.buy_aggressor_volume,
        classified_trade_count=stats.classified_trade_count,
        close=stats.close,
        end=stats.window_end,
        first_observed_at=stats.first_observed_at,
        first_sequence=stats.first_sequence,
        high=stats.high,
        invalid_trade_count=stats.invalid_trade_count,
        last_observed_at=stats.last_observed_at,
        last_sequence=stats.last_sequence,
        low=stats.low,
        net_aggressor_quote_volume=stats.net_aggressor_quote_volume,
        net_aggressor_volume=stats.net_aggressor_volume,
        open=stats.open,
        product_id=stats.product_id,
        quote_volume=stats.quote_volume,
        sell_aggressor_quote_volume=stats.sell_aggressor_quote_volume,
        sell_aggressor_volume=stats.sell_aggressor_volume,
        source_sequences=stats.source_sequences,
        start=stats.window_start,
        status=stats.status,
        trade_count=stats.trade_count,
        unclassified_trade_count=stats.unclassified_trade_count,
        valid_price_count=stats.valid_price_count,
        valid_volume_count=stats.valid_volume_count,
    )


def _twap(
    price_points: list[tuple[datetime, int, Decimal]],
    *,
    window_end: datetime | None,
) -> Decimal | None:
    if not price_points or not isinstance(window_end, datetime):
        return None
    ordered = tuple(sorted(price_points, key=lambda item: (item[0], item[1])))
    weighted_sum = Decimal("0")
    total_duration = Decimal("0")
    for index, (observed_at, _, price) in enumerate(ordered):
        next_observed_at = ordered[index + 1][0] if index + 1 < len(ordered) else window_end
        if next_observed_at <= observed_at:
            continue
        duration = _duration_seconds_decimal(next_observed_at - observed_at)
        weighted_sum += price * duration
        total_duration += duration
    if total_duration <= 0:
        return None
    return weighted_sum / total_duration


def _realized_volatility(prices: tuple[Decimal, ...]) -> Decimal | None:
    if len(prices) < 2:
        return None
    returns: list[Decimal] = []
    for previous, current in zip(prices, prices[1:]):
        if previous <= 0 or current <= 0:
            continue
        try:
            returns.append((current / previous).ln())
        except InvalidOperation:
            continue
    if not returns:
        return None
    mean_return = sum(returns, Decimal("0")) / Decimal(len(returns))
    variance = sum(
        ((item - mean_return) * (item - mean_return) for item in returns),
        Decimal("0"),
    ) / Decimal(len(returns))
    return variance.sqrt()


def _duration_seconds_decimal(value: timedelta) -> Decimal:
    return (
        Decimal(value.days * 86400)
        + Decimal(value.seconds)
        + (Decimal(value.microseconds) / Decimal("1000000"))
    )


def _timedelta_microseconds(value: timedelta) -> int:
    return ((value.days * 86400 + value.seconds) * 1000000) + value.microseconds


def _positive_decimal_or_none(value: Any) -> Decimal | None:
    parsed = _decimal_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not decimal.is_finite():
        return None
    return decimal


def _payload(raw: dict[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(
        {
            key: _json_safe(value)
            for key, value in raw.items()
        }
    )
    if not isinstance(normalized, dict):
        raise TypeError("market data helper payload must normalize to an object")
    return normalized


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value
