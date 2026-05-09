from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from core.enums import CoinbaseWebSocketChannel, CoinbaseWebSocketEndpoint, EventType
from core.errors import ExchangeAuthError, ExchangeTransportError, FeedSourceError
from exchanges.coinbase.advanced_trade_ws import (
    CoinbaseAdvancedTradeFeedSource,
    CoinbaseMessageNormalizer,
    CoinbaseWebSocketConfig,
)


class FakeWebSocket:
    def __init__(self, incoming: list[str]) -> None:
        self.incoming = incoming
        self.sent: list[str] = []

    async def __aenter__(self) -> "FakeWebSocket":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[str]:
        for item in self.incoming:
            yield item


class FakeConnector:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket
        self.urls: list[str] = []

    def connect(self, url: str) -> FakeWebSocket:
        self.urls.append(url)
        return self.websocket


class RaisingConnector:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def connect(self, url: str) -> FakeWebSocket:
        raise self._exc


def test_coinbase_config_builds_one_subscription_per_channel_with_heartbeat():
    config = CoinbaseWebSocketConfig(
        source_id="coinbase-primary",
        product_ids=("BTC-PERP-INTX",),
        channels=(CoinbaseWebSocketChannel.LEVEL2, CoinbaseWebSocketChannel.TICKER),
        jwt_factory=lambda message: f"jwt-for-{message['channel']}",
    )

    messages = config.subscription_messages()

    assert [message["channel"] for message in messages] == ["heartbeats", "level2", "ticker"]
    assert "product_ids" not in messages[0]
    assert messages[1]["product_ids"] == ["BTC-PERP-INTX"]
    assert [message["jwt"] for message in messages] == [
        "jwt-for-heartbeats",
        "jwt-for-level2",
        "jwt-for-ticker",
    ]


def test_coinbase_config_wraps_jwt_factory_failures_as_auth_errors():
    config = CoinbaseWebSocketConfig(
        source_id="coinbase-primary",
        product_ids=("BTC-PERP-INTX",),
        channels=(CoinbaseWebSocketChannel.USER,),
        endpoint=CoinbaseWebSocketEndpoint.USER_ORDER_DATA,
        jwt_factory=lambda message: (_ for _ in ()).throw(RuntimeError("jwt failed")),
    )

    with pytest.raises(ExchangeAuthError, match="JWT factory"):
        config.subscription_messages()


def test_coinbase_websocket_config_rejects_invalid_boundary_values():
    with pytest.raises(ValueError, match="product_ids"):
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=(),
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
        )

    with pytest.raises(ValueError, match="channels"):
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=("BTC-PERP-INTX",),
            channels=(CoinbaseWebSocketChannel.LEVEL2, CoinbaseWebSocketChannel.LEVEL2),
        )

    with pytest.raises(ValueError, match="USER_ORDER_DATA"):
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=("BTC-PERP-INTX",),
            channels=(CoinbaseWebSocketChannel.USER,),
        )

    with pytest.raises(ValueError, match="separately"):
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=("BTC-PERP-INTX",),
            channels=(CoinbaseWebSocketChannel.USER, CoinbaseWebSocketChannel.LEVEL2),
            endpoint=CoinbaseWebSocketEndpoint.USER_ORDER_DATA,
        )


def test_coinbase_normalizer_generates_source_independent_duplicate_keys():
    raw = {
        "channel": "level2",
        "sequence_num": 7,
        "timestamp": "2026-01-01T00:00:00Z",
        "events": [{"type": "snapshot", "product_id": "BTC-PERP-INTX"}],
    }

    first = CoinbaseMessageNormalizer().normalize("coinbase-primary", raw)[0]
    second = CoinbaseMessageNormalizer().normalize("coinbase-secondary", raw)[0]

    assert first.message_key == second.message_key
    assert first.source_id == "coinbase-primary"
    assert second.source_id == "coinbase-secondary"


def test_coinbase_normalizer_distinguishes_reused_sequence_numbers_across_sessions():
    first_raw = {
        "channel": "l2_data",
        "sequence_num": 7,
        "timestamp": "2026-01-01T00:00:00Z",
        "events": [
            {
                "product_id": "BTC-PERP-INTX",
                "type": "update",
                "updates": [{"new_quantity": "1", "price_level": "100", "side": "bid"}],
            }
        ],
    }
    second_raw = {
        **first_raw,
        "timestamp": "2026-01-01T00:01:00Z",
        "events": [
            {
                "product_id": "BTC-PERP-INTX",
                "type": "update",
                "updates": [{"new_quantity": "2", "price_level": "100", "side": "bid"}],
            }
        ],
    }

    first = CoinbaseMessageNormalizer().normalize("coinbase-primary", first_raw)[0]
    second = CoinbaseMessageNormalizer().normalize("coinbase-primary", second_raw)[0]

    assert first.message_key.startswith("coinbase:l2_data:7:")
    assert second.message_key.startswith("coinbase:l2_data:7:")
    assert first.message_key != second.message_key


def test_coinbase_normalizer_emits_sequence_gap_before_current_message():
    normalizer = CoinbaseMessageNormalizer()
    normalizer.normalize(
        "coinbase-primary",
        {
            "channel": "ticker",
            "sequence_num": 10,
            "events": [{"tickers": [{"product_id": "ETH-PERP-INTX"}]}],
        },
    )

    messages = normalizer.normalize(
        "coinbase-primary",
        {
            "channel": "ticker",
            "sequence_num": 13,
            "events": [{"tickers": [{"product_id": "ETH-PERP-INTX"}]}],
        },
    )

    assert [message.event_type for message in messages] == [
        EventType.DATA_SEQUENCE_GAP,
        EventType.DATA_RECEIVED,
    ]
    assert messages[0].payload["gap_size"] == 2


def test_coinbase_normalizer_tracks_sequence_across_channels():
    normalizer = CoinbaseMessageNormalizer()

    first = normalizer.normalize(
        "coinbase-primary",
        {"channel": "subscriptions", "sequence_num": 0},
    )
    second = normalizer.normalize(
        "coinbase-primary",
        {
            "channel": "l2_data",
            "sequence_num": 1,
            "events": [{"updates": [{"product_id": "BIT-29MAY26-CDE"}]}],
        },
    )
    third = normalizer.normalize(
        "coinbase-primary",
        {"channel": "heartbeats", "sequence_num": 2},
    )
    fourth = normalizer.normalize(
        "coinbase-primary",
        {
            "channel": "l2_data",
            "sequence_num": 3,
            "events": [{"updates": [{"product_id": "BIT-29MAY26-CDE"}]}],
        },
    )

    messages = (*first, *second, *third, *fourth)

    assert EventType.DATA_SEQUENCE_GAP not in {message.event_type for message in messages}


def test_coinbase_normalizer_emits_out_of_order_without_rewinding_sequence():
    normalizer = CoinbaseMessageNormalizer()
    normalizer.normalize("coinbase-primary", {"channel": "heartbeats", "sequence_num": 20})

    messages = normalizer.normalize("coinbase-primary", {"channel": "heartbeats", "sequence_num": 19})

    assert messages[0].event_type == EventType.DATA_OUT_OF_ORDER
    assert messages[0].payload["previous_sequence"] == 20
    assert messages[0].payload["observed_sequence"] == 19


def test_coinbase_normalizer_emits_heartbeats_as_feed_health():
    messages = CoinbaseMessageNormalizer().normalize(
        "coinbase-primary",
        {
            "channel": "heartbeats",
            "sequence_num": 21,
            "timestamp": "2026-01-01T00:00:00Z",
        },
    )

    assert len(messages) == 1
    assert messages[0].event_type == EventType.FEED_HEARTBEAT
    assert messages[0].message_key.startswith("coinbase:heartbeats:21:")
    assert messages[0].payload["sequence_num"] == 21


def test_coinbase_normalizer_extracts_user_channel_order_updates():
    messages = CoinbaseMessageNormalizer().normalize(
        "coinbase-primary",
        {
            "channel": "user",
            "sequence_num": 30,
            "timestamp": "2026-01-01T00:00:00Z",
            "events": [
                {
                    "type": "snapshot",
                    "orders": [
                        {
                            "client_order_id": "client-1",
                            "order_id": "exchange-1",
                            "order_side": "BUY",
                            "order_type": "LIMIT",
                            "product_id": "BTC-USD",
                            "status": "OPEN",
                        }
                    ],
                }
            ],
        },
    )

    assert len(messages) == 1
    assert messages[0].event_type == EventType.EXCHANGE_ORDER_UPDATE
    assert messages[0].message_key.startswith("coinbase:user-order:exchange-1:30:0:0:")
    assert messages[0].payload["order"]["status"] == "OPEN"


def test_coinbase_normalizer_keeps_unknown_user_payloads_as_generic_data():
    messages = CoinbaseMessageNormalizer().normalize(
        "coinbase-primary",
        {
            "channel": "user",
            "sequence_num": 30,
            "events": [{"type": "snapshot", "positions": []}],
        },
    )

    assert len(messages) == 1
    assert messages[0].event_type == EventType.DATA_RECEIVED
    assert messages[0].message_key.startswith("coinbase:user:30:")


def test_coinbase_feed_source_sends_subscriptions_and_yields_normalized_messages():
    websocket = FakeWebSocket(
        [
            (
                '{"channel":"level2","sequence_num":1,'
                '"events":[{"type":"snapshot","product_id":"BTC-PERP-INTX"}]}'
            )
        ]
    )
    connector = FakeConnector(websocket)
    source = CoinbaseAdvancedTradeFeedSource(
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=("BTC-PERP-INTX",),
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
            endpoint=CoinbaseWebSocketEndpoint.MARKET_DATA,
        ),
        connector=connector,
    )

    async def collect():
        return [message async for message in source.stream()]

    messages = asyncio.run(collect())

    assert connector.urls == [CoinbaseWebSocketEndpoint.MARKET_DATA.value]
    assert '"channel":"heartbeats"' in websocket.sent[0]
    assert '"channel":"level2"' in websocket.sent[1]
    assert messages[0].message_key.startswith("coinbase:level2:1:")


def test_coinbase_feed_source_wraps_connector_failures_as_transport_errors():
    source = CoinbaseAdvancedTradeFeedSource(
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=("BTC-PERP-INTX",),
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
        ),
        connector=RaisingConnector(ConnectionError("connect failed")),
    )

    async def collect():
        return [message async for message in source.stream()]

    with pytest.raises(ExchangeTransportError) as exc_info:
        asyncio.run(collect())

    assert exc_info.value.retryable is True


def test_coinbase_feed_source_raises_feed_source_error_for_invalid_json():
    source = CoinbaseAdvancedTradeFeedSource(
        CoinbaseWebSocketConfig(
            source_id="coinbase-primary",
            product_ids=("BTC-PERP-INTX",),
            channels=(CoinbaseWebSocketChannel.LEVEL2,),
        ),
        connector=FakeConnector(FakeWebSocket(["not-json"])),
    )

    async def collect():
        return [message async for message in source.stream()]

    with pytest.raises(FeedSourceError, match="valid JSON"):
        asyncio.run(collect())
