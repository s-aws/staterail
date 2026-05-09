from __future__ import annotations

import json
from hashlib import sha256
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from core.enums import (
    CoinbaseWebSocketChannel,
    CoinbaseWebSocketEndpoint,
    EventType,
    WebSocketOperation,
)
from core.errors import ExchangeAuthError, ExchangeTransportError, FeedSourceError
from core.json_tools import canonical_json, normalize_json
from feeds.router import FeedMessage


JwtFactory = Callable[[dict[str, Any]], str]


class WebSocketConnection(Protocol):
    async def send(self, payload: str) -> None:
        ...

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        ...


class WebSocketConnector(Protocol):
    def connect(self, url: str) -> Any:
        ...


@dataclass(frozen=True)
class CoinbaseWebSocketConfig:
    source_id: str
    product_ids: tuple[str, ...]
    channels: tuple[CoinbaseWebSocketChannel, ...]
    endpoint: CoinbaseWebSocketEndpoint = CoinbaseWebSocketEndpoint.MARKET_DATA
    include_heartbeats: bool = True
    jwt_factory: JwtFactory | None = None

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id is required")
        if not self.channels:
            raise ValueError("At least one Coinbase websocket channel is required")
        for channel in self.channels:
            if not isinstance(channel, CoinbaseWebSocketChannel):
                raise TypeError("channels must be CoinbaseWebSocketChannel values")
        if len(self.channels) != len(set(self.channels)):
            raise ValueError("channels must be unique")
        if not isinstance(self.endpoint, CoinbaseWebSocketEndpoint):
            raise TypeError("endpoint must be a CoinbaseWebSocketEndpoint")
        if not isinstance(self.include_heartbeats, bool):
            raise TypeError("include_heartbeats must be a bool")
        for product_id in self.product_ids:
            if not isinstance(product_id, str) or not product_id:
                raise TypeError("product_ids must contain non-empty strings")
        if len(self.product_ids) != len(set(self.product_ids)):
            raise ValueError("product_ids must be unique")
        if any(_channel_uses_products(channel) for channel in self.channels) and not self.product_ids:
            raise ValueError("product_ids are required for product-scoped websocket channels")
        if CoinbaseWebSocketChannel.USER in self.channels:
            if len(self.channels) > 1:
                raise ValueError("user websocket channel must be configured separately")
            if self.endpoint != CoinbaseWebSocketEndpoint.USER_ORDER_DATA:
                raise ValueError("user websocket channel must use USER_ORDER_DATA endpoint")
        elif self.endpoint == CoinbaseWebSocketEndpoint.USER_ORDER_DATA:
            raise ValueError("USER_ORDER_DATA endpoint requires the user websocket channel")

    def subscription_channels(self) -> tuple[CoinbaseWebSocketChannel, ...]:
        channels = list(self.channels)
        if self.include_heartbeats and CoinbaseWebSocketChannel.HEARTBEATS not in channels:
            channels.insert(0, CoinbaseWebSocketChannel.HEARTBEATS)
        return tuple(dict.fromkeys(channels))

    def subscription_messages(
        self,
        operation: WebSocketOperation = WebSocketOperation.SUBSCRIBE,
    ) -> tuple[dict[str, Any], ...]:
        if not isinstance(operation, WebSocketOperation):
            raise TypeError("operation must be a WebSocketOperation")

        messages: list[dict[str, Any]] = []
        for channel in self.subscription_channels():
            message: dict[str, Any] = {
                "channel": channel.value,
                "type": operation.value,
            }
            if _channel_uses_products(channel):
                message["product_ids"] = list(self.product_ids)
            if self.jwt_factory is not None:
                try:
                    message["jwt"] = self.jwt_factory(message.copy())
                except ExchangeAuthError:
                    raise
                except Exception as exc:
                    raise ExchangeAuthError(
                        "Coinbase websocket JWT factory failed",
                        context={"channel": channel.value, "source_id": self.source_id},
                    ) from exc
            messages.append(message)
        return tuple(messages)


class CoinbaseAdvancedTradeFeedSource:
    def __init__(
        self,
        config: CoinbaseWebSocketConfig,
        *,
        connector: WebSocketConnector | None = None,
        normalizer: "CoinbaseMessageNormalizer | None" = None,
    ) -> None:
        self._config = config
        self._connector = connector or WebsocketsConnector()
        self._normalizer = normalizer or CoinbaseMessageNormalizer()

    @property
    def source_id(self) -> str:
        return self._config.source_id

    async def stream(self) -> AsyncIterator[FeedMessage]:
        try:
            async with self._connector.connect(self._config.endpoint.value) as websocket:
                for message in self._config.subscription_messages():
                    await websocket.send(canonical_json(message))

                async for raw_payload in websocket:
                    raw_message = _decode_message(raw_payload)
                    for normalized_message in self._normalizer.normalize(self.source_id, raw_message):
                        yield normalized_message
        except (ExchangeAuthError, ExchangeTransportError, FeedSourceError):
            raise
        except Exception as exc:
            raise ExchangeTransportError(
                "Coinbase websocket stream failed",
                context={
                    "endpoint": self._config.endpoint.value,
                    "source_id": self.source_id,
                },
            ) from exc


class CoinbaseMessageNormalizer:
    def __init__(self) -> None:
        self._last_sequence: int | None = None

    def normalize(self, source_id: str, raw_message: dict[str, Any]) -> tuple[FeedMessage, ...]:
        normalized = normalize_json(raw_message)
        if not isinstance(normalized, dict):
            raise TypeError("Coinbase websocket messages must be JSON objects")

        channel = str(normalized.get("channel", "unknown"))
        sequence = _optional_int(normalized.get("sequence_num"))
        timestamp = normalized.get("timestamp")

        messages: list[FeedMessage] = []
        if sequence is not None:
            messages.extend(
                self._sequence_anomalies(
                    source_id=source_id,
                    channel=channel,
                    sequence=sequence,
                    timestamp=str(timestamp) if timestamp is not None else None,
                )
            )

        if channel == CoinbaseWebSocketChannel.HEARTBEATS.value:
            messages.append(
                FeedMessage(
                    source_id=source_id,
                    message_key=_message_key(channel, sequence, normalized),
                    event_type=EventType.FEED_HEARTBEAT,
                    payload={
                        "channel": channel,
                        "raw": normalized,
                        "sequence_num": sequence,
                        "timestamp": timestamp,
                    },
                )
            )
            return tuple(messages)

        user_messages = _user_order_messages(
            source_id=source_id,
            normalized=normalized,
            sequence=sequence,
            timestamp=str(timestamp) if timestamp is not None else None,
        )
        if user_messages:
            messages.extend(user_messages)
            return tuple(messages)

        messages.append(
            FeedMessage(
                source_id=source_id,
                message_key=_message_key(channel, sequence, normalized),
                event_type=EventType.DATA_RECEIVED,
                payload={
                    "channel": channel,
                    "raw": normalized,
                    "sequence_num": sequence,
                    "timestamp": timestamp,
                },
            )
        )
        return tuple(messages)

    def _sequence_anomalies(
        self,
        *,
        source_id: str,
        channel: str,
        sequence: int,
        timestamp: str | None,
    ) -> list[FeedMessage]:
        previous = self._last_sequence
        self._last_sequence = max(sequence, previous or sequence)

        if previous is None or sequence == previous + 1:
            return []

        if sequence <= previous:
            return [
                FeedMessage(
                    source_id=source_id,
                    message_key=f"coinbase:out-of-order:connection:{channel}:{previous}:{sequence}",
                    event_type=EventType.DATA_OUT_OF_ORDER,
                    payload={
                        "channel": channel,
                        "observed_sequence": sequence,
                        "previous_sequence": previous,
                        "timestamp": timestamp,
                        "track_key": f"connection:{channel}",
                    },
                )
            ]

        return [
            FeedMessage(
                source_id=source_id,
                message_key=f"coinbase:sequence-gap:connection:{channel}:{previous}:{sequence}",
                event_type=EventType.DATA_SEQUENCE_GAP,
                payload={
                    "channel": channel,
                    "gap_size": sequence - previous - 1,
                    "observed_sequence": sequence,
                    "previous_sequence": previous,
                    "timestamp": timestamp,
                    "track_key": f"connection:{channel}",
                },
            )
        ]


class WebsocketsConnector:
    def connect(self, url: str) -> Any:
        try:
            import websockets
        except ImportError as exc:
            raise ExchangeTransportError(
                "Install the 'websockets' package to use the live Coinbase adapter",
                context={"url": url},
                retryable=False,
            ) from exc
        try:
            return websockets.connect(url, close_timeout=1)
        except ExchangeTransportError:
            raise
        except Exception as exc:
            raise ExchangeTransportError(
                "Coinbase websocket connect failed",
                context={"url": url},
            ) from exc


def _channel_uses_products(channel: CoinbaseWebSocketChannel) -> bool:
    return channel not in {
        CoinbaseWebSocketChannel.FUTURES_BALANCE_SUMMARY,
        CoinbaseWebSocketChannel.HEARTBEATS,
    }


def _decode_message(raw_payload: str | bytes) -> dict[str, Any]:
    if isinstance(raw_payload, bytes):
        raw_payload = raw_payload.decode("utf-8")
    try:
        parsed = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise FeedSourceError("Coinbase websocket message was not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise FeedSourceError("Coinbase websocket message was not a JSON object")
    return parsed


def _message_key(channel: str, sequence: int | None, raw_message: dict[str, Any]) -> str:
    digest = _message_digest(raw_message)
    if sequence is not None:
        return f"coinbase:{channel}:{sequence}:{digest}"
    return f"coinbase:{channel}:no-sequence:{digest}"


def _message_digest(raw_message: dict[str, Any]) -> str:
    return sha256(canonical_json(raw_message).encode("utf-8")).hexdigest()[:16]


def _user_order_messages(
    *,
    source_id: str,
    normalized: dict[str, Any],
    sequence: int | None,
    timestamp: str | None,
) -> tuple[FeedMessage, ...]:
    if normalized.get("channel") != CoinbaseWebSocketChannel.USER.value:
        return ()

    messages: list[FeedMessage] = []
    for event_index, event in enumerate(_events(normalized.get("events"))):
        for order_index, order in enumerate(_orders(event.get("orders"))):
            messages.append(
                FeedMessage(
                    source_id=source_id,
                    message_key=_user_order_message_key(
                        order,
                        sequence,
                        event_index,
                        order_index,
                        timestamp,
                    ),
                    event_type=EventType.EXCHANGE_ORDER_UPDATE,
                    payload={
                        "channel": CoinbaseWebSocketChannel.USER.value,
                        "event_type": event.get("type"),
                        "order": order,
                        "sequence_num": sequence,
                        "timestamp": timestamp,
                    },
                )
            )
    return tuple(messages)


def _user_order_message_key(
    order: dict[str, Any],
    sequence: int | None,
    event_index: int,
    order_index: int,
    timestamp: str | None,
) -> str:
    order_id = _string_or_none(order.get("order_id")) or _string_or_none(order.get("client_order_id")) or "unknown"
    digest_payload = {"order": order, "timestamp": timestamp}
    digest = _message_digest(digest_payload)
    if sequence is not None:
        return f"coinbase:user-order:{order_id}:{sequence}:{event_index}:{order_index}:{digest}"
    return f"coinbase:user-order:{order_id}:no-sequence:{digest}"


def _events(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _orders(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
