from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from actions.gateway import ActionCommand, ActionGateway, CancelOrderIntent, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ErrorCategory,
    EventType,
    ExchangeLookupStatus,
    ExecutionMode,
    ExecutionStatus,
    HttpMethod,
    MarginType,
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
    ProductVenue,
    TimeInForce,
)
from core.errors import ExchangeAuthError, ExchangeTransportError
from exchanges.coinbase.advanced_trade_rest import (
    CoinbaseAdvancedTradeAccountLookupClient,
    CoinbaseAdvancedTradeFillLookupClient,
    CoinbaseAdvancedTradeOrderLookupClient,
    CoinbaseAdvancedTradePositionLookupClient,
    CoinbaseAdvancedTradeRestExecutor,
    CoinbaseRestConfig,
    CoinbaseRestRetryPolicy,
    CoinbaseRetryingHttpTransport,
    HttpResponse,
    build_create_order_request,
    coinbase_account_to_exchange_balance,
    coinbase_cfm_position_to_exchange_position,
    coinbase_intx_position_to_exchange_position,
    coinbase_rest_fill_to_exchange_fill,
    coinbase_rest_order_to_exchange_update,
)
from exchanges.coinbase.auth import RestAuthRequest, static_token_provider
from projections.state import SourceOfTruthProjection


class FakeTransport:
    def __init__(self, responses: list[HttpResponse], get_responses: list[HttpResponse] | None = None) -> None:
        self._responses = responses
        self._get_responses = get_responses or []
        self.gets: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        self.gets.append({"headers": dict(headers), "query_params": query_params, "url": url})
        return self._get_responses.pop(0)

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        self.posts.append({"headers": dict(headers), "json_body": dict(json_body), "url": url})
        return self._responses.pop(0)


class RaisingTransport:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        raise self._exc

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        raise self._exc


class RaisingThenGetTransport:
    def __init__(self, exc: Exception, response: HttpResponse) -> None:
        self._exc = exc
        self._response = response
        self.gets: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        self.gets.append({"headers": dict(headers), "query_params": query_params, "url": url})
        if len(self.gets) == 1:
            raise self._exc
        return self._response

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        raise AssertionError("unexpected POST")


def test_coinbase_create_order_request_maps_cfm_limit_gtc_order():
    request = build_create_order_request(
        _place_order("place-1", idempotency_key="client-1"),
        portfolio_id="portfolio-1",
    )

    assert request.path == "/orders"
    assert request.body == {
        "client_order_id": "client-1",
        "leverage": "3",
        "margin_type": "CROSS",
        "order_configuration": {
            "limit_limit_gtc": {
                "base_size": "0.01",
                "limit_price": "100000",
                "post_only": False,
            }
        },
        "product_id": "BIT-29MAY26-CDE",
        "retail_portfolio_id": "portfolio-1",
        "side": "BUY",
    }


def test_coinbase_create_order_request_honors_post_only_limit_gtc_order():
    request = build_create_order_request(
        _place_order("place-1", post_only=True),
        portfolio_id="portfolio-1",
    )

    assert request.body["order_configuration"] == {
        "limit_limit_gtc": {
            "base_size": "0.01",
            "limit_price": "100000",
            "post_only": True,
        }
    }


def test_coinbase_create_order_request_maps_market_ioc_order():
    request = build_create_order_request(
        _place_order(
            "place-1",
            order_type=OrderType.MARKET,
            limit_price=None,
            time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL,
        )
    )

    assert request.body["order_configuration"] == {
        "market_market_ioc": {
            "base_size": "0.01",
        }
    }


def test_coinbase_rest_order_lookup_fetches_historical_order_by_id():
    transport = FakeTransport(
        [],
        get_responses=[
            HttpResponse(
                status_code=200,
                body={
                    "order": {
                        "client_order_id": "client-1",
                        "order_configuration": {
                            "limit_limit_gtc": {
                                "base_size": "0.01",
                                "limit_price": "100000",
                            }
                        },
                        "order_id": "exchange-1",
                        "product_id": "BTC-PERP-INTX",
                        "side": "BUY",
                        "status": "OPEN",
                    }
                },
            )
        ],
    )

    result = _lookup_client(transport).get_order("exchange-1")

    assert transport.gets[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/orders/historical/exchange-1"
    assert transport.gets[0]["headers"]["Authorization"] == "Bearer test-token"
    assert result.status == ExchangeLookupStatus.FOUND
    assert result.order_update["order_id"] == "exchange-1"
    assert result.order_update["order_type"] == "LIMIT"
    assert result.order_update["limit_price"] == "100000"


def test_coinbase_rest_token_provider_receives_request_specific_jwt_uri():
    requests: list[RestAuthRequest] = []
    transport = FakeTransport(
        [],
        get_responses=[
            HttpResponse(
                status_code=200,
                body={
                    "order": {
                        "client_order_id": "client-1",
                        "order_id": "exchange-1",
                        "product_id": "BTC-USD",
                        "side": "BUY",
                        "status": "OPEN",
                    }
                },
            )
        ],
    )
    client = CoinbaseAdvancedTradeOrderLookupClient(
        CoinbaseRestConfig(),
        token_provider=lambda request: requests.append(request) or "signed-token",
        transport=transport,
    )

    result = client.get_order("exchange-1")

    assert result.status == ExchangeLookupStatus.FOUND
    assert requests[0].method == HttpMethod.GET
    assert requests[0].path == "/api/v3/brokerage/orders/historical/exchange-1"
    assert requests[0].jwt_uri == "GET api.coinbase.com/api/v3/brokerage/orders/historical/exchange-1"
    assert transport.gets[0]["headers"]["Authorization"] == "Bearer signed-token"


def test_coinbase_rest_order_lookup_normalizes_http_failures():
    transport = FakeTransport(
        [],
        get_responses=[HttpResponse(status_code=500, body={"message": "server unavailable"})],
    )

    result = _lookup_client(transport).get_order("exchange-1")

    assert result.status == ExchangeLookupStatus.FAILED
    assert result.error_code == "http_500"
    assert result.error_message == "server unavailable"
    assert result.error_category == ErrorCategory.EXCHANGE_TRANSPORT
    assert result.retryable is True


def test_coinbase_retrying_transport_retries_retryable_http_response_and_audits(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    sleep_calls: list[float] = []
    transport = FakeTransport(
        [],
        get_responses=[
            HttpResponse(status_code=429, body={"message": "slow down"}),
            HttpResponse(
                status_code=200,
                body={
                    "order": {
                        "client_order_id": "client-1",
                        "order_id": "exchange-1",
                        "product_id": "BTC-USD",
                        "side": "BUY",
                        "status": "OPEN",
                    }
                },
            ),
        ],
    )
    retrying_transport = CoinbaseRetryingHttpTransport(
        transport,
        core=core,
        policy=CoinbaseRestRetryPolicy(max_attempts=2, initial_delay_seconds=0.5),
        sleep=sleep_calls.append,
    )

    result = _lookup_client(retrying_transport).get_order("exchange-1")
    projection = SourceOfTruthProjection.from_ledger(ledger)
    retry = projection.exchange_request_retries[0]

    assert result.status == ExchangeLookupStatus.FOUND
    assert len(transport.gets) == 2
    assert sleep_calls == [0.5]
    assert [record.event_type for record in ledger.iter_records()] == [EventType.EXCHANGE_REQUEST_RETRY]
    assert retry.attempt == 1
    assert retry.next_attempt == 2
    assert retry.error_category == ErrorCategory.EXCHANGE_RATE_LIMIT
    assert retry.error_code == "http_429"
    assert retry.status_code == 429


def test_coinbase_retrying_transport_retries_retryable_post_response_and_audits(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    sleep_calls: list[float] = []
    transport = FakeTransport(
        [
            HttpResponse(status_code=503, body={"message": "orders unavailable"}),
            HttpResponse(
                status_code=200,
                body={
                    "success": True,
                    "success_response": {
                        "client_order_id": "client-1",
                        "order_id": "exchange-1",
                    },
                },
            ),
        ]
    )
    retrying_transport = CoinbaseRetryingHttpTransport(
        transport,
        core=core,
        policy=CoinbaseRestRetryPolicy(max_attempts=2, initial_delay_seconds=0.5),
        sleep=sleep_calls.append,
    )
    executor = CoinbaseAdvancedTradeRestExecutor(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=retrying_transport,
    )

    result = executor.execute(_place_order("place-1", idempotency_key="client-1"))
    retry = SourceOfTruthProjection.from_ledger(ledger).exchange_request_retries[0]

    assert result.status == ExecutionStatus.ACCEPTED
    assert result.exchange_order_id == "exchange-1"
    assert len(transport.posts) == 2
    assert sleep_calls == [0.5]
    assert [record.event_type for record in ledger.iter_records()] == [EventType.EXCHANGE_REQUEST_RETRY]
    assert retry.attempt == 1
    assert retry.next_attempt == 2
    assert retry.error_category == ErrorCategory.EXCHANGE_TRANSPORT
    assert retry.error_code == "http_503"
    assert retry.method == HttpMethod.POST
    assert retry.payload["has_json_body"] is True
    assert retry.status_code == 503


def test_coinbase_retrying_transport_does_not_retry_non_retryable_http_response(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    sleep_calls: list[float] = []
    transport = FakeTransport(
        [],
        get_responses=[HttpResponse(status_code=401, body={"message": "not authorized"})],
    )
    retrying_transport = CoinbaseRetryingHttpTransport(
        transport,
        core=core,
        policy=CoinbaseRestRetryPolicy(max_attempts=3),
        sleep=sleep_calls.append,
    )

    result = _lookup_client(retrying_transport).get_order("exchange-1")

    assert result.status == ExchangeLookupStatus.FAILED
    assert len(transport.gets) == 1
    assert sleep_calls == []
    assert ledger.iter_records() == ()


def test_coinbase_retrying_transport_retries_retryable_transport_exception(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    core = AuditCore(ledger)
    sleep_calls: list[float] = []
    transport = RaisingThenGetTransport(
        TimeoutError("timed out"),
        HttpResponse(
            status_code=200,
            body={
                "order": {
                    "client_order_id": "client-1",
                    "order_id": "exchange-1",
                    "product_id": "BTC-USD",
                    "side": "BUY",
                    "status": "OPEN",
                }
            },
        ),
    )
    retrying_transport = CoinbaseRetryingHttpTransport(
        transport,
        core=core,
        policy=CoinbaseRestRetryPolicy(max_attempts=2, initial_delay_seconds=0),
        sleep=sleep_calls.append,
    )

    result = _lookup_client(retrying_transport).get_order("exchange-1")
    retry = SourceOfTruthProjection.from_ledger(ledger).exchange_request_retries[0]

    assert result.status == ExchangeLookupStatus.FOUND
    assert len(transport.gets) == 2
    assert sleep_calls == [0]
    assert retry.error_category == ErrorCategory.EXCHANGE_TRANSPORT
    assert retry.payload["exception_type"] == "ExchangeTransportError"


def test_coinbase_rest_order_lookup_classifies_auth_failures():
    transport = FakeTransport(
        [],
        get_responses=[HttpResponse(status_code=401, body={"message": "not authorized"})],
    )

    result = _lookup_client(transport).get_order("exchange-1")

    assert result.status == ExchangeLookupStatus.FAILED
    assert result.error_code == "http_401"
    assert result.error_category == ErrorCategory.EXCHANGE_AUTH
    assert result.retryable is False


def test_coinbase_rest_fill_lookup_fetches_fills_by_order_id():
    transport = FakeTransport(
        [],
        get_responses=[
            HttpResponse(
                status_code=200,
                body={
                    "cursor": "cursor-2",
                    "fills": [
                        {
                            "commission": "1.25",
                            "entry_id": "fill-1",
                            "order_id": "exchange-1",
                            "price": "100000",
                            "product_id": "BTC-USD",
                            "side": "BUY",
                            "size": "0.01",
                            "trade_id": "trade-1",
                            "trade_time": "2026-01-01T00:00:00Z",
                            "trade_type": "FILL",
                        }
                    ],
                    "proof_token_required": True,
                },
            )
        ],
    )

    result = _fill_lookup_client(transport).list_fills(order_ids=("exchange-1",), limit=50)

    assert transport.gets[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/orders/historical/fills"
    assert transport.gets[0]["query_params"] == {"limit": 50, "order_ids": ["exchange-1"]}
    assert result.status == ExchangeLookupStatus.FOUND
    assert result.cursor == "cursor-2"
    assert result.proof_token_required is True
    assert result.fills[0]["fill_id"] == "fill-1"
    assert result.fills[0]["order_id"] == "exchange-1"


def test_coinbase_rest_fill_lookup_normalizes_http_failures():
    transport = FakeTransport(
        [],
        get_responses=[HttpResponse(status_code=503, body={"message": "fills unavailable"})],
    )

    result = _fill_lookup_client(transport).list_fills(order_ids=("exchange-1",))

    assert result.status == ExchangeLookupStatus.FAILED
    assert result.error_code == "http_503"
    assert result.error_message == "fills unavailable"
    assert result.error_category == ErrorCategory.EXCHANGE_TRANSPORT
    assert result.retryable is True


def test_coinbase_rest_fill_update_has_stable_fallback_fill_id():
    update = coinbase_rest_fill_to_exchange_fill(
        {
            "order_id": "exchange-1",
            "price": "100000",
            "product_id": "BTC-USD",
            "side": "SELL",
            "size": "0.01",
            "trade_time": "2026-01-01T00:00:00Z",
        }
    )

    assert update["fill_id"] == "exchange-1:2026-01-01T00:00:00Z:100000:0.01"
    assert update["side"] == "SELL"


def test_coinbase_account_lookup_fetches_paginated_accounts():
    transport = FakeTransport(
        [],
        get_responses=[
            HttpResponse(
                status_code=200,
                body={
                    "accounts": [
                        {
                            "available_balance": {"currency": "BTC", "value": "1.23"},
                            "currency": "BTC",
                            "hold": {"currency": "BTC", "value": "0.1"},
                            "name": "BTC Wallet",
                            "ready": True,
                            "type": "CRYPTO",
                            "uuid": "account-1",
                        }
                    ],
                    "cursor": "cursor-2",
                    "has_next": True,
                    "size": 1,
                },
            )
        ],
    )

    result = _account_lookup_client(transport).list_accounts(limit=25)

    assert transport.gets[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/accounts"
    assert transport.gets[0]["query_params"] == {"limit": 25}
    assert result.status == ExchangeLookupStatus.FOUND
    assert result.has_next is True
    assert result.cursor == "cursor-2"
    assert result.accounts[0]["account_id"] == "account-1"
    assert result.accounts[0]["venue"] == ProductVenue.CBE.value


def test_coinbase_position_lookup_fetches_cfm_and_intx_positions():
    transport = FakeTransport(
        [],
        get_responses=[
            HttpResponse(
                status_code=200,
                body={
                    "positions": [
                        {
                            "avg_entry_price": "100000",
                            "current_price": "100100",
                            "number_of_contracts": "2",
                            "product_id": "BTC-FUT",
                            "side": "SHORT",
                        }
                    ]
                },
            ),
            HttpResponse(
                status_code=200,
                body={
                    "positions": [
                        {
                            "entry_vwap": {"currency": "USDC", "value": "100000"},
                            "mark_price": {"currency": "USDC", "value": "100100"},
                            "net_size": "0.01",
                            "position_side": "POSITION_SIDE_LONG",
                            "product_id": "BTC-PERP-INTX",
                            "symbol": "BTC-PERP",
                        }
                    ]
                },
            ),
        ],
    )
    client = _position_lookup_client(transport)

    cfm = client.list_us_futures_positions()
    intx = client.list_perpetual_positions("portfolio-1")

    assert transport.gets[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/cfm/positions"
    assert transport.gets[1]["url"] == "https://api.coinbase.com/api/v3/brokerage/intx/positions/portfolio-1"
    assert cfm.positions[0]["net_size"] == "-2"
    assert cfm.positions[0]["venue"] == ProductVenue.FCM.value
    assert intx.positions[0]["net_size"] == "0.01"
    assert intx.positions[0]["venue"] == ProductVenue.INTX.value


def test_coinbase_exchange_state_normalizers_keep_core_fields():
    balance = coinbase_account_to_exchange_balance(
        {
            "available_balance": {"currency": "USDC", "value": "10"},
            "currency": "USDC",
            "hold": {"value": "2"},
            "uuid": "account-1",
        }
    )
    cfm = coinbase_cfm_position_to_exchange_position(
        {"number_of_contracts": "3", "product_id": "ETH-FUT", "side": "LONG"}
    )
    intx = coinbase_intx_position_to_exchange_position(
        {
            "entry_vwap": {"value": "1000"},
            "net_size": "-0.5",
            "product_id": "ETH-PERP-INTX",
        }
    )

    assert balance["available"] == "10"
    assert balance["hold"] == "2"
    assert cfm["net_size"] == "3"
    assert intx["average_entry_price"] == "1000"
    assert intx["net_size"] == "-0.5"


def test_coinbase_rest_order_update_normalizes_market_order_shape():
    update = coinbase_rest_order_to_exchange_update(
        {
            "client_order_id": "client-1",
            "order_configuration": {"market_market_ioc": {"base_size": "0.01"}},
            "order_id": "exchange-1",
            "product_id": "BTC-USD",
            "side": "SELL",
            "status": "FILLED",
        }
    )

    assert update["leaves_quantity"] == "0.01"
    assert update["order_side"] == "SELL"
    assert update["order_type"] == "MARKET"


def test_coinbase_rest_executor_places_order_and_normalizes_success_response(workspace_tmp_path):
    transport = FakeTransport(
        [
            HttpResponse(
                status_code=200,
                body={
                    "success": True,
                    "success_response": {
                        "client_order_id": "client-1",
                        "order_id": "exchange-1",
                    },
                },
            )
        ]
    )
    executor = _executor(transport)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(
        _place_order("place-1", idempotency_key="client-1"),
        executor,
    )

    records = ledger.iter_records()
    projection = SourceOfTruthProjection.from_ledger(ledger)
    executed_record = next(record for record in records if record.event_type == EventType.ACTION_EXECUTED)
    execution_result = executed_record.payload["execution_result"]
    assert receipt.status == ActionStatus.EXECUTED
    assert transport.posts[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/orders"
    assert transport.posts[0]["headers"]["Authorization"] == "Bearer test-token"
    assert execution_result["status"] == ExecutionStatus.ACCEPTED.value
    assert execution_result["mode"] == ExecutionMode.LIVE.value
    assert execution_result["exchange_order_id"] == "exchange-1"
    assert projection.orders_by_exchange_order_id["exchange-1"].lifecycle_status == OrderLifecycleStatus.OPEN


def test_coinbase_rest_executor_returns_rejected_result_for_order_error_response():
    transport = FakeTransport(
        [
            HttpResponse(
                status_code=200,
                body={
                    "success": False,
                    "error_response": {
                        "error": "UNKNOWN_FAILURE_REASON",
                        "message": "order rejected",
                    },
                },
            )
        ]
    )

    result = _executor(transport).execute(_place_order("place-1"))

    assert result.status == ExecutionStatus.REJECTED
    assert result.error_code == "UNKNOWN_FAILURE_REASON"
    assert result.error_message == "order rejected"


def test_coinbase_rest_executor_cancels_order_by_exchange_order_id(workspace_tmp_path):
    transport = FakeTransport(
        [
            HttpResponse(
                status_code=200,
                body={
                    "results": [
                        {
                            "order_id": "exchange-1",
                            "success": True,
                        }
                    ]
                },
            )
        ]
    )
    executor = _executor(transport)
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger))

    receipt = gateway.submit_and_execute(
        CancelOrderIntent(action_id="cancel-1", exchange_order_id="exchange-1").to_command(),
        executor,
    )

    execution_result = ledger.iter_records()[-1].payload["execution_result"]
    assert receipt.status == ActionStatus.EXECUTED
    assert transport.posts[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/orders/batch_cancel"
    assert transport.posts[0]["json_body"] == {"order_ids": ["exchange-1"]}
    assert execution_result["status"] == ExecutionStatus.CANCELLED.value
    assert execution_result["exchange_order_id"] == "exchange-1"


def test_coinbase_rest_executor_rejects_cancel_without_exchange_order_id_without_http_call():
    transport = FakeTransport([])
    command = CancelOrderIntent(action_id="cancel-1", client_order_id="client-1").to_command()

    result = _executor(transport).execute(command)

    assert result.status == ExecutionStatus.REJECTED
    assert result.error_code == "exchange_order_id_required"
    assert transport.posts == []


def test_coinbase_rest_executor_marks_http_errors_as_failed():
    transport = FakeTransport([HttpResponse(status_code=500, body={"message": "server error"})])

    result = _executor(transport).execute(_place_order("place-1"))

    assert result.status == ExecutionStatus.FAILED
    assert result.error_code == "http_500"
    assert result.error_message == "server error"
    assert result.error_category == ErrorCategory.EXCHANGE_TRANSPORT
    assert result.retryable is True


def test_coinbase_rest_executor_classifies_rate_limit_failures():
    transport = FakeTransport([HttpResponse(status_code=429, body={"message": "rate limited"})])

    result = _executor(transport).execute(_place_order("place-1"))

    assert result.status == ExecutionStatus.FAILED
    assert result.error_code == "http_429"
    assert result.error_category == ErrorCategory.EXCHANGE_RATE_LIMIT
    assert result.retryable is True


def test_coinbase_rest_empty_token_raises_auth_error():
    executor = CoinbaseAdvancedTradeRestExecutor(
        CoinbaseRestConfig(),
        token_provider=static_token_provider(""),
        transport=FakeTransport([]),
    )

    with pytest.raises(ExchangeAuthError, match="empty token"):
        executor.execute(_place_order("place-1"))


def test_coinbase_rest_transport_exceptions_raise_typed_transport_error():
    client = CoinbaseAdvancedTradeOrderLookupClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=RaisingTransport(TimeoutError("timed out")),
    )

    with pytest.raises(ExchangeTransportError) as exc_info:
        client.get_order("exchange-1")

    assert exc_info.value.retryable is True


def _executor(transport: FakeTransport) -> CoinbaseAdvancedTradeRestExecutor:
    return CoinbaseAdvancedTradeRestExecutor(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )


def _lookup_client(transport: FakeTransport) -> CoinbaseAdvancedTradeOrderLookupClient:
    return CoinbaseAdvancedTradeOrderLookupClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )


def _fill_lookup_client(transport: FakeTransport) -> CoinbaseAdvancedTradeFillLookupClient:
    return CoinbaseAdvancedTradeFillLookupClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )


def _account_lookup_client(transport: FakeTransport) -> CoinbaseAdvancedTradeAccountLookupClient:
    return CoinbaseAdvancedTradeAccountLookupClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )


def _position_lookup_client(transport: FakeTransport) -> CoinbaseAdvancedTradePositionLookupClient:
    return CoinbaseAdvancedTradePositionLookupClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )


def _place_order(
    action_id: str,
    *,
    idempotency_key: str | None = None,
    order_type: OrderType = OrderType.LIMIT,
    limit_price: str | None = "100000",
    time_in_force: TimeInForce = TimeInForce.GOOD_UNTIL_CANCELLED,
    post_only: bool = False,
) -> ActionCommand:
    return PlaceOrderIntent(
        action_id=action_id,
        product_id="BIT-29MAY26-CDE",
        side=OrderSide.BUY,
        order_type=order_type,
        size="0.01",
        limit_price=limit_price,
        leverage="3",
        margin_type=MarginType.CROSS,
        time_in_force=time_in_force,
        idempotency_key=idempotency_key,
        post_only=post_only,
    ).to_command()
