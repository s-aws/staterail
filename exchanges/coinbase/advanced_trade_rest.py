from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from actions.execution import ExecutionResult
from actions.gateway import ActionCommand
from core.engine import AuditCore
from core.enums import (
    ActionType,
    ErrorCategory,
    ErrorCode,
    EventType,
    ExchangeLookupStatus,
    ExecutionMode,
    ExecutionStatus,
    HttpMethod,
    MarginType,
    OrderSide,
    OrderType,
    ProductVenue,
    TimeInForce,
)
from core.errors import BotError, ExchangeAuthError, ExchangeRateLimitError, ExchangeTransportError
from core.json_tools import JsonValue, normalize_json
from exchanges.coinbase.auth import RestAuthRequest, TokenProvider, rest_auth_request


RestRetrySleep = Callable[[float], None]


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    body: Mapping[str, Any]


@dataclass(frozen=True)
class CoinbaseHttpFailure:
    status_code: int
    category: ErrorCategory
    error_code: str
    message: str
    retryable: bool


@dataclass(frozen=True)
class CoinbaseRestRetryPolicy:
    max_attempts: int = 1
    initial_delay_seconds: float = 0.25
    max_delay_seconds: float = 2.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int):
            raise TypeError("max_attempts must be an int")
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if isinstance(self.initial_delay_seconds, bool) or not isinstance(self.initial_delay_seconds, int | float):
            raise TypeError("initial_delay_seconds must be numeric")
        if self.initial_delay_seconds < 0:
            raise ValueError("initial_delay_seconds must be non-negative")
        if isinstance(self.max_delay_seconds, bool) or not isinstance(self.max_delay_seconds, int | float):
            raise TypeError("max_delay_seconds must be numeric")
        if self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must be non-negative")
        if isinstance(self.multiplier, bool) or not isinstance(self.multiplier, int | float):
            raise TypeError("multiplier must be numeric")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")

    def delay_for_attempt(self, completed_attempts: int) -> float:
        if completed_attempts <= 0:
            raise ValueError("completed_attempts must be positive")
        delay = self.initial_delay_seconds * self.multiplier ** max(completed_attempts - 1, 0)
        return min(delay, self.max_delay_seconds)


class HttpTransport(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        ...

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        ...


class CoinbaseRetryingHttpTransport:
    def __init__(
        self,
        transport: HttpTransport,
        *,
        core: AuditCore,
        policy: CoinbaseRestRetryPolicy | None = None,
        sleep: RestRetrySleep | None = None,
    ) -> None:
        self._core = core
        self._policy = policy or CoinbaseRestRetryPolicy()
        self._sleep = sleep or time.sleep
        self._transport = transport

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        return self._request(
            HttpMethod.GET,
            url,
            query_params=query_params,
            request=lambda: self._transport.get(url, headers=headers, query_params=query_params),
        )

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        return self._request(
            HttpMethod.POST,
            url,
            has_json_body=True,
            request=lambda: self._transport.post(url, headers=headers, json_body=json_body),
        )

    def _request(
        self,
        method: HttpMethod,
        url: str,
        *,
        request: Callable[[], HttpResponse],
        has_json_body: bool = False,
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        attempt = 1
        while True:
            try:
                response = request()
            except Exception as exc:
                classified = _transport_exception(exc, method=method, url=url)
                if not _should_retry_exception(classified, attempt=attempt, policy=self._policy):
                    if classified is exc:
                        raise
                    raise classified from exc
                self._audit_retry(
                    method,
                    url,
                    attempt=attempt,
                    delay_seconds=self._policy.delay_for_attempt(attempt),
                    exc=classified,
                    has_json_body=has_json_body,
                    query_params=query_params,
                )
                attempt += 1
                continue

            if not _should_retry_response(response, attempt=attempt, policy=self._policy):
                return response

            failure = _http_failure(
                response.status_code,
                _normalized_body(response.body),
                fallback="Coinbase HTTP request failed",
            )
            self._audit_retry(
                method,
                url,
                attempt=attempt,
                delay_seconds=self._policy.delay_for_attempt(attempt),
                failure=failure,
                has_json_body=has_json_body,
                query_params=query_params,
            )
            attempt += 1

    def _audit_retry(
        self,
        method: HttpMethod,
        url: str,
        *,
        attempt: int,
        delay_seconds: float,
        exc: Exception | None = None,
        failure: CoinbaseHttpFailure | None = None,
        has_json_body: bool,
        query_params: Mapping[str, Any] | None,
    ) -> None:
        payload: dict[str, Any] = {
            "attempt": attempt,
            "delay_seconds": delay_seconds,
            "has_json_body": has_json_body,
            "max_attempts": self._policy.max_attempts,
            "method": method.value,
            "next_attempt": attempt + 1,
            "retryable": True,
            "url": url,
        }
        if query_params is not None:
            payload["query_params"] = normalize_json(query_params)
        if failure is not None:
            payload.update(
                {
                    "error_category": failure.category.value,
                    "error_code": failure.error_code,
                    "message": failure.message,
                    "status_code": failure.status_code,
                }
            )
        elif exc is not None:
            payload.update(_exception_retry_payload(exc))

        self._core.emit(EventType.EXCHANGE_REQUEST_RETRY, payload)
        self._sleep(delay_seconds)

@dataclass(frozen=True)
class CoinbaseRestConfig:
    base_url: str = "https://api.coinbase.com/api/v3/brokerage"
    portfolio_id: str | None = None
    execution_mode: ExecutionMode = ExecutionMode.LIVE

    def __post_init__(self) -> None:
        if not self.base_url:
            raise ValueError("base_url is required")
        if not isinstance(self.execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")


@dataclass(frozen=True)
class CoinbaseOrderRequest:
    path: str
    body: Mapping[str, Any]


@dataclass(frozen=True)
class CoinbaseOrderLookupResult:
    status: ExchangeLookupStatus
    status_code: int
    order_update: Mapping[str, Any] = field(default_factory=dict)
    raw_response: Mapping[str, Any] = field(default_factory=dict)
    error_category: ErrorCategory | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.status, ExchangeLookupStatus):
            raise TypeError("status must be an ExchangeLookupStatus")
        if self.error_category is not None and not isinstance(self.error_category, ErrorCategory):
            raise TypeError("error_category must be an ErrorCategory")


@dataclass(frozen=True)
class CoinbaseFillsLookupResult:
    status: ExchangeLookupStatus
    status_code: int
    fills: tuple[Mapping[str, Any], ...] = ()
    cursor: str | None = None
    proof_token_required: bool = False
    raw_response: Mapping[str, Any] = field(default_factory=dict)
    error_category: ErrorCategory | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.status, ExchangeLookupStatus):
            raise TypeError("status must be an ExchangeLookupStatus")
        if self.error_category is not None and not isinstance(self.error_category, ErrorCategory):
            raise TypeError("error_category must be an ErrorCategory")


@dataclass(frozen=True)
class CoinbaseAccountsLookupResult:
    status: ExchangeLookupStatus
    status_code: int
    accounts: tuple[Mapping[str, Any], ...] = ()
    cursor: str | None = None
    has_next: bool = False
    raw_response: Mapping[str, Any] = field(default_factory=dict)
    error_category: ErrorCategory | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.status, ExchangeLookupStatus):
            raise TypeError("status must be an ExchangeLookupStatus")
        if self.error_category is not None and not isinstance(self.error_category, ErrorCategory):
            raise TypeError("error_category must be an ErrorCategory")


@dataclass(frozen=True)
class CoinbasePositionsLookupResult:
    status: ExchangeLookupStatus
    status_code: int
    positions: tuple[Mapping[str, Any], ...] = ()
    raw_response: Mapping[str, Any] = field(default_factory=dict)
    error_category: ErrorCategory | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.status, ExchangeLookupStatus):
            raise TypeError("status must be an ExchangeLookupStatus")
        if self.error_category is not None and not isinstance(self.error_category, ErrorCategory):
            raise TypeError("error_category must be an ErrorCategory")


class CoinbaseAdvancedTradeRestExecutor:
    def __init__(
        self,
        config: CoinbaseRestConfig,
        *,
        token_provider: TokenProvider,
        transport: HttpTransport | None = None,
    ) -> None:
        self._config = config
        self._token_provider = token_provider
        self._transport = transport or UrlLibHttpTransport()

    def execute(self, command: ActionCommand) -> ExecutionResult:
        if command.action_type == ActionType.PLACE_ORDER:
            return self._place_order(command)
        if command.action_type == ActionType.CANCEL_ORDER:
            return self._cancel_order(command)
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=self._config.execution_mode,
            error_code=ErrorCode.UNSUPPORTED_ACTION_TYPE,
            error_message="unsupported action type",
            raw_response={"action": command.to_payload()},
        )

    def _place_order(self, command: ActionCommand) -> ExecutionResult:
        request = build_create_order_request(command, portfolio_id=self._config.portfolio_id)
        response = self._post(request)
        body = _normalized_body(response.body)
        if response.status_code >= 400:
            return _failed_result(command, self._config.execution_mode, body, response.status_code)

        if body.get("success") is True:
            success_response = _payload_dict(body.get("success_response"))
            return ExecutionResult(
                action_id=command.action_id,
                action_type=command.action_type,
                status=ExecutionStatus.ACCEPTED,
                mode=self._config.execution_mode,
                client_order_id=_string_or_none(success_response.get("client_order_id")) or _client_order_id(command),
                exchange_order_id=_string_or_none(success_response.get("order_id")),
                raw_response=body,
            )

        error_response = _payload_dict(body.get("error_response"))
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=self._config.execution_mode,
            client_order_id=_client_order_id(command),
            error_code=_string_or_none(error_response.get("error")) or "order_rejected",
            error_message=_error_message(error_response, fallback="Coinbase rejected order request"),
            raw_response=body,
        )

    def _cancel_order(self, command: ActionCommand) -> ExecutionResult:
        request_or_result = build_cancel_order_request(command)
        if isinstance(request_or_result, ExecutionResult):
            return request_or_result

        response = self._post(request_or_result)
        body = _normalized_body(response.body)
        if response.status_code >= 400:
            return _failed_result(command, self._config.execution_mode, body, response.status_code)

        result = _cancel_result(body)
        if result.get("success") is True:
            return ExecutionResult(
                action_id=command.action_id,
                action_type=command.action_type,
                status=ExecutionStatus.CANCELLED,
                mode=self._config.execution_mode,
                exchange_order_id=_string_or_none(result.get("order_id")) or _cancel_order_id(command),
                raw_response=body,
            )

        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=self._config.execution_mode,
            exchange_order_id=_cancel_order_id(command),
            error_code=_string_or_none(result.get("failure_reason")) or "cancel_rejected",
            error_message=_string_or_none(result.get("failure_reason")) or "Coinbase rejected cancel request",
            raw_response=body,
        )

    def _post(self, request: CoinbaseOrderRequest) -> HttpResponse:
        url = _url(self._config.base_url, request.path)
        return _transport_post(
            self._transport,
            url,
            headers=_auth_headers(
                self._token_provider,
                rest_auth_request(HttpMethod.POST, url),
                include_content_type=True,
            ),
            json_body=request.body,
        )


class CoinbaseAdvancedTradeOrderLookupClient:
    def __init__(
        self,
        config: CoinbaseRestConfig,
        *,
        token_provider: TokenProvider,
        transport: HttpTransport | None = None,
    ) -> None:
        self._config = config
        self._token_provider = token_provider
        self._transport = transport or UrlLibHttpTransport()

    def get_order(self, order_id: str) -> CoinbaseOrderLookupResult:
        if not order_id:
            raise ValueError("order_id is required")

        path_order_id = urllib.parse.quote(order_id, safe="")
        url = _url(self._config.base_url, f"/orders/historical/{path_order_id}")
        response = _transport_get(
            self._transport,
            url,
            headers=_auth_headers(self._token_provider, rest_auth_request(HttpMethod.GET, url)),
        )
        body = _normalized_body(response.body)
        if response.status_code == 404:
            return CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.NOT_FOUND,
                status_code=response.status_code,
                raw_response=body,
                error_code="order_not_found",
                error_message=_string_or_none(body.get("message")) or "Coinbase order was not found",
            )
        if response.status_code >= 400:
            failure = _http_failure(response.status_code, body, fallback="Coinbase order lookup failed")
            return CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=response.status_code,
                raw_response=body,
                error_category=failure.category,
                error_code=failure.error_code,
                error_message=failure.message,
                retryable=failure.retryable,
            )

        order = _payload_dict(body.get("order"))
        if not order:
            return CoinbaseOrderLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=response.status_code,
                raw_response=body,
                error_code="missing_order",
                error_message="Coinbase order lookup response did not include an order",
            )

        return CoinbaseOrderLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=response.status_code,
            order_update=coinbase_rest_order_to_exchange_update(order),
            raw_response=body,
        )


class CoinbaseAdvancedTradeFillLookupClient:
    def __init__(
        self,
        config: CoinbaseRestConfig,
        *,
        token_provider: TokenProvider,
        transport: HttpTransport | None = None,
    ) -> None:
        self._config = config
        self._token_provider = token_provider
        self._transport = transport or UrlLibHttpTransport()

    def list_fills(
        self,
        *,
        order_ids: tuple[str, ...],
        cursor: str | None = None,
        limit: int = 100,
    ) -> CoinbaseFillsLookupResult:
        if not order_ids:
            raise ValueError("order_ids must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")

        query_params: dict[str, Any] = {
            "limit": limit,
            "order_ids": list(order_ids),
        }
        if cursor is not None:
            query_params["cursor"] = cursor

        url = _url(self._config.base_url, "/orders/historical/fills")
        response = _transport_get(
            self._transport,
            url,
            headers=_auth_headers(self._token_provider, rest_auth_request(HttpMethod.GET, url)),
            query_params=query_params,
        )
        body = _normalized_body(response.body)
        if response.status_code == 404:
            return CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.NOT_FOUND,
                status_code=response.status_code,
                raw_response=body,
                error_code="fills_not_found",
                error_message=_string_or_none(body.get("message")) or "Coinbase fills were not found",
            )
        if response.status_code >= 400:
            failure = _http_failure(response.status_code, body, fallback="Coinbase fills lookup failed")
            return CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=response.status_code,
                raw_response=body,
                error_category=failure.category,
                error_code=failure.error_code,
                error_message=failure.message,
                retryable=failure.retryable,
            )

        fills = body.get("fills")
        if not isinstance(fills, list):
            return CoinbaseFillsLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=response.status_code,
                raw_response=body,
                error_code="missing_fills",
                error_message="Coinbase fills lookup response did not include fills",
            )

        return CoinbaseFillsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=response.status_code,
            fills=tuple(coinbase_rest_fill_to_exchange_fill(fill) for fill in fills if isinstance(fill, dict)),
            cursor=_string_or_none(body.get("cursor")),
            proof_token_required=body.get("proof_token_required") is True,
            raw_response=body,
        )


class CoinbaseAdvancedTradeAccountLookupClient:
    def __init__(
        self,
        config: CoinbaseRestConfig,
        *,
        token_provider: TokenProvider,
        transport: HttpTransport | None = None,
    ) -> None:
        self._config = config
        self._token_provider = token_provider
        self._transport = transport or UrlLibHttpTransport()

    def list_accounts(
        self,
        *,
        cursor: str | None = None,
        limit: int = 250,
        retail_portfolio_id: str | None = None,
    ) -> CoinbaseAccountsLookupResult:
        if limit <= 0:
            raise ValueError("limit must be positive")

        query_params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            query_params["cursor"] = cursor
        if retail_portfolio_id is not None:
            query_params["retail_portfolio_id"] = retail_portfolio_id

        url = _url(self._config.base_url, "/accounts")
        response = _transport_get(
            self._transport,
            url,
            headers=_auth_headers(self._token_provider, rest_auth_request(HttpMethod.GET, url)),
            query_params=query_params,
        )
        body = _normalized_body(response.body)
        if response.status_code >= 400:
            failure = _http_failure(response.status_code, body, fallback="Coinbase accounts lookup failed")
            return CoinbaseAccountsLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=response.status_code,
                raw_response=body,
                error_category=failure.category,
                error_code=failure.error_code,
                error_message=failure.message,
                retryable=failure.retryable,
            )

        accounts = body.get("accounts")
        if not isinstance(accounts, list):
            return CoinbaseAccountsLookupResult(
                status=ExchangeLookupStatus.FAILED,
                status_code=response.status_code,
                raw_response=body,
                error_code="missing_accounts",
                error_message="Coinbase accounts lookup response did not include accounts",
            )

        return CoinbaseAccountsLookupResult(
            status=ExchangeLookupStatus.FOUND,
            status_code=response.status_code,
            accounts=tuple(coinbase_account_to_exchange_balance(account) for account in accounts if isinstance(account, dict)),
            cursor=_string_or_none(body.get("cursor")),
            has_next=body.get("has_next") is True,
            raw_response=body,
        )


class CoinbaseAdvancedTradePositionLookupClient:
    def __init__(
        self,
        config: CoinbaseRestConfig,
        *,
        token_provider: TokenProvider,
        transport: HttpTransport | None = None,
    ) -> None:
        self._config = config
        self._token_provider = token_provider
        self._transport = transport or UrlLibHttpTransport()

    def list_us_futures_positions(self) -> CoinbasePositionsLookupResult:
        url = _url(self._config.base_url, "/cfm/positions")
        response = _transport_get(
            self._transport,
            url,
            headers=_auth_headers(self._token_provider, rest_auth_request(HttpMethod.GET, url)),
        )
        body = _normalized_body(response.body)
        return _positions_result(
            body=body,
            normalizer=coinbase_cfm_position_to_exchange_position,
            status_code=response.status_code,
            error_message="Coinbase US derivatives positions lookup failed",
        )

    def list_perpetual_positions(self, portfolio_uuid: str) -> CoinbasePositionsLookupResult:
        if not portfolio_uuid:
            raise ValueError("portfolio_uuid is required")
        path_portfolio_uuid = urllib.parse.quote(portfolio_uuid, safe="")
        url = _url(self._config.base_url, f"/intx/positions/{path_portfolio_uuid}")
        response = _transport_get(
            self._transport,
            url,
            headers=_auth_headers(self._token_provider, rest_auth_request(HttpMethod.GET, url)),
        )
        body = _normalized_body(response.body)
        return _positions_result(
            body=body,
            normalizer=coinbase_intx_position_to_exchange_position,
            status_code=response.status_code,
            error_message="Coinbase perpetual positions lookup failed",
        )


class UrlLibHttpTransport:
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        if query_params:
            query_string = urllib.parse.urlencode(normalize_json(query_params), doseq=True)
            url = f"{url}?{query_string}"
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with urllib.request.urlopen(request) as response:
                body = json.loads(response.read().decode("utf-8"))
                return HttpResponse(status_code=response.status, body=body)
        except urllib.error.HTTPError as exc:
            return _http_error_response(exc)
        except urllib.error.URLError as exc:
            raise ExchangeTransportError(
                "Coinbase HTTP GET failed",
                context={"reason": str(exc.reason), "url": url},
            ) from exc
        except (OSError, TimeoutError) as exc:
            raise ExchangeTransportError(
                "Coinbase HTTP GET failed",
                context={"exception_type": exc.__class__.__name__, "url": url},
            ) from exc
        except json.JSONDecodeError as exc:
            raise ExchangeTransportError(
                "Coinbase HTTP GET response was not valid JSON",
                context={"url": url},
                retryable=False,
            ) from exc

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
    ) -> HttpResponse:
        request = urllib.request.Request(
            url,
            data=json.dumps(normalize_json(json_body)).encode("utf-8"),
            headers=dict(headers),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request) as response:
                body = json.loads(response.read().decode("utf-8"))
                return HttpResponse(status_code=response.status, body=body)
        except urllib.error.HTTPError as exc:
            return _http_error_response(exc)
        except urllib.error.URLError as exc:
            raise ExchangeTransportError(
                "Coinbase HTTP POST failed",
                context={"reason": str(exc.reason), "url": url},
            ) from exc
        except (OSError, TimeoutError) as exc:
            raise ExchangeTransportError(
                "Coinbase HTTP POST failed",
                context={"exception_type": exc.__class__.__name__, "url": url},
            ) from exc
        except json.JSONDecodeError as exc:
            raise ExchangeTransportError(
                "Coinbase HTTP POST response was not valid JSON",
                context={"url": url},
                retryable=False,
            ) from exc


def build_create_order_request(
    command: ActionCommand,
    *,
    portfolio_id: str | None = None,
) -> CoinbaseOrderRequest:
    if command.action_type != ActionType.PLACE_ORDER:
        raise ValueError("build_create_order_request requires a place-order command")

    payload = _command_payload(command)
    order_configuration = _order_configuration(payload)
    request_body: dict[str, Any] = {
        "client_order_id": _client_order_id(command),
        "order_configuration": order_configuration,
        "product_id": _required_string(payload, "product_id"),
        "side": _side(payload).value.upper(),
    }
    leverage = _string_or_none(payload.get("leverage"))
    if leverage is not None:
        request_body["leverage"] = leverage
    margin_type = _margin_type_or_none(payload.get("margin_type"))
    if margin_type is not None:
        request_body["margin_type"] = margin_type.value.upper()
    if portfolio_id is not None:
        request_body["retail_portfolio_id"] = portfolio_id
    return CoinbaseOrderRequest(path="/orders", body=request_body)


def build_cancel_order_request(command: ActionCommand) -> CoinbaseOrderRequest | ExecutionResult:
    if command.action_type != ActionType.CANCEL_ORDER:
        raise ValueError("build_cancel_order_request requires a cancel-order command")

    order_id = _cancel_order_id(command)
    if order_id is None:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=ExecutionMode.LIVE,
            error_code="exchange_order_id_required",
            error_message="Coinbase batch cancel requires exchange_order_id",
            raw_response={"action": command.to_payload()},
        )
    return CoinbaseOrderRequest(path="/orders/batch_cancel", body={"order_ids": [order_id]})


def coinbase_rest_order_to_exchange_update(order: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = _payload_dict(order)
    order_configuration = _payload_dict(normalized.get("order_configuration"))
    configuration = _active_order_configuration(order_configuration)
    order_update: dict[str, JsonValue] = {
        "client_order_id": _string_or_none(normalized.get("client_order_id")),
        "limit_price": _string_or_none(configuration.get("limit_price")),
        "order_id": _string_or_none(normalized.get("order_id")),
        "order_side": _string_or_none(normalized.get("side")),
        "order_type": _rest_order_type(normalized, order_configuration),
        "product_id": _string_or_none(normalized.get("product_id")),
        "raw_rest_order": normalized,
        "status": _string_or_none(normalized.get("status")),
    }
    leaves_quantity = _string_or_none(normalized.get("workable_size")) or _string_or_none(
        configuration.get("base_size")
    )
    if leaves_quantity is not None:
        order_update["leaves_quantity"] = leaves_quantity
    return order_update


def coinbase_rest_fill_to_exchange_fill(fill: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = _payload_dict(fill)
    fill_id = (
        _string_or_none(normalized.get("entry_id"))
        or _string_or_none(normalized.get("trade_id"))
        or _stable_fill_id(normalized)
    )
    return {
        "commission": _string_or_none(normalized.get("commission")),
        "fill_id": fill_id,
        "liquidity_indicator": _string_or_none(normalized.get("liquidity_indicator")),
        "order_id": _string_or_none(normalized.get("order_id")),
        "price": _string_or_none(normalized.get("price")),
        "product_id": _string_or_none(normalized.get("product_id")),
        "raw_rest_fill": normalized,
        "side": _string_or_none(normalized.get("side")),
        "size": _string_or_none(normalized.get("size")),
        "size_in_quote": normalized.get("size_in_quote") if isinstance(normalized.get("size_in_quote"), bool) else None,
        "trade_id": _string_or_none(normalized.get("trade_id")),
        "trade_time": _string_or_none(normalized.get("trade_time")),
        "trade_type": _string_or_none(normalized.get("trade_type")),
    }


def coinbase_account_to_exchange_balance(account: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = _payload_dict(account)
    available_balance = _payload_dict(normalized.get("available_balance"))
    hold = _payload_dict(normalized.get("hold"))
    return {
        "account_id": _string_or_none(normalized.get("uuid")),
        "account_type": _string_or_none(normalized.get("type")),
        "available": _string_or_none(available_balance.get("value")),
        "currency": _string_or_none(normalized.get("currency")) or _string_or_none(available_balance.get("currency")),
        "hold": _string_or_none(hold.get("value")),
        "name": _string_or_none(normalized.get("name")),
        "raw_account": normalized,
        "ready": normalized.get("ready") if isinstance(normalized.get("ready"), bool) else None,
        "retail_portfolio_id": _string_or_none(normalized.get("retail_portfolio_id")),
        "venue": ProductVenue.CBE.value,
    }


def coinbase_cfm_position_to_exchange_position(position: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = _payload_dict(position)
    side = _string_or_none(normalized.get("side"))
    number_of_contracts = _string_or_none(normalized.get("number_of_contracts"))
    return {
        "average_entry_price": _string_or_none(normalized.get("avg_entry_price")),
        "current_price": _string_or_none(normalized.get("current_price")),
        "daily_realized_pnl": _string_or_none(normalized.get("daily_realized_pnl")),
        "net_size": _signed_size(number_of_contracts, side),
        "product_id": _string_or_none(normalized.get("product_id")),
        "raw_position": normalized,
        "side": side,
        "unrealized_pnl": _string_or_none(normalized.get("unrealized_pnl")),
        "venue": ProductVenue.FCM.value,
    }


def coinbase_intx_position_to_exchange_position(position: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = _payload_dict(position)
    return {
        "average_entry_price": _money_value(normalized.get("entry_vwap")),
        "current_price": _money_value(normalized.get("mark_price")),
        "leverage": _string_or_none(normalized.get("leverage")),
        "net_size": _string_or_none(normalized.get("net_size")),
        "position_notional": _money_value(normalized.get("position_notional")),
        "product_id": _string_or_none(normalized.get("product_id")) or _string_or_none(normalized.get("symbol")),
        "raw_position": normalized,
        "side": _string_or_none(normalized.get("position_side")),
        "symbol": _string_or_none(normalized.get("symbol")),
        "unrealized_pnl": _money_value(normalized.get("unrealized_pnl")),
        "venue": ProductVenue.INTX.value,
    }


def _order_configuration(payload: Mapping[str, JsonValue]) -> dict[str, Any]:
    order_type = _order_type(payload)
    time_in_force = _time_in_force(payload)
    size = _required_string(payload, "size")

    if order_type == OrderType.MARKET:
        if time_in_force == TimeInForce.FILL_OR_KILL:
            return {"market_market_fok": {"base_size": size}}
        return {"market_market_ioc": {"base_size": size}}

    limit_price = _required_string(payload, "limit_price")
    if time_in_force == TimeInForce.FILL_OR_KILL:
        return {"limit_limit_fok": {"base_size": size, "limit_price": limit_price}}
    if time_in_force == TimeInForce.IMMEDIATE_OR_CANCEL:
        return {"sor_limit_ioc": {"base_size": size, "limit_price": limit_price}}
    return {
        "limit_limit_gtc": {
            "base_size": size,
            "limit_price": limit_price,
            "post_only": _bool(payload.get("post_only"), default=False),
        }
    }


def _post_response_body(value: Mapping[str, Any]) -> dict[str, JsonValue]:
    normalized = normalize_json(value)
    if not isinstance(normalized, dict):
        return {}
    return normalized


def _normalized_body(value: Mapping[str, Any]) -> dict[str, JsonValue]:
    return _post_response_body(value)


def _positions_result(
    *,
    body: Mapping[str, JsonValue],
    normalizer: Callable[[Mapping[str, Any]], dict[str, JsonValue]],
    status_code: int,
    error_message: str,
) -> CoinbasePositionsLookupResult:
    if status_code >= 400:
        failure = _http_failure(status_code, body, fallback=error_message)
        return CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FAILED,
            status_code=status_code,
            raw_response=body,
            error_category=failure.category,
            error_code=failure.error_code,
            error_message=failure.message,
            retryable=failure.retryable,
        )

    positions = body.get("positions")
    if not isinstance(positions, list):
        return CoinbasePositionsLookupResult(
            status=ExchangeLookupStatus.FAILED,
            status_code=status_code,
            raw_response=body,
            error_code="missing_positions",
            error_message="Coinbase positions lookup response did not include positions",
        )

    return CoinbasePositionsLookupResult(
        status=ExchangeLookupStatus.FOUND,
        status_code=status_code,
        positions=tuple(normalizer(position) for position in positions if isinstance(position, dict)),
        raw_response=body,
    )


def _failed_result(
    command: ActionCommand,
    mode: ExecutionMode,
    body: Mapping[str, JsonValue],
    status_code: int,
) -> ExecutionResult:
    failure = _http_failure(status_code, body, fallback="Coinbase HTTP request failed")
    return ExecutionResult(
        action_id=command.action_id,
        action_type=command.action_type,
        status=ExecutionStatus.FAILED,
        mode=mode,
        client_order_id=_client_order_id(command) if command.action_type == ActionType.PLACE_ORDER else None,
        exchange_order_id=_cancel_order_id(command) if command.action_type == ActionType.CANCEL_ORDER else None,
        error_category=failure.category,
        error_code=failure.error_code,
        error_message=failure.message,
        raw_response=body,
        retryable=failure.retryable,
    )


def _cancel_result(body: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    results = body.get("results")
    if isinstance(results, list) and results and isinstance(results[0], dict):
        return results[0]
    return {}


def _error_message(error_response: Mapping[str, JsonValue], *, fallback: str) -> str:
    for key in ("message", "error_details", "new_order_failure_reason", "preview_failure_reason"):
        value = _string_or_none(error_response.get(key))
        if value is not None:
            return value
    return fallback


def _command_payload(command: ActionCommand) -> dict[str, JsonValue]:
    payload = command.to_payload()["payload"]
    if not isinstance(payload, dict):
        raise ValueError("command payload must be a JSON object")
    return payload


def _payload_dict(payload: Any) -> dict[str, JsonValue]:
    normalized = normalize_json(payload)
    if isinstance(normalized, dict):
        return normalized
    return {}


def _auth_headers(
    token_provider: TokenProvider,
    request: RestAuthRequest,
    *,
    include_content_type: bool = False,
) -> dict[str, str]:
    try:
        token = token_provider(request)
    except ExchangeAuthError:
        raise
    except Exception as exc:
        raise ExchangeAuthError("Coinbase token provider failed") from exc
    return _headers(token, include_content_type=include_content_type)


def _headers(token: str, *, include_content_type: bool = False) -> dict[str, str]:
    if not token:
        raise ExchangeAuthError("Coinbase token provider returned an empty token")
    headers = {"Authorization": f"Bearer {token}"}
    if include_content_type:
        headers["Content-Type"] = "application/json"
    return headers


def _transport_get(
    transport: HttpTransport,
    url: str,
    *,
    headers: Mapping[str, str],
    query_params: Mapping[str, Any] | None = None,
) -> HttpResponse:
    try:
        return transport.get(url, headers=headers, query_params=query_params)
    except (ExchangeAuthError, ExchangeRateLimitError, ExchangeTransportError):
        raise
    except Exception as exc:
        raise ExchangeTransportError(
            "Coinbase REST transport GET failed",
            context={"exception_type": exc.__class__.__name__, "url": url},
        ) from exc


def _transport_post(
    transport: HttpTransport,
    url: str,
    *,
    headers: Mapping[str, str],
    json_body: Mapping[str, Any],
) -> HttpResponse:
    try:
        return transport.post(url, headers=headers, json_body=json_body)
    except (ExchangeAuthError, ExchangeRateLimitError, ExchangeTransportError):
        raise
    except Exception as exc:
        raise ExchangeTransportError(
            "Coinbase REST transport POST failed",
            context={"exception_type": exc.__class__.__name__, "url": url},
        ) from exc


def _transport_exception(exc: Exception, *, method: HttpMethod, url: str) -> Exception:
    if isinstance(exc, (ExchangeAuthError, ExchangeRateLimitError, ExchangeTransportError)):
        return exc
    return ExchangeTransportError(
        f"Coinbase REST transport {method.value} failed",
        context={"exception_type": exc.__class__.__name__, "url": url},
    )


def _should_retry_exception(exc: Exception, *, attempt: int, policy: CoinbaseRestRetryPolicy) -> bool:
    return isinstance(exc, BotError) and exc.retryable and attempt < policy.max_attempts


def _should_retry_response(response: HttpResponse, *, attempt: int, policy: CoinbaseRestRetryPolicy) -> bool:
    if response.status_code < 400 or attempt >= policy.max_attempts:
        return False
    return _http_status_retryable(response.status_code)


def _exception_retry_payload(exc: Exception) -> dict[str, JsonValue]:
    if isinstance(exc, BotError):
        return {
            "error_category": exc.category.value,
            "error_code": exc.error_code,
            "exception_type": exc.__class__.__name__,
            "message": str(exc),
        }
    return {
        "error_category": ErrorCategory.EXCHANGE_TRANSPORT.value,
        "error_code": ErrorCode.EXCHANGE_TRANSPORT_FAILED.value,
        "exception_type": exc.__class__.__name__,
        "message": str(exc),
    }


def _http_failure(
    status_code: int,
    body: Mapping[str, JsonValue],
    *,
    fallback: str,
) -> CoinbaseHttpFailure:
    category = _http_error_category(status_code)
    message = _string_or_none(body.get("message")) or fallback
    return CoinbaseHttpFailure(
        status_code=status_code,
        category=category,
        error_code=f"http_{status_code}",
        message=message,
        retryable=_http_status_retryable(status_code),
    )


def _http_error_category(status_code: int) -> ErrorCategory:
    if status_code in {401, 403}:
        return ErrorCategory.EXCHANGE_AUTH
    if status_code == 429:
        return ErrorCategory.EXCHANGE_RATE_LIMIT
    return ErrorCategory.EXCHANGE_TRANSPORT


def _http_status_retryable(status_code: int) -> bool:
    return status_code == 429 or status_code == 408 or 500 <= status_code <= 599


def _url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _active_order_configuration(order_configuration: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    for value in order_configuration.values():
        if isinstance(value, dict) and value:
            return value
    return {}


def _rest_order_type(
    order: Mapping[str, JsonValue],
    order_configuration: Mapping[str, JsonValue],
) -> str | None:
    direct_order_type = _string_or_none(order.get("order_type"))
    if direct_order_type in {"LIMIT", "MARKET"}:
        return direct_order_type

    for key, value in order_configuration.items():
        if not isinstance(value, dict) or not value:
            continue
        if key.startswith("market_"):
            return "MARKET"
        if "limit" in key:
            return "LIMIT"
    return None


def _stable_fill_id(fill: Mapping[str, JsonValue]) -> str:
    parts = [
        _string_or_none(fill.get("order_id")) or "unknown-order",
        _string_or_none(fill.get("trade_time")) or "unknown-time",
        _string_or_none(fill.get("price")) or "unknown-price",
        _string_or_none(fill.get("size")) or "unknown-size",
    ]
    return ":".join(parts)


def _money_value(value: Any) -> str | None:
    payload = _payload_dict(value)
    return _string_or_none(payload.get("value"))


def _signed_size(size: str | None, side: str | None) -> str | None:
    if size is None:
        return None
    if isinstance(side, str) and side.upper() in {"SHORT", "SELL"} and not size.startswith("-"):
        return f"-{size}"
    return size


def _client_order_id(command: ActionCommand) -> str:
    return command.idempotency_key or command.action_id


def _cancel_order_id(command: ActionCommand) -> str | None:
    payload = _command_payload(command)
    return _string_or_none(payload.get("exchange_order_id"))


def _side(payload: Mapping[str, JsonValue]) -> OrderSide:
    try:
        return OrderSide(payload.get("side"))
    except (TypeError, ValueError) as exc:
        raise ValueError("side is required") from exc


def _order_type(payload: Mapping[str, JsonValue]) -> OrderType:
    try:
        return OrderType(payload.get("order_type"))
    except (TypeError, ValueError) as exc:
        raise ValueError("order_type is required") from exc


def _margin_type_or_none(value: JsonValue) -> MarginType | None:
    try:
        return MarginType(value)
    except (TypeError, ValueError):
        return None


def _time_in_force(payload: Mapping[str, JsonValue]) -> TimeInForce:
    try:
        return TimeInForce(payload.get("time_in_force"))
    except (TypeError, ValueError) as exc:
        raise ValueError("time_in_force is required") from exc


def _bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError("post_only must be a bool")
    return value


def _required_string(payload: Mapping[str, JsonValue], key: str) -> str:
    value = _string_or_none(payload.get(key))
    if value is None:
        raise ValueError(f"{key} is required")
    return value


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _http_error_response(exc: urllib.error.HTTPError) -> HttpResponse:
    raw_body = exc.read().decode("utf-8")
    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        body = {"raw_body": raw_body}
    return HttpResponse(status_code=exc.code, body=body)
