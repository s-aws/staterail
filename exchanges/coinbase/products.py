from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.enums import ErrorCategory, HttpMethod
from core.errors import ExchangeAuthError, ExchangeRateLimitError, ExchangeTransportError
from exchanges.coinbase.advanced_trade_rest import (
    CoinbaseRestConfig,
    HttpResponse,
    HttpTransport,
    UrlLibHttpTransport,
    _auth_headers,
    _http_failure,
    _transport_get,
)
from exchanges.coinbase.auth import TokenProvider, rest_auth_request
from products.catalog import ProductCatalog, ProductMetadata


class CoinbaseProductCatalogClient:
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

    def get_product(self, product_id: str, *, get_tradability_status: bool = True) -> ProductMetadata:
        response = self._get(
            f"/products/{product_id}",
            query_params={"get_tradability_status": str(get_tradability_status).lower()},
        )
        if response.status_code >= 400:
            _raise_http_failure("Coinbase get product failed", response)
        return ProductMetadata.from_coinbase_payload(response.body)

    def list_products(
        self,
        *,
        product_ids: tuple[str, ...] = (),
        product_type: str | None = None,
        contract_expiry_type: str | None = None,
        get_tradability_status: bool = True,
    ) -> ProductCatalog:
        query_params: dict[str, Any] = {
            "get_tradability_status": str(get_tradability_status).lower(),
        }
        if product_ids:
            query_params["product_ids"] = list(product_ids)
        if product_type is not None:
            query_params["product_type"] = product_type
        if contract_expiry_type is not None:
            query_params["contract_expiry_type"] = contract_expiry_type

        response = self._get("/products", query_params=query_params)
        if response.status_code >= 400:
            _raise_http_failure("Coinbase list products failed", response)
        products = response.body.get("products")
        if not isinstance(products, list):
            raise ExchangeTransportError(
                "Coinbase list products response did not include products",
                retryable=False,
            )
        return ProductCatalog.from_coinbase_payloads(
            product for product in products if isinstance(product, Mapping)
        )

    def _get(self, path: str, *, query_params: Mapping[str, Any]) -> HttpResponse:
        url = f"{self._config.base_url.rstrip('/')}/{path.lstrip('/')}"
        return _transport_get(
            self._transport,
            url,
            headers=_auth_headers(self._token_provider, rest_auth_request(HttpMethod.GET, url)),
            query_params=query_params,
        )


def _raise_http_failure(operation: str, response: HttpResponse) -> None:
    failure = _http_failure(response.status_code, response.body, fallback=operation)
    context = {
        "error_category": failure.category.value,
        "status_code": failure.status_code,
    }
    if failure.category == ErrorCategory.EXCHANGE_AUTH:
        raise ExchangeAuthError(failure.message, context=context, error_code=failure.error_code)
    if failure.category == ErrorCategory.EXCHANGE_RATE_LIMIT:
        raise ExchangeRateLimitError(failure.message, context=context, error_code=failure.error_code)
    raise ExchangeTransportError(
        failure.message,
        context=context,
        error_code=failure.error_code,
        retryable=failure.retryable,
    )
