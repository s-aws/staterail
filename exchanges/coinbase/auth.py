from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlparse

from core.enums import HttpMethod
from core.errors import ExchangeAuthError


RestJwtBuilder = Callable[[str, str, str], str]
TokenProvider = Callable[["RestAuthRequest"], str]
WsJwtBuilder = Callable[[str, str], str]


@dataclass(frozen=True)
class RestAuthRequest:
    method: HttpMethod
    url: str
    path: str
    jwt_uri: str

    def __post_init__(self) -> None:
        if not isinstance(self.method, HttpMethod):
            raise TypeError("method must be an HttpMethod")
        if not self.url:
            raise ValueError("url is required")
        if not self.path.startswith("/"):
            raise ValueError("path must start with /")
        if not self.jwt_uri:
            raise ValueError("jwt_uri is required")


@dataclass(frozen=True)
class CoinbaseJwtCredentials:
    api_key_name: str
    private_key: str

    def __post_init__(self) -> None:
        if not self.api_key_name:
            raise ValueError("api_key_name is required")
        if not self.private_key:
            raise ValueError("private_key is required")


class CoinbaseJwtProvider:
    def __init__(
        self,
        credentials: CoinbaseJwtCredentials,
        *,
        rest_jwt_builder: RestJwtBuilder | None = None,
        ws_jwt_builder: WsJwtBuilder | None = None,
    ) -> None:
        self._credentials = credentials
        self._rest_jwt_builder = rest_jwt_builder
        self._ws_jwt_builder = ws_jwt_builder

    def rest_token(self, request: RestAuthRequest) -> str:
        if not isinstance(request, RestAuthRequest):
            raise TypeError("request must be a RestAuthRequest")
        try:
            return self._rest_builder()(
                request.jwt_uri,
                self._credentials.api_key_name,
                self._credentials.private_key,
            )
        except ExchangeAuthError:
            raise
        except Exception as exc:
            raise ExchangeAuthError(
                "Coinbase REST JWT builder failed",
                context={"jwt_uri": request.jwt_uri},
            ) from exc

    def websocket_jwt(self, _message: dict[str, object] | None = None) -> str:
        try:
            return self._ws_builder()(
                self._credentials.api_key_name,
                self._credentials.private_key,
            )
        except ExchangeAuthError:
            raise
        except Exception as exc:
            raise ExchangeAuthError("Coinbase websocket JWT builder failed") from exc

    def _rest_builder(self) -> RestJwtBuilder:
        if self._rest_jwt_builder is not None:
            return self._rest_jwt_builder
        return _coinbase_sdk_rest_jwt_builder()

    def _ws_builder(self) -> WsJwtBuilder:
        if self._ws_jwt_builder is not None:
            return self._ws_jwt_builder
        return _coinbase_sdk_ws_jwt_builder()


def rest_auth_request(method: HttpMethod, url: str) -> RestAuthRequest:
    parsed = urlparse(url)
    if not parsed.netloc:
        raise ValueError("url must include a network location")
    path = parsed.path or "/"
    return RestAuthRequest(
        method=method,
        path=path,
        url=url,
        jwt_uri=f"{method.value} {parsed.netloc}{path}",
    )


def static_token_provider(token: str) -> TokenProvider:
    def provide(_request: RestAuthRequest) -> str:
        return token

    return provide


def _coinbase_sdk_rest_jwt_builder() -> RestJwtBuilder:
    try:
        from coinbase import jwt_generator
    except ImportError as exc:
        raise ExchangeAuthError(
            "Install 'coinbase-advanced-py' to use Coinbase JWT credentials",
            retryable=False,
        ) from exc
    return jwt_generator.build_rest_jwt


def _coinbase_sdk_ws_jwt_builder() -> WsJwtBuilder:
    try:
        from coinbase import jwt_generator
    except ImportError as exc:
        raise ExchangeAuthError(
            "Install 'coinbase-advanced-py' to use Coinbase websocket JWT credentials",
            retryable=False,
        ) from exc
    return jwt_generator.build_ws_jwt
