from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from core.errors import ConfigError
from exchanges.coinbase.auth import (
    CoinbaseJwtCredentials,
    CoinbaseJwtProvider,
    RestJwtBuilder,
    TokenProvider,
    WsJwtBuilder,
)
from exchanges.coinbase.advanced_trade_ws import JwtFactory


COINBASE_API_KEY_NAME_ENV = "STATERAIL_COINBASE_API_KEY_NAME"
COINBASE_API_PRIVATE_KEY_ENV = "STATERAIL_COINBASE_API_PRIVATE_KEY"
COINBASE_API_PRIVATE_KEY_FILE_ENV = "STATERAIL_COINBASE_API_PRIVATE_KEY_FILE"
LEGACY_COINBASE_API_KEY_NAME_ENV = "COINBASE_BOT_API_KEY_NAME"
LEGACY_COINBASE_API_PRIVATE_KEY_ENV = "COINBASE_BOT_API_PRIVATE_KEY"
LEGACY_COINBASE_API_PRIVATE_KEY_FILE_ENV = "COINBASE_BOT_API_PRIVATE_KEY_FILE"
COINBASE_SDK_API_KEY_ENV = "COINBASE_API_KEY"
COINBASE_SDK_API_SECRET_ENV = "COINBASE_API_SECRET"

_API_KEY_NAME_ENV_NAMES = (
    COINBASE_SDK_API_KEY_ENV,
    COINBASE_API_KEY_NAME_ENV,
)
_API_PRIVATE_KEY_ENV_NAMES = (
    COINBASE_SDK_API_SECRET_ENV,
    COINBASE_API_PRIVATE_KEY_ENV,
)
_ALL_COINBASE_CREDENTIAL_ENV_NAMES = (
    *_API_KEY_NAME_ENV_NAMES,
    *_API_PRIVATE_KEY_ENV_NAMES,
    COINBASE_API_PRIVATE_KEY_FILE_ENV,
    LEGACY_COINBASE_API_KEY_NAME_ENV,
    LEGACY_COINBASE_API_PRIVATE_KEY_ENV,
    LEGACY_COINBASE_API_PRIVATE_KEY_FILE_ENV,
)
_LEGACY_COINBASE_CREDENTIAL_ENV_NAMES = (
    LEGACY_COINBASE_API_KEY_NAME_ENV,
    LEGACY_COINBASE_API_PRIVATE_KEY_ENV,
    LEGACY_COINBASE_API_PRIVATE_KEY_FILE_ENV,
)


@dataclass(frozen=True)
class CoinbaseRuntimeCredentialProviders:
    jwt_factory: JwtFactory | None = None
    token_provider: TokenProvider | None = None

    @property
    def jwt_factory_configured(self) -> bool:
        return self.jwt_factory is not None

    @property
    def token_provider_configured(self) -> bool:
        return self.token_provider is not None


def has_coinbase_credentials_env(env: Mapping[str, str] | None = None) -> bool:
    environment = os.environ if env is None else env
    return any(key in environment for key in _ALL_COINBASE_CREDENTIAL_ENV_NAMES)


def load_coinbase_runtime_credentials_from_env(
    env: Mapping[str, str] | None = None,
    *,
    rest_jwt_builder: RestJwtBuilder | None = None,
    ws_jwt_builder: WsJwtBuilder | None = None,
) -> CoinbaseRuntimeCredentialProviders:
    credentials = load_coinbase_jwt_credentials_from_env(env)
    if credentials is None:
        return CoinbaseRuntimeCredentialProviders()
    provider = CoinbaseJwtProvider(
        credentials,
        rest_jwt_builder=rest_jwt_builder,
        ws_jwt_builder=ws_jwt_builder,
    )
    return CoinbaseRuntimeCredentialProviders(
        jwt_factory=provider.websocket_jwt,
        token_provider=provider.rest_token,
    )


def load_coinbase_jwt_credentials_from_env(
    env: Mapping[str, str] | None = None,
) -> CoinbaseJwtCredentials | None:
    environment = os.environ if env is None else env
    _reject_legacy_coinbase_credentials_env(environment)
    if not has_coinbase_credentials_env(environment):
        return None

    api_key_name = _single_env_value(
        environment,
        _API_KEY_NAME_ENV_NAMES,
        "API key",
    )
    if not api_key_name:
        raise ConfigError(
            f"{COINBASE_SDK_API_KEY_ENV} or {COINBASE_API_KEY_NAME_ENV} is required "
            "when Coinbase credentials are configured"
        )

    inline_private_key = _single_env_value(
        environment,
        _API_PRIVATE_KEY_ENV_NAMES,
        "private key",
        normalize=_normalize_inline_private_key,
    )
    private_key_file = environment.get(COINBASE_API_PRIVATE_KEY_FILE_ENV)
    if inline_private_key and private_key_file:
        raise ConfigError(
            f"{COINBASE_SDK_API_SECRET_ENV}/{COINBASE_API_PRIVATE_KEY_ENV} and "
            f"{COINBASE_API_PRIVATE_KEY_FILE_ENV} cannot both be set"
        )
    if inline_private_key:
        private_key = inline_private_key
    elif private_key_file:
        private_key = _read_private_key_file(private_key_file)
    else:
        raise ConfigError(
            f"{COINBASE_SDK_API_SECRET_ENV}, {COINBASE_API_PRIVATE_KEY_ENV}, or "
            f"{COINBASE_API_PRIVATE_KEY_FILE_ENV} is required "
            "when Coinbase credentials are configured"
        )

    return CoinbaseJwtCredentials(
        api_key_name=api_key_name,
        private_key=private_key,
    )


def _read_private_key_file(path: str) -> str:
    key_path = Path(path)
    try:
        return key_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            f"Could not read Coinbase private key file: {key_path}",
            context={"credential_path": str(key_path)},
        ) from exc


def _single_env_value(
    environment: Mapping[str, str],
    names: tuple[str, ...],
    description: str,
    *,
    normalize=lambda value: value,
) -> str | None:
    configured = tuple(
        (name, normalize(value))
        for name in names
        if (value := environment.get(name))
    )
    if not configured:
        return None

    values = {value for _, value in configured}
    if len(values) > 1:
        raise ConfigError(
            f"Conflicting Coinbase {description} environment variables are configured",
            context={"env_vars": tuple(name for name, _ in configured)},
        )

    return configured[0][1]


def _normalize_inline_private_key(value: str) -> str:
    return value.replace("\\n", "\n")


def _reject_legacy_coinbase_credentials_env(environment: Mapping[str, str]) -> None:
    legacy = tuple(name for name in _LEGACY_COINBASE_CREDENTIAL_ENV_NAMES if name in environment)
    if legacy:
        raise ConfigError(
            "COINBASE_BOT_* credential environment variables were renamed to "
            "STATERAIL_COINBASE_*: "
            f"{', '.join(legacy)}"
        )
