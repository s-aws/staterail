from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from core.enums import ExchangeOrderStatus


@dataclass(frozen=True)
class OrderUpdateContractResult:
    missing_fields: tuple[str, ...] = ()
    invalid_fields: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.missing_fields and not self.invalid_fields


def validate_exchange_order_update(payload: Mapping[str, object]) -> OrderUpdateContractResult:
    missing: list[str] = []
    invalid: list[str] = []

    if _string_or_none(payload.get("order_id")) is None and _string_or_none(payload.get("client_order_id")) is None:
        missing.append("order_id_or_client_order_id")
    if _string_or_none(payload.get("product_id")) is None:
        missing.append("product_id")

    status = _string_or_none(payload.get("status"))
    if status is None:
        missing.append("status")
    elif _exchange_order_status_or_none(status) is None:
        invalid.append("status")

    return OrderUpdateContractResult(
        invalid_fields=tuple(invalid),
        missing_fields=tuple(missing),
    )


def _exchange_order_status_or_none(value: object) -> ExchangeOrderStatus | None:
    try:
        status = ExchangeOrderStatus(value)
    except (TypeError, ValueError):
        return None
    if status == ExchangeOrderStatus.UNKNOWN:
        return None
    return status


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
