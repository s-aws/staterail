from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from core.enums import ProductVenue


@dataclass(frozen=True)
class ExchangeStateSnapshotContractResult:
    missing_fields: tuple[str, ...] = ()
    invalid_fields: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.missing_fields and not self.invalid_fields


def validate_exchange_balance_snapshot(payload: Mapping[str, object]) -> ExchangeStateSnapshotContractResult:
    missing: list[str] = []
    invalid: list[str] = []

    if _string_or_none(payload.get("account_id")) is None:
        missing.append("account_id")
    if _string_or_none(payload.get("currency")) is None:
        missing.append("currency")
    _validate_required_venue(payload.get("venue"), missing=missing, invalid=invalid)

    return ExchangeStateSnapshotContractResult(
        invalid_fields=tuple(invalid),
        missing_fields=tuple(missing),
    )


def validate_exchange_position_snapshot(payload: Mapping[str, object]) -> ExchangeStateSnapshotContractResult:
    missing: list[str] = []
    invalid: list[str] = []

    if _string_or_none(payload.get("product_id")) is None:
        missing.append("product_id")
    _validate_required_venue(payload.get("venue"), missing=missing, invalid=invalid)

    net_size = _string_or_none(payload.get("net_size"))
    if net_size is None:
        missing.append("net_size")
    elif _decimal_or_none(net_size) is None:
        invalid.append("net_size")

    return ExchangeStateSnapshotContractResult(
        invalid_fields=tuple(invalid),
        missing_fields=tuple(missing),
    )


def _validate_required_venue(value: object, *, missing: list[str], invalid: list[str]) -> None:
    venue = _string_or_none(value)
    if venue is None:
        missing.append("venue")
        return
    try:
        parsed = ProductVenue(venue)
    except ValueError:
        invalid.append("venue")
        return
    if parsed == ProductVenue.UNKNOWN:
        invalid.append("venue")


def _decimal_or_none(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
