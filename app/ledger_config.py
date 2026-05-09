from __future__ import annotations

from dataclasses import dataclass

from core.json_tools import JsonValue
from projections.state import SourceOfTruthProjection


@dataclass(frozen=True)
class LatestLedgerApplicationConfig:
    fingerprint: str | None
    fingerprint_algorithm: str | None
    schema_version: int | None
    startup_sequence: int | None


def latest_ledger_application_config(projection: SourceOfTruthProjection) -> LatestLedgerApplicationConfig:
    if not projection.system_starts:
        return LatestLedgerApplicationConfig(
            fingerprint=None,
            fingerprint_algorithm=None,
            schema_version=None,
            startup_sequence=None,
        )

    latest_start = projection.system_starts[-1]
    application_config = _latest_application_config_payload(projection)
    return LatestLedgerApplicationConfig(
        fingerprint=_string_or_none(application_config.get("fingerprint")),
        fingerprint_algorithm=_string_or_none(application_config.get("fingerprint_algorithm")),
        schema_version=_int_or_none(application_config.get("schema_version")),
        startup_sequence=latest_start.sequence,
    )


def _latest_application_config_payload(projection: SourceOfTruthProjection) -> dict[str, JsonValue]:
    if not projection.system_starts:
        return {}
    startup_metadata = projection.system_starts[-1].startup_metadata
    application_config = startup_metadata.get("application_config")
    if isinstance(application_config, dict):
        return application_config
    return {}


def _string_or_none(value: JsonValue) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _int_or_none(value: JsonValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None
