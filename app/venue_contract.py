from __future__ import annotations

from core.enums import (
    VenueCapabilityRequirement,
    VenueContractRequirementSet,
)
from core.json_tools import JsonValue, normalize_json
from products.capabilities import (
    CFM_LIVE_ORDER_ROUTING_REQUIREMENTS,
    LIVE_ORDER_ROUTING_REQUIREMENTS,
    venue_contract_report,
)


def venue_contract_report_payload(
    venue: str,
    *,
    requirement_set: VenueContractRequirementSet = VenueContractRequirementSet.LIVE_ORDER_ROUTING,
) -> dict[str, JsonValue]:
    if not isinstance(requirement_set, VenueContractRequirementSet):
        raise TypeError("requirement_set must be a VenueContractRequirementSet")
    report = venue_contract_report(
        venue,
        requirements=requirements_for_venue_contract_set(requirement_set),
    )
    payload = {
        **report.to_payload(),
        "read_only": True,
        "requirement_set": requirement_set.value,
        "schema_version": 1,
        "writes_ledger": False,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("venue contract report payload must normalize to an object")
    return normalized


def requirements_for_venue_contract_set(
    requirement_set: VenueContractRequirementSet,
) -> tuple[VenueCapabilityRequirement, ...]:
    if requirement_set == VenueContractRequirementSet.CFM_LIVE_ORDER_ROUTING:
        return CFM_LIVE_ORDER_ROUTING_REQUIREMENTS
    if requirement_set == VenueContractRequirementSet.LIVE_ORDER_ROUTING:
        return LIVE_ORDER_ROUTING_REQUIREMENTS
    if requirement_set == VenueContractRequirementSet.PRODUCT_METADATA_LOOKUP:
        return (VenueCapabilityRequirement.PRODUCT_METADATA_LOOKUP,)
    raise ValueError(f"unsupported venue contract requirement set: {requirement_set.value}")
