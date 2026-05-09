from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from core.enums import OrderLineageRelation, OrderPlacementKind, OrderPlacementStatus, OrderSide
from core.json_tools import JsonValue, normalize_json


ORDER_LINEAGE_SCHEMA_VERSION = 1
ORDER_PLACEMENT_SCHEMA_VERSION = 1
MANUAL_ASSOCIATION_APPROVAL_METADATA_KEY = "manual_association_approval"


@dataclass(frozen=True)
class ManualAssociationApproval:
    approved_by: str
    reason: str
    approved_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty("approved_by", self.approved_by)
        _require_non_empty("reason", self.reason)
        if not isinstance(self.approved_at, datetime):
            raise TypeError("approved_at must be datetime")

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "approved_at": self.approved_at,
            "approved_by": self.approved_by,
            "reason": self.reason,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Manual association approval payload must normalize to a JSON object")
        return normalized


def manual_association_metadata(approval: ManualAssociationApproval) -> dict[str, JsonValue]:
    if not isinstance(approval, ManualAssociationApproval):
        raise TypeError("approval must be ManualAssociationApproval")
    return {MANUAL_ASSOCIATION_APPROVAL_METADATA_KEY: approval.to_payload()}


@dataclass(frozen=True)
class LogicalOrderRecord:
    logical_order_id: str
    product_id: str
    side: OrderSide
    size: str
    lineage_relation: OrderLineageRelation = OrderLineageRelation.ROOT
    root_order_id: str | None = None
    parent_order_id: str | None = None
    source_order_ids: tuple[str, ...] = ()
    limit_price: str | None = None
    created_by_action_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("logical_order_id", self.logical_order_id)
        _require_non_empty("product_id", self.product_id)
        _require_positive_decimal("size", self.size)
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be an OrderSide")
        if not isinstance(self.lineage_relation, OrderLineageRelation):
            raise TypeError("lineage_relation must be an OrderLineageRelation")
        _validate_optional_non_empty("root_order_id", self.root_order_id)
        _validate_optional_non_empty("parent_order_id", self.parent_order_id)
        _validate_optional_non_empty("limit_price", self.limit_price)
        _validate_optional_non_empty("created_by_action_id", self.created_by_action_id)
        _validate_source_order_ids(self.source_order_ids)
        if self.lineage_relation == OrderLineageRelation.ROOT:
            if self.root_order_id not in {None, self.logical_order_id}:
                raise ValueError("root logical orders must use their own logical_order_id as root_order_id")
            if self.parent_order_id is not None:
                raise ValueError("root logical orders must not have parent_order_id")
            if self.source_order_ids:
                raise ValueError("root logical orders must not have source_order_ids")
        elif self.lineage_relation == OrderLineageRelation.EXTERNAL_IMPORT:
            if self.parent_order_id is not None:
                raise ValueError("external imports must not have parent_order_id")
        elif self.lineage_relation in {OrderLineageRelation.FOLLOWUP_AFTER_FILL, OrderLineageRelation.SPLIT_CHILD}:
            _require_non_empty("root_order_id", self.root_order_id)
            _require_non_empty("parent_order_id", self.parent_order_id)
            if not self.source_order_ids:
                raise ValueError(f"{self.lineage_relation.value} requires source_order_ids")
        elif self.lineage_relation == OrderLineageRelation.CONSOLIDATION:
            if len(self.source_order_ids) < 2:
                raise ValueError("consolidation requires at least two source_order_ids")
        elif self.lineage_relation == OrderLineageRelation.MANUAL_ASSOCIATION:
            if not self.source_order_ids:
                raise ValueError("manual_association requires source_order_ids")
        normalized_metadata = normalize_json(self.metadata)
        if not isinstance(normalized_metadata, dict):
            raise TypeError("metadata must normalize to a JSON object")
        if self.lineage_relation == OrderLineageRelation.MANUAL_ASSOCIATION:
            _validate_manual_association_approval(normalized_metadata)
        object.__setattr__(self, "metadata", normalized_metadata)

    def to_payload(self) -> dict[str, JsonValue]:
        root_order_id = self.root_order_id or self.logical_order_id
        payload = {
            "created_by_action_id": self.created_by_action_id,
            "lineage_relation": self.lineage_relation.value,
            "limit_price": self.limit_price,
            "logical_order_id": self.logical_order_id,
            "metadata": self.metadata,
            "parent_order_id": self.parent_order_id,
            "product_id": self.product_id,
            "root_order_id": root_order_id,
            "schema_version": ORDER_LINEAGE_SCHEMA_VERSION,
            "side": self.side.value,
            "size": self.size,
            "source_order_ids": list(self.source_order_ids),
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Logical order payload must normalize to a JSON object")
        return normalized


@dataclass(frozen=True)
class OrderPlacementRecord:
    placement_id: str
    logical_order_id: str
    placement_kind: OrderPlacementKind
    placement_status: OrderPlacementStatus
    product_id: str
    side: OrderSide
    size: str
    limit_price: str | None = None
    action_id: str | None = None
    venue_client_order_id: str | None = None
    exchange_order_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("placement_id", self.placement_id)
        _require_non_empty("logical_order_id", self.logical_order_id)
        _require_non_empty("product_id", self.product_id)
        _require_positive_decimal("size", self.size)
        if not isinstance(self.placement_kind, OrderPlacementKind):
            raise TypeError("placement_kind must be an OrderPlacementKind")
        if not isinstance(self.placement_status, OrderPlacementStatus):
            raise TypeError("placement_status must be an OrderPlacementStatus")
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be an OrderSide")
        _validate_optional_non_empty("limit_price", self.limit_price)
        _validate_optional_non_empty("action_id", self.action_id)
        _validate_optional_non_empty("venue_client_order_id", self.venue_client_order_id)
        _validate_optional_non_empty("exchange_order_id", self.exchange_order_id)
        if self.placement_status != OrderPlacementStatus.STAGED and not (
            self.venue_client_order_id or self.exchange_order_id
        ):
            raise ValueError("non-staged placements require venue_client_order_id or exchange_order_id")
        normalized_metadata = normalize_json(self.metadata)
        if not isinstance(normalized_metadata, dict):
            raise TypeError("metadata must normalize to a JSON object")
        object.__setattr__(self, "metadata", normalized_metadata)

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "action_id": self.action_id,
            "exchange_order_id": self.exchange_order_id,
            "limit_price": self.limit_price,
            "logical_order_id": self.logical_order_id,
            "metadata": self.metadata,
            "placement_id": self.placement_id,
            "placement_kind": self.placement_kind.value,
            "placement_status": self.placement_status.value,
            "product_id": self.product_id,
            "schema_version": ORDER_PLACEMENT_SCHEMA_VERSION,
            "side": self.side.value,
            "size": self.size,
            "venue_client_order_id": self.venue_client_order_id,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Order placement payload must normalize to a JSON object")
        return normalized


def _require_non_empty(field_name: str, value: str | None) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} is required")


def _validate_optional_non_empty(field_name: str, value: str | None) -> None:
    if value is not None and (not isinstance(value, str) or not value):
        raise ValueError(f"{field_name} must be non-empty when provided")


def _validate_source_order_ids(source_order_ids: tuple[str, ...]) -> None:
    if not isinstance(source_order_ids, tuple):
        raise TypeError("source_order_ids must be a tuple")
    if any(not isinstance(source_order_id, str) or not source_order_id for source_order_id in source_order_ids):
        raise ValueError("source_order_ids must contain only non-empty strings")
    if len(set(source_order_ids)) != len(source_order_ids):
        raise ValueError("source_order_ids must be unique")


def _validate_manual_association_approval(metadata: Mapping[str, JsonValue]) -> None:
    approval = metadata.get(MANUAL_ASSOCIATION_APPROVAL_METADATA_KEY)
    if not isinstance(approval, Mapping):
        raise ValueError("manual_association requires operator approval metadata")
    for field_name in ("approved_at", "approved_by", "reason"):
        value = approval.get(field_name)
        _require_non_empty(field_name, value if isinstance(value, str) else None)


def _require_positive_decimal(field_name: str, value: str) -> None:
    _require_non_empty(field_name, value)
    try:
        decimal = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal") from exc
    if not decimal.is_finite() or decimal <= 0:
        raise ValueError(f"{field_name} must be positive")
