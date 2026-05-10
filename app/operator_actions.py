from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from actions.gateway import ActionCommand, ActionReceipt, PlaceOrderIntent
from app.bootstrap import CoinbaseApplication, CoinbaseApplicationConfig
from app.config_fingerprint import (
    CONFIG_FINGERPRINT_ALGORITHM,
    application_config_fingerprint,
)
from app.ledger_view import load_verified_ledger_view
from audit.ledger import AuditLedger, AuditRecord
from core.engine import AuditCore
from core.enums import (
    ActionStatus,
    ActionType,
    EventType,
    ExchangeLookupStatus,
    MarginType,
    OperatorActionSkipReason,
    OperatorCanaryEvidenceIssue,
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
    ReadinessStatus,
    TimeInForce,
)
from core.json_tools import JsonValue, canonical_json, normalize_json
from projections.state import ActionSnapshot, OrderSnapshot, SourceOfTruthProjection


OPERATOR_ACTION_SCHEMA_VERSION = 1
OPERATOR_CANARY_EVIDENCE_RESULT_SCHEMA_VERSION = 1
OPERATOR_ORDER_LOOKUP_SCHEMA_VERSION = 1
OPERATOR_REQUESTED_BY_PREFIX = "operator:"
OPERATOR_CANCEL_REQUESTED_BY_PREFIX = OPERATOR_REQUESTED_BY_PREFIX
OPERATOR_CANCEL_ALL_DEFAULT_REASON = "operator cancel all tracked open orders"


def operator_open_orders_payload(
    ledger_path: str | Path,
    *,
    product_id: str | None = None,
) -> dict[str, JsonValue]:
    view = load_verified_ledger_view(ledger_path)
    open_orders = tuple(
        sorted(
            (
                order
                for order in view.projection.open_orders
                if product_id is None or order.product_id == product_id
            ),
            key=lambda order: (
                order.requested_sequence if order.requested_sequence is not None else 0,
                order.action_id,
            ),
        )
    )
    payload = {
        "ledger": {
            "last_hash": view.state.last_hash,
            "ledger_path": view.ledger_path.as_posix(),
            "next_sequence": view.state.next_sequence,
            "record_count": len(view.records),
            "verified": True,
        },
        "open_order_count": len(open_orders),
        "open_orders": [_open_order_payload(order) for order in open_orders],
        "product_id": product_id,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
        "status": ReadinessStatus.OK.value,
        "websocket_started": False,
        "writes_ledger": False,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator open-orders payload must normalize to an object")
    return normalized


def operator_lookup_order_payload(
    config: CoinbaseApplicationConfig,
    application: CoinbaseApplication,
    *,
    exchange_order_id: str,
    operator_id: str,
    reason: str,
) -> dict[str, JsonValue]:
    if not exchange_order_id:
        raise ValueError("exchange_order_id is required")
    if not operator_id:
        raise ValueError("operator_id is required")
    if not reason:
        raise ValueError("reason is required")

    lookup_client = application.assembly.order_lookup_client
    if lookup_client is None:
        payload = {
            "error_code": "order_lookup_client_missing",
            "error_message": "configured runtime does not provide an order lookup client",
            "exchange_order_id": exchange_order_id,
            "ledger_path": Path(config.ledger_path).as_posix(),
            "lookup_status": ExchangeLookupStatus.FAILED.value,
            "operator_id": operator_id,
            "order_endpoint_called": False,
            "order_update": None,
            "order_update_sequence": None,
            "reason": reason,
            "runtime_tasks_started": False,
            "schema_version": OPERATOR_ORDER_LOOKUP_SCHEMA_VERSION,
            "status": ReadinessStatus.ATTENTION_REQUIRED.value,
            "status_code": None,
            "websocket_started": False,
            "writes_ledger": False,
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("operator order lookup payload must normalize to an object")
        return normalized

    lookup = lookup_client.get_order(exchange_order_id)
    order_update = _payload_dict(lookup.order_update)
    raw_response = _payload_dict(lookup.raw_response)
    order_update_sequence: int | None = None
    if lookup.status == ExchangeLookupStatus.FOUND and order_update:
        record = application.core.emit(
            EventType.EXCHANGE_ORDER_UPDATE,
            {
                "exchange_order_id": exchange_order_id,
                "lookup_status": lookup.status.value,
                "operator_id": operator_id,
                "order_update": order_update,
                "raw_response": raw_response,
                "reason": reason,
                "schema_version": OPERATOR_ORDER_LOOKUP_SCHEMA_VERSION,
                "status_code": lookup.status_code,
            },
        )
        order_update_sequence = record.sequence

    payload = {
        "error_category": lookup.error_category.value if lookup.error_category is not None else None,
        "error_code": lookup.error_code,
        "error_message": lookup.error_message,
        "exchange_order_id": exchange_order_id,
        "ledger_path": Path(config.ledger_path).as_posix(),
        "lookup_status": lookup.status.value,
        "operator_id": operator_id,
        "order_endpoint_called": False,
        "order_update": order_update if order_update else None,
        "order_update_sequence": order_update_sequence,
        "raw_response": raw_response,
        "reason": reason,
        "retryable": lookup.retryable,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_ORDER_LOOKUP_SCHEMA_VERSION,
        "status": (
            ReadinessStatus.OK.value
            if lookup.status == ExchangeLookupStatus.FOUND
            else ReadinessStatus.ATTENTION_REQUIRED.value
        ),
        "status_code": lookup.status_code,
        "websocket_started": False,
        "writes_ledger": order_update_sequence is not None,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator order lookup payload must normalize to an object")
    return normalized


def operator_canary_evidence_payload(
    ledger_path: str | Path,
    *,
    action_id: str | None = None,
    exchange_order_id: str | None = None,
    product_id: str | None = None,
) -> dict[str, JsonValue]:
    view = load_verified_ledger_view(ledger_path)
    projection = view.projection
    order, selection_issues = _select_canary_order(
        projection,
        action_id=action_id,
        exchange_order_id=exchange_order_id,
        product_id=product_id,
    )
    issues = list(selection_issues)
    if order is not None:
        issues.extend(_canary_lifecycle_issues(projection, order, product_id=product_id))
    issues = _unique_canary_issues(issues)

    cancel_actions = tuple(
        projection.actions[cancel_action_id]
        for cancel_action_id in (order.cancel_action_ids if order is not None else ())
        if cancel_action_id in projection.actions
    )
    effective_product_id = product_id or (order.product_id if order is not None else None)
    open_orders = tuple(
        sorted(
            (
                open_order
                for open_order in projection.open_orders
                if effective_product_id is None or open_order.product_id == effective_product_id
            ),
            key=lambda open_order: (
                open_order.requested_sequence if open_order.requested_sequence is not None else 0,
                open_order.action_id,
            ),
        )
    )
    payload = {
        "action_id": order.action_id if order is not None else action_id,
        "cancel_action_count": len(cancel_actions),
        "cancel_actions": [_action_payload(action) for action in cancel_actions],
        "client_order_id": order.client_order_id if order is not None else None,
        "exchange_order_id": order.exchange_order_id if order is not None else exchange_order_id,
        "issues": [_issue_payload(issue) for issue in issues],
        "ledger": {
            "last_hash": view.state.last_hash,
            "ledger_path": view.ledger_path.as_posix(),
            "next_sequence": view.state.next_sequence,
            "record_count": len(view.records),
            "verified": True,
        },
        "logical_order_id": (
            projection.logical_order_id_by_action_id.get(order.action_id)
            if order is not None
            else None
        ),
        "open_order_count": len(open_orders),
        "open_orders": [_open_order_payload(open_order) for open_order in open_orders],
        "order": _open_order_payload(order) if order is not None else None,
        "place_action": (
            _action_payload(projection.actions[order.action_id])
            if order is not None and order.action_id in projection.actions
            else None
        ),
        "product_id": effective_product_id,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
        "status": (
            ReadinessStatus.OK.value
            if not issues
            else ReadinessStatus.ATTENTION_REQUIRED.value
        ),
        "websocket_started": False,
        "writes_ledger": False,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary evidence payload must normalize to an object")
    return normalized


def record_operator_canary_evidence_result(
    config: CoinbaseApplicationConfig,
    payload: dict[str, JsonValue],
) -> AuditRecord:
    record_payload = operator_canary_evidence_result_record_payload(config, payload)
    return AuditCore(AuditLedger(config.ledger_path)).emit(
        EventType.OPERATOR_CANARY_EVIDENCE_RESULT,
        record_payload,
    )


def operator_canary_evidence_result_record_payload(
    config: CoinbaseApplicationConfig,
    payload: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    ledger = _payload_dict(payload.get("ledger"))
    order = _payload_dict(payload.get("order"))
    place_action = _payload_dict(payload.get("place_action"))
    issue_names = _issue_names(payload.get("issues"))
    record_payload = {
        "action_id": _string_or_none(payload.get("action_id")),
        "cancel_action_count": _int_or_zero(payload.get("cancel_action_count")),
        "client_order_id": _string_or_none(payload.get("client_order_id")),
        "config_fingerprint": application_config_fingerprint(config),
        "evidence_ledger": {
            "last_hash": _string_or_none(ledger.get("last_hash")),
            "next_sequence": _int_or_none(ledger.get("next_sequence")),
            "record_count": _int_or_zero(ledger.get("record_count")),
            "verified": ledger.get("verified") is True,
        },
        "evidence_read_only": (
            payload.get("writes_ledger") is False
            and payload.get("runtime_tasks_started") is False
            and payload.get("websocket_started") is False
        ),
        "exchange_order_id": _string_or_none(payload.get("exchange_order_id")),
        "fingerprint_algorithm": CONFIG_FINGERPRINT_ALGORITHM,
        "issue_count": len(issue_names),
        "issue_names": issue_names,
        "ledger_path": config.ledger_path.as_posix(),
        "logical_order_id": _string_or_none(payload.get("logical_order_id")),
        "open_order_count": _int_or_zero(payload.get("open_order_count")),
        "order_endpoint_called": False,
        "order_lifecycle_status": _string_or_none(order.get("lifecycle_status")),
        "place_action_status": _string_or_none(place_action.get("status")),
        "product_id": _string_or_none(payload.get("product_id")),
        "recording_writes_ledger": True,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_CANARY_EVIDENCE_RESULT_SCHEMA_VERSION,
        "status": _string_or_none(payload.get("status")),
        "websocket_started": False,
    }
    normalized = normalize_json(record_payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary evidence result payload must normalize to an object")
    return normalized


def operator_cancel_all_open_orders_payload(
    config: CoinbaseApplicationConfig,
    application: CoinbaseApplication,
    *,
    action_id_prefix: str | None = None,
    operator_id: str,
    product_id: str | None = None,
    reason: str | None = None,
) -> dict[str, JsonValue]:
    if not operator_id:
        raise ValueError("operator_id is required")

    projection = SourceOfTruthProjection.from_ledger(application.ledger)
    matched_orders = tuple(
        sorted(
            (
                order
                for order in projection.open_orders
                if product_id is None or order.product_id == product_id
            ),
            key=lambda order: (
                order.requested_sequence if order.requested_sequence is not None else 0,
                order.action_id,
            ),
        )
    )
    results: list[dict[str, JsonValue]] = []
    submitted_count = 0
    failed_count = 0
    skipped_count = 0
    effective_reason = reason or OPERATOR_CANCEL_ALL_DEFAULT_REASON

    for index, order in enumerate(matched_orders, start=1):
        exchange_order_id = order.exchange_order_id
        client_order_id = order.client_order_id
        if exchange_order_id is None and client_order_id is None:
            skipped_count += 1
            results.append(
                _batch_result_payload(
                    action_id=None,
                    matched_order=order,
                    receipt=None,
                    skipped=True,
                    skip_reason=OperatorActionSkipReason.MISSING_ORDER_IDENTIFIER,
                    status=ReadinessStatus.ATTENTION_REQUIRED,
                )
            )
            continue

        resolved_action_id = action_id_prefix
        if resolved_action_id is not None:
            resolved_action_id = f"{resolved_action_id}-{index:04d}"
        else:
            resolved_action_id = _default_cancel_action_id(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                next_sequence=projection.last_sequence + index,
                operator_id=operator_id,
            )
        command = ActionCommand(
            action_id=resolved_action_id,
            action_type=ActionType.CANCEL_ORDER,
            idempotency_key=resolved_action_id,
            payload={
                "client_order_id": client_order_id,
                "exchange_order_id": exchange_order_id,
                "metadata": _metadata(
                    allow_untracked=False,
                    batch_index=index,
                    batch_size=len(matched_orders),
                    matched_order=order,
                    operator_id=operator_id,
                    reason=effective_reason,
                ),
            },
            requested_by=_operator_requested_by(operator_id),
        )
        receipt = application.submit_and_execute_action(command)
        submitted_count += 1
        if receipt.status != ActionStatus.EXECUTED:
            failed_count += 1
        results.append(
            _batch_result_payload(
                action_id=resolved_action_id,
                matched_order=order,
                receipt=_receipt_payload(receipt),
                skipped=False,
                skip_reason=None,
                status=(
                    ReadinessStatus.OK
                    if receipt.status == ActionStatus.EXECUTED
                    else ReadinessStatus.ATTENTION_REQUIRED
                ),
            )
        )

    updated_projection = SourceOfTruthProjection.from_ledger(application.ledger)
    remaining_open_orders = tuple(
        order
        for order in updated_projection.open_orders
        if product_id is None or order.product_id == product_id
    )
    status = (
        ReadinessStatus.OK
        if failed_count == 0 and skipped_count == 0 and not remaining_open_orders
        else ReadinessStatus.ATTENTION_REQUIRED
    )
    payload = {
        "action_id_prefix": action_id_prefix,
        "cancel_results": results,
        "failed_count": failed_count,
        "ledger_path": config.ledger_path.as_posix(),
        "matched_open_order_count": len(matched_orders),
        "operator_id": operator_id,
        "product_id": product_id,
        "reason": effective_reason,
        "remaining_open_order_count": len(remaining_open_orders),
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
        "skipped_count": skipped_count,
        "status": status.value,
        "submitted_count": submitted_count,
        "websocket_started": False,
        "writes_ledger": submitted_count > 0,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator cancel-all payload must normalize to an object")
    return normalized


def _select_canary_order(
    projection: SourceOfTruthProjection,
    *,
    action_id: str | None,
    exchange_order_id: str | None,
    product_id: str | None,
) -> tuple[OrderSnapshot | None, list[OperatorCanaryEvidenceIssue]]:
    issues: list[OperatorCanaryEvidenceIssue] = []
    action_order = projection.orders_by_action_id.get(action_id) if action_id is not None else None
    exchange_order = (
        projection.orders_by_exchange_order_id.get(exchange_order_id)
        if exchange_order_id is not None
        else None
    )
    if action_id is not None and action_order is None:
        issues.append(OperatorCanaryEvidenceIssue.NO_MATCHING_ORDER)
    if exchange_order_id is not None and exchange_order is None:
        issues.append(OperatorCanaryEvidenceIssue.NO_MATCHING_ORDER)
    if action_order is not None and exchange_order is not None and action_order.action_id != exchange_order.action_id:
        issues.append(OperatorCanaryEvidenceIssue.IDENTIFIER_MISMATCH)
        return None, issues

    order = action_order or exchange_order
    if order is None and action_id is None and exchange_order_id is None:
        candidates = tuple(
            sorted(
                (
                    candidate
                    for candidate in projection.orders_by_action_id.values()
                    if product_id is None or candidate.product_id == product_id
                ),
                key=lambda candidate: (
                    candidate.requested_sequence if candidate.requested_sequence is not None else 0,
                    candidate.action_id,
                ),
                reverse=True,
            )
        )
        order = candidates[0] if candidates else None
        if order is None:
            issues.append(OperatorCanaryEvidenceIssue.NO_MATCHING_ORDER)

    if order is not None and product_id is not None and order.product_id != product_id:
        issues.append(OperatorCanaryEvidenceIssue.PRODUCT_MISMATCH)
    return order, issues


def _canary_lifecycle_issues(
    projection: SourceOfTruthProjection,
    order: OrderSnapshot,
    *,
    product_id: str | None,
) -> list[OperatorCanaryEvidenceIssue]:
    issues: list[OperatorCanaryEvidenceIssue] = []
    place_action = projection.actions.get(order.action_id)
    if place_action is None:
        issues.append(OperatorCanaryEvidenceIssue.PLACE_ACTION_MISSING)
    elif place_action.status != ActionStatus.EXECUTED:
        issues.append(OperatorCanaryEvidenceIssue.PLACE_ACTION_NOT_EXECUTED)

    if order.lifecycle_status in {
        OrderLifecycleStatus.ACCEPTED,
        OrderLifecycleStatus.CANCEL_QUEUED,
        OrderLifecycleStatus.EXECUTION_UNKNOWN,
        OrderLifecycleStatus.OPEN,
        OrderLifecycleStatus.PENDING,
        OrderLifecycleStatus.REQUESTED,
    }:
        issues.append(OperatorCanaryEvidenceIssue.ORDER_STILL_OPEN)
    elif order.lifecycle_status == OrderLifecycleStatus.FILLED:
        issues.append(OperatorCanaryEvidenceIssue.ORDER_FILLED)
    elif order.lifecycle_status != OrderLifecycleStatus.CANCELLED:
        issues.append(OperatorCanaryEvidenceIssue.ORDER_NOT_CANCELLED)

    if not order.cancel_action_ids:
        issues.append(OperatorCanaryEvidenceIssue.CANCEL_ACTION_MISSING)
    for cancel_action_id in order.cancel_action_ids:
        cancel_action = projection.actions.get(cancel_action_id)
        if cancel_action is None:
            issues.append(OperatorCanaryEvidenceIssue.CANCEL_ACTION_MISSING)
        elif cancel_action.status != ActionStatus.EXECUTED:
            issues.append(OperatorCanaryEvidenceIssue.CANCEL_ACTION_NOT_EXECUTED)

    effective_product_id = product_id or order.product_id
    remaining_open_orders = tuple(
        open_order
        for open_order in projection.open_orders
        if effective_product_id is None or open_order.product_id == effective_product_id
    )
    if remaining_open_orders:
        issues.append(OperatorCanaryEvidenceIssue.OPEN_ORDERS_REMAIN_FOR_PRODUCT)
    return issues


def operator_cancel_order_payload(
    config: CoinbaseApplicationConfig,
    application: CoinbaseApplication,
    *,
    action_id: str | None = None,
    allow_untracked: bool = False,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    operator_id: str,
    reason: str | None = None,
) -> dict[str, JsonValue]:
    if not operator_id:
        raise ValueError("operator_id is required")
    if not exchange_order_id and not client_order_id:
        raise ValueError("exchange_order_id or client_order_id is required")

    projection = SourceOfTruthProjection.from_ledger(application.ledger)
    matched_order = _matching_open_order(
        projection,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )
    if matched_order is None and not allow_untracked:
        return _payload(
            config,
            action_id=action_id,
            allow_untracked=allow_untracked,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            matched_order=None,
            operator_id=operator_id,
            reason=reason,
            receipt=None,
            status=ReadinessStatus.ATTENTION_REQUIRED,
            submitted=False,
            writes_ledger=False,
        )

    resolved_action_id = action_id or _default_cancel_action_id(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        next_sequence=projection.last_sequence + 1,
        operator_id=operator_id,
    )
    command = ActionCommand(
        action_id=resolved_action_id,
        action_type=ActionType.CANCEL_ORDER,
        idempotency_key=resolved_action_id,
        payload={
            "client_order_id": client_order_id,
            "exchange_order_id": exchange_order_id,
            "metadata": _metadata(
                allow_untracked=allow_untracked,
                matched_order=matched_order,
                operator_id=operator_id,
                reason=reason,
            ),
        },
        requested_by=_operator_requested_by(operator_id),
    )
    receipt = application.submit_and_execute_action(command)
    updated_projection = SourceOfTruthProjection.from_ledger(application.ledger)
    updated_order = _matching_order(
        updated_projection,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )
    return _payload(
        config,
        action_id=resolved_action_id,
        allow_untracked=allow_untracked,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        matched_order=updated_order or matched_order,
        operator_id=operator_id,
        reason=reason,
        receipt=_receipt_payload(receipt),
        status=ReadinessStatus.OK if receipt.status == ActionStatus.EXECUTED else ReadinessStatus.ATTENTION_REQUIRED,
        submitted=True,
        writes_ledger=True,
    )


def operator_place_order_payload(
    config: CoinbaseApplicationConfig,
    application: CoinbaseApplication,
    *,
    action_id: str | None = None,
    client_order_id: str | None = None,
    leverage: str | None = None,
    limit_price: str,
    margin_type: MarginType | None = None,
    operator_id: str,
    order_type: OrderType,
    post_only: bool = False,
    product_id: str,
    reason: str,
    reduce_only: bool = False,
    side: OrderSide,
    size: str,
    time_in_force: TimeInForce,
) -> dict[str, JsonValue]:
    if not operator_id:
        raise ValueError("operator_id is required")
    if not reason:
        raise ValueError("reason is required")
    if not product_id:
        raise ValueError("product_id is required")
    if not size:
        raise ValueError("size is required")
    if not limit_price:
        raise ValueError("limit_price is required")
    if not isinstance(side, OrderSide):
        raise TypeError("side must be an OrderSide")
    if not isinstance(order_type, OrderType):
        raise TypeError("order_type must be an OrderType")
    if not isinstance(time_in_force, TimeInForce):
        raise TypeError("time_in_force must be a TimeInForce")
    if margin_type is not None and not isinstance(margin_type, MarginType):
        raise TypeError("margin_type must be a MarginType")

    projection = SourceOfTruthProjection.from_ledger(application.ledger)
    resolved_action_id = action_id or _default_place_order_action_id(
        limit_price=limit_price,
        next_sequence=projection.last_sequence + 1,
        operator_id=operator_id,
        order_type=order_type,
        product_id=product_id,
        side=side,
        size=size,
        time_in_force=time_in_force,
    )
    resolved_client_order_id = client_order_id or resolved_action_id
    intent = PlaceOrderIntent(
        action_id=resolved_action_id,
        idempotency_key=resolved_client_order_id,
        leverage=leverage,
        limit_price=limit_price,
        margin_type=margin_type,
        metadata=_place_order_metadata(
            operator_id=operator_id,
            reason=reason,
        ),
        order_type=order_type,
        post_only=post_only,
        product_id=product_id,
        reduce_only=reduce_only,
        requested_by=_operator_requested_by(operator_id),
        side=side,
        size=size,
        time_in_force=time_in_force,
    )
    receipt = application.submit_and_execute_action(intent.to_command())
    updated_projection = SourceOfTruthProjection.from_ledger(application.ledger)
    order = updated_projection.orders_by_action_id.get(resolved_action_id)
    logical_order_id = updated_projection.logical_order_id_by_action_id.get(resolved_action_id)
    payload = {
        "action_id": resolved_action_id,
        "client_order_id": order.client_order_id if order is not None else resolved_client_order_id,
        "exchange_order_id": order.exchange_order_id if order is not None else None,
        "ledger_path": config.ledger_path.as_posix(),
        "logical_order_id": logical_order_id,
        "operator_id": operator_id,
        "order": _open_order_payload(order) if order is not None else None,
        "reason": reason,
        "receipt": _receipt_payload(receipt),
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
        "status": (
            ReadinessStatus.OK.value
            if receipt.status == ActionStatus.EXECUTED
            else ReadinessStatus.ATTENTION_REQUIRED.value
        ),
        "submitted": True,
        "websocket_started": False,
        "writes_ledger": True,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator place-order payload must normalize to an object")
    return normalized


def _payload(
    config: CoinbaseApplicationConfig,
    *,
    action_id: str | None,
    allow_untracked: bool,
    client_order_id: str | None,
    exchange_order_id: str | None,
    matched_order: OrderSnapshot | None,
    operator_id: str,
    reason: str | None,
    receipt: Mapping[str, JsonValue] | None,
    status: ReadinessStatus,
    submitted: bool,
    writes_ledger: bool,
) -> dict[str, JsonValue]:
    payload = {
        "action_id": action_id,
        "allow_untracked": allow_untracked,
        "client_order_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "ledger_path": config.ledger_path.as_posix(),
        "matched_order": _order_payload(matched_order),
        "operator_id": operator_id,
        "reason": reason,
        "receipt": dict(receipt) if receipt is not None else None,
        "runtime_tasks_started": False,
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
        "status": status.value,
        "submitted": submitted,
        "websocket_started": False,
        "writes_ledger": writes_ledger,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator cancel payload must normalize to an object")
    return normalized


def _metadata(
    *,
    allow_untracked: bool,
    batch_index: int | None = None,
    batch_size: int | None = None,
    matched_order: OrderSnapshot | None,
    operator_id: str,
    reason: str | None,
) -> dict[str, JsonValue]:
    metadata = {
        "allow_untracked": allow_untracked,
        "batch_index": batch_index,
        "batch_size": batch_size,
        "matched_action_id": matched_order.action_id if matched_order is not None else None,
        "operator_id": operator_id,
        "reason": reason,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
    }
    normalized = normalize_json(metadata)
    if not isinstance(normalized, dict):
        raise TypeError("operator cancel metadata must normalize to an object")
    return normalized


def _place_order_metadata(
    *,
    operator_id: str,
    reason: str,
) -> dict[str, JsonValue]:
    metadata = {
        "operator_id": operator_id,
        "reason": reason,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": OPERATOR_ACTION_SCHEMA_VERSION,
    }
    normalized = normalize_json(metadata)
    if not isinstance(normalized, dict):
        raise TypeError("operator place-order metadata must normalize to an object")
    return normalized


def _batch_result_payload(
    *,
    action_id: str | None,
    matched_order: OrderSnapshot | None,
    receipt: Mapping[str, JsonValue] | None,
    skipped: bool,
    skip_reason: OperatorActionSkipReason | None,
    status: ReadinessStatus,
) -> dict[str, JsonValue]:
    payload = {
        "action_id": action_id,
        "matched_order": _order_payload(matched_order),
        "receipt": dict(receipt) if receipt is not None else None,
        "skip_reason": skip_reason.value if skip_reason is not None else None,
        "skipped": skipped,
        "status": status.value,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator cancel-all result payload must normalize to an object")
    return normalized


def _matching_open_order(
    projection: SourceOfTruthProjection,
    *,
    client_order_id: str | None,
    exchange_order_id: str | None,
) -> OrderSnapshot | None:
    order = _matching_order(
        projection,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )
    if order is None:
        return None
    if order.lifecycle_status in {
        OrderLifecycleStatus.ACCEPTED,
        OrderLifecycleStatus.CANCEL_QUEUED,
        OrderLifecycleStatus.EXECUTION_UNKNOWN,
        OrderLifecycleStatus.OPEN,
        OrderLifecycleStatus.PENDING,
        OrderLifecycleStatus.REQUESTED,
    }:
        return order
    return None


def _matching_order(
    projection: SourceOfTruthProjection,
    *,
    client_order_id: str | None,
    exchange_order_id: str | None,
) -> OrderSnapshot | None:
    if exchange_order_id is not None:
        order = projection.orders_by_exchange_order_id.get(exchange_order_id)
        if order is not None:
            return order
    if client_order_id is not None:
        return projection.orders_by_client_order_id.get(client_order_id)
    return None


def _order_payload(order: OrderSnapshot | None) -> dict[str, JsonValue] | None:
    if order is None:
        return None
    payload = {
        "action_id": order.action_id,
        "client_order_id": order.client_order_id,
        "exchange_order_id": order.exchange_order_id,
        "lifecycle_status": order.lifecycle_status.value,
        "product_id": order.product_id,
        "side": order.side.value if order.side is not None else None,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator cancel order payload must normalize to an object")
    return normalized


def _open_order_payload(order: OrderSnapshot) -> dict[str, JsonValue]:
    payload = {
        "action_id": order.action_id,
        "accepted_sequence": order.accepted_sequence,
        "client_order_id": order.client_order_id,
        "exchange_order_id": order.exchange_order_id,
        "executed_sequence": order.executed_sequence,
        "execution_mode": order.execution_mode.value if order.execution_mode is not None else None,
        "limit_price": order.limit_price,
        "lifecycle_status": order.lifecycle_status.value,
        "order_type": order.order_type.value if order.order_type is not None else None,
        "post_only": order.post_only,
        "product_id": order.product_id,
        "reduce_only": order.reduce_only,
        "requested_sequence": order.requested_sequence,
        "side": order.side.value if order.side is not None else None,
        "size": order.size,
        "time_in_force": order.time_in_force.value if order.time_in_force is not None else None,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator open order payload must normalize to an object")
    return normalized


def _receipt_payload(receipt: ActionReceipt) -> dict[str, JsonValue]:
    payload = {
        "action_id": receipt.action_id,
        "action_type": receipt.action_type.value,
        "decision_sequence": receipt.decision_sequence,
        "failure_reason": receipt.failure_reason.value if receipt.failure_reason is not None else None,
        "message": receipt.message,
        "rejection_reason": receipt.rejection_reason.value if receipt.rejection_reason is not None else None,
        "requested_sequence": receipt.requested_sequence,
        "status": receipt.status.value,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator cancel receipt payload must normalize to an object")
    return normalized


def _action_payload(action: ActionSnapshot) -> dict[str, JsonValue]:
    payload = {
        "accepted_sequence": action.accepted_sequence,
        "action_id": action.action_id,
        "executed_sequence": action.executed_sequence,
        "execution_started_sequence": action.execution_started_sequence,
        "failed_sequence": action.failed_sequence,
        "failure_reason": action.failure_reason.value if action.failure_reason is not None else None,
        "rejected_sequence": action.rejected_sequence,
        "requested_sequence": action.requested_sequence,
        "status": action.status.value,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator action payload must normalize to an object")
    return normalized


def _issue_payload(issue: OperatorCanaryEvidenceIssue) -> dict[str, JsonValue]:
    payload = {"issue": issue.value}
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("operator canary evidence issue payload must normalize to an object")
    return normalized


def _unique_canary_issues(
    issues: list[OperatorCanaryEvidenceIssue],
) -> list[OperatorCanaryEvidenceIssue]:
    unique: list[OperatorCanaryEvidenceIssue] = []
    seen: set[OperatorCanaryEvidenceIssue] = set()
    for issue in issues:
        if issue in seen:
            continue
        unique.append(issue)
        seen.add(issue)
    return unique


def _operator_requested_by(operator_id: str) -> str:
    return f"{OPERATOR_REQUESTED_BY_PREFIX}{operator_id}"


def _payload_dict(value: JsonValue) -> dict[str, JsonValue]:
    normalized = normalize_json(value)
    return normalized if isinstance(normalized, dict) else {}


def _int_or_none(value: JsonValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _int_or_zero(value: JsonValue) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None and parsed >= 0 else 0


def _string_or_none(value: JsonValue) -> str | None:
    return value if isinstance(value, str) and value else None


def _issue_names(value: JsonValue) -> list[str]:
    if not isinstance(value, list):
        return []
    issue_names: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        issue = item.get("issue")
        if isinstance(issue, str) and issue:
            issue_names.append(issue)
    return issue_names


def _default_cancel_action_id(
    *,
    client_order_id: str | None,
    exchange_order_id: str | None,
    next_sequence: int,
    operator_id: str,
) -> str:
    digest = sha256(
        canonical_json(
            {
                "client_order_id": client_order_id,
                "exchange_order_id": exchange_order_id,
                "next_sequence": next_sequence,
                "operator_id": operator_id,
            }
        ).encode("utf-8")
    ).hexdigest()[:20]
    return f"act-operator-cancel-{digest}"


def _default_place_order_action_id(
    *,
    limit_price: str,
    next_sequence: int,
    operator_id: str,
    order_type: OrderType,
    product_id: str,
    side: OrderSide,
    size: str,
    time_in_force: TimeInForce,
) -> str:
    digest = sha256(
        canonical_json(
            {
                "limit_price": limit_price,
                "next_sequence": next_sequence,
                "operator_id": operator_id,
                "order_type": order_type.value,
                "product_id": product_id,
                "side": side.value,
                "size": size,
                "time_in_force": time_in_force.value,
            }
        ).encode("utf-8")
    ).hexdigest()[:20]
    return f"act-operator-place-{digest}"
