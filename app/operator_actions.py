from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from actions.gateway import ActionCommand, ActionReceipt, PlaceOrderIntent
from app.bootstrap import CoinbaseApplication, CoinbaseApplicationConfig
from app.ledger_view import load_verified_ledger_view
from core.enums import (
    ActionStatus,
    ActionType,
    MarginType,
    OperatorActionSkipReason,
    OrderLifecycleStatus,
    OrderSide,
    OrderType,
    ReadinessStatus,
    TimeInForce,
)
from core.json_tools import JsonValue, canonical_json, normalize_json
from projections.state import OrderSnapshot, SourceOfTruthProjection


OPERATOR_ACTION_SCHEMA_VERSION = 1
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


def _operator_requested_by(operator_id: str) -> str:
    return f"{OPERATOR_REQUESTED_BY_PREFIX}{operator_id}"


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
