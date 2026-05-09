from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from actions.gateway import ActionCommand, CancelOrderIntent, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.clock import Clock
from core.engine import AuditCore
from core.enums import (
    ActionRejectionReason,
    ActionStatus,
    ActionType,
    CoinbaseWebSocketChannel,
    ExchangeOrderStatus,
    EventType,
    ExecutionMode,
    MarginType,
    OrderLineageRelation,
    OrderBookSide,
    OrderPlacementKind,
    OrderPlacementStatus,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    StrategySimulationStatus,
    TimeInForce,
)
from core.json_tools import JsonValue, normalize_json
from orders.lineage import LogicalOrderRecord, OrderPlacementRecord
from products.catalog import ProductCatalog
from products.replay import product_catalog_from_projection
from projections.state import SourceOfTruthProjection
from risk.gate import RiskGate
from strategies.harness import Strategy
from strategies.harness import StrategyInputRequirement
from strategies.static_intent import StaticIntentStrategy
from strategies.simulation import (
    StrategySimulationActionPreview,
    StrategySimulationReport,
    simulate_strategies,
)

if TYPE_CHECKING:
    from strategies.operator_policy import OperatorPolicy

STRATEGY_SCENARIO_SCHEMA_VERSION = 1
DEFAULT_SCENARIO_SOURCE_ID = "coinbase-primary"
EnumValue = TypeVar("EnumValue", bound=Enum)


@dataclass(frozen=True)
class StrategyScenarioEvent:
    event_type: EventType
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.event_type, EventType):
            raise TypeError("event_type must be an EventType")
        _object_payload(self.payload, "payload")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "event_type": self.event_type.value,
            "payload": _object_payload(self.payload, "payload"),
        }


@dataclass(frozen=True)
class StrategyScenarioExpectedActionPreview:
    strategy_id: str
    action_id: str
    status: ActionStatus
    rejection_reason: ActionRejectionReason | None = None
    command_payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.strategy_id:
            raise ValueError("strategy_id is required")
        if not self.action_id:
            raise ValueError("action_id is required")
        if not isinstance(self.status, ActionStatus):
            raise TypeError("status must be an ActionStatus")
        if self.rejection_reason is not None and not isinstance(
            self.rejection_reason,
            ActionRejectionReason,
        ):
            raise TypeError("rejection_reason must be an ActionRejectionReason")
        _object_payload(self.command_payload, "command_payload")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "action_id": self.action_id,
            "command_payload": _object_payload(self.command_payload, "command_payload"),
            "rejection_reason": (
                self.rejection_reason.value if self.rejection_reason is not None else None
            ),
            "status": self.status.value,
            "strategy_id": self.strategy_id,
        }


@dataclass(frozen=True)
class StrategyScenarioExpectations:
    status: StrategySimulationStatus | None = None
    accepted_action_count: int | None = None
    rejected_action_count: int | None = None
    completed_count: int | None = None
    failed_count: int | None = None
    intent_count: int | None = None
    action_previews: tuple[StrategyScenarioExpectedActionPreview, ...] = ()

    def __post_init__(self) -> None:
        if self.status is not None and not isinstance(self.status, StrategySimulationStatus):
            raise TypeError("status must be a StrategySimulationStatus")
        _require_optional_non_negative(self.accepted_action_count, "accepted_action_count")
        _require_optional_non_negative(self.rejected_action_count, "rejected_action_count")
        _require_optional_non_negative(self.completed_count, "completed_count")
        _require_optional_non_negative(self.failed_count, "failed_count")
        _require_optional_non_negative(self.intent_count, "intent_count")
        if not isinstance(self.action_previews, tuple):
            raise TypeError("action_previews must be a tuple")
        keys = tuple((preview.strategy_id, preview.action_id) for preview in self.action_previews)
        if len(keys) != len(set(keys)):
            raise ValueError("expected action previews must be unique by strategy_id and action_id")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "accepted_action_count": self.accepted_action_count,
            "action_previews": [preview.to_payload() for preview in self.action_previews],
            "completed_count": self.completed_count,
            "failed_count": self.failed_count,
            "intent_count": self.intent_count,
            "rejected_action_count": self.rejected_action_count,
            "status": self.status.value if self.status is not None else None,
        }


@dataclass(frozen=True)
class StrategyScenarioFailure:
    check: str
    expected: JsonValue
    observed: JsonValue
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.check:
            raise ValueError("check is required")
        normalize_json(self.expected)
        normalize_json(self.observed)
        _object_payload(self.context, "context")

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "check": self.check,
            "context": _object_payload(self.context, "context"),
            "expected": normalize_json(self.expected),
            "observed": normalize_json(self.observed),
        }


@dataclass(frozen=True)
class StrategyScenario:
    name: str
    events: tuple[StrategyScenarioEvent, ...] = ()
    expectations: StrategyScenarioExpectations = field(default_factory=StrategyScenarioExpectations)
    execution_mode: ExecutionMode = ExecutionMode.DRY_RUN
    static_strategies: tuple[StaticIntentStrategy, ...] = ()
    strategy_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name is required")
        if not isinstance(self.events, tuple):
            raise TypeError("events must be a tuple")
        if not isinstance(self.expectations, StrategyScenarioExpectations):
            raise TypeError("expectations must be a StrategyScenarioExpectations")
        if not isinstance(self.execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")
        _unique_non_empty_strings(self.strategy_ids, "strategy_ids", allow_empty=True)
        if not isinstance(self.static_strategies, tuple):
            raise TypeError("static_strategies must be a tuple")
        static_strategy_ids = tuple(strategy.strategy_id for strategy in self.static_strategies)
        _unique_non_empty_strings(static_strategy_ids, "static_strategies[].strategy_id", allow_empty=True)

    def to_payload(self) -> dict[str, JsonValue]:
        return {
            "event_count": len(self.events),
            "events": [event.to_payload() for event in self.events],
            "execution_mode": self.execution_mode.value,
            "expectations": self.expectations.to_payload(),
            "name": self.name,
            "schema_version": STRATEGY_SCENARIO_SCHEMA_VERSION,
            "static_strategies": [strategy.to_payload() for strategy in self.static_strategies],
            "strategy_ids": list(self.strategy_ids),
        }


@dataclass(frozen=True)
class StrategyScenarioResult:
    scenario: StrategyScenario
    ledger_path: Path
    simulation: StrategySimulationReport
    expectation_failures: tuple[StrategyScenarioFailure, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.scenario, StrategyScenario):
            raise TypeError("scenario must be a StrategyScenario")
        if not isinstance(self.ledger_path, Path):
            raise TypeError("ledger_path must be a pathlib.Path")
        if not isinstance(self.simulation, StrategySimulationReport):
            raise TypeError("simulation must be a StrategySimulationReport")
        if not isinstance(self.expectation_failures, tuple):
            raise TypeError("expectation_failures must be a tuple")

    @property
    def passed(self) -> bool:
        return not self.expectation_failures

    def to_payload(self) -> dict[str, JsonValue]:
        payload = {
            "expectation_failures": [failure.to_payload() for failure in self.expectation_failures],
            "ledger_path": self.ledger_path.as_posix(),
            "passed": self.passed,
            "scenario": self.scenario.to_payload(),
            "simulation": self.simulation.to_payload(),
        }
        normalized = normalize_json(payload)
        if not isinstance(normalized, dict):
            raise TypeError("Strategy scenario result payload must normalize to an object")
        return normalized


def scenario_order_book(
    *,
    product_id: str,
    bid: str,
    ask: str,
    bid_size: str = "1",
    ask_size: str = "1",
    sequence_num: int = 1,
    received_sequence: int = 1,
    source_id: str = DEFAULT_SCENARIO_SOURCE_ID,
    message_key: str | None = None,
) -> tuple[StrategyScenarioEvent, ...]:
    _required_string(product_id, "product_id")
    _required_string(bid, "bid")
    _required_string(ask, "ask")
    _required_string(bid_size, "bid_size")
    _required_string(ask_size, "ask_size")
    _require_positive_int(sequence_num, "sequence_num")
    _require_positive_int(received_sequence, "received_sequence")
    resolved_message_key = message_key or (
        f"coinbase:{CoinbaseWebSocketChannel.LEVEL2.value}:{product_id}:{sequence_num}"
    )
    return _feed_event_pair(
        feed_payload={
            "channel": CoinbaseWebSocketChannel.LEVEL2.value,
            "raw": {
                "channel": CoinbaseWebSocketChannel.LEVEL2.value,
                "events": [
                    {
                        "product_id": product_id,
                        "type": "snapshot",
                        "updates": [
                            {
                                "new_quantity": bid_size,
                                "price_level": bid,
                                "product_id": product_id,
                                "side": OrderBookSide.BID.value,
                            },
                            {
                                "new_quantity": ask_size,
                                "price_level": ask,
                                "product_id": product_id,
                                "side": OrderBookSide.ASK.value,
                            },
                        ],
                    }
                ],
                "sequence_num": sequence_num,
            },
            "sequence_num": sequence_num,
        },
        message_event_type=EventType.DATA_RECEIVED,
        message_key=resolved_message_key,
        received_sequence=received_sequence,
        source_id=source_id,
    )


def scenario_trade(
    *,
    product_id: str,
    price: str,
    size: str,
    side: OrderSide,
    trade_id: str = "scenario-trade-1",
    sequence_num: int = 1,
    received_sequence: int = 1,
    source_id: str = DEFAULT_SCENARIO_SOURCE_ID,
    message_key: str | None = None,
    trade_time: str | None = None,
) -> tuple[StrategyScenarioEvent, ...]:
    _required_string(product_id, "product_id")
    _required_string(price, "price")
    _required_string(size, "size")
    _required_string(trade_id, "trade_id")
    _require_enum(side, OrderSide, "side")
    _require_positive_int(sequence_num, "sequence_num")
    _require_positive_int(received_sequence, "received_sequence")
    resolved_message_key = message_key or (
        f"coinbase:{CoinbaseWebSocketChannel.MARKET_TRADES.value}:{product_id}:{sequence_num}"
    )
    trade_payload: dict[str, JsonValue] = {
        "price": price,
        "product_id": product_id,
        "side": side.name,
        "size": size,
        "trade_id": trade_id,
    }
    if trade_time is not None:
        trade_payload["time"] = _required_string(trade_time, "trade_time")
    return _feed_event_pair(
        feed_payload={
            "channel": CoinbaseWebSocketChannel.MARKET_TRADES.value,
            "raw": {
                "channel": CoinbaseWebSocketChannel.MARKET_TRADES.value,
                "events": [{"trades": [trade_payload]}],
                "sequence_num": sequence_num,
            },
            "sequence_num": sequence_num,
        },
        message_event_type=EventType.DATA_RECEIVED,
        message_key=resolved_message_key,
        received_sequence=received_sequence,
        source_id=source_id,
    )


def scenario_product_snapshot(
    *,
    product_id: str,
    product_type: ProductType = ProductType.SPOT,
    product_venue: ProductVenue = ProductVenue.CBE,
    base_increment: str | None = None,
    quote_increment: str | None = None,
    price_increment: str | None = None,
    base_min_size: str | None = None,
    base_max_size: str | None = None,
    quote_min_size: str | None = None,
    quote_max_size: str | None = None,
    contract_size: str | None = None,
    cancel_only: bool = False,
    is_disabled: bool = False,
    limit_only: bool = False,
    post_only: bool = False,
    trading_disabled: bool = False,
    tradable_for_new_orders: bool | None = None,
    view_only: bool = False,
    raw: Mapping[str, Any] | None = None,
) -> StrategyScenarioEvent:
    _required_string(product_id, "product_id")
    _require_enum(product_type, ProductType, "product_type")
    _require_enum(product_venue, ProductVenue, "product_venue")
    resolved_tradable = (
        not any((cancel_only, is_disabled, trading_disabled, view_only))
        if tradable_for_new_orders is None
        else tradable_for_new_orders
    )
    product_payload = {
        "base_increment": base_increment,
        "base_max_size": base_max_size,
        "base_min_size": base_min_size,
        "cancel_only": cancel_only,
        "contract_size": contract_size,
        "is_disabled": is_disabled,
        "limit_only": limit_only,
        "post_only": post_only,
        "price_increment": price_increment,
        "product_id": product_id,
        "product_type": product_type.value,
        "product_venue": product_venue.value,
        "quote_increment": quote_increment,
        "quote_max_size": quote_max_size,
        "quote_min_size": quote_min_size,
        "raw": _object_payload(raw or {}, "raw"),
        "trading_disabled": trading_disabled,
        "tradable_for_new_orders": resolved_tradable,
        "view_only": view_only,
    }
    return StrategyScenarioEvent(
        event_type=EventType.EXCHANGE_PRODUCT_SNAPSHOT,
        payload={
            "product_count": 1,
            "product_ids": [product_id],
            "products": [product_payload],
        },
    )


def scenario_staged_order(
    *,
    product_id: str,
    side: OrderSide,
    size: str,
    limit_price: str,
    action_id: str = "scenario-staged-order-1",
    logical_order_id: str | None = None,
    idempotency_key: str | None = None,
    requested_sequence: int = 1,
    requested_by: str = "scenario",
    metadata: Mapping[str, Any] | None = None,
) -> tuple[StrategyScenarioEvent, ...]:
    _required_string(product_id, "product_id")
    _require_enum(side, OrderSide, "side")
    _required_string(size, "size")
    _required_string(limit_price, "limit_price")
    _required_string(action_id, "action_id")
    _require_positive_int(requested_sequence, "requested_sequence")
    resolved_logical_order_id = logical_order_id or action_id
    resolved_idempotency_key = idempotency_key or action_id
    normalized_metadata = _object_payload(metadata or {}, "metadata")
    intent = PlaceOrderIntent(
        action_id=action_id,
        idempotency_key=resolved_idempotency_key,
        limit_price=limit_price,
        logical_order_id=resolved_logical_order_id,
        metadata=normalized_metadata,
        order_type=OrderType.LIMIT,
        placement_kind=OrderPlacementKind.STAGED_RELEASE,
        product_id=product_id,
        requested_by=requested_by,
        side=side,
        size=size,
    )
    return (
        StrategyScenarioEvent(
            event_type=EventType.ACTION_REQUESTED,
            payload=intent.to_command().to_payload(),
        ),
        StrategyScenarioEvent(
            event_type=EventType.ACTION_ACCEPTED,
            payload={
                "action_id": action_id,
                "action_type": ActionType.PLACE_ORDER.value,
                "requested_sequence": requested_sequence,
            },
        ),
        StrategyScenarioEvent(
            event_type=EventType.ORDER_LOGICAL_CREATED,
            payload=LogicalOrderRecord(
                created_by_action_id=action_id,
                limit_price=limit_price,
                logical_order_id=resolved_logical_order_id,
                metadata=normalized_metadata,
                product_id=product_id,
                side=side,
                size=size,
            ).to_payload(),
        ),
        StrategyScenarioEvent(
            event_type=EventType.ORDER_PLACEMENT_RECORDED,
            payload=OrderPlacementRecord(
                action_id=action_id,
                limit_price=limit_price,
                logical_order_id=resolved_logical_order_id,
                metadata=normalized_metadata,
                placement_id=action_id,
                placement_kind=OrderPlacementKind.STAGED_RELEASE,
                placement_status=OrderPlacementStatus.STAGED,
                product_id=product_id,
                side=side,
                size=size,
            ).to_payload(),
        ),
    )


def scenario_fill(
    *,
    fill_id: str,
    product_id: str,
    side: OrderSide,
    size: str,
    price: str,
    order_id: str | None = None,
    trade_id: str | None = None,
    commission: str | None = None,
    trade_time: str | None = None,
) -> StrategyScenarioEvent:
    _required_string(fill_id, "fill_id")
    _required_string(product_id, "product_id")
    _require_enum(side, OrderSide, "side")
    _required_string(size, "size")
    _required_string(price, "price")
    return StrategyScenarioEvent(
        event_type=EventType.EXCHANGE_FILL,
        payload={
            "commission": commission,
            "fill_id": fill_id,
            "order_id": order_id,
            "price": price,
            "product_id": product_id,
            "side": side.name,
            "size": size,
            "trade_id": trade_id,
            "trade_time": trade_time,
        },
    )


def scenario_open_order_import(
    *,
    product_id: str,
    side: OrderSide,
    size: str,
    limit_price: str,
    exchange_order_id: str = "scenario-exchange-order-1",
    client_order_id: str = "scenario-client-order-1",
    status: ExchangeOrderStatus = ExchangeOrderStatus.OPEN,
    order_type: OrderType = OrderType.LIMIT,
    sequence_num: int = 1,
    received_sequence: int = 1,
    source_id: str = DEFAULT_SCENARIO_SOURCE_ID,
    message_key: str | None = None,
) -> tuple[StrategyScenarioEvent, ...]:
    _required_string(product_id, "product_id")
    _require_enum(side, OrderSide, "side")
    _required_string(size, "size")
    _required_string(limit_price, "limit_price")
    _required_string(exchange_order_id, "exchange_order_id")
    _required_string(client_order_id, "client_order_id")
    _require_enum(status, ExchangeOrderStatus, "status")
    _require_enum(order_type, OrderType, "order_type")
    _require_positive_int(sequence_num, "sequence_num")
    _require_positive_int(received_sequence, "received_sequence")
    resolved_message_key = message_key or (
        f"coinbase:{CoinbaseWebSocketChannel.USER.value}:{exchange_order_id}:{sequence_num}"
    )
    return _feed_event_pair(
        feed_payload={
            "channel": CoinbaseWebSocketChannel.USER.value,
            "order": {
                "client_order_id": client_order_id,
                "leaves_quantity": size,
                "limit_price": limit_price,
                "order_id": exchange_order_id,
                "order_side": side.name,
                "order_type": order_type.name,
                "product_id": product_id,
                "status": status.value,
            },
            "sequence_num": sequence_num,
        },
        message_event_type=EventType.EXCHANGE_ORDER_UPDATE,
        message_key=resolved_message_key,
        received_sequence=received_sequence,
        source_id=source_id,
    )


def run_strategy_scenario(
    *,
    ledger_path: Path,
    scenario: StrategyScenario,
    strategies: tuple[Strategy, ...],
    clock: Clock | None = None,
    market_data_requirements: tuple[StrategyInputRequirement, ...] = (),
    operator_policy: OperatorPolicy | None = None,
    product_catalog: ProductCatalog | None = None,
    product_catalog_from_scenario: bool = False,
    risk_gate: RiskGate | None = None,
    risk_gate_factory: Callable[[ProductCatalog | None], RiskGate] | None = None,
) -> StrategyScenarioResult:
    if not isinstance(ledger_path, Path):
        raise TypeError("ledger_path must be a pathlib.Path")
    if not isinstance(scenario, StrategyScenario):
        raise TypeError("scenario must be a StrategyScenario")
    if not isinstance(strategies, tuple):
        raise TypeError("strategies must be a tuple")
    if risk_gate is not None and risk_gate_factory is not None:
        raise ValueError("risk_gate and risk_gate_factory cannot both be configured")

    ledger = AuditLedger(ledger_path, clock=clock)
    if ledger.iter_records():
        raise ValueError("strategy scenario ledger_path must be empty")

    core = AuditCore(ledger)
    for event in scenario.events:
        core.emit(event.event_type, event.payload)

    snapshot = ledger.snapshot()
    projection = SourceOfTruthProjection.from_records(snapshot.records)
    resolved_product_catalog = product_catalog
    if (
        resolved_product_catalog is None
        and product_catalog_from_scenario
        and projection.exchange_product_snapshot_count > 0
    ):
        resolved_product_catalog = product_catalog_from_projection(projection)
    resolved_risk_gate = risk_gate
    if risk_gate_factory is not None:
        resolved_risk_gate = risk_gate_factory(resolved_product_catalog)
    report = simulate_strategies(
        clock=clock,
        execution_mode=scenario.execution_mode,
        ledger_last_hash=snapshot.state.last_hash,
        ledger_path=ledger.path,
        ledger_record_count=len(snapshot.records),
        market_data_requirements=market_data_requirements,
        operator_policy=operator_policy,
        product_catalog=resolved_product_catalog,
        projection=projection,
        risk_gate=resolved_risk_gate,
        strategies=strategies,
    )
    return StrategyScenarioResult(
        expectation_failures=_expectation_failures(scenario.expectations, report),
        ledger_path=ledger.path,
        scenario=scenario,
        simulation=report,
    )


def load_strategy_scenario_from_json_file(path: Path) -> StrategyScenario:
    if not isinstance(path, Path):
        raise TypeError("path must be a pathlib.Path")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"strategy scenario file must be valid JSON: {exc}") from exc
    return strategy_scenario_from_mapping(raw)


def strategy_scenario_from_mapping(raw: object) -> StrategyScenario:
    data = _require_mapping(raw, "scenario")
    _reject_unknown_fields(
        data,
        "scenario",
        {
            "events",
            "execution_mode",
            "expectations",
            "name",
            "schema_version",
            "static_strategies",
            "strategy_ids",
        },
    )
    schema_version = data.get("schema_version")
    if schema_version != STRATEGY_SCENARIO_SCHEMA_VERSION:
        raise ValueError(
            "strategy scenario schema_version must be "
            f"{STRATEGY_SCENARIO_SCHEMA_VERSION}"
        )
    return StrategyScenario(
        events=_scenario_events(data.get("events", [])),
        execution_mode=_optional_enum(
            ExecutionMode,
            data.get("execution_mode"),
            "scenario.execution_mode",
            default=ExecutionMode.DRY_RUN,
        ),
        expectations=_scenario_expectations(data.get("expectations", {})),
        name=_required_string(data.get("name"), "scenario.name"),
        static_strategies=_static_strategies(data.get("static_strategies", [])),
        strategy_ids=_strategy_ids(data.get("strategy_ids", [])),
    )


def _expectation_failures(
    expectations: StrategyScenarioExpectations,
    report: StrategySimulationReport,
) -> tuple[StrategyScenarioFailure, ...]:
    failures: list[StrategyScenarioFailure] = []
    _append_count_failure(
        failures,
        check="status",
        expected=expectations.status.value if expectations.status is not None else None,
        observed=report.status.value,
        configured=expectations.status is not None,
    )
    _append_count_failure(
        failures,
        check="accepted_action_count",
        expected=expectations.accepted_action_count,
        observed=report.accepted_action_count,
        configured=expectations.accepted_action_count is not None,
    )
    _append_count_failure(
        failures,
        check="rejected_action_count",
        expected=expectations.rejected_action_count,
        observed=report.rejected_action_count,
        configured=expectations.rejected_action_count is not None,
    )
    _append_count_failure(
        failures,
        check="completed_count",
        expected=expectations.completed_count,
        observed=report.completed_count,
        configured=expectations.completed_count is not None,
    )
    _append_count_failure(
        failures,
        check="failed_count",
        expected=expectations.failed_count,
        observed=report.failed_count,
        configured=expectations.failed_count is not None,
    )
    _append_count_failure(
        failures,
        check="intent_count",
        expected=expectations.intent_count,
        observed=report.intent_count,
        configured=expectations.intent_count is not None,
    )
    preview_index = _action_preview_index(report)
    for expected_preview in expectations.action_previews:
        key = (expected_preview.strategy_id, expected_preview.action_id)
        observed_preview = preview_index.get(key)
        if observed_preview is None:
            failures.append(
                StrategyScenarioFailure(
                    check="action_preview_present",
                    context={
                        "action_id": expected_preview.action_id,
                        "strategy_id": expected_preview.strategy_id,
                    },
                    expected=True,
                    observed=False,
                )
            )
            continue
        preview = observed_preview.preview
        if preview.status != expected_preview.status:
            failures.append(
                StrategyScenarioFailure(
                    check="action_preview_status",
                    context={
                        "action_id": expected_preview.action_id,
                        "strategy_id": expected_preview.strategy_id,
                    },
                    expected=expected_preview.status.value,
                    observed=preview.status.value,
                )
            )
        if (
            expected_preview.rejection_reason is not None
            and preview.rejection_reason != expected_preview.rejection_reason
        ):
            failures.append(
                StrategyScenarioFailure(
                    check="action_preview_rejection_reason",
                    context={
                        "action_id": expected_preview.action_id,
                        "strategy_id": expected_preview.strategy_id,
                    },
                    expected=expected_preview.rejection_reason.value,
                    observed=preview.rejection_reason.value if preview.rejection_reason is not None else None,
                )
            )
        failures.extend(
            _command_payload_subset_failures(
                expected_preview=expected_preview,
                observed_command_payload=observed_preview.command.to_payload()["payload"],
            )
        )
    return tuple(failures)


def _append_count_failure(
    failures: list[StrategyScenarioFailure],
    *,
    check: str,
    expected: JsonValue,
    observed: JsonValue,
    configured: bool,
) -> None:
    if configured and expected != observed:
        failures.append(
            StrategyScenarioFailure(
                check=check,
                expected=expected,
                observed=observed,
            )
        )


def _action_preview_index(
    report: StrategySimulationReport,
) -> dict[tuple[str, str], StrategySimulationActionPreview]:
    return {
        (evaluation.strategy_id, action_preview.command.action_id): action_preview
        for evaluation in report.evaluations
        for action_preview in evaluation.action_previews
    }


def _command_payload_subset_failures(
    *,
    expected_preview: StrategyScenarioExpectedActionPreview,
    observed_command_payload: JsonValue,
) -> tuple[StrategyScenarioFailure, ...]:
    expected_payload = _object_payload(expected_preview.command_payload, "command_payload")
    if not expected_payload:
        return ()
    observed_payload = normalize_json(observed_command_payload)
    if not isinstance(observed_payload, dict):
        return (
            StrategyScenarioFailure(
                check="action_preview_command_payload",
                context={
                    "action_id": expected_preview.action_id,
                    "strategy_id": expected_preview.strategy_id,
                },
                expected=expected_payload,
                observed=observed_payload,
            ),
        )

    failures: list[StrategyScenarioFailure] = []
    _append_payload_subset_failures(
        failures,
        expected=expected_payload,
        observed=observed_payload,
        path="command_payload",
        preview=expected_preview,
    )
    return tuple(failures)


def _append_payload_subset_failures(
    failures: list[StrategyScenarioFailure],
    *,
    expected: Mapping[str, JsonValue],
    observed: Mapping[str, JsonValue],
    path: str,
    preview: StrategyScenarioExpectedActionPreview,
) -> None:
    for key, expected_value in expected.items():
        key_path = f"{path}.{key}"
        if key not in observed:
            failures.append(
                StrategyScenarioFailure(
                    check="action_preview_command_payload",
                    context={
                        "action_id": preview.action_id,
                        "path": key_path,
                        "strategy_id": preview.strategy_id,
                    },
                    expected=expected_value,
                    observed=None,
                )
            )
            continue
        observed_value = observed[key]
        if isinstance(expected_value, Mapping):
            if isinstance(observed_value, Mapping):
                _append_payload_subset_failures(
                    failures,
                    expected=expected_value,
                    observed=observed_value,
                    path=key_path,
                    preview=preview,
                )
                continue
            failures.append(
                StrategyScenarioFailure(
                    check="action_preview_command_payload",
                    context={
                        "action_id": preview.action_id,
                        "path": key_path,
                        "strategy_id": preview.strategy_id,
                    },
                    expected=expected_value,
                    observed=observed_value,
                )
            )
            continue
        if expected_value != observed_value:
            failures.append(
                StrategyScenarioFailure(
                    check="action_preview_command_payload",
                    context={
                        "action_id": preview.action_id,
                        "path": key_path,
                        "strategy_id": preview.strategy_id,
                    },
                    expected=expected_value,
                    observed=observed_value,
                )
            )


def _feed_event_pair(
    *,
    feed_payload: Mapping[str, Any],
    message_event_type: EventType,
    message_key: str,
    received_sequence: int,
    source_id: str,
) -> tuple[StrategyScenarioEvent, ...]:
    _object_payload(feed_payload, "feed_payload")
    _require_enum(message_event_type, EventType, "message_event_type")
    _required_string(message_key, "message_key")
    _require_positive_int(received_sequence, "received_sequence")
    _required_string(source_id, "source_id")
    return (
        StrategyScenarioEvent(
            event_type=EventType.DATA_RECEIVED,
            payload={
                "message_event_type": message_event_type.value,
                "message_key": message_key,
                "payload": feed_payload,
                "source_id": source_id,
            },
        ),
        StrategyScenarioEvent(
            event_type=EventType.DATA_ACCEPTED,
            payload={
                "message_event_type": message_event_type.value,
                "message_key": message_key,
                "received_sequence": received_sequence,
                "source_id": source_id,
            },
        ),
    )


def _object_payload(payload: Mapping[str, Any], field_name: str) -> dict[str, JsonValue]:
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must normalize to a JSON object")
    return normalized


def _require_enum(value: object, enum_type: type[EnumValue], field_name: str) -> EnumValue:
    if not isinstance(value, enum_type):
        raise TypeError(f"{field_name} must be a {enum_type.__name__}")
    return value


def _require_positive_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _require_optional_non_negative(value: int | None, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")


def _scenario_events(raw: object) -> tuple[StrategyScenarioEvent, ...]:
    if not isinstance(raw, list):
        raise TypeError("scenario.events must be a list")
    return tuple(_scenario_event(item, index) for index, item in enumerate(raw))


def _strategy_ids(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise TypeError("scenario.strategy_ids must be a list")
    strategy_ids = tuple(
        _required_string(item, f"scenario.strategy_ids[{index}]")
        for index, item in enumerate(raw)
    )
    _unique_non_empty_strings(strategy_ids, "scenario.strategy_ids", allow_empty=True)
    return strategy_ids


def _static_strategies(raw: object) -> tuple[StaticIntentStrategy, ...]:
    if not isinstance(raw, list):
        raise TypeError("scenario.static_strategies must be a list")
    strategies = tuple(_static_strategy(item, index) for index, item in enumerate(raw))
    strategy_ids = tuple(strategy.strategy_id for strategy in strategies)
    _unique_non_empty_strings(strategy_ids, "scenario.static_strategies[].strategy_id", allow_empty=True)
    return strategies


def _static_strategy(raw: object, index: int) -> StaticIntentStrategy:
    field_name = f"scenario.static_strategies[{index}]"
    data = _require_mapping(raw, field_name)
    _reject_unknown_fields(data, field_name, {"intents", "metadata", "strategy_id"})
    return StaticIntentStrategy(
        intents=_static_strategy_intents(data.get("intents", []), field_name),
        metadata=_object_payload(
            _require_mapping(data.get("metadata", {}), f"{field_name}.metadata"),
            "metadata",
        ),
        strategy_id=_required_string(data.get("strategy_id"), f"{field_name}.strategy_id"),
    )


def _static_strategy_intents(raw: object, field_name: str) -> tuple[ActionCommand | PlaceOrderIntent | CancelOrderIntent, ...]:
    if not isinstance(raw, list):
        raise TypeError(f"{field_name}.intents must be a list")
    return tuple(_static_strategy_intent(item, f"{field_name}.intents[{index}]") for index, item in enumerate(raw))


def _static_strategy_intent(raw: object, field_name: str) -> ActionCommand | PlaceOrderIntent | CancelOrderIntent:
    data = _require_mapping(raw, field_name)
    action_type = _enum_value(ActionType, data.get("action_type"), f"{field_name}.action_type")
    if action_type == ActionType.PLACE_ORDER:
        return _static_place_order_intent(data, field_name)
    if action_type == ActionType.CANCEL_ORDER:
        return _static_cancel_order_intent(data, field_name)
    raise ValueError(f"{field_name}.action_type has unsupported value: {action_type.value}")


def _static_place_order_intent(data: Mapping[str, Any], field_name: str) -> PlaceOrderIntent:
    _reject_unknown_fields(
        data,
        field_name,
        {
            "action_id",
            "action_type",
            "idempotency_key",
            "leverage",
            "limit_price",
            "lineage_relation",
            "logical_order_id",
            "margin_type",
            "order_type",
            "parent_order_id",
            "placement_kind",
            "post_only",
            "product_id",
            "reduce_only",
            "root_order_id",
            "side",
            "size",
            "source_order_ids",
            "time_in_force",
        },
    )
    return PlaceOrderIntent(
        action_id=_required_string(data.get("action_id"), f"{field_name}.action_id"),
        idempotency_key=_optional_string(data.get("idempotency_key"), f"{field_name}.idempotency_key"),
        leverage=_optional_string(data.get("leverage"), f"{field_name}.leverage"),
        limit_price=_optional_string(data.get("limit_price"), f"{field_name}.limit_price"),
        lineage_relation=_optional_enum(
            OrderLineageRelation,
            data.get("lineage_relation"),
            f"{field_name}.lineage_relation",
        ),
        logical_order_id=_optional_string(data.get("logical_order_id"), f"{field_name}.logical_order_id"),
        margin_type=_optional_enum(MarginType, data.get("margin_type"), f"{field_name}.margin_type"),
        order_type=_enum_value(OrderType, data.get("order_type"), f"{field_name}.order_type"),
        parent_order_id=_optional_string(data.get("parent_order_id"), f"{field_name}.parent_order_id"),
        placement_kind=_optional_enum(
            OrderPlacementKind,
            data.get("placement_kind"),
            f"{field_name}.placement_kind",
        ),
        post_only=_optional_bool(data.get("post_only"), f"{field_name}.post_only", default=False),
        product_id=_required_string(data.get("product_id"), f"{field_name}.product_id"),
        reduce_only=_optional_bool(data.get("reduce_only"), f"{field_name}.reduce_only", default=False),
        root_order_id=_optional_string(data.get("root_order_id"), f"{field_name}.root_order_id"),
        side=_enum_value(OrderSide, data.get("side"), f"{field_name}.side"),
        size=_required_string(data.get("size"), f"{field_name}.size"),
        source_order_ids=_string_tuple(data.get("source_order_ids", []), f"{field_name}.source_order_ids"),
        time_in_force=_optional_enum(
            TimeInForce,
            data.get("time_in_force"),
            f"{field_name}.time_in_force",
            default=TimeInForce.GOOD_UNTIL_CANCELLED,
        ),
    )


def _static_cancel_order_intent(data: Mapping[str, Any], field_name: str) -> CancelOrderIntent:
    _reject_unknown_fields(
        data,
        field_name,
        {"action_id", "action_type", "client_order_id", "exchange_order_id", "idempotency_key"},
    )
    return CancelOrderIntent(
        action_id=_required_string(data.get("action_id"), f"{field_name}.action_id"),
        client_order_id=_optional_string(data.get("client_order_id"), f"{field_name}.client_order_id"),
        exchange_order_id=_optional_string(
            data.get("exchange_order_id"),
            f"{field_name}.exchange_order_id",
        ),
        idempotency_key=_optional_string(data.get("idempotency_key"), f"{field_name}.idempotency_key"),
    )


def _scenario_event(raw: object, index: int) -> StrategyScenarioEvent:
    field_name = f"scenario.events[{index}]"
    data = _require_mapping(raw, field_name)
    _reject_unknown_fields(data, field_name, {"event_type", "payload"})
    return StrategyScenarioEvent(
        event_type=_enum_value(EventType, data.get("event_type"), f"{field_name}.event_type"),
        payload=_object_payload(
            _require_mapping(data.get("payload", {}), f"{field_name}.payload"),
            "payload",
        ),
    )


def _scenario_expectations(raw: object) -> StrategyScenarioExpectations:
    data = _require_mapping(raw, "scenario.expectations")
    _reject_unknown_fields(
        data,
        "scenario.expectations",
        {
            "accepted_action_count",
            "action_previews",
            "completed_count",
            "failed_count",
            "intent_count",
            "rejected_action_count",
            "status",
        },
    )
    return StrategyScenarioExpectations(
        accepted_action_count=_optional_non_negative_int(
            data.get("accepted_action_count"),
            "scenario.expectations.accepted_action_count",
        ),
        action_previews=_expected_action_previews(data.get("action_previews", [])),
        completed_count=_optional_non_negative_int(
            data.get("completed_count"),
            "scenario.expectations.completed_count",
        ),
        failed_count=_optional_non_negative_int(
            data.get("failed_count"),
            "scenario.expectations.failed_count",
        ),
        intent_count=_optional_non_negative_int(
            data.get("intent_count"),
            "scenario.expectations.intent_count",
        ),
        rejected_action_count=_optional_non_negative_int(
            data.get("rejected_action_count"),
            "scenario.expectations.rejected_action_count",
        ),
        status=_optional_enum(
            StrategySimulationStatus,
            data.get("status"),
            "scenario.expectations.status",
        ),
    )


def _expected_action_previews(
    raw: object,
) -> tuple[StrategyScenarioExpectedActionPreview, ...]:
    if not isinstance(raw, list):
        raise TypeError("scenario.expectations.action_previews must be a list")
    return tuple(_expected_action_preview(item, index) for index, item in enumerate(raw))


def _expected_action_preview(
    raw: object,
    index: int,
) -> StrategyScenarioExpectedActionPreview:
    field_name = f"scenario.expectations.action_previews[{index}]"
    data = _require_mapping(raw, field_name)
    _reject_unknown_fields(
        data,
        field_name,
        {"action_id", "command_payload", "rejection_reason", "status", "strategy_id"},
    )
    rejection_reason = data.get("rejection_reason")
    return StrategyScenarioExpectedActionPreview(
        action_id=_required_string(data.get("action_id"), f"{field_name}.action_id"),
        command_payload=_object_payload(
            _require_mapping(data.get("command_payload", {}), f"{field_name}.command_payload"),
            "command_payload",
        ),
        rejection_reason=(
            _enum_value(
                ActionRejectionReason,
                rejection_reason,
                f"{field_name}.rejection_reason",
            )
            if rejection_reason is not None
            else None
        ),
        status=_enum_value(ActionStatus, data.get("status"), f"{field_name}.status"),
        strategy_id=_required_string(data.get("strategy_id"), f"{field_name}.strategy_id"),
    )


def _require_mapping(raw: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise TypeError(f"{field_name} must be a JSON object")
    return raw


def _reject_unknown_fields(
    data: Mapping[str, Any],
    field_name: str,
    allowed_fields: set[str],
) -> None:
    unknown_fields = sorted(set(data) - allowed_fields)
    if unknown_fields:
        joined = ", ".join(unknown_fields)
        raise ValueError(f"{field_name} has unknown fields: {joined}")


def _required_string(raw: object, field_name: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{field_name} must be a non-empty string")
    return raw


def _optional_string(raw: object, field_name: str) -> str | None:
    if raw is None:
        return None
    return _required_string(raw, field_name)


def _optional_bool(raw: object, field_name: str, *, default: bool) -> bool:
    if raw is None:
        return default
    if not isinstance(raw, bool):
        raise TypeError(f"{field_name} must be a bool")
    return raw


def _string_tuple(raw: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise TypeError(f"{field_name} must be a list")
    values = tuple(
        _required_string(item, f"{field_name}[{index}]")
        for index, item in enumerate(raw)
    )
    _unique_non_empty_strings(values, field_name, allow_empty=True)
    return values


def _unique_non_empty_strings(
    values: tuple[str, ...],
    field_name: str,
    *,
    allow_empty: bool,
) -> None:
    if not allow_empty and not values:
        raise ValueError(f"{field_name} must not be empty")
    if any(not isinstance(value, str) or not value for value in values):
        raise TypeError(f"{field_name} must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must be unique")


def _optional_non_negative_int(raw: object, field_name: str) -> int | None:
    if raw is None:
        return None
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise TypeError(f"{field_name} must be an integer")
    if raw < 0:
        raise ValueError(f"{field_name} must not be negative")
    return raw


def _optional_enum(
    enum_type: type[EnumValue],
    raw: object,
    field_name: str,
    *,
    default: EnumValue | None = None,
) -> EnumValue | None:
    if raw is None:
        return default
    return _enum_value(enum_type, raw, field_name)


def _enum_value(
    enum_type: type[EnumValue],
    raw: object,
    field_name: str,
) -> EnumValue:
    if isinstance(raw, enum_type):
        return raw
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{field_name} must be a non-empty string")
    try:
        return enum_type(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name} has unsupported value: {raw}") from exc
