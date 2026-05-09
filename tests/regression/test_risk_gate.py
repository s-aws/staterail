from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from actions.gateway import ActionCommand, ActionGateway, CancelOrderIntent, PlaceOrderIntent
from actions.execution import ExecutionResult
from audit.ledger import AuditLedger
from core.clock import FixedClock
from core.engine import AuditCore
from core.enums import (
    ActionRejectionReason,
    ActionStatus,
    ExecutionMode,
    ExecutionStatus,
    EventType,
    OrderLineageRelation,
    OrderPlacementKind,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    RiskCheckStatus,
    RiskRule,
    TimeInForce,
)
from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection
from risk.gate import RiskGate, RiskPolicy


class SuccessfulExecutor:
    def execute(self, command: ActionCommand) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            client_order_id=command.idempotency_key or command.action_id,
            exchange_order_id=f"exchange-{command.action_id}",
            mode=ExecutionMode.DRY_RUN,
            status=ExecutionStatus.ACCEPTED,
        )


def test_risk_gate_accepts_order_and_audits_passed_checks(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(_policy()))

    receipt = gateway.submit(_place_order("action-1", leverage="3"))

    accepted_record = next(record for record in ledger.iter_records() if record.event_type == EventType.ACTION_ACCEPTED)
    assert receipt.status == ActionStatus.ACCEPTED
    assert accepted_record.event_type == EventType.ACTION_ACCEPTED
    assert accepted_record.payload["risk_evaluation"]["status"] == RiskCheckStatus.PASS.value
    assert {check["rule"] for check in accepted_record.payload["risk_evaluation"]["checks"]} == {
        RiskRule.ALLOWED_ORDER_TYPE.value,
        RiskRule.ALLOWED_PRODUCT.value,
        RiskRule.KILL_SWITCH.value,
        RiskRule.MAX_LEVERAGE.value,
        RiskRule.MAX_ORDER_NOTIONAL.value,
        RiskRule.MAX_ORDER_SIZE.value,
        RiskRule.REDUCE_ONLY_REQUIRED.value,
    }


def test_risk_gate_rejects_disallowed_product_after_auditing_request(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(_policy()))

    receipt = gateway.submit(
        _place_order("action-1", product_id="DOGE-PERP-INTX", leverage="3")
    )

    rejected_record = ledger.iter_records()[-1]
    assert receipt.status == ActionStatus.REJECTED
    assert receipt.rejection_reason == ActionRejectionReason.RISK_CHECK_FAILED
    assert [record.event_type for record in ledger.iter_records()] == [
        EventType.ACTION_REQUESTED,
        EventType.ACTION_REJECTED,
    ]
    assert rejected_record.payload["risk_evaluation"]["status"] == RiskCheckStatus.FAIL.value
    assert "product is not allowed" in rejected_record.payload["validation_errors"]


def test_risk_gate_rejects_notional_and_leverage_breaches(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(_policy()))

    receipt = gateway.submit(_place_order("action-1", size="0.2", leverage="10"))

    rejected_record = ledger.iter_records()[-1]
    failed_rules = {
        check["rule"]
        for check in rejected_record.payload["risk_evaluation"]["checks"]
        if check["status"] == RiskCheckStatus.FAIL.value
    }
    assert receipt.status == ActionStatus.REJECTED
    assert failed_rules == {
        RiskRule.MAX_LEVERAGE.value,
        RiskRule.MAX_ORDER_NOTIONAL.value,
        RiskRule.MAX_ORDER_SIZE.value,
    }


def test_risk_gate_uses_future_contract_size_for_notional_limits(workspace_tmp_path):
    product = ProductMetadata(
        base_increment=Decimal("1"),
        base_min_size=Decimal("1"),
        contract_size=Decimal("10000"),
        price_increment=Decimal("0.00001"),
        product_id="SHB-26JUN26-CDE",
        product_type=ProductType.FUTURE,
        product_venue=ProductVenue.FCM,
        quote_max_size=Decimal("100000000"),
        quote_min_size=Decimal("0"),
    )
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(
            RiskPolicy.from_values(
                allowed_products=("SHB-26JUN26-CDE",),
                max_order_notional="25",
                product_catalog=ProductCatalog((product,)),
            )
        ),
    )

    receipt = gateway.submit(
        _place_order(
            "action-1",
            product_id="SHB-26JUN26-CDE",
            size="1",
            limit_price="0.00636",
        )
    )

    rejected_record = ledger.iter_records()[-1]
    max_notional_check = next(
        check
        for check in rejected_record.payload["risk_evaluation"]["checks"]
        if check["rule"] == RiskRule.MAX_ORDER_NOTIONAL.value
    )
    product_notional_check = next(
        check
        for check in rejected_record.payload["risk_evaluation"]["checks"]
        if check["rule"] == RiskRule.PRODUCT_QUOTE_NOTIONAL.value
    )
    assert receipt.status == ActionStatus.REJECTED
    assert max_notional_check["status"] == RiskCheckStatus.FAIL.value
    assert max_notional_check["observed"] == "63.60000"
    assert product_notional_check["observed"] == "63.60000"


def test_risk_gate_rejects_missing_price_when_notional_limit_is_configured(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(_policy()))

    receipt = gateway.submit(
        _place_order(
            "action-1",
            order_type=OrderType.MARKET,
            limit_price=None,
            leverage="2",
        )
    )

    rejected_record = ledger.iter_records()[-1]
    assert receipt.status == ActionStatus.REJECTED
    assert "limit_price is required when max notional is configured" in rejected_record.payload[
        "validation_errors"
    ]


def test_risk_gate_reduce_only_mode_blocks_non_reduce_only_orders(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    policy = RiskPolicy.from_values(
        allowed_products=("BTC-PERP-INTX",),
        allowed_order_types=(OrderType.LIMIT,),
        require_reduce_only=True,
    )
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(policy))

    receipt = gateway.submit(_place_order("action-1"))

    assert receipt.status == ActionStatus.REJECTED
    assert ledger.iter_records()[-1].payload["validation_errors"] == [
        "reduce-only mode is required"
    ]


def test_risk_gate_kill_switch_blocks_place_orders_but_allows_cancel_orders(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    policy = RiskPolicy.from_values(kill_switch_enabled=True)
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(policy))

    blocked = gateway.submit(_place_order("place-1"))
    cancel = gateway.submit(
        CancelOrderIntent(action_id="cancel-1", exchange_order_id="exchange-1").to_command()
    )

    projection = SourceOfTruthProjection.from_ledger(ledger)
    assert blocked.status == ActionStatus.REJECTED
    assert cancel.status == ActionStatus.ACCEPTED
    assert projection.actions["place-1"].status == ActionStatus.REJECTED
    assert projection.actions["cancel-1"].status == ActionStatus.ACCEPTED


def test_risk_gate_enforces_operator_execution_constraints(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    policy = RiskPolicy.from_values(
        allowed_lineage_relations=(OrderLineageRelation.ROOT,),
        allowed_placement_kinds=(OrderPlacementKind.INITIAL,),
        allowed_sides=(OrderSide.SELL,),
        allowed_time_in_force=(TimeInForce.GOOD_UNTIL_CANCELLED,),
        max_visible_notional="10",
        require_post_only=True,
        require_staged_release_above_visible_limit=True,
    )
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(policy))

    receipt = gateway.submit(_place_order("action-1", size="1", limit_price="100"))

    rejected_record = ledger.iter_records()[-1]
    failed_rules = {
        check["rule"]
        for check in rejected_record.payload["risk_evaluation"]["checks"]
        if check["status"] == RiskCheckStatus.FAIL.value
    }
    assert receipt.status == ActionStatus.REJECTED
    assert failed_rules == {
        RiskRule.ALLOWED_SIDE.value,
        RiskRule.MAX_VISIBLE_NOTIONAL.value,
        RiskRule.POST_ONLY_REQUIRED.value,
    }


def test_risk_gate_enforces_daily_notional_and_open_order_limits(workspace_tmp_path):
    clock = FixedClock(datetime(2026, 1, 1, 12, tzinfo=timezone.utc))
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl", clock=clock)
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(
            RiskPolicy.from_values(
                max_daily_notional="150",
                max_open_orders=1,
            )
        ),
    )

    first = gateway.submit(_place_order("action-1", size="1", limit_price="100"))
    second = gateway.submit(_place_order("action-2", size="0.6", limit_price="100"))

    rejected_record = ledger.iter_records()[-1]
    failed_rules = {
        check["rule"]
        for check in rejected_record.payload["risk_evaluation"]["checks"]
        if check["status"] == RiskCheckStatus.FAIL.value
    }
    assert first.status == ActionStatus.ACCEPTED
    assert second.status == ActionStatus.REJECTED
    assert failed_rules == {
        RiskRule.MAX_DAILY_NOTIONAL.value,
        RiskRule.MAX_OPEN_ORDERS.value,
    }


def test_risk_gate_allows_oversized_staged_release_when_visible_limit_is_configured(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(
            RiskPolicy.from_values(
                max_visible_notional="10",
                require_staged_release_above_visible_limit=True,
            )
        ),
    )

    receipt = gateway.submit(
        _place_order(
            "action-1",
            limit_price="100",
            placement_kind=OrderPlacementKind.STAGED_RELEASE,
            size="1",
        )
    )

    accepted_record = next(record for record in ledger.iter_records() if record.event_type == EventType.ACTION_ACCEPTED)
    visible_check = next(
        check
        for check in accepted_record.payload["risk_evaluation"]["checks"]
        if check["rule"] == RiskRule.MAX_VISIBLE_NOTIONAL.value
    )
    assert receipt.status == ActionStatus.ACCEPTED
    assert visible_check["status"] == RiskCheckStatus.PASS.value


def test_risk_gate_enforces_order_replacement_limit(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(RiskPolicy.from_values(max_order_replacements=1)),
    )
    executor = SuccessfulExecutor()

    gateway.submit_and_execute(
        _place_order("root-action", logical_order_id="logical-order"),
        executor,
    )
    first_move = gateway.submit_and_execute(
        _place_order(
            "move-1",
            logical_order_id="logical-order",
            placement_kind=OrderPlacementKind.CANCEL_REPLACE,
        ),
        executor,
    )
    second_move = gateway.submit_and_execute(
        _place_order(
            "move-2",
            logical_order_id="logical-order",
            placement_kind=OrderPlacementKind.CANCEL_REPLACE,
        ),
        executor,
    )

    assert first_move.status == ActionStatus.EXECUTED
    assert second_move.status == ActionStatus.REJECTED
    assert "order replacement count exceeds limit" in ledger.iter_records()[-1].payload["validation_errors"]


def _policy() -> RiskPolicy:
    return RiskPolicy.from_values(
        allowed_products=("BTC-PERP-INTX", "ETH-PERP-INTX"),
        allowed_order_types=(OrderType.LIMIT,),
        max_order_size="0.1",
        max_order_notional="2000",
        max_leverage="5",
    )


def _place_order(
    action_id: str,
    *,
    logical_order_id: str | None = None,
    product_id: str = "BTC-PERP-INTX",
    order_type: OrderType = OrderType.LIMIT,
    placement_kind: OrderPlacementKind | None = None,
    size: str = "0.01",
    limit_price: str | None = "100000",
    leverage: str | None = None,
) -> ActionCommand:
    return PlaceOrderIntent(
        action_id=action_id,
        product_id=product_id,
        side=OrderSide.BUY,
        order_type=order_type,
        size=size,
        limit_price=limit_price,
        leverage=leverage,
        logical_order_id=logical_order_id,
        placement_kind=placement_kind,
    ).to_command()
