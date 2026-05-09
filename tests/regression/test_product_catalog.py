from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import pytest

from actions.gateway import ActionCommand, ActionGateway, PlaceOrderIntent
from audit.ledger import AuditLedger
from core.engine import AuditCore
from core.enums import (
    ActionRejectionReason,
    ActionStatus,
    ErrorCategory,
    EventType,
    OrderSide,
    OrderType,
    ProductType,
    ProductVenue,
    RiskCheckStatus,
    RiskRule,
)
from core.errors import ExchangeAuthError, ExchangeRateLimitError
from exchanges.coinbase.advanced_trade_rest import CoinbaseRestConfig, HttpResponse
from exchanges.coinbase.auth import static_token_provider
from exchanges.coinbase.products import CoinbaseProductCatalogClient
from products.catalog import ProductCatalog, ProductMetadata
from risk.gate import RiskGate, RiskPolicy


class FakeGetTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self._responses = responses
        self.gets: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query_params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        self.gets.append(
            {
                "headers": dict(headers),
                "query_params": dict(query_params or {}),
                "url": url,
            }
        )
        return self._responses.pop(0)


def test_product_metadata_parses_coinbase_spot_and_perp_payloads():
    spot = ProductMetadata.from_coinbase_payload(_product_payload("BTC-USD", product_type="SPOT", venue="CBE"))
    perp = ProductMetadata.from_coinbase_payload(
        _product_payload("BTC-PERP-INTX", product_type="FUTURE", venue="INTX")
    )
    cfm_future = ProductMetadata.from_coinbase_payload(
        _product_payload(
            "SHB-26JUN26-CDE",
            contract_size="10000",
            product_type="FUTURE",
            venue="FCM",
        )
    )

    assert spot.product_type == ProductType.SPOT
    assert spot.product_venue == ProductVenue.CBE
    assert perp.product_type == ProductType.FUTURE
    assert perp.product_venue == ProductVenue.INTX
    assert cfm_future.product_type == ProductType.FUTURE
    assert cfm_future.product_venue == ProductVenue.FCM
    assert cfm_future.contract_size == Decimal("10000")
    assert cfm_future.notional(Decimal("1"), Decimal("0.00636")) == Decimal("63.60000")
    assert spot.tradable_for_new_orders


def test_product_metadata_payload_is_auditable_json():
    product = ProductMetadata.from_coinbase_payload(
        _product_payload("BTC-USD", contract_size="10", product_type="FUTURE", venue="FCM")
    )

    payload = product.to_payload()
    replayed = ProductMetadata.from_audit_payload(payload)

    assert payload["product_id"] == "BTC-USD"
    assert payload["product_type"] == ProductType.FUTURE.value
    assert payload["product_venue"] == ProductVenue.FCM.value
    assert payload["base_increment"] == "0.0001"
    assert payload["contract_size"] == "10"
    assert payload["tradable_for_new_orders"] is True
    assert payload["raw"]["product_id"] == "BTC-USD"
    assert replayed.product_id == product.product_id
    assert replayed.product_type == product.product_type
    assert replayed.base_increment == product.base_increment
    assert replayed.contract_size == Decimal("10")


def test_product_metadata_calculates_minimum_valid_size_with_contract_multiplier():
    product = ProductMetadata.from_coinbase_payload(
        _product_payload(
            "SHB-26JUN26-CDE",
            base_increment="1",
            base_min_size="1",
            contract_size="10000",
            product_type="FUTURE",
            venue="FCM",
        )
    )

    assert product.minimum_valid_size(Decimal("0.00635")) == Decimal("1")
    assert product.notional(product.minimum_valid_size(Decimal("0.00635")), Decimal("0.00635")) == Decimal("63.50000")


def test_coinbase_product_catalog_client_fetches_single_product():
    transport = FakeGetTransport([HttpResponse(status_code=200, body=_product_payload("BTC-USD"))])
    client = CoinbaseProductCatalogClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )

    product = client.get_product("BTC-USD")

    assert product.product_id == "BTC-USD"
    assert transport.gets[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/products/BTC-USD"
    assert transport.gets[0]["headers"]["Authorization"] == "Bearer test-token"
    assert transport.gets[0]["query_params"] == {"get_tradability_status": "true"}


def test_coinbase_product_catalog_client_lists_filtered_products():
    transport = FakeGetTransport(
        [
            HttpResponse(
                status_code=200,
                body={
                    "products": [
                        _product_payload("BTC-USD", product_type="SPOT"),
                        _product_payload("BTC-PERP-INTX", product_type="FUTURE", venue="INTX"),
                    ]
                },
            )
        ]
    )
    client = CoinbaseProductCatalogClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )

    catalog = client.list_products(
        product_ids=("BTC-USD", "BTC-PERP-INTX"),
        product_type="FUTURE",
        contract_expiry_type="PERPETUAL",
    )

    assert {product.product_id for product in catalog.values()} == {"BTC-USD", "BTC-PERP-INTX"}
    assert transport.gets[0]["url"] == "https://api.coinbase.com/api/v3/brokerage/products"
    assert transport.gets[0]["query_params"] == {
        "contract_expiry_type": "PERPETUAL",
        "get_tradability_status": "true",
        "product_ids": ["BTC-USD", "BTC-PERP-INTX"],
        "product_type": "FUTURE",
    }


def test_coinbase_product_catalog_client_classifies_rate_limits():
    transport = FakeGetTransport([HttpResponse(status_code=429, body={"message": "slow down"})])
    client = CoinbaseProductCatalogClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider("test-token"),
        transport=transport,
    )

    with pytest.raises(ExchangeRateLimitError) as exc_info:
        client.list_products()

    assert exc_info.value.error_code == "http_429"
    assert exc_info.value.retryable is True
    assert exc_info.value.context["error_category"] == ErrorCategory.EXCHANGE_RATE_LIMIT.value


def test_coinbase_product_catalog_client_rejects_empty_token():
    client = CoinbaseProductCatalogClient(
        CoinbaseRestConfig(),
        token_provider=static_token_provider(""),
        transport=FakeGetTransport([]),
    )

    with pytest.raises(ExchangeAuthError, match="empty token"):
        client.get_product("BTC-USD")


def test_risk_gate_accepts_order_when_product_metadata_rules_pass(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(_metadata_policy()))

    receipt = gateway.submit(_place_order("action-1"))

    accepted_record = next(record for record in ledger.iter_records() if record.event_type == EventType.ACTION_ACCEPTED)
    metadata_rules = {
        check["rule"]
        for check in accepted_record.payload["risk_evaluation"]["checks"]
        if check["rule"].startswith("product_")
    }
    assert receipt.status == ActionStatus.ACCEPTED
    assert metadata_rules == {
        RiskRule.PRODUCT_BASE_SIZE.value,
        RiskRule.PRODUCT_PRICE_INCREMENT.value,
        RiskRule.PRODUCT_QUOTE_NOTIONAL.value,
        RiskRule.PRODUCT_TRADABLE.value,
    }


def test_risk_gate_rejects_disabled_product_from_metadata(workspace_tmp_path):
    catalog = ProductCatalog.from_coinbase_payloads(
        [_product_payload("BTC-USD", trading_disabled=True)]
    )
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(RiskPolicy.from_values(product_catalog=catalog)),
    )

    receipt = gateway.submit(_place_order("action-1"))

    rejected_record = ledger.iter_records()[-1]
    assert receipt.status == ActionStatus.REJECTED
    assert receipt.rejection_reason == ActionRejectionReason.RISK_CHECK_FAILED
    assert "product is not tradable for new orders" in rejected_record.payload["validation_errors"]


def test_risk_gate_rejects_product_size_price_and_notional_violations(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(AuditCore(ledger), risk_gate=RiskGate(_metadata_policy()))

    receipt = gateway.submit(_place_order("action-1", size="0.00005", limit_price="100000.05"))

    rejected_record = ledger.iter_records()[-1]
    failed_rules = {
        check["rule"]
        for check in rejected_record.payload["risk_evaluation"]["checks"]
        if check["status"] == RiskCheckStatus.FAIL.value
    }
    assert receipt.status == ActionStatus.REJECTED
    assert failed_rules == {
        RiskRule.PRODUCT_BASE_SIZE.value,
        RiskRule.PRODUCT_PRICE_INCREMENT.value,
        RiskRule.PRODUCT_QUOTE_NOTIONAL.value,
    }


def test_risk_gate_rejects_market_order_when_product_is_limit_only(workspace_tmp_path):
    catalog = ProductCatalog.from_coinbase_payloads([_product_payload("BTC-USD", limit_only=True)])
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(RiskPolicy.from_values(product_catalog=catalog)),
    )

    receipt = gateway.submit(
        _place_order("action-1", order_type=OrderType.MARKET, limit_price=None)
    )

    assert receipt.status == ActionStatus.REJECTED
    assert "product does not allow this order type" in ledger.iter_records()[-1].payload[
        "validation_errors"
    ]


def test_risk_gate_rejects_missing_product_metadata(workspace_tmp_path):
    ledger = AuditLedger(workspace_tmp_path / "audit.jsonl")
    gateway = ActionGateway(
        AuditCore(ledger),
        risk_gate=RiskGate(RiskPolicy.from_values(product_catalog=ProductCatalog())),
    )

    receipt = gateway.submit(_place_order("action-1"))

    assert receipt.status == ActionStatus.REJECTED
    assert "product metadata is missing" in ledger.iter_records()[-1].payload["validation_errors"]


def _metadata_policy() -> RiskPolicy:
    return RiskPolicy.from_values(
        product_catalog=ProductCatalog.from_coinbase_payloads([_product_payload("BTC-USD")])
    )


def _place_order(
    action_id: str,
    *,
    order_type: OrderType = OrderType.LIMIT,
    size: str = "0.01",
    limit_price: str | None = "100000",
) -> ActionCommand:
    return PlaceOrderIntent(
        action_id=action_id,
        product_id="BTC-USD",
        side=OrderSide.BUY,
        order_type=order_type,
        size=size,
        limit_price=limit_price,
    ).to_command()


def _product_payload(
    product_id: str,
    *,
    base_increment: str = "0.0001",
    base_min_size: str = "0.0001",
    contract_size: str | None = None,
    product_type: str = "SPOT",
    quote_min_size: str = "10",
    venue: str = "CBE",
    trading_disabled: bool = False,
    limit_only: bool = False,
) -> dict[str, Any]:
    payload = {
        "base_increment": base_increment,
        "base_max_size": "10",
        "base_min_size": base_min_size,
        "cancel_only": False,
        "is_disabled": False,
        "limit_only": limit_only,
        "price_increment": "0.1",
        "product_id": product_id,
        "product_type": product_type,
        "product_venue": venue,
        "quote_max_size": "1000000",
        "quote_min_size": quote_min_size,
        "trading_disabled": trading_disabled,
        "view_only": False,
    }
    if contract_size is not None:
        payload["future_product_details"] = {"contract_size": contract_size}
    return payload
