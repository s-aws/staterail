from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.bootstrap import CoinbaseApplicationConfig
from app.credentials import COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV
from app.main import ATTENTION_REQUIRED_EXIT_CODE, run_from_args
from app.product_catalog_smoke import product_catalog_smoke_payload
from audit.ledger import AuditLedger
from config.assembly import (
    CoinbaseBotConfig,
    CoinbaseRestApiConfig,
    ProductCatalogRuntimeConfig,
    RiskPolicyConfig,
    StrategyRuntimeConfig,
)
from config.assembly import TaskScheduleConfig
from core.enums import (
    EventType,
    ExecutionMode,
    OrderSide,
    PolicyViabilityReason,
    ProductType,
    ProductVenue,
    ReadinessStatus,
    RuntimeTask,
)
from core.errors import ConfigError, ExchangeTransportError
from exchanges.coinbase.auth import static_token_provider
from products.catalog import ProductCatalog
from strategies.passive_market_making import PASSIVE_MARKET_MAKING_STRATEGY_ID


class FakeProductCatalogClient:
    def __init__(self, catalog: ProductCatalog | Exception) -> None:
        self._catalog = catalog
        self.calls: list[dict[str, object]] = []

    def list_products(
        self,
        *,
        product_ids: tuple[str, ...] = (),
        get_tradability_status: bool = True,
    ) -> ProductCatalog:
        self.calls.append(
            {
                "get_tradability_status": get_tradability_status,
                "product_ids": product_ids,
            }
        )
        if isinstance(self._catalog, Exception):
            raise self._catalog
        return self._catalog


def test_product_catalog_smoke_appends_snapshot_without_runtime_or_order_tasks(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "catalog-smoke.jsonl"
    client = FakeProductCatalogClient(
        ProductCatalog.from_coinbase_payloads(
            [_product_payload("BIT-29MAY26-CDE", venue=ProductVenue.FCM)]
        )
    )
    config = _config(ledger_path)

    payload = product_catalog_smoke_payload(
        config,
        product_catalog_client=client,
        token_provider=static_token_provider("test-token"),
    )
    records = AuditLedger(ledger_path).iter_records()

    assert payload["status"] == ReadinessStatus.OK.value
    assert payload["configured_product_ids"] == ["BIT-29MAY26-CDE"]
    assert payload["missing_product_ids"] == []
    assert payload["order_endpoint_called"] is False
    assert payload["runtime_tasks_started"] is False
    assert payload["websocket_started"] is False
    assert payload["writes_ledger"] is True
    assert payload["policy_viability"]["status"] == ReadinessStatus.OK.value
    assert client.calls == [
        {"get_tradability_status": True, "product_ids": ("BIT-29MAY26-CDE",)}
    ]
    assert [record.event_type for record in records] == [EventType.EXCHANGE_PRODUCT_SNAPSHOT]
    assert records[0].payload["product_ids"] == ["BIT-29MAY26-CDE"]


def test_cli_product_catalog_smoke_can_fail_on_attention(workspace_tmp_path, capsys, monkeypatch):
    _clear_coinbase_env(monkeypatch)
    ledger_path = workspace_tmp_path / "catalog-smoke-attention.jsonl"
    config_path = workspace_tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "ledger_path": ledger_path.as_posix(),
                "bot": {
                    "rest": {"execution_mode": ExecutionMode.LIVE.value},
                    "product_catalog": {
                        "enabled": True,
                        "product_ids": ["BIT-29MAY26-CDE"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    client = FakeProductCatalogClient(
        ProductCatalog.from_coinbase_payloads(
            [
                _product_payload(
                    "BIT-29MAY26-CDE",
                    trading_disabled=True,
                    venue=ProductVenue.INTX,
                )
            ]
        )
    )
    monkeypatch.setenv(COINBASE_SDK_API_KEY_ENV, "organizations/org/apiKeys/key")
    monkeypatch.setenv(COINBASE_SDK_API_SECRET_ENV, "private-key")

    exit_code = asyncio.run(
        run_from_args(
            argparse.Namespace(
                config_file=str(config_path),
                ledger_path=None,
                max_cycles=99,
                product_catalog_smoke=True,
                product_catalog_smoke_fail_on_attention=True,
            ),
            product_catalog_client=client,
        )
    )
    payload = json.loads(capsys.readouterr().out)
    event_types = [record.event_type for record in AuditLedger(ledger_path).iter_records()]

    assert exit_code == ATTENTION_REQUIRED_EXIT_CODE
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["untradable_product_ids"] == ["BIT-29MAY26-CDE"]
    assert payload["unsupported_product_venues"] == [ProductVenue.INTX.value]
    assert event_types == [EventType.EXCHANGE_PRODUCT_SNAPSHOT]


def test_product_catalog_smoke_audits_lookup_errors(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "catalog-smoke-error.jsonl"
    client = FakeProductCatalogClient(ExchangeTransportError("catalog unavailable"))

    with pytest.raises(ExchangeTransportError, match="catalog unavailable"):
        product_catalog_smoke_payload(
            _config(ledger_path),
            product_catalog_client=client,
            token_provider=static_token_provider("test-token"),
        )

    records = AuditLedger(ledger_path).iter_records()

    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[-1].payload["stage"] == "product_catalog_smoke"
    assert records[-1].payload["retryable"] is True


def test_product_catalog_smoke_rejects_unresolved_placeholders_before_lookup(workspace_tmp_path):
    ledger_path = workspace_tmp_path / "catalog-smoke-placeholder.jsonl"
    config = _config(ledger_path, product_ids=("REPLACE_WITH_CFM_PRODUCT_ID",))
    client = FakeProductCatalogClient(ProductCatalog())

    with pytest.raises(ConfigError, match="placeholders"):
        product_catalog_smoke_payload(
            config,
            product_catalog_client=client,
            token_provider=static_token_provider("test-token"),
        )

    records = AuditLedger(ledger_path).iter_records()

    assert client.calls == []
    assert [record.event_type for record in records] == [EventType.ERROR]
    assert records[0].payload["placeholder_paths"] == ["$.bot.product_catalog.product_ids[0]"]


def test_product_catalog_smoke_reports_minimum_contract_notional_policy_breaches(
    workspace_tmp_path,
):
    ledger_path = workspace_tmp_path / "catalog-smoke-policy-breach.jsonl"
    client = FakeProductCatalogClient(
        ProductCatalog.from_coinbase_payloads(
            [
                _product_payload(
                    "SHB-26JUN26-CDE",
                    base_increment="1",
                    base_min_size="1",
                    contract_size="10000",
                    mid_market_price="0.00635",
                    venue=ProductVenue.FCM,
                )
            ]
        )
    )
    config = _config(
        ledger_path,
        product_ids=("SHB-26JUN26-CDE",),
        risk=RiskPolicyConfig(
            allowed_products=("SHB-26JUN26-CDE",),
            max_order_notional=Decimal("25"),
            max_visible_notional=Decimal("25"),
        ),
    )

    payload = product_catalog_smoke_payload(
        config,
        product_catalog_client=client,
        token_provider=static_token_provider("test-token"),
    )

    product_check = payload["policy_viability"]["product_checks"][0]
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert payload["policy_viability"]["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert product_check["product_id"] == "SHB-26JUN26-CDE"
    assert product_check["minimum_order_notional"] == "63.50000"
    assert product_check["notional_multiplier"] == "10000"
    assert product_check["reasons"] == [
        PolicyViabilityReason.MINIMUM_ORDER_NOTIONAL_EXCEEDS_MAX_ORDER_NOTIONAL.value,
        PolicyViabilityReason.MINIMUM_ORDER_NOTIONAL_EXCEEDS_MAX_VISIBLE_NOTIONAL.value,
    ]


def test_product_catalog_smoke_reports_passive_market_making_open_order_capacity(
    workspace_tmp_path,
):
    ledger_path = workspace_tmp_path / "catalog-smoke-pmm-capacity.jsonl"
    client = FakeProductCatalogClient(
        ProductCatalog.from_coinbase_payloads(
            [
                _product_payload("SHB-26JUN26-CDE", mid_market_price="0.00635"),
                _product_payload("AVA-29MAY26-CDE", mid_market_price="9.49"),
            ]
        )
    )
    config = _config(
        ledger_path,
        product_ids=("SHB-26JUN26-CDE", "AVA-29MAY26-CDE"),
        risk=RiskPolicyConfig(
            allowed_products=("SHB-26JUN26-CDE", "AVA-29MAY26-CDE"),
            allowed_sides=(OrderSide.BUY, OrderSide.SELL),
            max_open_orders=2,
        ),
        strategies=StrategyRuntimeConfig(
            strategy_ids=(PASSIVE_MARKET_MAKING_STRATEGY_ID,),
            strategy_parameters={
                PASSIVE_MARKET_MAKING_STRATEGY_ID: {
                    "max_products_per_evaluation": 2,
                    "max_staged_release_count_per_side": 1,
                }
            },
        ),
    )

    payload = product_catalog_smoke_payload(
        config,
        product_catalog_client=client,
        token_provider=static_token_provider("test-token"),
    )

    passive_market_making = payload["policy_viability"]["passive_market_making"]
    assert payload["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert passive_market_making["status"] == ReadinessStatus.ATTENTION_REQUIRED.value
    assert passive_market_making["expected_new_staged_order_count"] == 4
    assert passive_market_making["max_open_orders"] == 2
    assert passive_market_making["open_order_capacity_ok"] is False


def _config(
    ledger_path: Path,
    *,
    product_ids: tuple[str, ...] = ("BIT-29MAY26-CDE",),
    risk: RiskPolicyConfig | None = None,
    strategies: StrategyRuntimeConfig | None = None,
) -> CoinbaseApplicationConfig:
    return CoinbaseApplicationConfig(
        ledger_path=ledger_path,
        bot=CoinbaseBotConfig(
            rest=CoinbaseRestApiConfig(execution_mode=ExecutionMode.LIVE),
            product_catalog=ProductCatalogRuntimeConfig(
                schedule=TaskScheduleConfig(
                    task_id=RuntimeTask.PRODUCT_CATALOG_REFRESH,
                    interval=timedelta(hours=1),
                    enabled=True,
                ),
                product_ids=product_ids,
            ),
            risk=risk if risk is not None else RiskPolicyConfig(),
            strategies=strategies if strategies is not None else StrategyRuntimeConfig(),
        ),
    )


def _product_payload(
    product_id: str,
    *,
    base_increment: str = "1",
    base_min_size: str = "1",
    contract_size: str | None = None,
    mid_market_price: str | None = None,
    trading_disabled: bool = False,
    venue: ProductVenue = ProductVenue.FCM,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "base_increment": base_increment,
        "base_min_size": base_min_size,
        "product_id": product_id,
        "product_type": ProductType.FUTURE.value,
        "product_venue": venue.value,
        "trading_disabled": trading_disabled,
    }
    if contract_size is not None:
        payload["future_product_details"] = {"contract_size": contract_size}
    if mid_market_price is not None:
        payload["mid_market_price"] = mid_market_price
    return payload


def _clear_coinbase_env(monkeypatch) -> None:
    for key in list(os.environ):
        if (
            key.startswith("STATERAIL_")
            or key.startswith("COINBASE_BOT_")
            or key in {COINBASE_SDK_API_KEY_ENV, COINBASE_SDK_API_SECRET_ENV}
        ):
            monkeypatch.delenv(key, raising=False)
