from __future__ import annotations

from typing import Protocol

from core.clock import Clock, SystemClock
from core.engine import AuditCore
from core.enums import EventType
from core.json_tools import JsonValue, normalize_json
from products.catalog import ProductCatalog


class ProductCatalogLookup(Protocol):
    def list_products(
        self,
        *,
        product_ids: tuple[str, ...] = (),
        get_tradability_status: bool = True,
    ) -> ProductCatalog:
        ...


class ProductCatalogRefreshTask:
    def __init__(
        self,
        core: AuditCore,
        *,
        lookup_client: ProductCatalogLookup,
        product_catalog: ProductCatalog,
        product_ids: tuple[str, ...] = (),
        clock: Clock | None = None,
    ) -> None:
        self._core = core
        self._lookup_client = lookup_client
        self._product_catalog = product_catalog
        self._product_ids = product_ids
        self._clock = clock or SystemClock()

    def refresh(self) -> dict[str, JsonValue]:
        fetched_catalog = self._lookup_client.list_products(
            product_ids=self._product_ids,
            get_tradability_status=True,
        )
        products = tuple(sorted(fetched_catalog.values(), key=lambda product: product.product_id))
        self._product_catalog.update(products)
        product_ids = [product.product_id for product in products]
        record = self._core.emit(
            EventType.EXCHANGE_PRODUCT_SNAPSHOT,
            {
                "configured_product_ids": list(self._product_ids),
                "product_count": len(products),
                "product_ids": product_ids,
                "products": [product.to_payload() for product in products],
                "refreshed_at": self._clock.now(),
            },
        )
        result = {
            "product_count": len(products),
            "product_ids": product_ids,
            "snapshot_sequence": record.sequence,
        }
        normalized = normalize_json(result)
        if not isinstance(normalized, dict):
            raise TypeError("Product catalog refresh result must normalize to an object")
        return normalized
