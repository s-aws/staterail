from __future__ import annotations

from products.catalog import ProductCatalog, ProductMetadata
from projections.state import SourceOfTruthProjection


def product_catalog_from_projection(projection: SourceOfTruthProjection) -> ProductCatalog:
    return ProductCatalog(
        ProductMetadata.from_audit_payload(snapshot.payload)
        for snapshot in projection.exchange_products_by_product_id.values()
    )
