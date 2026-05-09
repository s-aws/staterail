from __future__ import annotations

from collections.abc import Iterable

from actions.execution import ExecutionResult
from actions.gateway import ActionCommand, ActionExecutor
from core.enums import ActionType, ErrorCategory, ErrorCode, ExecutionMode, ExecutionStatus, ProductVenue
from products.catalog import ProductCatalog


class ProductVenueRestrictedExecutor:
    def __init__(
        self,
        executor: ActionExecutor,
        *,
        allowed_venues: Iterable[ProductVenue],
        mode: ExecutionMode,
        product_catalog: ProductCatalog,
    ) -> None:
        self._allowed_venues = tuple(allowed_venues)
        if not self._allowed_venues:
            raise ValueError("allowed_venues must not be empty")
        for venue in self._allowed_venues:
            if not isinstance(venue, ProductVenue):
                raise TypeError("allowed_venues must contain ProductVenue values")
        if not isinstance(mode, ExecutionMode):
            raise TypeError("mode must be an ExecutionMode")
        if not isinstance(product_catalog, ProductCatalog):
            raise TypeError("product_catalog must be a ProductCatalog")

        self._executor = executor
        self._mode = mode
        self._product_catalog = product_catalog

    def execute(self, command: ActionCommand) -> ExecutionResult:
        if command.action_type != ActionType.PLACE_ORDER:
            return self._executor.execute(command)

        product_id = command.payload.get("product_id")
        if not isinstance(product_id, str) or not product_id:
            return self._rejected(
                command,
                error_code=ErrorCode.PRODUCT_ID_MISSING,
                error_message="product_id is required before venue restriction checks",
                product_id=None,
                product_venue=None,
            )

        product = self._product_catalog.get(product_id)
        if product is None:
            return self._rejected(
                command,
                error_code=ErrorCode.PRODUCT_METADATA_MISSING,
                error_message="product metadata is required before live execution",
                product_id=product_id,
                product_venue=None,
            )

        if product.product_venue not in self._allowed_venues:
            return self._rejected(
                command,
                error_code=ErrorCode.UNSUPPORTED_PRODUCT_VENUE,
                error_message=f"live execution is not enabled for product venue {product.product_venue.value}",
                product_id=product_id,
                product_venue=product.product_venue,
            )

        return self._executor.execute(command)

    def _rejected(
        self,
        command: ActionCommand,
        *,
        error_code: ErrorCode,
        error_message: str,
        product_id: str | None,
        product_venue: ProductVenue | None,
    ) -> ExecutionResult:
        return ExecutionResult(
            action_id=command.action_id,
            action_type=command.action_type,
            status=ExecutionStatus.REJECTED,
            mode=self._mode,
            error_category=ErrorCategory.ACTION_EXECUTOR,
            error_code=error_code,
            error_message=error_message,
            raw_response={
                "allowed_product_venues": [venue.value for venue in self._allowed_venues],
                "product_id": product_id,
                "product_venue": product_venue.value if product_venue is not None else None,
            },
        )
