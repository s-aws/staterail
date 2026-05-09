from __future__ import annotations

from core.json_tools import JsonValue, normalize_json
from strategies.harness import StrategyDecision, StrategySnapshot


POLICY_PROBE_STRATEGY_ID = "policy-probe"


class PolicyProbeStrategy:
    @property
    def strategy_id(self) -> str:
        return POLICY_PROBE_STRATEGY_ID

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        if not isinstance(snapshot, StrategySnapshot):
            raise TypeError("snapshot must be a StrategySnapshot")
        return StrategyDecision(metadata=_probe_metadata(snapshot))


def _probe_metadata(snapshot: StrategySnapshot) -> dict[str, JsonValue]:
    policy = snapshot.operator_policy
    product_ids = policy.scope.products if policy is not None else ()
    payload = {
        "operator_policy": (
            {
                "live_orders_allowed": policy.scope.live_orders_allowed,
                "max_daily_notional_usd": str(policy.risk_limits.max_daily_notional_usd),
                "max_open_orders": policy.risk_limits.max_open_orders,
                "max_order_notional_usd": str(policy.risk_limits.max_order_notional_usd),
                "policy_name": policy.policy_name,
                "products": list(policy.scope.products),
                "venue": policy.scope.venue.value,
            }
            if policy is not None
            else None
        ),
        "operator_policy_configured": policy is not None,
        "policy_product_metadata": [
            _product_metadata_payload(snapshot, product_id) for product_id in product_ids
        ],
        "product_catalog_configured": snapshot.product_catalog is not None,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("policy probe metadata must normalize to an object")
    return normalized


def _product_metadata_payload(snapshot: StrategySnapshot, product_id: str) -> dict[str, JsonValue]:
    product = snapshot.product_catalog.get(product_id) if snapshot.product_catalog is not None else None
    payload = {
        "present": product is not None,
        "product_id": product_id,
        "product_type": product.product_type.value if product is not None else None,
        "product_venue": product.product_venue.value if product is not None else None,
        "tradable_for_new_orders": product.tradable_for_new_orders if product is not None else None,
    }
    normalized = normalize_json(payload)
    if not isinstance(normalized, dict):
        raise TypeError("policy probe product metadata must normalize to an object")
    return normalized
