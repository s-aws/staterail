# Extending The Project

Extensions should preserve replayability. A contributor should be able to delete all in-memory state, replay the ledger, and recover the same source-of-truth view.

## Rules For New Work

- Use enums from `core/enums.py` for public event names, statuses, rule names, and modes.
- Emit audit events before accepting irreversible decisions.
- Add projection support for new durable events.
- Add ledger health checks for invariants that would make replay untrustworthy.
- Keep one code path per behavior. Do not add a second store for state that belongs in the ledger.
- Keep credentials and secrets out of serialized config snapshots and audit payloads.
- Preserve thread-safety around existing locks.
- Add regression tests and run `pytest tests/regression/ -v`.

## Adding An Action

1. Add or reuse an `ActionType`.
2. Extend the intent/command boundary in `actions/gateway.py`.
3. Audit request, acceptance/rejection, execution start, execution result, and executor failures through the existing gateway path.
4. Validate executor result contracts before recording execution.
5. Update projection state and ledger health checks if the action creates durable state.

Do not let strategies call exchange clients directly. Strategies should create audited intents.

In this project, an intent is a typed proposed action before the action gateway accepts, rejects, previews, or executes it. An intent is not an exchange order. The gateway records `action.requested`, applies validation and risk checks, then records `action.accepted` or `action.rejected`; only an accepted action can proceed to executor result records.

## Adding A Strategy

Implement the `Strategy` protocol from `strategies` and return a `StrategyDecision`.
Use [Strategy Taxonomy](strategy-taxonomy.md) when comparing this project to Hummingbot, Freqtrade, LEAN, or other bot frameworks. External terms can inform names and scenario fixtures, but strategies in this repo must still return typed intents and route through the audited gateway.

```python
from actions.gateway import PlaceOrderIntent
from core.enums import OrderSide, OrderType
from strategies import StrategyDecision, StrategySnapshot, strategy_action_id, strategy_client_order_id


class ExampleStrategy:
    @property
    def strategy_id(self) -> str:
        return "example"

    def evaluate(self, snapshot: StrategySnapshot) -> StrategyDecision:
        del snapshot
        return StrategyDecision(
            intents=(
                PlaceOrderIntent(
                    action_id=strategy_action_id(
                        self.strategy_id,
                        "entry-order",
                        "BTC-USD",
                        OrderSide.BUY,
                        "50000",
                    ),
                    idempotency_key=strategy_client_order_id(
                        self.strategy_id,
                        "entry-order",
                        "BTC-USD",
                        OrderSide.BUY,
                        "50000",
                    ),
                    product_id="BTC-USD",
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    size="0.01",
                    limit_price="50000",
                ),
            )
        )
```

Register strategy objects when building the application, or expose them from an installed package entry point, and select them by ID in `bot.strategies.strategy_ids`. The harness overwrites `requested_by`, submits through the action gateway, and audits start/completion/failure records. Live strategy evaluation also requires `bot.strategies.allow_live_execution=true`; keep it false until dry-run behavior, sizing, and risk limits have been reviewed. Strategy code should not append to the ledger directly, call Coinbase clients directly, or maintain its own source-of-truth store.

Use the strategy wizard when starting an external package:

```powershell
python -m tools.strategy_wizard --name my-strategy --target ..\my-strategy --template metadata_only
python -m pip install -e ..\my-strategy
python -m pytest ..\my-strategy\tests -v
```

The available templates are `metadata_only`, `current_market_data`, `market_window_stats`, `noop`, and `custom`. All generated templates emit no order intents by default. The scaffold includes package metadata, a `staterail.strategies` entry point, `py.typed`, a dry-run config, a JSON scenario fixture, and tests. Treat the generated package as the extension boundary; add strategy behavior inside that package after scenario and simulation evidence exist.

The built-in `policy-probe` strategy is a safe wiring check. It emits no order intents and records metadata showing the active operator policy and available product metadata for policy-scoped products.

The built-in `staged-release-manager` strategy is not alpha logic. It consumes already-staged placements from the replayed projection and, when selected explicitly, emits at most one fresh, policy-scoped `release` intent per evaluation through the same gateway path as any other order. It stays inert when `staged_or_hidden_release.allow_release=false`. When `release_only_when_conditions_match=true`, release evaluation requires a fresh order book and skips staged limit orders that would cross the current book.

Followup-after-fill is available as a helper, not an autonomous strategy. `strategy_followup_after_fill_intent()` takes a replayed fill ID, verifies the filled order and logical order from the projection, applies the active operator policy and product metadata, and returns a deterministic opposite-side `PlaceOrderIntent` with `followup_after_fill` lineage. Strategy code still decides when that helper is appropriate.

The built-in `followup-on-fill-manager` wraps that helper for conservative order-management automation. It emits at most one followup per evaluation, only for replayed fills that can be traced to a logical order, and only when an operator policy enables followups and product catalog metadata is present.

Split order planning is available as a helper, not an autonomous strategy. `strategy_split_order_intents()` takes a replayed logical order ID, verifies that the source order is an unfilled cancelable live placement, applies product metadata and split-lineage policy, and returns an explicit cancel intent followed by deterministic same-side child `PlaceOrderIntent` values with `split_child` lineage. Strategy code still decides when a split is appropriate and must submit the returned intents in order.

The built-in `consolidation-manager` wraps `strategy_consolidation_intent()` for conservative order tidying. It looks for two unfilled live orders with the same product, side, and limit price, emits explicit cancel intents for those source orders, then emits one consolidation placement. It requires merge lineage to be enabled in the operator policy, product catalog metadata, and fresh order-book input when the policy requires it.

The built-in `anchor-repricing-manager` is conservative order-movement infrastructure. It looks for one unfilled same-side live order whose limit price is outside the configured anchor band, emits an explicit cancel intent, then emits a `cancel_replace` placement under the same logical order ID. It requires anchor repricing, same-side moves, product catalog metadata, fresh order-book midpoint input, cooldown compliance, and cancel-replace fallback.

The built-in `passive-market-making` strategy is the first conservative strategy template. It uses a fresh replayed order-book midpoint, stages one bid and one ask by default, and emits staged placements only. Release is intentionally separate, so strategy tests can prove hidden/staged quote creation without also enabling visible order submission. Operator policies can set `staged_or_hidden_release.allow_release=false` when staged placements are allowed but release placement should remain blocked.

Passive quote state is replay-visible. Staged placements that carry `passive_market_making` metadata appear in the source-of-truth projection as `passive_market_making_quotes`, and ledger health summarizes released versus unreleased passive quotes under the order-lineage contract. Ledger health also validates the passive quote metadata against the placement product, side, bid/mid/ask ordering, positive spread, and side-specific limit price. Extension code should consume that projection view instead of rescanning raw audit records.

Read market data through the replayed projection on the snapshot:

```python
ticker = snapshot.projection.latest_ticker("BTC-USD")
book = snapshot.projection.order_book("BTC-USD")
trades = snapshot.projection.market_trades_for_product("BTC-USD")
```

These accessors are rebuilt from accepted non-duplicate ledger records. Strategy code should not parse raw websocket payloads unless it is adding a new projection field with tests.

When `bot.strategies.operator_policy_file` or inline `operator_policy` is configured, the loaded policy is available as `snapshot.operator_policy` during scheduled evaluation, simulation, and scenario runs. Treat it as read-only strategy input. Gateway risk enforcement remains authoritative.

Check required input freshness before emitting intents:

```python
from datetime import timedelta

from core.enums import MarketDataKind
from strategies import StrategyDecision

freshness = snapshot.market_data_freshness(
    data_kind=MarketDataKind.TICKER,
    max_age=timedelta(seconds=5),
    product_id="BTC-USD",
)
if not freshness.is_ok:
    return StrategyDecision(metadata={"market_data_freshness": freshness.to_payload()})
```

Freshness uses the ledger-observed timestamp for the accepted market-data record. That is the right clock for replayability; exchange timestamp strings remain available on projection snapshots for strategy-specific checks.

Operators can enforce the same check outside strategy code with `bot.strategies.market_data_requirements`. If a configured requirement is missing or stale, scheduled evaluation and simulation fail before `evaluate()` is called and before an action can reach the gateway. Use this for inputs that every selected strategy must have.

External packages can expose strategies without dynamic import paths in bot config:

```toml
[project.entry-points."staterail.strategies"]
example = "my_strategy_package:build_example_strategy"
```

The entry point name must match the returned strategy's `strategy_id`. The loaded object can be a strategy instance, a no-argument factory that returns one, or a factory that opts into config parameters with a `parameters` keyword argument. Parameterized factories receive the selected `bot.strategies.strategy_parameters[strategy_id]` mapping. If parameters are configured for an entry point that returns a prebuilt instance or a no-argument factory, startup fails instead of silently ignoring them.

The package publishes PEP 561 `py.typed` markers. External strategy packages should type-check against the public `strategies`, `actions`, `core.enums`, and projection interfaces instead of copying payload dictionaries.

## Adding Strategy-Facing Abstractions

Strategy authors should write against StateRail concepts. Venue adapters should translate venues into StateRail concepts. The ledger remains the source of truth between them.

Use [Roadmap](roadmap.md) to understand the planned helper surface and venue-adapter boundary. Reusable helpers should be added when they strengthen the framework contract, not because one strategy implementation happens to need a private shortcut.

Use this ownership model when adding an abstraction for strategy authors:

- Venue-specific parsing and normalization belongs in `exchanges/<venue>/`, feed adapters, REST clients, and projection replay code.
- Durable canonical data belongs in audited events and replayed projection snapshots under `projections/state.py`.
- Strategy-facing convenience belongs on `StrategySnapshot` or in small helper modules under `strategies/` when the helper is reusable across strategies.
- Product metadata, venue capability, sizing, and notional rules should use `products/`, `orders/`, and operator-policy helpers rather than raw venue fields.
- Strategy packages should consume those helpers and emit intents; they should not parse raw websocket payloads, call exchange clients, or maintain a parallel market-data cache.

For example, a rolling volume helper should read replayed `MarketTradeSnapshot` values from the projection and return a StateRail result such as base volume, quote notional, count, and source sequences for a lookback window. The helper should not expose Coinbase message shapes to strategy code.

Current strategy-facing helpers include:

```python
from datetime import timedelta
from core.enums import IncrementRoundingMode, OrderSide

top = snapshot.best_bid_ask("AVA-29MAY26-CDE")
midpoint = snapshot.midpoint("AVA-29MAY26-CDE")
spread = snapshot.spread("AVA-29MAY26-CDE")
book = snapshot.order_book_stats("AVA-29MAY26-CDE", levels=5)
book_window = snapshot.order_book_sample_window(
    "AVA-29MAY26-CDE",
    lookback=timedelta(minutes=5),
)
book_window_stats = snapshot.order_book_window_stats(
    "AVA-29MAY26-CDE",
    levels=5,
    lookback=timedelta(minutes=5),
)
latest_trade = snapshot.latest_trade("AVA-29MAY26-CDE")
series_window = snapshot.market_series_window(
    "AVA-29MAY26-CDE",
    lookback=timedelta(minutes=5),
)
window = snapshot.trade_window("AVA-29MAY26-CDE", lookback=timedelta(minutes=5))
retained_window = snapshot.trade_window(
    "AVA-29MAY26-CDE",
    lookback=timedelta(minutes=5),
    max_retained_trades=500,
)
stats = snapshot.market_window_stats("AVA-29MAY26-CDE", lookback=timedelta(minutes=5))
candles = snapshot.candles(
    "AVA-29MAY26-CDE",
    interval=timedelta(minutes=1),
    lookback=timedelta(minutes=15),
)
volume = snapshot.rolling_trade_volume("AVA-29MAY26-CDE", lookback=timedelta(minutes=5))
count = snapshot.rolling_trade_count("AVA-29MAY26-CDE", lookback=timedelta(minutes=5))
orders = snapshot.open_orders(product_id="AVA-29MAY26-CDE")
product = snapshot.product_rules("AVA-29MAY26-CDE")
venue_capabilities = snapshot.venue_capabilities("coinbase_cfm")
product_capabilities = snapshot.product_capabilities("AVA-29MAY26-CDE")
notional = snapshot.notional(product_id="AVA-29MAY26-CDE", size="1", price="100")
size_check = snapshot.validate_order_size(product_id="AVA-29MAY26-CDE", size="1")
price_check = snapshot.validate_limit_price(product_id="AVA-29MAY26-CDE", price="100")
notional_check = snapshot.validate_notional(product_id="AVA-29MAY26-CDE", size="1", price="100")
price = snapshot.price_tick_proposal(
    product_id="AVA-29MAY26-CDE",
    price="100.003",
    mode=IncrementRoundingMode.DOWN,
)
exposure = snapshot.product_exposure("AVA-29MAY26-CDE")
capacity = snapshot.order_capacity("AVA-29MAY26-CDE", side=OrderSide.BUY)
```

Market-data helpers return typed StateRail result objects with `status`, `is_ok`, replay sequence fields, observed timestamps where available, and `to_payload()` for strategy metadata. `market_series_window()` exposes the normalized time contract used by replay-derived trade helpers: product, `as_of`, `window_start`, `window_end`, `lookback`, `time_field`, `membership_rule`, and optional retention limit. `trade_window()` is the canonical replay-derived accepted-trade window; market-window stats, candles, rolling count, and rolling volume helpers share that same window-selection path. Time `lookback` defines the window. `max_retained_trades` is an optional helper-level retention cap applied after time filtering, not a replacement for the time window. Capped results report `retention_limit` and `retention_dropped_trade_count`. Operators can also configure `bot.strategies.max_market_trades_per_product` to cap accepted trades retained per product during strategy snapshot replay; the replayed projection reports dropped-trade counts in `market_trade_retention`. `order_book_stats()` reads the latest replayed book snapshot and requires one explicit depth filter: `levels=...` or `max_distance_bps=...`. `order_book_sample_window()` reads retained replay-backed book samples and reports `insufficient_data` unless at least `min_samples` samples exist inside the requested time window. `order_book_window_stats()` uses the same sample window and depth filter to summarize spread, midpoint, bid/ask volume, and book imbalance over time. The default snapshot replay cap keeps only one book sample per product; configure `bot.strategies.max_order_book_samples_per_product` above `1` before using historical book-sample windows. Configure `bot.strategies.order_book_sample_product_ids` when only selected products should retain historical book samples; latest-book state remains available for other products, and skipped sample counts are reported in projection retention metadata. Configure `bot.strategies.max_order_book_sample_depth_per_side` when retained samples should keep only top-of-book depth; the latest-book projection remains full-depth. Strategy logic that requires complete 5-minute history should not use caps, or should reject results/snapshots where retained trades or book samples were dropped or skipped by scope. Candle buckets are aligned to the requested replay window from `evaluated_at - lookback` forward by fixed intervals; empty buckets are returned explicitly as `missing`, and bucket membership rules are reported in candle payloads. These are projection-backed strategy helpers, not a standalone historical market-data storage or backtesting subsystem. A strategy should return metadata first while validating a new signal; it should emit order intents only after scenario and simulation coverage prove the helper behavior.

Product-rule validation helpers report typed failures and never round. Increment proposal helpers are explicitly named as proposals and require an `IncrementRoundingMode`, so strategy authors can separate "is this valid?" from "what nearby valid value could I use?"

Venue and product capability helpers report the current StateRail adapter contract, not every feature a venue may expose. Coinbase spot (`CBE`) and Coinbase Financial Markets futures (`FCM`) are live-executable through the current adapter. Capability payloads also expose read-side support such as product metadata lookup, market-data websocket support, user-order websocket support, order lookup, fill lookup, account lookup, and CFM position lookup. Coinbase INTX metadata can be replayed, but live INTX order routing is disabled. Amend, reduce-only forwarding, and attached orders are reported as unsupported until this adapter implements them.

Exposure and capacity helpers summarize replayed open orders, replayed fill-derived position exposure, daily notional usage, remaining open-order slots, and operator-policy capacity. They are advisory only; the action gateway and risk gate remain authoritative for order admission.

Action IDs and idempotency keys must be deterministic and unique for the intended decision. Use `strategy_action_id()` for the internal audited action ID and `strategy_client_order_id()` when you want a separate venue client order ID. Reusing an action ID on a later evaluation is rejected by replayed gateway state, so include enough semantic parts to identify the intended action and include an explicit generation when repeating a similar action is intentional.

The harness validates every returned decision as a set before submitting any action. Duplicate `action_id` values, or duplicate place-order client identities from `idempotency_key`/`action_id`, fail the strategy evaluation as a contract error before the gateway sees an intent. This is intentional: a bad decision should not partially execute.

Before enabling scheduled evaluation, run strategy simulation against a verified ledger:

```powershell
python -m app.main --config-file config.local.json --strategy-simulate --strategy-simulate-fail-on-attention
```

Simulation uses the replayed source-of-truth projection and the same gateway validation/risk checks, but it does not append audit records to the operator ledger, execute live orders, or call Coinbase. Ordered decisions are previewed against a temporary replay ledger, so later previews can see earlier accepted dry-run cancels or placements. The fail-on-attention flag makes contract failures and rejected action previews fail the process. A strategy that passes simulation can still be wrong economically; simulation only proves the contract and current risk/validation outcome.

Before enabling a live scheduled strategy, record qualification evidence from the same simulation path:

```powershell
python -m app.main --config-file config.local.json --strategy-simulate --strategy-simulate-record-result --strategy-simulate-fail-on-attention
```

This appends `runtime.strategy_simulation_result`. Live runtime startup requires a clean matching result when `bot.strategies.enabled=true` under live REST execution.

Use the scenario harness for strategy regression tests. A scenario writes a temporary hash-chained ledger from typed events, replays it, runs strategy simulation, and compares expected previews. Prefer the fixture builders for common setup instead of hand-writing venue feed payloads:

```python
from core.enums import ActionStatus, StrategySimulationStatus
from strategies import (
    StrategyScenario,
    StrategyScenarioExpectedActionPreview,
    StrategyScenarioExpectations,
    run_strategy_scenario,
    scenario_order_book,
)

scenario = StrategyScenario(
    name="entry-after-book",
    events=scenario_order_book(
        product_id="BTC-USD",
        bid="49999",
        ask="50001",
    ),
    expectations=StrategyScenarioExpectations(
        action_previews=(
            StrategyScenarioExpectedActionPreview(
                action_id="expected-action-id",
                command_payload={"product_id": "BTC-USD"},
                status=ActionStatus.ACCEPTED,
                strategy_id="example",
            ),
        ),
        completed_count=1,
        failed_count=0,
        status=StrategySimulationStatus.OK,
    ),
)

result = run_strategy_scenario(
    ledger_path=tmp_path / "scenario.jsonl",
    scenario=scenario,
    strategies=(ExampleStrategy(),),
)
assert result.passed
```

Available Python builders cover product metadata snapshots, order-book snapshots, trades, accepted staged orders, fill confirmations, and open-order imports. Builders that emit accepted feed data return a `DATA_RECEIVED`/`DATA_ACCEPTED` pair. When combining several builder outputs in one manually ordered scenario, set each builder's `received_sequence` to the sequence number where its `DATA_RECEIVED` event will land.

For strategy-package tests that assert diagnostic metadata from simulation output, use the public assertion helpers instead of duplicating payload traversal:

```python
from core.enums import StrategyMarketDataStatus
from strategies import assert_strategy_metadata_contains, assert_strategy_metadata_path

assert_strategy_metadata_contains(
    report,
    "example",
    {"book": {"status": StrategyMarketDataStatus.STALE.value}},
)
assert_strategy_metadata_path(
    report,
    "example",
    ("book", "depth", "status"),
    StrategyMarketDataStatus.INSUFFICIENT_DATA.value,
)
```

The same harness can load versioned JSON fixtures:

```json
{
  "schema_version": 1,
  "name": "noop-empty-ledger",
  "execution_mode": "dry_run",
  "events": [],
  "expectations": {
    "status": "ok",
    "accepted_action_count": 0,
    "rejected_action_count": 0,
    "completed_count": 1,
    "failed_count": 0,
    "intent_count": 0,
    "action_previews": []
  }
}
```

Run fixture scenarios from PowerShell with configured strategy IDs:

```powershell
python -m app.main --config-file docs\examples\config.dry-run.json --ledger-path test_runtime\scenario-smoke.jsonl --strategy-scenario-file docs\examples\strategy-scenario.noop.json
python -m app.main --config-file docs\examples\config.dry-run.json --ledger-path test_runtime\staged-order-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.staged-order.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\staged-release-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.staged-release-manager.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\followup-on-fill-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.followup-on-fill-manager.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\consolidation-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.consolidation-manager.json
python -m app.main --config-file docs\examples\config.anchor-repricing-manager.dry-run.json --ledger-path test_runtime\anchor-repricing-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.anchor-repricing-manager.json
python -m app.main --config-file docs\examples\config.passive-market-making.dry-run.json --ledger-path test_runtime\passive-market-making-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.passive-market-making.json
```

JSON scenarios may also declare `strategy_ids` and `static_strategies`. Use that for fixture-only order previews when you need to exercise gateway/risk behavior without packaging a strategy plugin. Expected action previews can include a `command_payload` object; it is matched as a subset against the previewed command payload so fixtures can assert behavior-critical fields without duplicating the full command.

Scenario ledgers must be empty before the run. Keep them in test/runtime directories, not in production `data/` paths.

## Adding Operator Policies

Operator policies capture execution constraints separately from strategy code. Use `strategies.operator_policy` to load a JSON policy, derive the risk-policy fragment, and derive strategy market-data requirements.

Examples:

- `docs/examples/operator-policy.conservative-cfm-v0.json`
- `docs/examples/operator-policy.stealth-orders-manager-v1.json`
- `docs/examples/operator-scenarios.conservative-cfm-v0.json`

Use `strategies.operator_scenarios` or `--operator-policy-scenarios-file` to keep policy examples executable. The checked runner covers deterministic safety, stale-input, lineage-sizing, consolidation, anchor repricing, tranche, adaptive-sizing, and slide-mode examples. It reports not-yet-implemented automatic order-creation ideas as `documented_only` until a concrete strategy and risk contract exist.

Set `bot.strategies.operator_policy_file` to a checked JSON policy, or use `operator_policy` inline in typed config. The effective action gateway risk policy is the stricter merge of `bot.risk` and the operator policy, so strategy simulation, scheduled strategy runs, and direct gateway submissions use the same auditable accept/reject path.

Policy fields enforced by the action gateway include allowed products, allowed order types, allowed sides, allowed time-in-force values, allowed lineage relations, allowed placement kinds, max order notional, daily notional caps, max open orders, visible-notional staged release limits, max replacement count per logical order, post-only mode, reduce-only mode, and the kill switch. Operator-policy order-book freshness requirements are merged into the scheduled strategy input gate. Product catalog requirements and redundant-feed minimums are emitted in the policy runtime fragment for operator config review; keep the actual runtime config explicit. For futures products, product metadata contract size is part of notional math, so strategy extensions should use product-catalog sizing helpers instead of spot-style `size * price` calculations.

Anchor repricing, tranche/adaptive sizing, and hotpoint replication are still strategy behavior. Use `strategies.policy_calculations` for deterministic policy math such as visible notional, tranche release sizes, adaptive reveal sizing, slide repricing, and anchor-band clamping. Use `StrategySnapshot.plan_staged_release_sizes()` when a strategy needs visible chunk planning from the active operator policy and product catalog. Keep those as explicit strategy decisions that emit gateway intents; do not let strategy code bypass the action gateway or silently mutate rejected policy decisions.

## Adding A Feed Source

Feed sources should normalize messages into `FeedMessage` values and pass through `RedundantFeedRouter`.

Required behavior:

- stable source IDs
- source-independent duplicate keys where possible
- heartbeat/liveness support
- malformed-message failures as typed feed errors
- replay-safe duplicate suppression

## Adding Triggers

Static trigger rules are configured in `bot.triggers`. Runtime polling is controlled by `bot.trigger_polling`.

When adding trigger behavior, keep firing auditable through `trigger.fired` and replay-seed one-shot suppression so restart does not refire non-repeatable rules.

## Adding Order Lineage Behavior

Use the existing logical order and placement records:

- `LogicalOrderRecord` for internal order identity and relationships.
- `OrderPlacementRecord` for venue, staged, and release placement attempts.
- `LineageSizingPolicy` for strict product-metadata-backed sizing decisions, including staged-release chunk planning under visible-notional caps.
- `StrategySnapshot.plan_staged_release_sizes()` for policy/catalog-backed staged-release sizing inside strategies.
- `StrategySnapshot.quote_pair_intents()` for deterministic bid/ask quote intents from replayed midpoint, product rules, and operator policy.
- `StrategySnapshot.ladder_plan()` for deterministic product-rule-checked ladder planning without emitting intents.
- `StrategySnapshot.scheduled_slice_plan()` for replay-safe TWAP/DCA-style next-slice planning without emitting intents.
- `strategy_staged_release_intents()` to convert accepted staged-release sizing decisions into deterministic staged `PlaceOrderIntent` chunks.
- `strategy_release_staged_placement_intent()` to convert a replayed staged placement into a deterministic live `release` intent through the normal gateway path.
- `strategy_followup_after_fill_intent()` to convert a replayed fill into a deterministic opposite-side followup intent.
- `strategy_split_order_intents()` to convert one unfilled live logical order into an explicit cancel intent plus deterministic same-side split-child placements.
- `strategy_consolidation_intent()` to convert two or more same-product, same-side logical orders into one deterministic consolidation placement.
- `ConsolidationManagerStrategy` for conservative same-price, same-side order tidying through explicit cancel-then-place intents.
- `PassiveMarketMakingStrategy` for conservative midpoint-based staged bid/ask quote creation.
- `PlaceOrderIntent.with_sizing_decision()` to carry accepted sizing into a gateway intent.
- `PlaceOrderIntent.as_staged_release()` for not-yet-submitted placement records.

Manual association requires explicit operator approval metadata. Consolidation helpers intentionally do not cancel source orders; managers that automate tidying must emit explicit ordered cancel and placement intents so simulation and runtime can evaluate the sequence through the gateway. Split helpers intentionally do return the cancel-plus-child placement sequence because splitting one active order without canceling the source would increase exposure. Hidden, reserve, or iceberg-style execution policy should be added later as strategy/execution policy that consumes staged placements and release-size plans, then emits explicit release intents through the action gateway.

## Adding A Venue Or Exchange

New venues should be added behind typed adapters:

- REST executor or lookup clients should return typed result objects and normalize retryable failures.
- Websocket adapters should normalize to `FeedMessage`.
- Product metadata should normalize into `ProductMetadata`.
- Live execution should have readiness checks, risk controls, venue-scope health checks, and tests before any HTTP order submission can happen.

Current live routing is Coinbase spot and CFM futures. INTX should remain disabled for live routing until separate tests and eligibility assumptions are documented.

Use `venue_contract_report()` before treating a venue as live-capable. The report derives from the existing `VenueCapabilities` source and checks explicit `VenueCapabilityRequirement` values, so future adapters have one capability contract instead of scattered boolean checks. The default live-routing requirements cover metadata lookup, market-data websocket, user-order websocket, live execution, limit order support, post-only support, good-til-cancelled time-in-force support, place/cancel order support, and order/fill/account lookup. CFM-style futures can use `CFM_LIVE_ORDER_ROUTING_REQUIREMENTS` when position lookup is also required.

### Venue Package Boundary

Keep Coinbase in this repository while it is still the first-party reference implementation for venue behavior. The integration should continue to be structured internally as if it could move to an external package later, but it should not be split into a separate repository until the venue/provider contracts are stable.

The intended long-term structure is:

- `staterail`: core event-sourced execution framework, provider interfaces, action gateway, risk/policy contracts, replay/source-of-truth projection, strategy harness, and venue conformance tests.
- `staterail-coinbase`: Coinbase Advanced Trade spot, Coinbase CFM futures, Coinbase auth, REST, websocket, and product catalog adapter.
- Shared conformance tests, either inside `staterail` or a later `staterail-venue-testkit`, that every venue adapter must pass before live execution is supported.

Do not let venue-specific code leak into core contracts. Core modules should depend on typed venue-agnostic interfaces and enums; Coinbase modules may adapt Coinbase-specific payloads into those contracts. A later split should be a packaging move, not a behavior rewrite.

## Adding Ledger Health Checks

Health checks should be deterministic from verified records. Prefer replayed projection state where possible, and raw record scanning only when sequence-level contract validation is required.

A health check should report:

- typed check name
- `ok` or `attention_required`
- count of anomalies
- details that identify action IDs, event types, sequences, and relevant observed values

## Testing Expectations

For every new durable behavior, add tests for:

- valid path
- invalid or corrupt replay path
- restart/replay behavior when applicable
- no duplicate emissions after restart when suppression is expected
- CLI/readiness/health behavior when operator-facing output changes
