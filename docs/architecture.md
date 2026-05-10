# Architecture

StateRail is event-sourced execution infrastructure built around a durable audit ledger. Runtime behavior should be recoverable from ledger replay, and new components should extend audited boundaries instead of creating side channels.

Coinbase Advanced Trade spot and Coinbase Financial Markets (CFM) futures are the first supported venue feature. Venue-specific code should remain behind explicit adapter, metadata, readiness, and risk boundaries.

For a compact component handoff map, see [System diagram](system-diagram.md).

## Core Principles

- The audit ledger is the source of truth for accepted inputs, actions, execution results, reconciliation findings, checkpoints, anchors, archives, and errors.
- Every behavior should have one code path. If a new feature needs a new event, projection, or health check, add it to the existing path instead of building a parallel store.
- Public boundaries use enums from `core/enums.py`, not magic strings.
- Runtime decisions should be replayable from immutable records. In-memory state is cache, not authority.
- Unexpected behavior is recorded as an `error` event with a typed category and code where possible.
- Strategy authors should write against StateRail concepts. Venue adapters should translate venues into StateRail concepts. The ledger remains the source of truth between them.

## Runtime Shape

1. Configuration is loaded from JSON, environment variables, or defaults, then converted to typed config objects.
2. Runtime startup records `system.started` with a normalized config snapshot and deterministic fingerprint.
3. The `AuditCore` appends records to the hash-chained JSONL ledger.
4. Hooks can observe append lifecycle events, but hook failures are isolated and audited.
5. Runtime tasks, feeds, action submission, exchange lookups, and reconciliation append events through the same core.
6. The source-of-truth projection rebuilds derived state strictly from ledger replay.
7. Ledger health verifies the hash chain, replays state, and reports contract mismatches.

## Ledger And Replay

The local ledger is append-only JSONL with hash chaining and append-boundary verification. Before appending, the ledger is verified so tampering, truncation, or missing terminal newlines are detected before new records are added.

The projection in `projections/state.py` is the replay model for derived state. It tracks action lifecycle, order state, logical order lineage, feed/data status, triggers, reconciliation, product metadata, checkpoints, anchors, archives, and errors.

Local files are tamper-evident, not administrator-proof immutable storage. Production immutability is provided by checkpoint anchors and full-ledger archives written to storage with retention controls, currently AWS S3 Object Lock.

## Actions And Orders

The action gateway records order intent before acceptance or rejection. Accepted actions may then execute through an executor. Execution results are audited and validated before placement records are emitted.

Order lineage separates internal logical order identity from venue placement attempts:

- Logical order records track parent/followup/split/consolidation/manual association relationships.
- Placement records track staged placements, release attempts, submitted/accepted venue attempts, cancel-replace, amend, and external imports.
- Staged placements do not execute through the executor path. A later `release` placement is a separate action that must pass the same gateway validation, risk checks, executor contract, and projection replay as any visible order.
- Sizing decisions are strict: they accept or reject based on product metadata and configured thresholds. They do not silently round or mutate exposure.
- Staged-release sizing plans visible chunks under a configured visible-notional cap and product metadata rules. It does not submit orders by itself; strategy code must still emit typed intents through the gateway.

Hidden, reserve, or iceberg-style execution policy is not part of the current live execution scope. The current staged placement, visible-chunk planning, and lineage model are designed so that a later execution policy can be added without changing the core ledger contract.

## Strategies

Strategies are scheduled runtime extensions, not exchange clients. A strategy receives a replayed `StrategySnapshot` containing the current source-of-truth projection, product catalog, ledger path, evaluation time, and execution mode. It returns a `StrategyDecision` with typed intents such as `PlaceOrderIntent` or `CancelOrderIntent`.

Strategy-facing abstractions should be framework concepts, not Coinbase payload concepts. Raw venue payload parsing belongs in venue adapters and projection replay. Reusable strategy helpers should live on `StrategySnapshot` or in small strategy-facing modules under `strategies/`, and they should derive their answers from the replayed projection, product catalog, and operator policy. For example, `trade_window()` exposes accepted market trades retained in replayed state, market-window stats, candles, rolling count, and rolling volume helpers share that same bounded-window path, `order_book_stats()` exposes latest-book depth metrics from the replayed order-book snapshot, and `order_book_sample_window()` plus `order_book_window_stats()` expose retained replay-backed book samples only when enough samples exist for a real window; strategy code should not parse raw Coinbase websocket messages to calculate it. Time `lookback` defines the market-data window. Optional helper-level retained-trade caps only limit how many trades are retained after time filtering, and capped results report `retention_limit` and `retention_dropped_trade_count`. Operators can also cap accepted trades, order-book sample count, retained sample depth, and historical book-sample product scope during strategy snapshot replay; the projection reports dropped and scope-skipped counts in `market_trade_retention` and `market_order_book_sample_retention`. Strategy logic that requires a complete time window should not use retained-data caps, or should reject capped/skipped results and snapshots where retained data was dropped. These helpers are replay-derived projection helpers, not a standalone historical market-data storage or backtesting subsystem.

`StrategySnapshot` exposes product-rule validation, increment proposal, and capability helpers so strategy authors can check size, price, notional, nearby tick/increment values, venue support, and product support without reimplementing product metadata rules. Validation helpers report typed failures and do not round. Proposal helpers require an explicit `IncrementRoundingMode` and return proposed values only; they do not imply an order should be submitted. Capability helpers report the current StateRail adapter contract: Coinbase spot (`CBE`) and Coinbase Financial Markets futures (`FCM`) are live-executable, while Coinbase INTX remains metadata-only in this project.

`StrategySnapshot.product_exposure()` and `StrategySnapshot.order_capacity()` expose advisory replay-derived exposure and operator-policy capacity. They reuse the same risk-gate daily-notional and live-open-order classification helpers, but they do not reserve capacity or authorize execution. The action gateway remains the only order-admission boundary.

`StrategySnapshot.plan_staged_release_sizes()` is the strategy-facing helper for visible chunk planning. It uses the snapshot product catalog, the configured operator policy visible-notional cap when available, and the core `LineageSizingPolicy` product-rule checks. For futures products with `contract_size` metadata, visible notional is calculated as `size * price * contract_size`. It returns a sizing decision only; it does not submit or release an order. `strategy_staged_release_intents()` converts an accepted staged-release sizing decision into deterministic `PlaceOrderIntent` chunks with stable action IDs, stable client order IDs, and `staged_release` placement kind. `StrategySnapshot.quote_pair_intents()` builds deterministic bid/ask `PlaceOrderIntent` values from the replayed order-book midpoint, product tick rules, operator policy, and optional staged-release chunking; it supports only `initial` or `staged_release` placement and does not perform live release. `StrategySnapshot.ladder_plan()` builds a product-rule-checked ladder plan without emitting intents. `StrategySnapshot.scheduled_slice_plan()` returns the next replay-safe TWAP/DCA-style slice plan, using deterministic action IDs and replayed action history to avoid duplicating a slice after restart; it does not emit intents. `strategy_release_staged_placement_intent()` reconstructs a live `release` intent from a staged placement in the replayed source-of-truth projection, with a distinct action/client identity and default one-live-order-per-logical-order protection. `strategy_followup_after_fill_intent()` reconstructs an opposite-side followup intent from a replayed fill, active followup-enabled operator policy, and product metadata; it is an intent helper, not autonomous fill-chasing logic. `strategy_split_order_intents()` reconstructs one unfilled live logical order into an explicit cancel intent plus same-side split-child placement intents. `strategy_consolidation_intent()` builds one consolidation placement from two or more replayed logical orders that share product and side. The built-in passive market-making strategy uses the same quote-pair price helper as the generic quote-pair builder. The built-in consolidation and anchor-repricing managers emit explicit cancel intents before replacement placements; they still do not pretend the sequence is atomic.

The strategy harness emits `strategy.evaluation_started`, then submits returned intents through `ActionGateway.submit_and_execute()`, then emits `strategy.evaluation_completed`. Intent submission is ordered and fail-closed: if any action receipt is rejected or fails, later intents from the same decision are not submitted, a typed `strategy_action_failed` error is recorded, and the evaluation closes as failed. A failed strategy also stops the remaining strategies in that scheduled cycle. Strategy exceptions or contract violations emit typed `error` events and `strategy.evaluation_failed`; they do not bypass runtime auditing. The harness overwrites action `requested_by` as `strategy:{strategy_id}` so order records identify the strategy without trusting strategy-authored metadata.

Strategy decisions are validated as a set before any action is submitted. Duplicate action IDs or duplicate place-order client identities fail the evaluation as a strategy contract error, so a malformed decision cannot partially execute one intent and then reject a later duplicate.

The read-only simulation harness evaluates the same `Strategy` interface against a verified ledger projection, converts decisions through the same strategy-to-command boundary, and previews actions through `ActionGateway.preview()`. Ordered decisions are previewed against a temporary replay ledger with the same gateway and dry-run executor, so cancel-then-place workflows are evaluated against the projected post-cancel state. Simulation reports `ok` only when strategies complete and all action previews pass validation/risk checks. It does not emit audit records to the operator ledger, execute live orders, or call Coinbase; it is for proving the decision contract and risk/validation outcome before scheduled evaluation is enabled.

The scenario harness is for developer regression tests. It builds a temporary hash-chained ledger from typed events, replays that ledger through the same projection, runs the simulation harness, and compares typed expectations against action previews. Scenarios can be constructed in Python with fixture builders for product metadata, order books, trades, staged orders, fills, and open-order imports, or loaded from versioned JSON fixtures through the CLI. JSON fixtures may declare scenario-local static-intent strategies for order preview tests without adding runtime strategy plugins. Scenario ledgers are fixtures; they are not substitutes for operator ledgers or live readiness.

Operator-policy scenarios are separate executable examples for policy math and safety expectations. They do not start runtime tasks, write operator ledgers, or call Coinbase. Scenarios that describe automatic order creation without an implemented strategy contract are reported as documented-only.

Strategy snapshots also expose typed market-data freshness checks for tickers, order books, and trades. Freshness is computed from the ledger-observed time of the accepted market-data record, not from an exchange timestamp string, so the check is replayable and consistent with the source-of-truth ledger. A strategy should treat missing or stale required inputs as a no-action decision unless it has an explicit recovery policy.

Operators can also configure strategy market-data requirements. When a required ticker, order book, or trade stream is missing or stale, the harness fails the strategy evaluation before calling strategy code and before any intent reaches the action gateway. Simulation applies the same gate, so CI/operator checks can detect stale-input failures without appending records.

Ledger health verifies strategy evaluation start/closure pairing and closure payload contracts, including status, submitted action counts, action receipt shape, and typed input-freshness details when present. Live runtime admission consumes ledger health, so an operator-visible ledger attention state blocks startup instead of being only a diagnostic report. Operators may append a scoped `operator.ledger_health_acknowledged` record after review; that acknowledgement is valid only for the current health-attention digest and reviewed ledger prefix, and it becomes stale after later non-acknowledgement records.

The built-in `noop` strategy is only a harness smoke target. The built-in `policy-probe` strategy is a no-order wiring target for proving operator-policy and product-metadata visibility. The built-in `staged-release-manager` strategy is order-management infrastructure: when explicitly selected, it can release at most one fresh, policy-scoped staged placement per evaluation through the normal action gateway, and it stays inert when `staged_or_hidden_release.allow_release=false`. When `release_only_when_conditions_match=true`, it requires a fresh order book and skips staged limit orders that would cross the current book. The built-in `followup-on-fill-manager` is also order-management infrastructure: when explicitly selected, it can create at most one policy-scoped opposite-side followup per evaluation from replayed fills and product metadata. The built-in `consolidation-manager` is conservative order-tidying infrastructure: when explicitly selected, it can cancel two same-product, same-side, same-price unfilled live orders and place one consolidation order through the normal gateway. The built-in `anchor-repricing-manager` is conservative order-movement infrastructure: when explicitly selected, it can cancel and replace one unfilled same-side live order whose price has drifted outside the operator policy anchor band, preserving the logical order ID and respecting configured cooldown and hourly replacement limits. The built-in `passive-market-making` strategy is the first conservative strategy template: when explicitly selected, it uses a fresh replayed order-book midpoint, creates one bid and one ask by default, and emits staged placements only. It does not claim profitability and does not submit visible orders unless a separate release workflow later acts on those staged placements. Real trading strategies should be registered by application code or exposed by installed Python packages through the `staterail.strategies` entry point group, then selected by explicit `bot.strategies.strategy_ids` config. Strategy action/client-order ID helpers produce stable replay-safe identifiers from semantic parts. Followup, split, consolidation, anchor repricing, and passive quoting helpers use the same deterministic ID boundary and reject already-created followups, split children, consolidation placements, duplicate moves, or active passive quotes. Live strategy evaluation requires `bot.strategies.allow_live_execution=true` in addition to the global live-trading approval and risk/product metadata gates. No strategy should call a REST executor, websocket source, or ledger append path directly.

## Feeds And Data

Websocket sources feed the redundant router. The router accepts one unique message, audits duplicates, tracks expected sources, and emits degradation when live feed coverage falls below policy.

The Coinbase websocket adapter normalizes messages into source-independent keys where possible and audits sequence gaps, out-of-order data, heartbeats, and user-channel order updates. Message keys include a digest of the normalized exchange payload so redundant feeds can still deduplicate the same message, while reused Coinbase `sequence_num` values from a later websocket session do not suppress fresh data after restart or reconnect. Coinbase websocket sequence continuity is tracked per connection because sequence numbers advance across channels on a connection. If a real sequence gap or out-of-order message is emitted, the supervisor disconnects that source and reconnects it through the configured backoff policy.

Only accepted, non-duplicate market-data messages update strategy-facing market snapshots in the source-of-truth projection. Strategies should read latest tickers, order books, and market trades from the projection instead of parsing raw websocket payloads or maintaining a parallel feed store.

## Reconciliation

Reconciliation tasks compare bot-derived state with exchange data:

- The watchdog detects missing user-channel confirmation and missing execution results.
- Recovery queries REST for mismatched orders.
- Fill reconciliation emits immutable fill records and replayed position snapshots.
- Exchange-state reconciliation records account, balance, and position snapshots and audits position drift.

These tasks are runtime safety infrastructure. They should not be treated as strategy logic.

## Live Scope

Current live execution scope is Coinbase spot (`CBE`) and Coinbase Financial Markets US futures (`FCM`) through Coinbase Advanced Trade. INTX live routing is intentionally out of scope until it can be added with separate eligibility assumptions, config, readiness checks, tests, and documentation.
