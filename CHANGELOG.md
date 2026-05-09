# Changelog

All notable changes to StateRail are documented here.

This changelog starts with the first public StateRail release.

## Unreleased

No unreleased changes.

## 0.1.5 - 2026-05-09

### Added

- Added a read-only operator canary evidence command that summarizes the replayed place/cancel lifecycle, cancellation evidence, and remaining open orders after a live canary.
- Added compact canary evidence to the generated canary plan and operator runbooks.
- Added a no-intent `market_window_stats` strategy-wizard template that demonstrates replayed trade-window and order-book-window helper access.
- Added `bot.strategies.order_book_sample_product_ids` to scope historical order-book sample retention without losing latest order-book state for other products.
- Added explicit venue capability flags for Coinbase product metadata, websocket market data, user-order streams, order/fill/account lookups, and CFM position lookups.

### Changed

- Tightened CFM live example risk caps to a $200 max order notional for the current Coinbase operator test posture.
- Added `bot.risk.max_daily_notional` and `bot.risk.max_open_orders` config/env loading so baseline risk policy can enforce daily notional and open-order caps without requiring an operator-policy overlay.

### Fixed

- Canary planning now validates proposed live canary price, size, and notional against replayed product metadata when the live ledger has product snapshots, preventing placeholder prices from producing unsafe live command plans.

## 0.1.4 - 2026-05-09

### Added

- Added an operator canary dry-run config renderer that derives an isolated dry-run rehearsal config from the live config while preserving risk scope.
- Added explicit normalized market-series window metadata for strategy trade windows, candles, rolling trade volume, and rolling trade count.
- Added a configurable strategy snapshot replay cap for retained accepted market trades per product, with replay metadata reporting dropped trades.
- Added replay-backed order-book sample retention and an `order_book_sample_window()` strategy helper that reports insufficient data unless enough retained samples exist in the requested window.
- Added `order_book_window_stats()` for derived spread, midpoint, volume, and imbalance metrics over retained replay-backed order-book sample windows.
- Added an optional retained order-book sample depth cap per side for strategy snapshot replay, while keeping latest-book projection state full-depth.

### Changed

- Updated operator canary documentation so dry-run rehearsal uses the rendered dry-run config instead of the live config.
- Updated strategy documentation to describe the market-series time field and membership-rule contract.

## 0.1.3 - 2026-05-09

### Added

- Added a one-shot operator place-order CLI that submits a single limit order through the normal gateway, risk gate, and configured executor without starting runtime tasks or websocket feeds.
- Added operator place-order regression coverage for accepted dry-run execution, risk rejection, malformed input rejection, mocked live REST execution, and JSON receipt shape.

### Changed

- Updated operator documentation with the safe first-canary command sequence using place-order, open-order inspection, cancel, replay, and ledger-health checks.
- Updated the roadmap so the next release focus is controlled Coinbase canary flow, followed by normalized historical market-series contracts.
- Moved maintainer planning documents under a guarded docs subtree that is excluded from public release export.

## 0.1.2 - 2026-05-09

### Added

- Added a `current_market_data` strategy wizard template that demonstrates safe access to current replayed ticker, order-book, and accepted-trade projection state without emitting order intents.

### Changed

- Clarified that market-data helpers are replay-derived projection helpers for current state, bounded accepted-trade windows, candles, rolling trade volume/count, and latest order-book metrics, not a standalone historical market-data platform.
- Tightened exported documentation rules so released docs focus on StateRail behavior, operator safety, and contributor extension points.
- Removed maintainer-only release publishing from installed console scripts while keeping release validation and package smoke tooling available.

## 0.1.1 - 2026-05-09

### Changed

- Corrected release metadata for the stable public repository namespace.
- Clarified the product scope as event-sourced execution infrastructure with Coinbase spot and CFM futures as the first supported venue feature.

## 0.1.0 - 2026-05-09

### Added

- Added the StateRail package identity, console scripts, release validation, package smoke tooling, config wizard, and strategy wizard.
- Added a hash-chained JSONL audit ledger with verification before append, process lock protection, deterministic export, checkpoints, local anchors, S3 Object Lock anchors, and S3 Object Lock ledger archives.
- Added replay-derived source-of-truth projection for actions, feed state, accepted market data, orders, logical order lineage, placements, fills, positions, product metadata, runtime lifecycle, health checks, anchors, archives, triggers, and errors.
- Added hook registry support with immutable hook payloads and audited hook failures.
- Added trigger support for before/on/after time and message-event conditions with replay-seeded one-shot suppression.
- Added redundant websocket feed routing with duplicate suppression, expected-source validation, heartbeat/liveness tracking, degraded-feed auditing, and restart replay behavior.
- Added Coinbase Advanced Trade websocket support for market data and user order updates.
- Added Coinbase Advanced Trade REST request building, JWT credential boundaries, injectable HTTP transport, retry auditing, dry-run execution, live place-order execution, and live cancel execution.
- Added Coinbase spot (`CBE`) and Coinbase Financial Markets futures (`FCM`) product catalog support, including CFM contract-size notional handling.
- Added a product-venue guard that allows live routing for Coinbase spot and CFM futures while rejecting unsupported venues before HTTP submission.
- Added strict JSON and environment config loading with typed enums, durations, execution modes, runtime task config, feed config, reconciliation config, risk config, and strategy config.
- Added runtime readiness checks for ledger path, locks, credentials, live approval, risk controls, product catalog, S3 Object Lock prerequisites, task config, config fingerprint drift, placeholders, and websocket redundancy.
- Added aggregate live no-order preflight covering readiness, product catalog smoke, feed smoke, and exchange-state smoke without starting order, strategy, or live runtime tasks.
- Added live runtime admission gate requiring clean ledger health or scoped acknowledgement, matching no-order preflight evidence, and strategy simulation qualification when strategies are enabled.
- Added ledger health checks for source-of-truth trust posture, runtime lifecycle, action lifecycle, order identity, order lineage, staged releases, passive quote metadata, exchange order updates, fills, reconciliation, strategy closure, feed/data flow, triggers, anchors, archives, product catalog coverage, and live venue scope.
- Added read-only CLIs for ledger summary, ledger export, source-of-truth export, ledger health, readiness, live runtime gate, open orders, strategy simulation, and strategy scenarios.
- Added audited operator CLIs for cancelling one tracked open order or all tracked open orders through the same gateway/executor path.
- Added a gateway-only action path for order intents with audited request, acceptance, rejection, execution start, execution result, execution failure, lineage records, and placement records.
- Added risk-gate checks for product scope, order type, side, order size, notional, leverage, reduce-only mode, post-only mode, open-order count, daily notional, replacement count, staged visible notional, lineage relation, placement kind, and kill switch.
- Added order lineage primitives for logical orders, venue placements, staged placements, releases, cancel-replace moves, followups, splits, consolidation, external imports, and manual association approval metadata.
- Added strict staged-release sizing and product-rule helpers for size, price, notional, tick proposals, increment proposals, futures contract size, and visible-notional caps.
- Added replay-derived exposure and capacity helpers for open orders, fill-derived positions, daily notional use, open-order slots, and operator-policy capacity.
- Added strategy runtime infrastructure: `StrategySnapshot`, `StrategyDecision`, deterministic action/client-order IDs, ordered gateway submission, set-level decision validation, audited evaluation start/completion/failure, and fail-closed input freshness requirements.
- Added read-only strategy simulation with ordered action preview against a temporary replay ledger.
- Added typed strategy scenario harness with JSON fixture support, scenario-local static strategies, expectation checks, and CLI execution.
- Added Python scenario fixture builders for product snapshots, order books, trades, staged orders, fills, and open-order imports.
- Added strategy metadata assertion helpers for external strategy package tests.
- Added strategy wizard for scaffolding external packages with entry points, examples, tests, and PEP 561 typing markers.
- Added config wizard and config template rendering for safe local operator config creation.
- Added built-in strategies and managers: no-op, policy probe, staged-release manager, followup-on-fill manager, consolidation manager, anchor-repricing manager, and passive market-making template.
- Added strategy-facing market-data helpers for best bid/ask, midpoint, spread, order-book stats, latest trade, trade windows, market-window stats, candles, rolling trade volume, and rolling trade count.
- Added strategy-facing execution planning helpers for staged-release sizing, quote-pair intents, ladder plans, and scheduled slice plans.
- Added normalized venue and product capability helpers for Coinbase spot, CFM futures, and metadata-only INTX awareness.
- Added operator policy loading, policy-derived risk config, policy-derived market-data requirements, and executable operator-policy scenarios.
- Added Coinbase user-channel order update reconciliation, REST order recovery, fill reconciliation, exchange balance/position snapshots, reconciliation drift records, and runtime reconciliation watchdog tasks.
- Added release documentation, operations documentation, configuration documentation, extension documentation, security policy, contributing guide, strategy taxonomy, system diagram, operator guide, and roadmap.

### Changed

- Replaced the root README with a release-focused project overview, installation guide, quick start, live-operation boundary, strategy-development guide, quality-check instructions, and risk notice.
- Documented INTX as metadata-replayable but not live-routable.
- Defined the current live scope around Coinbase spot and Coinbase Financial Markets US futures.

### Fixed

- Fixed duplicate action and duplicate client-order identity handling so malformed strategy decisions fail before partial gateway execution.
- Fixed replay/restart duplicate suppression for market data, user order updates, and fill reconciliation.
- Fixed ledger append safety by verifying terminal newline boundaries and locking verify/read/append operations.
- Fixed live execution venue safety by rejecting unsupported product venues before HTTP submission.
- Fixed strategy runtime safety by requiring explicit live strategy allowance and matching clean simulation qualification before live strategy tasks can start.
- Fixed release safety by checking exported trees for internal-only files, generated runtime state, credentials, private key material, sensitive content, license metadata, and manifest drift.
- Fixed futures notional handling by applying product metadata contract size where available instead of assuming spot-style `size * price`.
- Fixed staged release and order-lineage health coverage so staged placements, releases, passive quotes, and missing lineage records are replay-visible and checked.
- Fixed exchange-state trust gaps with no-order smoke paths, reconciliation recovery records, fill duplicate suppression, and exchange-position drift reporting.
