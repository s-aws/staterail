# Roadmap

This roadmap describes the near-term product direction for StateRail. It is intentionally scoped around event-sourced execution infrastructure, not around building every possible trading strategy.

## Current Focus

StateRail's primary goal is reliable, replayable execution infrastructure:

- Immutable append-only audit ledger with replay-derived source-of-truth state.
- Typed strategy intents that pass through one gateway, one risk path, and one audited execution boundary.
- Coinbase Advanced Trade spot and Coinbase Financial Markets US futures as the first live venue feature.
- Operator safety checks for readiness, no-order live preflight, strategy simulation, ledger health, recovery, one-shot place-order, open-order inspection, and cancel.
- Public package ergonomics for installation, configuration, strategy scaffolding, regression checks, and release validation.

## Planned Work

### Controlled Coinbase Canary Flow

Use the audited operator command surface as the repeatable first-live-order workflow:

- Render an isolated dry-run canary config from the live config while preserving risk scope.
- Generate a read-only canary plan that checks the dry-run/live configs and prints the exact operator command sequence.
- Dry-run one explicit post-only limit order through the gateway and risk gate, then clean it up.
- Run the same command live only after readiness, no-order preflight, simulation qualification when strategies are enabled, and live runtime gate are clean.
- Immediately inspect open orders, cancel the canary, look up the terminal venue order status, replay compact canary evidence, replay source-of-truth state, run ledger health, and record recovery evidence.
- Repeat the workflow against more than one low-notional, non-BTC CFM product before treating the Coinbase operator path as initially proven.
- Keep the canary product scope small and Coinbase spot/CFM-focused until the venue contract has more live evidence.

### Strategy Helper Surface

Current strategy helpers expose replay-derived market data retained in the source-of-truth projection:

- Normalized market-series window contracts: product, `as_of`, start/end bounds, lookback, time field, membership rule, and optional retention limit.
- Market-window statistics: base volume, quote volume, VWAP, TWAP, open, high, low, close, realized volatility, buy volume, sell volume, and aggressor volume.
- Order-book statistics: best bid, best ask, spread, spread bps, midpoint, microprice, bid volume, ask volume, top bid size, top ask size, imbalance, and weighted mid.
- Time-bounded trade windows, candles, rolling trade volume, and rolling trade count over accepted trades retained in replayed state, with optional retained-trade caps that report when trades are dropped.
- Configurable strategy snapshot replay limits for retained accepted trades per product.
- Replay-backed order-book sample windows with configurable per-product sample retention and insufficient-data status when only the latest book is retained.
- Derived order-book window statistics over retained samples, including spread, midpoint, bid/ask volume, and book imbalance.
- Optional retained order-book sample depth caps that reduce historical sample memory without changing latest-book state.
- Optional historical order-book sample product scope that keeps latest order-book state global while retaining replay-backed sample windows only for configured products.

These helpers are not yet a standalone historical market-data subsystem. Market-data work should keep three layers separate:

- Current replayed market state: latest ticker, latest order book, retained accepted trades, and freshness checks.
- Historical normalized market series: explicit tick/trade windows, candle construction contracts, bounded lookbacks, deterministic time anchors, scoped retained samples, and retained sample-window extensions where strategy demand justifies them.
- Derived strategy metrics: rolling volume, VWAP/TWAP, spread/midpoint/microprice, book imbalance, trade-flow imbalance, and volatility/regime metrics.

Further historical order-book metrics should be added only when retained sample-window contracts have enough replay evidence to keep simulation, recovery, and strategy behavior deterministic.

Strategy authors should write against StateRail concepts. Venue adapters should translate venues into StateRail concepts. The ledger remains the source of truth between them.

### Venue Adapter Contract

Keep Coinbase as the reference live adapter until the venue contract is stable. Additional venues should be added only after their adapters can satisfy the same typed contracts for:

- Product metadata and trading increments.
- Order submission, cancellation, and execution uncertainty.
- Market data normalization and duplicate suppression.
- Account/order/fill reconciliation.
- Replay and health-check visibility.
- A passing `venue_contract_report()` for the capability requirements needed by the intended live scope.

### Order Management Policies

Order-management policies are reusable infrastructure rather than strategy-specific exchange code. The current surface includes:

- Followup-on-fill management.
- Same-side order movement through amend or cancel-replace.
- Split workflows through explicit cancel-plus-child placement plans.
- Consolidation workflows through explicit cancel-then-replacement plans.
- Staged visible release for larger logical orders.
- Future hidden, reserve, or iceberg-style execution policy when the operational contract is explicit.

### Packaging And Distribution

Keep the project installable and testable as a normal Python package:

- Preserve PEP 561 typing markers.
- Keep console entry points stable.
- Build and smoke-test the wheel in CI with public-compatible commands.
- Keep public examples executable.
- Run full regression before release.
- Keep private operator state, credentials, local ledgers, and generated artifacts out of published source trees.

## Non-Goals

StateRail does not aim to provide financial advice, profit guarantees, or a library of ready-to-run trading strategies. Built-in strategies are conservative infrastructure examples used to exercise execution contracts.
