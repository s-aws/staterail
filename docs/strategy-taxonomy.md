# Strategy Taxonomy

This project can use established trading-bot terminology as reference material, but it should not copy execution models that conflict with the ledger-first design.

The compatibility rule is simple: outside frameworks may inform names, scenario fixtures, and documentation, but every live or simulated action in this project must remain replayable from the audit ledger and routed through the action gateway.

## Core Glossary

| Term | Meaning |
| --- | --- |
| Intent | A typed request to perform an action, emitted by a strategy or operator before the action gateway validates it, risk-checks it, audits it, accepts or rejects it, and optionally executes it. An intent is a proposed action, not an accepted action and not an exchange order. |
| Action | The audited gateway lifecycle for an intent, beginning with `action.requested` and then moving to `action.accepted` or `action.rejected`; accepted actions may continue to execution records. |
| Command | The gateway-normalized action payload derived from an accepted intent and passed to validation, risk, preview, or execution paths. |
| Exchange Order | A venue-side order object or identifier returned by the exchange after a submitted order is accepted by the venue. |

## Reference Projects

| Project | Useful Taxonomy | License Posture | Use In This Project |
| --- | --- | --- | --- |
| [Hummingbot V2](https://hummingbot.org/strategies/v2-strategies/) | Scripts, controllers, executors, market data provider, executor actions, PMM, grid, TWAP, DCA, arbitrage | Apache-2.0 according to Hummingbot public materials | Best conceptual reference for strategy/control/execution vocabulary. Do not make executors self-submit orders; map them to audited workflow decisions. |
| [NautilusTrader](https://nautilustrader.io/docs/latest/concepts/execution/) | Strategy, order emulator, execution algorithm, risk engine, execution engine, execution client | Check license before any code reuse | Good reference for event-driven component boundaries and pre-trade risk placement. Keep our risk gate authoritative. |
| [QuantConnect LEAN](https://www.quantconnect.com/docs/v2/writing-algorithms/key-concepts/algorithm-engine) | Algorithm lifecycle, universe selection, alpha, portfolio construction, risk, execution models | Apache-2.0 on the LEAN repository | Useful later for portfolio/risk model naming and backtest/live qualification language. Not a near-term compatibility target. |
| [Freqtrade](https://www.freqtrade.io/en/stable/strategy-101/) | Strategy modes, dry-run workflow, backtesting, hyperopt, lookahead checks, recursive analysis | GPL-3.0 on the repository | Use only as taxonomy and operator UX reference. Do not copy implementation into this Apache-2.0 project. |
| [Jesse](https://github.com/jesse-ai/jesse) | Simple strategy lifecycle, multi-timeframe/symbol language, position entry/exit callbacks, paper/live workflow | MIT for the open-source repository; live-trading plugin has separate terms | Useful for beginner-friendly strategy API ideas. Treat live-plugin behavior as out of scope. |
| [CCXT](https://github.com/ccxt/ccxt/wiki/manual) | Unified exchange capability terms, market metadata, order book, trade, ticker, order methods, per-exchange capability flags | MIT on the repository | Useful for future venue capability metadata. Do not add CCXT as a dependency unless Coinbase-first boundaries become too narrow. |
| [Backtrader](https://www.backtrader.com/docu/strategy/) | Strategy callbacks, broker notifications, analyzers, reusable indicators, backtest-first workflow | Check license before any code reuse | Useful for scenario/backtest vocabulary. Not a live execution reference for this project. |
| [Superalgos](https://superalgos.org/free-open-source-bitcoin-trading-bots.shtml) | Visual strategy blocks, data-mining/backtesting workflow, operations UX | Check license before any code reuse | Low-priority reference for public UX and visual workflow concepts, not core architecture. |

## Mapping Into This Project

| External Concept | Project Concept | Rule |
| --- | --- | --- |
| Hummingbot controller | `Strategy` implementation | A strategy may decide what should happen, but it must return `StrategyDecision` intents instead of submitting orders. |
| Hummingbot executor | Audited order workflow / lineage policy / future execution-policy helper | Any workflow state must be derivable from ledger records. No hidden executor store. |
| Hummingbot market data provider | `SourceOfTruthProjection` | Strategies consume replayed accepted data, not raw websocket payloads. |
| Executor action | `PlaceOrderIntent` / `CancelOrderIntent` / `ActionCommand` | Intent validation and submission go through the action gateway. |
| Freqtrade dry run | `ExecutionMode.DRY_RUN`, strategy simulation, scenario harness | Dry-run and simulation prove contract behavior, not profitability. |
| Freqtrade lookahead analysis | Scenario regression and replay determinism checks | Add explicit scenarios for any strategy that depends on prior market state. |
| LEAN risk model | `RiskGate` plus future portfolio-risk components | Pre-trade rejection must remain auditable before execution starts. |
| LEAN execution model | Future execution-policy helper | It may propose placement/move/cancel/followup intents, but cannot bypass the gateway. |
| CCXT market/order capabilities | Future product/venue capability metadata | Capabilities should constrain config, readiness, risk checks, and executor payloads. |

## Compatibility Position

Compatibility should be adapter-based, not inheritance-based.

Near-term strategy work should create clean examples with familiar names: passive market making, grid, TWAP, DCA, anchor repricing, tranche release, and adaptive sizing. Passive market making now has an initial staged quote template, and staged release, followup-on-fill, consolidation, and anchor repricing now have infrastructure managers. Strategy examples should use those typed interfaces instead of bypassing them.

Later, if Hummingbot-style compatibility is still useful, add an adapter package that translates a small subset of controller outputs into `StrategyDecision` values. That adapter should be optional, tested separately, and unable to access exchange clients directly.

## Non-Negotiable Constraints

- No strategy, adapter, or imported framework may call Coinbase or any venue directly.
- No strategy-owned database may become the source of truth for orders, fills, balances, positions, or market data.
- No compatibility layer may create action IDs or client order IDs nondeterministically.
- No GPL implementation code may be copied into this Apache-2.0 project.
- Any external-code reuse must preserve license notices and pass license and security review.

## Recommended Taxonomy For Our Strategy Work

Use these names unless a later reason is stronger:

| Term | Meaning |
| --- | --- |
| Strategy | Long-running decision component that reads a snapshot and returns intents. |
| Policy | Operator-authored constraints that shape risk, data requirements, sizing, and allowed order behavior. |
| Scenario | Deterministic fixture that proves expected strategy/risk behavior against a replayed ledger. |
| Execution Policy | A reusable helper that converts a desired trading workflow into audited intents. |
| Logical Order | Internal order identity used for lineage and replay. |
| Placement | One venue-facing or staged attempt to represent a logical order. |
| Staged Release | Non-submitted logical placement that can later be released as a normal order. |
| Release | A gateway-routed order placement that turns a staged placement into a submitted/accepted placement. |
| Move | Same-side amend or cancel-replace under the same logical order. |
| Followup | Opposite-side child order after a confirmed fill. |
| Consolidation | Merge of multiple logical orders into a new logical order. |
