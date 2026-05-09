# StateRail

StateRail is event-sourced execution infrastructure for auditable exchange workflows. It records runtime lifecycle, accepted market data, strategy intent, gateway decisions, order execution, reconciliation, checkpoints, immutable archives, and errors into a replayable hash-chained ledger.

Coinbase Advanced Trade spot and Coinbase Financial Markets (CFM) futures are the first supported venue feature. The framework is structured so additional venues can be added behind typed adapter, product metadata, risk, readiness, and replay contracts.

StateRail is not financial advice, not a trading strategy recommendation engine, and not a profitability system.

## Core Capabilities

- Hash-chained JSONL audit ledger with verification before append.
- Replay-derived source-of-truth projection for actions, feeds, orders, fills, positions, products, runtime state, checkpoints, anchors, and errors.
- Gateway-only order submission path with audited request, acceptance/rejection, execution start, execution result, and failure records.
- Dry-run executor and live Coinbase REST executor boundary.
- Redundant websocket feed routing with duplicate suppression, source validation, heartbeat/liveness checks, and replay-seeded restart behavior.
- Strict typed configuration loading with enum-backed runtime modes and fail-closed live gates.
- Product metadata catalog with Coinbase spot and CFM futures support, including futures contract-size notional handling.
- Risk gate for product scope, order type, side, size, notional, leverage, reduce-only mode, post-only mode, open-order count, daily notional, replacement count, and kill switch.
- Order lineage for logical orders, staged placements, release attempts, cancel-replace moves, followups, splits, consolidation, and manual association records.
- Strategy interface with read-only simulation, scenario fixtures, external strategy package entry points, deterministic action/client-order IDs, and typed strategy helper APIs.
- Built-in conservative infrastructure strategies: no-op, policy probe, staged-release manager, followup-on-fill manager, consolidation manager, anchor-repricing manager, and passive market-making template.
- Coinbase user-channel reconciliation, order recovery, fill reconciliation, exchange-state snapshots, and reconciliation drift reporting.
- Ledger health, readiness, live no-order preflight, live runtime admission gate, source-of-truth export, ledger export, checkpoint, anchor, archive, and operator place-order/cancel/open-order CLIs.
- AWS S3 Object Lock support for immutable checkpoint anchors and ledger archives.
- Quality tooling for release checks, package smoke, config wizard, and strategy wizard.

## Supported Venue Scope

Current live execution support is limited to:

- Coinbase Advanced Trade spot products with product venue `CBE`.
- Coinbase Financial Markets US futures products with product venue `FCM`.

Coinbase INTX metadata can be replayed, but INTX live order routing is intentionally disabled in this project.

## Installation

Use Python 3.11 or newer.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Install optional adapters only when needed:

```powershell
python -m pip install -e ".[aws,coinbase]"
```

After installation, the main console entry point is:

```powershell
staterail --help
```

The same command is also available as:

```powershell
python -m app.main --help
```

## Quick Start

Run the default dry-run configuration:

```powershell
python -m app.main --config-file docs\examples\config.dry-run.json --max-cycles 1
```

Create a local operator config with the wizard:

```powershell
python -m tools.config_wizard --list-profiles
python -m tools.config_wizard --profile dry_run --target config.local.json --force
```

Run strategy simulation without appending to the operator ledger:

```powershell
python -m app.main --config-file config.local.json --strategy-simulate --strategy-simulate-fail-on-attention
```

Run ledger health:

```powershell
python -m app.main --config-file config.local.json --ledger-health
```

## Live Operation

Live operation is gated deliberately. Before live runtime tasks can start, StateRail requires configured credentials, explicit live approval, risk controls, product metadata, readiness checks, clean ledger health or reviewed acknowledgement, aggregate no-order preflight evidence, and clean strategy simulation qualification when strategies are enabled.

The tested live templates are in `docs/examples/`:

- `config.cfm-live.json`
- `config.cfm-policy-probe.json`
- `config.cfm-passive-market-making.json`
- `config.cfm-passive-market-making-release.json`

Coinbase credentials are read from the operator environment. Do not commit secrets.

## Strategy Development

Strategies receive a replayed `StrategySnapshot` and return typed intents. Strategy code should use StateRail concepts and helper APIs instead of parsing raw Coinbase payloads or calling exchange clients directly. Market-data helpers are replay-derived projection helpers for current state, bounded accepted-trade windows, candles, rolling trade volume/count, and latest order-book metrics; they are not a standalone historical market-data platform.

Generate an external strategy package:

```powershell
python -m tools.strategy_wizard --name my-strategy --target ..\my-strategy --template metadata_only
```

Useful strategy-facing docs:

- [Extending](docs/extending.md)
- [Strategy taxonomy](docs/strategy-taxonomy.md)
- [Roadmap](docs/roadmap.md)

## Documentation

- [Architecture](docs/architecture.md)
- [Operations](docs/operations.md)
- [Operator guide](docs/operator-guide.md)
- [Configuration](docs/configuration.md)
- [System diagram](docs/system-diagram.md)
- [Roadmap](docs/roadmap.md)
- [Security policy](SECURITY.md)
- [Changelog](CHANGELOG.md)

## Quality Check

Run the regression suite:

```powershell
pytest tests/regression/ -v
```

Behavior-affecting changes should pass regression before they are considered ready.

## License

StateRail is licensed under the Apache License 2.0. See [LICENSE](LICENSE).

## Risk Notice

Trading is risky. StateRail can make execution behavior more auditable and replayable, but it cannot make a trading idea profitable, safe, or suitable for any account. Built-in strategies are conservative templates for exercising framework contracts. Passing tests, simulation, readiness, or preflight checks does not mean an order should be placed.
