# Operator Guide

This guide defines how a human operator should use StateRail safely. Runbooks can be product- or venue-specific; this file captures the common operating rules.

## Operator Model

StateRail should be operated through audited commands and replayable ledgers. A safe operator workflow has four properties:

- every irreversible action goes through the action gateway
- every setup check is repeatable from a documented command
- every live run has ledger health and no-order preflight evidence
- every recovery or cleanup action is audited before and after execution

Strategies are not an operator console. Strategies may emit intents, but they should not be the first path used to prove live order placement on a real account.

## First Live Canary Rule

Do not use a strategy as the first live canary order.

The first live canary order for an operator account should start with the read-only canary plan, then use a dedicated one-shot operator place-order command that:

- checks the dry-run and live config split before any order endpoint is called
- prints the exact dry-run, live placement, cleanup, replay, and health commands
- submits exactly one order through the existing action gateway
- records an operator ID and reason
- requires explicit product, side, size, order type, time in force, reason, and limit price
- supports dry-run execution before live submission
- refuses market orders
- refuses BTC futures/perpetual products for the initial Coinbase operator test scope
- prints the audited action receipt and venue order identifiers
- is immediately followed by operator open-order inspection, cancel, compact canary evidence, exchange-state smoke, replay, and ledger health

The canary plan plus operator place-order command is the only recommended first live canary path. Do not substitute a scheduled strategy run for this step.

When the live ledger already contains product snapshots, the canary planner validates the proposed canary price, size, and notional against replayed product metadata and the configured risk cap. If the ledger does not yet contain product metadata for the canary product, run the live no-order preflight first, then generate the canary plan again. The generated plan still includes a no-order preflight step as the final confirmation before live placement.

## Operator Progression

Use this order when bringing a real account online:

1. Build or update a local ignored config file.
2. Run readiness.
3. Run no-order Coinbase smoke checks.
4. Run aggregate no-order preflight.
5. Verify ledger health and source-of-truth replay.
6. Publish and verify S3 Object Lock evidence when configured.
7. Run strategy simulation without order submission.
8. Run staged-only strategy evaluation.
9. Run restart and recovery drills.
10. Render an isolated dry-run canary config from the live config.
11. Generate a clean read-only operator canary plan.
12. Run the planned dry-run operator place-order and dry-run cleanup.
13. Run the planned live preflight/gate sequence.
14. Place one tiny post-only live canary order.
15. Cancel immediately.
16. Verify compact canary evidence, exchange-state smoke, source-of-truth replay, and final ledger health.

Any attention result stops the progression until reviewed.

If the live config has no scheduled strategies enabled, the canary plan skips strategy simulation. If scheduled strategies are enabled, the plan includes simulation qualification immediately before the live runtime gate.

Use a separate dry-run canary config. Prefer rendering it from the live config with the dedicated command so it keeps the same risk scope while using a separate dry-run ledger path, `bot.rest.execution_mode=dry_run`, no REST-backed reconciliation, no scheduled strategies, no product-catalog refresh, no audit publication, and no websocket sources.

## Required Operator Commands

The current operator command surface includes:

```powershell
python -m app.main --config-file config.local.json --operator-canary-render-dry-run-config --operator-canary-dry-run-config-file config.canary-dry-run.local.json --operator-canary-dry-run-ledger-path data/canary-dry-run-audit.local.jsonl --operator-canary-dry-run-config-force
python -m app.main --config-file config.local.json --operator-canary-plan --operator-canary-dry-run-config-file config.canary-dry-run.local.json --operator-id "$env:USERNAME" --operator-place-product-id "<product-id>" --operator-place-side buy --operator-place-size "1" --operator-place-limit-price "<limit-price>" --operator-place-leverage "1" --operator-place-order-type limit --operator-place-time-in-force good_until_cancelled --operator-place-post-only --operator-place-reason "first operator canary"
python -m app.main --config-file config.canary-dry-run.local.json --operator-place-order --operator-id "$env:USERNAME" --operator-place-product-id "<product-id>" --operator-place-side buy --operator-place-size "1" --operator-place-limit-price "<limit-price>" --operator-place-leverage "1" --operator-place-order-type limit --operator-place-time-in-force good_until_cancelled --operator-place-post-only --operator-place-reason "first operator canary dry run"
python -m app.main --config-file config.canary-dry-run.local.json --operator-cancel-all-open-orders --operator-id "$env:USERNAME" --operator-cancel-product-id "<product-id>" --operator-cancel-action-id-prefix "operator-canary-dry-run-cancel" --operator-cancel-reason "first operator canary dry-run cleanup"
python -m app.main --config-file config.local.json --operator-open-orders
python -m app.main --config-file config.local.json --operator-cancel-order --operator-id "$env:USERNAME" --operator-cancel-exchange-order-id "<exchange-order-id>" --operator-cancel-reason "operator cleanup"
python -m app.main --config-file config.local.json --operator-canary-evidence --operator-canary-evidence-exchange-order-id "<exchange-order-id>" --operator-canary-evidence-product-id "<product-id>" --operator-canary-evidence-fail-on-attention
python -m app.main --config-file config.local.json --operator-cancel-all-open-orders --operator-id "$env:USERNAME" --operator-cancel-product-id "<product-id>" --operator-cancel-action-id-prefix "operator-cancel" --operator-cancel-reason "operator cleanup"
```
