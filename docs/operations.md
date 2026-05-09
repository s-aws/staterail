# Operations

Commands are written for Windows PowerShell from the repository root.

## Development Setup

Create and activate a local virtual environment:

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

`.env.example` is a dry-run reference file. The runtime does not auto-load `.env` files.

After install, `staterail` is equivalent to `python -m app.main`. Config-wizard, strategy-wizard, and config-template helpers are available as `staterail-config-wizard`, `staterail-strategy-wizard`, and `staterail-config-template`. If Windows reports that the Python scripts directory is not on `PATH`, either add that directory to `PATH` or continue using the `python -m ...` commands below.

For human operating rules, use [Operator Guide](operator-guide.md).

## Dry Run

Run one cycle with defaults:

```powershell
python -m app.main --ledger-path data/audit.jsonl --max-cycles 1
```

Run continuously:

```powershell
python -m app.main --ledger-path data/audit.jsonl --run-forever
```

Smoke-test the strategy harness without emitting orders:

```powershell
$env:STATERAIL_STRATEGIES_ENABLED = "true"
$env:STATERAIL_STRATEGY_IDS = "noop"
$env:STATERAIL_STRATEGIES_ALLOW_LIVE_EXECUTION = "false"
$env:STATERAIL_WATCHDOG_ENABLED = "false"
python -m app.main --ledger-path data/strategy-smoke.jsonl --max-cycles 1
python -m app.main --ledger-path data/strategy-smoke.jsonl --ledger-health --ledger-health-fail-on-attention
```

Simulate configured strategies against an existing verified ledger without appending records or submitting orders:

```powershell
python -m app.main --config-file docs\examples\config.dry-run.json --ledger-path data/strategy-smoke.jsonl --strategy-simulate --strategy-simulate-fail-on-attention
```

For live scheduled strategies, write replayable qualification evidence after simulation. The simulation remains no-order; only the compact `runtime.strategy_simulation_result` record is appended:

```powershell
python -m app.main --config-file config.local.json --strategy-simulate --strategy-simulate-record-result --strategy-simulate-fail-on-attention
```

Run a typed strategy scenario fixture. The ledger path is a temporary scenario ledger and must be empty or absent:

```powershell
python -m app.main --config-file docs\examples\config.dry-run.json --ledger-path test_runtime\scenario-smoke.jsonl --strategy-scenario-file docs\examples\strategy-scenario.noop.json
python -m app.main --config-file docs\examples\config.dry-run.json --ledger-path test_runtime\staged-order-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.staged-order.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\staged-release-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.staged-release-manager.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\staged-release-manager-blocked-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.staged-release-manager-blocked.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\followup-on-fill-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.followup-on-fill-manager.json
python -m app.main --config-file docs\examples\config.staged-release-manager.dry-run.json --ledger-path test_runtime\consolidation-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.consolidation-manager.json
python -m app.main --config-file docs\examples\config.anchor-repricing-manager.dry-run.json --ledger-path test_runtime\anchor-repricing-manager-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.anchor-repricing-manager.json
python -m app.main --config-file docs\examples\config.passive-market-making.dry-run.json --ledger-path test_runtime\passive-market-making-scenario.jsonl --strategy-scenario-file docs\examples\strategy-scenario.passive-market-making.json
```

Scenario fixtures may declare `strategy_ids` and `static_strategies` to preview explicit order intents without installing a plugin or changing runtime strategy config. This is for simulation and regression only; it does not start runtime tasks or call order endpoints.

Create an external strategy package scaffold when starting new strategy work:

```powershell
python -m tools.strategy_wizard --name my-strategy --target ..\my-strategy --template metadata_only
python -m pip install -e ..\my-strategy
python -m pytest ..\my-strategy\tests -v
```

The wizard creates package metadata, a `staterail.strategies` entry point, a safe no-intent strategy template, a dry-run config, a scenario fixture, and package-local tests. It is scaffolding only; add trading behavior after scenario and simulation evidence exist.

Run checked operator-policy examples without writing to the ledger:

```powershell
python -m app.main --config-file docs\examples\config.dry-run.json --operator-policy-scenarios-file docs\examples\operator-scenarios.conservative-cfm-v0.json
```

The command returns attention if an executable expectation fails. Scenarios that document future automatic order creation are reported as `documented_only`.

Operator policy examples live in `docs\examples\operator-policy.conservative-cfm-v0.json`, `docs\examples\operator-policy.stealth-orders-manager-v1.json`, and `docs\examples\operator-scenarios.conservative-cfm-v0.json`. To enforce a policy at runtime, set:

```json
{
  "bot": {
    "strategies": {
      "operator_policy_file": "docs/examples/operator-policy.conservative-cfm-v0.json"
    }
  }
}
```

An operator policy does not enable strategy scheduling by itself. It extends the effective action-gateway risk policy, so strategy simulation and live scheduled strategies reject policy violations through the same auditable path used by normal gateway submissions.

If `bot.strategies.market_data_requirements` is configured, simulation and scheduled evaluation use the same fail-closed input gate. A stale or missing required ticker, order book, or trade stream is reported as strategy attention before strategy code runs.

The built-in `staged-release-manager` strategy only acts on already-staged placements and remains inert when `staged_or_hidden_release.allow_release=false`. When `release_only_when_conditions_match=true`, it also requires a fresh order book and skips staged limit orders that would cross the current book. It still emits normal order intents when release is enabled, so run simulation and keep live strategy execution disabled until the intended release behavior is visible in previews.

The built-in `followup-on-fill-manager` only acts on replayed fills that can be tied back to bot logical orders. It requires an operator policy with followups enabled plus product catalog metadata, emits at most one followup per evaluation, and still routes the followup through the normal gateway preview/submission path.

The built-in `consolidation-manager` only acts on unfilled replayed live orders that share product, side, and limit price. It requires merge lineage to be allowed, product catalog metadata, and fresh order-book data when the operator policy requires it. It emits explicit cancels before the replacement placement, so ordered simulation should be reviewed before enabling it on a live schedule.

The built-in `anchor-repricing-manager` only acts on unfilled replayed live orders whose current limit price has drifted outside the configured anchor band. It requires anchor repricing and same-side moves to be allowed, product catalog metadata, fresh order-book midpoint input, and cancel-replace fallback because this project does not currently implement a venue amend executor. It emits an explicit cancel before the `cancel_replace` placement and preserves the logical order ID.

The built-in `passive-market-making` strategy stages one bid and one ask around a fresh replayed order-book midpoint by default. It requires a limit/post-only operator policy, staged release enabled, product catalog metadata, and fresh order-book data. Configure quote size/spread through `bot.strategies.strategy_parameters["passive-market-making"]`; the checked templates default to `$5` notional and `50` half-spread bps. For CFM futures, notional is `size * price * contract_size` when Coinbase product metadata provides a contract size. Low displayed prices can still have a high one-contract notional, so keep order and visible caps above the product's minimum one-contract notional before expecting staged PMM quotes. The checked CFM templates currently use `$200` order/visible caps, a `$400` global daily cap, a `4` open-order cap, and evaluate both SHB/AVA products in the two-product examples. For CFM futures, configure `order_behavior.default_leverage` and `order_behavior.default_margin_type` in the operator policy; the checked CFM templates use `1` and `cross`. It emits staged placements only; visible submission requires a separate release workflow and should remain disabled until scenario and simulation previews match the intended behavior.

## Readiness

Readiness is read-only. It does not create the ledger or start runtime tasks.

```powershell
python -m app.main --config-file config.local.json --readiness
```

Fail with exit code `2` when attention is required:

```powershell
python -m app.main --config-file config.local.json --readiness --readiness-fail-on-attention
```

Use readiness before every live run. It checks ledger path, config fingerprint drift, runtime tasks, strategy ID resolution, credentials, risk policy, live-trading approval, websocket redundancy, product catalog requirements, and configured anchor/archive stores. Use strategy simulation before enabling scheduled strategy evaluation.

Check live runtime admission gates without starting runtime tasks:

```powershell
python -m app.main --config-file config.local.json --live-runtime-gate --live-runtime-gate-fail-on-attention
```

## Quality Checks

Run regression before considering behavior-affecting changes complete:

```powershell
pytest tests/regression/ -v
```

When package metadata changes, also run a dry-run install check:

```powershell
python -m pip install -e . --dry-run
```

## CFM No-Order Preflight

Render a local ignored config from the public CFM template:

```powershell
python -m tools.config_wizard --profile coinbase_cfm_no_order --target config.local.json --ledger-path data\cfm-live-audit.jsonl --products "SHB-26JUN26-CDE,AVA-29MAY26-CDE" --force --no-input
```

Or render the template directly:

```powershell
python -m tools.config_template docs\examples\config.cfm-live.json config.local.json --set-json 'REPLACE_WITH_CFM_PRODUCT_IDS=[\"SHB-26JUN26-CDE\",\"AVA-29MAY26-CDE\"]' --ledger-path data\cfm-live-audit.jsonl --force
```

After install:

```powershell
staterail-config-wizard --profile coinbase_cfm_no_order --target config.local.json --ledger-path data\cfm-live-audit.jsonl --products "SHB-26JUN26-CDE,AVA-29MAY26-CDE" --force --no-input
staterail-config-template docs\examples\config.cfm-live.json config.local.json --set-json 'REPLACE_WITH_CFM_PRODUCT_IDS=[\"SHB-26JUN26-CDE\",\"AVA-29MAY26-CDE\"]' --ledger-path data\cfm-live-audit.jsonl --force
```

Then run the aggregate no-order preflight. It runs readiness, product-catalog smoke, feed smoke, and exchange-state smoke in order. It stops on the first attention result and never starts order, strategy, or live runtime tasks. Clean writable runs append a compact `runtime.live_preflight_result` record:

```powershell
python -m app.main --config-file config.local.json --live-no-order-preflight --live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention
```

For diagnostics, each preflight step can also be run separately.

Readiness does not create the ledger, start websocket tasks, call order endpoints, or submit orders:

```powershell
python -m app.main --config-file config.local.json --readiness --readiness-fail-on-attention
```

Then fetch and audit configured product metadata without starting runtime, websocket, strategy, or order tasks:

```powershell
python -m app.main --config-file config.local.json --product-catalog-smoke --product-catalog-smoke-fail-on-attention
```

The product-catalog smoke result includes `policy_viability`. It reports whether the current product metadata makes the configured risk caps viable before strategy simulation starts. For CFM futures, it compares the minimum valid one-contract notional against `max_order_notional` and `max_visible_notional`, using Coinbase `contract_size` when present. When passive market making is selected, it also reports the expected staged quote count for the configured product/side scope and flags attention if `max_open_orders` is too low.

Then run the configured websocket feeds briefly without starting runtime, strategy, or order tasks:

```powershell
python -m app.main --config-file config.local.json --feed-smoke --feed-smoke-seconds 10 --feed-smoke-fail-on-attention
```

Then fetch account balances and CFM/eligible position state once without starting websocket, runtime, strategy, or order tasks:

```powershell
python -m app.main --config-file config.local.json --exchange-state-smoke --exchange-state-smoke-fail-on-attention
```

This command snapshots all returned balances and positions for audit. Drift comparison is scoped by `bot.reconciliation.exchange_state.position_product_ids`; the CFM templates set it to the configured product scope. `drift_count` reports current observed drift, while `new_drift_record_count` reports only newly appended drift records.

Do not run the live runtime until aggregate preflight is clean and ledger health is either clean or explicitly reviewed. Live runtime startup requires `ledger_health=ok`, or a matching operator acknowledgement for the current reviewed ledger-health attention digest, plus a clean `runtime.live_preflight_result` record for the current config fingerprint. Use `--live-runtime-preflight-max-age-seconds` when an operating session should require fresh preflight evidence. Keep `bot.strategies.enabled=false` and `bot.strategies.allow_live_execution=false` until strategy simulation and scenario fixtures pass for the selected strategy.

When live scheduled strategies are enabled, runtime startup also requires a clean `runtime.strategy_simulation_result` record for the current config fingerprint, execution mode, and selected strategy IDs. Use `--strategy-simulate-record-result` to append that evidence, and `--live-runtime-strategy-simulation-max-age-seconds` on runtime startup when qualification evidence must be fresh.

Use the read-only runtime gate immediately before startup when you want to verify the same admission checks, including ledger health, without appending an error record:

```powershell
python -m app.main --config-file config.local.json --live-runtime-gate --live-runtime-gate-fail-on-attention
```

If ledger health attention has been reviewed and should not block the next live startup, append an operator acknowledgement:

```powershell
python -m app.main --ledger-path data\cfm-live-audit.jsonl --ledger-health-acknowledge --ledger-health-acknowledged-by "$env:USERNAME" --ledger-health-acknowledgement-reason "Reviewed ledger-health attention before live startup"
```

An acknowledgement is scoped to the current ledger-health attention digest and reviewed ledger prefix. It becomes stale when any later non-acknowledgement record is appended, and it does not make `--ledger-health` report `ok`.

## Policy Probe Strategy Qualification

Use the policy-probe template when you want to prove live strategy wiring without creating order intents. The built-in `policy-probe` strategy emits no actions; it only records strategy metadata about the active operator policy and product metadata.

```powershell
python -m tools.config_template docs\examples\config.cfm-policy-probe.json config.policy-probe.json --set-json 'REPLACE_WITH_CFM_PRODUCT_IDS=[\"SHB-26JUN26-CDE\",\"AVA-29MAY26-CDE\"]' --ledger-path data\cfm-policy-probe-audit.jsonl --force
python -m app.main --config-file config.policy-probe.json --live-no-order-preflight --live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention
python -m app.main --config-file config.policy-probe.json --strategy-simulate --strategy-simulate-record-result --strategy-simulate-fail-on-attention
python -m app.main --config-file config.policy-probe.json --live-runtime-gate --live-runtime-gate-fail-on-attention
```

The policy-probe template enables the strategy schedule and live strategy approval, but the selected strategy has no order intents. The inline operator policy uses a 60-second order-book freshness threshold for no-order wiring checks because quiet CFM books may not emit level2 updates every five seconds, and aggregate preflight runs exchange-state smoke after feed smoke. Tighten that threshold only after observing the selected products under feed smoke.

## Passive Market-Making Qualification

Use the passive market-making template when you want the first staged hidden quote workflow. It enables live-mode strategy scheduling because the runtime is reading live feeds and live account state, but the selected strategy emits staged placements only. The template sets `staged_or_hidden_release.allow_release=false`; do not enable release placement or add `staged-release-manager` until the separate visible-release behavior is intentionally being tested.

```powershell
python -m tools.config_template docs\examples\config.cfm-passive-market-making.json config.passive-mm.json --set-json 'REPLACE_WITH_CFM_PRODUCT_IDS=[\"SHB-26JUN26-CDE\",\"AVA-29MAY26-CDE\"]' --ledger-path data\cfm-passive-mm-audit.jsonl --force
python -m app.main --config-file config.passive-mm.json --live-no-order-preflight --live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention
python -m app.main --config-file config.passive-mm.json --strategy-simulate --strategy-simulate-record-result --strategy-simulate-fail-on-attention
python -m app.main --config-file config.passive-mm.json --live-runtime-gate --live-runtime-gate-fail-on-attention
python -m app.main --config-file config.passive-mm.json --stop-after-task strategies.evaluate --max-cycles 10 --runtime-fail-on-attention
```

The strategy simulation should preview staged `place_order` actions, not `release` placements. If it previews visible release placement, the selected strategy IDs or policy have changed and should be reviewed before runtime startup.

Use `--stop-after-task strategies.evaluate` for bounded live strategy checks instead of manually counting scheduled task cycles. `--max-cycles` can still be supplied as a safety cap if the target task does not run. Add `--runtime-fail-on-attention` so the command appends a compact `runtime.health_check_result` record and returns an attention exit code when the run leaves ledger health in an attention state.

After a dry-run or staged runtime evaluation, inspect the replayed quote state before considering any visible release workflow:

```powershell
python -m app.main --ledger-path data\cfm-passive-mm-audit.jsonl --ledger-summary
python -m app.main --ledger-path data\cfm-passive-mm-audit.jsonl --source-of-truth
python -m app.main --ledger-path data\cfm-passive-mm-audit.jsonl --ledger-health
```

Ledger summary includes passive quote counts and the latest runtime health-check result. The source-of-truth projection includes `passive_market_making_quotes` keyed by staged placement ID, `unreleased_passive_market_making_quote_ids`, and `runtime_health_check_results`. Ledger health validates runtime health result records and includes passive quote details under `order_lineage_contract`, including product, side, bid/mid/ask ordering, positive spread, and side-specific limit price consistency, so unreleased staged quotes are visible without parsing raw strategy metadata.

## Passive Market-Making Visible Release Qualification

Use the release template only after the passive market-making ledger contains reviewed unreleased staged quotes. This template selects `staged-release-manager`, enables `staged_or_hidden_release.allow_release=true`, and keeps `max_releases_per_evaluation=1` with `allow_live_overlap=false`. Use the same ledger path as the staged PMM run if the goal is to release those staged quotes. Because staging and release use different config fingerprints on the same source ledger, the release preflight requires an explicit reviewed config-transition flag.

```powershell
python -m tools.config_template docs\examples\config.cfm-passive-market-making-release.json config.passive-mm-release.json --set-json 'REPLACE_WITH_CFM_PRODUCT_IDS=[\"SHB-26JUN26-CDE\",\"AVA-29MAY26-CDE\"]' --ledger-path data\cfm-passive-mm-audit.jsonl --force
python -m app.main --config-file config.passive-mm-release.json --live-no-order-preflight --readiness-allow-config-fingerprint-mismatch --live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention
python -m app.main --config-file config.passive-mm-release.json --strategy-simulate --strategy-simulate-record-result --strategy-simulate-fail-on-attention
python -m app.main --config-file config.passive-mm-release.json --live-runtime-gate --live-runtime-gate-fail-on-attention
```

`--readiness-allow-config-fingerprint-mismatch` only downgrades the latest-ledger-startup fingerprint mismatch in the readiness step; it does not suppress placeholder, credential, risk, websocket, product-catalog, strategy, or ledger-path attention. The readiness payload still records both fingerprints and marks `ledger_config_fingerprint_mismatch_allowed=true`.

The strategy simulation should preview at most one `release` placement and should reference a reviewed staged placement ID from the source-of-truth projection. If it previews zero actions, there is no currently releasable staged quote under the configured policy and current order book. If it previews more than one action, the release template was changed and should be reviewed before runtime startup.

The next command can submit a visible live order. It should only be used after the no-order preflight, recorded simulation result, and runtime gate are clean for the same config fingerprint:

```powershell
python -m app.main --config-file config.passive-mm-release.json --stop-after-task strategies.evaluate --max-cycles 10 --runtime-fail-on-attention
```

After a visible-release run, inspect summary, source of truth, ledger health, and Coinbase open orders. A successful release should create normal action lifecycle records, a submitted release placement linked back to the staged placement, and later user-channel or recovery reconciliation records. Do not continue releasing additional staged quotes while ledger health reports staged-release anomalies, execution uncertainty, or missing order confirmation.

## Audited Operator Place Order

Use the one-shot operator place-order command for the first canary order. It submits exactly one typed order intent through the normal action gateway, risk gate, and configured executor. It does not start scheduled runtime tasks or websocket feeds.

Run the command in dry-run mode first:

```powershell
python -m app.main --config-file config.local.json --operator-place-order --operator-id "$env:USERNAME" --operator-place-product-id "SHB-26JUN26-CDE" --operator-place-side buy --operator-place-size "1" --operator-place-limit-price "100" --operator-place-leverage "1" --operator-place-order-type limit --operator-place-time-in-force good_until_cancelled --operator-place-post-only --operator-place-reason "Operator dry-run canary"
```

The JSON response includes `receipt`, `logical_order_id`, `client_order_id`, `exchange_order_id` when available, and `status`. A non-`ok` status means the gateway, risk gate, or executor rejected or failed the action and the operator should stop before live placement.

For live Coinbase use, run the same command only after readiness, no-order preflight, strategy simulation qualification, and live runtime gate are clean for the same config. Live submission still requires the configured live approval environment variable:

```powershell
$env:STATERAIL_ALLOW_LIVE_TRADING = "true"
python -m app.main --config-file config.local.json --operator-place-order --operator-id "$env:USERNAME" --operator-place-product-id "SHB-26JUN26-CDE" --operator-place-side buy --operator-place-size "1" --operator-place-limit-price "100" --operator-place-leverage "1" --operator-place-order-type limit --operator-place-time-in-force good_until_cancelled --operator-place-post-only --operator-place-reason "Operator live canary"
```

Use a product allowed by the local risk policy and avoid BTC futures/perpetual products for the initial Coinbase operator test scope. After a live canary, immediately inspect tracked open orders, cancel the canary if it remains open, replay the ledger, and run ledger health.

## Audited Operator Cancel

List currently tracked open orders before cancelling. This command verifies and replays the ledger, starts no runtime tasks, calls no exchange endpoints, and writes nothing.

```powershell
python -m app.main --config-file config.passive-mm-release.json --operator-open-orders
python -m app.main --config-file config.passive-mm-release.json --operator-open-orders --operator-open-orders-product-id "SHB-26JUN26-CDE"
```

Use the operator cancel command to close tracked live orders without bypassing the ledger. It does not start scheduled runtime tasks or websocket feeds. By default the single-order form only cancels an order that is already open in the replayed source-of-truth projection.

```powershell
$env:STATERAIL_ALLOW_LIVE_TRADING = "true"
python -m app.main --config-file config.passive-mm-release.json --operator-cancel-order --operator-cancel-exchange-order-id "REPLACE_WITH_EXCHANGE_ORDER_ID" --operator-id "$env:USERNAME" --operator-cancel-reason "Operator cancelled reviewed live order"
```

The command writes the normal `action.requested`, `action.accepted`, `action.execution_started`, and `action.executed` or `action.execution_failed` records through the same gateway and REST executor used by strategies. It returns attention without writing if no matching open order is found, unless `--operator-cancel-allow-untracked` is supplied. Use untracked cancel only for emergency exchange cleanup because the projection cannot close an order it never knew about.

To cancel every currently tracked open order, use the batch form. It replays the ledger, filters to open orders, and submits one normal cancel action per matched order. `--operator-cancel-product-id` is optional and limits the batch to a single product.

```powershell
$env:STATERAIL_ALLOW_LIVE_TRADING = "true"
python -m app.main --config-file config.passive-mm-release.json --operator-cancel-all-open-orders --operator-cancel-product-id "SHB-26JUN26-CDE" --operator-id "$env:USERNAME" --operator-cancel-reason "Operator emergency cleanup"
```

An empty batch is an `ok` no-op and writes nothing. A batch returns attention if any matched order cannot be cancelled or if any matched open order remains open after the submitted cancels replay.

After a cancel, run:

```powershell
python -m app.main --ledger-path data\cfm-passive-mm-audit.jsonl --ledger-summary
python -m app.main --ledger-path data\cfm-passive-mm-audit.jsonl --ledger-health
```

## Ledger Inspection

Summarize replayed state:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-summary
```

Export raw verified records and export digest:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-export
```

Export the replayed source-of-truth projection:

```powershell
python -m app.main --ledger-path data/audit.jsonl --source-of-truth
```

Run ledger health:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-health
```

Fail with exit code `2` when health requires attention:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-health --ledger-health-fail-on-attention
```

## Checkpoints, Anchors, And Archives

Append a checkpoint:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-checkpoint
```

Publish a local development anchor:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-anchor-dir anchors
```

Publish an S3 Object Lock checkpoint anchor:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-anchor-s3-bucket my-object-lock-bucket --ledger-anchor-s3-mode compliance --ledger-anchor-s3-retention-days 2555
```

If a remote publish fails after the local checkpoint was appended, publish that existing checkpoint instead of appending a replacement checkpoint:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-anchor-latest-checkpoint --ledger-anchor-s3-bucket my-object-lock-bucket --ledger-anchor-s3-mode compliance --ledger-anchor-s3-retention-days 2555
```

Publish a full ledger archive to S3 Object Lock:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-archive-s3-bucket my-object-lock-bucket --ledger-archive-s3-mode compliance --ledger-archive-s3-retention-days 2555
```

Verify remote S3 receipts during health:

```powershell
python -m app.main --ledger-path data/audit.jsonl --ledger-health --ledger-health-verify-s3-anchors --ledger-health-verify-s3-archives
```

Minimum IAM permissions for the S3 Object Lock adapter are scoped to bucket configuration preflight, object upload with object-level retention, and read-back verification. Replace the bucket name, account ID, and prefixes for the target environment:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VerifyObjectLockBucketConfiguration",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketObjectLockConfiguration",
        "s3:GetBucketVersioning"
      ],
      "Resource": "arn:aws:s3:::my-object-lock-bucket"
    },
    {
      "Sid": "WriteAndVerifyRetainedAuditObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectRetention",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:PutObjectRetention"
      ],
      "Resource": [
        "arn:aws:s3:::my-object-lock-bucket/audit-anchors/*",
        "arn:aws:s3:::my-object-lock-bucket/audit-ledger-archives/*"
      ]
    }
  ]
}
```

## Attention Required

Treat `attention_required` as an operational stop sign until reviewed. It can mean the hash chain is invalid, a contract was violated, a live order lacks expected confirmation, product metadata is missing, feed redundancy is degraded, or retained anchor/archive evidence does not verify.

For live execution, do not continue by deleting or editing ledger records. Append corrective events through existing recovery or operator workflows, then rerun health.

## Safe Local Cleanup

Local development ledgers and local anchors are disposable only when they are not being used as evidence. Do not delete production ledgers or retained artifacts. This repo intentionally keeps destructive cleanup out of normal commands.
