# Configuration

Configuration is intentionally strict. Unknown JSON fields, unknown `STATERAIL_` environment variables, invalid enum values, duplicate IDs, and non-positive intervals fail before runtime starts. Legacy `COINBASE_BOT_` project variables are rejected with a migration error; rename them to the matching `STATERAIL_` variables.

## Loading Order

`app.main` loads configuration in this order:

1. `--config-file path.json`, when provided.
2. `STATERAIL_` environment variables, when any known project variable is present.
3. Built-in dry-run defaults.

`--ledger-path` can override the loaded ledger path in all cases.

## Minimal Dry-Run Config

The tested example file is [docs/examples/config.dry-run.json](examples/config.dry-run.json).

```json
{
  "ledger_path": "data/audit.jsonl",
  "bot": {
    "rest": {
      "execution_mode": "dry_run"
    },
    "risk": {
      "allowed_products": ["BTC-USD"],
      "allowed_order_types": ["limit"],
      "max_order_size": "1",
      "max_order_notional": "1000"
    }
  }
}
```

Run it:

```powershell
python -m app.main --config-file config.local.json --max-cycles 1
```

Or run the checked-in example directly:

```powershell
python -m app.main --config-file docs\examples\config.dry-run.json --max-cycles 1
```

## CFM Live Config Template

The tested CFM live template is [docs/examples/config.cfm-live.json](examples/config.cfm-live.json). The tested no-order strategy wiring template is [docs/examples/config.cfm-policy-probe.json](examples/config.cfm-policy-probe.json). The tested anchor repricing dry-run template is [docs/examples/config.anchor-repricing-manager.dry-run.json](examples/config.anchor-repricing-manager.dry-run.json). The tested passive market-making dry-run template is [docs/examples/config.passive-market-making.dry-run.json](examples/config.passive-market-making.dry-run.json). The tested CFM passive market-making template is [docs/examples/config.cfm-passive-market-making.json](examples/config.cfm-passive-market-making.json). The tested CFM passive market-making release template is [docs/examples/config.cfm-passive-market-making-release.json](examples/config.cfm-passive-market-making-release.json).

For guided local config creation, run the config wizard:

```powershell
python -m tools.config_wizard
```

The wizard renders one of the checked templates, asks only for the missing local fields such as product IDs and ledger path, validates the rendered file with the real config loader, and prints the next operator commands. It does not ask for or write Coinbase credentials. For automation, the same tool can run without prompts:

```powershell
python -m tools.config_wizard --profile coinbase_cfm_staged_passive_market_making --target config.local.json --ledger-path data\operator-coinbase-audit.jsonl --products "SHB-26JUN26-CDE,AVA-29MAY26-CDE" --force --no-input
```

Before using it, replace every `REPLACE_WITH_...` value, configure Coinbase credentials through environment variables, and run readiness:

```powershell
python -m tools.config_template docs\examples\config.cfm-live.json config.local.json --set-json 'REPLACE_WITH_CFM_PRODUCT_IDS=[\"SHB-26JUN26-CDE\",\"AVA-29MAY26-CDE\"]' --ledger-path data\cfm-live-audit.jsonl --force
$env:COINBASE_API_KEY = "organizations/{org_id}/apiKeys/{key_id}"
$env:COINBASE_API_SECRET = "<private key contents>"
$env:STATERAIL_ALLOW_LIVE_TRADING = "true"
python -m app.main --config-file config.local.json --live-no-order-preflight --live-no-order-preflight-feed-seconds 10 --live-no-order-preflight-fail-on-attention
```

`--set-json` accepts JSON replacement values. The PowerShell examples escape JSON quotes so Python receives valid JSON. When a placeholder is the only item in a JSON list, a replacement list is spliced into that list. This is how one CFM template supports both one-product and multi-product scopes.

Do not start live runtime tasks until aggregate no-order preflight is clean and ledger health is either clean or explicitly reviewed. The aggregate preflight runs readiness, product-catalog smoke, feed smoke, and exchange-state smoke in order. Product-catalog smoke confirms the configured products are supported CFM `FCM` products, writes audited `exchange.product_snapshot` records, and reports `policy_viability` for minimum futures contract notional versus configured caps plus passive-market-making staged quote capacity versus `max_open_orders`. Feed smoke confirms websocket connectivity/data flow and writes normal feed lifecycle/data events. Exchange-state smoke confirms REST account/position reads behave as expected and writes balance/position snapshots plus any drift or lookup errors. Its `drift_count` reports current observed scoped drift; `new_drift_record_count` reports newly appended immutable drift records. The aggregate command never calls order endpoints and never starts strategy or live runtime tasks. Clean writable aggregate runs append `runtime.live_preflight_result`; live runtime startup requires ledger health to be `ok`, or a matching operator acknowledgement for the current reviewed ledger-health attention digest, and that preflight evidence to match the current config fingerprint. Add `--live-runtime-preflight-max-age-seconds` to the live runtime command when the preflight evidence must be recent.

If live scheduled strategies are enabled, also run strategy simulation with recorded evidence before runtime startup:

```powershell
python -m app.main --config-file config.local.json --strategy-simulate --strategy-simulate-record-result --strategy-simulate-fail-on-attention
```

That command appends `runtime.strategy_simulation_result` after the no-order simulation. Live runtime startup requires a clean matching strategy simulation result for the current config fingerprint, execution mode, and selected strategy IDs. Add `--live-runtime-strategy-simulation-max-age-seconds` when strategy qualification evidence must be recent.

Before starting live runtime tasks, run the read-only admission gate:

```powershell
python -m app.main --config-file config.local.json --live-runtime-gate --live-runtime-gate-fail-on-attention
```

It reports the same live safety, aggregate no-order preflight, and strategy simulation gates used by startup without starting runtime tasks.

Readiness reports `config_placeholders` when any normalized config value still starts with `REPLACE_WITH_`. Live runtime preflight blocks those placeholders even when credentials and operator approval are present.

When intentionally changing operator modes on the same ledger, such as moving from staged passive market-making to staged release, readiness reports the new config fingerprint as a mismatch against the ledger's latest startup fingerprint. Use `--readiness-allow-config-fingerprint-mismatch` only for that reviewed transition. The check still reports both fingerprints and the allowance flag in the payload; other readiness failures are unaffected.

The checked-in CFM template configures two market-data websocket sources and two user-order websocket sources for the same product scope. Keep that shape unless readiness is changed deliberately; a single websocket source for a live scope is reported as attention-required. The CFM template intentionally omits `perpetual_portfolio_uuid` because that field enables INTX position lookup. Add it only in an INTX-specific extension.

## Important Blocks

`bot.rest` controls Coinbase REST base URL, execution mode, retry policy, and portfolio IDs.

`bot.risk` controls product allowlists, order-type allowlists, maximum size/notional/leverage, reduce-only mode, and kill switch.

When product catalog metadata includes a futures `contract_size`, risk and sizing treat notional as `size * limit_price * contract_size`. This matters for CFM products: a displayed low price does not imply a low one-contract notional. Keep `max_order_notional`, operator-policy `max_order_notional_usd`, and staged-release `max_visible_notional_usd` above the product's minimum one-contract notional if a strategy is expected to produce live-visible orders.

`bot.websocket_sources` defines product-scoped and user websocket feeds. Product channels require `product_ids`. The user channel must use the user endpoint and requires Coinbase JWT credentials.

`bot.feed` controls redundant feed liveness: minimum live sources, stale threshold, reconnect policy, and the feed-health schedule.

`bot.product_catalog` controls the audited product metadata refresh task. Live execution requires product catalog metadata so venue restrictions are replay-backed.

`bot.strategies` controls scheduled strategy evaluation. It does not define strategy code. Strategy implementations are registered by application code or installed package entry points and selected by explicit `strategy_ids`. The built-in `noop` strategy can be used to smoke-test the harness without emitting orders. Live strategy evaluation is blocked unless `allow_live_execution` is explicitly true.

`bot.strategies.market_data_requirements` is an optional fail-closed input gate. Each requirement names a `product_id`, generic `data_kind` (`ticker`, `order_book`, or `trade`), and `max_age_seconds`. If any configured requirement is missing or stale, the strategy harness records a failed strategy evaluation before strategy code runs and before any action can be submitted. Simulation applies the same gate.

`bot.strategies.operator_policy_file` loads a checked operator-policy JSON file. `bot.strategies.operator_policy` can also contain the policy inline. The effective action gateway risk policy is the stricter merge of `bot.risk` and the operator policy, so violations become audited action rejections in both simulation preview and runtime submission.

`bot.strategies.strategy_parameters` is a typed parameter map for built-in strategies. The current supported keys include `passive-market-making`, with `target_notional_usd`, `half_spread_bps`, `max_products_per_evaluation`, and `max_staged_release_count_per_side`, and manager strategies such as `staged-release-manager`, with `max_releases_per_evaluation` and `allow_live_overlap`. Parameters for unselected strategies or unsupported strategy IDs fail during runtime assembly instead of being ignored.

Operator policy `order_behavior.default_leverage` and `order_behavior.default_margin_type` are optional execution defaults for strategies that create new CFM futures placements. Keep `bot.risk.max_leverage` as a ceiling, not as an order default. CFM strategy templates use `default_leverage: "1"` and `default_margin_type: "cross"` so staged placements can later be released without losing required futures order fields.

For environment config, set `STATERAIL_STRATEGIES_MARKET_DATA_REQUIREMENTS` to a JSON list with the same fields. Set `STATERAIL_STRATEGIES_PARAMETERS` to a JSON object keyed by strategy ID. Set `STATERAIL_STRATEGIES_OPERATOR_POLICY_FILE` to load a policy file from the operator environment.

Readiness reports unresolved strategy IDs and invalid built-in strategy parameters before runtime assembly. Entry point names are inspected without importing strategy code. If the built-in `staged-release-manager` is selected for scheduled evaluation, readiness also reports attention when no operator policy is configured, when `staged_or_hidden_release.enabled=false`, when `staged_or_hidden_release.allow_release=false`, or when condition-matched release is enabled without required order-book input. If the built-in `followup-on-fill-manager` is selected, readiness reports attention when no operator policy is configured, followup policy is disabled, or product catalog refresh is disabled. If the built-in `consolidation-manager` is selected, readiness reports attention when no operator policy is configured, merge/consolidation lineage is disabled, or product catalog refresh is disabled. If the built-in `anchor-repricing-manager` is selected, readiness reports attention when no operator policy is configured, anchor repricing is disabled, same-side moves are disabled, cancel-replace fallback is disabled, order-book input is not required, or product catalog refresh is disabled. If the built-in `passive-market-making` strategy is selected, readiness reports attention when no operator policy is configured, staged release is disabled, the default order type is not limit, post-only is disabled, order-book input is not required, or product catalog refresh is disabled. Passive market-making does not require `allow_release=true` because it creates staged placements only.

`--strategy-simulate` verifies the existing ledger and evaluates configured `strategy_ids` without starting runtime tasks. It previews emitted actions through the same gateway validation and risk checks used by live execution. Ordered decisions are evaluated against a temporary replay ledger, so later previews can see earlier accepted dry-run cancels or placements. The command does not append records to the operator ledger or call Coinbase. Add `--strategy-simulate-fail-on-attention` in CI or operator checks so contract failures and would-be rejected actions return a non-zero exit code. Use this before enabling `bot.strategies.enabled`.

`--strategy-scenario-file` loads a typed JSON scenario fixture, writes its events to the temporary ledger named by `--ledger-path`, replays that ledger, and evaluates configured strategy IDs through the same simulation path. A scenario may declare its own `strategy_ids` plus `static_strategies` when the fixture needs explicit order intents without an installed plugin. If the fixture includes `exchange.product_snapshot` events, those replayed products are used for strategy snapshots and risk-preview product metadata even when runtime product-catalog refresh is disabled. The scenario ledger must be empty or absent. Scenario expectation failures return exit code `2`, so this command is suitable for CI regression fixtures.

`--operator-policy-scenarios-file` runs a checked operator-policy scenario fixture without starting runtime tasks, writing to the ledger, or calling Coinbase. It is for executable policy examples such as safety stops, stale-data blocking, followup sizing, consolidation, and deterministic policy calculations. Scenarios that imply automatic order creation but do not yet have a concrete strategy contract are reported as `documented_only`, not silently treated as implemented behavior.

`bot.reconciliation` controls watchdog, order recovery, fill reconciliation, and exchange-state reconciliation schedules and policies. `bot.reconciliation.exchange_state.position_product_ids` limits drift comparison to the bot's configured product scope while still snapshotting every returned exchange position for audit. If omitted, assembly derives the drift scope from risk allowlists, product-catalog IDs, and websocket product IDs.

`bot.audit_anchor` controls periodic checkpoint anchoring. `bot.audit_archive` controls periodic full-ledger archive publishing.

`bot.triggers` and `bot.trigger_polling` define time and message triggers without strategy code.

Example no-op strategy harness schedule:

```json
{
  "bot": {
    "strategies": {
      "enabled": true,
      "interval_seconds": 5,
      "run_on_start": true,
      "allow_live_execution": false,
      "strategy_ids": ["noop"]
    }
  }
}
```

Example strategy input gate:

```json
{
  "bot": {
    "strategies": {
      "strategy_ids": ["my-strategy"],
      "market_data_requirements": [
        {
          "product_id": "BTC-USD",
          "data_kind": "ticker",
          "max_age_seconds": 5
        }
      ]
    }
  }
}
```

Example operator policy file:

```json
{
  "bot": {
    "strategies": {
      "strategy_ids": ["my-strategy"],
      "operator_policy_file": "docs/examples/operator-policy.conservative-cfm-v0.json"
    }
  }
}
```

Example built-in staged release manager selection:

```json
{
  "bot": {
    "strategies": {
      "strategy_ids": ["staged-release-manager"],
      "operator_policy_file": "docs/examples/operator-policy.conservative-cfm-v0.json"
    }
  }
}
```

Example built-in strategy parameters:

```json
{
  "bot": {
    "strategies": {
      "strategy_ids": [
        "passive-market-making",
        "staged-release-manager",
        "followup-on-fill-manager",
        "consolidation-manager",
        "anchor-repricing-manager"
      ],
      "strategy_parameters": {
        "passive-market-making": {
          "target_notional_usd": "5",
          "half_spread_bps": "50",
          "max_products_per_evaluation": 1,
          "max_staged_release_count_per_side": 1
        },
        "staged-release-manager": {
          "max_releases_per_evaluation": 1,
          "allow_live_overlap": false
        },
        "followup-on-fill-manager": {
          "max_followups_per_evaluation": 1
        },
        "consolidation-manager": {
          "max_consolidations_per_evaluation": 1,
          "max_source_orders_per_consolidation": 2
        },
        "anchor-repricing-manager": {
          "max_moves_per_evaluation": 1
        }
      }
    }
  }
}
```

Manager strategy limits bound how many actions can be emitted in one evaluation. Keep `staged-release-manager.allow_live_overlap` false unless the strategy is explicitly allowed to release a staged placement while another live placement already exists for the same logical order.

Custom strategies should emit `PlaceOrderIntent`, `CancelOrderIntent`, or `ActionCommand` values and let the harness submit them through the action gateway. Use the strategy ID helpers for stable action IDs and venue client order IDs. Do not put credentials, strategy secrets, or dynamic import paths in public config.

## Credentials

Coinbase credentials are loaded separately from config and are not serialized into startup config snapshots:

```powershell
$env:COINBASE_API_KEY = "organizations/{org_id}/apiKeys/{key_id}"
$env:COINBASE_API_SECRET = "<private key contents>"
```

`COINBASE_API_SECRET` is the private key content itself, not a file path. Escaped `\n` sequences are converted back to newlines before signing. File-based private keys are still supported through `STATERAIL_COINBASE_API_PRIVATE_KEY_FILE`.

Live REST execution also requires explicit operator approval:

```powershell
$env:STATERAIL_ALLOW_LIVE_TRADING = "true"
```

## S3 Object Lock

S3 Object Lock adapters require the optional AWS dependency:

```powershell
pip install ".[aws]"
```

For production immutability, prefer full ledger archives in addition to checkpoint anchors. Anchors prove a checkpoint; archives store the replayable ledger record prefix under Object Lock.

Example archive store:

```json
{
  "bot": {
    "audit_archive": {
      "enabled": true,
      "interval_seconds": 86400,
      "run_on_start": true,
      "store": {
        "provider": "aws_s3_object_lock",
        "bucket": "my-object-lock-bucket",
        "expected_bucket_owner": "123456789012",
        "key_prefix": "staterail/ledger-archives",
        "immutability_mode": "compliance",
        "retention_days": 2555,
        "verify_bucket_configuration": true
      }
    }
  }
}
```

Use `governance` only when your operating model explicitly allows privileged retention bypass. Use `compliance` when retained records must not be bypassable by ordinary administrators.

## Live Execution Checklist

- Keep `STATERAIL_ALLOW_LIVE_TRADING` unset until you are intentionally testing live execution.
- Configure product allowlists and order-type allowlists.
- Configure max size, max notional, and max leverage where applicable.
- Enable product catalog refresh for live execution.
- Keep strategy evaluation disabled until strategy IDs, action sizing, and risk limits have been tested in dry-run.
- For staged/hidden order workflows, test both staged placement creation and later `release` intents in dry-run or simulation before enabling live strategy evaluation.
- Run `--strategy-simulate` against a verified ledger before enabling scheduled strategy evaluation.
- Set `bot.strategies.allow_live_execution` to `true` only when strategy evaluation is intentionally allowed to submit live actions through the gateway.
- Run readiness before starting runtime tasks.
- Use CFM/spot product metadata only for current live routing. INTX live routing is not currently supported.
