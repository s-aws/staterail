# Audit Log Samples

These sample logs are sanitized, deterministic examples for website demos. They are generated through the real `ActionGateway` and `AuditLedger` paths, so the JSONL file is replayable and hash-chained, but it does not contain live exchange data, account identifiers, or credentials.

## Files

- `action-handling.jsonl`: append-only JSONL sample ledger with accepted, rejected, staged, venue-rejected, and executor-failed action paths.
- `action-handling.index.json`: website-friendly index of the important sequence ranges.
- `runtime-replay-recovery.jsonl`: append-only JSONL sample ledger with restart, replay-seeded recovery, fill reconciliation, exchange-state snapshots, drift append, and duplicate suppression after restart.
- `runtime-replay-recovery.index.json`: website-friendly index of the operational sequence ranges.

## Included Cases

| Case | Sequences | Handling |
| --- | ---: | --- |
| Accepted order placement | `1-6` | Request passes risk, executes in dry-run mode, and records an accepted placement. |
| Accepted cancel | `7-10` | Cancel request executes in dry-run mode and closes the prior order. |
| Risk rejection | `11-12` | Request is audited first, then rejected by the risk gate before execution starts. |
| Staged placement | `13-16` | Request is accepted and recorded as `staged_release`; no execution is started. |
| Venue rejection | `17-21` | Request passes internal risk, execution starts, venue response rejects placement, and the action is marked failed with `execution_rejected`. |
| Executor failure | `22-27` | Request passes internal risk, execution starts, an unexpected executor error is logged, and the action is marked failed with `executor_error`. |

## Included Operational Cases

| Case | Sequences | Handling |
| --- | ---: | --- |
| Initial runtime and mismatch | `1-10` | Startup records product metadata and a live accepted placement; the watchdog later appends `reconciliation.mismatch` for missing user-channel confirmation. |
| Restart recovery and reconciliation | `11-17` | A restarted process replays the ledger, recovers the order through a REST lookup, appends one fill, snapshots account/position state, and appends one position drift. |
| Restart duplicate suppression | `18-21` | Another restart replays existing recovery/fill/drift records; repeated recovery and fill reconciliation append nothing new, while exchange-state snapshots still append fresh observations. |

## Validation

```powershell
python -m app.main --ledger-path docs\examples\audit-log-samples\action-handling.jsonl --ledger-summary
```

The expected summary includes `verified: true`, `action_count: 6`, `failed_action_count: 2`, and `error_count: 1`.

```powershell
python -m app.main --ledger-path docs\examples\audit-log-samples\runtime-replay-recovery.jsonl --ledger-summary
python -m app.main --ledger-path docs\examples\audit-log-samples\runtime-replay-recovery.jsonl --ledger-health
```

The expected summary includes `verified: true`, `record_count: 21`, `reconciliation_mismatch_count: 1`, `reconciliation_recovery_count: 1`, `fill_count: 1`, and `reconciliation_drift_count: 1`. Ledger health is expected to be `attention_required` because the sample intentionally includes an unresolved position drift.
