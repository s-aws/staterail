# Contributing

This project is built around a durable audit ledger. Contributions should preserve replayability, typed boundaries, and one code path per behavior.

## Contribution License

This project is licensed under the Apache License 2.0. Unless explicitly stated otherwise, intentionally submitted contributions are provided under the same license.

## Development Setup

Use Windows PowerShell from the repository root:

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

## Rules

- Use enums from `core/enums.py` for public event names, statuses, rule names, modes, and other durable strings.
- Emit audit events before accepting irreversible decisions.
- Add projection support for new durable events.
- Add ledger health checks for replay invariants.
- Keep one code path per behavior; do not add side stores for source-of-truth state.
- Keep credentials and secrets out of serialized config snapshots, audit payloads, examples, and tests.
- Preserve thread-safety around existing locks.
- Do not let strategies call exchange clients directly. Strategies should submit audited intents.
- Keep INTX live routing disabled until it has separate eligibility assumptions, config, readiness checks, tests, and documentation.

## Verification

For behavior-affecting changes, run:

```powershell
pytest tests/regression/ -v
```

When package metadata changes, run a dry-run install check:

```powershell
python -m pip install -e . --dry-run
```

## Public Release Note

Public release candidates must include [LICENSE](LICENSE) and [SECURITY.md](SECURITY.md).
