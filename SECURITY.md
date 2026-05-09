# Security Policy

This project handles trading automation infrastructure. Treat security issues as operational-risk reports, not ordinary bug reports.

## Supported Versions

Only the current public main branch and the latest published release are expected to receive security fixes. Older snapshots may not be patched.

## Reporting A Vulnerability

Do not open a public issue that includes exploit details, credential material, account identifiers, private ledger records, or steps that could trigger unauthorized trading behavior.

Use GitHub private vulnerability reporting if it is enabled for the public repository. If private vulnerability reporting is not available, open a public issue asking for a private maintainer contact path without including sensitive details.

Include enough non-sensitive context to reproduce and triage the issue:

- affected command, module, or configuration path
- whether live execution, dry-run execution, readiness, or ledger replay is affected
- expected impact
- minimal redacted reproduction steps

## Sensitive Data

Never include Coinbase API keys, private keys, AWS credentials, account identifiers, portfolio IDs, real ledgers, order IDs, fills, positions, balances, or downloaded account reports in a report, issue, test fixture, or public repository.

If a credential or real trading ledger is exposed, rotate the credential first. Preserve local audit evidence separately for incident review.

## No Warranty

This project is provided under the Apache License 2.0 without warranty. Security reports are reviewed on a best-effort basis.
