# Security policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | yes       |

## Reporting a vulnerability

Do not open a public issue. Use GitHub's private vulnerability reporting:
[Report a vulnerability](https://github.com/cluu-ai/datalake-medallion-sdk/security/advisories/new).

Include the affected version, a description, reproduction steps and the impact you expect. You will get an acknowledgement within 5 business days and a remediation plan once the report is confirmed.

## Security properties

- **Tenant isolation**: tenant ids are restricted to `[a-z0-9_-]` with structural rules, so they cannot traverse or inject into object keys. Every key carries the `tenant_id=` partition.
- **PII**: by default, fields marked `x-dl-pii` are replaced by their SHA-256 or HMAC-SHA256 digest before encoding, in Bronze objects and in dead-letter entries. Masking can be disabled per client (`pii_masking=False`) or per call; the scheme applied is recorded in the `_dl_pii_masking` column, so unmasked data is always identifiable. Disabling it means raw personal data is stored, and protecting it is then the operator's responsibility. Unkeyed SHA-256 of low-entropy values (e-mail addresses, phone numbers) can be reversed by dictionary attack; use `pii_hmac_key` with a secret stored outside the lake.
- **No values in diagnostics**: schema errors are rendered with instance values masked, hook exceptions are reported by type only, and logs contain counts and identifiers only.
- **Contracts are offline**: `$ref` resolution over the network or the file system is disabled.
- **Least privilege**: the SDK never creates buckets; the writer role needs `s3:PutObject` on the data and dead-letter prefixes only. Credentials are never logged and are redacted from `repr`/`Debug`.

## Supply chain

- Releases are built in GitHub Actions and published with PyPI Trusted Publishing (no API tokens), with build provenance attestations.
- Actions are pinned to commit SHAs and updated by Dependabot.
- `cargo deny` (licenses, advisories, banned crates) and gitleaks run on every change; `cargo audit` runs weekly.
- Wheels embed a CycloneDX SBOM of the Rust dependencies.
