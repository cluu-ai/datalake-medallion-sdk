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
- **PII**: fields marked `x-dl-pii` are stored as received unless the tenant profile sets `pii_protection`. When it does, every value is replaced before encoding, in Bronze objects and in dead-letter entries alike: SHA-256 or HMAC-SHA256 digests, AES-256-GCM tokens bound to their tenant and field (with a data key the host obtains, e.g. from AWS KMS per tenant), or a custom scheme. The scheme is recorded in the `_dl_pii_protection` column and in each dead letter, so unprotected data is always identifiable; storing raw personal data is then the operator's responsibility. Unkeyed SHA-256 of low-entropy values (e-mail addresses, phone numbers) can be reversed by dictionary attack; prefer HMAC with a key stored outside the lake, or encryption. A failing protector fails the call before anything is written.
- **Key management**: with `PiiProtection.kms_envelope`, data keys come from the tenant's AWS KMS key with the encryption context `{tenant_id, purpose}`, are cached per tenant for at most one hour or 2^15 nonces, are never written or logged in clear and are wiped from memory when retired. Restrict `kms:GenerateDataKey` and `kms:Decrypt` with the `kms:EncryptionContext:tenant_id` condition so a role can only use keys of its tenant.
- **No values in diagnostics**: schema errors are rendered with instance values masked, hook exceptions are reported by type only, and logs contain counts and identifiers only.
- **Contracts are offline**: `$ref` resolution over the network or the file system is disabled.
- **Least privilege**: the SDK never creates buckets; the writer role needs `s3:PutObject` on each tenant's bronze and quarantine locations only. Credentials are never logged and are redacted from `repr`/`Debug`.

## Supply chain

- Releases are built in GitHub Actions and published with PyPI Trusted Publishing (no API tokens), with build provenance attestations.
- Actions are pinned to commit SHAs and updated by Dependabot.
- `cargo deny` (licenses, advisories, banned crates) and gitleaks run on every change; `cargo audit` runs weekly.
- Wheels embed a CycloneDX SBOM of the Rust dependencies.
