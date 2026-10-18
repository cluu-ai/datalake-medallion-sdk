# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Breaking changes to the client API: the next release is 0.2.0.

### Changed (breaking)

- Micro-tenancy: the client no longer has a bucket or prefixes. Each `ingest(batch, tenant=..., contract=...)` names its tenant, as a `TenantProfile` or as an id resolved through `tenants=` (a `TenantRegistry` or a function `(tenant_id) -> TenantProfile`). The profile fixes the tenant's Bronze and quarantine locations (`s3://bucket/prefix`, any bucket), optional Silver and Gold locations, the partition order and the PII protection. Profiles are validated when built: required layers, safe prefixes, valid buckets, and no two layers sharing or nesting a location.
- PII is stored as received by default. Protection is set per tenant with `PiiProtection.sha256()`, `.hmac_sha256(key)`, `.aes_256_gcm(data_key, key_ref=...)` or `.custom(obj)`. The `pii_masking` and `pii_hmac_key` client options and the per-call `pii_masking` override are removed.
- `_dl_pii_masking` is renamed `_dl_pii_protection`; `IngestReport.pii_masking` is renamed `pii_protection`; `IngestReport.tenant_id` is new. Dead letters record `pii_protection`.
- Dead letters go to the tenant's `quarantine` location (`quarantine/` by default, was `dlq/`).
- Rust: `BronzePipeline::new` no longer takes a path resolver, `IngestRequest` carries an `Arc<TenantProfile>`, and `ObjectStorage::put` receives the full object path including the bucket. `BronzePathResolver` and `PiiHasher` are replaced by `TenantProfile` and `FieldProtector`.

### Added

- AES-256-GCM envelope encryption for PII with a host-provided data key (for example an AWS KMS data key per tenant). Values are authenticated with their tenant and field; the key reference is stored in the new `_dl_pii_key_ref` column. `PiiProtection.decrypt` reads values back.
- `FieldProtector` (Rust trait and Python protocol) for custom protection schemes, called once per field and batch. A failing protector raises `ProtectionError` and nothing is written.
- `TenantRegistry`, `TenantProfile.location()` and `TenantProfile.table_uri()` for resolving tenant locations, including Silver and Gold.
- `examples/throughput.rs`: end-to-end throughput and peak heap on a large batch for each input type and protection scheme.
- `PreparedBatch::encoded_bytes()`: size of the objects a commit will upload.
- Rust: the Python bindings are behind the default `python` feature. Rust applications (for example an AWS Lambda) can depend on the crate with `default-features = false` and need no Python interpreter or `libpython`.

### Performance

- Lineage columns that hold one value per batch are built as one-entry dictionaries (one byte per row instead of a copy of the value) and ingest ids are formatted without a heap allocation per row. On a 200 MiB Arrow batch this halves peak memory (335 to 173 MiB) and cuts time by 16%; Parquet output is unchanged in size.
- Parquet files no longer embed the Arrow schema; column types come from the Parquet logical types (`string`, `int64`, `double`, `boolean`, `timestamp[us, UTC]`), so every reader sees plain strings.
- Without PII protection (the new default), Arrow ingestion is 25% faster and JSON ingestion 15% faster than with SHA-256 hashing.
- Columnar fast path for Arrow input. When every rule of a contract applies to one field at a time (`type`, `required`, `enum`, `const`, numeric bounds, `multipleOf`, string length, `pattern`, `format`, PII), Arrow batches are validated, masked and stored column by column, and only the rows that may be invalid are converted to JSON and checked as before. Bronze rows, dead letters and reports are identical to the row path (enforced by a property test); other contracts, hooks and JSON input keep the row path.

### Changed

- Upgraded jsonschema to 0.58, ulid to 3, sha2 to 0.11, hmac to 0.13 and criterion to 0.8. Contract compilation still performs no network or file I/O, and every built-in string format (including `idn-email` and `idn-hostname`) stays asserted; a test now fails if an upgrade drops one.
- Lineage ids (`_dl_ingest_id`) stay strictly increasing within a batch even when more ids are generated in one millisecond than the random part can hold.
- Python dict and list input is serialized without whitespace before it reaches the core, so `max_record_bytes` and the dead-letter `payload_bytes` measure the same bytes for dict, JSON and Arrow input.

## [0.1.0] - 2026-10-17

First public release.

### Added

- `ContractRegistry`: compiles JSON Schema contracts at boot (all-or-nothing), with SDK keywords under a configurable extension prefix (`x-dl-` by default). Remote and file `$ref` resolution is disabled.
- `BronzeIngestionClient` (Python) and `BronzePipeline` (Rust): one validation pipeline for every input — tenant id, payload size and reserved fields, JSON Schema with format assertion, strict/permissive/schema-on-read modes, host hooks with re-validation of rewritten records, future-timestamp guard on `date-time` fields, optional PII hashing, contract-derived Arrow types and lineage columns.
- PII pseudonymization with SHA-256 or keyed HMAC-SHA256, applied to Bronze data and to dead-letter entries. On by default; configurable per client (`pii_masking`) and overridable per `ingest` call. The applied scheme is recorded in `_dl_pii_masking` and `IngestReport.pii_masking`.
- Dead-letter objects grouped by error code (`dlq/…/error_code=…/<batch_id>.jsonl`), written before the data object so retries never duplicate Bronze rows.
- `on_invalid="raise"` fail-fast mode with typed exceptions (`SchemaViolationError`, `HookError`, `SemanticViolationError`, `TypeCoercionError`, …).
- Arrow input through the PyCapsule interface (pyarrow, Polars) without a pyarrow dependency.
- `HookRegistry`, `@datalake_retry` (retries `StorageError` only, with jitter), `@emit_metrics` with logging, CloudWatch EMF and in-memory sinks.
- `ObjectStorage` port with S3 and in-memory adapters; the S3 client is built once and reused.

### Changed (from internal pre-releases)

- Package renamed to `datalake-medallion-sdk` (import `datalake_medallion_sdk`); licensed MIT OR Apache-2.0.
- Lineage is stored as flat `_dl_*` columns (prefix configurable) instead of a nested envelope.
- Parquet schemas are derived from the contract instead of inferred per batch.
- Tenant ids are validated by character set and structure; substring blocklists that rejected valid names were removed.
- Minimum supported Rust version is 1.94.1 (required by the AWS SDK).

### Removed (from internal pre-releases)

- PostgreSQL hot/cold client, tenant database router and migrations.
- Kinesis Firehose client.
- AWS Glue catalog helper.
- Context builder, client factory and strategy enum (one client remains).
- Mandatory `pyarrow`, `polars` and `pydantic` dependencies.
