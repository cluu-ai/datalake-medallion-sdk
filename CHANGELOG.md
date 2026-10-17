# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
