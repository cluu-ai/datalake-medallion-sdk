# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Lambda recipe: each S3 object announced through SQS is ingested as a batch of its own, named by its key and ETag. A duplicated or redelivered notification grouped with other messages produced a new batch id and wrote the object twice (found in an end-to-end test on AWS). `LambdaResult.reports` is now a list, since a tenant may appear in several objects.

## [0.4.0rc1] - 2026-10-30

OTLP/protobuf and compressed log requests, with the same rows as OTLP/JSON and faster to read.

### Added

- OTLP/protobuf logs: `ingest_otel_logs`, `ingest_otel_log_batch` and `otel.flatten_logs` read serialized `ExportLogsServiceRequest`s (what OTLP exporters and the Collector's `awss3exporter` write by default) as well as OTLP/JSON, with the same rows. A wire-format reader writes borrowed strings straight into the Arrow builders: no generated code, no copies.
- gzip and zstd compressed OTLP requests, detected by their magic bytes. `encoding=` (`auto`, `json`, `protobuf`) and `compression=` (`auto`, `none`, `gzip`, `zstd`) restrict what is accepted; one batch may mix them.
- `max_decompressed_mb=64`: a compressed request that expands beyond it fails with the new `PayloadTooLargeError`, a `MalformedPayloadError` (Rust: `DatalakeError::PayloadTooLarge`).
- Rust: `otel::flatten_logs` and `otel::flatten_logs_many` with `OtlpOptions`; `FlattenedRequests::encodings`. `flatten_logs_json` and `flatten_logs_json_many` remain.
- Span attribute `datalake.otlp.encoding` (`json`, `protobuf` or `mixed`) on the OTLP log ingestion spans.
- Lambda recipe: raw protobuf in Kinesis, protobuf in base64 in SQS (`content-type` message attribute or detection), and S3 objects announced through SQS, with the batch id derived from the object keys and ETags. gzip messages are decompressed with a size limit.
- `fuzz.yml`: weekly and on-demand fuzzing of the contract compiler and the OTLP reader.

### Changed

- OTLP/JSON reads like protobuf where protobuf cannot tell "unset" from a default: a zero `severityNumber` or `flags` and an all-zero trace or span id are now null. `doubleValue` also accepts the strings `"NaN"`, `"Infinity"` and `"-Infinity"` (null in the rows) instead of failing the request.
- Values (arrays and key-value lists) nested deeper than 24 levels make a request malformed, in both encodings.
- `otel.otlp_bytes` no longer rejects protobuf; it only checks the payload type (`TypeError` otherwise). `str` and mapping payloads are OTLP/JSON.

### Fixed

- Windows: the `pyarrow`, `read` and `write` extras install `tzdata`, which PyArrow needs to convert timezone-aware timestamps to Python objects.

### Security

- `rustls-webpki` 0.103.16 (RUSTSEC-2026-0382) and `ctutils` 0.4.6 (RUSTSEC-2026-0385). `aws-sdk-s3` is built without `sigv4a` (S3 Multi-Region Access Points, not used by the SDK), which removes `crypto-bigint` (RUSTSEC-2026-0387).
- CI installs Python dependencies from a hash-pinned lock (`requirements/ci.txt`) and the release smoke test installs only the locally built wheel.

## [0.3.0] - 2026-10-20

The SDK now covers the whole medallion lake: contract-checked Bronze ingestion, Silver and Gold Iceberg writes with their maintenance, and tenant-scoped reads of every layer. This release includes everything in 0.2.0rc6 to 0.3.0rc1 below.

### Changed

- README, package metadata and repository description present the SDK for Bronze, Silver and Gold.

### Fixed

- `docs/iam.md`: the least-privilege ingestion and reader policies now include the `_manifests/` prefix used with `manifests=True` and `LakeReader.changes()`; policies for `LakeWriter` and `LakeMaintenance` added.

## [0.3.0rc1] - 2026-10-20

### Added

- `LakeWriter` (extra `[write]`, PyIceberg 0.12): `append`, `upsert(keys)` and `replace(where)` of a tenant's rows in Silver and Gold Iceberg tables through a catalog (AWS Glue, REST, SQL…), and `create_table` for tables shared by tenants (required `tenant_id`, partitioned by it) or owned by one tenant (below its location). Rows of another tenant are rejected and every `upsert` / `replace` is scoped to the tenant. On a real conflict with another writer the operation is applied again on the new state; `batch_id` makes writes idempotent. `WriteResult` with inserted, updated and deleted rows.
- `LakeMaintenance`, for scheduled jobs outside the write path: `compact` rewrites small files per partition and leaves a partition a writer changed meanwhile for the next run; `expire_snapshots(older_than_hours, retain_last)`.
- `LakeReader.read_changes(tenant, table, changes)`: the rows of the objects of a `ChangeSet`, refusing objects of another tenant; with `LakeWriter(batch_id=...)` it makes incremental Bronze to Silver jobs exactly-once.
- `LakeWriter.upsert(partition_scoped=True)` and Arrow matching for upserts of more than 1,000 keys (100 k keys over a whole table on AWS: 545 s to 14.1 s); exact counts when keys were stored twice. `LakeWriter.add_columns` for additive schema changes. `LakeMaintenance.remove_orphan_files`; `compact` reads files in parallel, works in bounded groups and stops within `max_seconds` (reports `remaining`).
- Commits rejected by the catalog (`CommitFailedException`) are applied again by the writer; new tables get more PyIceberg commit retries.
- `LakeReader(cache_mb=...)` also caches the data files of Iceberg tables (the tenant's partition of a shared table): an API pattern over Gold tables went from a p50 of 229 ms to 6.3 ms on AWS. `docs/serving-gold.md`.

### Fixed

- Upserts with thousands of composite keys: PyIceberg's own upsert nests one `Or` per key and overflowed the stack with 10 k keys (always composite in shared tables). `LakeWriter.upsert` uses one overwrite with a shallow key filter instead.

## [0.2.0rc12] - 2026-10-19

### Added

- Commit manifests (opt-in, `BronzeIngestionClient(manifests=True)`, Rust `PipelineConfig::manifests`): after each committed batch, one immutable JSON entry with its objects, record counts, contract, schema hash and event-time range, written only after every object it lists. Streams write one entry per `flush()`, per object closed by age and at the end. `LakeReader.changes(tenant, table, since=)` returns the entries after a watermark in commit order (`ChangeSet`), for incremental jobs that should not list every object. `TenantProfile.manifest_prefix(data_type)`.
- Optional OpenTelemetry instrumentation (`[otel]` extra, API only, no exporter): spans for ingests and reads and the metrics `datalake.records`, `datalake.bytes` and `datalake.operation.duration`. Without the package nothing is wrapped; installed but not configured, about 2 µs per call.

## [0.2.0rc11] - 2026-10-19

### Added

- OpenTelemetry logs: `datalake_medallion_sdk.otel.logs_contract()` (the Logs Data Model columns, typed timestamps, zstd) and `BronzeIngestionClient.ingest_otel_logs(body)`, which flattens an OTLP/JSON request, groups its records by the `tenant.id` resource attribute and writes each tenant once, in Rust and without pyarrow. `otel.flatten_logs()` returns the rows as pyarrow batches. Rust: `otel::flatten_logs_json`, `otel::flatten_logs_json_many`, `otel::logs_contract`.
- Queue and stream batches: `ingest_otel_log_batch(bodies)` flattens many requests in parallel and returns a `TenantOutcome` per tenant (the requests that carried it, its report or error), so a consumer retries only the messages of a failed tenant or an unreadable message. `batch_id_for(message_ids, time_ms=)` gives a deterministic batch id, so a redelivered batch rewrites the same objects. Rust: `pipeline::batch_id_for`.
- `examples/aws_lambda_handler.py`: a tested recipe for Kinesis, SQS and CloudWatch Logs events in AWS Lambda, with partial-batch failures.
- `examples/otel_throughput.rs`. 200 k log records (84 MiB OTLP/JSON), no network: 0.66 s as one request, 0.24 s as 100 requests.

### Fixed

- Contracts with `contentMediaType`, `contentEncoding` or `contentSchema` sent every Arrow batch down the row path. These keywords are annotations in drafts 2019-09 and 2020-12 (the default), so such contracts now keep the columnar path there (OpenTelemetry logs: 1.5 s to 0.13 s for 200 k records); drafts 6 and 7 are unchanged.

### Changed

- Small objects are uploaded concurrently. Streams bound uploads by bytes in flight (`max_inflight_parts` × `part_bytes`, the same memory as before) and at most 32 requests, so the many small objects of event-day partitions go out together instead of one by one; `ingest` writes up to 16 objects at once, dead letters first. On AWS, a stream of 3.1 M log records over 212 event days took 13.7 s in 0.2.0rc9 against 3.1 s without partitions.
- `LakeReader` looks up column types only when a filter value is text or a date.

## [0.2.0rc9] - 2026-10-19

### Added

- Contract storage options, all optional and off by default:
  - `x-dl-compression` (`snappy` default, `zstd`, `lz4`, `none`) and `x-dl-compression_level` (zstd, 1 to 22, default 1). On 1 M log rows zstd level 1 writes objects 41% smaller than Snappy for about 20% more encoding CPU; Snappy stays the default because it is the fastest.
  - `x-dl-type: "timestamp"` stores a `date-time` field as `timestamp(µs, UTC)`; values that do not parse are quarantined.
  - `x-dl-partition_by` partitions Bronze by the event day of a `date-time` field, in `ingest` (one object per event day, `IngestReport.data_uris`) and in streams (one open object per event day, at most `max_open_days`).
- Streams for continuous sources: `ingest_stream(max_object_seconds=...)` closes objects by age. Rust: `StreamOptions::max_object_age`, `StreamOptions::max_open_days`, and `BronzeStream::flush()`, which waits until the open objects are uploaded.
- `examples/compression_bench.rs`: size and speed of every codec on log-like data.

### Fixed

- `LakeReader` filters on timestamp and date columns accept ISO text, dates and datetimes in both engines, and values without a zone are UTC. Before, Polars rejected text values and DuckDB read zone-less values in the machine's local time zone. Filters on text columns are unchanged.
- The `lz4` codec was not compiled in. Every codec a contract can declare now comes from one list, and tests encode each one in Rust and with the built wheel.

## [0.2.0rc8] - 2026-10-19

### Changed

- `ingest_file` reads S3 objects with parallel ranged requests: 4 ranges of 8 MiB in flight, about 32 MiB of buffer. Compressed files are decompressed on the fly. Inside a Lambda a single S3 stream reached about 37 MiB/s; on AWS `ingest_file` went from 11.9 to 5.5 s for 468 MB of CSV and from 20.6 to 10.2 s for 793 MB of JSON Lines.
- `PiiDecryptor.decrypt_column` processes the token column chunk by chunk (no combined copy) and takes key references as a dictionary: an index per row plus the distinct references, instead of a long string per row. Memory added for 3.1 M values with real KMS references: +934 MiB with `decrypt_many`, +91 MiB with `decrypt_column`. Rust: `KeyRefs::Indexed`.

## [0.2.0rc7] - 2026-10-19

### Added

- `PiiDecryptor.decrypt_column(tokens, tenant_id=, field=, key_refs=)`: decrypts an Arrow column (pyarrow, Polars) into a pyarrow `StringArray`, in parallel and without Python strings. `key_refs` can be the `_dl_pii_key_ref` column, so rows encrypted with different data keys decrypt in one call. On 3.1 M values: 0.34 s and +252 MiB, against 2.34 s and +945 MiB for `decrypt_many`. Rust: `PiiDecryptor::decrypt_array` with `KeyRefs`.
- Streams read the next chunk on a separate thread (2 chunks ahead), so storage I/O, decompression and parsing overlap with validation and encoding. Rust: `stream::prefetch`.

### Changed

- JSON Lines parsed into typed columns in parallel parts, without copying the chunk when every line takes the typed path. Streaming 500 MB without network: JSON Lines 5.65 → 3.61 s, CSV 5.30 → 3.53 s, Parquet 4.60 → 3.68 s.
- `LakeReader`: both engines sort nulls last.

### Fixed

- `LakeReader(cache_mb=...)` discarded large tables as too big for the cache, because the constant lineage columns were expanded to one value per row. These columns are now kept as dictionaries, and results still return plain strings. With Polars, warm reads of 3.1 M rows on AWS went from 1.4 s to 20–60 ms.

## [0.2.0rc6] - 2026-10-18

### Added

- Streaming ingestion with bounded memory: `BronzeIngestionClient.ingest_stream(source, ...)` for any Arrow stream or iterable of chunks, and `ingest_file(path, ...)` for CSV, TSV, JSON Lines and Parquet files, local or on S3, optionally compressed. Chunks are validated exactly like `ingest`. Accepted rows go to Parquet objects of about `target_object_mb` (256 MB), uploaded with S3 multipart upload in parallel parts while the next chunk is validated. Objects are named after the stream id, so a retry with the same `stream_id` rewrites the same keys. Unfinished uploads are aborted on error. Returns a `StreamReport`.
- Rust: `BronzePipeline::stream` / `ingest_stream`, `BronzeStream`, `StreamOptions`, `StreamReport`; `ObjectStorage` gains multipart methods (S3 and in-memory adapters implement them; other stores fall back to one `put` per object).
- `RecordSource::JsonLines` (one record per line, a malformed line quarantined alone) and `RecordSource::ArrowText` (text columns parsed with the contract's types, as for CSV).
- JSON Lines that fit the contract are parsed straight into typed Arrow columns and validated column by column; lines with `null`, and blocks that do not fit, keep the row path, with identical results.

- `ingest(..., batch_id=)` (Python) and `IngestRequest::with_batch_id` (Rust): retrying a call with the same ULID rewrites the same keys, and partitions follow the id's date.
- Uploads are retried inside the call: `ingest` retries its commit (4 attempts, exponential backoff with jitter) over the same keys instead of re-validating with a new batch id; stream parts and objects are retried the same way before the stream is aborted.
- Parquet encoding writes the columns of each row group in parallel (the `parquet` crate's `ArrowColumnWriter` pattern), for single calls and streams.

### Fixed

- `LakeReader(cache_mb=...)` keyed cached objects by path only; an object rewritten in place (a stream retried with the same id) could be served stale. Entries are now keyed by path, size and modification time.

- Arrow input with timestamps in a named time zone (pyarrow's `timestamp[..., tz=UTC]`, inferred from CSV and Parquet) failed with "Invalid timezone"; dates and timestamps given for `string` fields are now stored as their date-time text, on the columnar path.

## [0.2.0rc5] - 2026-10-18

### Added

- `LakeReader(cache_mb=...)`: keeps the contents of Parquet objects in memory (least recently used out). Objects written by the ingestion client never change, so warm reads fetch only new objects. On AWS Lambda against S3, warm Bronze reads of one tenant went from 0.9-1.1 s to 5-27 ms with Polars.
- Documentation: quickstart-first README, IAM policy examples (`docs/iam.md`), performance on AWS (`docs/performance.md`), an errors table and upgrade notes from 0.1.

### Changed

- `LakeReader` with DuckDB builds filters with DuckDB expressions instead of bound parameters, so tenant and caller filters prune Hive and Iceberg partitions (an Iceberg aggregate went from 3.1 s to 71 ms on S3), and uses more threads than cores for S3 reads.
- Package description and keywords cover reads and per-tenant keys; SECURITY.md lists 0.2.x as supported.

## [0.2.0rc4] - 2026-10-18

Pre-release on TestPyPI (0.2.0rc1 to rc4 were published as the work progressed). Breaking changes to the client API; see *Upgrading from 0.1* in the README.

### Changed (breaking)

- Micro-tenancy: the client no longer has a bucket or prefixes. Each `ingest(batch, tenant=..., contract=...)` names its tenant, as a `TenantProfile` or as an id resolved through `tenants=` (a `TenantRegistry` or a function `(tenant_id) -> TenantProfile`). The profile fixes the tenant's Bronze and quarantine locations (`s3://bucket/prefix`, any bucket), optional Silver and Gold locations, the partition order and the PII protection. Profiles are validated when built: required layers, safe prefixes, valid buckets, and no two layers sharing or nesting a location.
- PII is stored as received by default (it was hashed by default in 0.1). Protection is set per tenant with `PiiProtection.sha256()`, `.hmac_sha256(key)`, `.kms_envelope(kms_key_id)`, `.envelope(provider)`, `.aes_256_gcm(data_key, key_ref=...)` or `.custom(obj)`. The `pii_masking` and `pii_hmac_key` client options and the per-call `pii_masking` override are removed.
- `_dl_pii_masking` is renamed `_dl_pii_protection`; `IngestReport.pii_masking` is renamed `pii_protection`; `IngestReport.tenant_id` is new. Dead letters record `pii_protection`.
- Dead letters go to the tenant's `quarantine` location (`quarantine/` by default, was `dlq/`).
- Rust: `BronzePipeline::new` no longer takes a path resolver, `IngestRequest` carries an `Arc<TenantProfile>`, and `ObjectStorage::put` receives the full object path including the bucket. `BronzePathResolver` and `PiiHasher` are replaced by `TenantProfile` and `FieldProtector`.

### Added

- AES-256-GCM encryption for PII. Values are authenticated with their tenant and field; the key reference is stored in the new `_dl_pii_key_ref` column. `PiiProtection.decrypt` reads values back with a known data key.
- `FieldProtector` (Rust trait and Python protocol) for custom protection schemes, called once per field and batch. A failing protector raises `ProtectionError` and nothing is written.
- `TenantRegistry`, `TenantProfile.location()` and `TenantProfile.table_uri()` for resolving tenant locations, including Silver and Gold.
- `datalake-medallion-sdk/examples/throughput.rs`: end-to-end throughput and peak heap on a large batch for each input type and protection scheme.
- `PreparedBatch::encoded_bytes()`: size of the objects a commit will upload.
- Envelope encryption with cached data keys: `PiiProtection.kms_envelope(kms_key_id)` (AWS KMS, Rust feature `aws-kms`) and `PiiProtection.envelope(provider)` for any `DataKeyProvider`. One data key per tenant, reused until it is 1 hour old or has drawn 2^15 nonces, refreshed ahead of expiry, never shared between tenants; failures with an expired key write nothing. KMS data keys carry the encryption context `{tenant_id, purpose}` and key references record the key ARN (`kms1:`). Protection stays optional.
- `PiiDecryptor` (`.kms()`, `.from_provider()`) to read protected values back, with `decrypt_many` for whole columns outside the GIL.
- `LakeReader` and `TableRef`: tenant-scoped reads of Bronze Parquet and of Silver/Gold Parquet or Iceberg tables straight from S3, without Athena or Glue. `read()` (projection, pushed-down filters, ordering, keyset pages) returns Arrow; `scan()` returns a tenant-filtered Polars `LazyFrame` or DuckDB relation. Objects are listed only below the tenant's prefix, `tenant_id` filters are bound by the reader, and listings and snapshots are cached for `max_staleness`. Engines: Polars (default, extra `[read]`) or DuckDB (extra `[duckdb]`).
- `TenantRegistry.from_config(dict | path)`: tenant profiles from a strict configuration document (unknown fields rejected, HMAC keys from environment variables).
- Rust: `BronzePipeline::commit` takes `&PreparedBatch`, so a failed upload can be retried without re-validating or re-encoding; the retry rewrites the same keys and duplicates neither Bronze rows nor dead letters.
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
