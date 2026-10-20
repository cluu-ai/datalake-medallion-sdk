# datalake-medallion-sdk

[![CI](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/datalake-medallion-sdk)](https://pypi.org/project/datalake-medallion-sdk/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org)

**Build and serve a multi-tenant medallion data lake on S3.** Ingest raw data into Bronze with every record checked against a contract, write Silver and Gold as Iceberg tables, and read any layer per tenant in milliseconds, without Athena, Glue jobs or Spark. Rust core, Python API.

## Why this SDK

- **The whole lake, one SDK.** Bronze ingestion, Silver and Gold writes, incremental Bronze-to-Silver jobs, table maintenance and tenant-scoped reads.
- **Micro-tenant by design.** Create a client once and use it for every tenant: each call names its tenant, and the tenant's profile decides where data is written to or read from (its own buckets or prefixes). Nothing is global.
- **Any input.** Arrow tables and streams, pyarrow datasets, Polars and DuckDB results, dicts, JSON, CSV, JSON Lines and Parquet files, OpenTelemetry logs.
- **Contract first.** Every record is validated against a JSON Schema contract before anything is written; invalid records go to quarantine with the reason, never to the lake.
- **Privacy, off by default.** Fields marked as PII are stored as received unless the tenant's profile protects them (hash, HMAC, AES-256-GCM with per-tenant KMS data keys, or your own scheme).
- **Open table formats.** Hive-partitioned Parquet in Bronze, Iceberg (or Parquet) in Silver and Gold, readable by any engine.
- **Fast and safe to retry.** Columnar paths in Rust, bounded memory for inputs of any size, idempotent writes that never duplicate rows, and correct results with concurrent writers.

## The lake at a glance

| Layer | Stored as | Write with | Guarantees |
|---|---|---|---|
| **Bronze**: data as received | Parquet, partitioned by tenant, table and day | `BronzeIngestionClient` | contract validation, quarantine, lineage columns, optional PII protection, optional commit manifests |
| **Silver**: cleaned and conformed | Iceberg tables (shared by tenants or per tenant) | `LakeWriter` | tenant isolation, append / upsert / replace in one commit, conflict-safe, idempotent by batch |
| **Gold**: aggregates for consumers | Iceberg tables | `LakeWriter` | the same |
| any layer | | `LakeReader` reads, `LakeMaintenance` compacts and expires | reads always filtered to the tenant; maintenance never in the write path |

## Install

```bash
pip install datalake-medallion-sdk                    # Bronze ingestion
pip install "datalake-medallion-sdk[read,write]"      # + LakeReader and LakeWriter
```

| Extra | Adds | For |
|---|---|---|
| *(none)* | nothing | ingestion of dicts, JSON and Arrow (via the [PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)) |
| `[read]` | Polars, pyarrow, pyiceberg | `LakeReader` with the default engine |
| `[duckdb]` | DuckDB, pyarrow | `LakeReader(engine="duckdb")` |
| `[write]` | pyarrow, pyiceberg 0.12 | `LakeWriter`, `LakeMaintenance` |
| `[otel]` | opentelemetry-api | spans and metrics of the SDK's own operations |
| `[pyarrow]`, `[polars]` | that library | building Arrow input yourself, `ingest_file` |

Wheels are `abi3` (one wheel per platform for CPython 3.12+) for Linux (glibc and musl, x86_64 and aarch64), macOS on Apple silicon and Windows x86_64. Other platforms build from the source distribution and need a Rust toolchain.

## Quickstart

**1. Describe the data.** A contract is a JSON Schema with a few `x-dl-` keywords. Save it as `contracts/security/login_event.schema.json` (more in [`examples/contracts`](examples/contracts)):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "x-dl-domain": "security",
  "x-dl-datatype": "login_event",
  "x-dl-schema_version": "1.0.0",
  "required": ["event_id", "occurred_at", "outcome"],
  "properties": {
    "event_id":    {"type": "string", "format": "uuid"},
    "occurred_at": {"type": "string", "format": "date-time"},
    "outcome":     {"type": "string", "enum": ["SUCCESS", "FAILURE", "LOCKED"]},
    "user_email":  {"type": "string", "format": "email", "x-dl-pii": true}
  }
}
```

**2. Set up the clients once.** Tenants are profiles (locations and protection); the client serves all of them. Credentials come from the AWS default chain.

```python
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    TenantProfile,
    TenantRegistry,
)

tenants = TenantRegistry(
    [
        TenantProfile.for_bucket("acme-001", "acme-lake"),  # its own bucket
        TenantProfile.for_bucket("globex-002", "globex-lake"),  # another one; PII stays as received
    ]
)
client = BronzeIngestionClient(ContractRegistry.from_directory("contracts"), tenants=tenants)
```

**3. Ingest into Bronze**, naming the tenant on each call. Any input works: dicts, JSON, Arrow, Polars, files, streams.

```python
report = await client.ingest(
    [
        {
            "event_id": "0b7d6a52-3c1e-4f5e-9a4b-1f2d3c4b5a69",
            "occurred_at": "2026-10-01T12:00:00Z",
            "outcome": "SUCCESS",
            "user_email": "ana@example.com",
        },
        {"event_id": 42},  # invalid: quarantined, not written
    ],
    tenant="acme-001",
    contract="security/login_event/1.0.0",
)
print(report.records_ok, report.records_quarantined)  # 1 1
await client.ingest_file(
    "s3://landing/globex/logins.csv.gz", tenant="globex-002", contract="security/login_event/1.0.0"
)
```

**4. Write Silver and read it back** (`pip install "datalake-medallion-sdk[read,write]"`):

```python
from pyiceberg.catalog import load_catalog
from datalake_medallion_sdk import LakeReader, LakeWriter, TableRef

catalog = load_catalog("lake", type="glue")  # or REST, SQL...
reader = LakeReader(tenants, catalog=catalog)
writer = LakeWriter(tenants, catalog=catalog)

logins = TableRef.iceberg("silver", "logins", identifier="silver.logins")  # shared by tenants
bronze = reader.read("acme-001", TableRef.bronze("login_event"), columns=["event_id", "outcome"])
writer.create_table("acme-001", logins, bronze.schema, keys=["event_id"])
writer.upsert("acme-001", logins, bronze, batch_id="2026-10-01")  # idempotent

print(reader.read("acme-001", logins).num_rows)  # only acme-001's rows, as Arrow
```

The rest of this page covers each piece in depth.

## Which client do I use?

| You have… | Use | Measured on AWS¹ |
|---|---|---|
| Micro-batches: an API call, a queue message, a Lambda batch | `client.ingest(batch, tenant=, contract=)` with dicts, JSON or Arrow | 200 k records in 1.4–2.2 s (SDK part) |
| A file, local or on S3: CSV, TSV, JSON Lines, Parquet, gz/zst | `client.ingest_file("s3://…")` | 3.16 M records: Parquet **4.0 s**, CSV 5.6 s, JSONL 10.2 s; **0.5–0.9 GB** of memory |
| An Arrow stream: pyarrow dataset, DuckDB result, Polars, Kafka batches… | `client.ingest_stream(reader)` | 0.7–0.8 M records/s with bounded memory |
| A DataFrame already in memory | `client.ingest(df)` | 0.5 M records/s (one object) |
| Application logs in OTLP/JSON, from an API or a queue batch | `client.ingest_otel_logs(body)` / `ingest_otel_log_batch(bodies)` | 1 M records (396 MiB) in **2.5 s**, one object per tenant |
| Cleaned or aggregated data for Silver and Gold | `LakeWriter.append / upsert / replace` | 3.16 M rows appended in **8.1 s**; 29 k-row upsert in **2.5 s** |
| Small files and old snapshots to clean up | `LakeMaintenance` on a schedule | 210 small files compacted into 7 in 67 s |
| Repeated per-tenant reads: APIs, dashboards | `LakeReader(cache_mb=1024)` (Polars) | **6–67 ms** per query (Gold and Bronze, 2–3 M rows) |
| One-off or low-memory reads, SQL | `LakeReader(engine="duckdb")` | 100–950 ms, 0.3–0.7 GB |
| Encrypted PII to read back | `PiiDecryptor.decrypt_column(...)` | 3.1 M values in **1.1 s** |

¹ AWS Lambda with 10 GB / 6 vCPU, reading from and writing to S3 in the same region. Methodology, local numbers and every configuration are in [docs/performance.md](docs/performance.md).

Rules of thumb:
- **Columnar input** (Parquet, Arrow) is 3–6x faster than JSON or dicts.
- **Streams** keep memory flat: 2 GB of JSON Lines peaks at about 0.4 GB.
- **Retries** with the same `batch_id` / `stream_id` never duplicate rows.
- **PII stays as received** unless the tenant's profile protects it.

## Data contracts

A contract is a JSON Schema (draft 2020-12) document plus a few keywords under an extension prefix (`x-dl-` by default, configurable per registry):

| Keyword | Required | Meaning |
|---|---|---|
| `x-dl-domain`, `x-dl-datatype`, `x-dl-schema_version` | yes | identify the contract; the key `domain/datatype/schema_version` (e.g. `security/login_event/1.0.0`) selects it at ingest time, and `datatype` names the Bronze table |
| `x-dl-flexibility_mode` | no | `strict` (default): undeclared fields rejected. `permissive`: undeclared fields kept as JSON in `_dl_extra`. `schema_on_read`: no schema enforcement; PII protection, type coercion and lineage still apply |
| `x-dl-pii` | no | on a property: the field holds personal data (see [PII protection](#pii-protection)) |
| `x-dl-layer` | no | free label of the layer the contract describes |
| `x-dl-compression` | no | Parquet codec of the contract's Bronze objects: `snappy` (default), `zstd`, `lz4` or `none`. Readers need no setting: the codec is recorded in each file |
| `x-dl-compression_level` | no | zstd level, 1 to 22 (default 1) |
| `x-dl-type: "timestamp"` | no | on a `date-time` string property: store it as `timestamp(µs, UTC)` instead of text, so engines filter and sort it as time. A value that does not parse quarantines its record |
| `x-dl-partition_by` | no | name of a `date-time` property: partition Bronze by the day of that event time instead of the ingest day (see [Object layout](#object-layout)) |

**Partitioning logs by event time.** Use `x-dl-partition_by` when a tenant writes a lot per day (hundreds of MB or more) or many events arrive days late. With little data per day it creates one small object per day, and reading many small objects is slower than reading a few large ones: on AWS, 3.1 M records spread over 212 days became 212 objects of about 0.6 MB, and an uncached DuckDB export went from 0.7 s (one object) to 8.6 s. For low-volume logs, keep the default ingest-day partitions, declare the event time with `x-dl-type: "timestamp"` and filter on it: Parquet statistics let the readers skip row groups outside the range. Merging small objects into large ones is the job of the Silver layer.

**Choosing a codec.** Snappy is the default because it is the fastest to write. For logs and other text-heavy data, `"x-dl-compression": "zstd"` makes objects about 40% smaller (less storage, upload and scan) for about 20% more encoding CPU; higher levels cost much more CPU for almost no gain. Measured on 1 M log rows: Snappy 74 MiB, zstd level 1 43 MiB, zstd level 6 43 MiB at 3.5× the encoding time of level 1.

```json
{
  "x-dl-domain": "app", "x-dl-datatype": "request_log", "x-dl-schema_version": "1.0.0",
  "x-dl-compression": "zstd",
  "x-dl-partition_by": "timestamp",
  "properties": {
    "timestamp": {"type": "string", "format": "date-time", "x-dl-type": "timestamp"},
    "level": {"type": "string"},
    "message": {"type": "string"}
  }
}
```

The Parquet schema comes from the contract, not from the data, so every object of a contract version has the same columns: `string` → utf8 (`timestamp(µs, UTC)` with `x-dl-type: "timestamp"`), `integer` → int64, `number` → float64, `boolean` → bool, anything else (objects, arrays, unions) → JSON text. When a tenant protects PII, PII fields are stored as utf8 (digest or token). Keep a tenant on one protection setting if a PII field is not a string, so its column type stays stable.

Contracts must be self-contained: `$ref` to remote URLs or files is not resolved, so loading contracts never performs network I/O. `ContractRegistry.from_schemas([...])` builds a registry from dicts instead of files.

## Tenants and destinations

The client holds contracts and a connection, never a destination. Each `ingest` names its tenant, and the tenant's `TenantProfile` decides, for that tenant only:

- an `s3://bucket/prefix` location per layer: `bronze` and `quarantine` are required (ingestion writes them); `silver` and `gold` are optional, so downstream jobs and `LakeReader` resolve them from the same profile;
- the order of the `tenant_id`/`data_type` partitions;
- the PII protection scheme, if any.

Profiles are validated when built: bucket names follow the AWS rules, prefixes are safe key segments, and two layers may not share or nest a location. Every key still carries a `tenant_id=` partition, even in a tenant's own bucket.

```python
import os

from datalake_medallion_sdk import PiiProtection, TenantProfile, TenantRegistry

acme = TenantProfile.for_bucket("acme-001", "acme-lake")  # bronze/, quarantine/, silver/, gold/
beta = TenantProfile(
    "beta-002",
    bronze="s3://beta-raw/landing",
    quarantine="s3://beta-quarantine/rejected",
    silver="s3://beta-curated/silver",
    partition_order="data_type_first",
    pii_protection=PiiProtection.hmac_sha256(os.environ["BETA_PII_KEY"].encode()),  # 16+ bytes
)
client = BronzeIngestionClient(contracts, tenants=TenantRegistry([acme, beta]))

# inside an async function:
await client.ingest(rows, tenant="beta-002", contract="security/login_event/1.0.0")  # looked up
await client.ingest(rows, tenant=acme, contract="security/login_event/1.0.0")  # passed directly
```

`tenants=` also accepts a function `(tenant_id) -> TenantProfile` to load profiles from your own configuration store; a profile returned for another tenant is rejected. `profile.location("silver")` and `profile.table_uri("silver", "login_event")` give the resolved locations.

Profiles can also come from a configuration document, a dict or the path of a JSON file, with `TenantRegistry.from_config(...)`. Unknown fields are rejected, so a typo cannot silently disable protection, and secrets never live in the document:

```json
{"tenants": [
  {"tenant_id": "acme-001", "bronze": "s3://acme-lake/bronze", "quarantine": "s3://acme-lake/quarantine"},
  {"tenant_id": "beta-002", "bronze": "s3://beta-raw/landing", "quarantine": "s3://beta-quarantine/rejected",
   "silver": "s3://beta-curated/silver", "partition_order": "data_type_first",
   "pii_protection": {"scheme": "hmac-sha256", "key_env": "BETA_PII_KEY"}},
  {"tenant_id": "gamma-003", "bronze": "s3://gamma-lake/bronze", "quarantine": "s3://gamma-lake/quarantine",
   "pii_protection": {"scheme": "aes-256-gcm", "provider": "aws-kms", "kms_key_id": "alias/lake-gamma-003",
                      "data_key_max_age_seconds": 3600, "max_nonce_prefixes_per_key": 32768,
                      "region": "us-east-1"}}
]}
```

`pii_protection.scheme` is one of `none`, `sha256`, `hmac-sha256` (`key_env` names the environment variable holding the key) or `aes-256-gcm` (`provider: "aws-kms"`; `endpoint_url` is optional).

## What happens to a record

| Level | Check | On failure |
|---|---|---|
| 0 | Contracts compile at boot; hooks must target known contracts; contract fields may not start with the lineage prefix (`_dl_`) | `ContractBootError` |
| 1 | Tenant id: 3–63 chars of `[a-z0-9_-]`, no leading/trailing or doubled separators, not a reserved word | `TenantViolationError` |
| 2 | Record is a JSON object, within the size limit (10 MiB default), without `_dl_` fields | quarantined |
| 3 | JSON Schema (formats asserted); in `strict` mode, no undeclared fields | quarantined |
| 4 | Optional [hook](#hooks) may accept, rewrite or reject; rewritten records are validated again | quarantined |
| 5 | `date-time` fields not more than 1 h (configurable) in the future | quarantined |
| 6 | PII fields protected with the tenant's scheme (none by default) | `ProtectionError` (nothing written) |
| 7 | Each value fits the Parquet column type derived from the contract | quarantined |
| 8 | Lineage columns appended | always applied |

`IngestReport` tells what happened: `records_received`, `records_ok`, `records_quarantined`, `rejections` (count per error code), `data_uri` (the Bronze object, `None` if nothing was valid), `dlq_uris` (the quarantine objects), `batch_id`, `tenant_id` and `pii_protection`.

With `on_invalid="raise"` the client writes nothing and raises the typed error (`SchemaViolationError`, `HookError`, …) for the first invalid record instead of quarantining. Error messages and quarantine entries never contain record values: schema errors are rendered with values masked, and hook exceptions are recorded by type name only.

**Inputs.** `ingest` accepts a `dict`, a list of dicts, JSON `str`/`bytes`, or any Arrow object implementing the PyCapsule interface, such as a pyarrow `Table`/`RecordBatch` or a Polars `DataFrame`. Arrow input gets the same validation and results as JSON and is about 4x faster: when every rule of the contract applies to one field at a time, it is checked column by column and only rows that may be invalid are converted to JSON. Validation and encoding run in Rust outside the GIL.

## Large files and streams

`ingest` validates one batch in memory and writes one object. Large files and continuous sources need different handling, because neither fits in memory. `ingest_stream` and `ingest_file` ingest input of any size in chunks, so memory stays bounded:

```python
# Files: CSV, TSV, JSON Lines or Parquet, local or on S3, optionally .gz/.bz2/.zst/.lz4.
report = await client.ingest_file(
    "s3://landing/acme/events-2026-10.jsonl.gz",
    tenant="acme-001",
    contract="security/login_event/1.0.0",
)
print(report.records_ok, report.records_quarantined, report.data_uris)

# Streams: any Arrow stream (pyarrow RecordBatchReader or dataset scanner, Polars, DuckDB, ...)
# or any iterable of chunks (lists of dicts, JSON text, Arrow batches...).
import pyarrow.dataset as ds

scanner = ds.dataset("s3://landing/acme/export/", format="parquet").scanner(batch_size=100_000)
report = await client.ingest_stream(
    scanner.to_reader(), tenant="acme-001", contract="security/login_event/1.0.0"
)
```

- **Bounded memory.**
  - Records are validated in chunks (`chunk_rows`, default 131,072 rows) exactly as `ingest` validates a batch.
  - Accepted rows are encoded into Parquet row groups (`row_group_mb`, default 64).
  - Each object is uploaded with S3 multipart upload in `part_mb` parts (default 16), up to `max_inflight_parts` at a time (default 4), while the next chunk is validated.
  - Memory therefore stays at about one chunk, one row group and the parts in flight, whatever the input size.
- **Right-sized objects.** A new Parquet object starts when the current one reaches `target_object_mb` (default 256 MB). Readers get a few large files instead of thousands of small ones, and no object reaches S3's 5 GB single-upload limit.
- **Retries without duplicates.**
  - Objects become visible only when complete, and are named `<stream_id>-00000.parquet`, `-00001`, …
  - Running the same input again with the same `stream_id` (from `report.stream_id`, or the id in the error log) rewrites the same keys.
  - An unfinished upload is aborted on error. Add an S3 lifecycle rule `AbortIncompleteMultipartUpload` as a safety net for processes that crash.
- **Formats.**
  - **CSV and TSV** are read as text and each value is parsed with the contract's type; empty cells are absent values, and a value that does not parse quarantines its record with the original text.
  - **JSON Lines** are parsed straight into typed columns when they fit the contract. Lines with `null` or that do not fit take the row path; either way a malformed line quarantines only itself.
  - **Parquet** is read one row group at a time.
- **Continuous sources (logs, events).**
  - `max_object_seconds` closes an object once it is that old, so records become visible within a bounded time even when traffic is low. It is checked as chunks arrive.
  - With `x-dl-partition_by`, rows go to one object per event day, and at most `max_open_days` (default 4) are open at once. Late events for a day already closed start a new object for that day.
  - From Rust, `BronzeStream::flush()` completes the open objects and waits until they are uploaded, so the input can be checkpointed.
- **Reports.** `StreamReport` gives `records_received`, `records_ok`, `records_quarantined`, `rejections`, `data_uris`, `dlq_uris`, `bytes_written` and `stream_id`.

`ingest_file` needs pyarrow (`pip install "datalake-medallion-sdk[pyarrow]"`).

From Rust:
- Use `BronzePipeline::ingest_stream(tenant, contract, hook, chunks, StreamOptions::default())` with any iterator of `RecordSource` chunks: `Arrow`, `JsonLines`, `ArrowText` for CSV read as text, `Json` or `Values`.
- Or open a `BronzeStream` and `push` chunks yourself.

## Application logs (OpenTelemetry)

Applications that emit logs with [OpenTelemetry](https://opentelemetry.io/) can send them to the lake as OTLP/JSON. The SDK flattens each request into one row per log record, with the columns of the [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/), groups the records by tenant and writes each tenant once. Parsing, validation and encoding run in Rust, without the GIL and without pyarrow.

```python
from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry, TenantRegistry
from datalake_medallion_sdk.otel import logs_contract

tenants = TenantRegistry.from_config({...})
client = BronzeIngestionClient(
    ContractRegistry.from_schemas([logs_contract()]),  # contract key "otel/logs/1.0.0"
    tenants=tenants,
)

# One OTLP/JSON export request (bytes, str or the decoded dict). The tenant is the
# resource attribute "tenant.id"; records without it go to default_tenant.
reports = await client.ingest_otel_logs(body, contract="otel/logs/1.0.0", default_tenant="shared")
for tenant_id, report in reports.items():
    print(tenant_id, report.records_ok, report.records_quarantined, report.data_uri)
```

**The contract.** `logs_contract()` returns the JSON Schema of the flattened records:

| Column | Type | From the OTLP record |
|---|---|---|
| `timestamp` | `timestamp(µs, UTC)`, required | `timeUnixNano`, or `observedTimeUnixNano` when unset |
| `observed_timestamp` | `timestamp(µs, UTC)` | `observedTimeUnixNano` |
| `severity_number`, `severity_text` | int64, text | severity |
| `body` | text | a string body as is, a structured body as JSON text |
| `attributes`, `resource_attributes` | JSON text | record and resource attributes |
| `trace_id`, `span_id` | hex text | trace context (checked: 32 and 16 hex digits) |
| `flags`, `event_name` | int64, text | |
| `service_name`, `scope_name`, `scope_version` | text | `service.name` resource attribute, instrumentation scope |

It uses zstd (logs are text-heavy) and ingest-day partitions. Change the defaults with `logs_contract(datatype=, schema_version=, compression=, partition_by_event_time=)`; turn event-day partitions on only for high daily volumes per tenant (see [Data contracts](#data-contracts)). Configure exporters with the OTLP/JSON encoding: protobuf requests are rejected with a clear error.

**Queues and streams.** A consumer of Kinesis, SQS, Kafka or Pub/Sub ingests a whole batch of messages with one call, with one write per tenant instead of one object per message:

```python
from datalake_medallion_sdk import batch_id_for

batch_id = batch_id_for([m.id for m in messages], time_ms=max(m.arrival_ms for m in messages))
outcomes = await client.ingest_otel_log_batch(
    [m.body for m in messages], contract="otel/logs/1.0.0", batch_id=batch_id
)
failed = {messages[i].id for o in outcomes if o.error for i in o.requests}
```

- `batch_id_for` derives the batch id from the message ids, so a batch delivered again (a timeout, a crash) rewrites the same objects instead of adding rows.
- Each `TenantOutcome` lists the requests that carried that tenant's records. A tenant that fails (unknown tenant, upload error) or a message that cannot be read fails alone; retry or report only those messages.
- Messages are flattened in parallel.

[`examples/aws_lambda_handler.py`](examples/aws_lambda_handler.py) is a tested recipe for AWS Lambda: Kinesis, SQS and CloudWatch Logs subscriptions (CloudWatch events are mapped to the same OpenTelemetry columns, so every log ends up in one table), with `batchItemFailures` for partial failures. It is an example to copy and adapt, not part of the SDK API.

**Reading logs back.** Filter the typed `timestamp` with text, dates or datetimes; values without a zone are UTC:

```python
from datalake_medallion_sdk import LakeReader, TableRef

reader = LakeReader(tenants, cache_mb=1024)
errors = reader.read(
    "acme-001",
    TableRef.bronze("logs"),
    columns=["timestamp", "service_name", "severity_text", "body", "trace_id"],
    where=[("timestamp", ">=", "2026-10-19T08:00:00Z"), ("severity_number", ">=", 17)],
    order_by=[("timestamp", "desc")],
    limit=100,
)
```

**Performance.** On AWS Lambda (10 GB / 6 vCPU), a batch of 500 requests with 1 M log records (396 MiB of OTLP/JSON) for 8 tenants is written to S3 in **2.5 s**, one object per tenant; the Lambda recipe handles a Kinesis batch of 100 requests in 0.5 s. Without the network (`examples/otel_throughput.rs`), 200 k records take 0.66 s as one request and 0.24 s as 100 requests (350 MiB/s). Details in [docs/performance.md](docs/performance.md).

## PII protection

Off by default: PII fields keep their declared type and value. Set `pii_protection` on a tenant profile to protect them in Bronze and in the quarantine entries:

| Scheme | `PiiProtection.` | Output | Reversible |
|---|---|---|---|
| `sha256` | `sha256()` | 64 hex chars | no; low-entropy values can be guessed |
| `hmac-sha256` | `hmac_sha256(key)` | 64 hex chars, equal values still join | no; keep the 16+ byte key outside the lake |
| `aes-256-gcm` | `kms_envelope(kms_key_id)` (recommended) or `aes_256_gcm(data_key, key_ref=...)` | `v1:` + base64url token | yes, with the data key |
| your own | `custom(obj)` | whatever `obj.protect_batch` returns | your choice |

**Per-tenant keys with AWS KMS** (envelope encryption, recommended for reversible protection):

```python
protection = PiiProtection.kms_envelope("alias/lake-acme-001")  # the tenant's KMS key
acme = TenantProfile.for_bucket("acme-001", "acme-lake", pii_protection=protection)
```

The SDK asks KMS for a data key the first time a tenant needs one and reuses it for that tenant until it is one hour old or has encrypted 2^15 batches (`data_key_max_age`, `max_nonce_prefixes_per_key`), refreshing it ahead of expiry. That is about one KMS call per tenant per hour instead of one per batch. Keys are never shared between tenants; if KMS fails and the current key has expired, the call fails with `ProtectionError` and nothing is written.

Data keys carry the encryption context `{"tenant_id": <tenant>, "purpose": "datalake-medallion-sdk/pii"}`, so IAM can restrict each role to its tenant, CloudTrail shows which tenant every key belongs to, and a key copied to another tenant cannot be decrypted. The plaintext key lives only in memory; `_dl_pii_key_ref` stores `kms1:<key arn>:<encrypted key>`. Other key services (GCP KMS, Azure Key Vault, Vault, …) plug in through the `DataKeyProvider` protocol (`generate`, `decrypt`) and `PiiProtection.envelope(provider)`.

**Reading protected values back** (one `Decrypt` per data key, not per value). Prefer `decrypt_column`: Arrow in and out, in parallel. Read `_dl_pii_key_ref` as a dictionary so the long key reference is not repeated per row:

```python
import pyarrow.parquet as pq
from datalake_medallion_sdk import PiiDecryptor

table = pq.read_table(uri, read_dictionary=["_dl_pii_key_ref"])  # or reader.read(...)
emails = PiiDecryptor.kms().decrypt_column(  # a pyarrow StringArray
    table.column("user_email"),
    tenant_id="acme-001",
    field="user_email",
    key_refs=table.column("_dl_pii_key_ref"),  # rows may use different data keys
)
```

`decrypt_many(list, key_ref=...)` remains for small lists of Python strings.

Grant the ingestion role `kms:GenerateDataKey` and readers `kms:Decrypt` on the tenant's key, both conditioned on `kms:EncryptionContext:tenant_id` ([policy examples](docs/iam.md)). Enable automatic rotation of the KMS key; old data keys keep working. Encrypt only what must be reversible: tokens do not compress, so encrypted columns cost more to store and scan; HMAC suits fields that only need to be joined.

**Custom schemes** are any object with a `scheme` name, an optional `key_ref` and `protect_batch(tenant_id, field, values) -> list[str]` (the `FieldProtector` protocol). It is called once per field and batch, so it can reach a key service in bulk; if it raises, the call fails with `ProtectionError` and nothing is written.

## Reading the lake

`LakeReader` reads one tenant's Parquet and Iceberg tables directly from S3 with an in-process engine and returns Arrow. It needs no Athena query, no Glue table and no query queue.

```python
import polars as pl
from datalake_medallion_sdk import LakeReader, TableRef

reader = LakeReader(tenants=tenants, cache_mb=512)  # engine="polars" by default
events = TableRef.bronze("login_event")

# Keyset pagination: _dl_ingest_id is unique and increases with ingest time.
cursor = ""
while True:
    page = reader.read(
        "acme-001",
        events,
        columns=["_dl_ingest_id", "event_id", "occurred_at", "outcome"],
        where=[("_dl_ingest_id", ">", cursor)],  # pushed down to the scan
        order_by=["_dl_ingest_id"],
        limit=1000,
    )
    if page.num_rows == 0:
        break
    cursor = page.column("_dl_ingest_id")[-1].as_py()

# Filters: ==, !=, <, <=, >, >=, in, not in (combined with AND)
recent = reader.read("acme-001", events, where=[("occurred_at", ">=", "2026-10-01T00:00:00Z")])
some = reader.read(
    "acme-001", events, where=[("event_id", "in", ["0b7d6a52-3c1e-4f5e-9a4b-1f2d3c4b5a69"])]
)

# Aggregations run in the engine on a scan already filtered to the tenant.
summary = TableRef.iceberg("gold", "login_summary", location="s3://shared-gold/login_summary")
by_outcome = (
    reader.scan("acme-001", summary).group_by("outcome").agg(pl.col("total").sum()).collect()
)
```

- **Tenant isolation.** Callers name a tenant and a table, never a bucket, a path or a tenant filter (filtering on `tenant_id` is rejected). Parquet objects are listed only below the tenant's prefix, a `tenant_id` filter is always applied when the data has that column, and a shared Iceberg table without a `tenant_id` column is refused.
- **Tables.**
  - `TableRef.bronze(data_type)`: the ingestion client's output.
  - `TableRef.parquet(layer, name)`: Hive-partitioned Parquet in Silver or Gold, below `profile.table_uri(layer, name)`.
  - `TableRef.iceberg(layer, name)`: an Iceberg table in the tenant's location, or a table shared by tenants named with `location=`, `metadata_location=` or `identifier=` (with `catalog=`, any [pyiceberg catalog](https://py.iceberg.apache.org/configuration/): REST, Glue, SQL, …). Without a catalog the current snapshot is the newest `metadata/*.metadata.json`; use a catalog when several writers commit concurrently.
- **Caching.** Listings, Iceberg snapshots and scan plans are reused for `max_staleness` seconds (default 60); `reader.refresh(tenant)` makes new data visible at once. With `cache_mb=512` (off by default), the contents of the files stay in memory as well: Bronze objects and the data files of an Iceberg table's current snapshot (only the tenant's partition of a shared table). Neither is ever rewritten in place, so warm reads fetch only new files. Use it in services that read the same tables repeatedly; tables with delete files or an evolved schema are scanned by the engine instead.
- **Pruning.** Filters on `year`, `month` and `day` (the Bronze partition columns) skip whole objects.
- **Engines.** `engine="polars"` (default, extra `[read]`) is the fastest with `cache_mb`. `engine="duckdb"` (extra `[duckdb]`) is the better choice without a cache and uses the least memory and `scan()` returns a DuckDB relation that accepts SQL (`relation.query("t", "SELECT ...")`). DuckDB downloads its `httpfs` and `iceberg` extensions on first use; on AWS Lambda pass `duckdb_config={"home_directory": "/tmp"}`, or point `extension_directory` at pre-installed extensions for offline environments.
- **Credentials.** The same options as the client (`aws_region`, `aws_endpoint_url`, keys) or the AWS default chain. The reader role needs `s3:ListBucket` and `s3:GetObject` on the tenant's locations only ([policy examples](docs/iam.md)).

### Incremental reads: changes since a watermark

A job that processes new Bronze data (a Silver build, a search indexer) should not list every object of a table on each run. With `manifests=True`, the client writes one small JSON entry per committed batch, and `LakeReader.changes()` returns the entries after a watermark, in commit order:

```python
client = BronzeIngestionClient(registry, tenants=tenants, manifests=True)
...
changes = reader.changes("acme-001", TableRef.bronze("login_event"), since=state.get("watermark"))
for entry in changes.entries:  # batch_id, data and dead_letters objects, record counts,
    process(entry["data"])  # contract, schema hash and event-time range
state["watermark"] = changes.watermark  # store it with the job's output
```

- An entry is written only after every object it lists, so readers never see a half-written batch. Streams write one entry per `flush()`, per object closed by `max_object_seconds` and at the end.
- Entries live under `<bronze>/_manifests/tenant_id=…/data_type=…/date=…/`, outside the data tree (engines that scan Bronze skip folders starting with `_`). Add an S3 lifecycle rule on that prefix to expire old entries.
- Entries younger than `settle_seconds` (default 120) wait for the next call, so a writer whose clock is behind cannot slip an entry in before the watermark.
- A retried upload rewrites the same entry; ingesting the same `batch_id` again adds another entry for the same objects. Treat `batch_id` (or the object URI) as the unit your job replaces, so reprocessing is idempotent.
- Off by default; it adds one small `PUT` per batch.

Serving Gold tables to an API: [docs/serving-gold.md](docs/serving-gold.md).

On AWS Lambda against S3, warm queries over 3.1 M rows take 22–67 ms with Polars and `cache_mb`, and 100–950 ms with DuckDB without a cache; Iceberg tables take 67–147 ms without a cache. Athena takes 0.9–2.1 s for the same queries. Details in [docs/performance.md](docs/performance.md).

## Writing Silver and Gold (Iceberg)

`LakeWriter` writes a tenant's rows to Iceberg tables through a catalog (AWS Glue, REST, SQL…), and `LakeMaintenance` keeps those tables compact. Install with `pip install "datalake-medallion-sdk[write]"` (PyIceberg 0.12).

```python
from pyiceberg.catalog import load_catalog
from datalake_medallion_sdk import LakeWriter, LakeMaintenance, TableRef

catalog = load_catalog("lake", type="glue")
writer = LakeWriter(tenants, catalog=catalog)

logins = TableRef.iceberg("silver", "logins", identifier="silver.logins")  # shared by tenants
writer.create_table("acme-001", logins, arrow_schema, partition_by=["day"], keys=["event_id"])

writer.append("acme-001", logins, table, batch_id=batch_id)  # Arrow, Polars, dicts
writer.upsert("acme-001", logins, changed_rows, batch_id=batch_id)  # by keys, one commit
writer.replace("acme-001", logins, day_rows, where=[("day", "==", "2026-10-19")])
```

- **Tenant isolation.** A shared table (named with `identifier=`) has a required `tenant_id` column and is partitioned by it first. The writer sets or checks `tenant_id` on every row, rejects rows of another tenant, and scopes every `upsert` and `replace` to the tenant, even when another tenant has the same keys. A table that belongs to one tenant (`TableRef.iceberg("silver", "events")`) lives below the tenant's Silver or Gold location and is registered as `silver.events__<tenant>`. `LakeReader` reads both kinds.
- **Concurrent writers.** Commits are optimistic. A commit that raced with an unrelated one (another tenant's partition) is retried by PyIceberg; when another writer changed the same rows, the writer reloads the table and applies the operation again on the new state (`conflict_retries`, default 3). Nothing is lost: verified with two writers on the same snapshot, in every combination.
- **Idempotency.** `batch_id=` is stored in the snapshot summary with the tenant, and a batch already committed is skipped (`WriteResult.skipped`). Snapshot expiry bounds how far back batches are remembered.
- **Results.** `WriteResult` gives the rows inserted, updated and deleted, the files added, the snapshot and the attempts.
- **Upserts** replace the rows whose keys appear in the data in one commit; keys must be unique within the data, and rows already stored twice under one key are collapsed. They rewrite the files that hold matched rows (copy-on-write), so their cost follows how many partitions the keys reach: include the partition columns in `keys`, or pass `partition_scoped=True` when a row never changes partition (an event's day), to match only in the partitions of the data. Above 1,000 keys the rows are matched in Arrow, reading key columns in parallel. Concurrent writers never store a key twice: a commit raced by another writer is applied again.
- **Schema.** `writer.add_columns(tenant, table, pa_schema)` adds optional columns; existing rows read them as null. Changing a column's type is refused.

### Incremental Bronze to Silver

With [commit manifests](#incremental-reads-changes-since-a-watermark), a job processes each new Bronze batch exactly once, even if it crashes before saving its watermark:

```python
from datalake_medallion_sdk import ChangeSet

changes = reader.changes("acme-001", TableRef.bronze("login_event"), since=watermark)
for entry in changes.entries:
    rows = reader.read_changes(
        "acme-001", TableRef.bronze("login_event"), ChangeSet([entry], changes.watermark)
    )
    writer.upsert("acme-001", logins, transform(rows), batch_id=entry["batch_id"])
save(changes.watermark)
```

`read_changes` refuses objects outside the tenant's table. A batch already in Silver is skipped, so running the job again after a crash writes nothing twice.

### Maintenance, outside the write path

Run `LakeMaintenance` on a schedule (an EventBridge rule, cron, Airflow), never inside ingestion or writes:

```python
maintenance = LakeMaintenance(writer)
maintenance.compact("acme-001", logins, target_file_mb=128)  # small files -> large ones
maintenance.expire_snapshots("acme-001", logins, older_than_hours=168, retain_last=10)
maintenance.remove_orphan_files("acme-001", logins, older_than_hours=72)
```

- `compact` rewrites the tenant's small files in groups of about twice `target_file_mb`, most fragmented partitions first, reading files in parallel. It stops after `max_seconds` (default 600) and reports what is left in `remaining`, so a scheduled run fits its time limit and the next one continues. A group a writer changed meanwhile is left for the next run: writers always win and no row is lost or resurrected.
- `expire_snapshots` removes snapshots older than the limit except the last `retain_last`; their batch ids are then forgotten by the idempotency check.
- `remove_orphan_files` deletes files under the table's location that no snapshot or metadata references (leftovers of failed commits), only past the safety window; `dry_run=True` lists them first.

On AWS Lambda (10 GB) with an AWS Glue catalog and S3 in the same region:

| Operation | Time |
|---|---|
| Append 3.16 M rows | 8.1 s |
| Upsert 29 k changed rows of 2 days (`partition_scoped=True`) | 2.5 s |
| Upsert 100 k keys spread over every partition | 14.1 s |
| Replace one day | 2.7 s |
| 16 appends by 4 tenants at once to one shared table | all committed, no error |
| Compact 210 small files into 7 | 67 s |
| A batch already committed, skipped | 0.36 s |

Details and the comparison with DuckDB writes are in [docs/performance.md](docs/performance.md).

## Object layout

```
<bronze location>/tenant_id=<tenant>/data_type=<datatype>/year=YYYY/month=MM/day=DD/<batch_id>.parquet
<quarantine location>/tenant_id=<tenant>/data_type=<datatype>/error_code=<code>/year=YYYY/month=MM/day=DD/<batch_id>.jsonl
```

The date is the ingest day, or the event day when the contract sets `x-dl-partition_by`. Then each object is named `<batch_id>-YYYYMMDD.parquet` (`<stream_id>-YYYYMMDD-00000.parquet` for streams), one per event day in the batch, and rows without a usable event time go to the ingest day. `IngestReport.data_uris` lists every object.

With `partition_order="data_type_first"` the `data_type=` partition comes before `tenant_id=`. Each `ingest` call writes at most one Parquet object plus one quarantine object per error code (streams write one object per `target_object_mb`). Quarantine objects are written first, so retrying a failed call never duplicates Bronze rows.

Lineage columns (prefix `_dl_`, configurable with `envelope_prefix`):

| Column | Type | Content |
|---|---|---|
| `_dl_ingest_id` | utf8 | ULID per row, monotonic within the batch |
| `_dl_batch_id` | utf8 | ULID of the call; names the objects |
| `_dl_tenant_id` | utf8 | tenant |
| `_dl_data_type`, `_dl_schema_version`, `_dl_schema_hash` | utf8 | contract identity and SHA-256 |
| `_dl_received_at` | timestamp(µs, UTC) | ingest time |
| `_dl_pii_protection` | utf8 | `none`, `sha256`, `hmac-sha256`, `aes-256-gcm` or a custom scheme |
| `_dl_pii_key_ref` | utf8, nullable | key reference of reversible schemes (e.g. the KMS-encrypted data key) |
| `_dl_sdk_version` | utf8 | SDK version |

## Hooks

A hook runs your own rule on each record of one contract, after schema validation:

```python
from datalake_medallion_sdk import HookRegistry, PreIngestHookResult

hooks = HookRegistry()


@hooks.register("security/login_event/1.0.0")
def drop_test_accounts(record: dict) -> PreIngestHookResult:
    if record.get("user_email", "").endswith("@test.invalid"):
        return PreIngestHookResult.reject("ERR_TEST_ACCOUNT", "test account")
    return PreIngestHookResult.accept(record)


client = BronzeIngestionClient(contracts, tenants=tenants, hooks=hooks.as_mapping())
```

## Operations

- **Credentials**: the AWS default chain (environment, profile, SSO, web identity, ECS/EC2 roles) unless `aws_access_key_id`/`aws_secret_access_key` are passed. The SDK never creates buckets; the writer role needs `s3:PutObject` on each tenant's bronze and quarantine locations only. See [IAM policy examples](docs/iam.md).
- **S3-compatible stores** (MinIO, LocalStack, …): `aws_endpoint_url=` (path-style addressing is enabled automatically).
- **Large inputs**: use `ingest_file` / `ingest_stream` (above) rather than one `ingest` call; S3 lifecycle rule `AbortIncompleteMultipartUpload` recommended.
- **Client options**: `max_record_bytes` (default 10 MiB), `max_future_skew_seconds` (default 3600, `None` disables), `on_invalid` (`"quarantine"` or `"raise"`), `envelope_prefix` (default `"_dl_"`), `manifests` (default off, see [Incremental reads](#incremental-reads-changes-since-a-watermark)).
- **Retries and idempotency**: uploads are retried inside the call (4 attempts, exponential backoff with jitter) over the same object keys, so a transient S3 error never duplicates rows. To retry a whole call, for example after a crash, pass the earlier `batch_id=` (or `stream_id=` for streams): the same keys are rewritten instead of adding rows. `@datalake_retry()` retries `StorageError` only.
- **Metrics**: `@emit_metrics(sink)` records duration, failures and record counts to logging, CloudWatch Embedded Metric Format or any `MetricsSink`.
- **OpenTelemetry** (optional, `pip install "datalake-medallion-sdk[otel]"`): ingests and reads become spans (`datalake.ingest`, `datalake.ingest_stream`, `datalake.read`, …) with `tenant.id`, `datalake.contract`, `datalake.data_type` and record counts, and the metrics `datalake.records` (by outcome), `datalake.bytes` and `datalake.operation.duration`. The SDK uses the OpenTelemetry API only and ships no exporter: configure the OpenTelemetry SDK and exporter in your application. Without the package nothing is wrapped; installed but not configured, a call costs about 2 µs.
- **Concurrency**: one client and one reader per process serve every tenant and bucket; connections are created once and reused. Buckets must be reachable in the client's region.

## Errors

All errors derive from `DatalakeError`.

| Exception | Raised when | What to do |
|---|---|---|
| `ContractBootError` | a contract does not compile, or a hook targets an unknown contract | fix the contract; the registry loads all-or-nothing |
| `ConfigurationError` | invalid options or profile, unknown contract, a tenant id without `tenants=`, a reader engine that is not installed | fix the configuration |
| `TenantViolationError` | invalid or unknown tenant id, a resolver returned another tenant, a reader filter on `tenant_id` | check the tenant id and the registry |
| `ProtectionError` | the tenant's PII protection or key service failed; nothing was written | check KMS permissions and availability, then retry |
| `StorageError` | S3 failed | retry (`@datalake_retry`); quarantine objects are not duplicated |
| `MalformedPayloadError` | the batch is not JSON or Arrow | fix the input |
| `SchemaViolationError`, `HookError`, `SemanticViolationError`, `TypeCoercionError` | a record is invalid and `on_invalid="raise"` | fix the record, or quarantine instead |

## Upgrading from 0.1

- The client has no bucket or prefixes any more: pass `tenants=` to the client and `tenant=` to each `ingest`.
- **PII is no longer hashed by default.** To keep 0.1 behaviour, set `pii_protection=PiiProtection.sha256()` (was `pii_masking=True`) or `PiiProtection.hmac_sha256(key)` (was `pii_hmac_key`) on each tenant profile.
- The `_dl_pii_masking` column is now `_dl_pii_protection`; `IngestReport.pii_masking` is now `pii_protection`.
- Rejected records go to the tenant's `quarantine` location (`quarantine/` by default, was `dlq/`).

See the [changelog](CHANGELOG.md) for the full list.

## Performance

- **One call:** 200 MiB validated and encoded in **0.38 s from Arrow** (550 MiB/s) and 2.3 s from JSON, without network.
- **Streams:** files of any size at 0.3–0.8 M records/s with under 1 GB of memory.
- **Reads:** 22–67 ms per tenant query with the reader cache.

Full results, the environment and tuning are in [docs/performance.md](docs/performance.md).

## Rust crate

The core is usable from Rust (`BronzePipeline`, `ContractRegistry`, `TenantProfile`, `FieldProtector`, `DataKeyProvider`, the `ObjectStorage` port with S3 and in-memory adapters). It is not published on crates.io yet; depend on it from Git:

```toml
[dependencies]
datalake-medallion-sdk = { git = "https://github.com/cluu-ai/datalake-medallion-sdk", tag = "v0.3.0", default-features = false, features = ["aws-kms"] }
```

The library is imported as `_core` (`use _core::pipeline::BronzePipeline;`). `default-features = false` builds without Python, for example for an AWS Lambda; the `aws-kms` feature adds the KMS data key provider. The Rust API may change before 1.0.

## Development

```bash
pip install -e ".[dev]"             # builds the extension with maturin
pytest pytests                      # Python tests, end-to-end against an in-process S3 emulator (moto)
cargo test --all-targets            # Rust tests
cargo clippy --all-targets -- -D warnings && ruff check python pytests && mypy
```

Minimum supported Rust version: 1.94.1. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Scope

The SDK covers the data plane of a medallion lake on S3 and S3-compatible stores: contract-checked ingestion into Bronze (single calls, streams and files of any size, OpenTelemetry logs), Silver and Gold writes to Iceberg through any PyIceberg catalog, their maintenance, and tenant-scoped reads of Parquet and Iceberg tables. Provisioning (buckets, catalogs, IAM, KMS keys) and orchestration (schedulers, queues) stay in your infrastructure; [`examples/`](examples) shows common setups.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option. Unless you state otherwise, any contribution you submit for inclusion is dual licensed as above, without additional terms.
