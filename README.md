# datalake-medallion-sdk

[![CI](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/datalake-medallion-sdk)](https://pypi.org/project/datalake-medallion-sdk/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org)

Ingest data into a multi-tenant data lake on S3 with every record checked against a data contract, and read it back per tenant without Athena or Glue. Rust core, Python API.

A *medallion* lake keeps data in layers: **Bronze** holds raw data as received (validated here), **Silver** cleaned and conformed data, **Gold** aggregates for consumers. This SDK writes Bronze and reads every layer:

- **Contract first**: each record is validated against a JSON Schema contract before anything is written. Valid records become one Hive-partitioned Parquet object with lineage columns; invalid records go to a quarantine location with the reason attached.
- **Multi-tenant by design**: every call names its tenant, and the tenant's profile decides where its data lives (its own buckets or prefixes) and how its personal data is protected.
- **PII protection, off by default**: fields marked as PII can be hashed, HMAC-ed, encrypted with a per-tenant KMS data key, or protected with your own scheme.
- **Fast reads without a query service**: `LakeReader` scans a tenant's Parquet and Iceberg tables straight from S3 with Polars or DuckDB and returns Arrow.

## Install

```bash
pip install datalake-medallion-sdk            # ingestion
pip install "datalake-medallion-sdk[read]"    # + LakeReader with Polars (recommended)
```

| Extra | Adds | For |
|---|---|---|
| *(none)* | nothing | ingestion of dicts, JSON and Arrow (via the [PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)) |
| `[read]` | Polars, pyarrow, pyiceberg | `LakeReader` with the default engine |
| `[duckdb]` | DuckDB, pyarrow | `LakeReader(engine="duckdb")` |
| `[pyarrow]`, `[polars]` | that library | building Arrow input yourself |

Wheels are `abi3` (one wheel per platform for CPython 3.12+) for Linux (glibc and musl, x86_64 and aarch64), macOS on Apple silicon and Windows x86_64. Other platforms build from the source distribution and need a Rust toolchain.

## Quickstart

**1. Write a contract.** Save this as `contracts/security/login_event.schema.json` (a fuller version is in [`examples/contracts`](examples/contracts)). `ContractRegistry.from_directory` loads every `*.schema.json` below a folder.

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
    "attempts":    {"type": "integer", "minimum": 1},
    "user_email":  {"type": "string", "format": "email", "x-dl-pii": true}
  }
}
```

**2. Ingest.** Credentials come from the AWS default chain (environment, profile, SSO, role). The bucket must exist.

```python
import asyncio

from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    TenantProfile,
    TenantRegistry,
)

contracts = ContractRegistry.from_directory("contracts")  # compiles every contract, all-or-nothing
tenants = TenantRegistry([TenantProfile.for_bucket("acme-001", "acme-lake")])
client = BronzeIngestionClient(contracts, tenants=tenants)


async def main() -> None:
    report = await client.ingest(
        [
            {
                "event_id": "0b7d6a52-3c1e-4f5e-9a4b-1f2d3c4b5a69",
                "occurred_at": "2026-10-01T12:00:00Z",
                "outcome": "SUCCESS",
                "user_email": "ana@example.com",
            },
            {"event_id": 42},  # invalid: quarantined, not written to Bronze
        ],
        tenant="acme-001",
        contract="security/login_event/1.0.0",
    )
    print(report.records_ok, report.records_quarantined, report.rejections)
    # 1 1 {'ERR_SCHEMA_VIOLATION': 1}
    print(report.data_uri)
    # s3://acme-lake/bronze/tenant_id=acme-001/data_type=login_event/year=…/<batch_id>.parquet


asyncio.run(main())
```

**3. Read it back** (`pip install "datalake-medallion-sdk[read]"`):

```python
from datalake_medallion_sdk import LakeReader, TableRef

reader = LakeReader(tenants=tenants)
events = reader.read("acme-001", TableRef.bronze("login_event"), columns=["event_id", "outcome"])
print(events.num_rows)  # a pyarrow.Table
```

The rest of this page covers each piece in depth.

## Data contracts

A contract is a JSON Schema (draft 2020-12) document plus a few keywords under an extension prefix (`x-dl-` by default, configurable per registry):

| Keyword | Required | Meaning |
|---|---|---|
| `x-dl-domain`, `x-dl-datatype`, `x-dl-schema_version` | yes | identify the contract; the key `domain/datatype/schema_version` (e.g. `security/login_event/1.0.0`) selects it at ingest time, and `datatype` names the Bronze table |
| `x-dl-flexibility_mode` | no | `strict` (default): undeclared fields rejected. `permissive`: undeclared fields kept as JSON in `_dl_extra`. `schema_on_read`: no schema enforcement; PII protection, type coercion and lineage still apply |
| `x-dl-pii` | no | on a property: the field holds personal data (see [PII protection](#pii-protection)) |
| `x-dl-layer` | no | free label of the layer the contract describes |

The Parquet schema comes from the contract, not from the data, so every object of a contract version has the same columns: `string` → utf8, `integer` → int64, `number` → float64, `boolean` → bool, anything else (objects, arrays, unions) → JSON text. When a tenant protects PII, PII fields are stored as utf8 (digest or token). Keep a tenant on one protection setting if a PII field is not a string, so its column type stays stable.

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
- **Reports.** `StreamReport` gives `records_received`, `records_ok`, `records_quarantined`, `rejections`, `data_uris`, `dlq_uris`, `bytes_written` and `stream_id`.

`ingest_file` needs pyarrow (`pip install "datalake-medallion-sdk[pyarrow]"`).

From Rust:
- Use `BronzePipeline::ingest_stream(tenant, contract, hook, chunks, StreamOptions::default())` with any iterator of `RecordSource` chunks: `Arrow`, `JsonLines`, `ArrowText` for CSV read as text, `Json` or `Values`.
- Or open a `BronzeStream` and `push` chunks yourself.

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

**Reading protected values back** (one `Decrypt` per data key, not per value):

```python
from datalake_medallion_sdk import PiiDecryptor

table = reader.read("acme-001", TableRef.bronze("login_event"))  # see Reading the lake
emails = PiiDecryptor.kms().decrypt_many(
    table.column("user_email").to_pylist(),
    tenant_id="acme-001",
    field="user_email",
    key_ref=table.column("_dl_pii_key_ref")[0].as_py(),  # rows of one batch share a key
)
```

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
- **Caching.** Listings, Iceberg snapshots and scan plans are reused for `max_staleness` seconds (default 60); `reader.refresh(tenant)` makes new data visible at once. With `cache_mb=512` (off by default), the contents of Parquet objects stay in memory as well: the SDK never rewrites an object, so warm reads fetch only new ones. Use it in services that read Bronze repeatedly.
- **Pruning.** Filters on `year`, `month` and `day` (the Bronze partition columns) skip whole objects.
- **Engines.** `engine="polars"` (default, extra `[read]`) is the fastest for repeated reads. `engine="duckdb"` (extra `[duckdb]`) uses about half the memory, starts fastest (good for short-lived functions) and `scan()` returns a DuckDB relation that accepts SQL (`relation.query("t", "SELECT ...")`). DuckDB downloads its `httpfs` and `iceberg` extensions on first use; on AWS Lambda pass `duckdb_config={"home_directory": "/tmp"}`, or point `extension_directory` at pre-installed extensions for offline environments.
- **Credentials.** The same options as the client (`aws_region`, `aws_endpoint_url`, keys) or the AWS default chain. The reader role needs `s3:ListBucket` and `s3:GetObject` on the tenant's locations only ([policy examples](docs/iam.md)).

On AWS Lambda against S3, warm reads of one tenant out of 8 (3.16 M rows) take 5–27 ms from Bronze with `cache_mb` and 67–147 ms from Iceberg, against 0.9–2.1 s for the same queries in Athena. Details in [docs/performance.md](docs/performance.md).

## Object layout

```
<bronze location>/tenant_id=<tenant>/data_type=<datatype>/year=YYYY/month=MM/day=DD/<batch_id>.parquet
<quarantine location>/tenant_id=<tenant>/data_type=<datatype>/error_code=<code>/year=YYYY/month=MM/day=DD/<batch_id>.jsonl
```

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
- **Client options**: `max_record_bytes` (default 10 MiB), `max_future_skew_seconds` (default 3600, `None` disables), `on_invalid` (`"quarantine"` or `"raise"`), `envelope_prefix` (default `"_dl_"`).
- **Retries and idempotency**: uploads are retried inside the call (4 attempts, exponential backoff with jitter) over the same object keys, so a transient S3 error never duplicates rows. To retry a whole call, for example after a crash, pass the earlier `batch_id=` (or `stream_id=` for streams): the same keys are rewritten instead of adding rows. `@datalake_retry()` retries `StorageError` only.
- **Metrics**: `@emit_metrics(sink)` records duration, failures and record counts to logging, CloudWatch Embedded Metric Format or any `MetricsSink`.
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

A 200 MiB batch (869 k records) is validated and encoded in 0.42 s from Arrow (500 MiB/s, 200 MiB peak heap) and 2.71 s from JSON text, without PII protection; protection adds 30–85%. Streaming a 2 GB JSON Lines file peaks at 436 MB. Benchmarks, methodology and reader numbers are in [docs/performance.md](docs/performance.md).

## Rust crate

The core is usable from Rust (`BronzePipeline`, `ContractRegistry`, `TenantProfile`, `FieldProtector`, `DataKeyProvider`, the `ObjectStorage` port with S3 and in-memory adapters). It is not published on crates.io yet; depend on it from Git:

```toml
[dependencies]
datalake-medallion-sdk = { git = "https://github.com/cluu-ai/datalake-medallion-sdk", tag = "v0.2.0rc6", default-features = false, features = ["aws-kms"] }
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

The SDK covers Bronze ingestion to S3 and S3-compatible stores (single calls and streams of any size), and tenant-scoped reads of Parquet and Iceberg tables in every layer. Silver/Gold transformations, writing Iceberg or Delta tables, catalog registration and other object stores are not part of this release.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option. Unless you state otherwise, any contribution you submit for inclusion is dual licensed as above, without additional terms.
