# datalake-medallion-sdk

[![CI](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/datalake-medallion-sdk)](https://pypi.org/project/datalake-medallion-sdk/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org)

Contract-enforced Bronze-layer ingestion for medallion data lakes, with a Rust core and a Python API.

Every record is checked against a JSON Schema data contract before anything is written. Valid records become one Hive-partitioned Parquet object with lineage columns; invalid records go to a dead-letter prefix with the reason attached. Every call names its tenant, and the tenant's profile decides where its data goes. Fields the contract marks as PII are stored as received by default; a tenant profile can protect them with SHA-256, HMAC-SHA256, AES-256-GCM envelope encryption (for example a KMS data key per tenant) or a scheme of your own, in the data and in the dead letters alike. `LakeReader` reads a tenant's Parquet and Iceberg tables back straight from S3, without Athena or Glue.

```python
import asyncio
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    TenantProfile,
    TenantRegistry,
)

registry = ContractRegistry.from_directory("contracts")  # compiles every contract, all-or-nothing
tenants = TenantRegistry([TenantProfile.for_bucket("acme-001", "acme-lake")])
client = BronzeIngestionClient(registry, tenants=tenants)  # AWS default credential chain

report = asyncio.run(
    client.ingest(
        [
            {
                "event_id": "e1",
                "occurred_at": "2026-10-01T12:00:00Z",
                "user_email": "ana@example.com",
            },
            {"event_id": 42},
        ],
        tenant="acme-001",
        contract="security/login_event/1.0.0",
    )
)
print(report.records_ok, report.records_quarantined, report.rejections)
# 1 1 {'ERR_SCHEMA_VIOLATION': 1}
```

## Install

```bash
pip install datalake-medallion-sdk
```

Wheels are `abi3` (one wheel per platform for CPython 3.12+) for Linux (glibc and musl, x86_64 and aarch64), macOS on Apple silicon and Windows x86_64. Other platforms, including Intel macOS, build from the source distribution and need a Rust toolchain. There are no mandatory Python dependencies: Arrow input uses the [PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html), so pyarrow and Polars are optional. Reading the lake uses the `[read]` extra (Polars, pyarrow, pyiceberg) or the `[duckdb]` extra.

## What happens to a record

| Level | Check | On failure |
|---|---|---|
| 0 | Contracts compile at boot; hooks must target known contracts; contract fields may not use the envelope prefix | `ContractBootError` |
| 1 | Tenant id: 3–63 chars of `[a-z0-9_-]`, no leading/trailing or doubled separators, not a reserved word | `TenantViolationError` |
| 2 | Record is a JSON object, within the size limit (10 MiB default), without fields in the reserved envelope namespace | quarantined |
| 3 | JSON Schema (formats asserted); in `strict` mode, no undeclared fields | quarantined |
| 4 | Optional host hook may accept, rewrite or reject; rewritten records are validated again | quarantined |
| 5 | `date-time` fields not more than 1 h (configurable) in the future | quarantined |
| 6 | PII fields protected with the tenant's scheme: none by default; SHA-256, HMAC-SHA256, AES-256-GCM or your own | `ProtectionError` (nothing written) |
| 7 | Each value fits the Parquet column type derived from the contract | quarantined |
| 8 | Lineage columns appended | always applied |

With `on_invalid="raise"` the client writes nothing and raises the typed error (`SchemaViolationError`, `HookError`, …) for the first invalid record instead of quarantining.

Error messages and dead-letter entries never contain record values: schema errors are rendered with values masked, and hook exceptions are recorded by type name only.

## Data contracts

A contract is a JSON Schema document plus a few keywords under an extension prefix (`x-dl-` by default, configurable per registry):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "x-dl-domain": "security",
  "x-dl-datatype": "login_event",
  "x-dl-schema_version": "1.0.0",
  "x-dl-flexibility_mode": "strict",
  "required": ["event_id", "occurred_at"],
  "properties": {
    "event_id":    {"type": "string"},
    "occurred_at": {"type": "string", "format": "date-time"},
    "attempts":    {"type": "integer", "minimum": 0},
    "user_email":  {"type": "string", "x-dl-pii": true}
  }
}
```

The key `domain/datatype/schema_version` (here `security/login_event/1.0.0`) selects the contract at ingest time. Flexibility modes:

- `strict` (default): schema enforced, undeclared fields rejected.
- `permissive`: schema enforced, undeclared fields kept as JSON in `_dl_extra`.
- `schema_on_read`: no schema enforcement; PII protection (when the tenant has one), type coercion and lineage still apply.

The Parquet schema comes from the contract, not from the data, so every object of a contract version has the same columns: `string` → utf8, `integer` → int64, `number` → float64, `boolean` → bool, anything else (objects, arrays, unions) → JSON text. When the tenant protects PII, PII fields are stored as utf8 (digest or token); otherwise they keep their declared type. Keep a tenant on one protection setting if a PII field is not a string, so the column type of its tables stays stable.

Contracts must be self-contained: `$ref` to remote URLs or files is not resolved, so loading contracts never performs network I/O.

## Tenants and destinations

The client holds contracts and a connection, never a destination. Each `ingest` names its tenant, and the tenant's `TenantProfile` fixes, for that tenant only:

- an `s3://bucket/prefix` location per medallion layer: `bronze` and `quarantine` are required (ingestion writes them); `silver` and `gold` are optional and resolved the same way, so downstream jobs read their locations from the same profile (`profile.location("silver")`, `profile.table_uri("silver", "login_event")`);
- the order of the `tenant_id`/`data_type` partitions;
- the PII protection scheme, if any.

Profiles are validated when they are built: bucket names follow the AWS rules, prefixes are safe key segments, and two layers may not share or nest a location. Every key still carries a `tenant_id=` partition, even in a tenant's own bucket.

```python
from datalake_medallion_sdk import PiiProtection, TenantProfile, TenantRegistry

acme = TenantProfile.for_bucket("acme-001", "acme-lake")  # bronze/, quarantine/, silver/, gold/
beta = TenantProfile(
    "beta-002",
    bronze="s3://beta-raw/landing",
    quarantine="s3://beta-quarantine/dlq",
    silver="s3://beta-curated/silver",
    partition_order="data_type_first",
    pii_protection=PiiProtection.hmac_sha256(beta_key),
)

client = BronzeIngestionClient(registry, tenants=TenantRegistry([acme, beta]))
await client.ingest(rows, tenant="beta-002", contract="security/login_event/1.0.0")  # looked up
await client.ingest(rows, tenant=acme, contract="security/login_event/1.0.0")  # passed directly
```

`tenants=` also accepts a function `(tenant_id) -> TenantProfile`, to load profiles from your own configuration store; a profile returned for another tenant is rejected.

Profiles can also come from a configuration document (a dict or a JSON file) with `TenantRegistry.from_config(...)`; unknown fields are rejected and secrets never live in it:

```json
{"tenants": [{
  "tenant_id": "acme-001",
  "bronze": "s3://acme-lake/bronze", "quarantine": "s3://acme-lake/quarantine",
  "pii_protection": {"scheme": "aes-256-gcm", "provider": "aws-kms",
                     "kms_key_id": "alias/lake-acme-001", "data_key_max_age_seconds": 3600}
}]}
```

`pii_protection` is optional (`none`, `sha256`, `hmac-sha256` with `key_env` naming the environment variable that holds the key, or `aes-256-gcm` with `aws-kms`).

## PII protection

Off by default: PII fields keep their declared type and value. Set `pii_protection` on a tenant profile to protect them in Bronze and in the dead letters:

| Scheme | `PiiProtection.` | Output | Reversible |
|---|---|---|---|
| `sha256` | `sha256()` | 64 hex chars | no; low-entropy values can be guessed |
| `hmac-sha256` | `hmac_sha256(key)` | 64 hex chars, equal values still join | no; keep the 16+ byte key outside the lake |
| `aes-256-gcm` | `kms_envelope(kms_key_id)` (recommended) or `aes_256_gcm(data_key, key_ref=...)` | `v1:` + base64url token | yes, with the data key |
| your own | `custom(obj)` | whatever `obj.protect_batch` returns | your choice |

Per-tenant keys with AWS KMS (envelope encryption, recommended for reversible protection):

```python
protection = PiiProtection.kms_envelope("alias/lake-acme-001")  # the tenant's KMS key
acme = TenantProfile.for_bucket("acme-001", "acme-lake", pii_protection=protection)
```

The SDK asks KMS for a data key the first time a tenant needs one and reuses it until it is one hour old or has drawn 2^15 nonces (`data_key_max_age`, `max_nonce_prefixes_per_key`), refreshing it ahead of expiry. Keys are cached per tenant and never shared; if KMS fails and the current key has expired, the call fails with `ProtectionError` and nothing is written. Data keys carry the encryption context `{"tenant_id": <tenant>, "purpose": "datalake-medallion-sdk/pii"}`, so IAM can restrict each role to its tenant, CloudTrail shows which tenant every key belongs to, and a key reference copied to another tenant cannot be decrypted. The plaintext key lives only in memory; `_dl_pii_key_ref` stores `kms1:<key arn>:<encrypted key>`. Any other key service plugs in through the `DataKeyProvider` protocol (`generate`, `decrypt`) and `PiiProtection.envelope(provider)`.

Reading protected values back (one `Decrypt` per data key, not per value):

```python
reader = PiiDecryptor.kms()
emails = reader.decrypt_many(
    table.column("user_email").to_pylist(),
    tenant_id="acme-001",
    field="user_email",
    key_ref=table.column("_dl_pii_key_ref")[0].as_py(),
)
```

Least-privilege IAM for this scheme: the ingestion role needs `kms:GenerateDataKey` and readers `kms:Decrypt` on the tenant's key, both with the condition `"StringEquals": {"kms:EncryptionContext:tenant_id": "<tenant>"}`. Enable automatic rotation of the KMS key; old data keys keep working. Encrypt only what must be reversible: tokens do not compress, so encrypted columns cost more to store and scan; HMAC suits fields that only need to be joined.

A custom scheme is any object with a `scheme` name, an optional `key_ref` and `protect_batch(tenant_id, field, values) -> list[str]` (the `FieldProtector` protocol). It is called once per field and batch, so it can reach a key service in bulk; if it raises, the call fails with `ProtectionError` and nothing is written.

## Object layout

```
<bronze location>/tenant_id=<tenant>/data_type=<datatype>/year=YYYY/month=MM/day=DD/<batch_id>.parquet
<quarantine location>/tenant_id=<tenant>/data_type=<datatype>/error_code=<code>/year=YYYY/month=MM/day=DD/<batch_id>.jsonl
```

Locations and the order of the `tenant_id`/`data_type` partitions (`partition_order="data_type_first"`) come from the tenant profile. Each ingest call writes at most one Parquet object plus one dead-letter object per error code. Dead letters are written first, so retrying a failed call never duplicates Bronze rows.

Lineage columns (prefix `_dl_`, configurable):

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

## Inputs

`ingest` accepts a `dict`, a list of dicts, JSON `str`/`bytes`, or any object implementing the Arrow PyCapsule interface, such as a pyarrow `Table`/`RecordBatch` or a Polars `DataFrame`. Arrow rows get the same validation and the same results as JSON; when every rule of the contract applies to one field at a time, Arrow input is checked column by column and only rows that may be invalid are converted to JSON. Validation and encoding run in Rust outside the GIL.

## Reading the lake

`LakeReader` reads one tenant's Parquet and Iceberg tables directly from S3 with an in-process engine. It needs no Athena query, no Glue table and no query queue. Results are Arrow tables.

```python
import polars as pl
from datalake_medallion_sdk import LakeReader, TableRef

reader = LakeReader(tenants=tenants)  # pip install "datalake-medallion-sdk[read]"

page = reader.read(
    "acme-001",
    TableRef.bronze("login_event"),
    columns=["event_id", "occurred_at"],
    where=[("occurred_at", ">", last_seen)],  # pushed down to the scan
    order_by=["occurred_at", "event_id"],
    limit=1000,  # keyset pagination
)

summary = TableRef.iceberg(
    "gold", "compliance_summary", location="s3://lake-gold/compliance_summary"
)
by_framework = (
    reader.scan("acme-001", summary).group_by("framework").agg(pl.col("total").sum()).collect()
)
```

- **Tenant isolation.**
  - Callers name a tenant and a table, never a bucket, a path or a tenant filter. Filtering on `tenant_id` is rejected.
  - Parquet objects are listed only below the tenant's prefix (`TenantProfile.table_uri`).
  - A `tenant_id` filter is always applied when the data has that column.
  - A shared Iceberg table without a `tenant_id` column is refused.
- **Tables.**
  - `TableRef.bronze(data_type)` reads the ingestion client's output.
  - `TableRef.parquet(layer, name)` reads Hive-partitioned Parquet in Silver or Gold.
  - `TableRef.iceberg(layer, name)` reads an Iceberg table:
    - by default, the table in the tenant's location;
    - for a shared table, one of `location=`, `metadata_location=` or `identifier=` (with `catalog=`, any pyiceberg catalog: REST, Glue, SQL).
  - Without a catalog, the current snapshot is the newest `metadata/*.metadata.json`. Use a catalog when several writers commit concurrently.
- **Caching.**
  - Listings, Iceberg snapshots and scan plans are reused for `max_staleness` seconds (default 60).
  - `refresh(tenant)` makes new data visible at once.
- **Engines.**
  - `engine="polars"` (default, extra `[read]`) is the fastest for repeated reads.
  - `engine="duckdb"` (extra `[duckdb]`) uses about half the memory, starts fastest and gives SQL over `scan()` (`relation.query(...)`).
  - Pass `duckdb_config={"home_directory": "/tmp"}` on AWS Lambda.
  - DuckDB installs its `httpfs` and `iceberg` extensions on first use, unless `extension_directory` points to pre-installed ones.
- **Credentials:**
  - The same options as the client (`aws_region`, `aws_endpoint_url`, keys) or the AWS default chain.
  - The reader role needs `s3:ListBucket` and `s3:GetObject` on the tenant's locations only.

Measured on 3.16 M rows in 8 tenants on local disk, warm reads of one tenant.

| Query | polars, Bronze | DuckDB, Bronze | polars, Iceberg | DuckDB, Iceberg |
|---|---|---|---|---|
| Aggregate one month | 14 ms | 47 ms | 8 ms | 15 ms |
| 3 ids | 19 ms | 63 ms | 14 ms | 29 ms |
| Keyset page of 1,000 | 23 ms | 48 ms | 20 ms | 24 ms |
| Export 395 k rows to Arrow | 69 ms | 227 ms | 25 ms | 69 ms |

- Peak memory: 86–174 MiB with DuckDB and 113–398 MiB with polars.
- First query: about 300 ms with DuckDB and 600–950 ms with polars.
- Listing only the tenant's prefix instead of the whole lake made reads 6.5x faster.
- For comparison, Athena takes 0.5–2.5 s per query on similar data, plus queueing.

## Hooks

```python
from datalake_medallion_sdk import HookRegistry, PreIngestHookResult

hooks = HookRegistry()


@hooks.register("security/login_event/1.0.0")
def drop_test_accounts(record: dict) -> PreIngestHookResult:
    if record.get("user_email", "").endswith("@test.invalid"):
        return PreIngestHookResult.reject("ERR_TEST_ACCOUNT", "test account")
    return PreIngestHookResult.accept(record)


client = BronzeIngestionClient(registry, tenants=tenants, hooks=hooks.as_mapping())
```

## Operations

- **Credentials**: the AWS default chain (environment, profile, SSO, web identity, ECS/EC2 roles) unless `aws_access_key_id`/`aws_secret_access_key` are passed. The SDK never creates buckets; the writer role needs `s3:PutObject` on each tenant's bronze and quarantine locations only.
- **S3-compatible stores**: `aws_endpoint_url=` (path-style addressing is enabled automatically).
- **PII**: stored as received unless the tenant profile sets `pii_protection`. `IngestReport.pii_protection`, the `_dl_pii_protection`/`_dl_pii_key_ref` columns and each dead letter's `pii_protection` record what was applied.
- **Retries**: `@datalake_retry()` retries `StorageError` only, with exponential backoff and jitter.
- **Metrics**: `@emit_metrics(sink)` records duration, failures and record counts to logging, CloudWatch Embedded Metric Format or any `MetricsSink`.
- **Concurrency**: one client per process serves every tenant and bucket; the S3 connection is created once and reused. Buckets must be reachable in the client's region.

## Performance

One batch of about 200 MiB (868,961 records, 7 fields of which 1 is PII), measured on 2026-10-17 on a laptop (AMD Ryzen 7 7730U, 8 cores / 16 threads, Windows 11) with `examples/throughput.rs`, built with `opt-level = 3` and LTO disabled (published wheels use LTO). `prepare` covers every validation level, PII protection and Parquet encoding, without network I/O. Peak heap is the memory allocated on top of the input.

| Input | PII protection | Time | Throughput | Records/s | Peak heap | Parquet |
|---|---|---|---|---|---|---|
| Arrow | none (default) | 0.64 s | 326 MiB/s | 1.35 M | 173 MiB | 18 MiB |
| Arrow | `sha256` | 0.89 s | 236 MiB/s | 0.98 M | 254 MiB | 71 MiB |
| Arrow | `aes-256-gcm` | 1.03 s | 204 MiB/s | 0.85 M | 242 MiB | 59 MiB |
| JSON text | none (default) | 2.76 s | 76 MiB/s | 0.32 M | 784 MiB | 18 MiB |
| JSON text | `sha256` | 3.36 s | 62 MiB/s | 0.26 M | 895 MiB | 71 MiB |
| JSON text | `aes-256-gcm` | 3.38 s | 62 MiB/s | 0.26 M | 940 MiB | 59 MiB |

Arrow input is about 4x faster than JSON and needs less extra memory than its own size: accepted columns are reused without copying and only rows that may be invalid are converted. Protection costs 40-60% more time on Arrow, and protected values compress far less than the originals.

Reproduce with `DL_BENCH_MB=200 cargo run --release --example throughput`; `cargo bench` runs the Criterion micro-benchmarks (contract checks, PII schemes, Parquet encoding, `prepare` by input type). Upload time depends on the network and is not included.

## Rust crate

The core is usable from Rust (`BronzePipeline`, `ContractRegistry`, `TenantProfile`, `FieldProtector`, the `ObjectStorage` port with S3 and in-memory adapters). Disable the default `python` feature (`default-features = false`) to build without Python, for example for an AWS Lambda.

## Development

```bash
cargo test --all-targets            # Rust unit tests
pip install -e ".[dev]"             # builds the extension with maturin
pytest pytests                      # Python tests, including end-to-end against an in-process S3 emulator (moto)
cargo clippy --all-targets -- -D warnings && ruff check python pytests && mypy
```

Minimum supported Rust version: 1.94.1.

## Scope

The SDK covers Bronze-layer ingestion to S3 and S3-compatible stores, and tenant-scoped reads of Parquet and Iceberg tables in every layer. Silver/Gold transformations, writing Iceberg or Delta tables, catalog registration and other object stores are not part of this release.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option. Unless you state otherwise, any contribution you submit for inclusion is dual licensed as above, without additional terms.
