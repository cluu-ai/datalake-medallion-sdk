# datalake-medallion-sdk

[![CI](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/cluu-ai/datalake-medallion-sdk/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/datalake-medallion-sdk)](https://pypi.org/project/datalake-medallion-sdk/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org)

Contract-enforced Bronze-layer ingestion for medallion data lakes, with a Rust core and a Python API.

Every record is checked against a JSON Schema data contract before anything is written. Valid records become one Hive-partitioned Parquet object with lineage columns; invalid records go to a dead-letter prefix with the reason attached. Fields the contract marks as PII are hashed before they reach storage, in the data and in the dead letters alike; masking is on by default and can be turned off per client or per call.

```python
import asyncio
from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry

registry = ContractRegistry.from_directory("contracts")  # compiles every contract, all-or-nothing
client = BronzeIngestionClient("acme-lake", registry)  # AWS default credential chain

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
        tenant_id="acme-001",
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

Wheels are `abi3` (one wheel per platform for CPython 3.12+) for Linux (glibc and musl, x86_64 and aarch64), macOS on Apple silicon and Windows x86_64. Other platforms, including Intel macOS, build from the source distribution and need a Rust toolchain. There are no mandatory Python dependencies: Arrow input uses the [PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html), so pyarrow and Polars are optional.

## What happens to a record

| Level | Check | On failure |
|---|---|---|
| 0 | Contracts compile at boot; hooks must target known contracts; contract fields may not use the envelope prefix | `ContractBootError` |
| 1 | Tenant id: 3–63 chars of `[a-z0-9_-]`, no leading/trailing or doubled separators, not a reserved word | `TenantViolationError` |
| 2 | Record is a JSON object, within the size limit (10 MiB default), without fields in the reserved envelope namespace | quarantined |
| 3 | JSON Schema (formats asserted); in `strict` mode, no undeclared fields | quarantined |
| 4 | Optional host hook may accept, rewrite or reject; rewritten records are validated again | quarantined |
| 5 | `date-time` fields not more than 1 h (configurable) in the future | quarantined |
| 6 | PII fields hashed (SHA-256, or HMAC-SHA256 with your key); on by default, optional | applied when enabled |
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
- `schema_on_read`: no schema enforcement; PII masking (when enabled), type coercion and lineage still apply.

The Parquet schema comes from the contract, not from the data, so every object of a contract version has the same columns: `string` → utf8, `integer` → int64, `number` → float64, `boolean` → bool, anything else (objects, arrays, unions) → JSON text. When masking is on, PII fields are stored as their 64-char hex digest (utf8); when it is off they keep their declared type. Keep one masking mode per contract if a PII field is not a string, so its column type stays stable.

Contracts must be self-contained: `$ref` to remote URLs or files is not resolved, so loading contracts never performs network I/O.

## Object layout

```
s3://<bucket>/bronze/tenant_id=<tenant>/data_type=<datatype>/year=YYYY/month=MM/day=DD/<batch_id>.parquet
s3://<bucket>/dlq/tenant_id=<tenant>/data_type=<datatype>/error_code=<code>/year=YYYY/month=MM/day=DD/<batch_id>.jsonl
```

Prefixes and the order of the `tenant_id`/`data_type` partitions (`partition_order="data_type_first"`) are configurable. Each ingest call writes at most one Parquet object plus one dead-letter object per error code. Dead letters are written first, so retrying a failed call never duplicates Bronze rows.

Lineage columns (prefix `_dl_`, configurable):

| Column | Type | Content |
|---|---|---|
| `_dl_ingest_id` | utf8 | ULID per row, monotonic within the batch |
| `_dl_batch_id` | utf8 | ULID of the call; names the objects |
| `_dl_tenant_id` | utf8 | tenant |
| `_dl_data_type`, `_dl_schema_version`, `_dl_schema_hash` | utf8 | contract identity and SHA-256 |
| `_dl_received_at` | timestamp(µs, UTC) | ingest time |
| `_dl_pii_masking` | utf8 | `sha256`, `hmac-sha256` or `none` |
| `_dl_sdk_version` | utf8 | SDK version |

## Inputs

`ingest` accepts a `dict`, a list of dicts, JSON `str`/`bytes`, or any object implementing the Arrow PyCapsule interface, such as a pyarrow `Table`/`RecordBatch` or a Polars `DataFrame`. Arrow rows get the same validation and the same results as JSON; when every rule of the contract applies to one field at a time, Arrow input is checked column by column and only rows that may be invalid are converted to JSON. Validation and encoding run in Rust outside the GIL.

## Hooks

```python
from datalake_medallion_sdk import HookRegistry, PreIngestHookResult

hooks = HookRegistry()


@hooks.register("security/login_event/1.0.0")
def drop_test_accounts(record: dict) -> PreIngestHookResult:
    if record.get("user_email", "").endswith("@test.invalid"):
        return PreIngestHookResult.reject("ERR_TEST_ACCOUNT", "test account")
    return PreIngestHookResult.accept(record)


client = BronzeIngestionClient("acme-lake", registry, hooks=hooks.as_mapping())
```

## Operations

- **Credentials**: the AWS default chain (environment, profile, SSO, web identity, ECS/EC2 roles) unless `aws_access_key_id`/`aws_secret_access_key` are passed. The SDK never creates buckets; the writer role needs `s3:PutObject` on the data and dead-letter prefixes only.
- **S3-compatible stores**: `aws_endpoint_url=` (path-style addressing is enabled automatically).
- **PII masking**: on by default. `BronzeIngestionClient(..., pii_masking=False)` turns it off for the client; `ingest(..., pii_masking=True|False)` overrides it for one call. `IngestReport.pii_masking` and the `_dl_pii_masking` column record what was applied.
- **Pseudonymization key**: pass `pii_hmac_key=` (16+ bytes, kept outside the lake) to use HMAC-SHA256; unkeyed SHA-256 of low-entropy values such as e-mail addresses can be reversed by dictionary attack.
- **Retries**: `@datalake_retry()` retries `StorageError` only, with exponential backoff and jitter.
- **Metrics**: `@emit_metrics(sink)` records duration, failures and record counts to logging, CloudWatch Embedded Metric Format or any `MetricsSink`.
- **Concurrency**: one client per process can serve every tenant; the S3 connection is created once and reused.

## Performance

Measured 2026-10-13 with Criterion on a laptop (AMD Ryzen 7 7730U, 8 cores, Windows 11, release build), contract with 4 fields of which 1 is PII. `prepare` covers every validation level, PII hashing and Parquet encoding, without network I/O.

| Benchmark | Records | Mean | Throughput |
|---|---|---|---|
| `prepare`, JSON text input | 100,000 | 199 ms | 0.50 M records/s |
| `prepare`, Arrow input (columnar path) | 100,000 | 81 ms | 1.24 M records/s |
| `prepare`, Arrow input forced through the row path | 100,000 | 206 ms | 0.49 M records/s |
| Contract check, valid records | 10,000 | 1.96 ms | 5.1 M records/s |
| Contract check, invalid records (errors rendered) | 10,000 | 22.4 ms | 0.45 M records/s |
| PII masking, SHA-256, 2 fields | 10,000 | 4.3 ms | 2.3 M records/s |
| PII masking, HMAC-SHA256, 2 fields | 10,000 | 5.2 ms | 1.9 M records/s |

The `prepare` rows were measured on 2026-10-17 with `opt-level = 3` and LTO disabled; published wheels are built with LTO. Reproduce with `cargo bench`. Upload time depends on the network and is not included.

## Rust crate

The core is usable from Rust (`BronzePipeline`, `ContractRegistry`, the `ObjectStorage` port with S3 and in-memory adapters). See the crate documentation.

## Development

```bash
cargo test --all-targets            # Rust unit tests
pip install -e ".[dev]"             # builds the extension with maturin
pytest pytests                      # Python tests, including end-to-end against an in-process S3 emulator (moto)
cargo clippy --all-targets -- -D warnings && ruff check python pytests && mypy
```

Minimum supported Rust version: 1.94.1.

## Scope

Version 0.1 covers Bronze-layer ingestion to S3 and S3-compatible stores. Silver/Gold transformations, table formats (Iceberg/Delta), catalog registration and other object stores are not part of this release.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option. Unless you state otherwise, any contribution you submit for inclusion is dual licensed as above, without additional terms.
