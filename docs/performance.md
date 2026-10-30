# Performance

The numbers on this page are real runs of the published package. Use them to pick a client and a configuration.

## How it was measured

| | Cloud (AWS) | Local |
|---|---|---|
| **Machine** | AWS Lambda, 10 GB memory (6 vCPU), Python 3.12, x86_64, same region as the bucket | Laptop with 8 cores / 16 threads, Windows 11 |
| **SDK** | `0.2.0rc8` installed from the package index | Same code, release build |
| **Storage** | Amazon S3, real uploads | A store that discards bytes, so only the SDK's own work is timed |
| **Data** | 3.16 M records, 10 fields, 1% invalid (quarantined) | Same shape |

Each case runs in a fresh process.
- **Memory** is the process's peak resident set (`VmHWM`), including Python and the input.
- **Throughput** is input size divided by wall time. That includes reading the source and uploading to S3.

---

## Ingestion: one client per use case

3.16 M records from S3 into S3, on AWS:

| How you call it | Input | Time | Records/s | Peak memory |
|---|---|---|---|---|
| `ingest_stream(pyarrow dataset scanner)` | Parquet, 92 MB | **4.1 s** | **0.77 M** | 1.4 GB |
| `ingest_file("s3://…/x.parquet")` | Parquet, 92 MB | **4.0 s** | **0.77 M** | **0.9 GB** |
| `ingest_stream(duckdb result)` | Parquet via DuckDB | 4.4 s | 0.72 M | 1.0 GB |
| `ingest_file("s3://…/x.csv")` | CSV, 468 MB | 5.6 s | 0.56 M | **0.7 GB** |
| `ingest(polars.DataFrame)` | whole frame in memory | 6.1 s¹ | 0.52 M | 1.8 GB |
| `ingest(pyarrow.Table)` | whole table in memory | 6.7 s¹ | 0.47 M | 2.1 GB |
| `ingest_file("s3://…/x.jsonl")` | JSON Lines, 793 MB | 10.2 s | 0.31 M | **0.5 GB** |
| `ingest(list of dicts)`, 200 k records | Python objects | 4.6 s² | 0.09 M | 1.5 GB |
| `ingest(json text)`, 200 k records | JSON string | 3.6 s² | 0.14 M | 1.4 GB |

¹ Includes about 1 s to read the Parquet input into memory.
² Most of the time is building the dicts or the JSON text in Python (2.2–2.3 s); the SDK part takes 1.4–2.2 s.

What this means:
- **Streams are the fastest and lightest way in.** `ingest_file` and `ingest_stream` validate chunk by chunk, read the next chunk while the current one is encoded, and upload Parquet in parallel parts. Memory stays flat whatever the input size: 2 GB of JSON Lines peaks at about 0.4 GB.
- **Columnar input is the fast path.** Parquet and Arrow are validated column by column. JSON and dicts are validated record by record, about 3–6x slower per record.
- **Prefer `ingest_file` for files on S3.** It reads them with 4 parallel ranged requests. A single S3 stream inside a Lambda reached about 37 MiB/s; parallel ranges reach about 90 MiB/s.
- **Use `ingest(...)` for micro-batches** (an API call, a queue message, a Lambda batch): one call writes one object.

### Without the network

The same inputs written to a store that discards the bytes, measuring only the SDK (local, `examples/stream_throughput.rs`):

| Input, ~500 MB | Stream | Peak heap |
|---|---|---|
| JSON Lines | 3.3 s (152 MiB/s) | 294 MiB |
| CSV | 3.3 s (151 MiB/s) | 358 MiB |
| Parquet | 3.3 s (0.95 M records/s) | 415 MiB |

0.2.0rc9 against 0.2.0rc8, run back to back and alternated (median of 4–10 rounds): CSV 3.34 → 3.32 s, JSON Lines 3.30 → 3.29 s, Parquet 3.37 → 3.34 s, with the same output and peak heap.

### Storage options for logs

The same 500 MB streams with the contract options of 0.2.0rc9 (median of 3 rounds):

| Contract | CSV | Parquet | Bronze written |
|---|---|---|---|
| default (Snappy, event time as text) | 3.29 s | 3.32 s | 257 MiB |
| `x-dl-compression: zstd` | 3.32 s (+1%) | 3.38 s (+2%) | **178 MiB (−31%)** |
| `x-dl-type: timestamp` | 3.35 s (+2%) | 3.43 s (+3%) | 251 MiB |
| both | 3.39 s (+3%) | 3.37 s (+1%) | 180 MiB (−30%) |

Streams encode columns in parallel, so zstd adds little time. On one thread (`examples/compression_bench.rs`, 1 M log rows) zstd level 1 encodes about 20% slower than Snappy and writes 41% less; higher levels cost much more CPU for almost no gain. Snappy remains the default because it is the fastest; zstd is the recommendation for logs and other text-heavy tables.

### One call, by input type and protection

`examples/throughput.rs`: 200 MiB, 869 k records, 7 fields, 1 of them PII; local, no network. Mean of 3 rounds alternated with the previous release.

| Input | PII protection | Time | Throughput | Peak heap |
|---|---|---|---|---|
| Arrow | none (default) | **0.38 s** | 550 MiB/s | 200 MiB |
| Arrow | `sha256` | 0.55 s | 380 MiB/s | 243 MiB |
| Arrow | `aes-256-gcm` | 0.72 s | 290 MiB/s | 250 MiB |
| JSON text | none | 2.3 s | 90 MiB/s | 649 MiB |
| JSON text | `sha256` | 2.9 s | 73 MiB/s | 712 MiB |
| JSON text | `aes-256-gcm` | 3.1 s | 67 MiB/s | 768 MiB |

Parquet columns are encoded in parallel. Protected values compress far less than the originals (Parquet output: 18 MiB unprotected, 59–71 MiB protected).

---

## PII protection on AWS

Same 3.16 M records, `PiiProtection.kms_envelope` with a per-tenant KMS key:

| Step | Result |
|---|---|
| `ingest_file` Parquet + KMS envelope | 5.5 s, 0.9 GB peak (one KMS call per tenant per hour, not per record) |
| `PiiDecryptor.decrypt_column`, 3.1 M values | **1.1 s**, about +0.1 GB |
| `PiiDecryptor.decrypt_many`, same values (Python lists) | 3.2 s, about +0.9 GB |

To keep memory low:
- Use `decrypt_column`: Arrow in, Arrow out.
- Read `_dl_pii_key_ref` as a dictionary (`read_dictionary=["_dl_pii_key_ref"]`), so the long key reference is not repeated on every row.

---

## Logs on AWS (0.2.0rc10)

The same 3.16 M records as application logs: event time typed with `x-dl-type: "timestamp"`, streamed in time order through `ingest_stream`. Upload time, not counting reading the source.

| Contract | Objects | Time | Bronze written |
|---|---|---|---|
| zstd, ingest-day partitions, `max_object_seconds=2` | 2 | 3.4 s | 95 MiB |
| zstd, `x-dl-partition_by` (212 event days) | 212 | 3.7 s | 125 MiB |
| Snappy, `x-dl-partition_by` | 212 | 3.3 s | 192 MiB |
| `ingest` of 200 k records over 212 days, zstd | 212 | 1.2 s | 9 MiB |
| 500 k records out of time order, `max_open_days=4` | 848 | 1.8 s | 25 MiB |

Small objects are uploaded concurrently within the stream's memory budget, so event-day partitions cost about the same as one large object to write; 0.2.0rc9 uploaded them one by one (13.7 s for the 212-day stream). Reading them is a different matter: many small objects make uncached scans slower, see the partitioning guidance in the README.

Streams that write one large object were compared with 0.2.0rc9 in the same function, alternating the two versions, 6 runs each (median): DuckDB result 4.58 → 4.61 s, Parquet file 4.25 → 4.13 s, dataset scanner 4.09 → 4.14 s, CSV file 5.37 → 5.49 s. The differences are within the spread of each version (about ±8%).

---

## OpenTelemetry logs (local)

`examples/otel_throughput.rs`: OTLP logs requests with 3 attributes per record, trace context and 4 tenants, flattened, validated against `logs_contract()` and encoded with zstd into a store that discards the bytes. Every case carries the same logical requests. *Flatten* is decoding into Arrow rows; *total* adds validation and Parquet encoding. Medians of alternating runs (0.4.0rc1 code).

| 200 k records | Size | Flatten | Total |
|---|---|---|---|
| OTLP/JSON, one request | 84 MiB | 0.55 s | 0.70 s |
| OTLP/JSON, 100 requests (one queue batch) | 84 MiB | 0.15 s | 0.27 s |
| OTLP/protobuf, one request | 32 MiB | **0.20 s** (2.8x) | **0.36 s** |
| OTLP/protobuf, 100 requests | 32 MiB | **0.075 s** (2.0x) | **0.18 s** |
| OTLP/protobuf + gzip, 100 requests | 7.2 MiB | 0.073 s | 0.18 s |
| OTLP/protobuf + zstd, 100 requests | 6.0 MiB | 0.077 s | 0.18 s |
| OTLP/JSON + zstd, 100 requests | 5.9 MiB | 0.17 s | 0.28 s |

- **Peak memory** with 100 requests, including the requests the benchmark holds: JSON 270-283 MiB, protobuf 184-189 MiB, protobuf + zstd 153-161 MiB.
- **Why protobuf is cheaper:** 2.6x fewer bytes, no text escaping, binary ids and timestamps. The reader parses the wire format straight into the Arrow builders with borrowed strings. Decoding the same 200 k-record request into the structs generated by `prost` (`opentelemetry-proto` 0.33) takes 0.25 s on its own, more than this reader's complete decode and flatten (0.21 s).
- **OTLP/JSON against 0.3.0:** builds of both versions on the same machine, 8 runs each in rotating order. One request: flatten 0.54 → 0.55 s, total 0.69 → 0.70 s. 100 requests: 0.15 → 0.15 s. Peak memory unchanged. All within run-to-run noise.

On AWS (Lambda 10 GB / 6 vCPU, writing to S3 in the same region, 0.2.0rc11):

| Case | Result |
|---|---|
| `ingest_otel_log_batch`: 1 M records in 500 requests (396 MiB of OTLP/JSON), 8 tenants | **2.5 s** including the uploads (158 MiB/s, 0.4 M records/s), one object per tenant, 1.5 GB peak including the requests held in Python |
| The Lambda recipe: a Kinesis batch of 100 requests (100 k records), one unreadable and one with an unknown tenant | **0.5 s**; only those two messages reported in `batchItemFailures`; redelivering the batch rewrote the same objects |

Single-object streams were compared with 0.2.0rc10 in the same function, alternating versions (median of 3): dataset scanner 4.49 → 4.33 s, DuckDB result 4.78 → 4.60 s.

Requests of a batch are flattened in parallel. OTLP is read into typed structures with borrowed strings, and the rows take the columnar validation path; the first implementation, which built a JSON tree per record and fell back to the row path, took 3.98 s for the single request.

---

## Reading: `LakeReader`

Bronze table of 3.16 M records in one object. Warm queries are the median of 5 calls in the same process, on AWS:

| Query | Polars, `cache_mb=1024` | Polars, no cache | DuckDB, no cache | DuckDB, `cache_mb=1024` |
|---|---|---|---|---|
| Aggregate | **22 ms** | 81 ms | 100 ms | 107 ms |
| Keyset page of 1,000 | **37 ms** | 1,402 ms | 185 ms | 183 ms |
| Export 3.1 M rows to Arrow | **67 ms** | 1,487 ms | 955 ms | 664 ms |
| First query in a new process | 2.4–2.6 s | 0.9–1.3 s | 0.8–1.9 s | 2.5–3.3 s |
| Peak memory | 1.9–2.1 GB | 0.4–0.8 GB | 0.3–0.7 GB | 1.5–1.9 GB |

What this means:
- **Services that read the same tenants repeatedly** (APIs, dashboards): Polars with `cache_mb` answers in tens of milliseconds. Objects written by the SDK never change, so later reads fetch only new objects.
- **Without a cache** (short-lived jobs, low memory): DuckDB, whose Parquet reader skips data well when reading straight from S3.
- **Iceberg tables** (few, large files) need no cache. Earlier runs measured 67–147 ms warm with either engine.
- **A query service** (Athena) took 0.9–2.1 s per query on the same data, plus queueing.

---

## Iceberg writes on AWS (0.3.0)

`LakeWriter` on AWS Lambda (10 GB / 6 vCPU) with the AWS Glue catalog, S3 in the same region; a shared table partitioned by `tenant_id` and day.

| Operation | Time | Notes |
|---|---|---|
| Append 3.16 M rows | 8.1 s | one commit |
| The same batch again | 0.36 s | skipped by `batch_id` |
| Upsert 29 k rows of 2 days, `partition_scoped=True` | 2.5 s | only those partitions rewritten |
| Upsert 100 k keys spread over every partition | 14.1 s | matched in Arrow; 545 s before (PyIceberg binds its `In` filter again per data file) |
| Replace one day | 2.7 s | |
| 16 appends by 4 tenants at once | all committed | catalog conflicts retried, no error |
| Two upserts of the same 29 k keys at once | the second re-applied | no key stored twice |
| Compact 210 small files into 7 | 67 s | within a 600 s budget |
| Expire 35 snapshots | 0.95 s | |

Writing the same 1 M rows with DuckDB 1.5 through the Glue Iceberg REST catalog took 53.5 s against 8.1 s for 3.16 M rows with `LakeWriter`: the writer is built on PyIceberg, the only engine with commit retries and conflict validation for overwrite and upsert.

## Serving Gold on AWS

An API pattern over Gold Iceberg tables (scan lookup, family total, page of 100 rows), 1.92 M rows for 4 tenants over 12 monthly partitions, 60 requests across tenants after one cold request:

| Reader | p50 | p95 | First request |
|---|---|---|---|
| `LakeReader(engine="polars", cache_mb=2048)` | **6.3 ms** | **36.4 ms** | 2.3 s |
| DuckDB attached to the Glue Iceberg REST catalog, `MAX_TABLE_STALENESS '2 minutes'` | 208 ms | 555 ms | 2.7 s |
| `LakeReader(engine="duckdb")`, no cache | 823 ms | 1,166 ms | 3.1 s |

The cache keeps the tenant's data files in memory (about 1.8 GB for these tables); see [serving-gold.md](serving-gold.md).

## Configuration in brief

| Setting | Default | When to change it |
|---|---|---|
| `ingest_file(block_mb=)` | 8 | Rarely. Larger blocks used more memory and were not faster. |
| `chunk_rows` | 131,072 | Lower it for very wide records or tight memory limits. |
| `row_group_mb` / `row_group_bytes` | 64 MB | 128 MB was 2–7% faster for about 170 MB more memory. |
| `target_object_mb` | 256 MB | Lower it for faster visibility of continuous streams; keep 128–512 MB for analytics. |
| `max_inflight_parts` | 4 | Raise it on high-bandwidth networks. |
| `LakeReader(engine=)` | `"polars"` | `"duckdb"` for no cache, the lowest memory or SQL. |
| `LakeReader(cache_mb=)` | 0 (off) | Size it to the tenants a service reads repeatedly. |
| `batch_id=` / `stream_id=` | new ULID | Pass the earlier id to retry a call without duplicating rows. |

---

## Reproduce

- `cargo run --release --example throughput`: one call, by input type and protection (`DL_BENCH_MB` sets the size).
- `cargo run --release --example stream_throughput -- <file> <contracts dir> <contract key>`: streams without network. `DL_PII=aes|sha256` adds protection; `DL_ONESHOT=1` compares with one call.
- `cargo bench`: Criterion micro-benchmarks.

Releases are compared with the previous one by building both and running them alternately. A release must not be slower.
