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
