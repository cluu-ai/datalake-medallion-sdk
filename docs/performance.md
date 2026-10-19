# Performance

## Ingestion

**Setup**
- One batch of about 200 MiB: 868,961 records, 7 fields, 1 of them PII.
- Measured on 2026-10-17 on a laptop: AMD Ryzen 7 7730U, 8 cores / 16 threads, Windows 11.
- Tool: [`datalake-medallion-sdk/examples/throughput.rs`](../datalake-medallion-sdk/examples/throughput.rs), built with `opt-level = 3` and LTO disabled. Published wheels use LTO.

**What is measured**
- `prepare` covers every validation level, PII protection and Parquet encoding.
- No network I/O is included; upload time depends on the network.
- Peak heap is the memory allocated on top of the input.

| Input | PII protection | Time | Throughput | Records/s | Peak heap | Parquet |
|---|---|---|---|---|---|---|
| Arrow | none (default) | 0.64 s | 326 MiB/s | 1.35 M | 173 MiB | 18 MiB |
| Arrow | `sha256` | 0.89 s | 236 MiB/s | 0.98 M | 254 MiB | 71 MiB |
| Arrow | `aes-256-gcm` | 1.03 s | 204 MiB/s | 0.85 M | 242 MiB | 59 MiB |
| JSON text | none (default) | 2.76 s | 76 MiB/s | 0.32 M | 784 MiB | 18 MiB |
| JSON text | `sha256` | 3.36 s | 62 MiB/s | 0.26 M | 895 MiB | 71 MiB |
| JSON text | `aes-256-gcm` | 3.38 s | 62 MiB/s | 0.26 M | 940 MiB | 59 MiB |

Findings:
- **Arrow input is about 4x faster than JSON.** It also needs less extra memory than its own size: accepted columns are reused without copying, and only rows that may be invalid are converted.
- **Protection costs 40–60% more time on Arrow.**
- **Protected values compress far less** than the originals.

To reproduce, run these from the `datalake-medallion-sdk/` folder:
- `DL_BENCH_MB=200 cargo run --release --example throughput`: the table above.
- `cargo bench`: the Criterion micro-benchmarks (contract checks, PII schemes, Parquet encoding, `prepare` by input type).

## Streaming ingestion (`ingest_file`, `ingest_stream`)

**Setup**
- Inputs: 500 MB files, and one 2 GB JSON Lines file (6.7 M records).
- Defaults: 8 MB read blocks, 131,072-row chunks, 64 MB row groups, 16 MB parts, 4 parts in flight.
- S3 is an emulator running in another process, so the memory shown is the SDK process alone.
- Measured on 2026-10-18 on the 16-thread laptop. Times include S3 uploads to the emulator.

| Input | One `ingest` call | `ingest_file` (streaming) |
|---|---|---|
| CSV, 500 MB (3.16 M records) | 6.9 s, peak 2,743 MB | 7.0 s, peak **765 MB** |
| JSON Lines, 500 MB (1.68 M records) | 22.5 s, peak 4,489 MB | 6.4 s, peak **276 MB** |
| Parquet, 264 MB (3.16 M records) | 6.7 s, peak 2,326 MB | 6.8 s, peak **798 MB** |
| JSON Lines, 2 GB (6.7 M records) | (about 18 GB of memory) | 30.6 s, peak **436 MB**, 2 objects |

Findings:
- **Memory stays bounded.** It does not grow with the input: 4x the data costs 160 MB more, which is the second object's parts.
- **Streaming is as fast as one call** for CSV and Parquet, because validating a chunk overlaps with uploading the previous one.
- **Streaming JSON Lines is 3.5x faster** than one call. Lines that fit the contract are parsed straight into typed columns, and only lines with `null` take the row path.
- **Tuning:**
  - 8 MB blocks were faster and lighter than 32 MB (CSV peak 765 vs 1,226 MB).
  - 32 MB row groups save about 100 MB more but cost about 20% throughput.
  - On real S3, raise `max_inflight_parts` when the network allows; uploads then overlap more.

## Reading (`LakeReader`)

**Data**
- 3.16 M rows spread over 8 tenants. Every query reads one tenant: about 395 k rows.
- **Bronze**: Parquet in the SDK layout, 2,313 objects, 217 MiB.
- **Iceberg**: one table shared by all tenants, partitioned by `tenant_id` and month; 88 objects, 230 MiB.

**Method**
- Each engine and query runs in a fresh process.
- *First* is the first call, including imports, reader setup, listing and planning.
- *Warm* is the median of the next 5 calls.

### Local disk (16 cores)

| Query | Polars, Bronze | DuckDB, Bronze | Polars, Iceberg | DuckDB, Iceberg |
|---|---|---|---|---|
| Aggregate one month | 14 ms | 47 ms | 8 ms | 15 ms |
| 3 ids | 19 ms | 63 ms | 14 ms | 29 ms |
| Keyset page of 1,000 | 23 ms | 48 ms | 20 ms | 24 ms |
| Export 395 k rows to Arrow | 69 ms | 227 ms | 25 ms | 69 ms |

- Peak memory: 86–174 MiB with DuckDB, 113–398 MiB with Polars.
- First query: about 300 ms with DuckDB, 600–950 ms with Polars.
- Listing only the tenant's prefix made reads 6.5x faster than listing the whole lake and filtering.

### AWS Lambda in us-east-1 against S3 (4 GB memory, about 2 vCPU)

Same data and queries, read in-region with the 0.2.0rc4 wheel plus this release's reader. Warm median of 5 calls.

| Query | Polars, Bronze | Polars, Bronze, `cache_mb=512` | DuckDB, Bronze | DuckDB, Bronze, `cache_mb=512` | Polars, Iceberg | DuckDB, Iceberg | Athena, Bronze | Athena, Iceberg |
|---|---|---|---|---|---|---|---|---|
| Aggregate one month | 914 ms | **5 ms** | 3,319 ms | 43 ms | 67 ms | 71 ms | 1,216 ms | 917 ms |
| 3 ids | 1,120 ms | **5 ms** | 3,340 ms | 39 ms | 132 ms | 119 ms | 1,102 ms | 992 ms |
| Keyset page of 1,000 | 924 ms | **13 ms** | 3,263 ms | 55 ms | 111 ms | 103 ms | 1,282 ms | 1,174 ms |
| Export 395 k rows | 1,125 ms | **27 ms** | 8,097 ms | 98 ms | 128 ms | 147 ms | 2,084 ms* | 2,088 ms* |

\* Athena: query execution only (`TotalExecutionTimeInMillis`, 55–93 ms of it queueing). Fetching the 395 k-row result takes longer still.

**First read** in a new process, including imports and listing:
- Polars: 1.5–2.0 s.
- DuckDB: 1.2–2.1 s.
- DuckDB, Bronze without the cache: up to 23 s for the full export.

**Peak memory**: 180–380 MiB. The cache holds the tenant's Bronze table (about 100 MiB here).

What this shows:
- **Bronze on S3 is many small objects.** Here that is about 212 per tenant, one per day; the ingestion client writes one per call. Without the cache, every read pays one S3 round trip per object.
- **`cache_mb` removes those round trips.** Objects the SDK writes never change, so warm reads fetch only new objects. The result is 5–27 ms with Polars: 40–200x faster than reading S3 again, and 77–240x faster than Athena.
- **Iceberg needs no cache to be fast.** It has few, larger files and prunes them with manifests, giving 67–147 ms with either engine.
- **Athena** spends 0.9–2.1 s per query, and that is without fetching results.

**Recommendations**
- **Services that read Bronze repeatedly** (APIs, dashboards): use `LakeReader(cache_mb=...)` sized to the tenants' Bronze tables, with the default Polars engine.
- **Silver and Gold**: compact into Iceberg or into larger Parquet files.
- **Batch jobs that read once**: they don't need the cache.
- **Pruning Bronze**: filter on the `year`, `month` and `day` partition columns. This skips whole objects even without the cache.

### Choosing an engine

- **Polars** (default) gives the fastest repeated reads. Use it for long-running services and batch jobs.
- **DuckDB** uses about half the memory and starts fastest. Use it for short-lived functions, memory-bound containers, or when you want SQL.
- With `cache_mb`, Polars is the fastest on Bronze (5–27 ms on S3). On Iceberg both engines take 67–147 ms. Athena takes 0.9–2.1 s on the same data.
