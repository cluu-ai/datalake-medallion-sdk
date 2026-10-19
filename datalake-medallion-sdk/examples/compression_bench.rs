//! Bronze Parquet codecs on log-like data: size, encode and decode time.
//!
//! ```text
//! cargo run --release --example compression_bench -- [rows]
//! ```
//!
//! The data mimics application logs: typed timestamps, a few low-cardinality
//! columns (level, service, route), random trace and span ids, templated
//! messages with numbers, and the constant lineage columns Bronze adds.

use std::sync::Arc;
use std::time::{Duration, Instant};

use _core::contracts::BronzeCompression;
use _core::storage::ParquetEncoder;
use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const ROUNDS: usize = 5;

/// xorshift64*: deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn logs(rows: usize) -> RecordBatch {
    const LEVELS: [&str; 4] = ["INFO", "INFO", "WARN", "ERROR"];
    const SERVICES: [&str; 6] = ["api", "auth", "billing", "search", "worker", "gateway"];
    const ROUTES: [&str; 8] = [
        "/v1/users",
        "/v1/orders",
        "/v1/orders/{id}",
        "/v1/search",
        "/v1/login",
        "/v1/invoices",
        "/health",
        "/v1/assets/{id}",
    ];
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let start: i64 = 1_772_323_200_000_000; // 2026-03-01T00:00:00Z
    let mut ts = Vec::with_capacity(rows);
    let (mut level, mut service, mut route) = (Vec::new(), Vec::new(), Vec::new());
    let (mut trace, mut span, mut msg) = (Vec::new(), Vec::new(), Vec::new());
    let (mut status, mut duration) = (Vec::new(), Vec::new());
    let mut t = start;
    for i in 0..rows {
        let r = rng.next();
        t += i64::try_from(r % 2_000).unwrap_or(0);
        ts.push(t);
        let lvl = LEVELS[usize::try_from(r >> 8).unwrap_or(0) % LEVELS.len()];
        let svc = SERVICES[usize::try_from(r >> 16).unwrap_or(0) % SERVICES.len()];
        let rt = ROUTES[usize::try_from(r >> 24).unwrap_or(0) % ROUTES.len()];
        level.push(lvl);
        service.push(svc);
        route.push(rt);
        trace.push(format!("{:016x}{:016x}", rng.next(), rng.next()));
        span.push(format!("{:016x}", rng.next()));
        let code = if lvl == "ERROR" { 500 } else { 200 };
        status.push(code);
        let ms = f64::from(u32::try_from(r % 50_000).unwrap_or(0)) / 100.0;
        duration.push(ms);
        msg.push(format!(
            "{svc} handled GET {rt} for user u{} in {ms:.2} ms (status {code}, req {i})",
            rng.next() % 100_000
        ));
    }
    let tz: Arc<str> = Arc::from("UTC");
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone())),
            false,
        ),
        Field::new("level", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("route", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("status", DataType::Int32, false),
        Field::new("duration_ms", DataType::Float64, false),
        Field::new("msg", DataType::Utf8, false),
        Field::new("_dl_batch_id", DataType::Utf8, false),
        Field::new("_dl_schema_version", DataType::Utf8, false),
    ]));
    let constant = |v: &str| -> ArrayRef { Arc::new(StringArray::from(vec![v; rows])) };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(ts).with_timezone(tz)),
        Arc::new(StringArray::from(level)),
        Arc::new(StringArray::from(service)),
        Arc::new(StringArray::from(route)),
        Arc::new(StringArray::from(trace)),
        Arc::new(StringArray::from(span)),
        Arc::new(Int32Array::from(status)),
        Arc::new(Float64Array::from(duration)),
        Arc::new(StringArray::from(msg)),
        constant("01JQ0000000000000000000000"),
        constant("1.0.0"),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort_unstable();
    v[v.len() / 2]
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1_000_000);
    let batch = logs(rows);
    let arrow_mb = batch.get_array_memory_size() as f64 / 1_048_576.0;
    println!("{rows} log rows, {arrow_mb:.1} MiB in Arrow, median of {ROUNDS} rounds");
    println!(
        "{:<10} {:>9} {:>7} {:>11} {:>11}",
        "codec", "size MiB", "ratio", "encode ms", "decode ms"
    );
    let codecs = [
        ("none", BronzeCompression::None),
        ("snappy", BronzeCompression::Snappy),
        ("lz4", BronzeCompression::Lz4),
        ("zstd(1)", BronzeCompression::Zstd(1)),
        ("zstd(3)", BronzeCompression::Zstd(3)),
        ("zstd(6)", BronzeCompression::Zstd(6)),
    ];
    for (name, codec) in codecs {
        let encoder = ParquetEncoder::for_contract(codec);
        let mut encode = Vec::new();
        let mut decode = Vec::new();
        let mut size = 0;
        for _ in 0..ROUNDS {
            let t = Instant::now();
            let body = encoder.encode(std::slice::from_ref(&batch)).unwrap();
            encode.push(t.elapsed());
            size = body.len();
            let t = Instant::now();
            let read: usize = ParquetRecordBatchReaderBuilder::try_new(body)
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            decode.push(t.elapsed());
            assert_eq!(read, rows);
        }
        let mb = size as f64 / 1_048_576.0;
        println!(
            "{name:<10} {mb:>9.2} {:>7.2} {:>11.0} {:>11.0}",
            arrow_mb / mb,
            median(encode).as_secs_f64() * 1e3,
            median(decode).as_secs_f64() * 1e3,
        );
    }
}
