//! End-to-end `prepare` throughput and peak heap on a large batch.
//!
//! Builds about `DL_BENCH_MB` megabytes (default 200) of JSON-equivalent
//! records, then runs `prepare` (all validation levels, PII protection and
//! Parquet encoding, no network) for each input type and protection scheme:
//!
//! ```text
//! CARGO_PROFILE_RELEASE_LTO=false cargo run --release --example throughput
//! DL_BENCH_MB=50 cargo run --release --example throughput -- arrow
//! ```
//!
//! An optional argument filters scenarios by name (`json`, `arrow`, `aes`, ...).
//! Peak heap is measured with a counting global allocator and excludes the
//! input, which is built before each measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use _core::contracts::ContractRegistry;
use _core::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
use _core::storage::InMemoryStorage;
use _core::tenant::{TenantId, TenantProfile};
use _core::validation::pii::{Aes256GcmEncryptor, FieldProtector, Sha256Hasher};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::json;

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    let now = CURRENT.fetch_add(by, Ordering::Relaxed) + by;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every call is forwarded unchanged to the system allocator; the
// counters are only bookkeeping.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as `GlobalAlloc::alloc`.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as `GlobalAlloc::dealloc`.
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: same contract as `GlobalAlloc::realloc`.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            if new_size >= layout.size() {
                grew(new_size - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const CONTRACT: &str = "bench/event/1.0.0";
const SEVERITIES: [&str; 3] = ["LOW", "MEDIUM", "HIGH"];

fn registry() -> Arc<ContractRegistry> {
    let mut registry = ContractRegistry::default();
    registry
        .register(&json!({
            "type": "object",
            "x-dl-domain": "bench", "x-dl-datatype": "event", "x-dl-schema_version": "1.0.0",
            "required": ["event_id", "severity"],
            "properties": {
                "event_id": {"type": "string", "pattern": "^evt-[0-9]+$"},
                "severity": {"type": "string", "enum": SEVERITIES},
                "score": {"type": "integer", "minimum": 0},
                "occurred_at": {"type": "string", "format": "date-time"},
                "user_email": {"type": "string", "x-dl-pii": true},
                "device": {"type": "string", "maxLength": 64},
                "note": {"type": "string", "maxLength": 256}
            }
        }))
        .expect("contract");
    Arc::new(registry)
}

type Row = (String, &'static str, i64, String, String, String, String);

/// Column values of record `i`.
fn row(i: usize) -> Row {
    (
        format!("evt-{i}"),
        SEVERITIES[i % 3],
        i64::try_from(i % 10_000).expect("small"),
        format!("2026-09-{:02}T{:02}:{:02}:00Z", 1 + i % 28, i % 24, i % 60),
        format!("user{}@example.com", i % 50_000),
        format!(
            "device-{:08x}-firmware-2.{}",
            i.wrapping_mul(2_654_435_761),
            i % 9
        ),
        format!(
            "login from branch office {} after password rotation, session {i}",
            i % 97
        ),
    )
}

fn json_text(n: usize) -> String {
    let mut out = String::with_capacity(n * 230);
    out.push('[');
    for i in 0..n {
        if i > 0 {
            out.push(',');
        }
        let (id, sev, score, at, email, device, note) = row(i);
        let record = json!({
            "event_id": id, "severity": sev, "score": score, "occurred_at": at,
            "user_email": email, "device": device, "note": note
        });
        out.push_str(&record.to_string());
    }
    out.push(']');
    out
}

fn arrow_batch(n: usize) -> RecordBatch {
    let rows: Vec<_> = (0..n).map(row).collect();
    let strings = |f: fn(&Row) -> &str| Arc::new(StringArray::from_iter_values(rows.iter().map(f)));
    let schema = Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("occurred_at", DataType::Utf8, false),
        Field::new("user_email", DataType::Utf8, false),
        Field::new("device", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            strings(|r| &r.0),
            strings(|r| r.1),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            strings(|r| &r.3),
            strings(|r| &r.4),
            strings(|r| &r.5),
            strings(|r| &r.6),
        ],
    )
    .expect("batch")
}

fn mib(bytes: usize) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let b = bytes as f64;
    b / (1024.0 * 1024.0)
}

fn main() {
    let target_mb: usize = std::env::var("DL_BENCH_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let filter = std::env::args().nth(1).unwrap_or_default();

    // Size the batch so that its JSON text is about `target_mb` MiB.
    let probe = json_text(1000);
    let records = target_mb * 1024 * 1024 * 1000 / probe.len();
    drop(probe);
    let json_len = json_text(records).len();

    let pipeline = BronzePipeline::new(
        registry(),
        Arc::new(InMemoryStorage::new()),
        PipelineConfig::default(),
    )
    .expect("pipeline");
    let schemes: [(&str, Option<Arc<dyn FieldProtector>>); 3] = [
        ("none", None),
        ("sha256", Some(Arc::new(Sha256Hasher))),
        (
            "aes-256-gcm",
            Some(Arc::new(
                Aes256GcmEncryptor::new(&[7u8; 32], "bench-key-ref").expect("key"),
            )),
        ),
    ];

    println!(
        "records: {records}, threads: {}",
        std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
    );
    println!(
        "{:<26} {:>9} {:>9} {:>10} {:>12} {:>11} {:>11}",
        "scenario", "input MiB", "time s", "MiB/s", "records/s", "peak MiB", "output MiB"
    );
    for input in ["json", "arrow"] {
        for (scheme, protector) in &schemes {
            let name = format!("{input} / {scheme}");
            if !name.contains(filter.as_str()) {
                continue;
            }
            let tenant = Arc::new(
                TenantProfile::for_bucket(TenantId::new("bench-001").expect("id"), "bench-lake")
                    .expect("bucket")
                    .pii_protection(protector.clone())
                    .build()
                    .expect("profile"),
            );
            let source = if input == "json" {
                RecordSource::Json(json_text(records))
            } else {
                RecordSource::Arrow(vec![arrow_batch(records)])
            };
            let baseline = CURRENT.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            let started = Instant::now();
            let prepared = pipeline
                .prepare(IngestRequest::new(tenant, CONTRACT, source))
                .expect("prepare");
            let secs = started.elapsed().as_secs_f64();
            let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
            let report = prepared.report().clone();
            assert_eq!(
                report.records_ok, records,
                "{name}: every record must be valid"
            );
            let output = prepared.encoded_bytes();
            drop(prepared);
            #[allow(clippy::cast_precision_loss)]
            let per_sec = records as f64 / secs;
            println!(
                "{name:<26} {:>9.1} {secs:>9.2} {:>10.1} {per_sec:>12.0} {:>11.1} {:>11.1}",
                mib(json_len),
                mib(json_len) / secs,
                mib(peak),
                mib(output),
            );
        }
    }
}
