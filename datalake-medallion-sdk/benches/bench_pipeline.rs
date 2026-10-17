//! End-to-end `prepare` throughput (Levels 2–8 + Parquet encoding, no I/O).

use std::sync::Arc;

use _core::contracts::ContractRegistry;
use _core::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
use _core::storage::InMemoryStorage;
use _core::tenant::{TenantId, TenantProfile};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{json, Value};

const CONTRACT: &str = "bench/event/1.0.0";

fn pipeline(arrow_fast_path: bool) -> BronzePipeline {
    let mut registry = ContractRegistry::default();
    registry
        .register(&json!({
            "type": "object",
            "x-dl-domain": "bench",
            "x-dl-datatype": "event",
            "x-dl-schema_version": "1.0.0",
            "required": ["event_id", "severity"],
            "properties": {
                "event_id": {"type": "string"},
                "severity": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
                "score": {"type": "integer", "minimum": 0},
                "user_email": {"type": "string", "x-dl-pii": true}
            }
        }))
        .expect("contract");
    BronzePipeline::new(
        Arc::new(registry),
        Arc::new(InMemoryStorage::new()),
        PipelineConfig {
            arrow_fast_path,
            ..PipelineConfig::default()
        },
    )
    .expect("pipeline")
}

fn records(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            let severity = ["LOW", "MEDIUM", "HIGH"][i % 3];
            json!({
                "event_id": format!("evt-{i}"),
                "severity": severity,
                "score": i,
                "user_email": format!("user{i}@example.com")
            })
        })
        .collect()
}

fn arrow_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("user_email", DataType::Utf8, false),
    ]));
    let n_i64 = i64::try_from(n).expect("size");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("evt-{i}")),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| ["LOW", "MEDIUM", "HIGH"][i % 3]),
            )),
            Arc::new(Int64Array::from_iter_values(0..n_i64)),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("user{i}@example.com")),
            )),
        ],
    )
    .expect("batch")
}

fn bench_prepare(c: &mut Criterion) {
    let p = pipeline(true);
    let row_path = pipeline(false);
    let tenant = Arc::new(
        TenantProfile::for_bucket(
            TenantId::new("bench-tenant").expect("tenant"),
            "bench-bucket",
        )
        .expect("bucket")
        .build()
        .expect("profile"),
    );
    let mut group = c.benchmark_group("prepare");
    for n in [1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        let json_text = serde_json::to_string(&records(n)).expect("json");
        group.bench_with_input(BenchmarkId::new("json_text", n), &json_text, |b, text| {
            b.iter(|| {
                p.prepare(IngestRequest::new(
                    Arc::clone(&tenant),
                    CONTRACT,
                    RecordSource::Json(text.clone()),
                ))
                .expect("prepare")
            });
        });
        let batch = arrow_batch(n);
        group.bench_with_input(BenchmarkId::new("arrow", n), &batch, |b, batch| {
            b.iter(|| {
                p.prepare(IngestRequest::new(
                    Arc::clone(&tenant),
                    CONTRACT,
                    RecordSource::Arrow(vec![batch.clone()]),
                ))
                .expect("prepare")
            });
        });
        group.bench_with_input(BenchmarkId::new("arrow_row_path", n), &batch, |b, batch| {
            b.iter(|| {
                row_path
                    .prepare(IngestRequest::new(
                        Arc::clone(&tenant),
                        CONTRACT,
                        RecordSource::Arrow(vec![batch.clone()]),
                    ))
                    .expect("prepare")
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_prepare);
criterion_main!(benches);
