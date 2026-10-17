//! Parquet encoding throughput.

use std::sync::Arc;

use _core::storage::ParquetEncoder;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;

fn batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let n_i64 = i64::try_from(n).expect("size");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..n_i64)),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("label-{}", i % 100)),
            )),
            Arc::new(Float64Array::from_iter_values(
                (0..n_i64).map(|i| i as f64 * 0.5),
            )),
        ],
    )
    .expect("batch")
}

fn bench_encode(c: &mut Criterion) {
    let encoder = ParquetEncoder::default();
    let mut group = c.benchmark_group("parquet_encode");
    for n in [10_000usize, 100_000, 1_000_000] {
        let b = batch(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &b, |bench, b| {
            bench.iter(|| {
                encoder
                    .encode(black_box(std::slice::from_ref(b)))
                    .expect("encode")
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
