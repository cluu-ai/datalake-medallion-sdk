//! PII masking throughput: SHA-256 vs HMAC-SHA256.

use _core::validation::pii::PiiHasher;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{json, Map, Value};

fn records(n: usize) -> Vec<Map<String, Value>> {
    (0..n)
        .map(|i| match json!({"email": format!("user{i}@example.com"), "ip": format!("10.0.{}.{}", i % 255, i % 7), "event": "login"}) {
            Value::Object(m) => m,
            _ => Map::new(),
        })
        .collect()
}

fn bench_mask(c: &mut Criterion) {
    let fields = vec!["email".to_owned(), "ip".to_owned()];
    let hashers = [
        ("sha256", PiiHasher::Sha256),
        (
            "hmac_sha256",
            PiiHasher::hmac_sha256(&[7u8; 32]).expect("key"),
        ),
    ];
    let mut group = c.benchmark_group("pii_mask");
    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        for (name, hasher) in &hashers {
            group.bench_with_input(BenchmarkId::new(*name, n), &n, |b, &n| {
                b.iter_batched(
                    || records(n),
                    |mut rs| {
                        for r in &mut rs {
                            hasher.mask(black_box(r), &fields);
                        }
                        rs
                    },
                    criterion::BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_mask);
criterion_main!(benches);
