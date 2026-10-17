//! PII protection throughput per scheme: SHA-256, HMAC-SHA256 and AES-256-GCM.

use std::sync::Arc;

use _core::validation::pii::{
    protect_records, Aes256GcmEncryptor, FieldProtector, HmacSha256Hasher, Sha256Hasher,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{json, Map, Value};
use std::hint::black_box;

fn records(n: usize) -> Vec<Map<String, Value>> {
    (0..n)
        .map(|i| match json!({"email": format!("user{i}@example.com"), "ip": format!("10.0.{}.{}", i % 255, i % 7), "event": "login"}) {
            Value::Object(m) => m,
            _ => Map::new(),
        })
        .collect()
}

fn bench_protect(c: &mut Criterion) {
    let fields = vec!["email".to_owned(), "ip".to_owned()];
    let schemes: [(&str, Arc<dyn FieldProtector>); 3] = [
        ("sha256", Arc::new(Sha256Hasher)),
        (
            "hmac_sha256",
            Arc::new(HmacSha256Hasher::new(&[7u8; 32]).expect("key")),
        ),
        (
            "aes_256_gcm",
            Arc::new(Aes256GcmEncryptor::new(&[7u8; 32], "bench-key-ref").expect("key")),
        ),
    ];
    let mut group = c.benchmark_group("pii_protect");
    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        for (name, protector) in &schemes {
            group.bench_with_input(BenchmarkId::new(*name, n), &n, |b, &n| {
                b.iter_batched(
                    || records(n),
                    |mut rs| {
                        let mut refs: Vec<&mut Map<String, Value>> = rs.iter_mut().collect();
                        protect_records(
                            protector.as_ref(),
                            "bench-tenant",
                            black_box(&mut refs),
                            &fields,
                        )
                        .expect("protect");
                        rs
                    },
                    criterion::BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_protect);
criterion_main!(benches);
