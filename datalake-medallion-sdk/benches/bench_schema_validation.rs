//! Level 3 contract check throughput (valid and invalid records).

use _core::contracts::ContractRegistry;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{json, Value};

fn bench_check(c: &mut Criterion) {
    let mut registry = ContractRegistry::default();
    let key = registry
        .register(&json!({
            "type": "object",
            "x-dl-domain": "bench",
            "x-dl-datatype": "event",
            "x-dl-schema_version": "1.0.0",
            "required": ["event_id", "severity"],
            "properties": {
                "event_id": {"type": "string", "minLength": 1},
                "severity": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
                "occurred_at": {"type": "string", "format": "date-time"},
                "score": {"type": "number", "minimum": 0, "maximum": 10}
            }
        }))
        .expect("contract");
    let contract = registry.get(&key).expect("registered").clone();

    let valid: Vec<Value> = (0..10_000)
        .map(|i| json!({"event_id": format!("e{i}"), "severity": "HIGH", "occurred_at": "2026-10-01T12:00:00Z", "score": 5.5}))
        .collect();
    let invalid: Vec<Value> = (0..10_000)
        .map(|i| json!({"event_id": format!("e{i}"), "severity": "CRITICAL", "score": 11}))
        .collect();

    let mut group = c.benchmark_group("contract_check");
    group.throughput(Throughput::Elements(10_000));
    for (name, data) in [("valid", &valid), ("invalid", &invalid)] {
        group.bench_with_input(BenchmarkId::from_parameter(name), data, |b, data| {
            b.iter(|| {
                data.iter()
                    .filter(|r| contract.check(black_box(r)).is_none())
                    .count()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_check);
criterion_main!(benches);
