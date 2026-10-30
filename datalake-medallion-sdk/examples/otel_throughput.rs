//! OTLP logs: flattening and ingestion throughput, without network I/O.
//!
//! ```text
//! cargo run --release --example otel_throughput -- [records] [tenants] [requests] [json|protobuf] [none|gzip|zstd]
//! ```
//!
//! Builds OTLP export requests (several, as in one queue batch, when
//! `requests` > 1) with log records spread over a few tenants, in the given
//! encoding and compression (the same logical requests in every case), then
//! times `otel::flatten_logs_many` and the ingestion of every tenant's rows
//! into a store that discards the bytes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use _core::contracts::ContractRegistry;
use _core::error::DatalakeError;
use _core::otel::{self, OtlpOptions};
use _core::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
use _core::storage::ObjectStorage;
use _core::tenant::{ObjectPath, TenantId, TenantProfile};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{json, Value};

#[path = "../src/otel/transcode.rs"]
mod transcode;

struct Discard {
    bytes: AtomicU64,
}

#[async_trait]
impl ObjectStorage for Discard {
    async fn put(&self, _: &ObjectPath, body: Bytes, _: &str) -> Result<(), DatalakeError> {
        self.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(())
    }
    async fn create_multipart(&self, _: &ObjectPath, _: &str) -> Result<String, DatalakeError> {
        Ok("upload".into())
    }
    async fn upload_part(
        &self,
        _: &ObjectPath,
        _: &str,
        n: i32,
        body: Bytes,
    ) -> Result<String, DatalakeError> {
        self.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(n.to_string())
    }
    async fn complete_multipart(
        &self,
        _: &ObjectPath,
        _: &str,
        _: Vec<(i32, String)>,
    ) -> Result<(), DatalakeError> {
        Ok(())
    }
}

fn request(records: usize, tenants: usize) -> Value {
    let base: u64 = 1_772_359_200_000_000_000;
    let resources: Vec<Value> = (0..tenants)
        .map(|t| {
            let logs: Vec<Value> = (0..records / tenants)
                .map(|i| {
                    let n = (t * records + i) as u64;
                    json!({
                        "timeUnixNano": (base + n * 1_000_000).to_string(),
                        "observedTimeUnixNano": (base + n * 1_000_000 + 500).to_string(),
                        "severityNumber": if i % 50 == 0 { 17 } else { 9 },
                        "severityText": if i % 50 == 0 { "ERROR" } else { "INFO" },
                        "body": {"stringValue": format!("GET /v1/orders/{} served in {} ms", n % 10_000, n % 900)},
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": "GET"}},
                            {"key": "http.status_code", "value": {"intValue": "200"}},
                            {"key": "user.id", "value": {"stringValue": format!("u{}", n % 50_000)}}
                        ],
                        "traceId": format!("{:032x}", n.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
                        "spanId": format!("{:016x}", n.wrapping_mul(0x2545_F491_4F6C_DD1D)),
                        "flags": 1
                    })
                })
                .collect();
            json!({
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": format!("svc-{}", t % 4)}},
                    {"key": "tenant.id", "value": {"stringValue": format!("tenant-{t:03}")}}
                ]},
                "scopeLogs": [{"scope": {"name": "app", "version": "1.0"}, "logRecords": logs}]
            })
        })
        .collect();
    json!({ "resourceLogs": resources })
}

fn encode(request: &Value, encoding: &str, compression: &str) -> Vec<u8> {
    use std::io::Write as _;

    let raw = match encoding {
        "protobuf" => transcode::request(request),
        _ => serde_json::to_vec(request).unwrap(),
    };
    match compression {
        "gzip" => {
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gz.write_all(&raw).unwrap();
            gz.finish().unwrap()
        }
        "zstd" => zstd::encode_all(raw.as_slice(), 3).unwrap(),
        _ => raw,
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let records: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let tenants: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(4);
    let requests: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(1);
    let encoding = args.next().unwrap_or_else(|| "json".into());
    let compression = args.next().unwrap_or_else(|| "none".into());
    let bodies: Vec<Vec<u8>> = (0..requests)
        .map(|_| {
            encode(
                &request(records / requests, tenants),
                &encoding,
                &compression,
            )
        })
        .collect();
    let format = match (encoding.as_str(), compression.as_str()) {
        ("protobuf", "none") => "OTLP/protobuf".to_owned(),
        ("protobuf", c) => format!("OTLP/protobuf+{c}"),
        (_, "none") => "OTLP/JSON".to_owned(),
        (_, c) => format!("OTLP/JSON+{c}"),
    };
    let slices: Vec<&[u8]> = bodies.iter().map(Vec::as_slice).collect();
    let mib = bodies.iter().map(Vec::len).sum::<usize>() as f64 / 1_048_576.0;

    let mut registry = ContractRegistry::default();
    registry
        .register(&otel::logs_contract("otel", "logs", "1.0.0", "zstd", false))
        .unwrap();
    let store = Arc::new(Discard {
        bytes: AtomicU64::new(0),
    });
    let config = PipelineConfig {
        max_future_skew: None,
        ..PipelineConfig::default()
    };
    let pipeline = Arc::new(
        BronzePipeline::new(
            Arc::new(registry),
            Arc::clone(&store) as Arc<dyn ObjectStorage>,
            config,
        )
        .unwrap(),
    );
    for round in 0..3 {
        let t0 = Instant::now();
        let groups = otel::flatten_logs_many(&slices, &OtlpOptions::default())
            .unwrap()
            .tenants;
        let flatten = t0.elapsed();
        let mut ok = 0;
        for group in groups {
            let tenant = Arc::new(
                TenantProfile::for_bucket(TenantId::new(&group.tenant.unwrap()).unwrap(), "lake")
                    .unwrap()
                    .build()
                    .unwrap(),
            );
            let p = Arc::clone(&pipeline);
            let report = tokio::task::spawn_blocking(move || {
                p.prepare(IngestRequest::new(
                    tenant,
                    "otel/logs/1.0.0",
                    RecordSource::Arrow(vec![group.batch]),
                ))
            })
            .await
            .unwrap()
            .unwrap();
            ok += pipeline.commit(&report).await.unwrap().records_ok;
        }
        let total = t0.elapsed();
        println!(
            "round {round}: {records} records in {requests} requests, {mib:.1} MiB {format}, {tenants} tenants: flatten {:.0} ms ({:.2} M records/s), total {:.0} ms ({:.0} MiB/s, {:.2} M records/s), ok {ok}",
            flatten.as_secs_f64() * 1e3,
            records as f64 / flatten.as_secs_f64() / 1e6,
            total.as_secs_f64() * 1e3,
            mib / total.as_secs_f64(),
            records as f64 / total.as_secs_f64() / 1e6,
        );
    }
}
