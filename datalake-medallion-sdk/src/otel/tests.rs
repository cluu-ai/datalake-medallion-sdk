use super::*;
use crate::contracts::ContractRegistry;
use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use proptest::prelude::*;

fn request() -> Value {
    json!({"resourceLogs": [
        {"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "checkout"}},
            {"key": "tenant.id", "value": {"stringValue": "acme-001"}}]},
         "scopeLogs": [{"scope": {"name": "app", "version": "1.2"}, "logRecords": [
            {"timeUnixNano": "1772359200123456789", "observedTimeUnixNano": 1_772_359_200_200_000_000_u64,
             "severityNumber": 9, "severityText": "INFO",
             "body": {"stringValue": "order placed"},
             "attributes": [{"key": "order.id", "value": {"intValue": "42"}},
                            {"key": "ok", "value": {"boolValue": true}}],
             "traceId": "5B8EFFF798038103D269B633813FC60C", "spanId": "EEE19B7EC3C1B174", "flags": 1},
            {"observedTimeUnixNano": "1772359201000000000", "severityNumber": 17,
             "body": {"kvlistValue": {"values": [{"key": "error", "value": {"stringValue": "boom"}}]}}}]}]},
        {"resource": {"attributes": [{"key": "tenant.id", "value": {"stringValue": "beta-002"}}]},
         "scope_logs": [{"log_records": [{"time_unix_nano": "1772359202000000000"}]}]},
        {"resource": {}, "scopeLogs": [{"logRecords": [
            {"timeUnixNano": "1772359203000000000",
             "attributes": [{"key": "tenant.id", "value": {"stringValue": "acme-001"}}]},
            {"timeUnixNano": "1772359204000000000"}]}]}
    ]})
}

/// Every value type, nesting, ids and edge cases, for the differential tests.
fn rich_request() -> Value {
    let nested = (0..5).fold(json!({"stringValue": "leaf"}), |inner, i| {
        if i % 2 == 0 {
            json!({"arrayValue": {"values": [inner, {"intValue": i}]}})
        } else {
            json!({"kvlistValue": {"values": [{"key": format!("k{i}"), "value": inner}, {"key": "none"}]}})
        }
    });
    json!({"resourceLogs": [
        {"resource": {"attributes": [
            {"key": "tenant.id", "value": {"intValue": "7"}},
            {"key": "tenant.id", "value": {"stringValue": "ignored: the first key decides"}},
            {"key": "service.name", "value": {"stringValue": "svc \"quoted\" \u{e9}\u{1F600}"}},
            {"key": "host.cpu", "value": {"doubleValue": 2.5}}]},
         "scopeLogs": [
            {"scope": {"name": "", "version": "9"}, "logRecords": [
                {"timeUnixNano": "1772359200000000001", "severityNumber": 0, "flags": 0,
                 "traceId": "00000000000000000000000000000000", "spanId": "",
                 "body": {"stringValue": ""},
                 "attributes": [
                    {"key": "tenant.id", "value": {"stringValue": "rec-tenant"}},
                    {"key": "s", "value": {"stringValue": "line\nbreak\t\"q\" \\ \u{1}"}},
                    {"key": "b", "value": {"boolValue": false}},
                    {"key": "i", "value": {"intValue": "-9223372036854775808"}},
                    {"key": "d", "value": {"doubleValue": -0.000_123_4}},
                    {"key": "nan", "value": {"doubleValue": "NaN"}},
                    {"key": "inf", "value": {"doubleValue": "-Infinity"}},
                    {"key": "bytes", "value": {"bytesValue": "AAEC/w=="}},
                    {"key": "empty", "value": {}},
                    {"key": "missing"},
                    {"key": "nested", "value": nested},
                    {"key": "list", "value": {"arrayValue": {}}},
                    {"key": "map", "value": {"kvlistValue": {}}}]},
                {"observedTimeUnixNano": "1772359200000000002", "severityText": "",
                 "eventName": "browser.click", "body": {"doubleValue": "Infinity"},
                 "traceId": "0102030405060708090a0b0c0d0e0f10", "spanId": "a1A2b3B4c5C6d7D8"},
                {"body": {"arrayValue": {"values": [{"bytesValue": ""}, {"boolValue": true}]}},
                 "severityNumber": 24, "flags": 257}]},
            {"logRecords": [{"timeUnixNano": 1_772_359_200_000_000_003_u64, "body": {}}]}]},
        {"scopeLogs": [{"logRecords": [{"timeUnixNano": "1772359200000000004"}]}]}
    ]})
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
    batch.column_by_name(name).unwrap()
}

fn protobuf(json: &Value) -> Vec<u8> {
    transcode::request(json)
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn zstd(data: &[u8]) -> Vec<u8> {
    zstd::encode_all(data, 3).unwrap()
}

fn flatten(body: &[u8]) -> Result<Vec<TenantLogs>, DatalakeError> {
    flatten_logs(body, &OtlpOptions::default())
}

fn same(a: &[TenantLogs], b: &[TenantLogs]) {
    assert_eq!(a.len(), b.len());
    for (a, b) in a.iter().zip(b) {
        assert_eq!(a.tenant, b.tenant);
        for (name, _) in COLUMNS {
            assert_eq!(
                column(&a.batch, name).as_ref(),
                column(&b.batch, name).as_ref(),
                "column {name} of tenant {:?}",
                a.tenant
            );
        }
    }
}

#[test]
fn records_are_flattened_and_grouped_by_tenant() {
    let groups = flatten_logs_json(request().to_string().as_bytes(), "tenant.id").unwrap();
    let tenants: Vec<_> = groups.iter().map(|g| g.tenant.as_deref()).collect();
    assert_eq!(tenants, [None, Some("acme-001"), Some("beta-002")]);
    let acme = &groups[1].batch;
    assert_eq!(
        acme.num_rows(),
        3,
        "two records plus one tagged on the record"
    );
    let ts = column(acme, "timestamp").as_string::<i32>();
    assert_eq!(ts.value(0), "2026-03-01T10:00:00.123456Z");
    assert_eq!(
        ts.value(1),
        "2026-03-01T10:00:01.000000Z",
        "falls back to observed time"
    );
    let body = column(acme, "body").as_string::<i32>();
    assert_eq!(body.value(0), "order placed");
    assert_eq!(body.value(1), r#"{"error":"boom"}"#);
    assert_eq!(
        column(acme, "attributes").as_string::<i32>().value(0),
        r#"{"order.id":42,"ok":true}"#
    );
    assert_eq!(
        column(acme, "trace_id").as_string::<i32>().value(0),
        "5b8efff798038103d269b633813fc60c"
    );
    assert_eq!(
        column(acme, "service_name").as_string::<i32>().value(0),
        "checkout"
    );
    assert_eq!(
        column(acme, "scope_version").as_string::<i32>().value(0),
        "1.2"
    );
    let severity = column(acme, "severity_number").as_primitive::<Int64Type>();
    assert_eq!((severity.value(0), severity.value(1)), (9, 17));
    assert!(column(acme, "severity_text").is_null(1));
    assert!(column(acme, "resource_attributes").is_null(2));
    assert_eq!(
        groups[2].batch.num_rows(),
        1,
        "snake_case names are read too"
    );
    assert_eq!(groups[0].batch.num_rows(), 1);
}

#[test]
fn protobuf_gives_the_same_rows_as_json() {
    for json in [request(), rich_request()] {
        let text = json.to_string();
        let expected = flatten_logs_json(text.as_bytes(), "tenant.id").unwrap();
        let pb = protobuf(&json);
        same(&flatten(&pb).unwrap(), &expected);
        let strict = OtlpOptions::default()
            .with_encoding(OtlpEncoding::Protobuf)
            .with_compression(OtlpCompression::None);
        same(&flatten_logs(&pb, &strict).unwrap(), &expected);
        same(&flatten(text.as_bytes()).unwrap(), &expected);
    }
}

#[test]
fn compressed_requests_are_detected() {
    let json = rich_request();
    let text = json.to_string();
    let expected = flatten_logs_json(text.as_bytes(), "tenant.id").unwrap();
    let pb = protobuf(&json);
    for body in [
        gzip(text.as_bytes()),
        zstd(text.as_bytes()),
        gzip(&pb),
        zstd(&pb),
    ] {
        same(&flatten(&body).unwrap(), &expected);
    }
    // Concatenated gzip members and zstd frames, as appended files are.
    let half = text.len() / 2;
    let mut members = gzip(&text.as_bytes()[..half]);
    members.extend(gzip(&text.as_bytes()[half..]));
    same(&flatten(&members).unwrap(), &expected);
    let mut frames = zstd(&pb[..pb.len() / 2]);
    frames.extend(zstd(&pb[pb.len() / 2..]));
    same(&flatten(&frames).unwrap(), &expected);
    // Explicit settings.
    let gz = OtlpOptions::default()
        .with_compression(OtlpCompression::Gzip)
        .with_encoding(OtlpEncoding::Json);
    same(
        &flatten_logs(&gzip(text.as_bytes()), &gz).unwrap(),
        &expected,
    );
    let none = OtlpOptions::default().with_compression(OtlpCompression::None);
    assert!(flatten_logs(&zstd(&pb), &none).is_err());
}

#[test]
fn values_protobuf_cannot_express_are_null_in_both_encodings() {
    let groups = flatten_logs_json(rich_request().to_string().as_bytes(), "tenant.id").unwrap();
    let tenants: Vec<_> = groups.iter().map(|g| g.tenant.as_deref()).collect();
    assert_eq!(
        tenants,
        [None, Some("rec-tenant")],
        "a non-string tenant attribute on the resource does not fall back to the record"
    );
    let first = &groups[1].batch;
    assert!(
        column(first, "severity_number").is_null(0),
        "severity 0 is unspecified"
    );
    assert!(column(first, "flags").is_null(0));
    assert!(column(first, "trace_id").is_null(0), "all-zero id");
    assert!(column(first, "span_id").is_null(0), "empty id");
    assert!(column(first, "scope_name").is_null(0));
    assert_eq!(
        column(first, "body").as_string::<i32>().value(0),
        "",
        "an empty string body is kept"
    );
    let attributes: Value =
        serde_json::from_str(column(first, "attributes").as_string::<i32>().value(0)).unwrap();
    assert_eq!(attributes["i"], json!(i64::MIN));
    assert_eq!(attributes["nan"], Value::Null);
    assert_eq!(attributes["bytes"], json!("AAEC/w=="));
    assert_eq!(attributes["missing"], Value::Null);
    assert_eq!(attributes["empty"], Value::Null);
    assert!(
        attributes["nested"][0]["k3"][0]["k1"].is_array(),
        "nested values are plain JSON"
    );
    assert_eq!(attributes["list"], json!([]));
    assert_eq!(attributes["map"], json!({}));

    let rest = &groups[0].batch;
    assert_eq!(rest.num_rows(), 4);
    assert!(column(rest, "severity_text").is_null(0));
    assert_eq!(
        column(rest, "span_id").as_string::<i32>().value(0),
        "a1a2b3b4c5c6d7d8"
    );
    let body = column(rest, "body").as_string::<i32>();
    assert!(body.is_null(0), "non-finite doubles are null");
    assert_eq!(body.value(1), r#"["",true]"#);
    assert!(body.is_null(2));
    let flags = column(rest, "flags").as_primitive::<Int64Type>();
    assert_eq!(flags.value(1), 257);
}

/// A value nested `depth` levels deep, alternating arrays and key-value lists.
fn nested(depth: usize) -> Value {
    (1..depth).fold(json!({"stringValue": "leaf"}), |inner, i| {
        if i % 2 == 0 {
            json!({"arrayValue": {"values": [inner]}})
        } else {
            json!({"kvlistValue": {"values": [{"key": "k", "value": inner}]}})
        }
    })
}

#[test]
fn nesting_is_limited_the_same_way_in_both_encodings() {
    for depth in [MAX_VALUE_DEPTH, MAX_VALUE_DEPTH + 1] {
        for place in ["body", "attributes", "resource"] {
            let record = match place {
                "body" => json!({"timeUnixNano": "1", "body": nested(depth)}),
                "attributes" => {
                    json!({"timeUnixNano": "1", "attributes": [{"key": "a", "value": nested(depth)}]})
                }
                _ => json!({"timeUnixNano": "1"}),
            };
            let resource = if place == "resource" {
                json!({"attributes": [{"key": "a", "value": nested(depth)}]})
            } else {
                json!({})
            };
            let request = json!({"resourceLogs": [
                {"resource": resource, "scopeLogs": [{"logRecords": [record]}]}]});
            let from_json = flatten_logs_json(request.to_string().as_bytes(), "tenant.id");
            let from_protobuf = flatten(&protobuf(&request));
            if depth > MAX_VALUE_DEPTH {
                for result in [from_json, from_protobuf] {
                    let err = result.unwrap_err().to_string();
                    assert!(err.contains("deeper than"), "{place}: {err}");
                }
            } else {
                same(&from_protobuf.unwrap(), &from_json.unwrap());
            }
        }
    }
}

#[test]
fn unknown_protobuf_fields_are_skipped() {
    let mut pb = protobuf(&request());
    // Unknown fields of every wire type, at the top level...
    pb.extend([0xf8, 0x06, 0x01]); // field 111, varint
    pb.extend([0xf9, 0x06, 1, 2, 3, 4, 5, 6, 7, 8]); // field 111, i64
    pb.extend([0xfa, 0x06, 0x02, 0xff, 0xfe]); // field 111, len
    pb.extend([0xfd, 0x06, 1, 2, 3, 4]); // field 111, i32
    let expected = flatten_logs_json(request().to_string().as_bytes(), "tenant.id").unwrap();
    same(&flatten(&pb).unwrap(), &expected);
    // ...and inside a log record (field 13, a string from a newer schema).
    let mut record = Vec::new();
    record.extend([0x09]);
    record.extend(1_772_359_200_000_000_000_u64.to_le_bytes());
    record.extend([0x6a, 0x03, b'n', b'e', b'w']);
    let mut scope = vec![0x12, u8::try_from(record.len()).unwrap()];
    scope.extend(&record);
    let mut resource_logs = vec![0x12, u8::try_from(scope.len()).unwrap()];
    resource_logs.extend(&scope);
    let mut body = vec![0x0a, u8::try_from(resource_logs.len()).unwrap()];
    body.extend(&resource_logs);
    assert_eq!(flatten(&body).unwrap()[0].batch.num_rows(), 1);
}

#[test]
fn malformed_protobuf_is_rejected_without_panicking() {
    let pb = protobuf(&rich_request());
    let protobuf_only = OtlpOptions::default().with_encoding(OtlpEncoding::Protobuf);
    let cases: [(&str, Vec<u8>); 8] = [
        ("truncated", pb[..pb.len() - 3].to_vec()),
        (
            "varint longer than 10 bytes",
            vec![
                0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ],
        ),
        ("truncated varint", vec![0x08, 0x80]),
        ("length beyond", vec![0x0a, 0x05, 0x01]),
        (
            "length beyond",
            vec![0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f],
        ),
        ("wire type 3", vec![0x0b]),
        ("field number 0", vec![0x00, 0x00]),
        ("wrong wire type", vec![0x08, 0x01]),
    ];
    for (what, body) in cases {
        let err = flatten_logs(&body, &protobuf_only).unwrap_err();
        assert!(
            matches!(err, DatalakeError::MalformedPayload { .. }),
            "{what}: {err}"
        );
    }
    // Invalid UTF-8 in a string: record attribute key.
    let request = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [
        {"attributes": [{"key": "KEY", "value": {"stringValue": "v"}}]}]}]}]});
    let mut bad = protobuf(&request);
    let at = bad.windows(3).position(|w| w == b"KEY").unwrap();
    bad[at] = 0xff;
    assert!(flatten(&bad).unwrap_err().to_string().contains("UTF-8"));
    // Every prefix of a valid request either reads or fails cleanly.
    for end in 0..pb.len() {
        let _ = flatten_logs(&pb[..end], &protobuf_only);
    }
}

#[test]
fn decompression_is_limited() {
    let bomb = zstd(&vec![b'{'; 1 << 20]);
    let small = OtlpOptions::default().with_max_decompressed_bytes(64 << 10);
    let err = flatten_logs(&bomb, &small).unwrap_err();
    assert!(matches!(err, DatalakeError::PayloadTooLarge { limit } if limit == 64 << 10));
    let err = flatten_logs(&gzip(&vec![0u8; 1 << 20]), &small).unwrap_err();
    assert!(matches!(err, DatalakeError::PayloadTooLarge { .. }));
    // A forged gzip size does not reserve memory beyond the limit.
    let mut forged = gzip(b"{}");
    let n = forged.len();
    forged[n - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(flatten(&forged).is_err(), "the size check fails the stream");
    // Corrupt streams are malformed.
    let mut corrupt = zstd(b"{\"resourceLogs\": []}");
    corrupt.truncate(corrupt.len() - 2);
    assert!(matches!(
        flatten(&corrupt).unwrap_err(),
        DatalakeError::MalformedPayload { .. }
    ));
    assert!(flatten(&[0x1f, 0x8b, 0x08]).is_err());
}

#[test]
fn options_parse_and_print() {
    // The Python signatures repeat the default as a literal.
    assert_eq!(DEFAULT_MAX_DECOMPRESSED_BYTES, 67_108_864);
    for e in [
        OtlpEncoding::Auto,
        OtlpEncoding::Json,
        OtlpEncoding::Protobuf,
    ] {
        assert_eq!(e.as_str().parse::<OtlpEncoding>().unwrap(), e);
    }
    for c in [
        OtlpCompression::Auto,
        OtlpCompression::None,
        OtlpCompression::Gzip,
        OtlpCompression::Zstd,
    ] {
        assert_eq!(c.as_str().parse::<OtlpCompression>().unwrap(), c);
    }
    assert!("xml".parse::<OtlpEncoding>().is_err());
    assert!("brotli".parse::<OtlpCompression>().is_err());
}

#[test]
fn contract_matches_the_flattened_columns() {
    let mut registry = ContractRegistry::default();
    registry
        .register(&logs_contract("otel", "logs", "1.0.0", "zstd", true))
        .unwrap();
    let contract = registry.get("otel/logs/1.0.0").unwrap();
    let groups = flatten_logs_json(request().to_string().as_bytes(), "tenant.id").unwrap();
    let schema = groups[0].batch.schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let declared: Vec<_> = contract.fields().iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names.len(), declared.len());
    for name in names {
        assert!(declared.contains(&name), "{name} is not declared");
    }
    assert_eq!(contract.partition_by(), Some("timestamp"));
}

#[tokio::test]
async fn flattened_logs_ingest_with_typed_timestamps() {
    use crate::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
    use crate::storage::InMemoryStorage;
    use crate::tenant::{TenantId, TenantProfile};
    use arrow::datatypes::TimeUnit;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut registry = ContractRegistry::default();
    registry
        .register(&logs_contract("otel", "logs", "1.0.0", "zstd", false))
        .unwrap();
    let storage = InMemoryStorage::new();
    let config = PipelineConfig {
        max_future_skew: None,
        ..PipelineConfig::default()
    };
    let pipeline =
        BronzePipeline::new(Arc::new(registry), Arc::new(storage.clone()), config).unwrap();
    let groups = flatten(&zstd(&protobuf(&request()))).unwrap();
    let acme = groups
        .into_iter()
        .find(|g| g.tenant.as_deref() == Some("acme-001"))
        .unwrap();
    let tenant = Arc::new(
        TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "lake")
            .unwrap()
            .build()
            .unwrap(),
    );
    let report = pipeline
        .ingest(IngestRequest::new(
            tenant,
            "otel/logs/1.0.0",
            RecordSource::Arrow(vec![acme.batch]),
        ))
        .await
        .unwrap();
    assert_eq!((report.records_ok, report.records_quarantined), (3, 0));
    let uri = report.data_uri.unwrap();
    let body = storage
        .get(uri.strip_prefix("s3://").unwrap())
        .unwrap()
        .body;
    let stored = ParquetRecordBatchReaderBuilder::try_new(body)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.column_by_name("timestamp").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn several_requests_merge_by_tenant() {
    let one = request().to_string();
    let out =
        flatten_logs_json_many(&[one.as_bytes(), b"nope", one.as_bytes()], "tenant.id").unwrap();
    let rows: Vec<_> = out.tenants.iter().map(|g| g.batch.num_rows()).collect();
    assert_eq!(rows, [2, 6, 2]);
    assert!(out.tenants.iter().all(|g| g.sources == [0, 2]));
    assert_eq!(out.malformed.len(), 1);
    assert_eq!(
        out.malformed[0].0, 1,
        "the unreadable request is reported, the rest kept"
    );
}

#[test]
fn mixed_encodings_merge_by_tenant() {
    let json = request().to_string();
    let pb = protobuf(&request());
    let gz = gzip(&pb);
    let bodies: [&[u8]; 4] = [json.as_bytes(), &pb, b"\x0b", &gz];
    let out = flatten_logs_many(&bodies, &OtlpOptions::default()).unwrap();
    let rows: Vec<_> = out.tenants.iter().map(|g| g.batch.num_rows()).collect();
    assert_eq!(rows, [3, 9, 3]);
    assert_eq!(
        out.encodings,
        [
            Some(OtlpEncoding::Json),
            Some(OtlpEncoding::Protobuf),
            None,
            Some(OtlpEncoding::Protobuf)
        ]
    );
    assert_eq!(out.malformed[0].0, 2);
}

/// The contract must keep the columnar fast path: content keywords are
/// annotations in draft 2020-12, but may be asserted by draft 7.
#[test]
fn contract_keeps_the_columnar_path() {
    let mut registry = ContractRegistry::default();
    registry
        .register(&logs_contract("otel", "logs", "1.0.0", "zstd", false))
        .unwrap();
    assert!(registry
        .get("otel/logs/1.0.0")
        .unwrap()
        .columnar()
        .is_some());
    let mut draft7 = logs_contract("otel", "logs7", "1.0.0", "zstd", false);
    draft7["$schema"] = json!("http://json-schema.org/draft-07/schema#");
    registry.register(&draft7).unwrap();
    assert!(registry
        .get("otel/logs7/1.0.0")
        .unwrap()
        .columnar()
        .is_none());
}

#[test]
fn invalid_payloads_are_rejected() {
    assert!(flatten_logs_json(b"not json", "tenant.id").is_err());
    assert!(flatten_logs_json(b"[1, 2]", "tenant.id").is_err());
    assert!(flatten_logs_json(b"{}", "tenant.id").unwrap().is_empty());
    assert!(
        flatten(b"").unwrap().is_empty(),
        "an empty protobuf request has no records"
    );
}

static RICH_PROTOBUF: std::sync::LazyLock<Vec<u8>> =
    std::sync::LazyLock::new(|| protobuf(&rich_request()));

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Edited requests (bytes overwritten, inserted or cut) are read or
    /// rejected, never a panic.
    #[test]
    fn edited_requests_never_panic(
        edits in proptest::collection::vec((any::<usize>(), any::<u8>(), 0u8..3), 1..8),
        json in any::<bool>(),
        compress in 0u8..3,
    ) {
        let mut body = if json {
            rich_request().to_string().into_bytes()
        } else {
            RICH_PROTOBUF.clone()
        };
        for (at, byte, op) in edits {
            let at = at % (body.len() + 1);
            match op {
                0 if at < body.len() => body[at] = byte,
                1 => body.insert(at, byte),
                _ => body.truncate(at),
            }
        }
        let body = match compress {
            1 => gzip(&body),
            2 => zstd(&body),
            _ => body,
        };
        let _ = flatten(&body);
    }
}
