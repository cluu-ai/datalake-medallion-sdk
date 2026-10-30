//! OTLP/JSON logs requests re-encoded as OTLP/protobuf, for tests and
//! benchmarks: the same logical request in both encodings. Only depends on
//! `serde_json` and `base64`, so examples include it with `#[path]`.

use base64::Engine as _;
use serde_json::Value;

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(u8::try_from(v & 0x7f).unwrap_or(0) | 0x80);
        v >>= 7;
    }
    out.push(u8::try_from(v).unwrap_or(0));
}

fn key(out: &mut Vec<u8>, field: u64, wire: u64) {
    varint(out, (field << 3) | wire);
}

fn bytes(out: &mut Vec<u8>, field: u64, data: &[u8]) {
    key(out, field, 2);
    varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn message(out: &mut Vec<u8>, field: u64, write: impl FnOnce(&mut Vec<u8>)) {
    let mut inner = Vec::new();
    write(&mut inner);
    bytes(out, field, &inner);
}

fn get<'a>(v: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    v.get(camel).or_else(|| v.get(snake))
}

fn list(v: Option<&Value>) -> &[Value] {
    v.and_then(Value::as_array).map_or(&[], Vec::as_slice)
}

fn int(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}

fn uint(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str()?.parse().ok())
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .filter_map(|i| u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok())
        .collect()
}

/// The protobuf `ExportLogsServiceRequest` of an OTLP/JSON request.
pub fn request(json: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    for resource_logs in list(get(json, "resourceLogs", "resource_logs")) {
        message(&mut out, 1, |m| write_resource_logs(m, resource_logs));
    }
    out
}

fn write_resource_logs(out: &mut Vec<u8>, v: &Value) {
    if let Some(resource) = v.get("resource") {
        message(out, 1, |m| {
            for kv in list(resource.get("attributes")) {
                message(m, 1, |m| key_value(m, kv));
            }
        });
    }
    for scope_logs in list(get(v, "scopeLogs", "scope_logs")) {
        message(out, 2, |m| write_scope_logs(m, scope_logs));
    }
}

fn write_scope_logs(out: &mut Vec<u8>, v: &Value) {
    if let Some(scope) = v.get("scope") {
        message(out, 1, |m| {
            if let Some(name) = scope.get("name").and_then(Value::as_str) {
                bytes(m, 1, name.as_bytes());
            }
            if let Some(version) = scope.get("version").and_then(Value::as_str) {
                bytes(m, 2, version.as_bytes());
            }
        });
    }
    for record in list(get(v, "logRecords", "log_records")) {
        message(out, 2, |m| write_record(m, record));
    }
}

fn write_record(out: &mut Vec<u8>, v: &Value) {
    let text = |camel, snake| get(v, camel, snake).and_then(Value::as_str);
    if let Some(t) = get(v, "timeUnixNano", "time_unix_nano").and_then(uint) {
        key(out, 1, 1);
        out.extend_from_slice(&t.to_le_bytes());
    }
    if let Some(n) = get(v, "severityNumber", "severity_number").and_then(int) {
        key(out, 2, 0);
        varint(out, n.cast_unsigned());
    }
    if let Some(s) = text("severityText", "severity_text") {
        bytes(out, 3, s.as_bytes());
    }
    if let Some(body) = v.get("body") {
        message(out, 5, |m| any(m, body));
    }
    for kv in list(v.get("attributes")) {
        message(out, 6, |m| key_value(m, kv));
    }
    if let Some(flags) = v.get("flags").and_then(uint) {
        key(out, 8, 5);
        out.extend_from_slice(&u32::try_from(flags).unwrap_or(0).to_le_bytes());
    }
    if let Some(id) = text("traceId", "trace_id") {
        bytes(out, 9, &hex(id));
    }
    if let Some(id) = text("spanId", "span_id") {
        bytes(out, 10, &hex(id));
    }
    if let Some(t) = get(v, "observedTimeUnixNano", "observed_time_unix_nano").and_then(uint) {
        key(out, 11, 1);
        out.extend_from_slice(&t.to_le_bytes());
    }
    if let Some(name) = text("eventName", "event_name") {
        bytes(out, 12, name.as_bytes());
    }
}

fn key_value(out: &mut Vec<u8>, kv: &Value) {
    bytes(
        out,
        1,
        kv.get("key")
            .and_then(Value::as_str)
            .unwrap_or("")
            .as_bytes(),
    );
    if let Some(value) = kv.get("value") {
        message(out, 2, |m| any(m, value));
    }
}

fn any(out: &mut Vec<u8>, v: &Value) {
    let double = |d: &Value| {
        d.as_f64().or_else(|| match d.as_str()? {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            s => s.parse().ok(),
        })
    };
    if let Some(s) = get(v, "stringValue", "string_value").and_then(Value::as_str) {
        bytes(out, 1, s.as_bytes());
    } else if let Some(b) = get(v, "boolValue", "bool_value").and_then(Value::as_bool) {
        key(out, 2, 0);
        varint(out, u64::from(b));
    } else if let Some(i) = get(v, "intValue", "int_value").and_then(int) {
        key(out, 3, 0);
        varint(out, i.cast_unsigned());
    } else if let Some(d) = get(v, "doubleValue", "double_value").and_then(double) {
        key(out, 4, 1);
        out.extend_from_slice(&d.to_bits().to_le_bytes());
    } else if let Some(a) = get(v, "arrayValue", "array_value") {
        message(out, 5, |m| {
            for item in list(a.get("values")) {
                message(m, 1, |m| any(m, item));
            }
        });
    } else if let Some(kv) = get(v, "kvlistValue", "kvlist_value") {
        message(out, 6, |m| {
            for item in list(kv.get("values")) {
                message(m, 1, |m| key_value(m, item));
            }
        });
    } else if let Some(b) = get(v, "bytesValue", "bytes_value").and_then(Value::as_str) {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b)
            .unwrap_or_default();
        bytes(out, 7, &decoded);
    }
}
