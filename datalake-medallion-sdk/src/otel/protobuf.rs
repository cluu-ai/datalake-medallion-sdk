//! OTLP/protobuf logs requests, read straight from the wire format.
//!
//! Strings and bytes are borrowed from the request and written into the
//! same row builders as the JSON path: no message structs, no copies, no
//! generated code. Field numbers follow `opentelemetry-proto` v1.11.1
//! (`opentelemetry/proto/{collector/logs,logs,common,resource}/v1/*.proto`).
//! Other fields, including those added in later versions and the
//! `*_strindex` fields that only the profiles signal uses, are skipped by
//! their wire type; the deprecated group wire types are rejected.

use std::io::Write as _;

use base64::Engine as _;

use super::{
    as_text, hex_byte_id, malformed, non_empty, push_double, push_json_str, too_deep, Groups,
    Origin, Record, MAX_VALUE_DEPTH,
};
use crate::error::DatalakeError;

/// A field value by wire type.
#[derive(Clone, Copy)]
enum Wire<'a> {
    Varint(u64),
    I64(u64),
    Len(&'a [u8]),
    I32(u32),
}

/// The fields of one message, in wire order.
struct Fields<'a> {
    buf: &'a [u8],
    pos: usize,
}

fn error(reason: impl std::fmt::Display) -> DatalakeError {
    malformed(format!(
        "OTLP logs request is not valid OTLP/protobuf: {reason}"
    ))
}

impl<'a> Fields<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn varint(&mut self) -> Result<u64, DatalakeError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let Some(&byte) = self.buf.get(self.pos) else {
                return Err(error("truncated varint"));
            };
            self.pos += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(error("varint longer than 10 bytes"))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DatalakeError> {
        let bytes = self
            .pos
            .checked_add(n)
            .and_then(|end| self.buf.get(self.pos..end))
            .ok_or_else(|| error("length beyond the end of the message"))?;
        self.pos += n;
        Ok(bytes)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], DatalakeError> {
        <[u8; N]>::try_from(self.take(N)?).map_err(|_| error("truncated fixed-size field"))
    }

    /// The next field number and value, or `None` at the end.
    fn next(&mut self) -> Result<Option<(u64, Wire<'a>)>, DatalakeError> {
        if self.pos >= self.buf.len() {
            return Ok(None);
        }
        let key = self.varint()?;
        let number = key >> 3;
        if number == 0 {
            return Err(error("field number 0"));
        }
        let value = match key & 7 {
            0 => Wire::Varint(self.varint()?),
            1 => Wire::I64(u64::from_le_bytes(self.fixed()?)),
            2 => {
                let n = usize::try_from(self.varint()?)
                    .map_err(|_| error("length beyond the end of the message"))?;
                Wire::Len(self.take(n)?)
            }
            5 => Wire::I32(u32::from_le_bytes(self.fixed()?)),
            wire => return Err(error(format_args!("unsupported wire type {wire}"))),
        };
        Ok(Some((number, value)))
    }
}

fn wrong_type(field: &str) -> DatalakeError {
    error(format_args!("{field} has the wrong wire type"))
}

fn bytes<'a>(value: Wire<'a>, field: &str) -> Result<&'a [u8], DatalakeError> {
    match value {
        Wire::Len(b) => Ok(b),
        _ => Err(wrong_type(field)),
    }
}

fn string<'a>(value: Wire<'a>, field: &str) -> Result<&'a str, DatalakeError> {
    std::str::from_utf8(bytes(value, field)?)
        .map_err(|_| error(format_args!("{field} is not UTF-8")))
}

fn varint(value: Wire<'_>, field: &str) -> Result<u64, DatalakeError> {
    match value {
        Wire::Varint(v) => Ok(v),
        _ => Err(wrong_type(field)),
    }
}

fn fixed64(value: Wire<'_>, field: &str) -> Result<u64, DatalakeError> {
    match value {
        Wire::I64(v) => Ok(v),
        _ => Err(wrong_type(field)),
    }
}

fn fixed32(value: Wire<'_>, field: &str) -> Result<u32, DatalakeError> {
    match value {
        Wire::I32(v) => Ok(v),
        _ => Err(wrong_type(field)),
    }
}

/// The value of an `AnyValue` (the last member of its `oneof` wins).
#[derive(Clone, Copy)]
enum Any<'a> {
    Null,
    Str(&'a str),
    Bool(bool),
    Int(i64),
    Double(f64),
    Array(&'a [u8]),
    KvList(&'a [u8]),
    Bytes(&'a [u8]),
}

fn any(data: &[u8]) -> Result<Any<'_>, DatalakeError> {
    let mut out = Any::Null;
    let mut fields = Fields::new(data);
    while let Some((number, value)) = fields.next()? {
        out = match number {
            1 => Any::Str(string(value, "AnyValue.string_value")?),
            2 => Any::Bool(varint(value, "AnyValue.bool_value")? != 0),
            3 => Any::Int(varint(value, "AnyValue.int_value")?.cast_signed()),
            4 => Any::Double(f64::from_bits(fixed64(value, "AnyValue.double_value")?)),
            5 => Any::Array(bytes(value, "AnyValue.array_value")?),
            6 => Any::KvList(bytes(value, "AnyValue.kvlist_value")?),
            7 => Any::Bytes(bytes(value, "AnyValue.bytes_value")?),
            _ => continue,
        };
    }
    Ok(out)
}

/// `KeyValue`: the key and the encoded `AnyValue`, if any.
fn key_value(data: &[u8]) -> Result<(&str, Option<&[u8]>), DatalakeError> {
    let (mut key, mut value) = ("", None);
    let mut fields = Fields::new(data);
    while let Some((number, v)) = fields.next()? {
        match number {
            1 => key = string(v, "KeyValue.key")?,
            2 => value = Some(bytes(v, "KeyValue.value")?),
            _ => {}
        }
    }
    Ok((key, value))
}

/// The string of an encoded `AnyValue`, if it holds one.
fn string_value(value: Option<&[u8]>) -> Result<Option<&str>, DatalakeError> {
    if let Some(Any::Str(s)) = value.map(any).transpose()? {
        Ok(Some(s))
    } else {
        Ok(None)
    }
}

/// An encoded `AnyValue` (or its absence) at nesting `depth` as plain JSON.
fn push_value(out: &mut Vec<u8>, value: Option<&[u8]>, depth: usize) -> Result<(), DatalakeError> {
    if let Some(data) = value {
        push_any(out, any(data)?, depth)
    } else {
        out.extend_from_slice(b"null");
        Ok(())
    }
}

/// As the JSON path writes it: bytes as base64 text, as OTLP/JSON carries them.
fn push_any(out: &mut Vec<u8>, value: Any<'_>, depth: usize) -> Result<(), DatalakeError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(too_deep());
    }
    match value {
        Any::Null => out.extend_from_slice(b"null"),
        Any::Str(s) => push_json_str(out, s),
        Any::Bool(b) => out.extend_from_slice(if b { b"true" } else { b"false" }),
        Any::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Any::Double(d) => push_double(out, d),
        Any::Array(data) => {
            out.push(b'[');
            let mut fields = Fields::new(data);
            let mut first = true;
            while let Some((number, v)) = fields.next()? {
                if number == 1 {
                    if !first {
                        out.push(b',');
                    }
                    first = false;
                    push_value(out, Some(bytes(v, "ArrayValue.values")?), depth + 1)?;
                }
            }
            out.push(b']');
        }
        Any::KvList(data) => {
            out.push(b'{');
            let mut fields = Fields::new(data);
            let mut first = true;
            while let Some((number, v)) = fields.next()? {
                if number == 1 {
                    if !first {
                        out.push(b',');
                    }
                    first = false;
                    push_entry(out, bytes(v, "KeyValueList.values")?, depth + 1)?;
                }
            }
            out.push(b'}');
        }
        Any::Bytes(b) => {
            out.push(b'"');
            let start = out.len();
            let engine = base64::engine::general_purpose::STANDARD;
            out.resize(start + base64::encoded_len(b.len(), true).unwrap_or(0), 0);
            let written = engine
                .encode_slice(b, out.get_mut(start..).unwrap_or_default())
                .unwrap_or(0);
            out.truncate(start + written);
            out.push(b'"');
        }
    }
    Ok(())
}

/// One `"key":value` member from an encoded `KeyValue`; returns its key and value.
fn push_entry<'a>(
    out: &mut Vec<u8>,
    data: &'a [u8],
    depth: usize,
) -> Result<(&'a str, Option<&'a [u8]>), DatalakeError> {
    let (key, value) = key_value(data)?;
    push_json_str(out, key);
    out.push(b':');
    push_value(out, value, depth)?;
    Ok((key, value))
}

/// Buffers reused across records.
#[derive(Default)]
struct Scratch {
    resource: Vec<u8>,
    attributes: Vec<u8>,
    body: Vec<u8>,
    trace: String,
    span: String,
}

/// Reads an `ExportLogsServiceRequest` into `groups`.
pub(super) fn read(
    data: &[u8],
    tenant_attribute: &str,
    groups: &mut Groups,
) -> Result<(), DatalakeError> {
    let mut scratch = Scratch::default();
    let mut fields = Fields::new(data);
    while let Some((number, value)) = fields.next()? {
        if number == 1 {
            let resource_logs = bytes(value, "ExportLogsServiceRequest.resource_logs")?;
            read_resource_logs(resource_logs, tenant_attribute, groups, &mut scratch)?;
        }
    }
    Ok(())
}

fn read_resource_logs(
    data: &[u8],
    tenant_attribute: &str,
    groups: &mut Groups,
    scratch: &mut Scratch,
) -> Result<(), DatalakeError> {
    // Encoders write the resource first, but fields may come in any order.
    let mut resource: &[u8] = &[];
    let mut fields = Fields::new(data);
    while let Some((number, value)) = fields.next()? {
        if number == 1 {
            resource = bytes(value, "ResourceLogs.resource")?;
        }
    }
    let Scratch {
        resource: resource_json,
        attributes,
        body,
        trace,
        span,
    } = scratch;
    // The first attribute with each key decides, as in the JSON path.
    let (mut tenant, mut service) = (None, None);
    resource_json.clear();
    let mut fields = Fields::new(resource);
    while let Some((number, value)) = fields.next()? {
        if number == 1 {
            resource_json.push(if resource_json.is_empty() { b'{' } else { b',' });
            let (key, value) = push_entry(resource_json, bytes(value, "Resource.attributes")?, 1)?;
            if tenant.is_none() && key == tenant_attribute {
                tenant = Some(string_value(value)?);
            }
            if service.is_none() && key == "service.name" {
                service = Some(string_value(value)?);
            }
        }
    }
    if !resource_json.is_empty() {
        resource_json.push(b'}');
    }
    let resource_tenant = tenant.flatten();
    let mut scope_logs = Fields::new(data);
    while let Some((number, value)) = scope_logs.next()? {
        if number != 2 {
            continue;
        }
        let data = bytes(value, "ResourceLogs.scope_logs")?;
        let mut scope: &[u8] = &[];
        let mut fields = Fields::new(data);
        while let Some((number, value)) = fields.next()? {
            if number == 1 {
                scope = bytes(value, "ScopeLogs.scope")?;
            }
        }
        let (mut name, mut version) = ("", "");
        let mut fields = Fields::new(scope);
        while let Some((number, value)) = fields.next()? {
            match number {
                1 => name = string(value, "InstrumentationScope.name")?,
                2 => version = string(value, "InstrumentationScope.version")?,
                _ => {}
            }
        }
        let origin = Origin {
            service: service.flatten(),
            resource: non_empty(as_text(resource_json)),
            scope_name: non_empty(name),
            scope_version: non_empty(version),
        };
        let mut records = Fields::new(data);
        while let Some((number, value)) = records.next()? {
            if number != 2 {
                continue;
            }
            let record = bytes(value, "ScopeLogs.log_records")?;
            let mut buffers = RecordBuffers {
                attributes: &mut *attributes,
                body: &mut *body,
                trace: &mut *trace,
                span: &mut *span,
            };
            read_record(
                record,
                tenant_attribute,
                resource_tenant,
                &origin,
                groups,
                &mut buffers,
            )?;
        }
    }
    Ok(())
}

struct RecordBuffers<'s> {
    attributes: &'s mut Vec<u8>,
    body: &'s mut Vec<u8>,
    trace: &'s mut String,
    span: &'s mut String,
}

fn read_record(
    data: &[u8],
    tenant_attribute: &str,
    resource_tenant: Option<&str>,
    origin: &Origin<'_>,
    groups: &mut Groups,
    buffers: &mut RecordBuffers<'_>,
) -> Result<(), DatalakeError> {
    let mut record = Record::default();
    let (mut body, mut trace, mut span): (Option<&[u8]>, &[u8], &[u8]) = (None, &[], &[]);
    let mut record_tenant = None;
    let attributes = &mut *buffers.attributes;
    attributes.clear();
    let mut fields = Fields::new(data);
    while let Some((number, value)) = fields.next()? {
        match number {
            1 => record.time = fixed64(value, "LogRecord.time_unix_nano")?,
            2 => {
                record.severity_number = varint(value, "LogRecord.severity_number")?.cast_signed();
            }
            3 => record.severity_text = string(value, "LogRecord.severity_text")?,
            5 => body = Some(bytes(value, "LogRecord.body")?),
            6 => {
                attributes.push(if attributes.is_empty() { b'{' } else { b',' });
                let (key, value) =
                    push_entry(attributes, bytes(value, "LogRecord.attributes")?, 1)?;
                if record_tenant.is_none() && key == tenant_attribute {
                    record_tenant = Some(string_value(value)?);
                }
            }
            8 => record.flags = i64::from(fixed32(value, "LogRecord.flags")?),
            9 => trace = bytes(value, "LogRecord.trace_id")?,
            10 => span = bytes(value, "LogRecord.span_id")?,
            11 => record.observed = fixed64(value, "LogRecord.observed_time_unix_nano")?,
            12 => record.event_name = string(value, "LogRecord.event_name")?,
            _ => {}
        }
    }
    if !attributes.is_empty() {
        attributes.push(b'}');
    }
    record.body = match body.map(any).transpose()? {
        None | Some(Any::Null) => None,
        Some(Any::Str(s)) => Some(s),
        Some(value) => {
            buffers.body.clear();
            push_any(buffers.body, value, 1)?;
            let text = as_text(buffers.body);
            (text != "null").then_some(text)
        }
    };
    record.attributes = non_empty(as_text(attributes));
    record.trace_id = hex_byte_id(trace, buffers.trace);
    record.span_id = hex_byte_id(span, buffers.span);
    let tenant = resource_tenant.or(record_tenant.flatten());
    groups.push(tenant, origin, &record);
    Ok(())
}
