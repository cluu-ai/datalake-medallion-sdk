//! Conversions between JSON records and Arrow.
//!
//! The Arrow schema of a Bronze batch is derived from the contract, not
//! inferred from the data, so every Parquet object of a contract version has
//! the same columns and types regardless of which fields a batch happens to
//! carry. Level 7 checks that each value fits its column before encoding.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::contracts::{Contract, FieldKind, FieldSpec};
use crate::envelope::{Envelope, EnvelopePrefix};
use crate::error::DatalakeError;
use crate::validation::{codes, level, Rejection};

/// Column type of a declared field. Masked PII fields are stored as hex digests;
/// unmasked PII fields keep their declared type.
pub(crate) fn storage_kind(field: &FieldSpec, pii_masked: bool) -> FieldKind {
    if field.pii && pii_masked {
        FieldKind::String
    } else {
        field.kind
    }
}

pub(crate) fn arrow_type(kind: FieldKind) -> DataType {
    match kind {
        FieldKind::String | FieldKind::Json => DataType::Utf8,
        FieldKind::Integer => DataType::Int64,
        FieldKind::Number => DataType::Float64,
        FieldKind::Boolean => DataType::Boolean,
    }
}

/// Arrow schema to parse JSON records of `contract` into, or `None` when a
/// field is an object, array or union (those stay on the row path).
pub(crate) fn json_input_schema(contract: &Contract) -> Option<Schema> {
    contract
        .fields()
        .iter()
        .map(|f| match f.kind {
            FieldKind::Json => None,
            kind => Some(Field::new(&f.name, arrow_type(kind), true)),
        })
        .collect::<Option<Vec<_>>>()
        .map(Schema::new)
}

/// Integers may arrive as integral floats (`1.0` is a JSON Schema integer).
fn as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        let f = value.as_f64()?;
        #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
        let in_range = f.fract() == 0.0 && f >= i64::MIN as f64 && f < i64::MAX as f64;
        #[allow(clippy::cast_possible_truncation)]
        in_range.then_some(f as i64)
    })
}

fn fits(kind: FieldKind, value: &Value) -> bool {
    match kind {
        FieldKind::String => value.is_string(),
        FieldKind::Integer => as_i64(value).is_some(),
        FieldKind::Number => value.is_number(),
        FieldKind::Boolean => value.is_boolean(),
        FieldKind::Json => true,
    }
}

fn kind_name(kind: FieldKind) -> &'static str {
    match kind {
        FieldKind::String => "string",
        FieldKind::Integer => "integer (signed 64-bit)",
        FieldKind::Number => "number",
        FieldKind::Boolean => "boolean",
        FieldKind::Json => "json",
    }
}

/// Level 7: every declared, non-null value fits its column type.
pub(crate) fn check_types(
    contract: &Contract,
    record: &Map<String, Value>,
    pii_masked: bool,
) -> Option<Rejection> {
    contract.fields().iter().find_map(|f| {
        let kind = storage_kind(f, pii_masked);
        match record.get(&f.name) {
            None | Some(Value::Null) => None,
            Some(v) if fits(kind, v) => None,
            Some(_) => Some(Rejection::new(
                level::TYPE_COERCION,
                codes::TYPE_COERCION,
                format!("field '{}' expects {}", f.name, kind_name(kind)),
            )),
        }
    })
}

/// Arrow schema of a Bronze batch for `contract`: declared fields, the extra
/// column when the mode keeps undeclared fields, then the envelope.
pub(crate) fn bronze_schema(
    contract: &Contract,
    prefix: &EnvelopePrefix,
    pii_masked: bool,
) -> Schema {
    let mut fields: Vec<Field> = contract
        .fields()
        .iter()
        .map(|f| Field::new(&f.name, arrow_type(storage_kind(f, pii_masked)), true))
        .collect();
    if contract.mode().keeps_undeclared() {
        fields.push(Field::new(prefix.extra_column(), DataType::Utf8, true));
    }
    fields.extend(prefix.fields());
    Schema::new(fields)
}

/// Builds the Bronze batch. Records must already have passed [`check_types`].
#[cfg(test)]
pub(crate) fn build_batch(
    contract: &Contract,
    prefix: &EnvelopePrefix,
    records: &[Map<String, Value>],
    envelope: &Envelope<'_>,
    pii_masked: bool,
) -> Result<RecordBatch, DatalakeError> {
    let columns = declared_columns(contract, records, pii_masked)?;
    finish_batch(
        contract,
        prefix,
        columns,
        envelope,
        pii_masked,
        records.len(),
    )
}

/// Declared columns, then the extra column when the mode keeps undeclared
/// fields, for records that already passed [`check_types`].
pub(crate) fn declared_columns(
    contract: &Contract,
    records: &[Map<String, Value>],
    pii_masked: bool,
) -> Result<Vec<ArrayRef>, DatalakeError> {
    let rows = records.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(contract.fields().len() + 1);

    for field in contract.fields() {
        let values = records
            .iter()
            .map(|r| r.get(&field.name).filter(|v| !v.is_null()));
        let column: ArrayRef = match storage_kind(field, pii_masked) {
            FieldKind::String => {
                let mut b = StringBuilder::with_capacity(rows, rows * 16);
                for v in values {
                    match v {
                        None => b.append_null(),
                        Some(Value::String(s)) => b.append_value(s),
                        Some(_) => return Err(mismatch(&field.name)),
                    }
                }
                Arc::new(b.finish())
            }
            FieldKind::Json => {
                let mut b = StringBuilder::with_capacity(rows, rows * 32);
                for v in values {
                    b.append_option(v.map(Value::to_string));
                }
                Arc::new(b.finish())
            }
            FieldKind::Integer => {
                let mut b = Int64Builder::with_capacity(rows);
                for v in values {
                    match v {
                        None => b.append_null(),
                        Some(v) => b.append_value(as_i64(v).ok_or_else(|| mismatch(&field.name))?),
                    }
                }
                Arc::new(b.finish())
            }
            FieldKind::Number => {
                let mut b = Float64Builder::with_capacity(rows);
                for v in values {
                    match v {
                        None => b.append_null(),
                        Some(v) => b.append_value(v.as_f64().ok_or_else(|| mismatch(&field.name))?),
                    }
                }
                Arc::new(b.finish())
            }
            FieldKind::Boolean => {
                let mut b = BooleanBuilder::with_capacity(rows);
                for v in values {
                    match v {
                        None => b.append_null(),
                        Some(v) => {
                            b.append_value(v.as_bool().ok_or_else(|| mismatch(&field.name))?);
                        }
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(column);
    }

    if contract.mode().keeps_undeclared() {
        let mut b = StringBuilder::with_capacity(rows, rows * 16);
        for record in records {
            let extra: Map<String, Value> = record
                .iter()
                .filter(|(k, _)| !contract.declares(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if extra.is_empty() {
                b.append_null();
            } else {
                b.append_value(Value::Object(extra).to_string());
            }
        }
        columns.push(Arc::new(b.finish()));
    }
    Ok(columns)
}

/// Appends the envelope for `rows` rows to the declared columns and assembles
/// the Bronze batch.
pub(crate) fn finish_batch(
    contract: &Contract,
    prefix: &EnvelopePrefix,
    mut columns: Vec<ArrayRef>,
    envelope: &Envelope<'_>,
    pii_masked: bool,
    rows: usize,
) -> Result<RecordBatch, DatalakeError> {
    columns.extend(envelope.columns(rows));
    RecordBatch::try_new(
        Arc::new(bronze_schema(contract, prefix, pii_masked)),
        columns,
    )
    .map_err(|e| DatalakeError::storage("arrow", format!("cannot assemble batch: {e}")))
}

fn mismatch(field: &str) -> DatalakeError {
    DatalakeError::storage(
        "arrow",
        format!("value of '{field}' does not match its column type after Level 7"),
    )
}

/// Serializes Arrow rows to JSON objects so columnar input goes through the
/// same validation as JSON input. Supports every type `arrow-json` can write
/// (nested lists and structs, decimals, temporal types); anything else fails
/// the batch instead of being silently altered.
/// Result of [`coerce_text`].
pub(crate) struct Coerced {
    /// Rows whose values all parsed, with declared types.
    pub typed: RecordBatch,
    /// Input index of each row of `typed`.
    pub kept: Vec<usize>,
    /// Rows with a value that did not parse, as received, with their indices.
    pub unparsed: Option<(RecordBatch, Vec<usize>)>,
}

/// Parses text columns of `integer`, `number` and `boolean` fields into
/// `Int64`, `Float64` and `Boolean`. Other columns are left as they are.
pub(crate) fn coerce_text(
    contract: &Contract,
    batch: &RecordBatch,
) -> Result<Coerced, DatalakeError> {
    use arrow::array::{Array, BooleanArray};
    use arrow::compute::filter_record_batch;
    use arrow::compute::kernels::cast::{cast_with_options, CastOptions};

    let arrow_err = |e: arrow::error::ArrowError| DatalakeError::MalformedPayload {
        reason: format!("cannot parse text columns: {e}"),
    };
    let rows = batch.num_rows();
    let options = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    let mut failed = vec![false; rows];
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (input, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let target = contract
            .fields()
            .iter()
            .find(|f| f.name == *input.name())
            .and_then(|f| match f.kind {
                FieldKind::Integer => Some(DataType::Int64),
                FieldKind::Number => Some(DataType::Float64),
                FieldKind::Boolean => Some(DataType::Boolean),
                _ => None,
            });
        let is_text = matches!(
            array.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        match target {
            Some(target) if is_text => {
                let parsed = cast_with_options(array, &target, &options).map_err(arrow_err)?;
                for (i, flag) in failed.iter_mut().enumerate() {
                    if array.is_valid(i) && parsed.is_null(i) {
                        *flag = true;
                    }
                }
                fields.push(Field::new(input.name(), target, true));
                columns.push(parsed);
            }
            _ => {
                fields.push(input.as_ref().clone());
                columns.push(Arc::clone(array));
            }
        }
    }
    let typed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(arrow_err)?;
    if !failed.iter().any(|f| *f) {
        return Ok(Coerced {
            typed,
            kept: (0..rows).collect(),
            unparsed: None,
        });
    }
    let keep = BooleanArray::from_iter(failed.iter().map(|f| Some(!f)));
    let drop = BooleanArray::from_iter(failed.iter().map(|f| Some(*f)));
    let typed = filter_record_batch(&typed, &keep).map_err(arrow_err)?;
    let rest = filter_record_batch(batch, &drop).map_err(arrow_err)?;
    Ok(Coerced {
        typed,
        kept: (0..rows).filter(|i| !failed[*i]).collect(),
        unparsed: Some((rest, (0..rows).filter(|i| failed[*i]).collect())),
    })
}

pub(crate) fn arrow_rows(batches: &[RecordBatch]) -> Result<Vec<Box<RawValue>>, DatalakeError> {
    let mut writer = LineDelimitedWriter::new(Vec::new());
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    writer
        .write_batches(&refs)
        .and_then(|()| writer.finish())
        .map_err(|e| DatalakeError::MalformedPayload {
            reason: format!("Arrow input cannot be converted to JSON rows: {e}"),
        })?;
    let text =
        String::from_utf8(writer.into_inner()).map_err(|e| DatalakeError::MalformedPayload {
            reason: format!("Arrow input produced invalid UTF-8: {e}"),
        })?;
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            RawValue::from_string(l.to_owned()).map_err(|e| DatalakeError::MalformedPayload {
                reason: format!("Arrow row is not valid JSON: {e}"),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::ContractRegistry;
    use arrow::array::{Array, Int32Array, Int64Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;
    use chrono::Utc;
    use serde_json::json;

    fn contract(mode: &str) -> Arc<Contract> {
        let mut r = ContractRegistry::default();
        let key = r
            .register(&json!({
                "type": "object",
                "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
                "x-dl-flexibility_mode": mode,
                "properties": {
                    "s": {"type": "string"},
                    "i": {"type": "integer"},
                    "n": {"type": ["number", "null"]},
                    "b": {"type": "boolean"},
                    "j": {"type": "object"},
                    "p": {"type": "integer", "x-dl-pii": true}
                }
            }))
            .unwrap();
        r.get(&key).unwrap().clone()
    }

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => unreachable!(),
        }
    }

    fn envelope() -> Envelope<'static> {
        Envelope {
            batch_id: "B",
            tenant_id: "t-1",
            data_type: "t",
            schema_version: "1",
            schema_hash: "h",
            received_at: Utc::now(),
            pii_protection: "sha256",
            pii_key_ref: None,
        }
    }

    #[test]
    fn check_types_accepts_fitting_values() {
        let c = contract("strict");
        let r = obj(json!({"s": "x", "i": 3, "n": 1.5, "b": true, "j": {"k": [1]}, "p": "digest"}));
        assert!(check_types(&c, &r, true).is_none());
        assert!(check_types(&c, &obj(json!({"i": 2.0})), true).is_none());
        assert!(check_types(&c, &obj(json!({"s": null})), true).is_none());
    }

    #[test]
    fn check_types_rejects_mismatches() {
        let c = contract("schema_on_read");
        for bad in [
            json!({"i": "3"}),
            json!({"i": 1.5}),
            json!({"i": 1e30}),
            json!({"b": 1}),
            json!({"s": 1}),
        ] {
            let rej = check_types(&c, &obj(bad.clone()), true).unwrap();
            assert_eq!(rej.code, codes::TYPE_COERCION, "{bad}");
        }
        assert!(check_types(&c, &obj(json!({"i": u64::MAX})), true).is_some());
    }

    #[test]
    fn schema_is_contract_derived_and_stable() {
        let c = contract("strict");
        let p = EnvelopePrefix::default();
        let a = build_batch(&c, &p, &[obj(json!({"s": "x"}))], &envelope(), true).unwrap();
        let b = build_batch(
            &c,
            &p,
            &[obj(json!({"i": 1, "b": false}))],
            &envelope(),
            true,
        )
        .unwrap();
        assert_eq!(a.schema(), b.schema());
        assert_eq!(
            a.schema().field_with_name("i").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            a.schema().field_with_name("p").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(a.schema().field_with_name("_dl_extra").is_err());
        assert!(a.schema().field_with_name("_dl_tenant_id").is_ok());
    }

    #[test]
    fn values_land_in_typed_columns() {
        let c = contract("strict");
        let rows = [
            obj(json!({"s": "x", "i": 7, "j": {"a": 1}})),
            obj(json!({"s": null, "i": 8.0})),
        ];
        let batch = build_batch(&c, &EnvelopePrefix::default(), &rows, &envelope(), true).unwrap();
        let i = batch
            .column_by_name("i")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((i.value(0), i.value(1)), (7, 8));
        let s = batch
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(s.is_null(1));
        let j = batch
            .column_by_name("j")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(j.value(0), r#"{"a":1}"#);
    }

    #[test]
    fn permissive_collects_undeclared_fields() {
        let c = contract("permissive");
        let rows = [obj(json!({"s": "x", "zz": 1})), obj(json!({"s": "y"}))];
        let batch = build_batch(&c, &EnvelopePrefix::default(), &rows, &envelope(), true).unwrap();
        let extra = batch
            .column_by_name("_dl_extra")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(extra.value(0), r#"{"zz":1}"#);
        assert!(extra.is_null(1));
    }

    #[test]
    fn unmasked_pii_keeps_declared_type() {
        let c = contract("strict");
        let p = EnvelopePrefix::default();
        let masked =
            build_batch(&c, &p, &[obj(json!({"p": "digest"}))], &envelope(), true).unwrap();
        let plain = build_batch(&c, &p, &[obj(json!({"p": 5}))], &envelope(), false).unwrap();
        assert_eq!(
            masked.schema().field_with_name("p").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            plain.schema().field_with_name("p").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(check_types(&c, &obj(json!({"p": "x"})), false).is_some());
        assert!(check_types(&c, &obj(json!({"p": "digest"})), true).is_none());
    }

    #[test]
    fn arrow_rows_handles_nested_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
        ]));
        let ids = Int32Array::from(vec![Some(1), None]);
        let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(tags)]).unwrap();
        let rows = arrow_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        let first: Value = serde_json::from_str(rows[0].get()).unwrap();
        assert_eq!(first, json!({"id": 1, "tags": [1, 2]}));
        let second: Value = serde_json::from_str(rows[1].get()).unwrap();
        assert_eq!(second, json!({}));
    }
}
