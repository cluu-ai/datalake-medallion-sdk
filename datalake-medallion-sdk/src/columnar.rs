//! Columnar fast path for Arrow input.
//!
//! The row path turns every Arrow row into JSON and validates it with the
//! engine used for JSON input. When a contract only states scalar, per-field
//! rules, [`ColumnarPlan`] checks an Arrow batch column by column and keeps
//! the accepted values as Arrow arrays. Rows it cannot prove valid are
//! *flagged* and go down the row path, which decides them (and writes their
//! dead letters) exactly as it does for any other input.
//!
//! Soundness rule: a row is accepted here only when the row path would accept
//! it and store the same values. Anything this module cannot decide is
//! flagged, never rejected, so both paths produce the same Bronze rows and the
//! same dead letters; only the speed differs.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, AsArray, BooleanArray, StringArray};
use arrow::compute::{cast, filter, filter_record_batch, interleave};
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};
use jsonschema::Validator;
use rayon::prelude::*;
use serde_json::{Map, Value};

use crate::codec;
use crate::contracts::{Contract, FieldKind, FlexibilityMode};
use crate::error::DatalakeError;
use crate::validation::pii::{FieldProtector, ProtectionContext};

/// Columns at least this long are checked and hashed on the rayon pool.
const PARALLEL_THRESHOLD: usize = 4096;

/// Keywords that never change whether an instance is valid.
const ANNOTATIONS: &[&str] = &[
    "$comment",
    "title",
    "description",
    "examples",
    "default",
    "deprecated",
    "readOnly",
    "writeOnly",
];

/// Root keywords the plan accounts for. With every input column declared,
/// `additionalProperties` (a boolean) cannot fail and `$defs` is unused
/// because no property may contain `$ref`.
const ROOT_KEYWORDS: &[&str] = &[
    "$schema",
    "$id",
    "$defs",
    "definitions",
    "type",
    "properties",
    "required",
];

/// Property keywords that only look at the value they apply to, so checking
/// a property on its own gives the same answer as checking the whole record.
const SCALAR_KEYWORDS: &[&str] = &[
    "type",
    "enum",
    "const",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "pattern",
    "format",
];

/// How the plan checks one declared field.
enum Rule {
    /// Scalar field: the Arrow type must fit, then the property schema (if it
    /// says more than `type`) is evaluated per value.
    Scalar { validator: Option<Validator> },
    /// Object, array, union or untyped field: only an absent or all-null
    /// column keeps the batch on the fast path.
    AbsentOnly,
}

struct FieldRule {
    rule: Rule,
    required: bool,
}

/// Per-contract plan for validating Arrow batches column by column.
pub(crate) struct ColumnarPlan {
    rules: Vec<FieldRule>,
    /// `false` in `schema_on_read`, where Level 3 is skipped.
    enforce_schema: bool,
}

impl ColumnarPlan {
    /// Builds the plan, or returns `None` when the contract uses a rule that
    /// cannot be evaluated one field at a time (`allOf`, `if`, `$ref`,
    /// `dependentRequired`, object-valued `additionalProperties`, ...).
    pub(crate) fn compile(
        root: &Map<String, Value>,
        contract_fields: &[crate::contracts::FieldSpec],
        mode: FlexibilityMode,
        extension_prefix: &str,
    ) -> Option<Self> {
        let ignorable = |k: &str| k.starts_with(extension_prefix) || ANNOTATIONS.contains(&k);
        let root_ok = root.iter().all(|(k, v)| {
            ignorable(k)
                || ROOT_KEYWORDS.contains(&k.as_str())
                || (k == "additionalProperties" && v.is_boolean())
        });
        if !root_ok {
            return None;
        }
        let required: HashSet<&str> = match root.get("required") {
            None => HashSet::new(),
            Some(Value::Array(names)) => names.iter().map(Value::as_str).collect::<Option<_>>()?,
            Some(_) => return None,
        };
        if required
            .iter()
            .any(|r| !contract_fields.iter().any(|f| f.name == *r))
        {
            return None;
        }
        let props = root.get("properties").and_then(Value::as_object);
        let draft = root.get("$schema");

        let mut rules = Vec::with_capacity(contract_fields.len());
        for field in contract_fields {
            let prop = props?.get(&field.name)?.as_object()?;
            let rule = if field.kind == FieldKind::Json {
                Rule::AbsentOnly
            } else {
                let mut sub = Map::new();
                for (k, v) in prop {
                    if ignorable(k) {
                        continue;
                    }
                    if !SCALAR_KEYWORDS.contains(&k.as_str()) {
                        return None;
                    }
                    sub.insert(k.clone(), v.clone());
                }
                // `type` alone is enforced by the column type check.
                let validator = if sub.keys().all(|k| k == "type") {
                    None
                } else {
                    if let Some(d) = draft {
                        sub.insert("$schema".into(), d.clone());
                    }
                    Some(
                        jsonschema::options()
                            .should_validate_formats(true)
                            .build(&Value::Object(sub))
                            .ok()?,
                    )
                };
                Rule::Scalar { validator }
            };
            rules.push(FieldRule {
                rule,
                required: required.contains(field.name.as_str()),
            });
        }
        Some(Self {
            rules,
            enforce_schema: mode != FlexibilityMode::SchemaOnRead,
        })
    }

    /// Splits `batch` into rows accepted here and rows for the row path.
    ///
    /// Returns `Ok(None)` when the batch as a whole is not eligible (an
    /// undeclared or duplicated column, an Arrow type the plan does not map,
    /// a missing required column) or when no row would be accepted; the
    /// caller then sends the whole batch down the row path.
    pub(crate) fn split(
        &self,
        contract: &Contract,
        batch: &RecordBatch,
        ctx: &ScreenContext<'_>,
    ) -> Result<Option<Split>, DatalakeError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(None);
        }
        let Some(columns) = self.bind(contract, batch)? else {
            return Ok(None);
        };

        let mut flagged = vec![false; rows];
        for ((field, rule), column) in contract.fields().iter().zip(&self.rules).zip(&columns) {
            let Some(array) = column else {
                if rule.required && self.enforce_schema {
                    return Ok(None);
                }
                continue;
            };
            let masked = field.pii && ctx.protector.is_some();
            let mut checks: Vec<Check<'_>> = Vec::new();
            if rule.required && self.enforce_schema {
                checks.push(Check::NotNull);
            }
            match array.data_type() {
                DataType::Float64 => checks.push(Check::Finite),
                DataType::UInt64 if field.kind == FieldKind::Integer => {
                    checks.push(Check::FitsI64);
                }
                _ => {}
            }
            if let Rule::Scalar { validator: Some(v) } = &rule.rule {
                if self.enforce_schema {
                    checks.push(Check::Schema(v));
                }
            }
            if field.date_time && !masked {
                if let Some(skew) = ctx.max_future_skew {
                    checks.push(Check::NotFuture(ctx.now + skew));
                }
            }
            flag_rows(array, &checks, &mut flagged);
        }
        flag_oversized(contract, &columns, ctx.max_record_bytes, &mut flagged);

        let flagged_count = flagged.iter().filter(|f| **f).count();
        if flagged_count == rows {
            return Ok(None);
        }
        let keep = BooleanArray::from_iter(flagged.iter().map(|f| Some(!f)));
        let accepted_rows = rows - flagged_count;
        let select = |a: &ArrayRef| -> Result<ArrayRef, ArrowError> {
            if flagged_count == 0 {
                Ok(Arc::clone(a))
            } else {
                filter(a.as_ref(), &keep)
            }
        };

        let mut out = Vec::with_capacity(columns.len() + 1);
        for (field, column) in contract.fields().iter().zip(&columns) {
            let masked = field.pii && ctx.protector.is_some();
            let target = codec::arrow_type(codec::storage_kind(field, masked));
            let array = match column {
                None => new_null_array(&target, accepted_rows),
                Some(a) => {
                    let selected = select(a).map_err(arrow_error)?;
                    match ctx.protector {
                        Some(p) if field.pii => {
                            protect_column(p, ctx.tenant_id, &field.name, &selected)?
                        }
                        _ => cast(&selected, &target).map_err(arrow_error)?,
                    }
                }
            };
            out.push(array);
        }
        if contract.mode().keeps_undeclared() {
            out.push(new_null_array(&DataType::Utf8, accepted_rows));
        }

        let indices = (0..rows).filter(|i| !flagged[*i]).collect();
        let flagged_rows = if flagged_count == 0 {
            None
        } else {
            let mask = BooleanArray::from_iter(flagged.iter().map(|f| Some(*f)));
            let rest = filter_record_batch(batch, &mask).map_err(arrow_error)?;
            Some((rest, (0..rows).filter(|i| flagged[*i]).collect()))
        };
        Ok(Some(Split {
            accepted: ColumnarRows {
                columns: out,
                indices,
            },
            flagged: flagged_rows,
        }))
    }

    /// Maps input columns to declared fields and normalizes their types to
    /// `Utf8`, `Int64`, `UInt64`, `Float64` or `Boolean`. `None` when the batch is not
    /// eligible.
    fn bind(
        &self,
        contract: &Contract,
        batch: &RecordBatch,
    ) -> Result<Option<Vec<Option<ArrayRef>>>, DatalakeError> {
        let fields = contract.fields();
        let mut columns: Vec<Option<ArrayRef>> = vec![None; fields.len()];
        let mut seen = HashSet::new();
        for (input, array) in batch.schema().fields().iter().zip(batch.columns()) {
            let name = input.name();
            if !seen.insert(name.as_str()) {
                return Ok(None);
            }
            let Some(i) = fields.iter().position(|f| f.name == *name) else {
                return Ok(None);
            };
            if array.null_count() == array.len() {
                continue;
            }
            let normalized = match (&self.rules[i].rule, fields[i].kind, array.data_type()) {
                (Rule::AbsentOnly, ..) => return Ok(None),
                (_, FieldKind::String, DataType::Utf8)
                | (_, FieldKind::Integer | FieldKind::Number, DataType::Int64 | DataType::UInt64)
                | (_, FieldKind::Number, DataType::Float64)
                | (_, FieldKind::Boolean, DataType::Boolean) => Arc::clone(array),
                (_, FieldKind::String, DataType::LargeUtf8 | DataType::Utf8View) => {
                    cast(array, &DataType::Utf8).map_err(arrow_error)?
                }
                // Dates and timestamps (pyarrow infers them from CSV and
                // Parquet) as the same text the row path's JSON gives them.
                (
                    _,
                    FieldKind::String,
                    DataType::Timestamp(..) | DataType::Date32 | DataType::Date64,
                ) => cast(array, &DataType::Utf8).map_err(arrow_error)?,
                (
                    _,
                    FieldKind::Integer | FieldKind::Number,
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32,
                ) => cast(array, &DataType::Int64).map_err(arrow_error)?,
                // Float32 and Float16 print a shorter decimal than their f64
                // value, so the row path would store a different number.
                _ => return Ok(None),
            };
            columns[i] = Some(normalized);
        }
        Ok(Some(columns))
    }
}

/// Call-level values the screen needs.
pub(crate) struct ScreenContext<'a> {
    pub now: DateTime<Utc>,
    pub max_future_skew: Option<Duration>,
    pub max_record_bytes: usize,
    pub tenant_id: &'a str,
    pub protector: Option<&'a dyn FieldProtector>,
}

/// Rows accepted by the columnar path.
pub(crate) struct ColumnarRows {
    /// Declared columns in contract order, then the extra column when the
    /// mode keeps undeclared fields, already in their storage types.
    pub columns: Vec<ArrayRef>,
    /// Input index of each accepted row, ascending.
    pub indices: Vec<usize>,
}

/// Result of [`ColumnarPlan::split`].
pub(crate) struct Split {
    pub accepted: ColumnarRows,
    /// Rows for the row path, as the original (not normalized) columns, with
    /// their input indices in ascending order.
    pub flagged: Option<(RecordBatch, Vec<usize>)>,
}

enum Check<'a> {
    NotNull,
    Finite,
    FitsI64,
    Schema(&'a Validator),
    NotFuture(DateTime<Utc>),
}

impl Check<'_> {
    /// Whether the non-null value at `i` must go to the row path.
    fn flags(&self, array: &ArrayRef, i: usize) -> bool {
        match self {
            Self::NotNull => false,
            Self::Finite => !array.as_primitive::<Float64Type>().value(i).is_finite(),
            Self::FitsI64 => i64::try_from(array.as_primitive::<UInt64Type>().value(i)).is_err(),
            Self::Schema(v) => !v.is_valid(&cell_value(array, i)),
            Self::NotFuture(limit) => {
                DateTime::parse_from_rfc3339(array.as_string::<i32>().value(i))
                    .is_ok_and(|t| t.with_timezone(&Utc) > *limit)
            }
        }
    }
}

fn flag_rows(array: &ArrayRef, checks: &[Check<'_>], flagged: &mut [bool]) {
    if checks.is_empty() {
        return;
    }
    let not_null = checks.iter().any(|c| matches!(c, Check::NotNull));
    let decide = |(i, flag): (usize, &mut bool)| {
        if *flag {
            return;
        }
        *flag = if array.is_null(i) {
            not_null
        } else {
            checks.iter().any(|c| c.flags(array, i))
        };
    };
    if flagged.len() >= PARALLEL_THRESHOLD {
        flagged.par_iter_mut().enumerate().for_each(decide);
    } else {
        flagged.iter_mut().enumerate().for_each(decide);
    }
}

/// Level 2 without serializing: flags every row whose JSON text could exceed
/// `max_bytes`, using an upper bound of the text `arrow-json` writes (nulls
/// are omitted, any byte may need a six-byte `\u00XX` escape).
fn flag_oversized(
    contract: &Contract,
    columns: &[Option<ArrayRef>],
    max_bytes: usize,
    flagged: &mut [bool],
) {
    let present: Vec<(usize, &ArrayRef)> = contract
        .fields()
        .iter()
        .zip(columns)
        .filter_map(|(f, c)| c.as_ref().map(|a| (escaped(f.name.len()) + 2, a)))
        .collect();
    let cell = |a: &ArrayRef, i: usize| -> usize {
        match a.data_type() {
            DataType::Utf8 => escaped(a.as_string::<i32>().value(i).len()),
            DataType::Boolean => 5,
            _ => 32,
        }
    };
    let widest = |a: &ArrayRef| -> usize {
        match a.data_type() {
            DataType::Utf8 => escaped(
                a.as_string::<i32>()
                    .offsets()
                    .windows(2)
                    .map(|w| usize::try_from(w[1] - w[0]).unwrap_or(usize::MAX))
                    .max()
                    .unwrap_or(0),
            ),
            DataType::Boolean => 5,
            _ => 32,
        }
    };
    let bound: usize = 2 + present
        .iter()
        .map(|(name, a)| name.saturating_add(widest(a)))
        .fold(0usize, usize::saturating_add);
    if bound <= max_bytes {
        return;
    }
    for (i, flag) in flagged.iter_mut().enumerate() {
        let row: usize = 2 + present
            .iter()
            .filter(|(_, a)| a.is_valid(i))
            .map(|(name, a)| name + cell(a, i))
            .fold(0usize, usize::saturating_add);
        if row > max_bytes {
            *flag = true;
        }
    }
}

/// Upper bound of a JSON string literal holding `len` bytes, plus a separator.
fn escaped(len: usize) -> usize {
    len.saturating_mul(6).saturating_add(3)
}

/// The JSON value the row path would see for the non-null cell `i`.
fn cell_value(array: &ArrayRef, i: usize) -> Value {
    match array.data_type() {
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(i).to_owned()),
        DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(i)),
        DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(i)),
        DataType::Float64 => Value::from(array.as_primitive::<Float64Type>().value(i)),
        DataType::Boolean => Value::Bool(array.as_boolean().value(i)),
        other => unreachable!("column normalized to {other}"),
    }
}

/// Text the row path protects for the non-null cell `i`: strings as-is, any
/// other value as its compact JSON.
fn protection_input(array: &ArrayRef, i: usize) -> Cow<'_, str> {
    match array.data_type() {
        DataType::Utf8 => Cow::Borrowed(array.as_string::<i32>().value(i)),
        _ => Cow::Owned(cell_value(array, i).to_string()),
    }
}

/// Protects the non-null cells of one column with a single
/// [`FieldProtector::protect_batch`] call; nulls stay null.
fn protect_column(
    protector: &dyn FieldProtector,
    tenant_id: &str,
    field: &str,
    array: &ArrayRef,
) -> Result<ArrayRef, DatalakeError> {
    let inputs: Vec<Cow<'_, str>> = (0..array.len())
        .filter(|i| array.is_valid(*i))
        .map(|i| protection_input(array, i))
        .collect();
    let refs: Vec<&str> = inputs.iter().map(AsRef::as_ref).collect();
    let ctx = ProtectionContext { tenant_id, field };
    let protected = protector.protect_batch(&ctx, &refs)?;
    if protected.len() != refs.len() {
        return Err(DatalakeError::protection(format!(
            "scheme '{}' returned {} values for {} inputs of field '{field}'",
            protector.scheme(),
            protected.len(),
            refs.len()
        )));
    }
    let mut values = protected.into_iter();
    let column: StringArray = (0..array.len())
        .map(|i| {
            if array.is_valid(i) {
                values.next()
            } else {
                None
            }
        })
        .collect();
    Ok(Arc::new(column))
}

/// Interleaves columnar rows and row-path rows back into input order.
pub(crate) fn merge(
    fast: &ColumnarRows,
    rows: &[ArrayRef],
    row_indices: &[usize],
) -> Result<Vec<ArrayRef>, DatalakeError> {
    let mut order = Vec::with_capacity(fast.indices.len() + row_indices.len());
    let (mut a, mut b) = (0, 0);
    while a < fast.indices.len() || b < row_indices.len() {
        let take_fast =
            b == row_indices.len() || (a < fast.indices.len() && fast.indices[a] < row_indices[b]);
        if take_fast {
            order.push((0, a));
            a += 1;
        } else {
            order.push((1, b));
            b += 1;
        }
    }
    fast.columns
        .iter()
        .zip(rows)
        .map(|(f, r)| interleave(&[f.as_ref(), r.as_ref()], &order).map_err(arrow_error))
        .collect()
}

#[allow(clippy::needless_pass_by_value)]
fn arrow_error(e: ArrowError) -> DatalakeError {
    DatalakeError::storage("arrow", format!("columnar path: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::ContractRegistry;
    use serde_json::json;

    fn plan_for(schema: &Value) -> Option<bool> {
        let mut r = ContractRegistry::default();
        let key = r.register(schema).ok()?;
        Some(r.get(&key).unwrap().columnar().is_some())
    }

    fn base(properties: &Value) -> Value {
        json!({
            "type": "object",
            "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
            "properties": properties
        })
    }

    #[test]
    fn scalar_contracts_get_a_plan() {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "title": "events",
            "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
            "required": ["id"],
            "additionalProperties": false,
            "properties": {
                "id": {"type": "string", "pattern": "^e[0-9]+$", "description": "id"},
                "level": {"type": "string", "enum": ["LOW", "HIGH"]},
                "n": {"type": ["integer", "null"], "minimum": 0, "maximum": 9},
                "at": {"type": "string", "format": "date-time"},
                "nested": {"type": "object", "properties": {"x": {"type": "integer"}}},
                "mail": {"type": "string", "x-dl-pii": true}
            }
        });
        assert_eq!(plan_for(&schema), Some(true));
    }

    #[test]
    fn rules_spanning_fields_or_references_disable_the_plan() {
        for schema in [
            json!({"type": "object", "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
                   "properties": {"a": {"type": "string"}}, "minProperties": 1}),
            json!({"type": "object", "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
                   "properties": {"a": {"type": "string"}},
                   "if": {"required": ["a"]}, "then": {"required": ["a"]}}),
            json!({"type": "object", "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
                   "properties": {"a": {"type": "string"}}, "additionalProperties": {"type": "string"}}),
            json!({"type": "object", "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
                   "$defs": {"s": {"type": "string"}},
                   "properties": {"a": {"type": "string", "$ref": "#/$defs/s"}}}),
            base(&json!({"a": {"type": "string", "oneOf": [{"minLength": 1}, {"maxLength": 0}]}})),
        ] {
            assert_eq!(plan_for(&schema), Some(false), "{schema}");
        }
    }
}
