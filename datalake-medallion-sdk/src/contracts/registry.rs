//! Compiled contract registry: Level 0 (boot) and Level 3 (record validation).
//!
//! The registry is built once and then shared read-only behind an `Arc`, so
//! validation takes no locks. Boot is all-or-nothing: if any schema fails to
//! compile, no registry is produced and the error lists every failure.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use jsonschema::Validator;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::provider::SchemaProvider;
use crate::error::DatalakeError;
use crate::validation::{codes, level, Rejection};

/// Default namespace for SDK keywords inside JSON Schema documents.
pub const DEFAULT_EXTENSION_PREFIX: &str = "x-dl-";

/// Maximum number of schema errors reported per rejected record.
const MAX_REPORTED_ERRORS: usize = 8;

/// How a contract treats fields it does not declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlexibilityMode {
    /// Undeclared fields reject the record; the JSON Schema is enforced.
    #[default]
    Strict,
    /// The JSON Schema is enforced; undeclared fields are kept in the
    /// `<envelope-prefix>extra` JSON column.
    Permissive,
    /// The JSON Schema is not enforced (Level 3 is skipped); PII masking,
    /// type coercion and the envelope still apply. Undeclared fields go to
    /// the `<envelope-prefix>extra` column.
    SchemaOnRead,
}

impl FlexibilityMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "strict" => Some(Self::Strict),
            "permissive" => Some(Self::Permissive),
            "schema_on_read" => Some(Self::SchemaOnRead),
            _ => None,
        }
    }

    /// Keyword value of this mode.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Permissive => "permissive",
            Self::SchemaOnRead => "schema_on_read",
        }
    }

    /// Whether undeclared fields are stored instead of rejected.
    #[must_use]
    pub fn keeps_undeclared(self) -> bool {
        !matches!(self, Self::Strict)
    }
}

/// Storage type of a declared field, derived from its JSON Schema `type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// `"string"`, stored as UTF-8.
    String,
    /// `"integer"`, stored as signed 64-bit.
    Integer,
    /// `"number"`, stored as 64-bit float.
    Number,
    /// `"boolean"`.
    Boolean,
    /// Objects, arrays, unions or untyped fields, stored as compact JSON text.
    Json,
}

/// A top-level field declared under `properties`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSpec {
    /// Field name, also the Parquet column name.
    pub name: String,
    /// Storage type.
    pub kind: FieldKind,
    /// Marked as PII; stored as its 64-char hex digest.
    pub pii: bool,
    /// Declared as `"type": "string", "format": "date-time"`.
    pub date_time: bool,
}

/// One compiled data contract.
pub struct Contract {
    key: String,
    domain: String,
    data_type: String,
    version: String,
    layer: Option<String>,
    mode: FlexibilityMode,
    fields: Vec<FieldSpec>,
    field_index: HashMap<String, usize>,
    pii_fields: Vec<String>,
    date_time_fields: Vec<String>,
    schema_hash: String,
    validator: Validator,
}

impl fmt::Debug for Contract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Contract")
            .field("key", &self.key)
            .field("mode", &self.mode)
            .field("fields", &self.fields.len())
            .field("pii_fields", &self.pii_fields)
            .finish_non_exhaustive()
    }
}

impl Contract {
    /// Compiles a contract from its JSON Schema document.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::ContractBoot`] when a required keyword is missing or
    /// invalid, or when the schema does not compile.
    pub fn compile(schema: &Value, extension_prefix: &str) -> Result<Self, DatalakeError> {
        let root = schema
            .as_object()
            .ok_or_else(|| DatalakeError::boot("a contract must be a JSON object"))?;
        if let Some(t) = root.get("type") {
            if t != "object" {
                return Err(DatalakeError::boot(
                    "a contract must describe records with \"type\": \"object\"",
                ));
            }
        }

        let kw = |name: &str| format!("{extension_prefix}{name}");
        let domain = required_slug(root, &kw("domain"))?;
        let data_type = required_slug(root, &kw("datatype"))?;
        let version = required_version(root, &kw("schema_version"))?;
        let key = format!("{domain}/{data_type}/{version}");
        let ctx = |msg: String| DatalakeError::boot(format!("contract '{key}': {msg}"));

        let layer = match root.get(&kw("layer")) {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => return Err(ctx(format!("'{}' must be a string", kw("layer")))),
        };

        let mode = match root.get(&kw("flexibility_mode")) {
            None => FlexibilityMode::default(),
            Some(Value::String(s)) => FlexibilityMode::parse(s).ok_or_else(|| {
                ctx(format!(
                    "'{}' must be strict, permissive or schema_on_read",
                    kw("flexibility_mode")
                ))
            })?,
            Some(_) => {
                return Err(ctx(format!(
                    "'{}' must be a string",
                    kw("flexibility_mode")
                )))
            }
        };

        let mut fields = Vec::new();
        if let Some(props) = root.get("properties") {
            let props = props
                .as_object()
                .ok_or_else(|| ctx("'properties' must be an object".into()))?;
            for (name, prop) in props {
                let pii = match prop.get(kw("pii")) {
                    None | Some(Value::Bool(false)) => false,
                    Some(Value::Bool(true)) => true,
                    Some(_) => {
                        return Err(ctx(format!(
                            "'{}' on '{name}' must be a boolean",
                            kw("pii")
                        )))
                    }
                };
                let kind = field_kind(prop);
                let date_time = kind == FieldKind::String
                    && prop.get("format").and_then(Value::as_str) == Some("date-time");
                fields.push(FieldSpec {
                    name: name.clone(),
                    kind,
                    pii,
                    date_time,
                });
            }
        }

        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(schema)
            .map_err(|e| ctx(format!("schema does not compile: {}", e.masked())))?;

        let field_index = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        let pii_fields = fields
            .iter()
            .filter(|f| f.pii)
            .map(|f| f.name.clone())
            .collect();
        let date_time_fields = fields
            .iter()
            .filter(|f| f.date_time)
            .map(|f| f.name.clone())
            .collect();

        Ok(Self {
            schema_hash: hex::encode(Sha256::digest(schema.to_string().as_bytes())),
            key,
            domain,
            data_type,
            version,
            layer,
            mode,
            fields,
            field_index,
            pii_fields,
            date_time_fields,
            validator,
        })
    }

    /// Registry key: `domain/datatype/version`.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Contract domain.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }
    /// Data type slug; also the `data_type=` partition.
    #[must_use]
    pub fn data_type(&self) -> &str {
        &self.data_type
    }
    /// Schema version string as declared.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
    /// Optional medallion layer label.
    #[must_use]
    pub fn layer(&self) -> Option<&str> {
        self.layer.as_deref()
    }
    /// Flexibility mode.
    #[must_use]
    pub fn mode(&self) -> FlexibilityMode {
        self.mode
    }
    /// Declared top-level fields, sorted by name (this is also the Parquet column order).
    #[must_use]
    pub fn fields(&self) -> &[FieldSpec] {
        &self.fields
    }
    /// Names of PII fields.
    #[must_use]
    pub fn pii_fields(&self) -> &[String] {
        &self.pii_fields
    }
    /// Names of `date-time` fields.
    #[must_use]
    pub fn date_time_fields(&self) -> &[String] {
        &self.date_time_fields
    }
    /// SHA-256 of the schema document, pinned in every record envelope.
    #[must_use]
    pub fn schema_hash(&self) -> &str {
        &self.schema_hash
    }
    /// Whether `name` is a declared top-level field.
    #[must_use]
    pub fn declares(&self, name: &str) -> bool {
        self.field_index.contains_key(name)
    }

    /// Level 3 check of one record: undeclared fields in strict mode, then the
    /// JSON Schema. Error messages never contain record values.
    #[must_use]
    pub fn check(&self, record: &Value) -> Option<Rejection> {
        if self.mode == FlexibilityMode::SchemaOnRead {
            return None;
        }
        if self.mode == FlexibilityMode::Strict {
            if let Some(obj) = record.as_object() {
                if let Some(name) = obj.keys().find(|k| !self.declares(k)) {
                    return Some(Rejection::new(
                        level::SCHEMA,
                        codes::UNDECLARED_FIELD,
                        format!("field '{name}' is not declared by contract '{}'", self.key),
                    ));
                }
            }
        }
        if self.validator.is_valid(record) {
            return None;
        }
        let messages: Vec<String> = self
            .validator
            .iter_errors(record)
            .take(MAX_REPORTED_ERRORS)
            .map(|e| {
                let path = e.instance_path().to_string();
                let path = if path.is_empty() { "/" } else { path.as_str() };
                format!("{path}: {}", e.masked())
            })
            .collect();
        Some(Rejection::new(
            level::SCHEMA,
            codes::SCHEMA_VIOLATION,
            messages.join("; "),
        ))
    }
}

/// Immutable set of compiled contracts, keyed by `domain/datatype/version`.
#[derive(Debug, Clone)]
pub struct ContractRegistry {
    contracts: BTreeMap<String, Arc<Contract>>,
    extension_prefix: String,
}

impl Default for ContractRegistry {
    fn default() -> Self {
        Self {
            contracts: BTreeMap::new(),
            extension_prefix: DEFAULT_EXTENSION_PREFIX.to_owned(),
        }
    }
}

impl ContractRegistry {
    /// Creates an empty registry using `extension_prefix` for SDK keywords.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] unless the prefix matches
    /// `x-[a-z0-9-]*-` and is 4 to 32 characters long.
    pub fn new(extension_prefix: &str) -> Result<Self, DatalakeError> {
        validate_extension_prefix(extension_prefix)?;
        Ok(Self {
            contracts: BTreeMap::new(),
            extension_prefix: extension_prefix.to_owned(),
        })
    }

    /// Loads and compiles every schema from `provider` (Level 0).
    ///
    /// # Errors
    ///
    /// [`DatalakeError::ContractBoot`] if the provider fails, if any schema
    /// fails to compile, or if two schemas share a key. All failures are
    /// reported together and no partial registry is returned.
    pub fn from_provider(
        provider: &dyn SchemaProvider,
        extension_prefix: &str,
    ) -> Result<Self, DatalakeError> {
        let mut registry = Self::new(extension_prefix)?;
        let mut failures = Vec::new();
        for entry in provider.provide_schemas()? {
            if let Err(e) = registry.register(&entry.content) {
                failures.push(format!("{}: {e}", entry.source_id));
            }
        }
        if !failures.is_empty() {
            return Err(DatalakeError::boot(format!(
                "{} contract(s) failed to load: {}",
                failures.len(),
                failures.join(" | ")
            )));
        }
        tracing::info!(contracts = registry.len(), "contract registry loaded");
        Ok(registry)
    }

    /// Compiles and adds one schema, returning its key.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::ContractBoot`] if the schema is invalid or its key is
    /// already registered.
    pub fn register(&mut self, schema: &Value) -> Result<String, DatalakeError> {
        let contract = Contract::compile(schema, &self.extension_prefix)?;
        let key = contract.key().to_owned();
        if self.contracts.contains_key(&key) {
            return Err(DatalakeError::boot(format!(
                "contract '{key}' is defined more than once"
            )));
        }
        self.contracts.insert(key.clone(), Arc::new(contract));
        Ok(key)
    }

    /// Looks up a contract.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::UnknownContract`] when `key` is not registered.
    pub fn get(&self, key: &str) -> Result<&Arc<Contract>, DatalakeError> {
        self.contracts
            .get(key)
            .ok_or_else(|| DatalakeError::UnknownContract {
                key: key.to_owned(),
            })
    }

    /// Whether `key` is registered.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.contracts.contains_key(key)
    }

    /// Registered keys in sorted order.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.contracts.keys().map(String::as_str)
    }

    /// Registered contracts in key order.
    pub fn contracts(&self) -> impl Iterator<Item = &Arc<Contract>> {
        self.contracts.values()
    }

    /// Number of contracts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    /// Whether no contract is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Prefix used for SDK keywords.
    #[must_use]
    pub fn extension_prefix(&self) -> &str {
        &self.extension_prefix
    }
}

fn validate_extension_prefix(prefix: &str) -> Result<(), DatalakeError> {
    let ok = (4..=32).contains(&prefix.len())
        && prefix.starts_with("x-")
        && prefix.ends_with('-')
        && prefix
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(DatalakeError::config(format!(
            "extension prefix {prefix:?} must match x-[a-z0-9-]*- (4 to 32 chars), e.g. \"x-dl-\""
        )))
    }
}

fn required_slug(root: &Map<String, Value>, keyword: &str) -> Result<String, DatalakeError> {
    let value = root
        .get(keyword)
        .and_then(Value::as_str)
        .ok_or_else(|| DatalakeError::boot(format!("missing string keyword '{keyword}'")))?;
    let ok = (1..=100).contains(&value.len())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok {
        Ok(value.to_owned())
    } else {
        Err(DatalakeError::boot(format!(
            "'{keyword}' must match [a-z0-9_]{{1,100}}, got {value:?}"
        )))
    }
}

fn required_version(root: &Map<String, Value>, keyword: &str) -> Result<String, DatalakeError> {
    let value = root
        .get(keyword)
        .and_then(Value::as_str)
        .ok_or_else(|| DatalakeError::boot(format!("missing string keyword '{keyword}'")))?;
    let ok = (1..=32).contains(&value.len())
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_'));
    if ok {
        Ok(value.to_owned())
    } else {
        Err(DatalakeError::boot(format!(
            "'{keyword}' must match [0-9A-Za-z.+_-]{{1,32}}, got {value:?}"
        )))
    }
}

fn field_kind(prop: &Value) -> FieldKind {
    let single = |t: &str| match t {
        "string" => FieldKind::String,
        "integer" => FieldKind::Integer,
        "number" => FieldKind::Number,
        "boolean" => FieldKind::Boolean,
        _ => FieldKind::Json,
    };
    match prop.get("type") {
        Some(Value::String(t)) => single(t),
        Some(Value::Array(types)) => {
            let non_null: Vec<&str> = types
                .iter()
                .filter_map(Value::as_str)
                .filter(|t| *t != "null")
                .collect();
            match non_null.as_slice() {
                [t] => single(t),
                _ => FieldKind::Json,
            }
        }
        _ => FieldKind::Json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::EmbeddedSchemaProvider;
    use serde_json::json;

    pub(crate) fn sample_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "x-dl-domain": "security",
            "x-dl-datatype": "login_event",
            "x-dl-schema_version": "1.0.0",
            "required": ["event_id", "severity"],
            "properties": {
                "event_id": {"type": "string"},
                "severity": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
                "occurred_at": {"type": "string", "format": "date-time"},
                "attempts": {"type": "integer"},
                "user_email": {"type": "string", "x-dl-pii": true}
            }
        })
    }

    fn registry() -> ContractRegistry {
        let mut r = ContractRegistry::new(DEFAULT_EXTENSION_PREFIX).unwrap();
        r.register(&sample_schema()).unwrap();
        r
    }

    #[test]
    fn compiles_metadata_and_fields() {
        let r = registry();
        let c = r.get("security/login_event/1.0.0").unwrap();
        assert_eq!(c.data_type(), "login_event");
        assert_eq!(c.mode(), FlexibilityMode::Strict);
        assert_eq!(c.pii_fields(), ["user_email"]);
        assert_eq!(c.date_time_fields(), ["occurred_at"]);
        assert_eq!(c.schema_hash().len(), 64);
        let kinds: Vec<_> = c
            .fields()
            .iter()
            .map(|f| (f.name.as_str(), f.kind))
            .collect();
        assert!(kinds.contains(&("attempts", FieldKind::Integer)));
        assert!(
            c.fields().windows(2).all(|w| w[0].name < w[1].name),
            "fields (and Parquet columns) are sorted by name"
        );
    }

    #[test]
    fn valid_record_passes() {
        let r = registry();
        let c = r.get("security/login_event/1.0.0").unwrap();
        assert!(c
            .check(&json!({"event_id": "e1", "severity": "HIGH"}))
            .is_none());
    }

    #[test]
    fn schema_violation_masks_values() {
        let r = registry();
        let c = r.get("security/login_event/1.0.0").unwrap();
        let rej = c
            .check(&json!({"event_id": "e1", "severity": "HIGH", "user_email": 42, "attempts": "secret-token"}))
            .unwrap();
        assert_eq!(rej.code, codes::SCHEMA_VIOLATION);
        assert!(!rej.message.contains("secret-token"), "{}", rej.message);
        assert!(!rej.message.contains("42"), "{}", rej.message);
        assert!(rej.message.contains("/attempts"), "{}", rej.message);
    }

    #[test]
    fn date_time_format_is_asserted() {
        let r = registry();
        let c = r.get("security/login_event/1.0.0").unwrap();
        let rej = c
            .check(&json!({"event_id": "e1", "severity": "LOW", "occurred_at": "yesterday"}))
            .unwrap();
        assert_eq!(rej.code, codes::SCHEMA_VIOLATION);
    }

    #[test]
    fn every_builtin_format_is_asserted() {
        // A format the validator does not know is silently accepted, so a
        // dependency upgrade that drops one must fail here.
        let cases = [
            ("date", "2026-10-17", "17/10/2026"),
            ("email", "ana@example.com", "ana"),
            ("idn-email", "ñandú@example.com", "ñandú"),
            ("hostname", "example.com", "-bad-.com"),
            ("idn-hostname", "ejemplo.españa", "-bad-.com"),
            ("ipv4", "10.0.0.1", "10.0.0.256"),
            ("ipv6", "::1", "1:::1"),
            ("uri", "https://example.com/a", "not a uri"),
            ("uuid", "67e55044-10b1-426f-9247-bb680e5fe0c8", "67e55044"),
        ];
        for (format, good, bad) in cases {
            let mut r = ContractRegistry::default();
            let key = r
                .register(&json!({
                    "type": "object",
                    "x-dl-domain": "d", "x-dl-datatype": "t", "x-dl-schema_version": "1",
                    "properties": {"v": {"type": "string", "format": format}}
                }))
                .unwrap();
            let c = r.get(&key).unwrap();
            assert!(c.check(&json!({ "v": good })).is_none(), "{format}: {good}");
            assert!(c.check(&json!({ "v": bad })).is_some(), "{format}: {bad}");
        }
    }

    #[test]
    fn strict_rejects_undeclared_fields() {
        let r = registry();
        let c = r.get("security/login_event/1.0.0").unwrap();
        let rej = c
            .check(&json!({"event_id": "e1", "severity": "LOW", "surprise": 1}))
            .unwrap();
        assert_eq!(rej.code, codes::UNDECLARED_FIELD);
    }

    #[test]
    fn permissive_keeps_undeclared_but_enforces_schema() {
        let mut schema = sample_schema();
        schema["x-dl-flexibility_mode"] = json!("permissive");
        let mut r = ContractRegistry::default();
        let key = r.register(&schema).unwrap();
        let c = r.get(&key).unwrap();
        assert!(c
            .check(&json!({"event_id": "e", "severity": "LOW", "x": 1}))
            .is_none());
        assert!(c
            .check(&json!({"event_id": "e", "severity": "BAD"}))
            .is_some());
    }

    #[test]
    fn schema_on_read_skips_level_3() {
        let mut schema = sample_schema();
        schema["x-dl-flexibility_mode"] = json!("schema_on_read");
        let mut r = ContractRegistry::default();
        let key = r.register(&schema).unwrap();
        assert!(r.get(&key).unwrap().check(&json!({})).is_none());
    }

    #[test]
    fn custom_extension_prefix() {
        let schema = json!({
            "type": "object",
            "x-acme-domain": "security",
            "x-acme-datatype": "finding",
            "x-acme-schema_version": "2.1.0",
            "properties": {"email": {"type": "string", "x-acme-pii": true}}
        });
        let mut r = ContractRegistry::new("x-acme-").unwrap();
        let key = r.register(&schema).unwrap();
        assert_eq!(key, "security/finding/2.1.0");
        assert_eq!(r.get(&key).unwrap().pii_fields(), ["email"]);
    }

    #[test]
    fn rejects_bad_extension_prefixes() {
        for p in ["", "x-", "dl-", "x-DL-", "x-dl", "acme:"] {
            assert!(ContractRegistry::new(p).is_err(), "{p:?} must be rejected");
        }
    }

    #[test]
    fn missing_keywords_fail_boot() {
        let mut r = ContractRegistry::default();
        let err = r.register(&json!({"type": "object"})).unwrap_err();
        assert!(matches!(err, DatalakeError::ContractBoot { .. }));
    }

    #[test]
    fn invalid_keyword_values_fail_boot() {
        let mut bad_mode = sample_schema();
        bad_mode["x-dl-flexibility_mode"] = json!("loose");
        let mut bad_pii = sample_schema();
        bad_pii["properties"]["user_email"]["x-dl-pii"] = json!("yes");
        let mut bad_slug = sample_schema();
        bad_slug["x-dl-datatype"] = json!("Login Event");
        let mut not_object = sample_schema();
        not_object["type"] = json!("array");
        for schema in [bad_mode, bad_pii, bad_slug, not_object] {
            assert!(ContractRegistry::default().register(&schema).is_err());
        }
    }

    #[test]
    fn duplicate_key_fails_boot() {
        let mut r = registry();
        assert!(r.register(&sample_schema()).is_err());
    }

    #[test]
    fn remote_refs_are_not_fetched() {
        let mut schema = sample_schema();
        schema["properties"]["event_id"] = json!({"$ref": "https://example.invalid/schema.json"});
        assert!(ContractRegistry::default().register(&schema).is_err());
    }

    #[test]
    fn unknown_contract_is_an_error() {
        let err = registry().get("nope/nope/1").unwrap_err();
        assert!(matches!(err, DatalakeError::UnknownContract { .. }));
    }

    #[test]
    fn from_provider_reports_all_failures() {
        let provider = EmbeddedSchemaProvider::new()
            .with_schema("good", sample_schema())
            .with_schema("bad-1", json!("not an object"))
            .with_schema("bad-2", json!({"type": "object"}));
        let err = ContractRegistry::from_provider(&provider, DEFAULT_EXTENSION_PREFIX).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("2 contract(s)"), "{msg}");
        assert!(msg.contains("bad-1") && msg.contains("bad-2"), "{msg}");
    }

    #[test]
    fn schema_hash_is_deterministic() {
        let a = registry();
        let b = registry();
        let key = "security/login_event/1.0.0";
        assert_eq!(
            a.get(key).unwrap().schema_hash(),
            b.get(key).unwrap().schema_hash()
        );
    }

    #[test]
    fn field_kinds_from_type_unions() {
        assert_eq!(
            field_kind(&json!({"type": ["integer", "null"]})),
            FieldKind::Integer
        );
        assert_eq!(
            field_kind(&json!({"type": ["integer", "string"]})),
            FieldKind::Json
        );
        assert_eq!(field_kind(&json!({"type": "object"})), FieldKind::Json);
        assert_eq!(field_kind(&json!({})), FieldKind::Json);
    }
}
