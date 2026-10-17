//! The Bronze ingestion pipeline: the only path from records to storage.
//!
//! [`BronzePipeline::prepare`] runs Levels 2 to 8 on CPU and produces the
//! encoded Parquet object and dead-letter objects without touching storage.
//! [`BronzePipeline::commit`] uploads them: dead letters first, then data, so
//! a failed call can be retried without duplicating Bronze rows.
//! [`PreparedBatch`] can only be built by `prepare`, so nothing reaches
//! storage without passing validation.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rayon::prelude::*;
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use ulid::Ulid;

use crate::codec;
use crate::columnar::{self, ColumnarRows, ScreenContext};
use crate::contracts::{Contract, ContractRegistry};
use crate::dlq::{self, DeadLetter};
use crate::envelope::{Envelope, EnvelopePrefix};
use crate::error::DatalakeError;
use crate::storage::{ObjectStorage, ParquetEncoder, NDJSON_CONTENT_TYPE, PARQUET_CONTENT_TYPE};
use crate::tenant::{BronzePathResolver, ObjectPath, TenantId};
use crate::validation::hooks::{HookOutcome, RecordHook};
use crate::validation::pii::PiiHasher;
use crate::validation::{codes, level, semantic, Rejection};

/// Default per-record size limit (10 MiB).
pub const DEFAULT_MAX_RECORD_BYTES: usize = 10 * 1024 * 1024;

/// Batches at least this large are validated on the rayon thread pool.
const PARALLEL_THRESHOLD: usize = 512;

/// What to do with records that fail Levels 2 to 7.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InvalidRecordPolicy {
    /// Write valid records and quarantine invalid ones to the dead-letter prefix.
    #[default]
    Quarantine,
    /// Write nothing and return [`DatalakeError::RecordRejected`] for the first invalid record.
    Reject,
}

/// Pipeline settings.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Prefix of envelope columns and reserved payload fields.
    pub envelope_prefix: EnvelopePrefix,
    /// Per-record size limit in bytes (Level 2).
    pub max_record_bytes: usize,
    /// Allowed future skew of `date-time` fields; `None` disables the rule (Level 5).
    pub max_future_skew: Option<Duration>,
    /// Hashing scheme used when PII masking is on (Level 6).
    pub pii_hasher: PiiHasher,
    /// Whether fields marked as PII are masked. Default `true`; an
    /// [`IngestRequest`] can override it per call.
    pub mask_pii: bool,
    /// Handling of invalid records.
    pub invalid_records: InvalidRecordPolicy,
    /// Check Arrow input column by column when the contract allows it.
    /// Accepted rows, rejections and dead letters are the same as on the row
    /// path; turn it off only to compare the two paths or to debug.
    pub arrow_fast_path: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            envelope_prefix: EnvelopePrefix::default(),
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_future_skew: Some(Duration::hours(1)),
            pii_hasher: PiiHasher::default(),
            mask_pii: true,
            invalid_records: InvalidRecordPolicy::default(),
            arrow_fast_path: true,
        }
    }
}

/// Records to ingest.
#[derive(Debug)]
pub enum RecordSource {
    /// A JSON object or an array of JSON objects.
    Json(String),
    /// Parsed JSON values.
    Values(Vec<Value>),
    /// Arrow batches; each row is validated like a JSON object.
    Arrow(Vec<RecordBatch>),
}

/// One ingest call.
pub struct IngestRequest {
    /// Owner of the records.
    pub tenant_id: TenantId,
    /// Contract key (`domain/datatype/version`).
    pub contract: String,
    /// The records.
    pub records: RecordSource,
    /// Optional Level 4 hook.
    pub hook: Option<Arc<dyn RecordHook>>,
    /// Per-call PII masking override; `None` uses [`PipelineConfig::mask_pii`].
    pub mask_pii: Option<bool>,
}

impl IngestRequest {
    /// Creates a request without a hook.
    #[must_use]
    pub fn new(tenant_id: TenantId, contract: impl Into<String>, records: RecordSource) -> Self {
        Self {
            tenant_id,
            contract: contract.into(),
            records,
            hook: None,
            mask_pii: None,
        }
    }

    /// Attaches a Level 4 hook.
    #[must_use]
    pub fn with_hook(mut self, hook: Arc<dyn RecordHook>) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Enables or disables PII masking for this call only.
    #[must_use]
    pub fn with_pii_masking(mut self, enabled: bool) -> Self {
        self.mask_pii = Some(enabled);
        self
    }
}

/// Outcome of an ingest call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReport {
    /// ULID of the call; names the Parquet and dead-letter objects.
    pub batch_id: String,
    /// Records submitted.
    pub records_received: usize,
    /// Records written to the Bronze object.
    pub records_ok: usize,
    /// Records written to dead-letter objects.
    pub records_quarantined: usize,
    /// Quarantined records per error code.
    pub rejections: BTreeMap<String, usize>,
    /// URI of the Parquet object, if any record was valid.
    pub data_uri: Option<String>,
    /// URIs of dead-letter objects, one per error code.
    pub dlq_uris: Vec<String>,
    /// PII scheme applied: `sha256`, `hmac-sha256` or `none`.
    pub pii_masking: String,
}

struct PendingObject {
    path: ObjectPath,
    body: Bytes,
    content_type: &'static str,
}

/// Validated and encoded output of [`BronzePipeline::prepare`], ready to upload.
pub struct PreparedBatch {
    report: IngestReport,
    dead_letters: Vec<PendingObject>,
    data: Option<PendingObject>,
}

impl PreparedBatch {
    /// The report that [`BronzePipeline::commit`] will return on success.
    #[must_use]
    pub fn report(&self) -> &IngestReport {
        &self.report
    }
}

enum Raw {
    Text(Box<RawValue>),
    Parsed(Value),
}

enum State {
    Valid(Map<String, Value>),
    Rejected(Rejection, Option<Map<String, Value>>),
}

/// Values fixed for one ingest call and shared by the later stages.
struct CallContext<'a> {
    tenant: &'a TenantId,
    batch_id: String,
    now: DateTime<Utc>,
    hasher: Option<&'a PiiHasher>,
}

/// Decoded input: rows already accepted by the columnar path, and the rest,
/// with their input index, for the row path.
struct Decoded {
    received: usize,
    columnar: Option<ColumnarRows>,
    raws: Vec<(usize, Raw)>,
}

struct Slot {
    index: usize,
    bytes: usize,
    state: State,
}

/// Validates, encodes and stores Bronze records.
pub struct BronzePipeline {
    registry: Arc<ContractRegistry>,
    storage: Arc<dyn ObjectStorage>,
    paths: BronzePathResolver,
    config: PipelineConfig,
    encoder: ParquetEncoder,
}

impl BronzePipeline {
    /// Creates a pipeline.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for a zero size limit or a negative
    /// skew; [`DatalakeError::ContractBoot`] when a contract declares a field
    /// inside the reserved envelope namespace.
    pub fn new(
        registry: Arc<ContractRegistry>,
        storage: Arc<dyn ObjectStorage>,
        paths: BronzePathResolver,
        config: PipelineConfig,
    ) -> Result<Self, DatalakeError> {
        if config.max_record_bytes == 0 {
            return Err(DatalakeError::config(
                "max_record_bytes must be greater than zero",
            ));
        }
        if config.max_future_skew.is_some_and(|d| d < Duration::zero()) {
            return Err(DatalakeError::config(
                "max_future_skew must not be negative",
            ));
        }
        for contract in registry.contracts() {
            if let Some(f) = contract
                .fields()
                .iter()
                .find(|f| config.envelope_prefix.is_reserved(&f.name))
            {
                return Err(DatalakeError::boot(format!(
                    "contract '{}' declares field '{}' inside the reserved envelope prefix '{}'",
                    contract.key(),
                    f.name,
                    config.envelope_prefix.as_str()
                )));
            }
        }
        Ok(Self {
            registry,
            storage,
            paths,
            config,
            encoder: ParquetEncoder::default(),
        })
    }

    /// The contract registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<ContractRegistry> {
        &self.registry
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Validates and ingests in one step.
    ///
    /// # Errors
    ///
    /// See [`prepare`](Self::prepare) and [`commit`](Self::commit).
    pub async fn ingest(&self, request: IngestRequest) -> Result<IngestReport, DatalakeError> {
        let prepared = self.prepare(request)?;
        self.commit(prepared).await
    }

    /// Runs Levels 2 to 8 and encodes the output. Performs no I/O.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::UnknownContract`], [`DatalakeError::MalformedPayload`]
    /// when the batch cannot be decoded, [`DatalakeError::RecordRejected`]
    /// under [`InvalidRecordPolicy::Reject`], or [`DatalakeError::Storage`]
    /// when encoding fails.
    pub fn prepare(&self, request: IngestRequest) -> Result<PreparedBatch, DatalakeError> {
        let contract = Arc::clone(self.registry.get(&request.contract)?);
        let call = CallContext {
            tenant: &request.tenant_id,
            batch_id: Ulid::generate().to_string(),
            now: Utc::now(),
            hasher: request
                .mask_pii
                .unwrap_or(self.config.mask_pii)
                .then_some(&self.config.pii_hasher),
        };
        let Decoded {
            received,
            columnar,
            raws,
        } = self.decode(&contract, &call, request.records, request.hook.is_some())?;

        let mut slots: Vec<Slot> = if raws.len() >= PARALLEL_THRESHOLD {
            raws.into_par_iter()
                .map(|(i, raw)| self.admit(&contract, i, raw))
                .collect()
        } else {
            raws.into_iter()
                .map(|(i, raw)| self.admit(&contract, i, raw))
                .collect()
        };

        if let Some(hook) = &request.hook {
            for slot in &mut slots {
                self.run_hook(&contract, hook.as_ref(), slot);
            }
        }

        if slots.len() >= PARALLEL_THRESHOLD {
            slots
                .par_iter_mut()
                .for_each(|slot| self.finish(&contract, &call, slot));
        } else {
            for slot in &mut slots {
                self.finish(&contract, &call, slot);
            }
        }

        if self.config.invalid_records == InvalidRecordPolicy::Reject {
            if let Some((index, rejection)) = slots.iter().find_map(|s| match &s.state {
                State::Rejected(r, _) => Some((s.index, r)),
                State::Valid(_) => None,
            }) {
                return Err(DatalakeError::rejected(index, rejection));
            }
        }

        self.assemble(&contract, &call, received, slots, columnar)
    }

    /// Uploads a prepared batch: dead-letter objects first, then data.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] from the storage adapter. Retrying the same
    /// records is safe for the Bronze layer; dead-letter entries may repeat.
    pub async fn commit(&self, prepared: PreparedBatch) -> Result<IngestReport, DatalakeError> {
        for object in prepared.dead_letters.iter().chain(prepared.data.iter()) {
            self.storage
                .put(&object.path.key, object.body.clone(), object.content_type)
                .await?;
        }
        let r = &prepared.report;
        tracing::info!(
            batch_id = %r.batch_id,
            received = r.records_received,
            ok = r.records_ok,
            quarantined = r.records_quarantined,
            "bronze batch committed"
        );
        Ok(prepared.report)
    }

    /// Decodes the input. Arrow input is screened column by column first when
    /// the contract allows it; only the rows the screen cannot accept are
    /// serialized for the row path.
    fn decode(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        source: RecordSource,
        has_hook: bool,
    ) -> Result<Decoded, DatalakeError> {
        if let RecordSource::Arrow(batches) = &source {
            // Hooks see and may rewrite whole records, so they need the row path.
            if self.config.arrow_fast_path && !has_hook {
                if let Some(decoded) = self.decode_columnar(contract, call, batches)? {
                    return Ok(decoded);
                }
            }
        }
        let raws = decode_rows(source)?;
        Ok(Decoded {
            received: raws.len(),
            columnar: None,
            raws: raws.into_iter().enumerate().collect(),
        })
    }

    fn decode_columnar(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        batches: &[RecordBatch],
    ) -> Result<Option<Decoded>, DatalakeError> {
        let Some(plan) = contract.columnar() else {
            return Ok(None);
        };
        let concatenated;
        let batch = match batches {
            [] => return Ok(None),
            [one] => one,
            [first, ..] => match concat_batches(&first.schema(), batches) {
                Ok(b) => {
                    concatenated = b;
                    &concatenated
                }
                // Batches with different schemas stay on the row path.
                Err(_) => return Ok(None),
            },
        };
        let ctx = ScreenContext {
            now: call.now,
            max_future_skew: self.config.max_future_skew,
            max_record_bytes: self.config.max_record_bytes,
            hasher: call.hasher,
        };
        let Some(split) = plan.split(contract, batch, &ctx)? else {
            return Ok(None);
        };
        let raws: Vec<(usize, Raw)> = match split.flagged {
            None => Vec::new(),
            Some((rest, indices)) => indices
                .into_iter()
                .zip(codec::arrow_rows(&[rest])?.into_iter().map(Raw::Text))
                .collect(),
        };
        tracing::debug!(
            columnar = split.accepted.indices.len(),
            row_path = raws.len(),
            "arrow input screened"
        );
        Ok(Some(Decoded {
            received: batch.num_rows(),
            columnar: Some(split.accepted),
            raws,
        }))
    }

    /// Level 2 and Level 3.
    fn admit(&self, contract: &Contract, index: usize, raw: Raw) -> Slot {
        let max = self.config.max_record_bytes;
        let too_large = |bytes: usize| Slot {
            index,
            bytes,
            state: State::Rejected(
                Rejection::new(
                    level::PAYLOAD,
                    codes::RECORD_TOO_LARGE,
                    format!("record is {bytes} bytes; the limit is {max}"),
                ),
                None,
            ),
        };
        let (bytes, value) = match raw {
            Raw::Text(text) => {
                let bytes = text.get().len();
                if bytes > max {
                    return too_large(bytes);
                }
                match serde_json::from_str::<Value>(text.get()) {
                    Ok(v) => (bytes, v),
                    Err(e) => {
                        return Slot {
                            index,
                            bytes,
                            state: State::Rejected(
                                Rejection::new(
                                    level::PAYLOAD,
                                    codes::RECORD_NOT_OBJECT,
                                    format!("record is not valid JSON: {e}"),
                                ),
                                None,
                            ),
                        }
                    }
                }
            }
            Raw::Parsed(value) => {
                let bytes = json_len(&value);
                if bytes > max {
                    return too_large(bytes);
                }
                (bytes, value)
            }
        };
        let Value::Object(map) = value else {
            return Slot {
                index,
                bytes,
                state: State::Rejected(
                    Rejection::new(
                        level::PAYLOAD,
                        codes::RECORD_NOT_OBJECT,
                        "record is not a JSON object",
                    ),
                    None,
                ),
            };
        };
        Slot {
            index,
            bytes,
            state: self.check_contract(contract, map),
        }
    }

    /// Reserved-field check (Level 2) followed by the contract (Level 3).
    fn check_contract(&self, contract: &Contract, map: Map<String, Value>) -> State {
        let prefix = &self.config.envelope_prefix;
        if let Some(field) = map.keys().find(|k| prefix.is_reserved(k)) {
            let rejection = Rejection::new(
                level::PAYLOAD,
                codes::RESERVED_FIELD,
                format!(
                    "field '{field}' uses the reserved envelope prefix '{}'",
                    prefix.as_str()
                ),
            );
            return State::Rejected(rejection, Some(map));
        }
        let value = Value::Object(map);
        let rejection = contract.check(&value);
        let map = match value {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        match rejection {
            None => State::Valid(map),
            Some(r) => State::Rejected(r, Some(map)),
        }
    }

    /// Level 4. A rewritten record is checked against the contract again.
    fn run_hook(&self, contract: &Contract, hook: &dyn RecordHook, slot: &mut Slot) {
        let State::Valid(map) = std::mem::replace(&mut slot.state, State::Valid(Map::new())) else {
            return;
        };
        let original = map.clone();
        slot.state = match hook.apply(map) {
            HookOutcome::Accept(updated) => self.check_contract(contract, updated),
            HookOutcome::Reject { code, reason } => {
                State::Rejected(Rejection::new(level::HOOK, &code, reason), Some(original))
            }
            HookOutcome::Failed { message } => State::Rejected(
                Rejection::new(level::HOOK, codes::HOOK_EXCEPTION, message),
                Some(original),
            ),
        };
    }

    /// Level 6 (when enabled) for every payload, then Levels 5 and 7 for valid records.
    fn finish(&self, contract: &Contract, call: &CallContext<'_>, slot: &mut Slot) {
        let hasher = call.hasher;
        let mask = |map: &mut Map<String, Value>| {
            if let Some(h) = hasher {
                h.mask(map, contract.pii_fields());
            }
        };
        match &mut slot.state {
            State::Rejected(_, Some(map)) => mask(map),
            State::Rejected(_, None) => {}
            State::Valid(map) => {
                mask(map);
                let rejection = self
                    .config
                    .max_future_skew
                    .and_then(|skew| {
                        semantic::check_future_timestamps(
                            map,
                            contract.date_time_fields(),
                            call.now,
                            skew,
                        )
                    })
                    .or_else(|| codec::check_types(contract, map, hasher.is_some()));
                if let Some(r) = rejection {
                    let map = std::mem::take(map);
                    slot.state = State::Rejected(r, Some(map));
                }
            }
        }
    }

    fn assemble(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        received: usize,
        slots: Vec<Slot>,
        columnar: Option<ColumnarRows>,
    ) -> Result<PreparedBatch, DatalakeError> {
        let (tenant, batch_id, now, hasher) =
            (call.tenant, call.batch_id.as_str(), call.now, call.hasher);
        let pii_masking = hasher.map_or("none", PiiHasher::algorithm);
        let mut valid = Vec::new();
        let mut valid_indices = Vec::new();
        let mut by_code: BTreeMap<String, Vec<DeadLetter>> = BTreeMap::new();
        let quarantined_at = now.to_rfc3339_opts(SecondsFormat::Millis, true);

        for slot in slots {
            match slot.state {
                State::Valid(map) => {
                    valid.push(map);
                    valid_indices.push(slot.index);
                }
                State::Rejected(rejection, payload) => {
                    by_code
                        .entry(rejection.code.clone())
                        .or_default()
                        .push(DeadLetter {
                            batch_id: batch_id.to_owned(),
                            record_index: slot.index,
                            tenant_id: tenant.to_string(),
                            contract: contract.key().to_owned(),
                            data_type: contract.data_type().to_owned(),
                            validation_level: rejection.level,
                            error_code: rejection.code,
                            error_message: rejection.message,
                            quarantined_at: quarantined_at.clone(),
                            sdk_version: env!("CARGO_PKG_VERSION").to_owned(),
                            payload_bytes: slot.bytes,
                            payload: payload.map(Value::Object),
                        });
                }
            }
        }

        let mut dead_letters = Vec::with_capacity(by_code.len());
        let mut rejections = BTreeMap::new();
        for (code, entries) in &by_code {
            rejections.insert(code.clone(), entries.len());
            dead_letters.push(PendingObject {
                path: self
                    .paths
                    .dlq_path(tenant, contract.data_type(), code, &now, batch_id)?,
                body: dlq::encode_ndjson(entries)?,
                content_type: NDJSON_CONTENT_TYPE,
            });
        }

        let records_ok = valid.len() + columnar.as_ref().map_or(0, |c| c.indices.len());
        let data = if records_ok == 0 {
            None
        } else {
            let envelope = Envelope {
                batch_id,
                tenant_id: tenant.as_str(),
                data_type: contract.data_type(),
                schema_version: contract.version(),
                schema_hash: contract.schema_hash(),
                received_at: now,
                pii_masking,
            };
            let pii_masked = hasher.is_some();
            let columns = match columnar {
                None => codec::declared_columns(contract, &valid, pii_masked)?,
                Some(fast) if valid.is_empty() => fast.columns,
                Some(fast) => columnar::merge(
                    &fast,
                    &codec::declared_columns(contract, &valid, pii_masked)?,
                    &valid_indices,
                )?,
            };
            let batch = codec::finish_batch(
                contract,
                &self.config.envelope_prefix,
                columns,
                &envelope,
                pii_masked,
                records_ok,
            )?;
            Some(PendingObject {
                path: self
                    .paths
                    .data_path(tenant, contract.data_type(), &now, batch_id)?,
                body: self.encoder.encode(&[batch])?,
                content_type: PARQUET_CONTENT_TYPE,
            })
        };

        let records_quarantined = rejections.values().sum();
        Ok(PreparedBatch {
            report: IngestReport {
                batch_id: batch_id.to_owned(),
                records_received: received,
                records_ok,
                records_quarantined,
                rejections,
                data_uri: data.as_ref().map(|d| d.path.uri.clone()),
                dlq_uris: dead_letters.iter().map(|d| d.path.uri.clone()).collect(),
                pii_masking: pii_masking.to_owned(),
            },
            dead_letters,
            data,
        })
    }
}

fn decode_rows(source: RecordSource) -> Result<Vec<Raw>, DatalakeError> {
    match source {
        RecordSource::Json(text) => {
            let malformed = |e: serde_json::Error| DatalakeError::MalformedPayload {
                reason: format!("batch is not valid JSON: {e}"),
            };
            match text.trim_start().as_bytes().first() {
                Some(b'[') => serde_json::from_str::<Vec<Box<RawValue>>>(&text)
                    .map(|v| v.into_iter().map(Raw::Text).collect())
                    .map_err(malformed),
                Some(b'{') => serde_json::from_str::<Box<RawValue>>(&text)
                    .map(|v| vec![Raw::Text(v)])
                    .map_err(malformed),
                _ => Err(DatalakeError::MalformedPayload {
                    reason: "batch must be a JSON object or an array of JSON objects".into(),
                }),
            }
        }
        RecordSource::Values(values) => Ok(values.into_iter().map(Raw::Parsed).collect()),
        RecordSource::Arrow(batches) => Ok(codec::arrow_rows(&batches)?
            .into_iter()
            .map(Raw::Text)
            .collect()),
    }
}

/// Serialized length of a value without allocating the text.
fn json_len(value: &Value) -> usize {
    struct Counter(usize);
    impl io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => counter.0,
        Err(_) => usize::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryStorage;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;
    use std::sync::Mutex;

    const CONTRACT: &str = "security/login_event/1.0.0";
    const EMAIL: &str = "alice@example.com";

    fn schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "x-dl-domain": "security",
            "x-dl-datatype": "login_event",
            "x-dl-schema_version": "1.0.0",
            "required": ["event_id"],
            "properties": {
                "event_id": {"type": "string"},
                "attempts": {"type": "integer", "minimum": 0},
                "occurred_at": {"type": "string", "format": "date-time"},
                "user_email": {"type": "string", "x-dl-pii": true}
            }
        })
    }

    fn registry_with(schemas: &[Value]) -> Arc<ContractRegistry> {
        let mut r = ContractRegistry::default();
        for s in schemas {
            r.register(s).unwrap();
        }
        Arc::new(r)
    }

    fn pipeline_with(
        storage: Arc<dyn ObjectStorage>,
        config: PipelineConfig,
        schemas: &[Value],
    ) -> BronzePipeline {
        BronzePipeline::new(
            registry_with(schemas),
            storage,
            BronzePathResolver::new("acme-lake").unwrap(),
            config,
        )
        .unwrap()
    }

    fn pipeline(storage: &InMemoryStorage) -> BronzePipeline {
        pipeline_with(
            Arc::new(storage.clone()),
            PipelineConfig::default(),
            &[schema()],
        )
    }

    fn tenant() -> TenantId {
        TenantId::new("acme-001").unwrap()
    }

    fn request(records: Vec<Value>) -> IngestRequest {
        IngestRequest::new(tenant(), CONTRACT, RecordSource::Values(records))
    }

    fn read_parquet(storage: &InMemoryStorage, uri: &str) -> RecordBatch {
        let key = uri.splitn(4, '/').nth(3).unwrap();
        let body = storage.get(key).unwrap().body;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(body)
            .unwrap()
            .build()
            .unwrap();
        reader.next().unwrap().unwrap()
    }

    fn read_dlq(storage: &InMemoryStorage, uri: &str) -> Vec<DeadLetter> {
        let key = uri.splitn(4, '/').nth(3).unwrap();
        let body = storage.get(key).unwrap().body;
        std::str::from_utf8(&body)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn all_bytes(storage: &InMemoryStorage) -> Vec<u8> {
        storage
            .keys()
            .iter()
            .flat_map(|k| storage.get(k).unwrap().body.to_vec())
            .collect()
    }

    fn contains(haystack: &[u8], needle: &str) -> bool {
        haystack
            .windows(needle.len())
            .any(|w| w == needle.as_bytes())
    }

    #[tokio::test]
    async fn valid_records_land_in_one_parquet_object_with_envelope() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(request(vec![
                json!({"event_id": "e1", "attempts": 1}),
                json!({"event_id": "e2"}),
            ]))
            .await
            .unwrap();
        assert_eq!((report.records_ok, report.records_quarantined), (2, 0));
        let uri = report.data_uri.unwrap();
        assert!(uri.starts_with("s3://acme-lake/bronze/tenant_id=acme-001/data_type=login_event/"));
        assert!(uri.ends_with(&format!("{}.parquet", report.batch_id)));
        let batch = read_parquet(&storage, &uri);
        assert_eq!(batch.num_rows(), 2);
        let tenant_col = batch
            .column_by_name("_dl_tenant_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tenant_col.value(0), "acme-001");
        assert!(batch.column_by_name("_dl_schema_hash").is_some());
        let key = uri.strip_prefix("s3://acme-lake/").unwrap();
        assert_eq!(storage.get(key).unwrap().content_type, PARQUET_CONTENT_TYPE);
    }

    #[tokio::test]
    async fn invalid_records_are_quarantined_and_counted() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(request(vec![
                json!({"event_id": "ok"}),
                json!({"attempts": 3}),
                json!({"event_id": "x", "attempts": -1}),
                json!({"event_id": "y", "unknown": true}),
                json!("not an object"),
            ]))
            .await
            .unwrap();
        assert_eq!(report.records_received, 5);
        assert_eq!(report.records_ok, 1);
        assert_eq!(report.records_quarantined, 4);
        assert_eq!(report.rejections[codes::SCHEMA_VIOLATION], 2);
        assert_eq!(report.rejections[codes::UNDECLARED_FIELD], 1);
        assert_eq!(report.rejections[codes::RECORD_NOT_OBJECT], 1);
        assert_eq!(report.dlq_uris.len(), 3);
        let schema_dlq = report
            .dlq_uris
            .iter()
            .find(|u| u.contains("error_code=ERR_SCHEMA_VIOLATION"))
            .unwrap();
        assert!(schema_dlq.starts_with("s3://acme-lake/dlq/tenant_id=acme-001/"));
        let entries = read_dlq(&storage, schema_dlq);
        assert_eq!(
            entries.iter().map(|e| e.record_index).collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(entries.iter().all(|e| e.validation_level == level::SCHEMA));
    }

    #[tokio::test]
    async fn pii_never_reaches_storage_in_clear() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(request(vec![
                json!({"event_id": "ok", "user_email": EMAIL}),
                json!({"event_id": 5, "user_email": EMAIL}),
                json!({"event_id": "late", "user_email": EMAIL, "occurred_at": "2999-01-01T00:00:00Z"}),
            ]))
            .await
            .unwrap();
        assert_eq!(report.records_ok, 1);
        assert_eq!(report.records_quarantined, 2);
        assert!(
            !contains(&all_bytes(&storage), EMAIL),
            "PII leaked to storage"
        );

        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        let email = batch
            .column_by_name("user_email")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(email.value(0), PiiHasher::Sha256.hash(EMAIL));
    }

    #[tokio::test]
    async fn hmac_hasher_is_applied() {
        let storage = InMemoryStorage::new();
        let hasher = PiiHasher::hmac_sha256(&[7u8; 32]).unwrap();
        let config = PipelineConfig {
            pii_hasher: hasher.clone(),
            ..PipelineConfig::default()
        };
        let p = pipeline_with(Arc::new(storage.clone()), config, &[schema()]);
        let report = p
            .ingest(request(vec![json!({"event_id": "e", "user_email": EMAIL})]))
            .await
            .unwrap();
        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        let email = batch.column_by_name("user_email").unwrap();
        let email = email.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(email.value(0), hasher.hash(EMAIL));
    }

    #[tokio::test]
    async fn reject_policy_writes_nothing() {
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            invalid_records: InvalidRecordPolicy::Reject,
            ..PipelineConfig::default()
        };
        let p = pipeline_with(Arc::new(storage.clone()), config, &[schema()]);
        let err = p
            .ingest(request(vec![
                json!({"event_id": "ok"}),
                json!({"event_id": 1}),
            ]))
            .await
            .unwrap_err();
        match err {
            DatalakeError::RecordRejected {
                record_index,
                level: lvl,
                ..
            } => assert_eq!((record_index, lvl), (1, level::SCHEMA)),
            other => panic!("unexpected {other:?}"),
        }
        assert!(storage.is_empty());
    }

    #[tokio::test]
    async fn reserved_envelope_fields_are_rejected() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(request(vec![
                json!({"event_id": "e", "_dl_tenant_id": "other-tenant"}),
            ]))
            .await
            .unwrap();
        assert_eq!(report.rejections[codes::RESERVED_FIELD], 1);
        assert!(report.data_uri.is_none());
    }

    #[tokio::test]
    async fn oversized_records_are_rejected_without_payload() {
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            max_record_bytes: 64,
            ..PipelineConfig::default()
        };
        let p = pipeline_with(Arc::new(storage.clone()), config, &[schema()]);
        let big = "x".repeat(100);
        let report = p
            .ingest(IngestRequest::new(
                tenant(),
                CONTRACT,
                RecordSource::Json(format!(r#"[{{"event_id":"{big}"}},{{"event_id":"s"}}]"#)),
            ))
            .await
            .unwrap();
        assert_eq!(report.records_ok, 1);
        let entries = read_dlq(&storage, &report.dlq_uris[0]);
        assert_eq!(entries[0].error_code, codes::RECORD_TOO_LARGE);
        assert!(entries[0].payload.is_none());
        assert!(entries[0].payload_bytes > 64);
    }

    #[tokio::test]
    async fn json_text_object_and_malformed_batches() {
        let storage = InMemoryStorage::new();
        let p = pipeline(&storage);
        let report = p
            .ingest(IngestRequest::new(
                tenant(),
                CONTRACT,
                RecordSource::Json(r#"{"event_id":"single"}"#.into()),
            ))
            .await
            .unwrap();
        assert_eq!(report.records_ok, 1);
        for bad in ["", "42", "[{]", "nope"] {
            let err = p
                .prepare(IngestRequest::new(
                    tenant(),
                    CONTRACT,
                    RecordSource::Json(bad.into()),
                ))
                .err()
                .unwrap();
            assert!(
                matches!(err, DatalakeError::MalformedPayload { .. }),
                "{bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn arrow_input_is_validated_like_json() {
        let storage = InMemoryStorage::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, true),
            Field::new("attempts", DataType::Int32, true),
            Field::new("user_email", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(-5)])),
                Arc::new(StringArray::from(vec![Some(EMAIL), Some(EMAIL), None])),
            ],
        )
        .unwrap();
        let report = pipeline(&storage)
            .ingest(IngestRequest::new(
                tenant(),
                CONTRACT,
                RecordSource::Arrow(vec![batch]),
            ))
            .await
            .unwrap();
        assert_eq!((report.records_ok, report.records_quarantined), (1, 2));
        assert!(!contains(&all_bytes(&storage), EMAIL));
    }

    #[tokio::test]
    async fn hooks_accept_modify_reject_and_fail() {
        let storage = InMemoryStorage::new();
        let hook = |mut r: Map<String, Value>| match r.get("event_id").and_then(Value::as_str) {
            Some("reject") => HookOutcome::Reject {
                code: "err/blocked".into(),
                reason: "blocked by policy".into(),
            },
            Some("boom") => HookOutcome::Failed {
                message: "hook raised ValueError".into(),
            },
            Some("smuggle") => {
                r.insert("_dl_batch_id".into(), json!("forged"));
                HookOutcome::Accept(r)
            }
            Some("break") => {
                r.insert("attempts".into(), json!("many"));
                HookOutcome::Accept(r)
            }
            _ => {
                r.insert("attempts".into(), json!(42));
                HookOutcome::Accept(r)
            }
        };
        let records = ["fine", "reject", "boom", "smuggle", "break"]
            .iter()
            .map(|id| json!({"event_id": id}))
            .collect();
        let report = pipeline(&storage)
            .ingest(request(records).with_hook(Arc::new(hook)))
            .await
            .unwrap();
        assert_eq!(report.records_ok, 1);
        assert_eq!(report.rejections["ERR_BLOCKED"], 1);
        assert_eq!(report.rejections[codes::HOOK_EXCEPTION], 1);
        assert_eq!(report.rejections[codes::RESERVED_FIELD], 1);
        assert_eq!(report.rejections[codes::SCHEMA_VIOLATION], 1);
        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        let attempts = batch.column_by_name("attempts").unwrap();
        let attempts = attempts
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(attempts.value(0), 42);
    }

    #[tokio::test]
    async fn schema_on_read_still_coerces_types_and_masks() {
        let mut s = schema();
        s["x-dl-flexibility_mode"] = json!("schema_on_read");
        let storage = InMemoryStorage::new();
        let p = pipeline_with(Arc::new(storage.clone()), PipelineConfig::default(), &[s]);
        let report = p
            .ingest(request(vec![
                json!({"attempts": "three"}),
                json!({"user_email": EMAIL, "free": [1, 2]}),
            ]))
            .await
            .unwrap();
        assert_eq!(report.rejections[codes::TYPE_COERCION], 1);
        assert_eq!(report.records_ok, 1);
        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        let extra = batch.column_by_name("_dl_extra").unwrap();
        let extra = extra.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(extra.value(0), r#"{"free":[1,2]}"#);
        assert!(!contains(&all_bytes(&storage), EMAIL));
    }

    #[tokio::test]
    async fn future_skew_can_be_disabled() {
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            max_future_skew: None,
            ..PipelineConfig::default()
        };
        let p = pipeline_with(Arc::new(storage.clone()), config, &[schema()]);
        let report = p
            .ingest(request(vec![
                json!({"event_id": "e", "occurred_at": "2999-01-01T00:00:00Z"}),
            ]))
            .await
            .unwrap();
        assert_eq!(report.records_ok, 1);
    }

    #[tokio::test]
    async fn empty_batch_writes_nothing() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage).ingest(request(vec![])).await.unwrap();
        assert_eq!(report.records_received, 0);
        assert!(report.data_uri.is_none() && report.dlq_uris.is_empty());
        assert!(storage.is_empty());
    }

    #[tokio::test]
    async fn unknown_contract_fails_before_any_work() {
        let storage = InMemoryStorage::new();
        let err = pipeline(&storage)
            .prepare(IngestRequest::new(
                tenant(),
                "a/b/1",
                RecordSource::Values(vec![]),
            ))
            .err()
            .unwrap();
        assert!(matches!(err, DatalakeError::UnknownContract { .. }));
    }

    #[tokio::test]
    async fn large_batches_use_parallel_path_and_keep_order() {
        let storage = InMemoryStorage::new();
        let records = (0..2_000)
            .map(|i| {
                if i % 10 == 0 {
                    json!({"event_id": i})
                } else {
                    json!({"event_id": format!("e{i}"), "user_email": EMAIL})
                }
            })
            .collect();
        let report = pipeline(&storage).ingest(request(records)).await.unwrap();
        assert_eq!(
            (report.records_ok, report.records_quarantined),
            (1_800, 200)
        );
        let entries = read_dlq(&storage, &report.dlq_uris[0]);
        assert!(entries
            .windows(2)
            .all(|w| w[0].record_index < w[1].record_index));
        assert!(!contains(&all_bytes(&storage), EMAIL));
    }

    fn string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
        let col = batch.column_by_name(name).unwrap();
        let col = col.as_any().downcast_ref::<StringArray>().unwrap();
        (0..col.len()).map(|i| col.value(i).to_owned()).collect()
    }

    #[tokio::test]
    async fn masking_can_be_disabled_in_config() {
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            mask_pii: false,
            ..PipelineConfig::default()
        };
        let p = pipeline_with(Arc::new(storage.clone()), config, &[schema()]);
        let report = p
            .ingest(request(vec![
                json!({"event_id": "ok", "user_email": EMAIL}),
                json!({"event_id": 1, "user_email": EMAIL}),
            ]))
            .await
            .unwrap();
        assert_eq!(report.pii_masking, "none");
        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        assert_eq!(string_column(&batch, "user_email"), [EMAIL]);
        assert_eq!(string_column(&batch, "_dl_pii_masking"), ["none"]);
        let dead = read_dlq(&storage, &report.dlq_uris[0]);
        assert_eq!(dead[0].payload.as_ref().unwrap()["user_email"], EMAIL);
        assert!(!dead[0].error_message.contains(EMAIL));
    }

    #[tokio::test]
    async fn per_call_override_wins_in_both_directions() {
        let storage = InMemoryStorage::new();
        let record = || vec![json!({"event_id": "e", "user_email": EMAIL})];

        let masked_by_default = pipeline(&storage);
        let off = masked_by_default
            .ingest(request(record()).with_pii_masking(false))
            .await
            .unwrap();
        let off_batch = read_parquet(&storage, off.data_uri.as_ref().unwrap());
        assert_eq!(string_column(&off_batch, "user_email"), [EMAIL]);
        assert_eq!(off.pii_masking, "none");

        let config = PipelineConfig {
            mask_pii: false,
            pii_hasher: PiiHasher::hmac_sha256(&[9u8; 32]).unwrap(),
            ..PipelineConfig::default()
        };
        let plain_by_default = pipeline_with(Arc::new(storage.clone()), config, &[schema()]);
        let on = plain_by_default
            .ingest(request(record()).with_pii_masking(true))
            .await
            .unwrap();
        let on_batch = read_parquet(&storage, on.data_uri.as_ref().unwrap());
        assert_eq!(on.pii_masking, "hmac-sha256");
        assert_eq!(string_column(&on_batch, "_dl_pii_masking"), ["hmac-sha256"]);
        assert_eq!(
            string_column(&on_batch, "user_email"),
            [PiiHasher::hmac_sha256(&[9u8; 32]).unwrap().hash(EMAIL)]
        );
    }

    #[tokio::test]
    async fn masked_by_default_reports_algorithm() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(request(vec![json!({"event_id": "e", "user_email": EMAIL})]))
            .await
            .unwrap();
        assert_eq!(report.pii_masking, "sha256");
        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        assert_eq!(string_column(&batch, "_dl_pii_masking"), ["sha256"]);
    }

    #[derive(Default)]
    struct FlakyStorage {
        calls: Mutex<Vec<String>>,
        fail_on_parquet: bool,
    }

    #[async_trait]
    impl ObjectStorage for FlakyStorage {
        async fn put(&self, key: &str, _: Bytes, content_type: &str) -> Result<(), DatalakeError> {
            self.calls.lock().unwrap().push(key.to_owned());
            if self.fail_on_parquet && content_type == PARQUET_CONTENT_TYPE {
                return Err(DatalakeError::storage("s3", "unavailable"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn dead_letters_are_written_before_data_and_errors_propagate() {
        let storage = Arc::new(FlakyStorage {
            fail_on_parquet: true,
            ..FlakyStorage::default()
        });
        let p = pipeline_with(storage.clone(), PipelineConfig::default(), &[schema()]);
        let err = p
            .ingest(request(vec![json!({"event_id": "ok"}), json!({})]))
            .await
            .unwrap_err();
        assert!(matches!(err, DatalakeError::Storage { .. }));
        let calls = storage.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].starts_with("dlq/"));
        assert!(calls[1].starts_with("bronze/"));
    }

    #[test]
    fn contract_fields_cannot_use_the_envelope_prefix() {
        let mut s = schema();
        s["properties"]["_dl_custom"] = json!({"type": "string"});
        let result = BronzePipeline::new(
            registry_with(&[s]),
            Arc::new(InMemoryStorage::new()),
            BronzePathResolver::new("acme-lake").unwrap(),
            PipelineConfig::default(),
        );
        assert!(matches!(result, Err(DatalakeError::ContractBoot { .. })));
    }

    #[test]
    fn rejects_invalid_config() {
        let build = |config| {
            BronzePipeline::new(
                registry_with(&[schema()]),
                Arc::new(InMemoryStorage::new()),
                BronzePathResolver::new("acme-lake").unwrap(),
                config,
            )
        };
        assert!(build(PipelineConfig {
            max_record_bytes: 0,
            ..PipelineConfig::default()
        })
        .is_err());
        assert!(build(PipelineConfig {
            max_future_skew: Some(Duration::seconds(-1)),
            ..PipelineConfig::default()
        })
        .is_err());
    }

    #[test]
    fn json_len_matches_serialization() {
        let v = json!({"a": [1, 2, {"b": "c"}], "d": null});
        assert_eq!(json_len(&v), serde_json::to_string(&v).unwrap().len());
    }
}

/// The columnar path must be invisible: for any Arrow input, the Bronze rows,
/// the dead letters and the report match the row path exactly.
#[cfg(test)]
mod columnar_equivalence {
    use super::*;
    use crate::storage::InMemoryStorage;
    use arrow::array::RecordBatchOptions;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray,
        StringArray, StringViewArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use proptest::prelude::*;
    use serde_json::json;

    const CONTRACT: &str = "eq/event/1";
    const VOLATILE: [&str; 3] = ["_dl_ingest_id", "_dl_batch_id", "_dl_received_at"];

    fn contract(mode: &str) -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "x-dl-domain": "eq", "x-dl-datatype": "event", "x-dl-schema_version": "1",
            "x-dl-flexibility_mode": mode,
            "required": ["id"],
            "properties": {
                "id": {"type": "string", "pattern": "^e[0-9]+$"},
                "level": {"type": "string", "enum": ["LOW", "HIGH"]},
                "n": {"type": ["integer", "null"], "minimum": 0, "maximum": 1000},
                "big": {"type": "integer"},
                "score": {"type": "number", "exclusiveMaximum": 100},
                "ok": {"type": "boolean"},
                "at": {"type": "string", "format": "date-time"},
                "mail": {"type": "string", "minLength": 3, "x-dl-pii": true},
                "pnum": {"type": "integer", "x-dl-pii": true},
                "name": {"type": "string", "maxLength": 5}
            }
        })
    }

    #[derive(Debug, Clone)]
    struct Case {
        mode: &'static str,
        mask: bool,
        hmac: bool,
        string_type: u8,
        columns: u16,
        undeclared: bool,
        max_bytes: Option<usize>,
        rows: Vec<Row>,
    }

    #[derive(Debug, Clone)]
    struct Row {
        id: Option<String>,
        level: Option<&'static str>,
        n: Option<i32>,
        big: Option<u64>,
        score: Option<f64>,
        ok: Option<bool>,
        at: Option<&'static str>,
        mail: Option<String>,
        pnum: Option<i64>,
        name: Option<String>,
    }

    fn row() -> impl Strategy<Value = Row> {
        let id = proptest::option::weighted(0.9, "(e[0-9]{1,3})|(x[0-9])|");
        let level = proptest::option::of(prop_oneof![Just("LOW"), Just("HIGH"), Just("MID")]);
        let n = proptest::option::of(-5i32..1005);
        let big = proptest::option::of(prop_oneof![
            0u64..10,
            u64::try_from(i64::MAX).unwrap() - 2..=u64::MAX
        ]);
        let score = proptest::option::of(prop_oneof![
            -1e3f64..1e3,
            Just(f64::NAN),
            Just(f64::INFINITY),
            Just(100.0),
            Just(99.999_999_999_999),
            Just(0.1),
            Just(3.0),
        ]);
        let at = proptest::option::of(prop_oneof![
            Just("2026-01-01T00:00:00Z"),
            Just("2026-10-17T10:00:00+02:00"),
            Just("2999-01-01T00:00:00Z"),
            Just("nope"),
        ]);
        let mail = proptest::option::of("[a-zñ]{0,6}");
        let name = proptest::option::of("[a-zé😀\"\\\\]{0,7}");
        (
            id,
            level,
            n,
            big,
            score,
            proptest::option::of(any::<bool>()),
            at,
            mail,
            proptest::option::of(any::<i64>()),
            name,
        )
            .prop_map(|(id, level, n, big, score, ok, at, mail, pnum, name)| Row {
                id,
                level,
                n,
                big,
                score,
                ok,
                at,
                mail,
                pnum,
                name,
            })
    }

    fn case() -> impl Strategy<Value = Case> {
        (
            prop_oneof![Just("strict"), Just("permissive"), Just("schema_on_read")],
            any::<bool>(),
            any::<bool>(),
            0u8..3,
            // Mostly every column; sometimes a random subset.
            prop_oneof![3 => Just(u16::MAX), 1 => any::<u16>()],
            proptest::bool::weighted(0.1),
            proptest::option::weighted(0.2, 40usize..200),
            proptest::collection::vec(row(), 0..40),
        )
            .prop_map(
                |(mode, mask, hmac, string_type, columns, undeclared, max_bytes, rows)| Case {
                    mode,
                    mask,
                    hmac,
                    string_type,
                    columns,
                    undeclared,
                    max_bytes,
                    rows,
                },
            )
    }

    fn strings(kind: u8, values: Vec<Option<String>>) -> ArrayRef {
        match kind {
            0 => Arc::new(StringArray::from_iter(values)),
            1 => Arc::new(LargeStringArray::from_iter(values)),
            _ => Arc::new(StringViewArray::from_iter(values)),
        }
    }

    fn batch(case: &Case) -> RecordBatch {
        let r = &case.rows;
        let s =
            |f: fn(&Row) -> Option<String>| strings(case.string_type, r.iter().map(f).collect());
        let all: Vec<(&str, ArrayRef)> = vec![
            ("id", s(|x| x.id.clone())),
            ("level", s(|x| x.level.map(str::to_owned))),
            (
                "n",
                Arc::new(Int32Array::from(r.iter().map(|x| x.n).collect::<Vec<_>>())),
            ),
            (
                "big",
                Arc::new(UInt64Array::from(
                    r.iter().map(|x| x.big).collect::<Vec<_>>(),
                )),
            ),
            (
                "score",
                Arc::new(Float64Array::from(
                    r.iter().map(|x| x.score).collect::<Vec<_>>(),
                )),
            ),
            (
                "ok",
                Arc::new(BooleanArray::from(
                    r.iter().map(|x| x.ok).collect::<Vec<_>>(),
                )),
            ),
            ("at", s(|x| x.at.map(str::to_owned))),
            ("mail", s(|x| x.mail.clone())),
            (
                "pnum",
                Arc::new(Int64Array::from(
                    r.iter().map(|x| x.pnum).collect::<Vec<_>>(),
                )),
            ),
            ("name", s(|x| x.name.clone())),
        ];
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for (i, (name, array)) in all.into_iter().enumerate() {
            if case.columns & (1 << i) != 0 {
                fields.push(Field::new(name, array.data_type().clone(), true));
                arrays.push(array);
            }
        }
        if case.undeclared {
            fields.push(Field::new("extra", DataType::Int64, true));
            arrays.push(Arc::new(Int64Array::from(vec![Some(1); r.len()])));
        }
        let options = RecordBatchOptions::new().with_row_count(Some(r.len()));
        RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).unwrap()
    }

    fn pipeline(case: &Case, fast: bool) -> BronzePipeline {
        let mut registry = ContractRegistry::default();
        registry.register(&contract(case.mode)).unwrap();
        let config = PipelineConfig {
            pii_hasher: if case.hmac {
                PiiHasher::hmac_sha256(b"0123456789abcdef0123456789abcdef").unwrap()
            } else {
                PiiHasher::Sha256
            },
            mask_pii: case.mask,
            arrow_fast_path: fast,
            max_record_bytes: case.max_bytes.unwrap_or(DEFAULT_MAX_RECORD_BYTES),
            ..PipelineConfig::default()
        };
        BronzePipeline::new(
            Arc::new(registry),
            Arc::new(InMemoryStorage::new()),
            BronzePathResolver::new("eq-lake").unwrap(),
            config,
        )
        .unwrap()
    }

    /// Everything in a prepared batch that does not depend on ids or time.
    #[derive(Debug, PartialEq)]
    struct Outcome {
        received: usize,
        ok: usize,
        quarantined: usize,
        rejections: BTreeMap<String, usize>,
        pii_masking: String,
        data: Option<RecordBatch>,
        dead_letters: Vec<Value>,
    }

    fn outcome(prepared: PreparedBatch) -> Outcome {
        let data = prepared.data.map(|d| {
            let batch = ParquetRecordBatchReaderBuilder::try_new(d.body)
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let keep: Vec<usize> = (0..batch.num_columns())
                .filter(|i| !VOLATILE.contains(&batch.schema().field(*i).name().as_str()))
                .collect();
            batch.project(&keep).unwrap()
        });
        let dead_letters = prepared
            .dead_letters
            .iter()
            .flat_map(|d| {
                std::str::from_utf8(&d.body)
                    .unwrap()
                    .lines()
                    .map(|l| {
                        let mut v: Value = serde_json::from_str(l).unwrap();
                        let entry = v.as_object_mut().unwrap();
                        entry.remove("batch_id");
                        entry.remove("quarantined_at");
                        v
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let r = prepared.report;
        Outcome {
            received: r.records_received,
            ok: r.records_ok,
            quarantined: r.records_quarantined,
            rejections: r.rejections,
            pii_masking: r.pii_masking,
            data,
            dead_letters,
        }
    }

    fn run(case: &Case, fast: bool) -> Result<Outcome, String> {
        let request = IngestRequest::new(
            TenantId::new("acme-001").unwrap(),
            CONTRACT,
            RecordSource::Arrow(vec![batch(case)]),
        );
        pipeline(case, fast)
            .prepare(request)
            .map(outcome)
            .map_err(|e| e.to_string())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(192))]

        #[test]
        fn columnar_and_row_paths_agree(case in case()) {
            prop_assert_eq!(run(&case, true), run(&case, false));
        }
    }

    #[test]
    fn valid_batches_skip_the_row_path() {
        let case = Case {
            mode: "strict",
            mask: true,
            hmac: false,
            string_type: 2,
            columns: u16::MAX,
            undeclared: false,
            max_bytes: None,
            rows: (0..5000i64)
                .map(|i| Row {
                    id: Some(format!("e{i}")),
                    level: Some("LOW"),
                    n: Some(7),
                    big: Some(1),
                    score: Some(1.5),
                    ok: Some(true),
                    at: Some("2026-01-01T00:00:00Z"),
                    mail: Some(format!("u{i}@x")),
                    pnum: Some(i),
                    name: Some("ana".into()),
                })
                .collect(),
        };
        let p = pipeline(&case, true);
        let contract = Arc::clone(p.registry.get(CONTRACT).unwrap());
        let tenant = TenantId::new("acme-001").unwrap();
        let call = CallContext {
            tenant: &tenant,
            batch_id: String::new(),
            now: Utc::now(),
            hasher: Some(&p.config.pii_hasher),
        };
        let decoded = p
            .decode(
                &contract,
                &call,
                RecordSource::Arrow(vec![batch(&case)]),
                false,
            )
            .unwrap();
        assert!(decoded.raws.is_empty());
        assert_eq!(decoded.columnar.unwrap().indices.len(), 5000);
        assert_eq!(run(&case, true), run(&case, false));
    }

    #[test]
    fn hooks_and_split_batches_use_the_row_path_safely() {
        let case = Case {
            mode: "permissive",
            mask: true,
            hmac: false,
            string_type: 0,
            columns: u16::MAX,
            undeclared: false,
            max_bytes: None,
            rows: vec![],
        };
        let p = pipeline(&case, true);
        let contract = Arc::clone(p.registry.get(CONTRACT).unwrap());
        let tenant = TenantId::new("acme-001").unwrap();
        let call = CallContext {
            tenant: &tenant,
            batch_id: String::new(),
            now: Utc::now(),
            hasher: None,
        };
        let one = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["e1", "bad", "e3"]))],
        )
        .unwrap();
        let decoded = p
            .decode(
                &contract,
                &call,
                RecordSource::Arrow(vec![one.clone(), one.clone()]),
                false,
            )
            .unwrap();
        assert_eq!(decoded.received, 6);
        assert_eq!(decoded.columnar.unwrap().indices, vec![0, 2, 3, 5]);
        assert_eq!(
            decoded.raws.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![1, 4]
        );
        let with_hook = p
            .decode(&contract, &call, RecordSource::Arrow(vec![one]), true)
            .unwrap();
        assert!(with_hook.columnar.is_none());
        assert_eq!(with_hook.raws.len(), 3);
    }
}
