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
use crate::tenant::{ObjectPath, TenantProfile};
use crate::validation::hooks::{HookOutcome, RecordHook};
use crate::validation::pii::{self, FieldProtector};
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
    /// JSON Lines text: one JSON object per line, blank lines ignored. A
    /// malformed line is quarantined on its own.
    JsonLines(String),
    /// Arrow batches whose text columns hold values of other declared types,
    /// as read from CSV: `integer`, `number` and `boolean` fields are parsed
    /// from text. A value that does not parse quarantines its record with the
    /// original text.
    ArrowText(Vec<RecordBatch>),
}

/// One ingest call. The tenant profile decides where the records go and how
/// their PII is protected.
pub struct IngestRequest {
    /// Owner of the records, with its destinations and PII protection.
    pub tenant: Arc<TenantProfile>,
    /// Contract key (`domain/datatype/version`).
    pub contract: String,
    /// The records.
    pub records: RecordSource,
    /// Optional Level 4 hook.
    pub hook: Option<Arc<dyn RecordHook>>,
    /// ULID naming the call's objects; generated when `None`. Retrying a call
    /// with the same id rewrites the same keys, so an upload that reached
    /// storage but whose response was lost does not duplicate rows.
    pub batch_id: Option<String>,
}

impl IngestRequest {
    /// Creates a request without a hook.
    #[must_use]
    pub fn new(
        tenant: Arc<TenantProfile>,
        contract: impl Into<String>,
        records: RecordSource,
    ) -> Self {
        Self {
            tenant,
            contract: contract.into(),
            records,
            hook: None,
            batch_id: None,
        }
    }

    /// Fixes the batch id (a ULID), for retries over the same keys.
    #[must_use]
    pub fn with_batch_id(mut self, batch_id: impl Into<String>) -> Self {
        self.batch_id = Some(batch_id.into());
        self
    }

    /// Attaches a Level 4 hook.
    #[must_use]
    pub fn with_hook(mut self, hook: Arc<dyn RecordHook>) -> Self {
        self.hook = Some(hook);
        self
    }
}

/// Objects written at once by one batch (one per event day or error code);
/// their bodies are already in memory, so concurrency adds no memory.
const PUT_CONCURRENCY: usize = 16;

/// Writes `objects` concurrently; the first error is returned and the other
/// requests are cancelled (the batch can be committed again).
async fn put_all(
    storage: &Arc<dyn ObjectStorage>,
    objects: &[PendingObject],
) -> Result<(), DatalakeError> {
    if let [object] = objects {
        return storage
            .put(&object.path, object.body.clone(), object.content_type)
            .await;
    }
    let mut running = tokio::task::JoinSet::new();
    let finished = |r: Result<Result<(), DatalakeError>, tokio::task::JoinError>| {
        r.map_err(|e| DatalakeError::storage("commit", format!("upload failed: {e}")))?
    };
    for object in objects {
        while running.len() >= PUT_CONCURRENCY {
            if let Some(r) = running.join_next().await {
                finished(r)?;
            }
        }
        let storage = Arc::clone(storage);
        let (path, body, content_type) = (
            object.path.clone(),
            object.body.clone(),
            object.content_type,
        );
        running.spawn(async move { storage.put(&path, body, content_type).await });
    }
    while let Some(r) = running.join_next().await {
        finished(r)?;
    }
    Ok(())
}

/// Outcome of an ingest call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReport {
    /// ULID of the call; names the Parquet and dead-letter objects.
    pub batch_id: String,
    /// Tenant the records were written for.
    pub tenant_id: String,
    /// Records submitted.
    pub records_received: usize,
    /// Records written to the Bronze object.
    pub records_ok: usize,
    /// Records written to dead-letter objects.
    pub records_quarantined: usize,
    /// Quarantined records per error code.
    pub rejections: BTreeMap<String, usize>,
    /// URI of the Parquet object, if any record was valid (the first one
    /// when the contract partitions by event time).
    pub data_uri: Option<String>,
    /// URIs of every Parquet object: one, or one per event day.
    pub data_uris: Vec<String>,
    /// URIs of dead-letter objects, one per error code.
    pub dlq_uris: Vec<String>,
    /// PII scheme of the tenant profile: `none`, `sha256`, `hmac-sha256`,
    /// `aes-256-gcm` or a custom scheme name.
    pub pii_protection: String,
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
    data: Vec<PendingObject>,
}

impl PreparedBatch {
    /// The report that [`BronzePipeline::commit`] will return on success.
    #[must_use]
    pub fn report(&self) -> &IngestReport {
        &self.report
    }

    /// Bytes that [`BronzePipeline::commit`] will upload (Parquet and dead letters).
    #[must_use]
    pub fn encoded_bytes(&self) -> usize {
        self.dead_letters
            .iter()
            .chain(self.data.iter())
            .map(|o| o.body.len())
            .sum()
    }
}

enum Raw {
    Text(Box<RawValue>),
    Parsed(Value),
    /// A line of a JSON Lines chunk, not parsed yet.
    Line(Arc<str>, std::ops::Range<usize>),
}

enum State {
    Valid(Map<String, Value>),
    Rejected(Rejection, Option<Map<String, Value>>),
}

/// Values fixed for one ingest call and shared by the later stages.
pub(crate) struct CallContext<'a> {
    pub(crate) tenant: &'a TenantProfile,
    pub(crate) batch_id: String,
    pub(crate) now: DateTime<Utc>,
    pub(crate) protector: Option<&'a dyn FieldProtector>,
    /// Index of the first record within its call or stream (dead letters
    /// report positions in the whole input).
    pub(crate) index_offset: usize,
}

/// Validated output of one chunk, before encoding.
pub(crate) struct ChunkOutput {
    pub(crate) received: usize,
    pub(crate) records_ok: usize,
    /// Accepted records with their lineage columns.
    pub(crate) batch: Option<RecordBatch>,
    /// Rejected records by error code.
    pub(crate) dead_letters: BTreeMap<String, Vec<DeadLetter>>,
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

    pub(crate) fn storage(&self) -> &Arc<dyn ObjectStorage> {
        &self.storage
    }

    /// Encoder with the contract's compression.
    pub(crate) fn encoder_for(&self, contract: &Contract) -> ParquetEncoder {
        match contract.compression() {
            crate::contracts::BronzeCompression::Snappy => self.encoder.clone(),
            other => ParquetEncoder::for_contract(other),
        }
    }

    /// Validates and ingests in one step.
    ///
    /// # Errors
    ///
    /// See [`prepare`](Self::prepare) and [`commit`](Self::commit).
    pub async fn ingest(&self, request: IngestRequest) -> Result<IngestReport, DatalakeError> {
        let prepared = self.prepare(request)?;
        self.commit(&prepared).await
    }

    /// Runs Levels 2 to 8 and encodes the output. Performs no I/O.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::UnknownContract`], [`DatalakeError::MalformedPayload`]
    /// when the batch cannot be decoded, [`DatalakeError::RecordRejected`]
    /// under [`InvalidRecordPolicy::Reject`], [`DatalakeError::Protection`]
    /// when the tenant's PII protector fails, or [`DatalakeError::Storage`]
    /// when encoding fails.
    pub fn prepare(&self, request: IngestRequest) -> Result<PreparedBatch, DatalakeError> {
        let contract = Arc::clone(self.registry.get(&request.contract)?);
        // One key for the whole call: every field and the key_ref column agree.
        let bound = match request.tenant.protector_arc() {
            None => None,
            Some(p) => Some(p.bind(request.tenant.tenant_id().as_str())?.unwrap_or(p)),
        };
        // A given id also fixes the partition date (the ULID's time), so a
        // retry on another day writes the same keys.
        let (batch_id, now) = match &request.batch_id {
            None => (Ulid::generate().to_string(), Utc::now()),
            Some(text) => {
                let id = Ulid::from_string(text).map_err(|_| {
                    DatalakeError::config(format!("batch_id {text:?} is not a ULID"))
                })?;
                (id.to_string(), ulid_time(id))
            }
        };
        let call = CallContext {
            tenant: &request.tenant,
            batch_id,
            now,
            protector: bound.as_deref(),
            index_offset: 0,
        };
        let output =
            self.validate_chunk(&contract, &call, request.records, request.hook.as_ref())?;
        self.package(&contract, &call, &output)
    }

    /// Levels 2 to 8 for one chunk of records: accepted rows as one batch with
    /// lineage columns, rejected ones as dead letters. Performs no I/O.
    pub(crate) fn validate_chunk(
        &self,
        contract: &Arc<Contract>,
        call: &CallContext<'_>,
        records: RecordSource,
        hook: Option<&Arc<dyn RecordHook>>,
    ) -> Result<ChunkOutput, DatalakeError> {
        let Decoded {
            received,
            columnar,
            raws,
        } = self.decode(contract, call, records, hook.is_some())?;

        let mut slots: Vec<Slot> = if raws.len() >= PARALLEL_THRESHOLD {
            raws.into_par_iter()
                .map(|(i, raw)| self.admit(contract, i, raw))
                .collect()
        } else {
            raws.into_iter()
                .map(|(i, raw)| self.admit(contract, i, raw))
                .collect()
        };

        if let Some(hook) = hook {
            for slot in &mut slots {
                self.run_hook(contract, hook.as_ref(), slot);
            }
        }

        // Level 6 before anything else can copy a value: valid records and
        // the payloads of rejected ones, one protector call per field.
        if let Some(protector) = call.protector {
            let mut payloads: Vec<&mut Map<String, Value>> = slots
                .iter_mut()
                .filter_map(|s| match &mut s.state {
                    State::Valid(m) | State::Rejected(_, Some(m)) => Some(m),
                    State::Rejected(_, None) => None,
                })
                .collect();
            pii::protect_records(
                protector,
                call.tenant.tenant_id().as_str(),
                &mut payloads,
                contract.pii_fields(),
            )?;
        }

        if slots.len() >= PARALLEL_THRESHOLD {
            slots
                .par_iter_mut()
                .for_each(|slot| self.finish(contract, call, slot));
        } else {
            for slot in &mut slots {
                self.finish(contract, call, slot);
            }
        }

        if self.config.invalid_records == InvalidRecordPolicy::Reject {
            if let Some((index, rejection)) = slots.iter().find_map(|s| match &s.state {
                State::Rejected(r, _) => Some((s.index, r)),
                State::Valid(_) => None,
            }) {
                return Err(DatalakeError::rejected(
                    call.index_offset + index,
                    rejection,
                ));
            }
        }

        self.collect(contract, call, received, slots, columnar)
    }

    /// [`commit`](Self::commit) with up to `attempts` tries and exponential
    /// backoff with jitter on storage errors. Safe because a commit rewrites
    /// the same keys with the same bytes.
    ///
    /// # Errors
    ///
    /// The last storage error.
    pub async fn commit_with_retries(
        &self,
        prepared: &PreparedBatch,
        attempts: u32,
    ) -> Result<IngestReport, DatalakeError> {
        let attempts = attempts.max(1);
        let mut delay = std::time::Duration::from_millis(200);
        for attempt in 1..=attempts {
            match self.commit(prepared).await {
                Err(DatalakeError::Storage { .. }) if attempt < attempts => {
                    tokio::time::sleep(jittered(delay)).await;
                    delay = delay.saturating_mul(2);
                }
                other => return other,
            }
        }
        self.commit(prepared).await
    }

    /// Uploads a prepared batch: dead-letter objects first, then data.
    ///
    /// The batch is not consumed, so a failed upload can be retried with the
    /// same `prepared` value: it writes the same object keys again, without
    /// re-validating or re-encoding and without duplicating Bronze rows or
    /// dead letters.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] from the storage adapter.
    pub async fn commit(&self, prepared: &PreparedBatch) -> Result<IngestReport, DatalakeError> {
        // Every dead letter is stored before any data object, so a failure
        // never leaves Bronze rows whose rejected siblings were lost.
        put_all(&self.storage, &prepared.dead_letters).await?;
        put_all(&self.storage, &prepared.data).await?;
        let r = &prepared.report;
        tracing::info!(
            batch_id = %r.batch_id,
            tenant_id = %r.tenant_id,
            received = r.records_received,
            ok = r.records_ok,
            quarantined = r.records_quarantined,
            "bronze batch committed"
        );
        Ok(prepared.report.clone())
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
        let source = match source {
            RecordSource::ArrowText(batches) => {
                return self.decode_text(contract, call, &batches, has_hook);
            }
            other => other,
        };
        if let RecordSource::Arrow(batches) = &source {
            // Hooks see and may rewrite whole records, so they need the row path.
            if self.config.arrow_fast_path && !has_hook {
                if let Some(decoded) = self.decode_columnar(contract, call, batches)? {
                    return Ok(decoded);
                }
            }
        }
        let source = match source {
            RecordSource::JsonLines(text) if self.config.arrow_fast_path && !has_hook => {
                return self.decode_json_lines(contract, call, text);
            }
            other => other,
        };
        let raws = decode_rows(source)?;
        Ok(Decoded {
            received: raws.len(),
            columnar: None,
            raws: raws.into_iter().enumerate().collect(),
        })
    }

    /// JSON Lines through the columnar screen: lines are parsed straight to
    /// typed Arrow with the contract's schema. Lines with an explicit `null`
    /// (which JSON Schema treats differently from an absent field) or above
    /// the size limit, and every line of a block that does not parse as the
    /// declared types, take the row path, so results match it exactly.
    fn decode_json_lines(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        text: String,
    ) -> Result<Decoded, DatalakeError> {
        let text: Arc<str> = Arc::from(text);
        let lines = line_ranges(&text);
        let schema = match (contract.columnar(), codec::json_input_schema(contract)) {
            (Some(_), Some(schema)) => Arc::new(schema),
            _ => {
                return Ok(Decoded {
                    received: lines.len(),
                    columnar: None,
                    raws: lines
                        .into_iter()
                        .enumerate()
                        .map(|(i, r)| (i, Raw::Line(Arc::clone(&text), r)))
                        .collect(),
                })
            }
        };
        let max = self.config.max_record_bytes;
        let (typed, rows): (Vec<usize>, Vec<usize>) = (0..lines.len()).partition(|&i| {
            let line = &text.as_bytes()[lines[i].clone()];
            line.len() <= max && memchr::memmem::find(line, b"null").is_none()
        });
        let row_path = |indices: &[usize]| -> Vec<(usize, Raw)> {
            indices
                .iter()
                .map(|&i| (i, Raw::Line(Arc::clone(&text), lines[i].clone())))
                .collect()
        };
        let parsed = if typed.is_empty() {
            None
        } else if rows.is_empty() {
            // Every line takes the typed path (the usual case): parse the
            // chunk as it is, without copying it.
            parse_json_lines(text.as_bytes(), &schema).filter(|b| b.num_rows() == typed.len())
        } else {
            let mut buffer = Vec::with_capacity(text.len());
            for &i in &typed {
                buffer.extend_from_slice(&text.as_bytes()[lines[i].clone()]);
                buffer.push(b'\n');
            }
            parse_json_lines(&buffer, &schema).filter(|b| b.num_rows() == typed.len())
        };
        let Some(batch) = parsed else {
            let all: Vec<usize> = (0..lines.len()).collect();
            return Ok(Decoded {
                received: lines.len(),
                columnar: None,
                raws: row_path(&all),
            });
        };
        let Some(mut decoded) = self.decode_columnar(contract, call, &[batch])? else {
            let all: Vec<usize> = (0..lines.len()).collect();
            return Ok(Decoded {
                received: lines.len(),
                columnar: None,
                raws: row_path(&all),
            });
        };
        // Indices refer to the parsed lines; map them back to the chunk.
        if let Some(columnar) = decoded.columnar.as_mut() {
            for i in &mut columnar.indices {
                *i = typed[*i];
            }
        }
        for (i, _) in &mut decoded.raws {
            *i = typed[*i];
        }
        decoded.raws.extend(row_path(&rows));
        decoded.raws.sort_unstable_by_key(|(i, _)| *i);
        decoded.received = lines.len();
        Ok(decoded)
    }

    /// Text columns parsed into their declared types; rows whose values do
    /// not parse keep their original text and go to the row path, where the
    /// contract rejects them.
    fn decode_text(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        batches: &[RecordBatch],
        has_hook: bool,
    ) -> Result<Decoded, DatalakeError> {
        let batch = match batches {
            [] => {
                return Ok(Decoded {
                    received: 0,
                    columnar: None,
                    raws: Vec::new(),
                })
            }
            [one] => one.clone(),
            [first, ..] => concat_batches(&first.schema(), batches).map_err(|e| {
                DatalakeError::MalformedPayload {
                    reason: format!("cannot combine Arrow batches: {e}"),
                }
            })?,
        };
        let received = batch.num_rows();
        let coerced = codec::coerce_text(contract, &batch)?;
        let mut decoded = if coerced.typed.num_rows() == 0 {
            Decoded {
                received: 0,
                columnar: None,
                raws: Vec::new(),
            }
        } else {
            let typed = RecordSource::Arrow(vec![coerced.typed]);
            self.decode(contract, call, typed, has_hook)?
        };
        // Indices refer to the parsed rows; map them back to the input.
        let original = |i: usize| coerced.kept[i];
        if let Some(columnar) = decoded.columnar.as_mut() {
            for i in &mut columnar.indices {
                *i = original(*i);
            }
        }
        for (i, _) in &mut decoded.raws {
            *i = original(*i);
        }
        if let Some((rest, indices)) = coerced.unparsed {
            decoded.raws.extend(
                indices
                    .into_iter()
                    .zip(codec::arrow_rows(&[rest])?.into_iter().map(Raw::Text)),
            );
            decoded.raws.sort_unstable_by_key(|(i, _)| *i);
        }
        decoded.received = received;
        Ok(decoded)
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
            tenant_id: call.tenant.tenant_id().as_str(),
            protector: call.protector,
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
        let parse = |text: &str| -> Result<(usize, Value), Slot> {
            let bytes = text.len();
            if bytes > max {
                return Err(too_large(bytes));
            }
            serde_json::from_str::<Value>(text)
                .map(|v| (bytes, v))
                .map_err(|e| Slot {
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
                })
        };
        let (bytes, value) = match raw {
            Raw::Line(chunk, range) => match parse(&chunk[range]) {
                Ok(parsed) => parsed,
                Err(slot) => return slot,
            },
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

    /// Levels 5 and 7 for valid records (PII is already protected).
    fn finish(&self, contract: &Contract, call: &CallContext<'_>, slot: &mut Slot) {
        match &mut slot.state {
            State::Rejected(..) => {}
            State::Valid(map) => {
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
                    .or_else(|| codec::check_types(contract, map, call.protector.is_some()));
                if let Some(r) = rejection {
                    let map = std::mem::take(map);
                    slot.state = State::Rejected(r, Some(map));
                }
            }
        }
    }

    fn collect(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        received: usize,
        slots: Vec<Slot>,
        columnar: Option<ColumnarRows>,
    ) -> Result<ChunkOutput, DatalakeError> {
        let (tenant, batch_id, now) = (call.tenant, call.batch_id.as_str(), call.now);
        let pii_protection = tenant.pii_scheme();
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
                            record_index: call.index_offset + slot.index,
                            tenant_id: tenant.tenant_id().to_string(),
                            contract: contract.key().to_owned(),
                            data_type: contract.data_type().to_owned(),
                            validation_level: rejection.level,
                            error_code: rejection.code,
                            error_message: rejection.message,
                            quarantined_at: quarantined_at.clone(),
                            sdk_version: env!("CARGO_PKG_VERSION").to_owned(),
                            payload_bytes: slot.bytes,
                            pii_protection: pii_protection.to_owned(),
                            payload: payload.map(Value::Object),
                        });
                }
            }
        }

        let records_ok = valid.len() + columnar.as_ref().map_or(0, |c| c.indices.len());
        let batch = if records_ok == 0 {
            None
        } else {
            let envelope = Envelope {
                batch_id,
                tenant_id: tenant.tenant_id().as_str(),
                data_type: contract.data_type(),
                schema_version: contract.version(),
                schema_hash: contract.schema_hash(),
                received_at: now,
                pii_protection,
                pii_key_ref: call.protector.and_then(FieldProtector::key_ref),
            };
            let pii_masked = call.protector.is_some();
            let columns = match columnar {
                None => codec::declared_columns(contract, &valid, pii_masked)?,
                Some(fast) if valid.is_empty() => fast.columns,
                Some(fast) => columnar::merge(
                    &fast,
                    &codec::declared_columns(contract, &valid, pii_masked)?,
                    &valid_indices,
                )?,
            };
            Some(codec::finish_batch(
                contract,
                &self.config.envelope_prefix,
                columns,
                &envelope,
                pii_masked,
                records_ok,
            )?)
        };
        Ok(ChunkOutput {
            received,
            records_ok,
            batch,
            dead_letters: by_code,
        })
    }

    /// Objects and report of a single-call ingest.
    fn package(
        &self,
        contract: &Contract,
        call: &CallContext<'_>,
        output: &ChunkOutput,
    ) -> Result<PreparedBatch, DatalakeError> {
        let (tenant, batch_id, now) = (call.tenant, call.batch_id.as_str(), call.now);
        let pii_protection = tenant.pii_scheme();
        let mut dead_letters = Vec::with_capacity(output.dead_letters.len());
        let mut rejections = BTreeMap::new();
        for (code, entries) in &output.dead_letters {
            rejections.insert(code.clone(), entries.len());
            dead_letters.push(PendingObject {
                path: tenant.dlq_path(contract.data_type(), code, &now, batch_id)?,
                body: dlq::encode_ndjson(entries)?,
                content_type: NDJSON_CONTENT_TYPE,
            });
        }
        let encoder = self.encoder_for(contract);
        let data = match (&output.batch, contract.partition_by()) {
            (None, _) => Vec::new(),
            (Some(batch), None) => vec![PendingObject {
                path: tenant.data_path(contract.data_type(), &now, batch_id)?,
                body: encoder.encode(std::slice::from_ref(batch))?,
                content_type: PARQUET_CONTENT_TYPE,
            }],
            // One object per event day, named after the batch and the day so
            // a retry with the same batch id rewrites the same keys.
            (Some(batch), Some(field)) => codec::event_day_groups(batch, field, now)?
                .into_iter()
                .map(|(day, part)| {
                    let file_id = format!("{batch_id}-{}", day.format("%Y%m%d"));
                    Ok(PendingObject {
                        path: tenant.data_path(contract.data_type(), &day, &file_id)?,
                        body: encoder.encode(std::slice::from_ref(&part))?,
                        content_type: PARQUET_CONTENT_TYPE,
                    })
                })
                .collect::<Result<_, DatalakeError>>()?,
        };
        let records_quarantined = rejections.values().sum();
        Ok(PreparedBatch {
            report: IngestReport {
                batch_id: batch_id.to_owned(),
                tenant_id: tenant.tenant_id().to_string(),
                records_received: output.received,
                records_ok: output.records_ok,
                records_quarantined,
                rejections,
                data_uri: data.first().map(|d| d.path.uri.clone()),
                data_uris: data.iter().map(|d| d.path.uri.clone()).collect(),
                dlq_uris: dead_letters.iter().map(|d| d.path.uri.clone()).collect(),
                pii_protection: pii_protection.to_owned(),
            },
            dead_letters,
            data,
        })
    }
}

/// Byte ranges of the non-blank lines of `text`, without line breaks.
fn line_ranges(text: &str) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    for line in text.split_inclusive('\n') {
        let content = line.trim_end_matches(['\n', '\r']);
        if !content.trim().is_empty() {
            out.push(start..start + content.len());
        }
        start += line.len();
    }
    out
}

/// Bytes of JSON Lines parsed per parallel task.
const JSON_PART_BYTES: usize = 1 << 20;

/// Parses newline-delimited JSON objects into one batch of `schema`, parts of
/// about [`JSON_PART_BYTES`] (cut on line breaks) in parallel; `None` when a
/// value does not fit its declared type or a field is undeclared.
fn parse_json_lines(buffer: &[u8], schema: &Arc<arrow::datatypes::Schema>) -> Option<RecordBatch> {
    let mut parts = Vec::new();
    let mut start = 0;
    while start < buffer.len() {
        let mut end = (start + JSON_PART_BYTES).min(buffer.len());
        if end < buffer.len() {
            end = memchr::memchr(b'\n', &buffer[end..]).map_or(buffer.len(), |i| end + i + 1);
        }
        // Blank parts (trailing line breaks) hold no record.
        if !buffer[start..end].iter().all(u8::is_ascii_whitespace) {
            parts.push(&buffer[start..end]);
        }
        start = end;
    }
    let batches: Vec<RecordBatch> = parts
        .par_iter()
        .map(|part| {
            let rows = memchr::memchr_iter(b'\n', part).count() + 1;
            parse_json_part(part, rows, schema)
        })
        .collect::<Option<Vec<_>>>()?;
    match batches.as_slice() {
        [] => None,
        [one] => Some(one.clone()),
        [first, ..] => concat_batches(&first.schema(), &batches).ok(),
    }
}

/// One part of [`parse_json_lines`].
fn parse_json_part(
    buffer: &[u8],
    rows: usize,
    schema: &Arc<arrow::datatypes::Schema>,
) -> Option<RecordBatch> {
    let mut decoder = arrow::json::ReaderBuilder::new(Arc::clone(schema))
        .with_strict_mode(true)
        .with_batch_size(rows.max(1))
        .build_decoder()
        .ok()?;
    let mut offset = 0;
    while offset < buffer.len() {
        let read = decoder.decode(&buffer[offset..]).ok()?;
        if read == 0 {
            break;
        }
        offset += read;
    }
    decoder.flush().ok()?
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
        RecordSource::JsonLines(text) => {
            let text: Arc<str> = Arc::from(text);
            Ok(line_ranges(&text)
                .into_iter()
                .map(|r| Raw::Line(Arc::clone(&text), r))
                .collect())
        }
        RecordSource::Arrow(batches) | RecordSource::ArrowText(batches) => {
            Ok(codec::arrow_rows(&batches)?
                .into_iter()
                .map(Raw::Text)
                .collect())
        }
    }
}

/// Time of a ULID (its first 48 bits).
/// Deterministic batch id for a set of source messages: a ULID whose time is
/// `time` (it also sets the Bronze partition date and the reference of the
/// future-skew check, so use the latest arrival time of the messages) and
/// whose randomness is a SHA-256 of the message ids. The same messages give
/// the same id, so a redelivered batch rewrites the same objects instead of
/// adding rows.
///
/// Use it with [`IngestRequest::with_batch_id`] when consuming a queue or a
/// stream (Kinesis sequence numbers, SQS message ids, Kafka offsets...).
#[must_use]
pub fn batch_id_for<S: AsRef<str>>(message_ids: &[S], time: DateTime<Utc>) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for (i, id) in message_ids.iter().enumerate() {
        if i > 0 {
            hash.update([0x1f]);
        }
        hash.update(id.as_ref().as_bytes());
    }
    let digest = hash.finalize();
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&digest[..10]);
    let ms = u64::try_from(time.timestamp_millis()).unwrap_or(0);
    Ulid::from_parts(ms, u128::from_be_bytes(random)).to_string()
}

pub(crate) fn ulid_time(id: Ulid) -> DateTime<Utc> {
    use chrono::TimeZone;
    let ms = i64::try_from(id.timestamp_ms()).unwrap_or(0);
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
}

/// `delay` scaled by a random factor in [0.5, 1.5).
pub(crate) fn jittered(delay: std::time::Duration) -> std::time::Duration {
    let mut byte = [0u8; 1];
    let _ = getrandom::fill(&mut byte);
    delay.mul_f64(0.5 + f64::from(byte[0]) / 256.0)
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
    use crate::tenant::{Layer, StorageLocation, TenantId};
    use crate::validation::pii::{
        Aes256GcmEncryptor, HmacSha256Hasher, ProtectionContext, Sha256Hasher,
    };
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
        BronzePipeline::new(registry_with(schemas), storage, config).unwrap()
    }

    fn pipeline(storage: &InMemoryStorage) -> BronzePipeline {
        pipeline_with(
            Arc::new(storage.clone()),
            PipelineConfig::default(),
            &[schema()],
        )
    }

    fn profile(protector: Option<Arc<dyn FieldProtector>>) -> Arc<TenantProfile> {
        Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "acme-lake")
                .unwrap()
                .pii_protection(protector)
                .build()
                .unwrap(),
        )
    }

    /// Default tenant: PII is stored as received.
    fn tenant() -> Arc<TenantProfile> {
        profile(None)
    }

    /// Tenant that hashes PII with SHA-256.
    fn hashing_tenant() -> Arc<TenantProfile> {
        profile(Some(Arc::new(Sha256Hasher)))
    }

    fn request(records: Vec<Value>) -> IngestRequest {
        IngestRequest::new(tenant(), CONTRACT, RecordSource::Values(records))
    }

    fn hashed_request(records: Vec<Value>) -> IngestRequest {
        IngestRequest::new(hashing_tenant(), CONTRACT, RecordSource::Values(records))
    }

    fn read_parquet(storage: &InMemoryStorage, uri: &str) -> RecordBatch {
        let body = storage
            .get(uri.strip_prefix("s3://").unwrap())
            .unwrap()
            .body;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(body)
            .unwrap()
            .build()
            .unwrap();
        reader.next().unwrap().unwrap()
    }

    fn read_dlq(storage: &InMemoryStorage, uri: &str) -> Vec<DeadLetter> {
        let body = storage
            .get(uri.strip_prefix("s3://").unwrap())
            .unwrap()
            .body;
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
        // Constant lineage columns are dictionaries in memory but plain strings on disk.
        for name in [
            "_dl_tenant_id",
            "_dl_schema_hash",
            "_dl_pii_protection",
            "_dl_pii_key_ref",
        ] {
            assert_eq!(
                batch.schema().field_with_name(name).unwrap().data_type(),
                &DataType::Utf8
            );
        }
        assert!(matches!(
            batch
                .schema()
                .field_with_name("_dl_received_at")
                .unwrap()
                .data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_))
        ));
        let key = uri.strip_prefix("s3://").unwrap();
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
        assert!(schema_dlq.starts_with("s3://acme-lake/quarantine/tenant_id=acme-001/"));
        let entries = read_dlq(&storage, schema_dlq);
        assert_eq!(
            entries.iter().map(|e| e.record_index).collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(entries.iter().all(|e| e.validation_level == level::SCHEMA));
    }

    #[tokio::test]
    async fn pii_never_reaches_storage_in_clear_when_protected() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(hashed_request(vec![
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
        assert_eq!(email.value(0), Sha256Hasher::hash(EMAIL));
        let dead = read_dlq(&storage, &report.dlq_uris[0]);
        assert!(dead.iter().all(|d| d.pii_protection == "sha256"));
    }

    #[tokio::test]
    async fn hmac_hasher_is_applied() {
        let storage = InMemoryStorage::new();
        let hasher = HmacSha256Hasher::new(&[7u8; 32]).unwrap();
        let report = pipeline(&storage)
            .ingest(IngestRequest::new(
                profile(Some(Arc::new(hasher.clone()))),
                CONTRACT,
                RecordSource::Values(vec![json!({"event_id": "e", "user_email": EMAIL})]),
            ))
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
                hashing_tenant(),
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
            .ingest(hashed_request(vec![
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
        let report = pipeline(&storage)
            .ingest(hashed_request(records))
            .await
            .unwrap();
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
    async fn pii_is_stored_as_received_by_default() {
        let storage = InMemoryStorage::new();
        let report = pipeline(&storage)
            .ingest(request(vec![
                json!({"event_id": "ok", "user_email": EMAIL}),
                json!({"event_id": 1, "user_email": EMAIL}),
            ]))
            .await
            .unwrap();
        assert_eq!(report.pii_protection, "none");
        let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
        assert_eq!(string_column(&batch, "user_email"), [EMAIL]);
        assert_eq!(string_column(&batch, "_dl_pii_protection"), ["none"]);
        assert_eq!(
            batch
                .column_by_name("_dl_pii_key_ref")
                .unwrap()
                .null_count(),
            1
        );
        let dead = read_dlq(&storage, &report.dlq_uris[0]);
        assert_eq!(dead[0].payload.as_ref().unwrap()["user_email"], EMAIL);
        assert_eq!(dead[0].pii_protection, "none");
        assert!(!dead[0].error_message.contains(EMAIL));
    }

    #[tokio::test]
    async fn each_call_writes_to_its_tenants_destination_with_its_protection() {
        let storage = InMemoryStorage::new();
        let p = pipeline(&storage);
        let aes = Arc::new(Aes256GcmEncryptor::new(&[5u8; 32], "kms-blob-acme").unwrap());
        let acme = Arc::new(
            TenantProfile::builder(TenantId::new("acme-001").unwrap())
                .location(
                    Layer::Bronze,
                    StorageLocation::parse("s3://acme-raw/lake/bronze").unwrap(),
                )
                .location(
                    Layer::Quarantine,
                    StorageLocation::parse("s3://acme-raw/lake/quarantine").unwrap(),
                )
                .pii_protection(Some(Arc::clone(&aes) as Arc<dyn FieldProtector>))
                .build()
                .unwrap(),
        );
        let beta = Arc::new(
            TenantProfile::for_bucket(TenantId::new("beta-002").unwrap(), "beta-lake")
                .unwrap()
                .build()
                .unwrap(),
        );
        let records = || {
            RecordSource::Values(vec![
                json!({"event_id": "e", "user_email": EMAIL}),
                json!({"event_id": 1, "user_email": EMAIL}),
            ])
        };
        let a = p
            .ingest(IngestRequest::new(acme, CONTRACT, records()))
            .await
            .unwrap();
        let b = p
            .ingest(IngestRequest::new(beta, CONTRACT, records()))
            .await
            .unwrap();

        let a_uri = a.data_uri.as_ref().unwrap();
        assert!(a_uri.starts_with("s3://acme-raw/lake/bronze/tenant_id=acme-001/"));
        assert!(a.dlq_uris[0].starts_with("s3://acme-raw/lake/quarantine/tenant_id=acme-001/"));
        assert!(b
            .data_uri
            .as_ref()
            .unwrap()
            .starts_with("s3://beta-lake/bronze/tenant_id=beta-002/"));
        assert_eq!(
            (a.tenant_id.as_str(), b.tenant_id.as_str()),
            ("acme-001", "beta-002")
        );
        assert_eq!(
            (a.pii_protection.as_str(), b.pii_protection.as_str()),
            ("aes-256-gcm", "none")
        );

        let ctx = ProtectionContext {
            tenant_id: "acme-001",
            field: "user_email",
        };
        let a_batch = read_parquet(&storage, a_uri);
        let token = &string_column(&a_batch, "user_email")[0];
        assert!(!token.contains(EMAIL));
        assert_eq!(aes.decrypt(&ctx, token).unwrap(), EMAIL);
        assert_eq!(
            string_column(&a_batch, "_dl_pii_key_ref"),
            ["kms-blob-acme"]
        );
        let dead = read_dlq(&storage, &a.dlq_uris[0]);
        let dead_token = dead[0].payload.as_ref().unwrap()["user_email"]
            .as_str()
            .unwrap();
        assert_eq!(aes.decrypt(&ctx, dead_token).unwrap(), EMAIL);
        let beta_ctx = ProtectionContext {
            tenant_id: "beta-002",
            field: "user_email",
        };
        assert!(
            aes.decrypt(&beta_ctx, token).is_err(),
            "bound to its tenant"
        );

        let b_batch = read_parquet(&storage, b.data_uri.as_ref().unwrap());
        assert_eq!(string_column(&b_batch, "user_email"), [EMAIL]);
        let acme_keys = storage
            .keys()
            .into_iter()
            .filter(|k| k.starts_with("acme-raw/"))
            .count();
        let beta_keys = storage
            .keys()
            .into_iter()
            .filter(|k| k.starts_with("beta-lake/"))
            .count();
        assert_eq!((acme_keys, beta_keys), (2, 2));
    }

    /// Provider that counts issued keys; key n has bytes n+1.
    #[derive(Debug, Default)]
    struct CountingProvider(std::sync::atomic::AtomicUsize);

    impl crate::validation::keys::DataKeyProvider for CountingProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn name(&self) -> &str {
            "counting"
        }
        fn generate(
            &self,
            tenant_id: &str,
        ) -> Result<crate::validation::keys::DataKey, DatalakeError> {
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            #[allow(clippy::cast_possible_truncation)]
            Ok(crate::validation::keys::DataKey {
                plaintext: zeroize::Zeroizing::new(vec![n as u8 + 1; 32]),
                key_ref: format!("count:{tenant_id}:{n}"),
            })
        }
        fn decrypt(
            &self,
            _: &str,
            key_ref: &str,
        ) -> Result<zeroize::Zeroizing<Vec<u8>>, DatalakeError> {
            let n: u8 = key_ref.rsplit(':').next().unwrap().parse().unwrap();
            Ok(zeroize::Zeroizing::new(vec![n + 1; 32]))
        }
    }

    #[tokio::test]
    async fn envelope_protection_reuses_one_data_key_per_tenant() {
        use crate::validation::keys::{EnvelopeProtector, KeyCachePolicy, PiiDecryptor};
        let storage = InMemoryStorage::new();
        let p = pipeline(&storage);
        let provider = Arc::new(CountingProvider::default());
        let envelope = Arc::new(
            EnvelopeProtector::new(
                Arc::clone(&provider) as Arc<dyn crate::validation::keys::DataKeyProvider>,
                KeyCachePolicy::default(),
            )
            .unwrap(),
        );
        let tenant = profile(Some(envelope as Arc<dyn FieldProtector>));
        let mut key_refs = Vec::new();
        let mut tokens = Vec::new();
        for i in 0..3 {
            let report = p
                .ingest(IngestRequest::new(
                    Arc::clone(&tenant),
                    CONTRACT,
                    RecordSource::Values(vec![
                        json!({"event_id": format!("e{i}"), "user_email": EMAIL}),
                        json!({"event_id": i, "user_email": EMAIL}),
                    ]),
                ))
                .await
                .unwrap();
            assert_eq!(report.pii_protection, "aes-256-gcm");
            let batch = read_parquet(&storage, report.data_uri.as_ref().unwrap());
            key_refs.extend(string_column(&batch, "_dl_pii_key_ref"));
            tokens.extend(string_column(&batch, "user_email"));
            let dead = read_dlq(&storage, &report.dlq_uris[0]);
            assert!(dead[0].payload.as_ref().unwrap()["user_email"]
                .as_str()
                .unwrap()
                .starts_with("v1:"));
        }
        assert_eq!(
            provider.0.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one data key for three calls"
        );
        assert!(key_refs.iter().all(|k| k == &key_refs[0]));
        assert!(!contains(&all_bytes(&storage), EMAIL));
        let reader = PiiDecryptor::new(provider, 4, std::time::Duration::from_secs(60)).unwrap();
        for token in &tokens {
            assert_eq!(
                reader
                    .decrypt("acme-001", "user_email", &key_refs[0], token)
                    .unwrap(),
                EMAIL
            );
        }
    }

    #[derive(Debug)]
    struct Reversing;
    impl FieldProtector for Reversing {
        #[allow(clippy::unnecessary_literal_bound)]
        fn scheme(&self) -> &str {
            "test-reverse"
        }
        fn key_ref(&self) -> Option<&str> {
            Some("key-7")
        }
        fn protect(&self, _: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError> {
            Ok(value.chars().rev().collect())
        }
    }

    #[derive(Debug)]
    struct Failing;
    impl FieldProtector for Failing {
        #[allow(clippy::unnecessary_literal_bound)]
        fn scheme(&self) -> &str {
            "test-fail"
        }
        fn protect(&self, _: &ProtectionContext<'_>, _: &str) -> Result<String, DatalakeError> {
            Err(DatalakeError::protection("key service unavailable"))
        }
    }

    #[tokio::test]
    async fn custom_protectors_are_used_and_failures_write_nothing() {
        let storage = InMemoryStorage::new();
        let p = pipeline(&storage);
        let records = || {
            RecordSource::Values(vec![
                json!({"event_id": "e", "user_email": "abc"}),
                json!({}),
            ])
        };
        let ok = p
            .ingest(IngestRequest::new(
                profile(Some(Arc::new(Reversing))),
                CONTRACT,
                records(),
            ))
            .await
            .unwrap();
        assert_eq!(ok.pii_protection, "test-reverse");
        let batch = read_parquet(&storage, ok.data_uri.as_ref().unwrap());
        assert_eq!(string_column(&batch, "user_email"), ["cba"]);
        assert_eq!(string_column(&batch, "_dl_pii_key_ref"), ["key-7"]);

        let empty = InMemoryStorage::new();
        let err = pipeline(&empty)
            .ingest(IngestRequest::new(
                profile(Some(Arc::new(Failing))),
                CONTRACT,
                records(),
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, DatalakeError::Protection { .. }));
        assert!(
            empty.is_empty(),
            "nothing may be written when protection fails"
        );
    }

    #[derive(Default)]
    struct FlakyStorage {
        calls: Mutex<Vec<String>>,
        fail_on_parquet: bool,
    }

    #[async_trait]
    impl ObjectStorage for FlakyStorage {
        async fn put(
            &self,
            path: &ObjectPath,
            _: Bytes,
            content_type: &str,
        ) -> Result<(), DatalakeError> {
            self.calls.lock().unwrap().push(path.key.clone());
            if self.fail_on_parquet && content_type == PARQUET_CONTENT_TYPE {
                return Err(DatalakeError::storage("s3", "unavailable"));
            }
            Ok(())
        }
    }

    /// Fails the first `failures` writes, then stores.
    struct FailFirst {
        failures: Mutex<usize>,
        inner: InMemoryStorage,
    }

    #[async_trait]
    impl ObjectStorage for FailFirst {
        async fn put(
            &self,
            path: &ObjectPath,
            body: Bytes,
            content_type: &str,
        ) -> Result<(), DatalakeError> {
            {
                let mut left = self.failures.lock().unwrap();
                if *left > 0 {
                    *left -= 1;
                    return Err(DatalakeError::storage("s3", "throttled"));
                }
            }
            self.inner.put(path, body, content_type).await
        }
    }

    #[tokio::test]
    async fn commit_can_be_retried_without_duplicates() {
        let inner = InMemoryStorage::new();
        let storage = Arc::new(FailFirst {
            failures: Mutex::new(1),
            inner: inner.clone(),
        });
        let p = pipeline_with(storage, PipelineConfig::default(), &[schema()]);
        let prepared = p
            .prepare(request(vec![json!({"event_id": "ok"}), json!({})]))
            .unwrap();
        assert!(p.commit(&prepared).await.is_err());
        let report = p.commit(&prepared).await.unwrap();
        assert_eq!(&report, prepared.report());
        // Same keys rewritten: one dead-letter object and one data object.
        assert_eq!(inner.len(), 2);
        assert_eq!(read_dlq(&inner, &report.dlq_uris[0]).len(), 1);
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
        assert!(calls[0].starts_with("quarantine/"));
        assert!(calls[1].starts_with("bronze/"));
    }

    #[test]
    fn contract_fields_cannot_use_the_envelope_prefix() {
        let mut s = schema();
        s["properties"]["_dl_custom"] = json!({"type": "string"});
        let result = BronzePipeline::new(
            registry_with(&[s]),
            Arc::new(InMemoryStorage::new()),
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
    use crate::tenant::TenantId;
    use crate::validation::pii::{
        Aes256GcmEncryptor, HmacSha256Hasher, ProtectionContext, Sha256Hasher,
    };
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray,
        StringArray, StringViewArray, UInt64Array,
    };
    use arrow::array::{AsArray, RecordBatchOptions};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
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
            arrow_fast_path: fast,
            max_record_bytes: case.max_bytes.unwrap_or(DEFAULT_MAX_RECORD_BYTES),
            ..PipelineConfig::default()
        };
        BronzePipeline::new(Arc::new(registry), Arc::new(InMemoryStorage::new()), config).unwrap()
    }

    /// Tenant of a case: PII off, SHA-256 or HMAC (deterministic, so both
    /// paths must produce identical bytes; AES is checked separately).
    fn tenant_for(case: &Case) -> Arc<TenantProfile> {
        let protector: Option<Arc<dyn FieldProtector>> = match (case.mask, case.hmac) {
            (false, _) => None,
            (true, false) => Some(Arc::new(Sha256Hasher)),
            (true, true) => Some(Arc::new(
                HmacSha256Hasher::new(b"0123456789abcdef0123456789abcdef").unwrap(),
            )),
        };
        Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "eq-lake")
                .unwrap()
                .pii_protection(protector)
                .build()
                .unwrap(),
        )
    }

    /// Everything in a prepared batch that does not depend on ids or time.
    #[derive(Debug, PartialEq)]
    struct Outcome {
        received: usize,
        ok: usize,
        quarantined: usize,
        rejections: BTreeMap<String, usize>,
        pii_protection: String,
        data: Option<RecordBatch>,
        dead_letters: Vec<Value>,
    }

    fn outcome(prepared: PreparedBatch) -> Outcome {
        let data = prepared.data.into_iter().next().map(|d| {
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
            pii_protection: r.pii_protection,
            data,
            dead_letters,
        }
    }

    fn run(case: &Case, fast: bool) -> Result<Outcome, String> {
        let request = IngestRequest::new(
            tenant_for(case),
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

    /// Replaces AES tokens of the PII columns (Parquet and dead letters) with
    /// their plaintext, failing on any token that does not decrypt.
    fn decrypted(mut o: Outcome, enc: &Aes256GcmEncryptor) -> Outcome {
        let open = |field: &str, token: &str| {
            enc.decrypt(
                &ProtectionContext {
                    tenant_id: "acme-001",
                    field,
                },
                token,
            )
            .unwrap()
        };
        if let Some(batch) = o.data.take() {
            let schema = batch.schema();
            let columns: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(f, c)| {
                    if ["mail", "pnum"].contains(&f.name().as_str()) {
                        let tokens = c.as_string::<i32>();
                        Arc::new(StringArray::from_iter(
                            tokens.iter().map(|t| t.map(|t| open(f.name(), t))),
                        )) as ArrayRef
                    } else {
                        Arc::clone(c)
                    }
                })
                .collect();
            o.data = Some(RecordBatch::try_new(schema, columns).unwrap());
        }
        for entry in &mut o.dead_letters {
            if let Some(payload) = entry.get_mut("payload").and_then(Value::as_object_mut) {
                for field in ["mail", "pnum"] {
                    if let Some(Value::String(token)) = payload.get(field).cloned() {
                        payload.insert(field.into(), Value::String(open(field, &token)));
                    }
                }
            }
        }
        o
    }

    #[test]
    fn aes_paths_agree_after_decryption() {
        let enc = Arc::new(Aes256GcmEncryptor::new(&[9u8; 32], "kms-ref").unwrap());
        let tenant = Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "eq-lake")
                .unwrap()
                .pii_protection(Some(Arc::clone(&enc) as Arc<dyn FieldProtector>))
                .build()
                .unwrap(),
        );
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        for _ in 0..48 {
            let case = case().new_tree(&mut runner).unwrap().current();
            let run_with = |fast: bool| {
                pipeline(&case, fast)
                    .prepare(IngestRequest::new(
                        Arc::clone(&tenant),
                        CONTRACT,
                        RecordSource::Arrow(vec![batch(&case)]),
                    ))
                    .map(|p| decrypted(outcome(p), &enc))
                    .map_err(|e| e.to_string())
            };
            assert_eq!(run_with(true), run_with(false), "{case:?}");
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
        let tenant = tenant_for(&case);
        let call = CallContext {
            tenant: &tenant,
            batch_id: String::new(),
            now: Utc::now(),
            protector: tenant.protector(),
            index_offset: 0,
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
        let tenant = tenant_for(&case);
        let call = CallContext {
            tenant: &tenant,
            batch_id: String::new(),
            now: Utc::now(),
            protector: None,
            index_offset: 0,
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

#[cfg(test)]
mod storage_option_tests {
    //! Contract storage options: compression and typed timestamps.
    use super::*;
    use crate::storage::InMemoryStorage;
    use crate::tenant::TenantId;
    use crate::validation::pii::Sha256Hasher;
    use arrow::array::{AsArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use serde_json::json;

    const CONTRACT: &str = "app/log/1.0.0";

    fn schema(extra: &Value) -> Value {
        let mut s = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "x-dl-domain": "app",
            "x-dl-datatype": "log",
            "x-dl-schema_version": "1.0.0",
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "at": {"type": "string", "format": "date-time", "x-dl-type": "timestamp"},
                "user": {"type": "string", "format": "date-time", "x-dl-type": "timestamp", "x-dl-pii": true}
            }
        });
        if let (Some(base), Some(more)) = (s.as_object_mut(), extra.as_object()) {
            for (k, v) in more {
                base.insert(k.clone(), v.clone());
            }
        }
        s
    }

    fn pipeline(storage: &InMemoryStorage, schema: &Value) -> BronzePipeline {
        let mut r = ContractRegistry::default();
        r.register(schema).unwrap();
        BronzePipeline::new(
            Arc::new(r),
            Arc::new(storage.clone()),
            PipelineConfig::default(),
        )
        .unwrap()
    }

    fn tenant(hash: bool) -> Arc<TenantProfile> {
        let p: Option<Arc<dyn FieldProtector>> = if hash {
            Some(Arc::new(Sha256Hasher))
        } else {
            None
        };
        Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "acme-lake")
                .unwrap()
                .pii_protection(p)
                .build()
                .unwrap(),
        )
    }

    fn body(storage: &InMemoryStorage, uri: &str) -> bytes::Bytes {
        storage
            .get(uri.strip_prefix("s3://").unwrap())
            .unwrap()
            .body
    }

    fn batch(storage: &InMemoryStorage, uri: &str) -> RecordBatch {
        ParquetRecordBatchReaderBuilder::try_new(body(storage, uri))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
    }

    #[test]
    fn contract_storage_keywords_are_validated() {
        for bad in [
            json!({"x-dl-compression": "brotli"}),
            json!({"x-dl-compression": "snappy", "x-dl-compression_level": 3}),
            json!({"x-dl-compression": "zstd", "x-dl-compression_level": 30}),
            json!({"x-dl-compression": 1}),
        ] {
            let mut r = ContractRegistry::default();
            assert!(r.register(&schema(&bad)).is_err(), "{bad}");
        }
        let mut wrong_field = schema(&json!({}));
        wrong_field["properties"]["id"]["x-dl-type"] = json!("timestamp");
        assert!(ContractRegistry::default().register(&wrong_field).is_err());
        let mut r = ContractRegistry::default();
        r.register(&schema(
            &json!({"x-dl-compression": "zstd", "x-dl-compression_level": 7}),
        ))
        .unwrap();
        assert_eq!(
            r.get(CONTRACT).unwrap().compression(),
            crate::contracts::BronzeCompression::Zstd(7)
        );
        let mut r = ContractRegistry::default();
        r.register(&schema(&json!({"x-dl-compression": "zstd"})))
            .unwrap();
        assert_eq!(
            r.get(CONTRACT).unwrap().compression(),
            crate::contracts::BronzeCompression::Zstd(1),
            "zstd defaults to level 1"
        );
    }

    #[tokio::test]
    async fn timestamps_are_stored_typed_on_both_paths() {
        let rows = vec![
            json!({"id": "a", "at": "2026-03-01T10:00:00Z"}),
            json!({"id": "b", "at": "2026-03-01T12:30:00.123456+02:00"}),
            json!({"id": "c"}),
        ];
        let storage = InMemoryStorage::new();
        let p = pipeline(&storage, &schema(&json!({})));
        let json_report = p
            .ingest(IngestRequest::new(
                tenant(false),
                CONTRACT,
                RecordSource::Values(rows),
            ))
            .await
            .unwrap();
        let arrow_input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("at", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec![
                    Some("2026-03-01T10:00:00Z"),
                    Some("2026-03-01T12:30:00.123456+02:00"),
                    None,
                ])),
            ],
        )
        .unwrap();
        let arrow_report = p
            .ingest(IngestRequest::new(
                tenant(false),
                CONTRACT,
                RecordSource::Arrow(vec![arrow_input]),
            ))
            .await
            .unwrap();
        let read = |uri: &str| -> Vec<Option<i64>> {
            let b = batch(&storage, uri);
            let col = b.column_by_name("at").unwrap();
            assert_eq!(
                col.data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            );
            col.as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect()
        };
        let expected = vec![
            Some(1_772_359_200_000_000),
            Some(1_772_361_000_123_456),
            None,
        ];
        assert_eq!(read(json_report.data_uri.as_deref().unwrap()), expected);
        assert_eq!(read(arrow_report.data_uri.as_deref().unwrap()), expected);
    }

    #[tokio::test]
    async fn unparseable_timestamps_are_quarantined_and_protected_pii_stays_text() {
        let storage = InMemoryStorage::new();
        let p = pipeline(
            &storage,
            &schema(&json!({"x-dl-flexibility_mode": "schema_on_read"})),
        );
        let r = p
            .ingest(IngestRequest::new(
                tenant(false),
                CONTRACT,
                RecordSource::Values(vec![
                    json!({"id": "a", "at": "2026-03-01T10:00:00Z"}),
                    json!({"id": "b", "at": "yesterday"}),
                ]),
            ))
            .await
            .unwrap();
        assert_eq!((r.records_ok, r.records_quarantined), (1, 1));
        assert_eq!(r.rejections.get(codes::TYPE_COERCION).copied(), Some(1));

        let hashed = p
            .ingest(IngestRequest::new(
                tenant(true),
                CONTRACT,
                RecordSource::Values(vec![json!({"id": "a", "user": "2026-03-01T10:00:00Z"})]),
            ))
            .await
            .unwrap();
        let b = batch(&storage, hashed.data_uri.as_deref().unwrap());
        assert_eq!(
            b.column_by_name("user").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[tokio::test]
    async fn zstd_objects_record_their_codec() {
        let storage = InMemoryStorage::new();
        let p = pipeline(
            &storage,
            &schema(&json!({"x-dl-compression": "zstd", "x-dl-compression_level": 3})),
        );
        let r = p
            .ingest(IngestRequest::new(
                tenant(false),
                CONTRACT,
                RecordSource::Values(vec![json!({"id": "a", "at": "2026-03-01T10:00:00Z"})]),
            ))
            .await
            .unwrap();
        let reader =
            SerializedFileReader::new(body(&storage, r.data_uri.as_deref().unwrap())).unwrap();
        let codec = reader.metadata().row_group(0).column(0).compression();
        assert!(
            matches!(codec, parquet::basic::Compression::ZSTD(_)),
            "{codec:?}"
        );
    }
}

#[cfg(test)]
mod batch_id_tests {
    use super::*;

    #[test]
    fn batch_ids_are_deterministic_ulids_at_the_given_time() {
        use chrono::TimeZone;
        let t = Utc.with_ymd_and_hms(2026, 3, 1, 10, 0, 0).unwrap();
        let a = batch_id_for(&["seq-1", "seq-2"], t);
        assert_eq!(
            a,
            batch_id_for(&["seq-1", "seq-2"], t),
            "same messages, same id"
        );
        assert_ne!(a, batch_id_for(&["seq-1", "seq-3"], t));
        assert_ne!(a, batch_id_for(&["seq-1seq-2"], t), "ids are separated");
        let parsed = Ulid::from_string(&a).unwrap();
        assert_eq!(ulid_time(parsed), t);
    }
}

#[cfg(test)]
mod partition_tests {
    //! Event-time partitioning (`x-dl-partition_by`).
    use super::*;
    use crate::storage::InMemoryStorage;
    use crate::tenant::TenantId;
    use serde_json::json;

    const CONTRACT: &str = "app/log/1.0.0";

    fn pipeline(storage: &InMemoryStorage, timestamp: bool) -> BronzePipeline {
        let mut at = json!({"type": "string", "format": "date-time"});
        if timestamp {
            at["x-dl-type"] = json!("timestamp");
        }
        let mut r = ContractRegistry::default();
        r.register(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "x-dl-domain": "app",
            "x-dl-datatype": "log",
            "x-dl-schema_version": "1.0.0",
            "x-dl-partition_by": "at",
            "required": ["id"],
            "properties": {"id": {"type": "string"}, "at": at}
        }))
        .unwrap();
        let config = PipelineConfig {
            max_future_skew: None,
            ..PipelineConfig::default()
        };
        BronzePipeline::new(Arc::new(r), Arc::new(storage.clone()), config).unwrap()
    }

    fn tenant() -> Arc<TenantProfile> {
        Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "acme-lake")
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    fn rows() -> Vec<Value> {
        vec![
            json!({"id": "a", "at": "2026-03-01T23:59:59Z"}),
            json!({"id": "b", "at": "2026-03-02T00:00:00+00:00"}),
            json!({"id": "c", "at": "2026-03-01T22:00:00-05:00"}), // 03-02 UTC
            json!({"id": "d"}),
        ]
    }

    #[test]
    fn partition_by_must_name_a_date_time_field() {
        let mut r = ContractRegistry::default();
        let bad = json!({
            "type": "object", "x-dl-domain": "app", "x-dl-datatype": "log",
            "x-dl-schema_version": "1.0.0", "x-dl-partition_by": "id",
            "properties": {"id": {"type": "string"}}
        });
        assert!(r.register(&bad).is_err());
    }

    #[tokio::test]
    async fn one_object_per_event_day_with_stable_keys() {
        for timestamp in [false, true] {
            let storage = InMemoryStorage::new();
            let p = pipeline(&storage, timestamp);
            let id = Ulid::generate().to_string();
            let report = p
                .ingest(
                    IngestRequest::new(tenant(), CONTRACT, RecordSource::Values(rows()))
                        .with_batch_id(id.clone()),
                )
                .await
                .unwrap();
            assert_eq!(report.records_ok, 4);
            let today = ulid_time(Ulid::from_string(&id).unwrap())
                .format("%Y%m%d")
                .to_string();
            let mut expected = vec![
                format!("year=2026/month=03/day=01/{id}-20260301.parquet"),
                format!("year=2026/month=03/day=02/{id}-20260302.parquet"),
            ];
            if today != "20260301" && today != "20260302" {
                expected.push(format!("{id}-{today}.parquet"));
            }
            assert_eq!(
                report.data_uris.len(),
                expected.len(),
                "{:?}",
                report.data_uris
            );
            for (uri, tail) in report.data_uris.iter().zip(&expected) {
                assert!(uri.ends_with(tail), "{uri} vs {tail}");
            }
            assert_eq!(report.data_uri.as_ref(), report.data_uris.first());
            let keys = storage.keys();
            let again = p
                .ingest(
                    IngestRequest::new(tenant(), CONTRACT, RecordSource::Values(rows()))
                        .with_batch_id(id.clone()),
                )
                .await
                .unwrap();
            assert_eq!(again.data_uris, report.data_uris);
            assert_eq!(storage.keys(), keys, "a retry rewrites the same objects");
        }
    }
}
