//! Streaming ingestion with bounded memory.
//!
//! [`BronzeStream`] ingests input of any size in chunks. Each chunk goes
//! through the same validation as [`BronzePipeline::prepare`]; accepted rows
//! are appended to a Parquet object that is uploaded in parts while the next
//! chunk is validated, and a new object starts when the current one reaches
//! [`StreamOptions::target_object_bytes`]. Memory stays bounded by one chunk,
//! one row group and the parts in flight, whatever the input size.
//!
//! ```text
//! caller thread (CPU)                         upload task (async I/O)
//! push(chunk) ─ validate ─ encode row group ─┬─▶ UploadPart ×N (concurrent)
//!                                            └─▶ CompleteMultipartUpload
//! ```
//!
//! Objects become visible only when complete, under keys derived from the
//! stream id: `<stream_id>-00000.parquet`, `-00001`, … Running the same input
//! again with the same [`StreamOptions::stream_id`] rewrites the same keys,
//! so a failed stream can be retried without duplicating rows. Incomplete
//! multipart uploads are aborted on failure; configure an S3 lifecycle rule
//! (`AbortIncompleteMultipartUpload`) as a safety net for crashed processes.
//!
//! [`BronzeStream::push`] and [`BronzeStream::finish`] block the calling
//! thread (validation and back-pressure), so call them from a plain thread or
//! from `tokio::task::spawn_blocking`, never from inside an async task; or
//! use [`BronzePipeline::ingest_stream`], which does that for you.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex, PoisonError};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use ulid::Ulid;

use crate::contracts::Contract;
use crate::dlq;
use crate::error::DatalakeError;
use crate::pipeline::{
    jittered, ulid_time, BronzePipeline, CallContext, ChunkOutput, RecordSource,
};
use crate::storage::parquet::ParallelWriter;
use crate::storage::{ObjectStorage, NDJSON_CONTENT_TYPE, PARQUET_CONTENT_TYPE};
use crate::tenant::{ObjectPath, TenantProfile};
use crate::validation::hooks::RecordHook;

const MIB: usize = 1024 * 1024;
/// S3's minimum size for every part but the last.
const MIN_PART_BYTES: usize = 5 * MIB;

/// Settings of a stream. The defaults suit S3 and most engines that read the
/// result (objects of about 256 MiB, 16 MiB parts, 4 parts in flight).
#[derive(Debug, Clone)]
pub struct StreamOptions {
    /// ULID that names the stream's objects and fills `_dl_batch_id`. Reuse
    /// the id of a failed stream to retry it over the same keys. A new id is
    /// generated when `None`.
    pub stream_id: Option<String>,
    /// Size at which a Parquet object is closed and the next one started.
    pub target_object_bytes: usize,
    /// Size of each uploaded part (at least 5 MiB).
    pub part_bytes: usize,
    /// Accepted rows held, as Arrow memory, before they are encoded as one
    /// Parquet row group (columns in parallel). Row groups also stop at the
    /// writer's row limit (1 Mi rows).
    pub row_group_bytes: usize,
    /// Arrow rows gathered before a chunk is validated; larger chunks
    /// amortize per-chunk work, smaller ones use less memory.
    pub chunk_rows: usize,
    /// Parts uploaded concurrently (and the number queued behind them).
    pub max_inflight_parts: usize,
    /// Buffered dead-letter bytes that trigger writing quarantine objects.
    pub dead_letter_bytes: usize,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            stream_id: None,
            target_object_bytes: 256 * MIB,
            part_bytes: 16 * MIB,
            row_group_bytes: 64 * MIB,
            chunk_rows: 131_072,
            max_inflight_parts: 4,
            dead_letter_bytes: 8 * MIB,
        }
    }
}

impl StreamOptions {
    fn validate(&self) -> Result<(), DatalakeError> {
        // Tests use tiny parts on the in-memory store; S3 enforces 5 MiB.
        if self.part_bytes < MIN_PART_BYTES && !cfg!(test) {
            return Err(DatalakeError::config("part_bytes must be at least 5 MiB"));
        }
        if self.target_object_bytes < self.part_bytes {
            return Err(DatalakeError::config(
                "target_object_bytes must be at least part_bytes",
            ));
        }
        if self.row_group_bytes == 0 || self.chunk_rows == 0 || self.dead_letter_bytes == 0 {
            return Err(DatalakeError::config(
                "row_group_bytes, chunk_rows and dead_letter_bytes must be greater than zero",
            ));
        }
        if !(1..=64).contains(&self.max_inflight_parts) {
            return Err(DatalakeError::config(
                "max_inflight_parts must be between 1 and 64",
            ));
        }
        Ok(())
    }
}

/// Outcome of a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReport {
    /// ULID of the stream; prefix of every object it wrote.
    pub stream_id: String,
    /// Tenant the records were written for.
    pub tenant_id: String,
    /// Records submitted.
    pub records_received: usize,
    /// Records written to Bronze.
    pub records_ok: usize,
    /// Records written to quarantine objects.
    pub records_quarantined: usize,
    /// Quarantined records per error code.
    pub rejections: BTreeMap<String, usize>,
    /// URIs of the Parquet objects, in order.
    pub data_uris: Vec<String>,
    /// URIs of the quarantine objects.
    pub dlq_uris: Vec<String>,
    /// Bytes uploaded (Parquet and quarantine objects).
    pub bytes_written: u64,
    /// PII scheme of the tenant profile.
    pub pii_protection: String,
}

/// Work for the upload task.
enum Job {
    /// Part `number` of object `object`.
    Part {
        object: usize,
        path: Arc<ObjectPath>,
        number: i32,
        body: Bytes,
    },
    /// All parts of `object` were sent: complete it.
    Complete { object: usize },
    /// A small object written with one request.
    Put {
        path: ObjectPath,
        body: Bytes,
        content_type: &'static str,
    },
}

/// `Write` sink shared with the Parquet writer, drained into parts.
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl Sink {
    fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The first `n` bytes, leaving the rest buffered.
    fn take_front(&self, n: usize) -> Vec<u8> {
        let mut buffer = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let at = n.min(buffer.len());
        let rest = buffer.split_off(at);
        std::mem::replace(&mut *buffer, rest)
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The Parquet object being written.
struct OpenObject {
    index: usize,
    path: Arc<ObjectPath>,
    writer: ParallelWriter<Sink>,
    sink: Sink,
    parts_sent: i32,
    /// Accepted rows not yet encoded: one row group's worth at most.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    pending_bytes: usize,
}

/// An ingest of unbounded input; see the module documentation.
pub struct BronzeStream {
    pipeline: Arc<BronzePipeline>,
    contract: Arc<Contract>,
    tenant: Arc<TenantProfile>,
    hook: Option<Arc<dyn RecordHook>>,
    options: StreamOptions,
    stream_id: String,
    now: DateTime<Utc>,
    runtime: Handle,
    jobs: Option<mpsc::Sender<Job>>,
    uploader: Option<JoinHandle<Result<u64, DatalakeError>>>,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    object: Option<OpenObject>,
    next_object: usize,
    data_uris: Vec<String>,
    dead_letters: BTreeMap<String, Vec<u8>>,
    dead_letter_buffered: usize,
    dlq_objects: usize,
    dlq_uris: Vec<String>,
    received: usize,
    records_ok: usize,
    rejections: BTreeMap<String, usize>,
}

impl BronzePipeline {
    /// Opens a stream for `tenant` and `contract`. Must be called within a
    /// Tokio runtime (uploads run on it).
    ///
    /// # Errors
    ///
    /// [`DatalakeError::UnknownContract`], [`DatalakeError::Configuration`] for
    /// invalid options, an invalid `stream_id` or no runtime.
    pub fn stream(
        self: &Arc<Self>,
        tenant: Arc<TenantProfile>,
        contract: &str,
        hook: Option<Arc<dyn RecordHook>>,
        options: StreamOptions,
    ) -> Result<BronzeStream, DatalakeError> {
        options.validate()?;
        let contract = Arc::clone(self.registry().get(contract)?);
        let (stream_id, now) = match &options.stream_id {
            None => {
                let id = Ulid::generate();
                (id.to_string(), ulid_time(id))
            }
            Some(text) => {
                let id = Ulid::from_string(text).map_err(|_| {
                    DatalakeError::config(format!("stream_id {text:?} is not a ULID"))
                })?;
                // The partition date comes from the id, so a retry writes
                // the same keys even on another day.
                (id.to_string(), ulid_time(id))
            }
        };
        let runtime = Handle::try_current()
            .map_err(|_| DatalakeError::config("a stream must be opened inside a Tokio runtime"))?;
        let (jobs, receiver) = mpsc::channel(options.max_inflight_parts);
        let uploader = runtime.spawn(upload(
            Arc::clone(self.storage()),
            receiver,
            options.max_inflight_parts,
        ));
        Ok(BronzeStream {
            pipeline: Arc::clone(self),
            contract,
            tenant,
            hook,
            options,
            stream_id,
            now,
            runtime,
            jobs: Some(jobs),
            uploader: Some(uploader),
            pending: Vec::new(),
            pending_rows: 0,
            object: None,
            next_object: 0,
            data_uris: Vec::new(),
            dead_letters: BTreeMap::new(),
            dead_letter_buffered: 0,
            dlq_objects: 0,
            dlq_uris: Vec::new(),
            received: 0,
            records_ok: 0,
            rejections: BTreeMap::new(),
        })
    }

    /// Ingests every chunk of `source` as one stream, on a blocking thread.
    ///
    /// # Errors
    ///
    /// The first error of the source, of validation or of an upload; the
    /// unfinished object is discarded and the report is not produced.
    pub async fn ingest_stream<I>(
        self: &Arc<Self>,
        tenant: Arc<TenantProfile>,
        contract: &str,
        hook: Option<Arc<dyn RecordHook>>,
        source: I,
        options: StreamOptions,
    ) -> Result<StreamReport, DatalakeError>
    where
        I: IntoIterator<Item = Result<RecordSource, DatalakeError>> + Send + 'static,
        I::IntoIter: Send,
    {
        let mut stream = self.stream(tenant, contract, hook, options)?;
        tokio::task::spawn_blocking(move || {
            for chunk in source {
                stream.push(chunk?)?;
            }
            stream.finish()
        })
        .await
        .map_err(|e| DatalakeError::storage("runtime", format!("stream task failed: {e}")))?
    }
}

impl OpenObject {
    /// Encodes the pending rows as one row group.
    fn flush_row_group(&mut self) -> Result<(), DatalakeError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        self.pending_bytes = 0;
        let batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap_or_else(|| unreachable!())
        } else {
            concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| DatalakeError::storage("parquet", e.to_string()))?
        };
        self.writer.write_row_group(&batch)
    }
}

impl BronzeStream {
    /// ULID of this stream.
    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Adds records. Arrow batches are gathered up to
    /// [`StreamOptions::chunk_rows`] rows before validation; other inputs are
    /// validated as they come. Blocks while uploads catch up.
    ///
    /// # Errors
    ///
    /// Validation errors as in [`BronzePipeline::prepare`], or the error of a
    /// failed upload. The stream cannot be used after an error.
    pub fn push(&mut self, records: RecordSource) -> Result<(), DatalakeError> {
        let result = self.push_inner(records);
        if result.is_err() {
            self.shut_down();
        }
        result
    }

    fn push_inner(&mut self, records: RecordSource) -> Result<(), DatalakeError> {
        match records {
            RecordSource::Arrow(batches) => {
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.pending_rows += batch.num_rows();
                    self.pending.push(batch);
                    if self.pending_rows >= self.options.chunk_rows {
                        self.flush_pending()?;
                    }
                }
                Ok(())
            }
            other => {
                // Keep record order: Arrow rows received before go first.
                self.flush_pending()?;
                self.process(other)
            }
        }
    }

    fn flush_pending(&mut self) -> Result<(), DatalakeError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        // One chunk for the columnar screen; batches whose schemas differ are
        // validated one by one.
        let schema = batches[0].schema();
        if batches.iter().all(|b| b.schema() == schema) {
            let one =
                concat_batches(&schema, &batches).map_err(|e| DatalakeError::MalformedPayload {
                    reason: format!("cannot combine Arrow batches: {e}"),
                })?;
            self.process(RecordSource::Arrow(vec![one]))
        } else {
            for batch in batches {
                self.process(RecordSource::Arrow(vec![batch]))?;
            }
            Ok(())
        }
    }

    fn process(&mut self, records: RecordSource) -> Result<(), DatalakeError> {
        // The tenant's current data key for this chunk: long streams follow
        // the key cache policy (rotation by age and nonce budget).
        let bound = match self.tenant.protector_arc() {
            None => None,
            Some(p) => Some(p.bind(self.tenant.tenant_id().as_str())?.unwrap_or(p)),
        };
        let call = CallContext {
            tenant: &self.tenant,
            batch_id: self.stream_id.clone(),
            now: self.now,
            protector: bound.as_deref(),
            index_offset: self.received,
        };
        let output =
            self.pipeline
                .validate_chunk(&self.contract, &call, records, self.hook.as_ref())?;
        self.absorb(output)
    }

    fn absorb(&mut self, output: ChunkOutput) -> Result<(), DatalakeError> {
        self.received += output.received;
        self.records_ok += output.records_ok;
        for (code, entries) in &output.dead_letters {
            *self.rejections.entry(code.clone()).or_default() += entries.len();
            let encoded = dlq::encode_ndjson(entries)?;
            self.dead_letter_buffered += encoded.len();
            self.dead_letters
                .entry(code.clone())
                .or_default()
                .extend_from_slice(&encoded);
        }
        if self.dead_letter_buffered >= self.options.dead_letter_bytes {
            self.flush_dead_letters()?;
        }
        if let Some(batch) = output.batch {
            self.write(&batch)?;
        }
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), DatalakeError> {
        if self.object.is_none() {
            self.object = Some(self.open_object(batch)?);
        }
        let (part_bytes, row_group_bytes, row_group_rows, target) = (
            self.options.part_bytes,
            self.options.row_group_bytes,
            self.pipeline.encoder().max_row_group_rows(),
            self.options.target_object_bytes,
        );
        let mut ready = Vec::new();
        let mut full = false;
        if let Some(object) = self.object.as_mut() {
            object.pending_rows += batch.num_rows();
            object.pending_bytes += batch.get_array_memory_size();
            object.pending.push(batch.clone());
            if object.pending_rows >= row_group_rows || object.pending_bytes >= row_group_bytes {
                object.flush_row_group()?;
            }
            // Equal parts, uploaded concurrently: a row group of several
            // parts goes out in parallel.
            while object.sink.len() >= part_bytes {
                object.parts_sent += 1;
                ready.push(Job::Part {
                    object: object.index,
                    path: Arc::clone(&object.path),
                    number: object.parts_sent,
                    body: Bytes::from(object.sink.take_front(part_bytes)),
                });
            }
            full = object.writer.bytes_written() >= target;
        }
        for job in ready {
            self.send(job)?;
        }
        if full {
            self.close_object()?;
        }
        Ok(())
    }

    fn open_object(&mut self, batch: &RecordBatch) -> Result<OpenObject, DatalakeError> {
        let index = self.next_object;
        self.next_object += 1;
        let file_id = format!("{}-{index:05}", self.stream_id);
        let path = self
            .tenant
            .data_path(self.contract.data_type(), &self.now, &file_id)?;
        let sink = Sink::default();
        let writer = ParallelWriter::try_new(
            sink.clone(),
            batch.schema(),
            self.pipeline.encoder().properties(),
        )?;
        Ok(OpenObject {
            index,
            path: Arc::new(path),
            writer,
            sink,
            parts_sent: 0,
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
        })
    }

    fn close_object(&mut self) -> Result<(), DatalakeError> {
        let Some(mut object) = self.object.take() else {
            return Ok(());
        };
        object.flush_row_group()?;
        let OpenObject {
            index,
            path,
            writer,
            sink,
            parts_sent,
            ..
        } = object;
        writer.close()?;
        let rest = Bytes::from(sink.take());
        self.data_uris.push(path.uri.clone());
        if parts_sent == 0 {
            // Fits in one part: a single PutObject is cheaper.
            let path = ObjectPath::clone(&path);
            return self.send(Job::Put {
                path,
                body: rest,
                content_type: PARQUET_CONTENT_TYPE,
            });
        }
        if !rest.is_empty() {
            self.send(Job::Part {
                object: index,
                path,
                number: parts_sent + 1,
                body: rest,
            })?;
        }
        self.send(Job::Complete { object: index })
    }

    fn flush_dead_letters(&mut self) -> Result<(), DatalakeError> {
        let buffered = std::mem::take(&mut self.dead_letters);
        self.dead_letter_buffered = 0;
        for (code, body) in buffered {
            let file_id = format!("{}-{:05}", self.stream_id, self.dlq_objects);
            self.dlq_objects += 1;
            let path =
                self.tenant
                    .dlq_path(self.contract.data_type(), &code, &self.now, &file_id)?;
            self.dlq_uris.push(path.uri.clone());
            self.send(Job::Put {
                path,
                body: Bytes::from(body),
                content_type: NDJSON_CONTENT_TYPE,
            })?;
        }
        Ok(())
    }

    fn send(&mut self, job: Job) -> Result<(), DatalakeError> {
        let sent = self
            .jobs
            .as_ref()
            .is_some_and(|jobs| jobs.blocking_send(job).is_ok());
        if sent {
            return Ok(());
        }
        // The upload task stopped: report its error.
        Err(self
            .join_uploader()
            .err()
            .unwrap_or_else(|| DatalakeError::storage("stream", "upload task stopped")))
    }

    fn join_uploader(&mut self) -> Result<u64, DatalakeError> {
        self.jobs = None;
        match self.uploader.take() {
            None => Err(DatalakeError::storage("stream", "the stream already ended")),
            Some(handle) => self.runtime.block_on(handle).map_err(|e| {
                DatalakeError::storage("stream", format!("upload task failed: {e}"))
            })?,
        }
    }

    /// Stops after an error: the unfinished object is discarded and its
    /// multipart upload aborted.
    fn shut_down(&mut self) {
        self.object = None;
        self.pending.clear();
        if self.uploader.is_some() {
            let _ = self.join_uploader();
        }
    }

    /// Validates what is left, completes the last objects and waits for every
    /// upload.
    ///
    /// # Errors
    ///
    /// As [`push`](Self::push); nothing is reported on error.
    pub fn finish(mut self) -> Result<StreamReport, DatalakeError> {
        let result = self.finish_inner();
        if result.is_err() {
            self.shut_down();
        }
        result
    }

    fn finish_inner(&mut self) -> Result<StreamReport, DatalakeError> {
        self.flush_pending()?;
        self.close_object()?;
        self.flush_dead_letters()?;
        let bytes_written = self.join_uploader()?;
        let records_quarantined = self.rejections.values().sum();
        tracing::info!(
            stream_id = %self.stream_id,
            tenant_id = %self.tenant.tenant_id(),
            received = self.received,
            ok = self.records_ok,
            quarantined = records_quarantined,
            objects = self.data_uris.len(),
            bytes = bytes_written,
            "bronze stream committed"
        );
        Ok(StreamReport {
            stream_id: self.stream_id.clone(),
            tenant_id: self.tenant.tenant_id().to_string(),
            records_received: self.received,
            records_ok: self.records_ok,
            records_quarantined,
            rejections: std::mem::take(&mut self.rejections),
            data_uris: std::mem::take(&mut self.data_uris),
            dlq_uris: std::mem::take(&mut self.dlq_uris),
            bytes_written,
            pii_protection: self.tenant.pii_scheme().to_owned(),
        })
    }
}

impl Drop for BronzeStream {
    fn drop(&mut self) {
        // Without `finish`, closing the channel makes the upload task abort
        // the unfinished multipart upload.
        self.jobs = None;
    }
}

/// An object whose parts are being uploaded.
struct Upload {
    path: Arc<ObjectPath>,
    id: String,
}

/// Upload task: runs parts concurrently, completes objects in order, aborts
/// an unfinished upload when the stream stops early. Returns bytes uploaded.
async fn upload(
    storage: Arc<dyn ObjectStorage>,
    mut jobs: mpsc::Receiver<Job>,
    max_inflight: usize,
) -> Result<u64, DatalakeError> {
    let mut open: Option<(usize, Upload)> = None;
    let mut parts: JoinSet<Result<(i32, String, u64), DatalakeError>> = JoinSet::new();
    let mut done: Vec<(i32, String)> = Vec::new();
    let mut bytes: u64 = 0;
    let result: Result<(), DatalakeError> = async {
        while let Some(job) = jobs.recv().await {
            match job {
                Job::Put {
                    path,
                    body,
                    content_type,
                } => {
                    bytes += body.len() as u64;
                    retrying(|| storage.put(&path, body.clone(), content_type)).await?;
                }
                Job::Part {
                    object,
                    path,
                    number,
                    body,
                } => {
                    if open.as_ref().map(|(i, _)| *i) != Some(object) {
                        let id = storage
                            .create_multipart(&path, PARQUET_CONTENT_TYPE)
                            .await?;
                        open = Some((
                            object,
                            Upload {
                                path: Arc::clone(&path),
                                id,
                            },
                        ));
                        done.clear();
                    }
                    while parts.len() >= max_inflight {
                        if let Some(finished) = parts.join_next().await {
                            let (n, etag, size) = joined(finished)?;
                            done.push((n, etag));
                            bytes += size;
                        }
                    }
                    let storage = Arc::clone(&storage);
                    let id = open.as_ref().map(|(_, u)| u.id.clone()).unwrap_or_default();
                    parts.spawn(async move {
                        let size = body.len() as u64;
                        let etag =
                            retrying(|| storage.upload_part(&path, &id, number, body.clone()))
                                .await?;
                        Ok((number, etag, size))
                    });
                }
                Job::Complete { object } => {
                    while let Some(finished) = parts.join_next().await {
                        let (n, etag, size) = joined(finished)?;
                        done.push((n, etag));
                        bytes += size;
                    }
                    if let Some((index, upload)) = open.take() {
                        if index == object {
                            done.sort_unstable_by_key(|(n, _)| *n);
                            storage
                                .complete_multipart(
                                    &upload.path,
                                    &upload.id,
                                    std::mem::take(&mut done),
                                )
                                .await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    if result.is_err() || open.is_some() {
        parts.abort_all();
        if let Some((_, upload)) = open.take() {
            if let Err(e) = storage.abort_multipart(&upload.path, &upload.id).await {
                tracing::warn!(error = %e, uri = %upload.path.uri, "could not abort multipart upload");
            }
        }
    }
    result.map(|()| bytes)
}

/// Attempts per request beyond the AWS SDK's own retries: parts and objects
/// are rewritten whole, so repeating one is safe.
const ATTEMPTS: u32 = 4;

async fn retrying<T, F, Fut>(mut call: F) -> Result<T, DatalakeError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, DatalakeError>>,
{
    let mut delay = std::time::Duration::from_millis(250);
    let mut attempt = 1;
    loop {
        match call().await {
            Err(DatalakeError::Storage { .. }) if attempt < ATTEMPTS => {
                tokio::time::sleep(jittered(delay)).await;
                delay = delay.saturating_mul(2);
                attempt += 1;
            }
            other => return other,
        }
    }
}

fn joined(
    finished: Result<Result<(i32, String, u64), DatalakeError>, tokio::task::JoinError>,
) -> Result<(i32, String, u64), DatalakeError> {
    finished.map_err(|e| DatalakeError::storage("stream", format!("part upload failed: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::ContractRegistry;
    use crate::pipeline::{IngestRequest, InvalidRecordPolicy, PipelineConfig};
    use crate::storage::InMemoryStorage;
    use crate::tenant::TenantId;
    use arrow::array::{Array, AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use async_trait::async_trait;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const CONTRACT: &str = "security/login_event/1.0.0";

    fn registry() -> Arc<ContractRegistry> {
        let mut r = ContractRegistry::default();
        r.register(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "x-dl-domain": "security",
            "x-dl-datatype": "login_event",
            "x-dl-schema_version": "1.0.0",
            "required": ["event_id"],
            "properties": {
                "event_id": {"type": "string"},
                "attempts": {"type": "integer", "minimum": 0},
                "note": {"type": "string"},
                "occurred_at": {"type": "string", "format": "date-time"}
            }
        }))
        .unwrap();
        Arc::new(r)
    }

    fn pipeline(storage: Arc<dyn ObjectStorage>, config: PipelineConfig) -> Arc<BronzePipeline> {
        Arc::new(BronzePipeline::new(registry(), storage, config).unwrap())
    }

    fn tenant() -> Arc<TenantProfile> {
        Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "acme-lake")
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    /// `n` rows from `start`; every 10th has a negative `attempts` (invalid).
    fn chunk(start: i64, n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("attempts", DataType::Int64, false),
            Field::new("note", DataType::Utf8, false),
        ]));
        let ids = (start..start + n).map(|i| format!("e{i:08}"));
        let attempts = (start..start + n).map(|i| if i % 10 == 9 { -1 } else { i });
        let notes = (start..start + n).map(|i| format!("note {i} {}", "x".repeat(40)));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(ids)),
                Arc::new(Int64Array::from_iter_values(attempts)),
                Arc::new(StringArray::from_iter_values(notes)),
            ],
        )
        .unwrap()
    }

    fn small() -> StreamOptions {
        StreamOptions {
            target_object_bytes: 256 * 1024,
            part_bytes: 64 * 1024,
            row_group_bytes: 32 * 1024,
            chunk_rows: 2_000,
            max_inflight_parts: 3,
            dead_letter_bytes: 16 * 1024,
            ..StreamOptions::default()
        }
    }

    fn rows_of(storage: &InMemoryStorage, uri: &str) -> Vec<RecordBatch> {
        let body = storage
            .get(uri.strip_prefix("s3://").unwrap())
            .unwrap()
            .body;
        ParquetRecordBatchReaderBuilder::try_new(body)
            .unwrap()
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn sources(chunks: i64, rows: i64) -> Vec<Result<RecordSource, DatalakeError>> {
        (0..chunks)
            .map(|c| Ok(RecordSource::Arrow(vec![chunk(c * rows, rows)])))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn large_input_becomes_several_complete_objects() {
        let storage = InMemoryStorage::new();
        let p = pipeline(Arc::new(storage.clone()), PipelineConfig::default());
        let report = p
            .ingest_stream(tenant(), CONTRACT, None, sources(20, 1_500), small())
            .await
            .unwrap();
        assert_eq!(report.records_received, 30_000);
        assert_eq!(report.records_quarantined, 3_000);
        assert_eq!(report.records_ok, 27_000);
        assert!(report.data_uris.len() > 1, "{:?}", report.data_uris);
        assert_eq!(storage.open_uploads(), 0);

        let mut ids = Vec::new();
        for (n, uri) in report.data_uris.iter().enumerate() {
            assert!(
                uri.ends_with(&format!("{}-{n:05}.parquet", report.stream_id)),
                "{uri}"
            );
            for batch in rows_of(&storage, uri) {
                let col = batch.column_by_name("event_id").unwrap().as_string::<i32>();
                ids.extend(col.iter().map(|v| v.unwrap().to_owned()));
                let batch_ids = batch.column_by_name("_dl_batch_id").unwrap();
                let batch_ids = arrow::compute::cast(batch_ids, &DataType::Utf8).unwrap();
                assert!(batch_ids
                    .as_string::<i32>()
                    .iter()
                    .all(|v| v == Some(report.stream_id.as_str())));
            }
        }
        let expected: Vec<String> = (0..30_000)
            .filter(|i| i % 10 != 9)
            .map(|i| format!("e{i:08}"))
            .collect();
        assert_eq!(ids, expected, "every valid row once, in input order");

        // Dead letters keep the record's position in the whole stream.
        let mut indices = Vec::new();
        for uri in &report.dlq_uris {
            let body = storage
                .get(uri.strip_prefix("s3://").unwrap())
                .unwrap()
                .body;
            for line in std::str::from_utf8(&body).unwrap().lines() {
                let v: Value = serde_json::from_str(line).unwrap();
                indices.push(v["record_index"].as_u64().unwrap());
            }
        }
        indices.sort_unstable();
        let expected: Vec<u64> = (0..30_000).filter(|i| i % 10 == 9).collect();
        assert_eq!(indices, expected);
        assert!(report.bytes_written > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_matches_a_single_ingest() {
        let single = InMemoryStorage::new();
        let p = pipeline(Arc::new(single.clone()), PipelineConfig::default());
        let one = p
            .ingest(IngestRequest::new(
                tenant(),
                CONTRACT,
                RecordSource::Arrow(vec![chunk(0, 3_000)]),
            ))
            .await
            .unwrap();
        let streamed = InMemoryStorage::new();
        let s = pipeline(Arc::new(streamed.clone()), PipelineConfig::default());
        let report = s
            .ingest_stream(
                tenant(),
                CONTRACT,
                None,
                sources(3, 1_000),
                StreamOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.data_uris.len(), 1);
        assert_eq!(
            (
                report.records_ok,
                report.records_quarantined,
                report.rejections.clone()
            ),
            (
                one.records_ok,
                one.records_quarantined,
                one.rejections.clone()
            )
        );
        let a = rows_of(&single, one.data_uri.as_deref().unwrap());
        let b = rows_of(&streamed, &report.data_uris[0]);
        let values = |batches: &[RecordBatch], name: &str| -> Vec<i64> {
            batches
                .iter()
                .flat_map(|b| {
                    b.column_by_name(name)
                        .unwrap()
                        .as_primitive::<Int64Type>()
                        .values()
                        .to_vec()
                })
                .collect()
        };
        assert_eq!(values(&a, "attempts"), values(&b, "attempts"));
        assert_eq!(a[0].schema(), b[0].schema());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn json_lines_quarantine_bad_lines_alone() {
        let storage = InMemoryStorage::new();
        let p = pipeline(Arc::new(storage.clone()), PipelineConfig::default());
        let text =
            "{\"event_id\":\"a\",\"attempts\":1}\n\n{not json}\r\n{\"event_id\":\"b\"}\n[1]\n";
        let report = p
            .ingest_stream(
                tenant(),
                CONTRACT,
                None,
                vec![Ok(RecordSource::JsonLines(text.to_owned()))],
                StreamOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.records_received, 4);
        assert_eq!(report.records_ok, 2);
        assert_eq!(
            report.rejections.get("ERR_RECORD_NOT_OBJECT").copied(),
            Some(2)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn text_columns_are_parsed_by_the_contract() {
        let storage = InMemoryStorage::new();
        let p = pipeline(Arc::new(storage.clone()), PipelineConfig::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("attempts", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(StringArray::from(vec![
                    Some("3"),
                    Some("x7"),
                    None,
                    Some("-2"),
                ])),
            ],
        )
        .unwrap();
        let report = p
            .ingest_stream(
                tenant(),
                CONTRACT,
                None,
                vec![Ok(RecordSource::ArrowText(vec![batch]))],
                StreamOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!((report.records_ok, report.records_quarantined), (2, 2));
        let rows = rows_of(&storage, &report.data_uris[0]);
        let attempts = rows[0].column_by_name("attempts").unwrap();
        assert_eq!(attempts.data_type(), &DataType::Int64);
        assert_eq!(attempts.as_primitive::<Int64Type>().value(0), 3);
        assert!(attempts.is_null(1));
        let dlq: String = report
            .dlq_uris
            .iter()
            .map(|u| {
                let body = storage.get(u.strip_prefix("s3://").unwrap()).unwrap().body;
                String::from_utf8(body.to_vec()).unwrap()
            })
            .collect();
        assert!(
            dlq.contains("\"x7\""),
            "the record keeps its original text: {dlq}"
        );
        assert!(dlq.contains("\"record_index\":1") && dlq.contains("\"record_index\":3"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_with_the_same_id_rewrites_the_same_keys() {
        let storage = InMemoryStorage::new();
        let p = pipeline(Arc::new(storage.clone()), PipelineConfig::default());
        let id = Ulid::generate().to_string();
        let options = StreamOptions {
            stream_id: Some(id.clone()),
            ..small()
        };
        let first = p
            .ingest_stream(tenant(), CONTRACT, None, sources(6, 1_500), options.clone())
            .await
            .unwrap();
        let keys = storage.keys();
        let second = p
            .ingest_stream(tenant(), CONTRACT, None, sources(6, 1_500), options)
            .await
            .unwrap();
        assert_eq!(first.data_uris, second.data_uris);
        assert_eq!(storage.keys(), keys, "no new objects");
        assert!(first.data_uris[0].contains(&id));
        let bad = StreamOptions {
            stream_id: Some("not-a-ulid".into()),
            ..StreamOptions::default()
        };
        assert!(p.stream(tenant(), CONTRACT, None, bad).is_err());
    }

    /// Report, Bronze rows and dead-letter positions of one stream.
    async fn outcome(fast: bool, source: RecordSource) -> (usize, usize, Vec<String>, Vec<u64>) {
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            arrow_fast_path: fast,
            ..PipelineConfig::default()
        };
        let p = pipeline(Arc::new(storage.clone()), config);
        let r = p
            .ingest_stream(
                tenant(),
                CONTRACT,
                None,
                vec![Ok(source)],
                StreamOptions::default(),
            )
            .await
            .unwrap();
        let mut rows = Vec::new();
        for uri in &r.data_uris {
            for b in rows_of(&storage, uri) {
                let cols: Vec<_> = ["event_id", "attempts", "note", "occurred_at"]
                    .iter()
                    .map(|c| {
                        arrow::compute::cast(b.column_by_name(c).unwrap(), &DataType::Utf8).unwrap()
                    })
                    .collect();
                for i in 0..b.num_rows() {
                    let line: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let c = c.as_string::<i32>();
                            if c.is_null(i) {
                                "~".into()
                            } else {
                                c.value(i).to_owned()
                            }
                        })
                        .collect();
                    rows.push(line.join("|"));
                }
            }
        }
        let mut dead = Vec::new();
        for uri in &r.dlq_uris {
            let body = storage
                .get(uri.strip_prefix("s3://").unwrap())
                .unwrap()
                .body;
            for line in std::str::from_utf8(&body).unwrap().lines() {
                let v: Value = serde_json::from_str(line).unwrap();
                dead.push(v["record_index"].as_u64().unwrap());
            }
        }
        dead.sort_unstable();
        (r.records_ok, r.records_quarantined, rows, dead)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn json_lines_fast_path_matches_the_row_path() {
        let lines = [
            r#"{"event_id":"a","attempts":1,"note":"x","occurred_at":"2026-01-01T00:00:00Z"}"#,
            r#"{"event_id":"b","attempts":-3}"#,
            r#"{"event_id":"c","attempts":null}"#,
            r#"{"event_id":"d","note":"null in text"}"#,
            r#"{"attempts":5}"#,
            r#"{"event_id":"e","occurred_at":"2999-01-01T00:00:00Z"}"#,
            r#"{"event_id":"f","extra":1}"#,
            r#"{"event_id":"g","attempts":2.5}"#,
            "not json",
            r#"{"event_id":"h","attempts":7,"note":"\u00e9\n"}"#,
        ];
        // Whole block typed, and blocks that fall back (undeclared field, float).
        for drop in [vec![6, 7], vec![7], vec![]] {
            let text: String = lines
                .iter()
                .enumerate()
                .filter(|(i, _)| !drop.contains(i))
                .map(|(_, l)| *l)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n";
            let fast = outcome(true, RecordSource::JsonLines(text.clone())).await;
            let rows = outcome(false, RecordSource::JsonLines(text)).await;
            assert_eq!(fast, rows, "dropped {drop:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timestamp_columns_become_date_time_text() {
        use arrow::array::TimestampSecondArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new(
                "occurred_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("UTC".into())),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(
                    TimestampSecondArray::from(vec![
                        Some(1_767_225_600),
                        None,
                        Some(4_102_444_800),
                    ])
                    .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let fast = outcome(true, RecordSource::Arrow(vec![batch.clone()])).await;
        let rows = outcome(false, RecordSource::Arrow(vec![batch])).await;
        assert_eq!(fast, rows);
        assert_eq!(
            (fast.0, fast.1),
            (2, 1),
            "the 2100 timestamp is in the future"
        );
        assert!(fast.2[0].contains("2026-01-01T00:00:00"), "{:?}", fast.2);
    }

    /// Storage whose part uploads fail after `ok_parts` parts.
    struct FailingParts {
        inner: InMemoryStorage,
        ok_parts: usize,
        parts: AtomicUsize,
        aborted: AtomicUsize,
    }

    #[async_trait]
    impl ObjectStorage for FailingParts {
        async fn put(&self, path: &ObjectPath, body: Bytes, ct: &str) -> Result<(), DatalakeError> {
            self.inner.put(path, body, ct).await
        }
        async fn create_multipart(
            &self,
            path: &ObjectPath,
            ct: &str,
        ) -> Result<String, DatalakeError> {
            self.inner.create_multipart(path, ct).await
        }
        async fn upload_part(
            &self,
            path: &ObjectPath,
            id: &str,
            n: i32,
            body: Bytes,
        ) -> Result<String, DatalakeError> {
            if self.parts.fetch_add(1, Ordering::SeqCst) >= self.ok_parts {
                return Err(DatalakeError::storage("test", "part upload failed"));
            }
            self.inner.upload_part(path, id, n, body).await
        }
        async fn complete_multipart(
            &self,
            path: &ObjectPath,
            id: &str,
            parts: Vec<(i32, String)>,
        ) -> Result<(), DatalakeError> {
            self.inner.complete_multipart(path, id, parts).await
        }
        async fn abort_multipart(&self, path: &ObjectPath, id: &str) -> Result<(), DatalakeError> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
            self.inner.abort_multipart(path, id).await
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_upload_aborts_and_reports_the_error() {
        let inner = InMemoryStorage::new();
        let storage = Arc::new(FailingParts {
            inner: inner.clone(),
            ok_parts: 2,
            parts: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
        });
        let p = pipeline(
            Arc::clone(&storage) as Arc<dyn ObjectStorage>,
            PipelineConfig::default(),
        );
        let err = p
            .ingest_stream(tenant(), CONTRACT, None, sources(20, 1_500), small())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("part upload failed"), "{err}");
        assert_eq!(storage.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(inner.open_uploads(), 0);
        assert!(inner
            .keys()
            .iter()
            .all(|k| !k.ends_with(".parquet") || k.contains("-00000")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reject_policy_stops_the_stream() {
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            invalid_records: InvalidRecordPolicy::Reject,
            ..PipelineConfig::default()
        };
        let p = pipeline(Arc::new(storage.clone()), config);
        let err = p
            .ingest_stream(tenant(), CONTRACT, None, sources(3, 1_000), small())
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                DatalakeError::RecordRejected {
                    record_index: 9,
                    ..
                }
            ),
            "{err}"
        );
        assert_eq!(storage.open_uploads(), 0);
    }

    #[test]
    fn options_are_validated() {
        let bad = [
            StreamOptions {
                target_object_bytes: 1,
                ..StreamOptions::default()
            },
            StreamOptions {
                chunk_rows: 0,
                ..StreamOptions::default()
            },
            StreamOptions {
                max_inflight_parts: 0,
                ..StreamOptions::default()
            },
            StreamOptions {
                max_inflight_parts: 65,
                ..StreamOptions::default()
            },
        ];
        for o in bad {
            assert!(o.validate().is_err(), "{o:?}");
        }
        assert!(StreamOptions::default().validate().is_ok());
    }

    #[test]
    fn opening_a_stream_needs_a_runtime() {
        let p = pipeline(Arc::new(InMemoryStorage::new()), PipelineConfig::default());
        let err = p
            .stream(tenant(), CONTRACT, None, StreamOptions::default())
            .err()
            .unwrap();
        assert!(err.to_string().contains("Tokio runtime"));
        let _ = Array::len(&Int64Array::from(vec![1]));
    }
}
