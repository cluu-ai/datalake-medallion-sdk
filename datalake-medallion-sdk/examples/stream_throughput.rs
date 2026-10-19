//! Streaming ingestion throughput and peak heap, without network I/O.
//!
//! Streams a CSV, JSON Lines or Parquet file through `BronzePipeline::ingest_stream`
//! into a store that discards the bytes, so the numbers are the SDK's own
//! (reading, validation, Parquet encoding, part assembly), not the network's:
//!
//! ```text
//! cargo run --release --example stream_throughput -- <file> <contracts dir> <contract key>
//! ```
//!
//! Peak heap is measured with a counting global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use _core::contracts::{ContractRegistry, FileSystemSchemaProvider, DEFAULT_EXTENSION_PREFIX};
use _core::error::DatalakeError;
use _core::pipeline::{BronzePipeline, PipelineConfig, RecordSource};
use _core::storage::ObjectStorage;
use _core::stream::StreamOptions;
use _core::tenant::{ObjectPath, TenantId, TenantProfile};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    let now = CURRENT.fetch_add(by, Ordering::Relaxed) + by;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every call is forwarded unchanged to the system allocator; the
// counters are only bookkeeping.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as `GlobalAlloc::alloc`.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as `GlobalAlloc::dealloc`.
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: same contract as `GlobalAlloc::realloc`.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            if new_size >= layout.size() {
                grew(new_size - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Accepts every upload and keeps nothing.
#[derive(Default)]
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

const BLOCK: usize = 8 * 1024 * 1024;

/// JSON Lines in blocks of about 8 MiB that end on a line break.
fn json_lines(path: &str) -> impl Iterator<Item = Result<RecordSource, DatalakeError>> {
    let mut reader = BufReader::with_capacity(BLOCK, File::open(path).expect("open"));
    std::iter::from_fn(move || {
        let mut block = String::with_capacity(BLOCK + 4096);
        while block.len() < BLOCK {
            let read = reader.read_line(&mut block).expect("read");
            if read == 0 {
                break;
            }
        }
        (!block.is_empty()).then(|| Ok(RecordSource::JsonLines(block)))
    })
}

/// CSV read as text, 64k rows per batch.
fn csv(path: &str) -> impl Iterator<Item = Result<RecordSource, DatalakeError>> {
    let mut header = String::new();
    BufReader::new(File::open(path).expect("open"))
        .read_line(&mut header)
        .expect("header");
    let fields: Vec<Field> = header
        .trim_end()
        .split(',')
        .map(|n| Field::new(n, DataType::Utf8, true))
        .collect();
    let reader = arrow_csv::ReaderBuilder::new(Arc::new(Schema::new(fields)))
        .with_header(true)
        .with_batch_size(65_536)
        .build(File::open(path).expect("open"))
        .expect("csv reader");
    reader.map(|b| {
        b.map(|b| RecordSource::ArrowText(vec![b]))
            .map_err(|e| DatalakeError::MalformedPayload {
                reason: e.to_string(),
            })
    })
}

fn parquet(path: &str) -> impl Iterator<Item = Result<RecordSource, DatalakeError>> {
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).expect("open"),
    )
    .expect("parquet")
    .with_batch_size(65_536)
    .build()
    .expect("reader");
    reader.map(|b| {
        b.map(|b| RecordSource::Arrow(vec![b]))
            .map_err(|e| DatalakeError::MalformedPayload {
                reason: e.to_string(),
            })
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (path, dir, contract) = (&args[1], &args[2], &args[3]);
    let registry = ContractRegistry::from_provider(
        &FileSystemSchemaProvider::new(dir),
        DEFAULT_EXTENSION_PREFIX,
    )
    .expect("contracts");
    let storage = Arc::new(Discard::default());
    let config = PipelineConfig {
        max_future_skew: None,
        ..PipelineConfig::default()
    };
    let pipeline = Arc::new(
        BronzePipeline::new(
            Arc::new(registry),
            Arc::clone(&storage) as Arc<dyn ObjectStorage>,
            config,
        )
        .expect("pipeline"),
    );
    let tenant = Arc::new(
        TenantProfile::for_bucket(TenantId::new("testing").expect("id"), "bench")
            .expect("profile")
            .build()
            .expect("profile"),
    );
    let size = std::fs::metadata(path).expect("size").len() as f64 / 1_048_576.0;
    let mut probe = [0u8; 4];
    File::open(path)
        .expect("open")
        .read_exact(&mut probe)
        .expect("read");
    let base = PEAK.load(Ordering::Relaxed);
    let started = Instant::now();
    let source: Box<dyn Iterator<Item = Result<RecordSource, DatalakeError>> + Send> =
        if path.ends_with(".parquet") {
            Box::new(parquet(path))
        } else if path.ends_with(".csv") {
            Box::new(csv(path))
        } else {
            Box::new(json_lines(path))
        };
    if std::env::var("DL_STAGES").is_ok() {
        // Cost of each stage alone: reading, then Parquet encoding with the
        // SDK's writer properties (single-threaded ArrowWriter).
        let batches: Vec<_> = parquet(path)
            .flat_map(|c| match c.expect("chunk") {
                RecordSource::Arrow(b) => b,
                _ => unreachable!(),
            })
            .collect();
        let read = started.elapsed().as_secs_f64();
        let t = Instant::now();
        let encoded = _core::storage::ParquetEncoder::default()
            .encode(&batches)
            .expect("encode");
        println!(
            "read {read:.2} s; encode {:.2} s ({} MiB)",
            t.elapsed().as_secs_f64(),
            encoded.len() >> 20
        );
        return;
    }
    if std::env::var("DL_ONESHOT").is_ok() {
        // The same input as one `prepare` call, for comparison.
        let mut batches = Vec::new();
        let mut text = String::new();
        let mut as_text = false;
        for chunk in source {
            match chunk.expect("chunk") {
                RecordSource::Arrow(b) => batches.extend(b),
                RecordSource::ArrowText(b) => {
                    as_text = true;
                    batches.extend(b);
                }
                RecordSource::JsonLines(t) => text.push_str(&t),
                _ => unreachable!(),
            }
        }
        let records = if !text.is_empty() {
            RecordSource::JsonLines(text)
        } else if as_text {
            RecordSource::ArrowText(batches)
        } else {
            RecordSource::Arrow(batches)
        };
        let read = started.elapsed().as_secs_f64();
        let prepared = pipeline
            .prepare(_core::pipeline::IngestRequest::new(
                tenant, contract, records,
            ))
            .expect("prepare");
        let secs = started.elapsed().as_secs_f64();
        let r = prepared.report();
        println!(
            "{path} one-shot: {size:.0} MiB in {secs:.2} s (read {read:.2} s) = {:.0} MiB/s, peak heap {} MiB, ok {}",
            size / secs,
            (PEAK.load(Ordering::Relaxed) - base) >> 20,
            r.records_ok
        );
        return;
    }
    let report = pipeline
        .ingest_stream(tenant, contract, None, source, options())
        .await
        .expect("stream");
    let secs = started.elapsed().as_secs_f64();
    println!(
        "{path}: {size:.0} MiB in {secs:.2} s = {:.0} MiB/s, {:.2} M records/s, peak heap {} MiB, \
         ok {} quarantined {}, {} objects, {:.0} MiB written",
        size / secs,
        report.records_received as f64 / secs / 1e6,
        (PEAK.load(Ordering::Relaxed) - base) >> 20,
        report.records_ok,
        report.records_quarantined,
        report.data_uris.len(),
        storage.bytes.load(Ordering::Relaxed) as f64 / 1_048_576.0,
    );
}

/// Defaults, with `DL_ROW_GROUP_MB` overriding the row group size.
fn options() -> StreamOptions {
    let mut options = StreamOptions::default();
    if let Some(mb) = std::env::var("DL_ROW_GROUP_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        options.row_group_bytes = mb << 20;
    }
    options
}
