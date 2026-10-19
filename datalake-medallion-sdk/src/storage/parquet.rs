//! Parquet encoding of Bronze batches.

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::arrow_writer::{
    compute_leaves, ArrowColumnChunk, ArrowRowGroupWriterFactory, ArrowWriterOptions,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use rayon::prelude::*;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::DatalakeError;

/// Encodes record batches into one Parquet file (Snappy, page statistics).
///
/// The Arrow schema is not embedded: readers take column types from the
/// Parquet logical types, so in-memory encodings such as the one-entry
/// dictionaries of constant lineage columns read back as plain strings.
#[derive(Debug, Clone)]
pub struct ParquetEncoder {
    properties: WriterProperties,
}

impl Default for ParquetEncoder {
    fn default() -> Self {
        Self::with_compression(Compression::SNAPPY)
    }
}

impl ParquetEncoder {
    /// The SDK's writer settings with `compression`.
    #[must_use]
    pub fn with_compression(compression: Compression) -> Self {
        Self {
            properties: WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(compression)
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_created_by(
                    concat!("datalake-medallion-sdk ", env!("CARGO_PKG_VERSION")).into(),
                )
                .build(),
        }
    }

    /// Encoder for a contract's [`BronzeCompression`].
    #[must_use]
    pub fn for_contract(compression: crate::contracts::BronzeCompression) -> Self {
        Self::with_compression(compression.parquet())
    }

    /// Creates an encoder with custom writer properties.
    #[must_use]
    pub fn with_properties(properties: WriterProperties) -> Self {
        Self { properties }
    }

    /// Writer properties (compression, statistics, version).
    #[must_use]
    pub fn properties(&self) -> &WriterProperties {
        &self.properties
    }

    /// Encodes `batches` (all with the same schema) into one Parquet file.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] when `batches` is empty or encoding fails.
    pub fn encode(&self, batches: &[RecordBatch]) -> Result<Bytes, DatalakeError> {
        let first = batches
            .first()
            .ok_or_else(|| DatalakeError::storage("parquet", "no batches to encode"))?;
        let mut writer = ParallelWriter::try_new(Vec::new(), first.schema(), &self.properties)?;
        let max_rows = self.max_row_group_rows();
        // Row groups of up to `max_rows` rows, as ArrowWriter would form them:
        // small input batches are combined, large ones split.
        let mut group: Vec<RecordBatch> = Vec::new();
        let mut rows = 0;
        for batch in batches {
            let mut offset = 0;
            while offset < batch.num_rows() {
                let len = (max_rows - rows).min(batch.num_rows() - offset);
                group.push(batch.slice(offset, len));
                rows += len;
                offset += len;
                if rows == max_rows {
                    writer.write_row_group(&combine(&group)?)?;
                    group.clear();
                    rows = 0;
                }
            }
        }
        if rows > 0 {
            writer.write_row_group(&combine(&group)?)?;
        }
        Ok(Bytes::from(writer.close()?))
    }

    /// Rows per row group (the writer properties' limit, 1 Mi by default).
    pub(crate) fn max_row_group_rows(&self) -> usize {
        self.properties
            .max_row_group_row_count()
            .unwrap_or(1024 * 1024)
            .max(1)
    }
}

/// One batch from consecutive slices (no copy for a single slice).
fn combine(group: &[RecordBatch]) -> Result<RecordBatch, DatalakeError> {
    match group {
        [one] => Ok(one.clone()),
        [first, ..] => arrow::compute::concat_batches(&first.schema(), group)
            .map_err(|e| DatalakeError::storage("parquet", e.to_string())),
        [] => Err(DatalakeError::storage("parquet", "empty row group")),
    }
}

#[allow(clippy::needless_pass_by_value)] // used with map_err
fn parquet_err(e: parquet::errors::ParquetError) -> DatalakeError {
    DatalakeError::storage("parquet", e.to_string())
}

/// Parquet writer that encodes the columns of each row group in parallel
/// (on the rayon pool), as documented for `ArrowColumnWriter` in the
/// `parquet` crate. Output is a standard Parquet file, the same as
/// `ArrowWriter` writes with the same properties.
pub(crate) struct ParallelWriter<W: Write + Send> {
    writer: SerializedFileWriter<W>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    row_groups: usize,
}

impl<W: Write + Send> ParallelWriter<W> {
    pub(crate) fn try_new(
        sink: W,
        schema: SchemaRef,
        properties: &WriterProperties,
    ) -> Result<Self, DatalakeError> {
        let options = ArrowWriterOptions::new()
            .with_properties(properties.clone())
            .with_skip_arrow_metadata(true);
        let (writer, factory) =
            ArrowWriter::try_new_with_options(sink, Arc::clone(&schema), options)
                .and_then(ArrowWriter::into_serialized_writer)
                .map_err(parquet_err)?;
        Ok(Self {
            writer,
            factory,
            schema,
            row_groups: 0,
        })
    }

    /// Writes `batch` as one row group, its columns encoded concurrently.
    pub(crate) fn write_row_group(&mut self, batch: &RecordBatch) -> Result<(), DatalakeError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let writers = self
            .factory
            .create_column_writers(self.row_groups)
            .map_err(parquet_err)?;
        let mut leaves = Vec::with_capacity(writers.len());
        for (field, column) in self.schema.fields().iter().zip(batch.columns()) {
            leaves.extend(compute_leaves(field, column).map_err(parquet_err)?);
        }
        let chunks: Vec<ArrowColumnChunk> = writers
            .into_par_iter()
            .zip(leaves.into_par_iter())
            .map(|(mut writer, leaf)| {
                writer.write(&leaf)?;
                writer.close()
            })
            .collect::<Result<_, _>>()
            .map_err(parquet_err)?;
        let mut row_group = self.writer.next_row_group().map_err(parquet_err)?;
        for chunk in chunks {
            chunk
                .append_to_row_group(&mut row_group)
                .map_err(parquet_err)?;
        }
        row_group.close().map_err(parquet_err)?;
        self.row_groups += 1;
        Ok(())
    }

    /// Bytes written to the sink so far.
    pub(crate) fn bytes_written(&self) -> usize {
        self.writer.bytes_written()
    }

    /// Writes the footer and returns the sink.
    pub(crate) fn close(self) -> Result<W, DatalakeError> {
        self.writer.into_inner().map_err(parquet_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

    /// Every codec a contract can declare parses, is compiled in and
    /// round-trips (the `parquet` crate panics on a codec left out of its
    /// features, so this must hold at build time).
    #[test]
    fn every_contract_codec_round_trips() {
        use crate::contracts::{BronzeCompression, ContractRegistry};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let mut declared = Vec::new();
        for name in BronzeCompression::NAMES {
            let mut levels = vec![None];
            if name == "zstd" {
                levels.extend([Some(1), Some(22)]);
            }
            for level in levels {
                let mut schema = serde_json::json!({
                    "type": "object", "x-dl-domain": "app", "x-dl-datatype": "log",
                    "x-dl-schema_version": "1.0.0", "x-dl-compression": name,
                    "properties": {"id": {"type": "string"}}
                });
                if let Some(level) = level {
                    schema["x-dl-compression_level"] = level.into();
                }
                let mut registry = ContractRegistry::default();
                registry.register(&schema).unwrap();
                declared.push(registry.get("app/log/1.0.0").unwrap().compression());
            }
        }
        for codec in declared {
            let body = ParquetEncoder::for_contract(codec)
                .encode(&[batch(100)])
                .unwrap_or_else(|e| panic!("{codec:?}: {e}"));
            let reader = SerializedFileReader::new(body.clone()).unwrap();
            // Files record the codec, not the ZSTD level.
            let written = reader.metadata().row_group(0).column(0).compression();
            assert_eq!(
                std::mem::discriminant(&written),
                std::mem::discriminant(&codec.parquet()),
                "{codec:?}"
            );
            let rows: usize = ParquetRecordBatchReaderBuilder::try_new(body)
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            assert_eq!(rows, 100, "{codec:?}");
        }
    }

    fn batch(n: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..n)),
                Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("n{i}")),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn roundtrip_row_count_and_magic() {
        let bytes = ParquetEncoder::default()
            .encode(&[batch(3), batch(4)])
            .unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        let reader = SerializedFileReader::new(bytes).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 7);
        assert!(reader
            .metadata()
            .file_metadata()
            .created_by()
            .unwrap()
            .starts_with("datalake-medallion-sdk"));
    }

    #[test]
    fn empty_input_is_an_error() {
        assert!(ParquetEncoder::default().encode(&[]).is_err());
    }
}
