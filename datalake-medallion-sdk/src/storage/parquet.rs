//! Parquet encoding of Bronze batches.

use bytes::Bytes;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

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
        Self {
            properties: WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_created_by(
                    concat!("datalake-medallion-sdk ", env!("CARGO_PKG_VERSION")).into(),
                )
                .build(),
        }
    }
}

impl ParquetEncoder {
    /// Creates an encoder with custom writer properties.
    #[must_use]
    pub fn with_properties(properties: WriterProperties) -> Self {
        Self { properties }
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
        let err =
            |e: parquet::errors::ParquetError| DatalakeError::storage("parquet", e.to_string());
        let mut buffer = Vec::new();
        let options = ArrowWriterOptions::new()
            .with_properties(self.properties.clone())
            .with_skip_arrow_metadata(true);
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buffer, first.schema(), options).map_err(err)?;
        for batch in batches {
            writer.write(batch).map_err(err)?;
        }
        writer.close().map_err(err)?;
        Ok(Bytes::from(buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

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
