#![no_main]
//! Flattening arbitrary bytes as an OTLP logs request (protobuf or JSON,
//! gzip or zstd, all detected) must never panic, and never expand beyond
//! the decompression limit.

use _core::otel::{flatten_logs, OtlpOptions};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let options = OtlpOptions::default().with_max_decompressed_bytes(1 << 20);
    let _ = flatten_logs(data, &options);
});
