#![no_main]
//! Compiling arbitrary JSON as a contract must never panic.

use _core::contracts::{Contract, DEFAULT_EXTENSION_PREFIX};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

fuzz_target!(|data: &[u8]| {
    if let Ok(schema) = serde_json::from_slice::<Value>(data) {
        if let Ok(contract) = Contract::compile(&schema, DEFAULT_EXTENSION_PREFIX) {
            let _ = contract.check(&schema);
        }
    }
});
