//! Level 6: irreversible pseudonymization of PII fields.
//!
//! Fields marked with `<extension-prefix>pii: true` in a contract are hashed
//! before encoding and before a record is written to the dead-letter prefix.
//! Masking is on by default and can be turned off in the pipeline configuration
//! or per ingest call; the scheme applied is recorded in every row.
//!
//! Plain SHA-256 is deterministic but can be reversed by dictionary attack for
//! low-entropy values such as e-mail addresses. Prefer
//! [`PiiHasher::hmac_sha256`] with a secret key kept outside the data lake.

use std::fmt;

use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::DatalakeError;

/// Minimum HMAC key length accepted, in bytes.
pub const MIN_HMAC_KEY_LEN: usize = 16;

/// Hashing scheme applied to PII values. Output is always 64 lowercase hex chars.
#[derive(Clone, Default)]
pub enum PiiHasher {
    /// Unkeyed SHA-256.
    #[default]
    Sha256,
    /// Keyed HMAC-SHA256 (recommended).
    HmacSha256(Hmac<Sha256>),
}

impl fmt::Debug for PiiHasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.algorithm())
    }
}

impl PiiHasher {
    /// Builds a keyed hasher.
    ///
    /// # Errors
    ///
    /// Returns [`DatalakeError::Configuration`] when the key is shorter than
    /// [`MIN_HMAC_KEY_LEN`] bytes.
    pub fn hmac_sha256(key: &[u8]) -> Result<Self, DatalakeError> {
        if key.len() < MIN_HMAC_KEY_LEN {
            return Err(DatalakeError::config(format!(
                "PII HMAC key must be at least {MIN_HMAC_KEY_LEN} bytes"
            )));
        }
        Hmac::<Sha256>::new_from_slice(key)
            .map(Self::HmacSha256)
            .map_err(|e| DatalakeError::config(format!("invalid PII HMAC key: {e}")))
    }

    /// Algorithm name recorded in logs.
    #[must_use]
    pub fn algorithm(&self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::HmacSha256(_) => "hmac-sha256",
        }
    }

    /// Hashes one value and returns its hex digest.
    #[must_use]
    pub fn hash(&self, value: &str) -> String {
        match self {
            Self::Sha256 => hex::encode(Sha256::digest(value.as_bytes())),
            Self::HmacSha256(mac) => hex::encode(
                mac.clone()
                    .chain_update(value.as_bytes())
                    .finalize()
                    .into_bytes(),
            ),
        }
    }

    /// Replaces every non-null value of `fields` in `record` with its digest.
    ///
    /// Strings are hashed as-is; any other JSON value is hashed over its
    /// compact JSON text, so numbers, booleans and nested objects never leak.
    pub fn mask(&self, record: &mut Map<String, Value>, fields: &[String]) {
        for field in fields {
            if let Some(value) = record.get_mut(field) {
                let digest = match &*value {
                    Value::Null => continue,
                    Value::String(s) => self.hash(s),
                    other => self.hash(&other.to_string()),
                };
                *value = Value::String(digest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => unreachable!(),
        }
    }

    fn fields(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn sha256_known_vector() {
        assert_eq!(
            PiiHasher::Sha256.hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn hmac_known_vector_rfc4231_case1() {
        let hasher = PiiHasher::hmac_sha256(&[0x0b; 20]).unwrap();
        assert_eq!(
            hasher.hash("Hi There"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn hmac_rejects_short_key() {
        assert!(PiiHasher::hmac_sha256(b"short").is_err());
    }

    #[test]
    fn hmac_depends_on_key() {
        let a = PiiHasher::hmac_sha256(&[1u8; 32]).unwrap();
        let b = PiiHasher::hmac_sha256(&[2u8; 32]).unwrap();
        assert_ne!(a.hash("alice@example.com"), b.hash("alice@example.com"));
    }

    #[test]
    fn mask_hashes_strings_and_keeps_others() {
        let mut r = record(json!({"email": "alice@example.com", "event": "login"}));
        PiiHasher::Sha256.mask(&mut r, &fields(&["email"]));
        let masked = r["email"].as_str().unwrap();
        assert_eq!(masked.len(), 64);
        assert!(!masked.contains('@'));
        assert_eq!(r["event"], "login");
    }

    #[test]
    fn mask_hashes_non_string_values() {
        let mut r =
            record(json!({"phone": 5_550_100, "doc": {"ssn": "123-45-6789"}, "flag": true}));
        PiiHasher::Sha256.mask(&mut r, &fields(&["phone", "doc", "flag"]));
        for f in ["phone", "doc", "flag"] {
            assert_eq!(r[f].as_str().unwrap().len(), 64, "{f} must be hashed");
        }
        assert!(!r.values().any(|v| v.to_string().contains("123-45")));
    }

    #[test]
    fn mask_keeps_null_and_missing() {
        let mut r = record(json!({"email": null}));
        PiiHasher::Sha256.mask(&mut r, &fields(&["email", "absent"]));
        assert_eq!(r["email"], Value::Null);
        assert!(!r.contains_key("absent"));
    }

    #[test]
    fn mask_is_deterministic() {
        let mut a = record(json!({"email": "a@b.c"}));
        let mut b = record(json!({"email": "a@b.c"}));
        PiiHasher::Sha256.mask(&mut a, &fields(&["email"]));
        PiiHasher::Sha256.mask(&mut b, &fields(&["email"]));
        assert_eq!(a, b);
    }

    #[test]
    fn debug_never_prints_key() {
        let hasher = PiiHasher::hmac_sha256(b"super-secret-key-material").unwrap();
        assert_eq!(format!("{hasher:?}"), "hmac-sha256");
    }
}
