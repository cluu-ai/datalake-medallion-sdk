//! Level 6: protection of fields marked as PII.
//!
//! Protection is a strategy chosen per tenant (see
//! [`TenantProfile`](crate::tenant::TenantProfile)) and is **off unless the
//! profile sets one**: without it, PII fields are stored with their declared
//! type. When a protector is set, every non-null value of a field marked with
//! `<extension-prefix>pii: true` is replaced by its protected text before
//! encoding and before the record can reach the dead-letter prefix.
//!
//! The SDK ships three schemes and accepts any other through
//! [`FieldProtector`] (open for extension, closed for modification):
//!
//! | Scheme | Type | Reversible | Use |
//! |---|---|---|---|
//! | [`Sha256Hasher`] | one-way hash | no | joins on equal values; weak for low-entropy values |
//! | [`HmacSha256Hasher`] | keyed hash | no | joins without dictionary attacks (key kept outside the lake) |
//! | [`Aes256GcmEncryptor`] | envelope encryption | with the data key | per-tenant keys, e.g. an AWS KMS data key per tenant |
//!
//! Strings are protected as-is; any other JSON value is protected over its
//! compact JSON text, so numbers, booleans and objects never leak.

use std::borrow::Cow;
use std::fmt;

use aes_gcm::aead::{Aead, Nonce, Payload};
use aes_gcm::Aes256Gcm;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use rayon::prelude::*;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::DatalakeError;

/// Minimum HMAC key length accepted, in bytes.
pub const MIN_HMAC_KEY_LEN: usize = 16;

/// Data key length of [`Aes256GcmEncryptor`], in bytes.
pub const AES_256_KEY_LEN: usize = 32;

/// Batches at least this large are protected on the rayon pool.
const PARALLEL_THRESHOLD: usize = 2048;

/// Prefix of [`Aes256GcmEncryptor`] tokens; changes if the format changes.
const AES_TOKEN_PREFIX: &str = "v1:";

/// Where the values being protected come from. Schemes may bind their output
/// to it (the AES-GCM scheme authenticates both).
#[derive(Debug, Clone, Copy)]
pub struct ProtectionContext<'a> {
    /// Owner of the values.
    pub tenant_id: &'a str,
    /// Contract field the values belong to.
    pub field: &'a str,
}

/// A PII protection scheme. Implement it to add a scheme without changing the
/// pipeline, then set it in a tenant profile.
///
/// Return an error instead of a partial or cleartext result: an error fails
/// the whole ingest call before anything is written. Error messages must not
/// contain values.
pub trait FieldProtector: Send + Sync + fmt::Debug {
    /// Scheme name recorded in the `<envelope-prefix>pii_protection` column,
    /// e.g. `sha256`: 1-32 chars of `[a-z0-9-]`, not `none`.
    fn scheme(&self) -> &str;

    /// What a reader needs to reverse or verify the protection, such as an
    /// encrypted data key or a key id. Recorded in the
    /// `<envelope-prefix>pii_key_ref` column; `None` for one-way schemes.
    fn key_ref(&self) -> Option<&str> {
        None
    }

    /// Protects one value.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when the value cannot be protected.
    fn protect(&self, ctx: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError>;

    /// Protects every value of one field, in order. The default runs
    /// [`protect`](FieldProtector::protect) in parallel for large batches;
    /// override it when a scheme has a cheaper bulk operation.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when any value cannot be protected.
    fn protect_batch(
        &self,
        ctx: &ProtectionContext<'_>,
        values: &[&str],
    ) -> Result<Vec<String>, DatalakeError> {
        if values.len() >= PARALLEL_THRESHOLD {
            values.par_iter().map(|v| self.protect(ctx, v)).collect()
        } else {
            values.iter().map(|v| self.protect(ctx, v)).collect()
        }
    }
}

/// Checks a scheme name returned by a [`FieldProtector`].
///
/// # Errors
///
/// [`DatalakeError::Configuration`] for an empty, reserved or non-slug name.
pub fn validate_scheme(scheme: &str) -> Result<(), DatalakeError> {
    let ok = (1..=32).contains(&scheme.len())
        && scheme != "none"
        && scheme
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(DatalakeError::config(format!(
            "PII protection scheme {scheme:?} must be 1-32 chars of [a-z0-9-] and not \"none\""
        )))
    }
}

/// Text a scheme receives for a JSON value: strings as-is, anything else as
/// compact JSON. `None` for null.
#[must_use]
pub fn protection_input(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(Cow::Borrowed(s)),
        other => Some(Cow::Owned(other.to_string())),
    }
}

/// Protects `fields` in every record, one [`FieldProtector::protect_batch`]
/// call per field.
///
/// # Errors
///
/// [`DatalakeError::Protection`] from the protector or when it returns a
/// different number of values; records may then be partially protected and
/// must not be written.
pub fn protect_records(
    protector: &dyn FieldProtector,
    tenant_id: &str,
    records: &mut [&mut Map<String, Value>],
    fields: &[String],
) -> Result<(), DatalakeError> {
    for field in fields {
        let (positions, outputs) = {
            let mut positions = Vec::new();
            let mut inputs: Vec<Cow<'_, str>> = Vec::new();
            for (i, record) in records.iter().enumerate() {
                if let Some(text) = record.get(field).and_then(protection_input) {
                    positions.push(i);
                    inputs.push(text);
                }
            }
            if positions.is_empty() {
                continue;
            }
            let refs: Vec<&str> = inputs.iter().map(AsRef::as_ref).collect();
            let ctx = ProtectionContext { tenant_id, field };
            (positions, protector.protect_batch(&ctx, &refs)?)
        };
        if outputs.len() != positions.len() {
            return Err(DatalakeError::protection(format!(
                "scheme '{}' returned {} values for {} inputs of field '{field}'",
                protector.scheme(),
                outputs.len(),
                positions.len()
            )));
        }
        for (i, protected) in positions.into_iter().zip(outputs) {
            if let Some(v) = records[i].get_mut(field) {
                *v = Value::String(protected);
            }
        }
    }
    Ok(())
}

/// Unkeyed SHA-256, 64 lowercase hex chars. Deterministic; reversible by
/// dictionary attack for low-entropy values such as e-mail addresses.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sha256Hasher;

impl Sha256Hasher {
    /// Hex digest of `value`.
    #[must_use]
    pub fn hash(value: &str) -> String {
        hex::encode(Sha256::digest(value.as_bytes()))
    }
}

impl FieldProtector for Sha256Hasher {
    #[allow(clippy::unnecessary_literal_bound)] // the trait lends &self
    fn scheme(&self) -> &str {
        "sha256"
    }
    fn protect(&self, _: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError> {
        Ok(Self::hash(value))
    }
}

/// Keyed HMAC-SHA256, 64 lowercase hex chars. Deterministic for a key, so
/// equal values still join; keep the key outside the data lake.
#[derive(Clone)]
pub struct HmacSha256Hasher {
    mac: Hmac<Sha256>,
}

impl HmacSha256Hasher {
    /// Builds a hasher.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when the key is shorter than
    /// [`MIN_HMAC_KEY_LEN`] bytes.
    pub fn new(key: &[u8]) -> Result<Self, DatalakeError> {
        if key.len() < MIN_HMAC_KEY_LEN {
            return Err(DatalakeError::config(format!(
                "PII HMAC key must be at least {MIN_HMAC_KEY_LEN} bytes"
            )));
        }
        Hmac::<Sha256>::new_from_slice(key)
            .map(|mac| Self { mac })
            .map_err(|e| DatalakeError::config(format!("invalid PII HMAC key: {e}")))
    }

    /// Hex digest of `value`.
    #[must_use]
    pub fn hash(&self, value: &str) -> String {
        hex::encode(
            self.mac
                .clone()
                .chain_update(value.as_bytes())
                .finalize()
                .into_bytes(),
        )
    }
}

impl fmt::Debug for HmacSha256Hasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HmacSha256Hasher(<key redacted>)")
    }
}

impl FieldProtector for HmacSha256Hasher {
    #[allow(clippy::unnecessary_literal_bound)] // the trait lends &self
    fn scheme(&self) -> &str {
        "hmac-sha256"
    }
    fn protect(&self, _: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError> {
        Ok(self.hash(value))
    }
}

/// AES-256-GCM envelope encryption with a host-provided data key.
///
/// Typical use with AWS KMS: the host calls `GenerateDataKey` with the
/// tenant's KMS key, passes the plaintext data key here and the
/// `CiphertextBlob` (base64) as `key_ref`. The SDK never calls KMS and never
/// stores the plaintext key; readers decrypt the key reference with KMS and
/// then each value with [`Aes256GcmEncryptor::decrypt`].
///
/// Each value becomes `v1:` + base64url(nonce ‖ ciphertext ‖ tag). The tenant
/// id and field name are authenticated (AAD), so a value moved to another
/// tenant or column fails decryption. Nonces are 96 bits: a random 64-bit
/// prefix per batch plus a 32-bit counter, so they never repeat inside a
/// batch; rotate data keys long before 2^32 batches per key.
pub struct Aes256GcmEncryptor {
    cipher: Aes256Gcm,
    key_ref: String,
}

impl Aes256GcmEncryptor {
    /// Builds an encryptor.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when the key is not 32 bytes or the
    /// key reference is empty or longer than 8 KiB.
    pub fn new(data_key: &[u8], key_ref: impl Into<String>) -> Result<Self, DatalakeError> {
        let key_ref = key_ref.into();
        if data_key.len() != AES_256_KEY_LEN {
            return Err(DatalakeError::config(format!(
                "AES-256-GCM data key must be {AES_256_KEY_LEN} bytes"
            )));
        }
        if key_ref.is_empty() || key_ref.len() > 8192 {
            return Err(DatalakeError::config(
                "AES-256-GCM key_ref must be 1 to 8192 characters (e.g. the encrypted data key)",
            ));
        }
        let cipher = Aes256Gcm::new_from_slice(data_key)
            .map_err(|_| DatalakeError::config("invalid AES-256-GCM data key"))?;
        Ok(Self { cipher, key_ref })
    }

    fn aad(ctx: &ProtectionContext<'_>) -> Vec<u8> {
        let mut aad = Vec::with_capacity(ctx.tenant_id.len() + ctx.field.len() + 1);
        aad.extend_from_slice(ctx.tenant_id.as_bytes());
        aad.push(0x1f);
        aad.extend_from_slice(ctx.field.as_bytes());
        aad
    }

    fn seal(&self, nonce: [u8; 12], aad: &[u8], value: &str) -> Result<String, DatalakeError> {
        let sealed = self
            .cipher
            .encrypt(
                &Nonce::<Aes256Gcm>::from(nonce),
                Payload {
                    msg: value.as_bytes(),
                    aad,
                },
            )
            .map_err(|_| DatalakeError::protection("AES-256-GCM encryption failed"))?;
        let mut token = Vec::with_capacity(12 + sealed.len());
        token.extend_from_slice(&nonce);
        token.extend_from_slice(&sealed);
        Ok(format!(
            "{AES_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(token)
        ))
    }

    /// Decrypts a token produced for the same tenant and field.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when the token is malformed, was made
    /// with another key, or belongs to another tenant or field.
    pub fn decrypt(
        &self,
        ctx: &ProtectionContext<'_>,
        token: &str,
    ) -> Result<String, DatalakeError> {
        let invalid = || {
            DatalakeError::protection("token cannot be decrypted with this key, tenant and field")
        };
        let raw = token
            .strip_prefix(AES_TOKEN_PREFIX)
            .and_then(|b| URL_SAFE_NO_PAD.decode(b).ok())
            .filter(|r| r.len() >= 12 + 16)
            .ok_or_else(invalid)?;
        let (nonce, sealed) = raw.split_at(12);
        let nonce: [u8; 12] = nonce.try_into().map_err(|_| invalid())?;
        let plain = self
            .cipher
            .decrypt(
                &Nonce::<Aes256Gcm>::from(nonce),
                Payload {
                    msg: sealed,
                    aad: &Self::aad(ctx),
                },
            )
            .map_err(|_| invalid())?;
        String::from_utf8(plain).map_err(|_| invalid())
    }
}

impl fmt::Debug for Aes256GcmEncryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Aes256GcmEncryptor(<key redacted>)")
    }
}

fn random_bytes<const N: usize>() -> Result<[u8; N], DatalakeError> {
    let mut out = [0u8; N];
    getrandom::fill(&mut out)
        .map_err(|e| DatalakeError::protection(format!("system random source failed: {e}")))?;
    Ok(out)
}

impl FieldProtector for Aes256GcmEncryptor {
    #[allow(clippy::unnecessary_literal_bound)] // the trait lends &self
    fn scheme(&self) -> &str {
        "aes-256-gcm"
    }

    fn key_ref(&self) -> Option<&str> {
        Some(&self.key_ref)
    }

    fn protect(&self, ctx: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError> {
        self.seal(random_bytes::<12>()?, &Self::aad(ctx), value)
    }

    fn protect_batch(
        &self,
        ctx: &ProtectionContext<'_>,
        values: &[&str],
    ) -> Result<Vec<String>, DatalakeError> {
        if u32::try_from(values.len()).is_err() {
            return Err(DatalakeError::protection(
                "AES-256-GCM batches are limited to 2^32 values per field",
            ));
        }
        let prefix = random_bytes::<8>()?;
        let aad = Self::aad(ctx);
        let seal = |(i, v): (usize, &&str)| {
            let mut nonce = [0u8; 12];
            nonce[..8].copy_from_slice(&prefix);
            // Fits in u32: the batch length was checked above.
            #[allow(clippy::cast_possible_truncation)]
            nonce[8..].copy_from_slice(&(i as u32).to_be_bytes());
            self.seal(nonce, &aad, v)
        };
        if values.len() >= PARALLEL_THRESHOLD {
            values.par_iter().enumerate().map(seal).collect()
        } else {
            values.iter().enumerate().map(seal).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const CTX: ProtectionContext<'static> = ProtectionContext {
        tenant_id: "acme-001",
        field: "email",
    };

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
            Sha256Hasher::hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn hmac_known_vector_rfc4231_case1() {
        let hasher = HmacSha256Hasher::new(&[0x0b; 20]).unwrap();
        assert_eq!(
            hasher.hash("Hi There"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn hmac_rejects_short_key_and_depends_on_key() {
        assert!(HmacSha256Hasher::new(b"short").is_err());
        let a = HmacSha256Hasher::new(&[1u8; 32]).unwrap();
        let b = HmacSha256Hasher::new(&[2u8; 32]).unwrap();
        assert_ne!(a.hash("alice@example.com"), b.hash("alice@example.com"));
    }

    #[test]
    fn records_are_protected_for_strings_and_other_values() {
        let mut r = record(json!({
            "email": "alice@example.com", "phone": 5_550_100,
            "doc": {"ssn": "123-45-6789"}, "event": "login", "n": null
        }));
        protect_records(
            &Sha256Hasher,
            "acme-001",
            &mut [&mut r],
            &fields(&["email", "phone", "doc", "n", "absent"]),
        )
        .unwrap();
        for f in ["email", "phone", "doc"] {
            assert_eq!(r[f].as_str().unwrap().len(), 64, "{f} must be hashed");
        }
        assert_eq!(r["email"], Sha256Hasher::hash("alice@example.com"));
        assert_eq!(r["phone"], Sha256Hasher::hash("5550100"));
        assert_eq!(r["event"], "login");
        assert_eq!(r["n"], Value::Null);
        assert!(!r.contains_key("absent"));
        assert!(!r.values().any(|v| v.to_string().contains("123-45")));
    }

    #[test]
    fn aes_round_trips_and_binds_tenant_and_field() {
        let enc = Aes256GcmEncryptor::new(&[7u8; 32], "kms-blob").unwrap();
        assert_eq!(enc.key_ref(), Some("kms-blob"));
        let one = enc.protect(&CTX, "alice@example.com").unwrap();
        let two = enc.protect(&CTX, "alice@example.com").unwrap();
        assert_ne!(one, two, "random nonces");
        assert!(one.starts_with("v1:") && !one.contains("alice"));
        assert_eq!(enc.decrypt(&CTX, &one).unwrap(), "alice@example.com");

        let other_tenant = ProtectionContext {
            tenant_id: "evil-002",
            field: "email",
        };
        let other_field = ProtectionContext {
            tenant_id: "acme-001",
            field: "phone",
        };
        assert!(enc.decrypt(&other_tenant, &one).is_err());
        assert!(enc.decrypt(&other_field, &one).is_err());
        let wrong_key = Aes256GcmEncryptor::new(&[8u8; 32], "kms-blob").unwrap();
        assert!(wrong_key.decrypt(&CTX, &one).is_err());
        assert!(enc.decrypt(&CTX, "v1:AAAA").is_err());
        assert!(enc.decrypt(&CTX, "plain").is_err());
    }

    #[test]
    fn aes_batches_use_unique_nonces_and_decrypt() {
        let enc = Aes256GcmEncryptor::new(&[3u8; 32], "ref").unwrap();
        let values: Vec<String> = (0..5000).map(|i| format!("user{i}@x.io")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let tokens = enc.protect_batch(&CTX, &refs).unwrap();
        let nonces: std::collections::HashSet<_> = tokens
            .iter()
            .map(|t| URL_SAFE_NO_PAD.decode(&t[3..]).unwrap()[..12].to_vec())
            .collect();
        assert_eq!(nonces.len(), tokens.len());
        for (t, v) in tokens.iter().zip(&values) {
            assert_eq!(&enc.decrypt(&CTX, t).unwrap(), v);
        }
    }

    #[test]
    fn aes_rejects_bad_keys() {
        assert!(Aes256GcmEncryptor::new(&[1u8; 16], "ref").is_err());
        assert!(Aes256GcmEncryptor::new(&[1u8; 32], "").is_err());
    }

    #[test]
    fn debug_never_prints_keys() {
        let h = HmacSha256Hasher::new(b"super-secret-key-material").unwrap();
        assert!(!format!("{h:?}").contains("super-secret"));
        let a = Aes256GcmEncryptor::new(&[0x41; 32], "ref").unwrap();
        assert_eq!(format!("{a:?}"), "Aes256GcmEncryptor(<key redacted>)");
    }

    #[test]
    fn scheme_names_are_validated() {
        assert!(validate_scheme("tenant-kms-v2").is_ok());
        for bad in ["", "none", "Upper", "with space", &"x".repeat(33)] {
            assert!(validate_scheme(bad).is_err(), "{bad}");
        }
    }

    #[derive(Debug)]
    struct Miscounting;
    impl FieldProtector for Miscounting {
        #[allow(clippy::unnecessary_literal_bound)]
        fn scheme(&self) -> &str {
            "broken"
        }
        fn protect(&self, _: &ProtectionContext<'_>, _: &str) -> Result<String, DatalakeError> {
            Ok(String::new())
        }
        fn protect_batch(
            &self,
            _: &ProtectionContext<'_>,
            _: &[&str],
        ) -> Result<Vec<String>, DatalakeError> {
            Ok(vec![])
        }
    }

    #[test]
    fn protectors_returning_wrong_counts_fail() {
        let mut r = record(json!({"email": "a@b.c"}));
        let err = protect_records(&Miscounting, "t", &mut [&mut r], &fields(&["email"]));
        assert!(matches!(err, Err(DatalakeError::Protection { .. })));
    }
}
