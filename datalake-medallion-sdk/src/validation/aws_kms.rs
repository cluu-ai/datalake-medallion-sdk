//! AWS KMS adapter for [`DataKeyProvider`] (feature `aws-kms`).
//!
//! Every data key is issued with the encryption context
//! `{"tenant_id": <tenant>, "purpose": "datalake-medallion-sdk/pii"}`, so IAM or
//! key policies can require it (`kms:EncryptionContext:tenant_id`), `CloudTrail`
//! records which tenant each key belongs to, and a key reference copied to
//! another tenant cannot be decrypted. Key references have the form
//! `kms1:<key arn>:<base64 CiphertextBlob>`; decryption always passes the key
//! ARN, so a reference pointing at an unexpected key is rejected.
//!
//! KMS calls are async; they run on a small runtime owned by the provider and
//! the caller waits on a channel, so the provider works from synchronous code,
//! from `spawn_blocking` threads and from inside async tasks alike. Calls are
//! rare (about one per tenant and cache period).

use std::fmt;
use std::sync::{mpsc, OnceLock};

use aws_config::BehaviorVersion;
use aws_sdk_kms::config::Region;
use aws_sdk_kms::error::DisplayErrorContext;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::DataKeySpec;
use aws_sdk_kms::Client;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use zeroize::Zeroizing;

use super::keys::{DataKey, DataKeyProvider};
use crate::error::DatalakeError;

/// Value of the `purpose` entry of the encryption context.
pub const ENCRYPTION_PURPOSE: &str = "datalake-medallion-sdk/pii";
const KEY_REF_PREFIX: &str = "kms1:";

/// Connection options; `None` uses the AWS default chain.
#[derive(Debug, Clone, Default)]
pub struct KmsOptions {
    /// Region of the key.
    pub region: Option<String>,
    /// Endpoint override (`LocalStack`, moto, VPC endpoints).
    pub endpoint_url: Option<String>,
}

/// Issues and recovers data keys with one AWS KMS key (typically one per tenant).
pub struct AwsKmsDataKeyProvider {
    key_id: String,
    options: KmsOptions,
    runtime: OnceLock<Result<(tokio::runtime::Runtime, Client), String>>,
}

impl fmt::Debug for AwsKmsDataKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKmsDataKeyProvider")
            .field("key_id", &self.key_id)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl AwsKmsDataKeyProvider {
    /// `key_id` is a key id, key ARN, alias name (`alias/...`) or alias ARN.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for an empty key id.
    pub fn new(key_id: impl Into<String>, options: KmsOptions) -> Result<Self, DatalakeError> {
        let key_id = key_id.into();
        if key_id.trim().is_empty() || key_id.len() > 2048 {
            return Err(DatalakeError::config(
                "KMS key id must be 1 to 2048 characters",
            ));
        }
        Ok(Self {
            key_id,
            options,
            runtime: OnceLock::new(),
        })
    }

    /// Runs one KMS call on the provider's runtime and waits for it.
    fn call<T, F>(&self, f: impl FnOnce(Client) -> F) -> Result<T, DatalakeError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, DatalakeError>> + Send + 'static,
    {
        let (runtime, client) = self
            .runtime
            .get_or_init(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("dl-kms")
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                let options = self.options.clone();
                let (tx, rx) = mpsc::channel();
                runtime.spawn(async move {
                    let mut loader = aws_config::defaults(BehaviorVersion::latest());
                    if let Some(region) = options.region {
                        loader = loader.region(Region::new(region));
                    }
                    if let Some(endpoint) = options.endpoint_url {
                        loader = loader.endpoint_url(endpoint);
                    }
                    let _ = tx.send(Client::new(&loader.load().await));
                });
                let client = rx.recv().map_err(|e| e.to_string())?;
                Ok((runtime, client))
            })
            .as_ref()
            .map_err(|e| DatalakeError::protection(format!("KMS client unavailable: {e}")))?;
        let (tx, rx) = mpsc::channel();
        let fut = f(client.clone());
        runtime.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.recv()
            .map_err(|_| DatalakeError::protection("KMS call was interrupted"))?
    }
}

impl Drop for AwsKmsDataKeyProvider {
    fn drop(&mut self) {
        // Dropping a runtime inside an async context panics; let it wind down
        // in the background instead.
        if let Some(Ok((runtime, _))) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn kms_error(op: &str, e: impl std::error::Error) -> DatalakeError {
    DatalakeError::protection(format!("KMS {op} failed: {}", DisplayErrorContext(e)))
}

/// Splits `kms1:<arn>:<base64>`; the ARN itself contains colons.
fn parse_key_ref(key_ref: &str) -> Result<(String, Vec<u8>), DatalakeError> {
    let invalid =
        || DatalakeError::protection("not a KMS key reference (expected kms1:<key arn>:<base64>)");
    let rest = key_ref.strip_prefix(KEY_REF_PREFIX).ok_or_else(invalid)?;
    let (arn, blob) = rest.rsplit_once(':').ok_or_else(invalid)?;
    if !arn.starts_with("arn:") {
        return Err(invalid());
    }
    let blob = STANDARD.decode(blob).map_err(|_| invalid())?;
    Ok((arn.to_owned(), blob))
}

impl DataKeyProvider for AwsKmsDataKeyProvider {
    #[allow(clippy::unnecessary_literal_bound)] // the trait lends &self
    fn name(&self) -> &str {
        "aws-kms"
    }

    fn generate(&self, tenant_id: &str) -> Result<DataKey, DatalakeError> {
        let key_id = self.key_id.clone();
        let tenant = tenant_id.to_owned();
        self.call(move |client| async move {
            let out = client
                .generate_data_key()
                .key_id(key_id)
                .key_spec(DataKeySpec::Aes256)
                .encryption_context("tenant_id", tenant)
                .encryption_context("purpose", ENCRYPTION_PURPOSE)
                .send()
                .await
                .map_err(|e| kms_error("GenerateDataKey", e))?;
            let plaintext = out
                .plaintext()
                .ok_or_else(|| DatalakeError::protection("KMS returned no plaintext key"))?;
            let blob = out
                .ciphertext_blob()
                .ok_or_else(|| DatalakeError::protection("KMS returned no encrypted key"))?;
            let arn = out
                .key_id()
                .ok_or_else(|| DatalakeError::protection("KMS returned no key id"))?;
            Ok(DataKey {
                plaintext: Zeroizing::new(plaintext.as_ref().to_vec()),
                key_ref: format!("{KEY_REF_PREFIX}{arn}:{}", STANDARD.encode(blob.as_ref())),
            })
        })
    }

    fn decrypt(&self, tenant_id: &str, key_ref: &str) -> Result<Zeroizing<Vec<u8>>, DatalakeError> {
        let (arn, blob) = parse_key_ref(key_ref)?;
        let tenant = tenant_id.to_owned();
        self.call(move |client| async move {
            let out = client
                .decrypt()
                .key_id(arn)
                .ciphertext_blob(Blob::new(blob))
                .encryption_context("tenant_id", tenant)
                .encryption_context("purpose", ENCRYPTION_PURPOSE)
                .send()
                .await
                .map_err(|e| kms_error("Decrypt", e))?;
            let plaintext = out
                .plaintext()
                .ok_or_else(|| DatalakeError::protection("KMS returned no plaintext key"))?;
            Ok(Zeroizing::new(plaintext.as_ref().to_vec()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_refs_carry_the_key_arn() {
        let arn = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab";
        let key_ref = format!("kms1:{arn}:{}", STANDARD.encode(b"blob"));
        let (got_arn, blob) = parse_key_ref(&key_ref).unwrap();
        assert_eq!(
            (got_arn.as_str(), blob.as_slice()),
            (arn, b"blob".as_slice())
        );
        for bad in [
            "",
            "kms1:",
            "v1:abc",
            "kms1:alias/x:YWJj",
            "kms1:arn:aws:kms:x:not-base64!",
        ] {
            assert!(parse_key_ref(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn empty_key_id_is_rejected() {
        assert!(AwsKmsDataKeyProvider::new(" ", KmsOptions::default()).is_err());
        let p = AwsKmsDataKeyProvider::new("alias/lake-acme", KmsOptions::default()).unwrap();
        assert_eq!(p.name(), "aws-kms");
        assert!(format!("{p:?}").contains("alias/lake-acme"));
    }
}
