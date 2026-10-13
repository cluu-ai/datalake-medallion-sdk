//! Amazon S3 (and S3-compatible) [`ObjectStorage`] adapter.
//!
//! The client is built once and reused across calls. Credentials come from
//! the standard AWS chain (environment, profile, web identity, container or
//! instance role) unless static credentials are given. The SDK never creates
//! buckets: the ingestion role only needs `s3:PutObject` on its prefixes.

use std::fmt;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;

use super::ObjectStorage;
use crate::error::DatalakeError;

/// Explicit credentials. `Debug` never prints the secret parts.
#[derive(Clone)]
pub struct StaticCredentials {
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Optional session token for temporary credentials.
    pub session_token: Option<String>,
}

impl fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Connection options for [`S3Storage::connect`].
#[derive(Debug, Clone, Default)]
pub struct S3Options {
    /// Target bucket. It must already exist.
    pub bucket: String,
    /// Region; when `None`, resolved by the AWS default chain.
    pub region: Option<String>,
    /// Endpoint override for S3-compatible stores or `LocalStack`.
    pub endpoint_url: Option<String>,
    /// Path-style addressing; defaults to `true` when `endpoint_url` is set.
    pub force_path_style: Option<bool>,
    /// Static credentials; when `None`, the AWS default chain is used.
    pub credentials: Option<StaticCredentials>,
}

/// S3 adapter.
#[derive(Debug, Clone)]
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    /// Wraps an existing client.
    #[must_use]
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    /// Builds a client from `options`.
    pub async fn connect(options: S3Options) -> Self {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = &options.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(c) = &options.credentials {
            loader = loader.credentials_provider(Credentials::new(
                c.access_key_id.clone(),
                c.secret_access_key.clone(),
                c.session_token.clone(),
                None,
                "datalake-medallion-sdk-static",
            ));
        }
        if let Some(endpoint) = &options.endpoint_url {
            loader = loader.endpoint_url(endpoint.clone());
        }
        let shared = loader.load().await;
        let path_style = options
            .force_path_style
            .unwrap_or(options.endpoint_url.is_some());
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(path_style)
            .build();
        Self::new(Client::from_conf(config), options.bucket)
    }

    /// Bucket written to.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

#[async_trait]
impl ObjectStorage for S3Storage {
    async fn put(&self, key: &str, body: Bytes, content_type: &str) -> Result<(), DatalakeError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(body))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                DatalakeError::storage(
                    "s3",
                    format!(
                        "PutObject s3://{}/{key} failed: {}",
                        self.bucket,
                        DisplayErrorContext(&e)
                    ),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_debug_is_redacted() {
        let creds = StaticCredentials {
            access_key_id: "AKIAEXAMPLEKEY".into(),
            secret_access_key: "very-secret".into(),
            session_token: Some("token".into()),
        };
        let shown = format!("{creds:?}");
        assert!(!shown.contains("AKIAEXAMPLEKEY"));
        assert!(!shown.contains("very-secret"));
        assert!(!shown.contains("token\""));
    }

    #[tokio::test]
    async fn connect_with_endpoint_uses_bucket() {
        let storage = S3Storage::connect(S3Options {
            bucket: "local-bucket".into(),
            region: Some("us-east-1".into()),
            endpoint_url: Some("http://127.0.0.1:4566".into()),
            credentials: Some(StaticCredentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            }),
            ..S3Options::default()
        })
        .await;
        assert_eq!(storage.bucket(), "local-bucket");
    }

    #[tokio::test]
    #[ignore = "requires S3-compatible endpoint at AWS_ENDPOINT_URL (LocalStack)"]
    async fn put_object_against_localstack() {
        let endpoint =
            std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://127.0.0.1:4566".into());
        let storage = S3Storage::connect(S3Options {
            bucket: "sdk-it-bucket".into(),
            region: Some("us-east-1".into()),
            endpoint_url: Some(endpoint),
            credentials: Some(StaticCredentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            }),
            ..S3Options::default()
        })
        .await;
        let _ = storage
            .client
            .create_bucket()
            .bucket("sdk-it-bucket")
            .send()
            .await;
        storage
            .put("it/check.txt", Bytes::from_static(b"ok"), "text/plain")
            .await
            .unwrap();
        let got = storage
            .client
            .get_object()
            .bucket("sdk-it-bucket")
            .key("it/check.txt")
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes(),
            Bytes::from_static(b"ok")
        );
    }
}
