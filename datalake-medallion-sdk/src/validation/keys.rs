//! Envelope encryption with cached data keys per tenant.
//!
//! A [`DataKeyProvider`] issues data keys (for example from a tenant's AWS KMS
//! key, see [`aws_kms`](super::aws_kms)) and recovers them from their key
//! reference. [`EnvelopeProtector`] is the PII scheme built on it: it keeps one
//! data key per tenant and reuses it until the [`KeyCachePolicy`] retires it,
//! so the provider is called about once per tenant and period instead of once
//! per ingest call. [`PiiDecryptor`] is the reading side.
//!
//! Everything here is optional: a tenant profile without PII protection never
//! touches a provider.

use arrow::array::{Array, StringArray};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use zeroize::Zeroizing;

use super::pii::{Aes256GcmEncryptor, FieldProtector, ProtectionContext, AES_256_KEY_LEN};
use crate::error::DatalakeError;

/// A data key: its plaintext (kept in memory only, wiped on drop) and the
/// reference stored with the data so readers can recover it.
pub struct DataKey {
    /// 32-byte AES-256 key.
    pub plaintext: Zeroizing<Vec<u8>>,
    /// Opaque reference, e.g. `kms1:<key arn>:<base64 encrypted key>`.
    pub key_ref: String,
}

impl fmt::Debug for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataKey")
            .field("plaintext", &"<redacted>")
            .field("key_ref", &self.key_ref)
            .finish()
    }
}

/// Port for issuing and recovering data keys (AWS KMS, GCP KMS, Azure Key
/// Vault, Vault, ...). Calls are rare (about one per tenant and period), so
/// implementations may block.
pub trait DataKeyProvider: Send + Sync + fmt::Debug {
    /// Provider name for logs, e.g. `aws-kms`.
    fn name(&self) -> &str;

    /// Issues a new data key for `tenant_id`, bound to it where the backend
    /// supports it (encryption context).
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when the backend refuses or fails.
    fn generate(&self, tenant_id: &str) -> Result<DataKey, DatalakeError>;

    /// Recovers the plaintext of a data key issued for `tenant_id`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when the reference is malformed, was
    /// issued for another tenant or key, or the backend refuses.
    fn decrypt(&self, tenant_id: &str, key_ref: &str) -> Result<Zeroizing<Vec<u8>>, DatalakeError>;
}

/// When a cached data key is retired.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KeyCachePolicy {
    /// Maximum age of a data key. Default 1 hour; 1 minute to 24 hours.
    pub max_age: Duration,
    /// Maximum random nonce values per data key (one per protected field and
    /// batch). Default 2^15, which keeps the chance of a repeated 64-bit nonce
    /// prefix near 2^-35, below NIST's 2^-32 bound. At most 2^20.
    pub max_nonces: u64,
    /// Fraction of `max_age` after which a new key is requested while the
    /// current one still works. Default 0.9.
    pub refresh_ahead: f64,
}

impl Default for KeyCachePolicy {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(3600),
            max_nonces: 1 << 15,
            refresh_ahead: 0.9,
        }
    }
}

impl KeyCachePolicy {
    /// Checks the documented ranges.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for a value out of range.
    pub fn validate(&self) -> Result<(), DatalakeError> {
        if !(Duration::from_secs(60)..=Duration::from_hours(24)).contains(&self.max_age) {
            return Err(DatalakeError::config(
                "data key max age must be between 60 seconds and 24 hours",
            ));
        }
        if !(1..=1 << 20).contains(&self.max_nonces) {
            return Err(DatalakeError::config(
                "max nonce prefixes per data key must be between 1 and 2^20",
            ));
        }
        if !(0.5..=1.0).contains(&self.refresh_ahead) {
            return Err(DatalakeError::config(
                "refresh_ahead must be between 0.5 and 1.0",
            ));
        }
        Ok(())
    }
}

/// Monotonic clock; injectable so tests can move time.
pub type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

struct Entry {
    key: Arc<Aes256GcmEncryptor>,
    created: Instant,
}

/// AES-256-GCM PII protection with a data key per tenant, issued by a
/// [`DataKeyProvider`] and reused until the [`KeyCachePolicy`] retires it.
///
/// - One cache entry per tenant; keys are never shared between tenants.
/// - Single-flight: concurrent calls of a tenant wait for one provider call.
/// - Refresh ahead: past `refresh_ahead` of the age a new key is requested;
///   if that fails, the current key keeps working until it expires.
/// - Fail closed: with no valid key and a failing provider, the ingest call
///   fails before anything is written.
pub struct EnvelopeProtector {
    provider: Arc<dyn DataKeyProvider>,
    policy: KeyCachePolicy,
    tenants: Mutex<HashMap<String, Arc<Mutex<Option<Entry>>>>>,
    clock: Clock,
}

impl fmt::Debug for EnvelopeProtector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeProtector")
            .field("provider", &self.provider.name())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl EnvelopeProtector {
    /// Builds the protector.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for a policy out of range.
    pub fn new(
        provider: Arc<dyn DataKeyProvider>,
        policy: KeyCachePolicy,
    ) -> Result<Self, DatalakeError> {
        Self::with_clock(provider, policy, Arc::new(Instant::now))
    }

    /// Like [`new`](Self::new) with an explicit clock.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for a policy out of range.
    pub fn with_clock(
        provider: Arc<dyn DataKeyProvider>,
        policy: KeyCachePolicy,
        clock: Clock,
    ) -> Result<Self, DatalakeError> {
        policy.validate()?;
        Ok(Self {
            provider,
            policy,
            tenants: Mutex::new(HashMap::new()),
            clock,
        })
    }

    /// The cache policy.
    #[must_use]
    pub fn policy(&self) -> KeyCachePolicy {
        self.policy
    }

    fn issue(&self, tenant_id: &str) -> Result<Entry, DatalakeError> {
        let key = self.provider.generate(tenant_id)?;
        if key.plaintext.len() != AES_256_KEY_LEN {
            return Err(DatalakeError::protection(format!(
                "provider '{}' returned a data key of the wrong length",
                self.provider.name()
            )));
        }
        tracing::info!(
            tenant_id,
            provider = self.provider.name(),
            "PII data key issued"
        );
        Ok(Entry {
            key: Arc::new(Aes256GcmEncryptor::new(&key.plaintext, key.key_ref)?),
            created: (self.clock)(),
        })
    }

    /// The data key to use now for `tenant_id`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when no key is valid and the provider fails.
    pub fn current(&self, tenant_id: &str) -> Result<Arc<Aes256GcmEncryptor>, DatalakeError> {
        let slot = {
            let mut tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(tenants.entry(tenant_id.to_owned()).or_default())
        };
        // Held across the provider call: single-flight per tenant only.
        let mut entry = slot.lock().unwrap_or_else(PoisonError::into_inner);
        let now = (self.clock)();
        let (valid, stale) = match entry.as_ref() {
            None => (false, false),
            Some(e) => {
                let age = now.saturating_duration_since(e.created);
                let valid =
                    age < self.policy.max_age && e.key.nonces_used() < self.policy.max_nonces;
                let stale = age.as_secs_f64()
                    >= self.policy.max_age.as_secs_f64() * self.policy.refresh_ahead;
                (valid, stale)
            }
        };
        if !valid {
            *entry = Some(self.issue(tenant_id)?);
        } else if stale {
            match self.issue(tenant_id) {
                Ok(fresh) => *entry = Some(fresh),
                Err(e) => tracing::warn!(
                    tenant_id,
                    provider = self.provider.name(),
                    error = %e,
                    "PII data key refresh failed; the current key stays in use until it expires"
                ),
            }
        }
        entry
            .as_ref()
            .map(|e| Arc::clone(&e.key))
            .ok_or_else(|| DatalakeError::protection("no PII data key available"))
    }
}

impl FieldProtector for EnvelopeProtector {
    #[allow(clippy::unnecessary_literal_bound)] // the trait lends &self
    fn scheme(&self) -> &str {
        "aes-256-gcm"
    }

    fn bind(&self, tenant_id: &str) -> Result<Option<Arc<dyn FieldProtector>>, DatalakeError> {
        Ok(Some(self.current(tenant_id)? as Arc<dyn FieldProtector>))
    }

    fn protect(&self, ctx: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError> {
        self.current(ctx.tenant_id)?.protect(ctx, value)
    }

    fn protect_batch(
        &self,
        ctx: &ProtectionContext<'_>,
        values: &[&str],
    ) -> Result<Vec<String>, DatalakeError> {
        self.current(ctx.tenant_id)?.protect_batch(ctx, values)
    }
}

/// Reads AES-256-GCM PII values back: recovers each data key once through the
/// provider and keeps it in a small cache keyed by tenant and key reference.
pub struct PiiDecryptor {
    provider: Arc<dyn DataKeyProvider>,
    capacity: usize,
    max_age: Duration,
    cache: Mutex<HashMap<CacheKey, (Arc<Aes256GcmEncryptor>, Instant)>>,
}

/// (tenant id, key reference).
type CacheKey = (String, String);

impl fmt::Debug for PiiDecryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PiiDecryptor")
            .field("provider", &self.provider.name())
            .field("capacity", &self.capacity)
            .field("max_age", &self.max_age)
            .finish_non_exhaustive()
    }
}

impl PiiDecryptor {
    /// Builds a decryptor caching at most `capacity` keys for `max_age` each.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for a zero capacity or age.
    pub fn new(
        provider: Arc<dyn DataKeyProvider>,
        capacity: usize,
        max_age: Duration,
    ) -> Result<Self, DatalakeError> {
        if capacity == 0 || max_age.is_zero() {
            return Err(DatalakeError::config(
                "decryptor cache capacity and max age must be positive",
            ));
        }
        Ok(Self {
            provider,
            capacity,
            max_age,
            cache: Mutex::new(HashMap::new()),
        })
    }

    fn key(
        &self,
        tenant_id: &str,
        key_ref: &str,
    ) -> Result<Arc<Aes256GcmEncryptor>, DatalakeError> {
        let id = (tenant_id.to_owned(), key_ref.to_owned());
        let now = Instant::now();
        {
            let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((key, at)) = cache.get(&id) {
                if now.duration_since(*at) < self.max_age {
                    return Ok(Arc::clone(key));
                }
            }
        }
        let plaintext = self.provider.decrypt(tenant_id, key_ref)?;
        let key = Arc::new(Aes256GcmEncryptor::new(&plaintext, key_ref)?);
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache.retain(|_, (_, at)| now.duration_since(*at) < self.max_age);
        if cache.len() >= self.capacity {
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(id, (Arc::clone(&key), now));
        Ok(key)
    }

    /// Decrypts one token written for `tenant_id` and `field` with `key_ref`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Protection`] when the key cannot be recovered or the
    /// token does not belong to that tenant, field and key.
    pub fn decrypt(
        &self,
        tenant_id: &str,
        field: &str,
        key_ref: &str,
        token: &str,
    ) -> Result<String, DatalakeError> {
        self.key(tenant_id, key_ref)?
            .decrypt(&ProtectionContext { tenant_id, field }, token)
    }

    /// Decrypts a column; `None` stays `None`. One key lookup for the column.
    ///
    /// # Errors
    ///
    /// As [`decrypt`](Self::decrypt), for the first value that fails.
    pub fn decrypt_many(
        &self,
        tenant_id: &str,
        field: &str,
        key_ref: &str,
        tokens: &[Option<&str>],
    ) -> Result<Vec<Option<String>>, DatalakeError> {
        let key = self.key(tenant_id, key_ref)?;
        let ctx = ProtectionContext { tenant_id, field };
        tokens
            .iter()
            .map(|t| t.map(|t| key.decrypt(&ctx, t)).transpose())
            .collect()
    }

    /// Decrypts an Arrow column of tokens into a column of clear values, in
    /// parallel. `key_refs` is one reference for the whole column, or the
    /// `_dl_pii_key_ref` column (one per row, as written), so a column whose
    /// rows were encrypted with several data keys decrypts in one call; each
    /// distinct key is looked up once. Null tokens stay null.
    ///
    /// # Errors
    ///
    /// As [`decrypt`](Self::decrypt), for the first value that fails, or a
    /// non-null token without a key reference.
    pub fn decrypt_array(
        &self,
        tenant_id: &str,
        field: &str,
        tokens: &StringArray,
        key_refs: KeyRefs<'_>,
    ) -> Result<StringArray, DatalakeError> {
        use rayon::prelude::*;

        let ctx = ProtectionContext { tenant_id, field };
        let mut keys: HashMap<&str, Arc<Aes256GcmEncryptor>> = HashMap::new();
        let refs: Vec<Option<&str>> = match key_refs {
            KeyRefs::One(r) => vec![Some(r); tokens.len()],
            KeyRefs::PerRow(column) => {
                if column.len() != tokens.len() {
                    return Err(DatalakeError::protection(
                        "key_refs and tokens must have the same length",
                    ));
                }
                column.iter().collect()
            }
            KeyRefs::Indexed { indices, values } => {
                if indices.len() != tokens.len() {
                    return Err(DatalakeError::protection(
                        "key_refs and tokens must have the same length",
                    ));
                }
                indices
                    .iter()
                    .map(|i| {
                        i.and_then(|i| usize::try_from(i).ok())
                            .filter(|&i| i < values.len() && values.is_valid(i))
                            .map(|i| values.value(i))
                    })
                    .collect()
            }
        };
        for (token, key_ref) in tokens.iter().zip(&refs) {
            if let (Some(_), Some(r)) = (token, key_ref) {
                if !keys.contains_key(r) {
                    keys.insert(r, self.key(tenant_id, r)?);
                }
            }
        }
        // Each task turns its rows into a finished StringArray, so only one
        // chunk of decrypted strings exists per task at a time.
        let parts: Vec<StringArray> = (0..tokens.len())
            .collect::<Vec<_>>()
            .par_chunks(DECRYPT_CHUNK)
            .map(|rows| {
                rows.iter()
                    .map(|&i| match (tokens.is_valid(i), refs[i]) {
                        (false, _) => Ok(None),
                        (true, None) => Err(DatalakeError::protection(
                            "a protected value has no key reference",
                        )),
                        (true, Some(r)) => keys[r].decrypt(&ctx, tokens.value(i)).map(Some),
                    })
                    .collect::<Result<StringArray, _>>()
            })
            .collect::<Result<_, _>>()?;
        let arrays: Vec<&dyn Array> = parts.iter().map(|a| a as &dyn Array).collect();
        if arrays.is_empty() {
            return Ok(StringArray::from(Vec::<Option<&str>>::new()));
        }
        let joined = arrow::compute::concat(&arrays)
            .map_err(|e| DatalakeError::protection(format!("cannot combine values: {e}")))?;
        Ok(joined
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .unwrap_or_else(|| StringArray::from(Vec::<Option<&str>>::new())))
    }
}

/// Values per parallel decryption task.
const DECRYPT_CHUNK: usize = 16_384;

/// Key references for [`PiiDecryptor::decrypt_array`].
#[derive(Debug, Clone, Copy)]
pub enum KeyRefs<'a> {
    /// One reference for every value.
    One(&'a str),
    /// One reference per value (the `_dl_pii_key_ref` column).
    PerRow(&'a StringArray),
    /// One reference per value as dictionary indices into `values`: the same
    /// column without a copy of the reference per row.
    Indexed {
        /// Index of each row's reference in `values`.
        indices: &'a arrow::array::Int32Array,
        /// The distinct references.
        values: &'a StringArray,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-memory provider: `key_ref` = `fake:<tenant>:<n>`, plaintext derived from n.
    #[derive(Debug, Default)]
    struct FakeProvider {
        issued: AtomicUsize,
        fail: std::sync::atomic::AtomicBool,
    }

    impl DataKeyProvider for FakeProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn name(&self) -> &str {
            "fake"
        }
        fn generate(&self, tenant_id: &str) -> Result<DataKey, DatalakeError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(DatalakeError::protection("key service unavailable"));
            }
            let n = self.issued.fetch_add(1, Ordering::SeqCst);
            #[allow(clippy::cast_possible_truncation)]
            Ok(DataKey {
                plaintext: Zeroizing::new(vec![n as u8 + 1; 32]),
                key_ref: format!("fake:{tenant_id}:{n}"),
            })
        }
        fn decrypt(
            &self,
            tenant_id: &str,
            key_ref: &str,
        ) -> Result<Zeroizing<Vec<u8>>, DatalakeError> {
            let (t, n) = key_ref
                .strip_prefix("fake:")
                .and_then(|r| r.rsplit_once(':'))
                .ok_or_else(|| DatalakeError::protection("bad key_ref"))?;
            if t != tenant_id {
                return Err(DatalakeError::protection("key belongs to another tenant"));
            }
            let n: u8 = n
                .parse()
                .map_err(|_| DatalakeError::protection("bad key_ref"))?;
            Ok(Zeroizing::new(vec![n + 1; 32]))
        }
    }

    struct TestClock(Mutex<Instant>);
    impl TestClock {
        fn advance(&self, d: Duration) {
            *self.0.lock().unwrap() += d;
        }
    }

    fn setup(policy: KeyCachePolicy) -> (Arc<FakeProvider>, Arc<TestClock>, EnvelopeProtector) {
        let provider = Arc::new(FakeProvider::default());
        let clock = Arc::new(TestClock(Mutex::new(Instant::now())));
        let c = Arc::clone(&clock);
        let p = EnvelopeProtector::with_clock(
            Arc::clone(&provider) as Arc<dyn DataKeyProvider>,
            policy,
            Arc::new(move || *c.0.lock().unwrap()),
        )
        .unwrap();
        (provider, clock, p)
    }

    fn ctx(tenant: &str) -> ProtectionContext<'_> {
        ProtectionContext {
            tenant_id: tenant,
            field: "email",
        }
    }

    #[test]
    fn one_key_per_tenant_is_reused_within_its_age() {
        let (provider, clock, p) = setup(KeyCachePolicy::default());
        let a1 = p.current("acme-001").unwrap();
        clock.advance(Duration::from_mins(30));
        let a2 = p.current("acme-001").unwrap();
        assert!(Arc::ptr_eq(&a1, &a2));
        let b = p.current("beta-002").unwrap();
        assert_ne!(
            a1.key_ref(),
            b.key_ref(),
            "keys are never shared between tenants"
        );
        assert_eq!(provider.issued.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn keys_are_refreshed_ahead_and_retired_by_age() {
        let (provider, clock, p) = setup(KeyCachePolicy::default());
        let first = p.current("acme-001").unwrap();
        clock.advance(Duration::from_mins(55)); // past 90% of one hour
        let second = p.current("acme-001").unwrap();
        assert_ne!(first.key_ref(), second.key_ref());
        clock.advance(Duration::from_secs(3601));
        let third = p.current("acme-001").unwrap();
        assert_ne!(second.key_ref(), third.key_ref());
        assert_eq!(provider.issued.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn keys_are_retired_by_nonce_budget() {
        let policy = KeyCachePolicy {
            max_nonces: 3,
            ..KeyCachePolicy::default()
        };
        let (provider, _clock, p) = setup(policy);
        let first = p.current("acme-001").unwrap();
        for _ in 0..3 {
            first.protect_batch(&ctx("acme-001"), &["a", "b"]).unwrap();
        }
        assert_eq!(first.nonces_used(), 3);
        let second = p.current("acme-001").unwrap();
        assert_ne!(first.key_ref(), second.key_ref());
        assert_eq!(provider.issued.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn failed_refresh_keeps_a_valid_key_but_expired_keys_fail_closed() {
        let (provider, clock, p) = setup(KeyCachePolicy::default());
        let first = p.current("acme-001").unwrap();
        provider.fail.store(true, Ordering::SeqCst);
        clock.advance(Duration::from_secs(3400)); // stale but valid
        let still = p.current("acme-001").unwrap();
        assert!(Arc::ptr_eq(&first, &still));
        clock.advance(Duration::from_secs(300)); // expired
        assert!(matches!(
            p.current("acme-001"),
            Err(DatalakeError::Protection { .. })
        ));
    }

    #[test]
    fn bind_fixes_one_key_and_decryptor_reads_it_back() {
        let (provider, _clock, p) = setup(KeyCachePolicy::default());
        let bound = p.bind("acme-001").unwrap().unwrap();
        let tokens = bound
            .protect_batch(&ctx("acme-001"), &["ana@x.io", "bo@y.io"])
            .unwrap();
        let key_ref = bound.key_ref().unwrap().to_owned();
        let reader = PiiDecryptor::new(
            provider as Arc<dyn DataKeyProvider>,
            8,
            Duration::from_secs(60),
        )
        .unwrap();
        let refs: Vec<Option<&str>> = vec![Some(&tokens[0]), None, Some(&tokens[1])];
        let plain = reader
            .decrypt_many("acme-001", "email", &key_ref, &refs)
            .unwrap();
        assert_eq!(
            plain,
            vec![Some("ana@x.io".into()), None, Some("bo@y.io".into())]
        );
        assert!(reader
            .decrypt("beta-002", "email", &key_ref, &tokens[0])
            .is_err());
        assert!(reader
            .decrypt("acme-001", "phone", &key_ref, &tokens[0])
            .is_err());
    }

    #[test]
    fn arrow_columns_decrypt_across_rotated_keys() {
        let (provider, _clock, p) = setup(KeyCachePolicy {
            max_nonces: 1,
            ..KeyCachePolicy::default()
        });
        let first = p.bind("acme-001").unwrap().unwrap();
        let a = first
            .protect_batch(&ctx("acme-001"), &["ana@x.io", "cy@z.io"])
            .unwrap();
        // One nonce prefix per key: the next bind rotates to a new data key.
        let second = p.bind("acme-001").unwrap().unwrap();
        let b = second
            .protect_batch(&ctx("acme-001"), &["bo@y.io"])
            .unwrap();
        let (r1, r2) = (first.key_ref().unwrap(), second.key_ref().unwrap());
        assert_ne!(r1, r2);
        let reader = PiiDecryptor::new(
            provider as Arc<dyn DataKeyProvider>,
            8,
            Duration::from_secs(60),
        )
        .unwrap();
        let tokens = StringArray::from(vec![
            Some(a[0].as_str()),
            None,
            Some(b[0].as_str()),
            Some(a[1].as_str()),
        ]);
        let refs = StringArray::from(vec![Some(r1), None, Some(r2), Some(r1)]);
        let clear = reader
            .decrypt_array("acme-001", "email", &tokens, KeyRefs::PerRow(&refs))
            .unwrap();
        assert_eq!(
            clear.iter().collect::<Vec<_>>(),
            vec![Some("ana@x.io"), None, Some("bo@y.io"), Some("cy@z.io")]
        );
        // One reference for every row: the row of the other key fails.
        assert!(reader
            .decrypt_array("acme-001", "email", &tokens, KeyRefs::One(r1))
            .is_err());
        let short = StringArray::from(vec![Some(r1)]);
        assert!(reader
            .decrypt_array("acme-001", "email", &tokens, KeyRefs::PerRow(&short))
            .is_err());
        assert!(reader
            .decrypt_array("beta-002", "email", &tokens, KeyRefs::PerRow(&refs))
            .is_err());
    }

    #[test]
    fn policy_ranges_are_enforced() {
        for bad in [
            KeyCachePolicy {
                max_age: Duration::from_secs(10),
                ..KeyCachePolicy::default()
            },
            KeyCachePolicy {
                max_age: Duration::from_hours(25),
                ..KeyCachePolicy::default()
            },
            KeyCachePolicy {
                max_nonces: 0,
                ..KeyCachePolicy::default()
            },
            KeyCachePolicy {
                max_nonces: (1 << 20) + 1,
                ..KeyCachePolicy::default()
            },
            KeyCachePolicy {
                refresh_ahead: 0.2,
                ..KeyCachePolicy::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn debug_never_shows_key_material() {
        let key = DataKey {
            plaintext: Zeroizing::new(vec![0x41; 32]),
            key_ref: "r".into(),
        };
        assert!(!format!("{key:?}").contains("65"));
        let (_, _, p) = setup(KeyCachePolicy::default());
        assert!(format!("{p:?}").contains("fake"));
    }
}
