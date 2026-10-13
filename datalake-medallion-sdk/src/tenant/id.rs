//! `TenantId` newtype: compile-time tenant isolation (Level 1).
//!
//! A `TenantId` is validated at construction and cannot be built from an
//! arbitrary string otherwise, so every object key carries a safe identifier.

use crate::error::DatalakeError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

const MIN_LEN: usize = 3;
const MAX_LEN: usize = 63;

const RESERVED_WORDS: &[&str] = &[
    "admin", "root", "system", "public", "test", "default", "global", "internal", "shared",
    "common", "master", "primary", "replica",
];

/// Validated tenant identifier.
///
/// # Invariants
///
/// 1. 3 to 63 characters.
/// 2. Only `[a-z0-9_-]`, which rules out uppercase, whitespace, control
///    characters, path separators, dots and shell or SQL metacharacters.
/// 3. Starts and ends with a letter or digit.
/// 4. No consecutive separators (`--`, `__`, `-_`, `_-`).
/// 5. Not a reserved word (`admin`, `root`, `system`, `public`, `test`, ...).
///
/// # Examples
///
/// ```rust
/// use _core::tenant::TenantId;
///
/// let tid = TenantId::new("my-tenant-001").unwrap();
/// assert_eq!(tid.as_str(), "my-tenant-001");
/// assert!(TenantId::new("../etc/passwd").is_err());
/// assert!(TenantId::new("MyTenant").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TenantId(String);

impl TenantId {
    /// Validates `s` and wraps it.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::TenantViolation`] naming the first rule that failed.
    pub fn new(s: &str) -> Result<Self, DatalakeError> {
        validate(s).map_err(|reason| DatalakeError::TenantViolation {
            tenant_id: s.to_owned(),
            reason: reason.to_owned(),
        })?;
        Ok(Self(s.to_owned()))
    }

    /// The identifier as a string slice.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the identifier.
    #[inline]
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for TenantId {
    type Err = DatalakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for TenantId {
    type Error = DatalakeError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(&s)
    }
}

impl From<TenantId> for String {
    fn from(t: TenantId) -> Self {
        t.0
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn validate(s: &str) -> Result<(), &'static str> {
    if s.len() < MIN_LEN {
        return Err("too short (minimum 3 characters)");
    }
    if s.len() > MAX_LEN {
        return Err("too long (maximum 63 characters)");
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err("only lowercase letters, digits, '-' and '_' are allowed");
    }
    let is_sep = |b: u8| b == b'-' || b == b'_';
    let bytes = s.as_bytes();
    if is_sep(bytes[0]) || is_sep(bytes[bytes.len() - 1]) {
        return Err("must start and end with a letter or digit");
    }
    if bytes.windows(2).any(|w| is_sep(w[0]) && is_sep(w[1])) {
        return Err("consecutive separators are not allowed");
    }
    if RESERVED_WORDS.contains(&s) {
        return Err("reserved word");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_ids() {
        for id in [
            "my-tenant-001",
            "abc",
            "tenant123",
            "a1b",
            "acme_corp",
            "dropbox",
            "selectra",
        ] {
            assert!(TenantId::new(id).is_ok(), "{id} should be valid");
        }
    }

    #[test]
    fn rejects_length_violations() {
        for id in ["", "a", "ab"] {
            assert!(TenantId::new(id).is_err());
        }
        assert!(TenantId::new(&"a".repeat(64)).is_err());
        assert!(TenantId::new(&"a".repeat(63)).is_ok());
    }

    #[test]
    fn rejects_forbidden_characters() {
        for id in [
            "MyTenant",
            "tenant id",
            "tenant\0id",
            "tenant\nid",
            "../etc",
            "a/b",
            "a\\b",
            "a.b",
            "a;b",
            "a$b",
            "a|b",
            "a`b",
            "tenant-é",
        ] {
            assert!(TenantId::new(id).is_err(), "{id:?} should be rejected");
        }
    }

    #[test]
    fn rejects_bad_separators() {
        for id in [
            "-abc", "abc-", "_abc", "abc_", "ab--c", "ab__c", "ab-_c", "ab_-c",
        ] {
            assert!(TenantId::new(id).is_err(), "{id:?} should be rejected");
        }
    }

    #[test]
    fn rejects_reserved_words_exactly() {
        assert!(TenantId::new("admin").is_err());
        assert!(TenantId::new("test").is_err());
        assert!(TenantId::new("admin-corp").is_ok());
    }

    #[test]
    fn error_names_the_rule() {
        let msg = TenantId::new(&"a".repeat(64)).unwrap_err().to_string();
        assert!(msg.contains("too long"), "{msg}");
    }

    #[test]
    fn display_from_str_and_serde_roundtrip() {
        let tid: TenantId = "my-tenant-001".parse().unwrap();
        assert_eq!(tid.to_string(), "my-tenant-001");
        let json = serde_json::to_string(&tid).unwrap();
        assert_eq!(json, "\"my-tenant-001\"");
        let back: TenantId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, tid);
        assert!(serde_json::from_str::<TenantId>("\"Bad\"").is_err());
    }
}

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn never_panics(s in ".*") {
            let _ = TenantId::new(&s);
        }

        #[test]
        fn accepted_ids_are_key_safe(s in ".{0,70}") {
            if let Ok(t) = TenantId::new(&s) {
                prop_assert!(t.as_str().bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_'));
                prop_assert!(!t.as_str().contains(".."));
            }
        }

        #[test]
        fn simple_alnum_ids_roundtrip(s in "[a-z][a-z0-9]{3,15}[a-z0-9]") {
            if let Ok(t) = TenantId::new(&s) {
                prop_assert_eq!(t.as_str(), s.as_str());
            }
        }

        #[test]
        fn uppercase_always_rejected(s in "[A-Z][a-z0-9]{4,10}") {
            prop_assert!(TenantId::new(&s).is_err());
        }
    }
}
