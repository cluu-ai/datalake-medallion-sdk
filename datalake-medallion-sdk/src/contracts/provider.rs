//! `SchemaProvider`: port through which the host application supplies its
//! data-contract schemas.
//!
//! The SDK ships no business schemas. The host passes a provider at boot and
//! the [`ContractRegistry`][crate::contracts::ContractRegistry] compiles
//! everything it returns.
//!
//! Recommended layout for [`FileSystemSchemaProvider`]:
//!
//! ```text
//! contracts/
//! ├── bronze/      ← x-dl-layer = "bronze"
//! ├── silver/
//! └── gold/
//! ```
//!
//! The tree is scanned recursively for `*.schema.json`; entries whose name
//! starts with `_` are skipped and symbolic links are not followed.

use crate::error::DatalakeError;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tracing::debug;

// ── Public API ────────────────────────────────────────────────────────────────

/// A single schema entry returned by a [`SchemaProvider`].
#[derive(Debug, Clone)]
pub struct SchemaEntry {
    /// Human-readable source identifier for logging and error messages.
    /// Example: `"bronze/network_flow/v1_0.schema.json"`
    pub source_id: String,
    /// Parsed JSON content of the schema.
    pub content: Value,
}

/// **Port**: Provides data-contract schemas to the core library.
///
/// Implement this trait in the host project and pass it to
/// [`ContractRegistry::from_provider`][crate::contracts::ContractRegistry::from_provider],
/// which calls [`provide_schemas`](SchemaProvider::provide_schemas) once at
/// boot (Level 0) and compiles every returned schema.
///
/// ## Thread-safety
///
/// Implementations MUST be `Send + Sync` because the registry is shared across
/// async tasks via `Arc`.
pub trait SchemaProvider: Send + Sync {
    /// Returns an iterator over all schemas this provider supplies.
    ///
    /// # Errors
    ///
    /// Returns [`DatalakeError::ContractBoot`] if the provider cannot enumerate
    /// its schemas (e.g. directory not found, I/O failure).
    fn provide_schemas(&self) -> Result<Vec<SchemaEntry>, DatalakeError>;
}

// ── FileSystemSchemaProvider ──────────────────────────────────────────────────

/// **Adapter**: Discovers schemas by scanning a directory tree recursively.
///
/// Scans `root_dir` for all `*.schema.json` files and loads them as
/// [`SchemaEntry`] values. Files whose names start with `_` are skipped
/// (convention for internal examples and fixtures).
///
/// ## Recommended Layout
///
/// ```text
/// <root_dir>/          (e.g. contracts/models/)
/// ├── bronze/
/// │   ├── network_flow/
/// │   │   └── v1_0.schema.json
/// │   └── endpoint_telemetry/
/// │       └── v1_0.schema.json
/// ├── silver/
/// │   └── enriched_alert/
/// │       └── v1_0.schema.json
/// └── gold/
///     └── threat_summary/
///         └── v1_0.schema.json
/// ```
#[derive(Debug, Clone)]
pub struct FileSystemSchemaProvider {
    root_dir: PathBuf,
}

impl FileSystemSchemaProvider {
    /// Creates a new provider scanning from `root_dir`.
    ///
    /// The directory does not need to exist yet; it is validated in
    /// [`provide_schemas`].
    #[must_use]
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }

    /// Returns the configured root directory path.
    #[must_use]
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }
}

impl SchemaProvider for FileSystemSchemaProvider {
    fn provide_schemas(&self) -> Result<Vec<SchemaEntry>, DatalakeError> {
        if !self.root_dir.exists() {
            return Err(DatalakeError::ContractBoot {
                message: format!(
                    "Schema root directory not found: '{}'",
                    self.root_dir.display()
                ),
            });
        }

        let mut files = Vec::new();
        scan_dir(&self.root_dir, &self.root_dir, &mut files)?;
        files.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        Ok(files)
    }
}

/// Recursively scans `dir` for `*.schema.json` files, skipping `_`-prefixed
/// entries.
fn scan_dir(dir: &Path, root: &Path, entries: &mut Vec<SchemaEntry>) -> Result<(), DatalakeError> {
    let read = std::fs::read_dir(dir).map_err(|e| DatalakeError::ContractBoot {
        message: format!("Cannot read directory '{}': {}", dir.display(), e),
    })?;

    for entry in read.flatten() {
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();

        // Skip underscore-prefixed files and directories (examples, fixtures)
        if file_name.starts_with('_') {
            debug!(path = %path.display(), "Skipping _-prefixed entry");
            continue;
        }

        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            debug!(path = %path.display(), "Skipping symbolic link");
            continue;
        }

        if file_type.is_dir() {
            scan_dir(&path, root, entries)?;
        } else if file_name.ends_with(".schema.json") {
            let source_id = path.strip_prefix(root).map_or_else(
                |_| path.to_string_lossy().to_string(),
                |p| p.to_string_lossy().replace('\\', "/"),
            );

            let raw = std::fs::read_to_string(&path).map_err(|e| DatalakeError::ContractBoot {
                message: format!("Cannot read schema '{}': {}", path.display(), e),
            })?;

            let content: Value =
                serde_json::from_str(&raw).map_err(|e| DatalakeError::ContractBoot {
                    message: format!("Schema '{}' is not valid JSON: {}", path.display(), e),
                })?;

            entries.push(SchemaEntry { source_id, content });
        }
    }

    Ok(())
}

// ── EmbeddedSchemaProvider ────────────────────────────────────────────────────

/// **Adapter**: Provides schemas from in-memory values.
///
/// Useful in tests and for embedding reference schemas at compile time with
/// `include_str!()` in the host project.
///
/// ```rust
/// use _core::contracts::EmbeddedSchemaProvider;
///
/// let provider = EmbeddedSchemaProvider::new()
///     .with_schema("test/my_event/v1_0", serde_json::json!({
///         "$schema": "https://json-schema.org/draft/2020-12/schema",
///         "x-dl-layer": "bronze",
///         "x-dl-domain": "test",
///         "x-dl-datatype": "my_event",
///         "x-dl-schema_version": "1.0.0",
///         "type": "object",
///         "properties": {}
///     }));
/// ```
#[derive(Debug, Clone, Default)]
pub struct EmbeddedSchemaProvider {
    schemas: Vec<SchemaEntry>,
}

impl EmbeddedSchemaProvider {
    /// Creates an empty provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a schema to the provider (builder pattern).
    #[must_use]
    pub fn with_schema(mut self, source_id: impl Into<String>, content: Value) -> Self {
        self.schemas.push(SchemaEntry {
            source_id: source_id.into(),
            content,
        });
        self
    }
}

impl SchemaProvider for EmbeddedSchemaProvider {
    fn provide_schemas(&self) -> Result<Vec<SchemaEntry>, DatalakeError> {
        Ok(self.schemas.clone())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use tempfile::TempDir;

    fn write_schema(dir: &Path, name: &str, layer: &str) {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "x-dl-layer": layer,
            "x-dl-domain": "test",
            "x-dl-datatype": name,
            "x-dl-schema_version": "1.0.0",
            "type": "object",
            "properties": {"id": {"type": "string"}}
        });
        fs::write(
            dir.join(format!("{name}.schema.json")),
            serde_json::to_string(&schema).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn test_filesystem_provider_scans_recursively() {
        let tmp = TempDir::new().unwrap();
        let bronze = tmp.path().join("bronze").join("network_flow");
        let silver = tmp.path().join("silver");
        fs::create_dir_all(&bronze).unwrap();
        fs::create_dir_all(&silver).unwrap();

        write_schema(&bronze, "network_flow", "bronze");
        write_schema(&silver, "enriched_alert", "silver");

        let provider = FileSystemSchemaProvider::new(tmp.path());
        let schemas = provider.provide_schemas().unwrap();

        assert_eq!(schemas.len(), 2, "Should find 2 schemas");
        assert!(
            schemas.iter().any(|s| s.source_id.contains("network_flow")),
            "Should contain network_flow"
        );
        assert!(
            schemas
                .iter()
                .any(|s| s.source_id.contains("enriched_alert")),
            "Should contain enriched_alert"
        );
    }

    #[test]
    fn test_filesystem_provider_skips_underscore_prefix() {
        let tmp = TempDir::new().unwrap();
        write_schema(tmp.path(), "real_event", "bronze");

        // Write an _example that should be skipped
        let example = json!({"type": "object"});
        fs::write(
            tmp.path().join("_example.schema.json"),
            serde_json::to_string(&example).unwrap(),
        )
        .unwrap();

        let provider = FileSystemSchemaProvider::new(tmp.path());
        let schemas = provider.provide_schemas().unwrap();

        assert_eq!(schemas.len(), 1, "Should skip _-prefixed files");
        assert!(schemas[0].source_id.contains("real_event"));
    }

    #[test]
    fn test_filesystem_provider_missing_dir_returns_error() {
        let provider = FileSystemSchemaProvider::new("/nonexistent/path");
        let result = provider.provide_schemas();
        assert!(result.is_err(), "Missing directory should return error");
    }

    #[test]
    fn test_embedded_provider_returns_all_schemas() {
        let provider = EmbeddedSchemaProvider::new()
            .with_schema("bronze/event_a/v1_0", json!({"type": "object"}))
            .with_schema("silver/event_b/v1_0", json!({"type": "object"}));

        let schemas = provider.provide_schemas().unwrap();
        assert_eq!(schemas.len(), 2);
    }

    #[test]
    fn test_embedded_provider_empty() {
        let provider = EmbeddedSchemaProvider::new();
        let schemas = provider.provide_schemas().unwrap();
        assert!(schemas.is_empty());
    }
}
