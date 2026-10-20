"""Build and serve multi-tenant medallion data lakes on S3.

* ``BronzeIngestionClient``: contract-checked ingestion into Bronze (dicts,
  JSON, Arrow, Polars, DuckDB, files, streams, OpenTelemetry logs).
* ``LakeWriter`` / ``LakeMaintenance`` (extra ``[write]``): Silver and Gold
  Iceberg tables, written per tenant, and their scheduled maintenance.
* ``LakeReader`` (extra ``[read]``): tenant-scoped reads of Parquet and
  Iceberg tables in any layer.

Every client serves all tenants: each call names its tenant, whose profile
decides where data lives and how PII is protected (off by default)::

    from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry, TenantProfile

    client = BronzeIngestionClient(ContractRegistry.from_directory("contracts"))
    acme = TenantProfile.for_bucket("acme-001", "acme-lake")
    report = await client.ingest(rows, tenant=acme, contract="security/login_event/1.0.0")

See the README for writing Silver and Gold and for reading the lake.
"""

from __future__ import annotations

from importlib.metadata import version as _dist_version

from ._core import (
    ConfigurationError,
    ContractBootError,
    ContractRegistry,
    DatalakeError,
    HookError,
    IngestReport,
    MalformedPayloadError,
    PiiDecryptor,
    PiiProtection,
    ProtectionError,
    RecordRejectedError,
    SchemaViolationError,
    SemanticViolationError,
    StorageError,
    StreamReport,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
    TypeCoercionError,
)
from .client import BronzeIngestionClient, ClientOptions, TenantOutcome, batch_id_for
from .decorators import datalake_retry, emit_metrics
from .hooks import (
    Hook,
    HookRegistry,
    HookStatus,
    PreIngestHookResult,
    contract_validator,
    default_hooks,
)
from .metrics import (
    EmfMetricsSink,
    InMemoryMetricsSink,
    LoggingMetricsSink,
    Metric,
    MetricsSink,
)
from .reader import ChangeSet, LakeReader, TableRef
from .tenancy import DataKeyProvider, FieldProtector, TenantResolver
from .writer import CompactionResult, LakeMaintenance, LakeWriter, OrphanResult, WriteResult

# PEP 440 version of the installed distribution (the Rust crate uses SemVer,
# e.g. 0.1.0-rc.1 for 0.1.0rc1).
__version__: str = _dist_version("datalake-medallion-sdk")

__all__ = [
    "BronzeIngestionClient",
    "ChangeSet",
    "ClientOptions",
    "CompactionResult",
    "ConfigurationError",
    "ContractBootError",
    "ContractRegistry",
    "DataKeyProvider",
    "DatalakeError",
    "EmfMetricsSink",
    "FieldProtector",
    "Hook",
    "HookError",
    "HookRegistry",
    "HookStatus",
    "InMemoryMetricsSink",
    "IngestReport",
    "LakeMaintenance",
    "LakeReader",
    "LakeWriter",
    "LoggingMetricsSink",
    "MalformedPayloadError",
    "Metric",
    "MetricsSink",
    "OrphanResult",
    "PiiDecryptor",
    "PiiProtection",
    "PreIngestHookResult",
    "ProtectionError",
    "RecordRejectedError",
    "SchemaViolationError",
    "SemanticViolationError",
    "StorageError",
    "StreamReport",
    "TableRef",
    "TenantOutcome",
    "TenantProfile",
    "TenantRegistry",
    "TenantResolver",
    "TenantViolationError",
    "TypeCoercionError",
    "WriteResult",
    "__version__",
    "batch_id_for",
    "contract_validator",
    "datalake_retry",
    "default_hooks",
    "emit_metrics",
]
