"""Contract-enforced ingestion and tenant-scoped reads for medallion data lakes.

Quick start (see the README for the contract file)::

    import asyncio
    from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry, TenantProfile

    client = BronzeIngestionClient(ContractRegistry.from_directory("contracts"))
    acme = TenantProfile.for_bucket("acme-001", "acme-lake")


    async def main() -> None:
        report = await client.ingest(
            [{"event_id": "0b7d6a52-3c1e-4f5e-9a4b-1f2d3c4b5a69",
              "occurred_at": "2026-10-01T12:00:00Z", "outcome": "SUCCESS"}],
            tenant=acme,
            contract="security/login_event/1.0.0",
        )
        print(report.records_ok, report.data_uri)


    asyncio.run(main())

Read it back with ``LakeReader`` (extra ``[read]``)::

    from datalake_medallion_sdk import LakeReader, TableRef

    table = LakeReader().read(acme, TableRef.bronze("login_event"))
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
from .writer import CompactionResult, LakeMaintenance, LakeWriter, WriteResult

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
