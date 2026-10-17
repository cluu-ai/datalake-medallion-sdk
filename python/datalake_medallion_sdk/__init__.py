"""Contract-enforced Bronze-layer ingestion for medallion data lakes.

Quick start::

    from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry

    registry = ContractRegistry.from_directory("contracts")
    client = BronzeIngestionClient("acme-lake", registry)
    report = await client.ingest(
        [{"event_id": "e1", "user_email": "a@b.c"}],
        tenant_id="acme-001",
        contract="security/login_event/1.0.0",
    )
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
    RecordRejectedError,
    SchemaViolationError,
    SemanticViolationError,
    StorageError,
    TenantViolationError,
    TypeCoercionError,
)
from .client import BronzeIngestionClient, ClientOptions
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

# PEP 440 version of the installed distribution (the Rust crate uses SemVer,
# e.g. 0.1.0-rc.1 for 0.1.0rc1).
__version__: str = _dist_version("datalake-medallion-sdk")

__all__ = [
    "BronzeIngestionClient",
    "ClientOptions",
    "ConfigurationError",
    "ContractBootError",
    "ContractRegistry",
    "DatalakeError",
    "EmfMetricsSink",
    "Hook",
    "HookError",
    "HookRegistry",
    "HookStatus",
    "InMemoryMetricsSink",
    "IngestReport",
    "LoggingMetricsSink",
    "MalformedPayloadError",
    "Metric",
    "MetricsSink",
    "PreIngestHookResult",
    "RecordRejectedError",
    "SchemaViolationError",
    "SemanticViolationError",
    "StorageError",
    "TenantViolationError",
    "TypeCoercionError",
    "__version__",
    "contract_validator",
    "datalake_retry",
    "default_hooks",
    "emit_metrics",
]
