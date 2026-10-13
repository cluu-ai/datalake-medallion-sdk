"""Type stubs for the native module ``datalake_medallion_sdk._core``."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from os import PathLike
from typing import Any, Literal, final

__all__ = [
    "BronzeIngestionClient",
    "ConfigurationError",
    "ContractBootError",
    "ContractRegistry",
    "DatalakeError",
    "HookError",
    "IngestReport",
    "MalformedPayloadError",
    "RecordRejectedError",
    "SchemaViolationError",
    "SemanticViolationError",
    "StorageError",
    "TenantViolationError",
    "TypeCoercionError",
    "__version__",
]
__version__: str

class DatalakeError(Exception):
    """Base class for every SDK exception."""

class ContractBootError(DatalakeError):
    """A contract could not be loaded, compiled or bound (Level 0)."""

class ConfigurationError(DatalakeError):
    """Invalid configuration or unknown contract key."""

class TenantViolationError(DatalakeError):
    """The tenant identifier failed validation (Level 1)."""

class MalformedPayloadError(DatalakeError):
    """The batch could not be decoded, or a record is malformed (Level 2)."""

class RecordRejectedError(DatalakeError):
    """A record failed validation while ``on_invalid="raise"``."""

class SchemaViolationError(RecordRejectedError):
    """A record violates its JSON Schema contract (Level 3)."""

class HookError(RecordRejectedError):
    """A pre-ingest hook rejected a record or raised (Level 4)."""

class SemanticViolationError(RecordRejectedError):
    """A record failed the future-timestamp guard (Level 5)."""

class TypeCoercionError(RecordRejectedError):
    """A value does not fit its contract-derived column type (Level 7)."""

class StorageError(DatalakeError):
    """Encoding or object storage failure. Safe to retry."""

@final
class ContractRegistry:
    """Immutable set of compiled data contracts."""

    @staticmethod
    def from_directory(
        path: str | PathLike[str], *, extension_prefix: str = ...
    ) -> ContractRegistry:
        """Compile every ``*.schema.json`` under ``path``; all-or-nothing."""
    @staticmethod
    def from_schemas(
        schemas: Iterable[Mapping[str, Any] | str], *, extension_prefix: str = ...
    ) -> ContractRegistry:
        """Compile schemas given as dicts or JSON strings; all-or-nothing."""
    def keys(self) -> list[str]:
        """Contract keys (``domain/datatype/version``), sorted."""
    def pii_fields(self, key: str) -> list[str]:
        """Fields marked as PII in a contract."""
    def schema_hash(self, key: str) -> str:
        """SHA-256 of a contract document."""
    def flexibility_mode(self, key: str) -> Literal["strict", "permissive", "schema_on_read"]:
        """Flexibility mode of a contract."""
    @property
    def extension_prefix(self) -> str:
        """Keyword namespace of the contracts."""
    def __len__(self) -> int: ...
    def __contains__(self, key: object, /) -> bool: ...

@final
class IngestReport:
    """Outcome of :meth:`BronzeIngestionClient.ingest`."""

    @property
    def batch_id(self) -> str:
        """ULID of the call; names the Parquet and dead-letter objects."""
    @property
    def records_received(self) -> int: ...
    @property
    def records_ok(self) -> int: ...
    @property
    def records_quarantined(self) -> int: ...
    @property
    def rejections(self) -> dict[str, int]:
        """Quarantined records per error code."""
    @property
    def data_uri(self) -> str | None:
        """``s3://`` URI of the Parquet object, if any record was valid."""
    @property
    def pii_masking(self) -> str:
        """PII scheme applied: ``sha256``, ``hmac-sha256`` or ``none``."""
    @property
    def dlq_uris(self) -> list[str]:
        """``s3://`` URIs of the dead-letter objects, one per error code."""

@final
class BronzeIngestionClient:
    """Validating Bronze writer for S3 and S3-compatible object stores.

    The S3 client is created on the first ``ingest`` and reused. Credentials
    come from the AWS default chain unless both static keys are given.
    """

    def __new__(
        cls,
        bucket: str,
        registry: ContractRegistry,
        *,
        data_prefix: str = ...,
        dlq_prefix: str = ...,
        partition_order: Literal["tenant_first", "data_type_first"] = ...,
        envelope_prefix: str = ...,
        max_record_bytes: int = ...,
        max_future_skew_seconds: int | None = ...,
        pii_masking: bool = ...,
        pii_hmac_key: bytes | None = None,
        on_invalid: Literal["quarantine", "raise"] = ...,
        hooks: Mapping[str, Callable[[dict[str, Any]], Any]] | None = None,
        aws_region: str | None = None,
        aws_endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        force_path_style: bool | None = None,
    ) -> BronzeIngestionClient: ...
    async def ingest(
        self,
        batch: Any,
        *,
        tenant_id: str,
        contract: str,
        pii_masking: bool | None = None,
    ) -> IngestReport:
        """Validate ``batch`` against ``contract`` and write it for ``tenant_id``.

        ``batch`` is a dict, a list of dicts, JSON ``str``/``bytes``, or an
        object implementing ``__arrow_c_stream__`` / ``__arrow_c_array__``
        (pyarrow, Polars, ...).

        Raises:
            TenantViolationError: invalid ``tenant_id``.
            ConfigurationError: unknown ``contract``.
            MalformedPayloadError: the batch cannot be decoded.
            RecordRejectedError: a record is invalid and ``on_invalid="raise"``.
            StorageError: encoding or upload failed.
        """
    @property
    def bucket(self) -> str: ...
    @property
    def hooked_contracts(self) -> list[str]: ...
