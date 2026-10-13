"""Public ingestion client."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal, TypedDict, Unpack

from . import _core
from ._core import ContractRegistry, IngestReport

__all__ = ["BronzeIngestionClient", "ClientOptions"]


class ClientOptions(TypedDict, total=False):
    """Keyword options of :class:`BronzeIngestionClient`."""

    data_prefix: str
    """Key prefix of Bronze objects. Default ``"bronze"``."""
    dlq_prefix: str
    """Key prefix of dead-letter objects. Default ``"dlq"``."""
    partition_order: Literal["tenant_first", "data_type_first"]
    """Order of the tenant and data type partitions. Default ``"tenant_first"``."""
    envelope_prefix: str
    """Prefix of lineage columns; payload fields may not use it. Default ``"_dl_"``."""
    max_record_bytes: int
    """Per-record size limit. Default 10 MiB."""
    max_future_skew_seconds: int | None
    """Allowed future skew of ``date-time`` fields; ``None`` disables. Default 3600."""
    pii_masking: bool
    """Hash fields marked as PII in the contract. Default ``True``; overridable per call."""
    pii_hmac_key: bytes | None
    """Key (16+ bytes) for HMAC-SHA256 pseudonymization; plain SHA-256 when ``None``."""
    on_invalid: Literal["quarantine", "raise"]
    """Quarantine invalid records (default) or raise and write nothing."""
    hooks: Mapping[str, Callable[[dict[str, Any]], Any]] | None
    """Level 4 hooks by contract key, e.g. ``HookRegistry.as_mapping()``."""
    aws_region: str | None
    aws_endpoint_url: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    force_path_style: bool | None


class BronzeIngestionClient:
    """Validating Bronze writer for S3 and S3-compatible object stores.

    Every record is checked against its contract (Levels 1 to 7); valid
    records are written as one Hive-partitioned Parquet object with lineage
    columns and invalid ones are quarantined under the dead-letter prefix.
    PII masking is on by default (``pii_masking=False`` turns it off) and can
    be overridden on each ``ingest`` call.

    The client is stateless per tenant and safe to share: create one per
    process and pass ``tenant_id`` on each call. The S3 connection is created
    on first use and reused. Credentials come from the AWS default chain
    unless both static keys are given.

    Raises:
        ConfigurationError: invalid bucket, prefixes or options.
        ContractBootError: a hook targets an unknown contract, or a contract
            declares a field inside the envelope namespace.
    """

    __slots__ = ("_native",)

    def __init__(
        self, bucket: str, registry: ContractRegistry, **options: Unpack[ClientOptions]
    ) -> None:
        self._native = _core.BronzeIngestionClient(bucket, registry, **options)

    async def ingest(
        self,
        batch: Any,
        *,
        tenant_id: str,
        contract: str,
        pii_masking: bool | None = None,
    ) -> IngestReport:
        """Validate ``batch`` against ``contract`` and write it for ``tenant_id``.

        ``batch`` is a dict, a list of dicts, JSON ``str``/``bytes``, or any
        object implementing the Arrow PyCapsule interface (pyarrow, Polars, ...).
        Validation and encoding run outside the GIL.

        ``pii_masking`` overrides the client setting for this call only; ``None``
        keeps the client setting. The scheme applied is reported in
        :attr:`IngestReport.pii_masking` and in the ``_dl_pii_masking`` column.

        Raises:
            TenantViolationError: invalid ``tenant_id``.
            ConfigurationError: unknown ``contract``.
            MalformedPayloadError: the batch cannot be decoded.
            RecordRejectedError: a record is invalid and ``on_invalid="raise"``.
            StorageError: encoding or upload failed; safe to retry.
        """
        return await self._native.ingest(
            batch, tenant_id=tenant_id, contract=contract, pii_masking=pii_masking
        )

    @property
    def bucket(self) -> str:
        """Target bucket."""
        return self._native.bucket

    @property
    def hooked_contracts(self) -> list[str]:
        """Contracts with a bound hook."""
        return self._native.hooked_contracts

    def __repr__(self) -> str:
        return repr(self._native)
