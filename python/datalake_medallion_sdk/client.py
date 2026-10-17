"""Public ingestion client."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal, TypedDict, Unpack

from . import _core
from ._core import (
    ContractRegistry,
    IngestReport,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
)
from .tenancy import TenantResolver

__all__ = ["BronzeIngestionClient", "ClientOptions"]


class ClientOptions(TypedDict, total=False):
    """Keyword options of :class:`BronzeIngestionClient`."""

    tenants: TenantRegistry | TenantResolver | None
    """Where ``ingest(tenant="<id>")`` finds the tenant profile: a
    :class:`TenantRegistry` or a host function ``(tenant_id) -> TenantProfile``.
    Not needed when every call passes a :class:`TenantProfile`."""
    envelope_prefix: str
    """Prefix of lineage columns; payload fields may not use it. Default ``"_dl_"``."""
    max_record_bytes: int
    """Per-record size limit. Default 10 MiB."""
    max_future_skew_seconds: int | None
    """Allowed future skew of ``date-time`` fields; ``None`` disables. Default 3600."""
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

    Micro-tenant by design: the client holds contracts and a connection, never
    a destination. Every ``ingest`` names its tenant, and the tenant's
    :class:`TenantProfile` decides the Bronze and quarantine locations, the
    partition layout and whether PII is protected (it is not, by default).

    Every record is checked against its contract (Levels 1 to 7); valid
    records become one Hive-partitioned Parquet object with lineage columns
    and invalid ones are quarantined. One client per process can serve every
    tenant; the S3 connection is created on first use and reused for every
    bucket. Credentials come from the AWS default chain unless both static
    keys are given.

    Raises:
        ConfigurationError: invalid options.
        ContractBootError: a hook targets an unknown contract, or a contract
            declares a field inside the envelope namespace.
    """

    __slots__ = ("_native", "_resolver")

    def __init__(self, registry: ContractRegistry, **options: Unpack[ClientOptions]) -> None:
        native_options: dict[str, Any] = dict(options)
        tenants = native_options.pop("tenants", None)
        self._resolver: TenantResolver | None = None
        native_tenants: TenantRegistry | None = None
        if isinstance(tenants, TenantRegistry):
            native_tenants = tenants
        elif callable(tenants):
            self._resolver = tenants
        elif tenants is not None:
            raise TypeError(
                "tenants must be a TenantRegistry or a callable returning TenantProfile"
            )
        self._native = _core.BronzeIngestionClient(
            registry, tenants=native_tenants, **native_options
        )

    async def ingest(
        self, batch: Any, *, tenant: TenantProfile | str, contract: str
    ) -> IngestReport:
        """Validate ``batch`` against ``contract`` and write it for ``tenant``.

        ``tenant`` is a :class:`TenantProfile`, or a tenant id resolved with
        the client's ``tenants``. ``batch`` is a dict, a list of dicts, JSON
        ``str``/``bytes``, or any object implementing the Arrow PyCapsule
        interface (pyarrow, Polars, ...); Arrow is the fastest path.
        Validation, protection and encoding run outside the GIL.

        Raises:
            TenantViolationError: invalid or unknown tenant, or a resolver
                returned the profile of another tenant.
            ConfigurationError: unknown ``contract``, or a tenant id without
                a ``tenants`` registry or resolver.
            MalformedPayloadError: the batch cannot be decoded.
            RecordRejectedError: a record is invalid and ``on_invalid="raise"``.
            ProtectionError: the tenant's PII protector failed; nothing written.
            StorageError: encoding or upload failed; safe to retry.
        """
        if isinstance(tenant, str) and self._resolver is not None:
            profile = self._resolver(tenant)
            if not isinstance(profile, TenantProfile):
                raise TypeError("tenant resolver must return a TenantProfile")
            if profile.tenant_id != tenant:
                raise TenantViolationError(
                    f"tenant resolver returned the profile of {profile.tenant_id!r} for {tenant!r}"
                )
            tenant = profile
        return await self._native.ingest(batch, tenant=tenant, contract=contract)

    @property
    def tenant_ids(self) -> list[str]:
        """Tenant ids of the client's ``TenantRegistry`` (empty otherwise)."""
        return self._native.tenant_ids

    @property
    def hooked_contracts(self) -> list[str]:
        """Contracts with a bound hook."""
        return self._native.hooked_contracts

    def __repr__(self) -> str:
        return repr(self._native)
