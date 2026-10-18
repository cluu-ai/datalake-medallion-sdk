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
    "PiiDecryptor",
    "PiiProtection",
    "ProtectionError",
    "RecordRejectedError",
    "SchemaViolationError",
    "SemanticViolationError",
    "StorageError",
    "TenantProfile",
    "TenantRegistry",
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

class ProtectionError(DatalakeError):
    """PII protection failed (Level 6); nothing was written."""

@final
class PiiProtection:
    """A PII protection scheme for a :class:`TenantProfile`."""

    @staticmethod
    def sha256() -> PiiProtection:
        """Unkeyed SHA-256 (hex). Deterministic; weak for low-entropy values."""
    @staticmethod
    def hmac_sha256(key: bytes) -> PiiProtection:
        """Keyed HMAC-SHA256 (hex); ``key`` has 16+ bytes and lives outside the lake."""
    @staticmethod
    def aes_256_gcm(data_key: bytes, *, key_ref: str) -> PiiProtection:
        """AES-256-GCM envelope encryption with a 32-byte data key.

        ``key_ref`` is what readers need to recover the key, e.g. the
        ``CiphertextBlob`` returned by AWS KMS ``GenerateDataKey``.
        """
    @staticmethod
    def kms_envelope(
        kms_key_id: str,
        *,
        data_key_max_age: float = 3600.0,
        max_nonce_prefixes_per_key: int = 32768,
        region: str | None = None,
        endpoint_url: str | None = None,
    ) -> PiiProtection:
        """AES-256-GCM with a data key per tenant from an AWS KMS key, cached and
        reused until it is ``data_key_max_age`` seconds old (default 1 hour, 60 s
        to 24 h) or has drawn ``max_nonce_prefixes_per_key`` nonces (default 2^15).
        Data keys carry the encryption context ``{tenant_id, purpose}``."""
    @staticmethod
    def envelope(
        provider: Any, *, data_key_max_age: float = 3600.0, max_nonce_prefixes_per_key: int = 32768
    ) -> PiiProtection:
        """AES-256-GCM with data keys from any ``DataKeyProvider`` object, cached per tenant."""
    @staticmethod
    def custom(protector: Any) -> PiiProtection:
        """Wrap an object implementing :class:`datalake_medallion_sdk.FieldProtector`."""
    @property
    def scheme(self) -> str:
        """Scheme name recorded in ``_dl_pii_protection``."""
    @property
    def key_ref(self) -> str | None:
        """Key reference recorded in ``_dl_pii_key_ref``."""
    def decrypt(self, token: str, *, tenant_id: str, field: str) -> str:
        """Decrypt an ``aes-256-gcm`` value written for ``tenant_id`` and ``field``."""

@final
class TenantProfile:
    """Where a tenant's data lives per medallion layer, and how its PII is protected.

    Locations are ``s3://bucket/prefix`` URIs. ``bronze`` and ``quarantine``
    are required; ``silver`` and ``gold`` are optional. Layers may not share
    or nest a location. PII is stored as received unless ``pii_protection``
    is set.
    """

    def __new__(
        cls,
        tenant_id: str,
        *,
        bronze: str,
        quarantine: str,
        silver: str | None = None,
        gold: str | None = None,
        partition_order: Literal["tenant_first", "data_type_first"] = ...,
        pii_protection: PiiProtection | None = None,
    ) -> TenantProfile: ...
    @staticmethod
    def for_bucket(
        tenant_id: str,
        bucket: str,
        *,
        partition_order: Literal["tenant_first", "data_type_first"] = ...,
        pii_protection: PiiProtection | None = None,
    ) -> TenantProfile:
        """Every layer in one bucket under ``bronze/``, ``quarantine/``, ``silver/``, ``gold/``."""
    @property
    def tenant_id(self) -> str: ...
    @property
    def partition_order(self) -> Literal["tenant_first", "data_type_first"]: ...
    @property
    def pii_protection(self) -> str:
        """PII scheme name, or ``none``."""
    def location(self, layer: Literal["bronze", "quarantine", "silver", "gold"]) -> str | None:
        """``s3://bucket/prefix`` of ``layer``, or ``None`` when undefined."""
    def table_uri(
        self, layer: Literal["bronze", "quarantine", "silver", "gold"], data_type: str
    ) -> str | None:
        """``s3://`` prefix of this tenant's ``data_type`` table in ``layer``."""

@final
class TenantRegistry:
    """Immutable directory of tenant profiles, looked up by tenant id."""

    def __new__(cls, profiles: Iterable[TenantProfile]) -> TenantRegistry: ...
    @staticmethod
    def from_config(config: Mapping[str, Any] | str | PathLike[str]) -> TenantRegistry:
        """Registry from a configuration document: a dict, or the path of a JSON file.

        Unknown fields are rejected; secrets never live in the document (HMAC keys
        come from the environment variable named by ``key_env``).
        """
    def get(self, tenant_id: str) -> TenantProfile:
        """Profile of ``tenant_id``; raises :class:`TenantViolationError` when unknown."""
    def tenant_ids(self) -> list[str]: ...
    def __len__(self) -> int: ...
    def __contains__(self, tenant_id: str, /) -> bool: ...

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
    def tenant_id(self) -> str:
        """Tenant the records were written for."""
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
    def pii_protection(self) -> str:
        """PII scheme of the tenant profile (``none`` when stored as received)."""
    @property
    def dlq_uris(self) -> list[str]:
        """``s3://`` URIs of the dead-letter objects, one per error code."""

@final
class BronzeIngestionClient:
    """Validating Bronze writer for S3 and S3-compatible object stores.

    Holds no destination: each call names its tenant, whose profile decides
    where the records go. The S3 client is created on the first ``ingest``
    and reused for every bucket.
    """

    def __new__(
        cls,
        registry: ContractRegistry,
        *,
        tenants: TenantRegistry | None = None,
        envelope_prefix: str = ...,
        max_record_bytes: int = ...,
        max_future_skew_seconds: int | None = ...,
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
        tenant: TenantProfile | str,
        contract: str,
    ) -> IngestReport:
        """Validate ``batch`` against ``contract`` and write it for ``tenant``.

        ``batch`` is a dict, a list of dicts, JSON ``str``/``bytes``, or an
        object implementing ``__arrow_c_stream__`` / ``__arrow_c_array__``
        (pyarrow, Polars, ...).

        Raises:
            TenantViolationError: invalid or unknown tenant.
            ProtectionError: the tenant's PII protector failed.
            ConfigurationError: unknown ``contract``.
            MalformedPayloadError: the batch cannot be decoded.
            RecordRejectedError: a record is invalid and ``on_invalid="raise"``.
            StorageError: encoding or upload failed.
        """
    @property
    def tenant_ids(self) -> list[str]: ...
    @property
    def hooked_contracts(self) -> list[str]: ...

@final
class PiiDecryptor:
    """Reads AES-256-GCM PII values back, recovering each data key once and caching it."""

    @staticmethod
    def kms(
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        cache_size: int = 1024,
        max_age: float = 3600.0,
    ) -> PiiDecryptor:
        """Decryptor for keys issued by AWS KMS (``kms1:`` key references)."""
    @staticmethod
    def from_provider(
        provider: Any, *, cache_size: int = 1024, max_age: float = 3600.0
    ) -> PiiDecryptor:
        """Decryptor for keys from a ``DataKeyProvider`` object."""
    def decrypt(self, token: str, *, tenant_id: str, field: str, key_ref: str) -> str:
        """Decrypt one token written for ``tenant_id`` and ``field`` with ``key_ref``."""
    def decrypt_many(
        self, tokens: Iterable[str | None], *, tenant_id: str, field: str, key_ref: str
    ) -> list[str | None]:
        """Decrypt a column (``None`` stays ``None``) without holding the GIL."""
