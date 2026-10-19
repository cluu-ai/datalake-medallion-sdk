"""Public ingestion client."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Literal, TypedDict, Unpack

from . import _core
from ._core import (
    ConfigurationError,
    ContractRegistry,
    IngestReport,
    StreamReport,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
)
from .tenancy import TenantResolver

if TYPE_CHECKING:
    import pyarrow.fs as pafs

FileFormat = Literal["csv", "tsv", "jsonl", "parquet"]
_EXTENSIONS: dict[str, FileFormat] = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".json": "jsonl",
    ".parquet": "parquet",
    ".pq": "parquet",
}
_COMPRESSIONS = (".gz", ".bz2", ".zst", ".lz4")
_MIB = 1024 * 1024

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

    __slots__ = ("_native", "_resolver", "_s3")

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
        self._s3 = {
            k: native_options.get(k)
            for k in (
                "aws_region",
                "aws_endpoint_url",
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
            )
        }

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
        return await self._native.ingest(batch, tenant=self._resolve(tenant), contract=contract)

    async def ingest_stream(
        self,
        source: Any,
        *,
        tenant: TenantProfile | str,
        contract: str,
        stream_id: str | None = None,
        target_object_mb: int = 256,
        part_mb: int = 16,
        row_group_mb: int = 64,
        chunk_rows: int = 131_072,
        max_inflight_parts: int = 4,
        parse_text_columns: bool = False,
        json_lines: bool = False,
    ) -> StreamReport:
        """Ingest input of any size with bounded memory.

        Records are validated in chunks exactly like :meth:`ingest`, and
        written as Parquet objects of about ``target_object_mb`` that are
        uploaded to S3 in ``part_mb`` parts while the next chunk is validated.
        Memory stays at about one chunk, one ``row_group_mb`` row group and
        ``max_inflight_parts`` parts, whatever the input size.

        ``source`` is an Arrow stream (any object with ``__arrow_c_stream__``:
        a pyarrow ``RecordBatchReader``, ``Table`` or dataset scanner, a Polars
        data frame, a DuckDB result...), read batch by batch without the GIL;
        or an iterable of chunks, each anything :meth:`ingest` accepts (dicts,
        lists of dicts, JSON, Arrow batches). With ``json_lines=True``,
        ``str``/``bytes`` chunks are JSON Lines. With
        ``parse_text_columns=True``, text columns of ``integer``, ``number``
        and ``boolean`` fields are parsed (CSV read as text); a value that does
        not parse quarantines its record with the original text.

        Objects become visible only when complete and are named
        ``<stream_id>-00000.parquet``, ``-00001``... Retrying a failed stream
        with the same ``stream_id`` rewrites the same keys, so no row is
        duplicated. Use :meth:`ingest_file` for CSV, JSON Lines and Parquet
        files, local or on S3.

        Raises:
            As :meth:`ingest`; on error the unfinished object is discarded.
        """
        return await self._native.ingest_stream(
            source,
            tenant=self._resolve(tenant),
            contract=contract,
            stream_id=stream_id,
            target_object_mb=target_object_mb,
            part_mb=part_mb,
            row_group_mb=row_group_mb,
            chunk_rows=chunk_rows,
            max_inflight_parts=max_inflight_parts,
            parse_text_columns=parse_text_columns,
            json_lines=json_lines,
        )

    async def ingest_file(
        self,
        path: str | os.PathLike[str],
        *,
        tenant: TenantProfile | str,
        contract: str,
        format: FileFormat | None = None,
        stream_id: str | None = None,
        target_object_mb: int = 256,
        block_mb: int = 8,
        row_group_mb: int = 64,
        chunk_rows: int = 131_072,
        max_inflight_parts: int = 4,
    ) -> StreamReport:
        """Stream a CSV, TSV, JSON Lines or Parquet file into Bronze.

        ``path`` is a local path or an ``s3://bucket/key`` URI (read with the
        client's AWS settings); ``.gz``, ``.bz2``, ``.zst`` and ``.lz4``
        files are decompressed on the fly. The format comes from the
        extension unless ``format`` is given. Files are read ``block_mb`` at a
        time, so memory does not depend on the file size. Needs pyarrow
        (``pip install "datalake-medallion-sdk[pyarrow]"``).

        CSV is read as text and parsed with the contract's types (empty cells
        are absent values); JSON Lines lines are validated one by one, a
        malformed line quarantining only itself; Parquet is read one row
        group at a time.
        """
        fs, location = self._filesystem(os.fspath(path))
        name = location.lower()
        for suffix in _COMPRESSIONS:
            name = name.removesuffix(suffix)
        kind = format or _EXTENSIONS.get(os.path.splitext(name)[1])
        if kind is None:
            raise ConfigurationError(
                f"cannot tell the format of {location!r}; pass format='csv', 'tsv', "
                "'jsonl' or 'parquet'"
            )
        common: dict[str, Any] = {
            "tenant": tenant,
            "contract": contract,
            "stream_id": stream_id,
            "target_object_mb": target_object_mb,
            "row_group_mb": row_group_mb,
            "chunk_rows": chunk_rows,
            "max_inflight_parts": max_inflight_parts,
        }
        if kind == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq

            with fs.open_input_file(location) as handle:
                parquet = pq.ParquetFile(handle)
                reader = pa.RecordBatchReader.from_batches(
                    parquet.schema_arrow, parquet.iter_batches(batch_size=chunk_rows)
                )
                return await self.ingest_stream(reader, **common)
        with fs.open_input_stream(location, compression="detect") as handle:
            if kind == "jsonl":
                return await self.ingest_stream(
                    _line_blocks(handle, block_mb * _MIB), json_lines=True, **common
                )
            return await self.ingest_stream(
                _csv_reader(fs, location, handle, kind == "tsv", block_mb * _MIB),
                parse_text_columns=True,
                **common,
            )

    def _resolve(self, tenant: TenantProfile | str) -> TenantProfile | str:
        if isinstance(tenant, str) and self._resolver is not None:
            profile = self._resolver(tenant)
            if not isinstance(profile, TenantProfile):
                raise TypeError("tenant resolver must return a TenantProfile")
            if profile.tenant_id != tenant:
                raise TenantViolationError(
                    f"tenant resolver returned the profile of {profile.tenant_id!r} for {tenant!r}"
                )
            return profile
        return tenant

    def _filesystem(self, path: str) -> tuple[pafs.FileSystem, str]:
        try:
            import pyarrow.fs as pafs
        except ImportError as e:  # pragma: no cover - depends on the environment
            raise ConfigurationError(
                "ingest_file needs pyarrow: pip install 'datalake-medallion-sdk[pyarrow]'"
            ) from e
        if not path.startswith("s3://"):
            return pafs.LocalFileSystem(), os.path.abspath(path)
        s3 = self._s3
        endpoint = s3["aws_endpoint_url"]
        scheme, _, host = (endpoint or "").partition("://")
        fs = pafs.S3FileSystem(
            region=s3["aws_region"],
            access_key=s3["aws_access_key_id"],
            secret_key=s3["aws_secret_access_key"],
            session_token=s3["aws_session_token"],
            endpoint_override=host or None,
            scheme=scheme or "https",
        )
        return fs, path.removeprefix("s3://")

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


def _line_blocks(handle: Any, block_bytes: int) -> Iterator[bytes]:
    """Blocks of about ``block_bytes`` that end on a line break."""
    carry = b""
    while True:
        block = handle.read(block_bytes)
        if not block:
            break
        block = carry + block
        cut = block.rfind(b"\n")
        if cut < 0:
            carry = block
            continue
        carry = block[cut + 1 :]
        yield block[: cut + 1]
    if carry.strip():
        yield carry


def _csv_reader(
    fs: pafs.FileSystem, location: str, handle: Any, tab: bool, block_bytes: int
) -> Any:
    """Streaming CSV reader with every column as text (types come from the contract)."""
    import pyarrow as pa
    import pyarrow.csv as pcsv

    parse = pcsv.ParseOptions(delimiter="\t" if tab else ",")
    read = pcsv.ReadOptions(block_size=block_bytes)
    # The header names are needed to read every column as text.
    with fs.open_input_stream(location, compression="detect") as probe:
        names = pcsv.open_csv(
            probe, read_options=pcsv.ReadOptions(block_size=1 << 16), parse_options=parse
        ).schema.names
    convert = pcsv.ConvertOptions(
        column_types=dict.fromkeys(names, pa.string()), strings_can_be_null=True
    )
    return pcsv.open_csv(handle, read_options=read, parse_options=parse, convert_options=convert)
