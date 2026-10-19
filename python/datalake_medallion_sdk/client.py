"""Public ingestion client."""

from __future__ import annotations

import io
import os
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
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
_CODECS = {".gz": "gzip", ".bz2": "bz2", ".zst": "zstd", ".lz4": "lz4"}
# Ranged reads: a single S3 stream reaches about 40 MiB/s inside a Lambda,
# 4 parallel ranges about 90 MiB/s (measured 2026-10-19, us-east-1).
_RANGE_BYTES = 8 * 1024 * 1024
_RANGES_AHEAD = 4
_MIB = 1024 * 1024

__all__ = ["BronzeIngestionClient", "ClientOptions"]

OtlpPayload = bytes | bytearray | memoryview | str | Mapping[str, Any]
"""One OTLP/JSON logs request: encoded JSON, or the decoded mapping."""


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
        self,
        batch: Any,
        *,
        tenant: TenantProfile | str,
        contract: str,
        batch_id: str | None = None,
    ) -> IngestReport:
        """Validate ``batch`` against ``contract`` and write it for ``tenant``.

        ``tenant`` is a :class:`TenantProfile`, or a tenant id resolved with
        the client's ``tenants``. ``batch`` is a dict, a list of dicts, JSON
        ``str``/``bytes``, or any object implementing the Arrow PyCapsule
        interface (pyarrow, Polars, ...); Arrow is the fastest path.
        Validation, protection and encoding run outside the GIL.

        Uploads are retried (4 attempts, exponential backoff with jitter)
        over the same object keys, so a transient S3 error never duplicates
        rows. ``batch_id`` (a ULID, e.g. ``report.batch_id`` of a failed or
        uncertain earlier call) names the objects: calling again with the
        same id rewrites the same keys instead of adding rows.

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
        return await self._native.ingest(
            batch, tenant=self._resolve(tenant), contract=contract, batch_id=batch_id
        )

    async def ingest_otel_logs(
        self,
        payload: OtlpPayload | list[OtlpPayload],
        *,
        contract: str,
        tenant_attribute: str = "tenant.id",
        default_tenant: TenantProfile | str | None = None,
        batch_id: str | None = None,
    ) -> dict[str, IngestReport]:
        """Ingest an OTLP/JSON logs request for every tenant it contains.

        Records are flattened to the columns of
        :func:`datalake_medallion_sdk.otel.logs_contract` and grouped by the
        resource attribute ``tenant_attribute`` (or a record attribute of that
        name); each tenant id is resolved like ``ingest(tenant="<id>")``.
        Records without it go to ``default_tenant``, or the call raises
        :class:`TenantViolationError` before anything is written. Parsing,
        validation and encoding run in Rust without the GIL and without
        pyarrow; tenants are uploaded concurrently.

        ``batch_id`` names every tenant's objects: pass a deterministic ULID
        (for example derived from a queue message) so a redelivered request
        rewrites the same keys. Every tenant is attempted even when another
        fails; the first error is then raised, and calling again with the same
        ``batch_id`` rewrites what was already written. ``payload`` may also
        be a list of requests
        (the records of one queue batch): their rows are merged by tenant, so
        each tenant is written once. Returns the reports by tenant id.

        Raises:
            MalformedPayloadError: the request is not OTLP/JSON.
            TenantViolationError: a tenant is unknown, or records have no
                tenant and there is no ``default_tenant``.
            As :meth:`ingest` otherwise.
        """
        from .otel import otlp_bytes

        def resolve(tenant_id: str | None) -> TenantProfile | str:
            if tenant_id is not None:
                return self._resolve(tenant_id)
            if default_tenant is None:
                raise TenantViolationError(
                    f"tenant violation: log records without the {tenant_attribute!r} "
                    "attribute and no default_tenant"
                )
            return self._resolve(default_tenant)

        bodies = (
            [otlp_bytes(p) for p in payload] if isinstance(payload, list) else [otlp_bytes(payload)]
        )
        outcomes = await self._native.ingest_otel_logs(
            bodies,
            contract=contract,
            tenant_attribute=tenant_attribute,
            resolve=resolve,
            batch_id=batch_id,
        )
        # Every tenant was attempted; report the first failure after all.
        for _, _, _, error in outcomes:
            if error is not None:
                raise error
        return {report.tenant_id: report for _, _, report, _ in outcomes if report is not None}

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
        max_object_seconds: float | None = None,
        max_open_days: int = 4,
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

        For continuous sources (logs, events), ``max_object_seconds`` closes
        an object once it is that old, so records become visible within a
        bounded time even when traffic is low (checked as chunks arrive).
        When the contract sets ``x-dl-partition_by``, rows go to one object
        per event day (``<stream_id>-<YYYYMMDD>-00000.parquet`` under that
        day's partition); at most ``max_open_days`` are open at once, each
        holding up to one row group in memory.

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
            max_object_seconds=max_object_seconds,
            max_open_days=max_open_days,
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
        max_open_days: int = 4,
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
            "max_open_days": max_open_days,
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
        with _open_stream(fs, location) as handle:
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


class _RangedReader(io.RawIOBase):
    """Sequential reads served by parallel ranged reads of a remote object.

    Keeps ``ahead`` ranges of ``part`` bytes in flight, so memory stays at
    ``ahead * part`` while an object store answers several requests at once.
    """

    def __init__(self, handle: Any, size: int, part: int, ahead: int) -> None:
        self._handle = handle
        self._size = size
        self._part = part
        self._ahead = ahead
        self._pool = ThreadPoolExecutor(ahead, thread_name_prefix="dl-range")
        self._pending: deque[Future[bytes]] = deque()
        self._next = 0
        self._buffer = memoryview(b"")
        self._fill()

    def _fill(self) -> None:
        while len(self._pending) < self._ahead and self._next < self._size:
            length = min(self._part, self._size - self._next)
            self._pending.append(self._pool.submit(self._handle.read_at, length, self._next))
            self._next += length

    def readable(self) -> bool:
        return True

    def readinto(self, target: Any) -> int:
        if not self._buffer:
            if not self._pending:
                return 0
            self._buffer = memoryview(self._pending.popleft().result())
            self._fill()
        n = min(len(target), len(self._buffer))
        target[:n] = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return n

    def close(self) -> None:
        if not self.closed:
            self._pool.shutdown(wait=True, cancel_futures=True)
            self._handle.close()
        super().close()


def _open_stream(fs: Any, location: str, *, ranged: bool = True) -> Any:
    """Readable stream of ``location``, decompressed by extension. S3 objects
    are read with parallel ranged requests."""
    import pyarrow as pa
    import pyarrow.fs as pafs

    codec = next((c for ext, c in _CODECS.items() if location.lower().endswith(ext)), None)
    info = fs.get_file_info(location)
    if ranged and isinstance(fs, pafs.S3FileSystem) and info.size > _RANGE_BYTES:
        raw: Any = io.BufferedReader(
            _RangedReader(fs.open_input_file(location), info.size, _RANGE_BYTES, _RANGES_AHEAD),
            buffer_size=_RANGE_BYTES,
        )
        if codec is None:
            return raw
        return pa.CompressedInputStream(pa.PythonFile(raw, mode="r"), codec)
    return fs.open_input_stream(location, compression="detect")


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
    with _open_stream(fs, location, ranged=False) as probe:
        names = pcsv.open_csv(
            probe, read_options=pcsv.ReadOptions(block_size=1 << 16), parse_options=parse
        ).schema.names
    convert = pcsv.ConvertOptions(
        column_types=dict.fromkeys(names, pa.string()), strings_can_be_null=True
    )
    return pcsv.open_csv(handle, read_options=read, parse_options=parse, convert_options=convert)
