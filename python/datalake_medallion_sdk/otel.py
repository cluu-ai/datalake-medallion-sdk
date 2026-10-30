"""OpenTelemetry logs in Bronze.

Applications that already emit logs with OpenTelemetry send them to the lake
as OTLP. :func:`logs_contract` is the contract of the flattened records (one
row per log record, the columns of the OpenTelemetry Logs Data Model), and
:meth:`BronzeIngestionClient.ingest_otel_logs` ingests an OTLP request for
every tenant it contains. :func:`flatten_logs` returns the same rows as
pyarrow batches, for inspection or custom pipelines.

Both OTLP encodings are read and give the same rows: protobuf (OTLP/HTTP
``application/x-protobuf``, the Collector's ``awss3exporter`` with
``marshaler: otlp_proto``) and JSON (``application/json``, ``otlp_json``),
each optionally gzip or zstd compressed. With ``encoding="auto"`` and
``compression="auto"`` (the defaults) both are detected per request.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from . import _core

if TYPE_CHECKING:
    import pyarrow as pa

    from .client import OtlpCompression, OtlpEncoding, OtlpPayload

__all__ = ["flatten_logs", "logs_contract", "otlp_bytes"]

TENANT_ATTRIBUTE = "tenant.id"
"""Resource attribute that names the tenant by default."""


def logs_contract(
    *,
    domain: str = "otel",
    datatype: str = "logs",
    schema_version: str = "1.0.0",
    compression: str = "zstd",
    partition_by_event_time: bool = False,
) -> dict[str, Any]:
    """Contract (JSON Schema) for flattened OTel log records.

    Columns: ``timestamp`` and ``observed_timestamp`` (stored as
    ``timestamp(us, UTC)``), ``severity_number``, ``severity_text``, ``body``
    (text, or JSON text for structured bodies), ``attributes`` and
    ``resource_attributes`` (JSON text), ``trace_id``, ``span_id`` (hex),
    ``flags``, ``event_name``, ``service_name``, ``scope_name`` and
    ``scope_version``. Only ``timestamp`` is required: the record's time, or
    the time it was observed when the record has none.

    zstd is the default here because logs are text-heavy (about 40% smaller
    than Snappy). Event-day partitions are off: turn them on only for high
    daily volumes per tenant (see the README guidance).
    """
    return dict(
        json.loads(
            _core.otel_logs_contract(
                domain=domain,
                datatype=datatype,
                schema_version=schema_version,
                compression=compression,
                partition_by_event_time=partition_by_event_time,
            )
        )
    )


def otlp_bytes(payload: OtlpPayload, *, encoding: OtlpEncoding = "auto") -> bytes:
    """One OTLP request as ``bytes``, without decoding it.

    ``bytes`` pass through unchanged (``bytearray`` and ``memoryview`` are
    copied); ``str`` and mappings are OTLP/JSON, so they cannot be read with
    ``encoding="protobuf"``.
    """
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray | memoryview):
        return bytes(payload)
    if not isinstance(payload, str | Mapping):
        raise TypeError(
            "an OTLP request must be bytes, bytearray, memoryview, str or a mapping, "
            f"not {type(payload).__name__}"
        )
    if encoding == "protobuf":
        raise TypeError("str and mapping OTLP requests are JSON; pass protobuf as bytes")
    if isinstance(payload, str):
        return payload.encode()
    return json.dumps(payload, separators=(",", ":")).encode()


def flatten_logs(
    payload: OtlpPayload,
    *,
    tenant_attribute: str = TENANT_ATTRIBUTE,
    encoding: OtlpEncoding = "auto",
    compression: OtlpCompression = "auto",
    max_decompressed_mb: int = 64,
) -> dict[str | None, pa.RecordBatch]:
    """Flatten an OTLP logs request into one pyarrow batch per tenant.

    The request is read as in
    :meth:`BronzeIngestionClient.ingest_otel_logs` (protobuf or JSON,
    optionally compressed). The tenant is the string value of the resource
    attribute ``tenant_attribute`` (or of a record attribute with that name);
    records without it are under ``None``. Needs pyarrow.
    """
    pairs = _core.flatten_otlp_logs(
        otlp_bytes(payload, encoding=encoding),
        tenant_attribute=tenant_attribute,
        encoding=encoding,
        compression=compression,
        max_decompressed_bytes=max_decompressed_mb * 1024 * 1024,
    )
    return dict(pairs)
