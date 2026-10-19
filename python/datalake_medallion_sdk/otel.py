"""OpenTelemetry logs in Bronze.

Applications that already emit logs with OpenTelemetry send them to the lake
as OTLP. :func:`logs_contract` is the contract of the flattened records (one
row per log record, the columns of the OpenTelemetry Logs Data Model), and
:meth:`BronzeIngestionClient.ingest_otel_logs` ingests an OTLP/JSON request
for every tenant it contains. :func:`flatten_logs` returns the same rows as
pyarrow batches, for inspection or custom pipelines.

Only the OTLP/JSON encoding is read (OTLP/HTTP with ``application/json``, the
Firehose and Collector JSON exports). Configure exporters with JSON encoding.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from . import _core
from ._core import MalformedPayloadError

if TYPE_CHECKING:
    import pyarrow as pa

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


def otlp_bytes(payload: bytes | bytearray | memoryview | str | Mapping[str, Any]) -> bytes:
    """An OTLP/JSON request as bytes; rejects the protobuf encoding with a clear error."""
    if isinstance(payload, Mapping):
        return json.dumps(payload, separators=(",", ":")).encode()
    data = payload.encode() if isinstance(payload, str) else bytes(payload)
    if data.lstrip()[:1] not in (b"{", b""):
        raise MalformedPayloadError(
            "malformed payload: OTLP request is not JSON; only the OTLP/JSON encoding is "
            "supported (configure the exporter with JSON encoding)"
        )
    return data


def flatten_logs(
    payload: bytes | bytearray | memoryview | str | Mapping[str, Any],
    *,
    tenant_attribute: str = TENANT_ATTRIBUTE,
) -> dict[str | None, pa.RecordBatch]:
    """Flatten an OTLP/JSON logs request into one pyarrow batch per tenant.

    The tenant is the string value of the resource attribute
    ``tenant_attribute`` (or of a record attribute with that name); records
    without it are under ``None``. Needs pyarrow.
    """
    pairs = _core.flatten_otlp_logs(otlp_bytes(payload), tenant_attribute=tenant_attribute)
    return dict(pairs)
