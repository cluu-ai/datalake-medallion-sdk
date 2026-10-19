"""Optional OpenTelemetry instrumentation (moto S3, in-memory OTel SDK)."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    LakeReader,
    TableRef,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
)

from .conftest import CONTRACT

pytest.importorskip("opentelemetry.sdk")
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import Counter, Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import AggregationTemporality, InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

MakeClient = Callable[..., BronzeIngestionClient]
SPANS = InMemorySpanExporter()
# Delta temporality: each collection returns only what was recorded since the last.
METRICS = InMemoryMetricReader(
    preferred_temporality={
        Counter: AggregationTemporality.DELTA,
        Histogram: AggregationTemporality.DELTA,
    }
)


@pytest.fixture(scope="module", autouse=True)
def otel() -> None:
    """Global providers can be set once per process: set them for the module."""
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(SPANS))
    trace.set_tracer_provider(provider)
    metrics.set_meter_provider(MeterProvider(metric_readers=[METRICS]))


@pytest.fixture(autouse=True)
def clear() -> Iterator[None]:
    SPANS.clear()
    METRICS.get_metrics_data()  # drop what earlier tests recorded
    yield


def snapshot() -> dict[str, list[Any]]:
    """Data points by metric name, recorded since the last snapshot (delta)."""
    data = METRICS.get_metrics_data()
    out: dict[str, list[Any]] = {}
    for rm in data.resource_metrics if data else []:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                out.setdefault(m.name, []).extend(m.data.data_points)
    return out


def records(snap: dict[str, list[Any]], outcome: str) -> int:
    return sum(
        p.value
        for p in snap.get("datalake.records", [])
        if p.attributes["datalake.outcome"] == outcome
    )


def rows(n: int) -> list[dict[str, Any]]:
    return [{"event_id": f"e{i}", "attempts": -1 if i % 10 == 9 else i} for i in range(n)]


async def test_ingest_span_and_metrics(make_client: MakeClient) -> None:
    report = await make_client().ingest(rows(20), tenant="acme-001", contract=CONTRACT)
    (span,) = [s for s in SPANS.get_finished_spans() if s.name == "datalake.ingest"]
    attrs = dict(span.attributes or {})
    assert attrs["tenant.id"] == "acme-001"
    assert attrs["datalake.contract"] == CONTRACT
    assert attrs["datalake.data_type"] == "login_event"
    assert attrs["datalake.layer"] == "bronze"
    assert attrs["datalake.batch_id"] == report.batch_id
    assert (attrs["datalake.records.ok"], attrs["datalake.records.quarantined"]) == (18, 2)
    snap = snapshot()
    assert (records(snap, "ok"), records(snap, "quarantined")) == (18, 2)
    (duration,) = [
        p
        for p in snap["datalake.operation.duration"]
        if p.attributes["datalake.operation"] == "ingest"
    ]
    assert duration.count == 1 and "error.type" not in duration.attributes


async def test_files_are_counted_once(make_client: MakeClient, tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("\n".join(f'{{"event_id": "e{i}", "attempts": {i}}}' for i in range(50)))
    report = await make_client().ingest_file(path, tenant="acme-001", contract=CONTRACT)
    names = [s.name for s in SPANS.get_finished_spans()]
    assert "datalake.ingest_file" in names and "datalake.ingest_stream" in names
    stream = next(s for s in SPANS.get_finished_spans() if s.name == "datalake.ingest_stream")
    parent = next(s for s in SPANS.get_finished_spans() if s.name == "datalake.ingest_file")
    assert stream.parent is not None and stream.parent.span_id == parent.context.span_id
    snap = snapshot()
    assert records(snap, "ok") == report.records_ok == 50, "not counted twice"
    assert sum(p.value for p in snap["datalake.bytes"]) == report.bytes_written


async def test_failures_record_the_error_type(make_client: MakeClient) -> None:
    with pytest.raises(TenantViolationError):
        await make_client().ingest(rows(2), tenant="nobody", contract=CONTRACT)
    (span,) = SPANS.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    (duration,) = snapshot()["datalake.operation.duration"]
    assert duration.attributes["error.type"] == "TenantViolationError"


async def test_reads_are_spans(make_client: MakeClient, s3_endpoint: str, bucket: str) -> None:
    await make_client().ingest(rows(10), tenant="acme-001", contract=CONTRACT)
    reader = LakeReader(
        TenantRegistry([TenantProfile.for_bucket("acme-001", bucket)]),
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    try:
        reader.read("acme-001", TableRef.bronze("login_event"))
    finally:
        reader.close()
    (span,) = [s for s in SPANS.get_finished_spans() if s.name == "datalake.read"]
    attrs = dict(span.attributes or {})
    assert (attrs["datalake.rows"], attrs["datalake.data_type"], attrs["tenant.id"]) == (
        9,
        "login_event",
        "acme-001",
    )


def test_without_opentelemetry_everything_is_unchanged() -> None:
    code = """
import sys
sys.modules["opentelemetry"] = None  # as if the [otel] extra were not installed
from datalake_medallion_sdk import _telemetry, BronzeIngestionClient, LakeReader
assert not _telemetry.enabled()
assert BronzeIngestionClient.ingest.__qualname__ == "BronzeIngestionClient.ingest"
assert not hasattr(BronzeIngestionClient.ingest, "__wrapped__"), "no wrapper at all"
assert not hasattr(LakeReader.read, "__wrapped__")
with _telemetry.operation("ingest", tenant="t") as done:
    done(None)
print("ok")
"""
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.stdout.strip() == "ok", out.stderr
