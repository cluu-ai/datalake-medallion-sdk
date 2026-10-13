from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass

import pytest
from datalake_medallion_sdk import (
    EmfMetricsSink,
    InMemoryMetricsSink,
    LoggingMetricsSink,
    MetricsSink,
    SchemaViolationError,
    StorageError,
    datalake_retry,
    emit_metrics,
)


async def test_retry_recovers_from_transient_storage_errors() -> None:
    calls = 0

    @datalake_retry(attempts=3, base_delay=0)
    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise StorageError("unavailable")
        return "ok"

    assert await flaky() == "ok"
    assert calls == 3


async def test_retry_gives_up_after_attempts() -> None:
    calls = 0

    @datalake_retry(attempts=2, base_delay=0)
    async def always_fails() -> None:
        nonlocal calls
        calls += 1
        raise StorageError("down")

    with pytest.raises(StorageError):
        await always_fails()
    assert calls == 2


async def test_retry_does_not_retry_deterministic_errors() -> None:
    calls = 0

    @datalake_retry(attempts=5, base_delay=0)
    async def invalid() -> None:
        nonlocal calls
        calls += 1
        raise SchemaViolationError("bad record")

    with pytest.raises(SchemaViolationError):
        await invalid()
    assert calls == 1


@pytest.mark.parametrize("kwargs", [{"attempts": 0}, {"base_delay": -1}, {"max_delay": -1}])
def test_retry_validates_arguments(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        datalake_retry(**kwargs)  # type: ignore[arg-type]


@dataclass
class FakeReport:
    records_ok: int
    records_quarantined: int


async def test_emit_metrics_records_counts_and_duration() -> None:
    sink = InMemoryMetricsSink()

    @emit_metrics(sink, operation="ingest_events")
    async def ingest() -> FakeReport:
        return FakeReport(records_ok=7, records_quarantined=2)

    await ingest()
    assert sink.values("RecordsOk") == [7]
    assert sink.values("RecordsQuarantined") == [2]
    assert len(sink.values("DurationMs")) == 1
    assert all(m.dimensions == {"operation": "ingest_events"} for m in sink.metrics)


async def test_emit_metrics_counts_failures_and_reraises() -> None:
    sink = InMemoryMetricsSink()

    @emit_metrics(sink)
    async def broken() -> None:
        raise StorageError("down")

    with pytest.raises(StorageError):
        await broken()
    assert sink.values("Failures") == [1]
    assert len(sink.values("DurationMs")) == 1
    assert sink.values("RecordsOk") == []


def test_emf_sink_writes_valid_embedded_metric_format() -> None:
    out = io.StringIO()
    EmfMetricsSink("MyApp", stream=out).emit("RecordsOk", 5, "Count", {"operation": "ingest"})
    doc = json.loads(out.getvalue())
    directive = doc["_aws"]["CloudWatchMetrics"][0]
    assert directive["Namespace"] == "MyApp"
    assert directive["Dimensions"] == [["operation"]]
    assert directive["Metrics"] == [{"Name": "RecordsOk", "Unit": "Count"}]
    assert doc["RecordsOk"] == 5 and doc["operation"] == "ingest"
    assert isinstance(doc["_aws"]["Timestamp"], int)


def test_emf_sink_validates_namespace() -> None:
    with pytest.raises(ValueError):
        EmfMetricsSink("")


def test_logging_sink(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="datalake_medallion_sdk.metrics"):
        LoggingMetricsSink().emit("RecordsOk", 3, "Count", {"operation": "ingest"})
    assert "RecordsOk=3" in caplog.text


def test_sinks_satisfy_protocol() -> None:
    for sink in (LoggingMetricsSink(), EmfMetricsSink(stream=io.StringIO()), InMemoryMetricsSink()):
        assert isinstance(sink, MetricsSink)
