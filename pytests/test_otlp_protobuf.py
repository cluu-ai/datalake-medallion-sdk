"""OTLP/protobuf logs: the same rows as OTLP/JSON, compressed requests and limits (moto S3).

Requests are built with the official ``opentelemetry-proto`` package, so the
reader is checked against an independent encoder: each request is serialized
to protobuf and to OTLP/JSON, and both must flatten to identical batches.
"""

from __future__ import annotations

import base64
import gzip
import json
import math
from collections.abc import Callable
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    MalformedPayloadError,
    PayloadTooLargeError,
)
from datalake_medallion_sdk.otel import flatten_logs, logs_contract

from .conftest import Bucket
from .test_otel import ulid_now

pytest.importorskip("opentelemetry.proto")
from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    ArrayValue,
    InstrumentationScope,
    KeyValue,
    KeyValueList,
)
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, ResourceLogs, ScopeLogs
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

pa = pytest.importorskip("pyarrow")

try:
    from compression import zstd as _zstd  # type: ignore[import-not-found]  # Python 3.14

    def zstd(data: bytes) -> bytes:
        return bytes(_zstd.compress(data))
except ImportError:
    import zstandard

    def zstd(data: bytes) -> bytes:
        return zstandard.ZstdCompressor().compress(data)


MakeClient = Callable[..., BronzeIngestionClient]
CONTRACT = "otel/logs/1.0.0"
NS = 1_772_359_200_000_000_000


def s(value: str) -> AnyValue:
    return AnyValue(string_value=value)


def kv(key: str, value: AnyValue | None = None) -> KeyValue:
    return KeyValue(key=key) if value is None else KeyValue(key=key, value=value)


def every_value() -> list[KeyValue]:
    deep = AnyValue(kvlist_value=KeyValueList(values=[kv("deep", AnyValue(bool_value=True))]))
    array = AnyValue(array_value=ArrayValue(values=[s("x"), AnyValue(int_value=1), deep]))
    return [
        kv("str", s('héllo "wörld"\n\t\x01 😀')),
        kv("empty_str", s("")),
        kv("true", AnyValue(bool_value=True)),
        kv("false", AnyValue(bool_value=False)),
        kv("int_min", AnyValue(int_value=-(2**63))),
        kv("int_max", AnyValue(int_value=2**63 - 1)),
        kv("zero", AnyValue(int_value=0)),
        kv("double", AnyValue(double_value=3.14159)),
        kv("tiny", AnyValue(double_value=5e-324)),
        kv("nan", AnyValue(double_value=math.nan)),
        kv("inf", AnyValue(double_value=-math.inf)),
        kv("bytes", AnyValue(bytes_value=b"\x00\x01\xfe\xff")),
        kv("empty_list", AnyValue(array_value=ArrayValue())),
        kv("empty_map", AnyValue(kvlist_value=KeyValueList())),
        kv("unset", AnyValue()),
        kv("missing"),
        kv("nested", AnyValue(kvlist_value=KeyValueList(values=[kv("a", array)]))),
    ]


def request() -> ExportLogsServiceRequest:
    acme = Resource(attributes=[kv("service.name", s("checkout")), kv("tenant.id", s("acme-001"))])
    acme_records = [
        LogRecord(
            time_unix_nano=NS + 1,
            observed_time_unix_nano=NS + 2,
            severity_number=9,  # type: ignore[arg-type]
            severity_text="INFO",
            body=s("order placed"),
            attributes=every_value(),
            trace_id=bytes(range(16)),
            span_id=bytes(range(8)),
            flags=1,
            event_name="order.placed",
        ),
        LogRecord(
            observed_time_unix_nano=NS + 3,
            severity_number=17,  # type: ignore[arg-type]
            body=AnyValue(kvlist_value=KeyValueList(values=[kv("error", s("boom"))])),
        ),
        LogRecord(
            time_unix_nano=NS + 4,
            trace_id=bytes(16),
            span_id=b"",
            body=AnyValue(
                array_value=ArrayValue(
                    values=[AnyValue(double_value=1.5), AnyValue(bytes_value=b"")]
                )
            ),
        ),
        LogRecord(),  # nothing set: no timestamp, quarantined on ingest
    ]
    return ExportLogsServiceRequest(
        resource_logs=[
            ResourceLogs(
                resource=acme,
                scope_logs=[
                    ScopeLogs(
                        scope=InstrumentationScope(name="app", version="1.2"),
                        log_records=acme_records,
                    )
                ],
            ),
            ResourceLogs(
                resource=Resource(attributes=[kv("tenant.id", s("tenant-a"))]),
                scope_logs=[ScopeLogs(log_records=[LogRecord(time_unix_nano=NS + 5, body=s("ü"))])],
            ),
            ResourceLogs(
                scope_logs=[
                    ScopeLogs(
                        log_records=[
                            LogRecord(
                                time_unix_nano=NS + 6, attributes=[kv("tenant.id", s("tenant-a"))]
                            ),
                            LogRecord(time_unix_nano=NS + 7, body=AnyValue(double_value=math.inf)),
                        ]
                    )
                ]
            ),
        ]
    )


def otlp_json(message: ExportLogsServiceRequest) -> bytes:
    """OTLP/JSON: the protobuf JSON mapping, with hex ids and integer enums as
    the OTLP specification requires."""
    data: dict[str, Any] = MessageToDict(message, use_integers_for_enums=True)
    for resource_logs in data.get("resourceLogs", []):
        for scope_logs in resource_logs.get("scopeLogs", []):
            for record in scope_logs.get("logRecords", []):
                for key in ("traceId", "spanId"):
                    if key in record:
                        record[key] = base64.b64decode(record[key]).hex()
    return json.dumps(data).encode()


def assert_same(a: dict[str | None, Any], b: dict[str | None, Any]) -> None:
    assert a.keys() == b.keys()
    for tenant in a:
        for name in a[tenant].schema.names:
            assert a[tenant].column(name).equals(b[tenant].column(name)), (tenant, name)


def client(make_client: MakeClient) -> BronzeIngestionClient:
    return make_client(registry=ContractRegistry.from_schemas([logs_contract()]))


def test_protobuf_and_json_give_the_same_rows() -> None:
    message = request()
    pb, js = message.SerializeToString(), otlp_json(message)
    expected = flatten_logs(js)
    assert set(expected) == {None, "acme-001", "tenant-a"}
    for body in (pb, gzip.compress(pb), zstd(pb), gzip.compress(js), zstd(js)):
        assert_same(flatten_logs(body), expected)
    assert_same(flatten_logs(bytearray(pb)), expected)
    assert_same(flatten_logs(memoryview(pb)), expected)
    assert_same(flatten_logs(json.loads(js)), expected)
    assert_same(flatten_logs(pb, encoding="protobuf", compression="none"), expected)
    assert_same(flatten_logs(gzip.compress(js), encoding="json", compression="gzip"), expected)


def test_values_are_read_as_otlp_json_carries_them() -> None:
    groups = flatten_logs(request().SerializeToString())
    acme = groups["acme-001"].to_pylist()
    first = acme[0]
    assert first["trace_id"] == bytes(range(16)).hex()
    assert first["span_id"] == bytes(range(8)).hex()
    assert first["event_name"] == "order.placed"
    attributes = json.loads(first["attributes"])
    assert attributes["int_min"] == -(2**63) and attributes["int_max"] == 2**63 - 1
    assert attributes["bytes"] == base64.b64encode(b"\x00\x01\xfe\xff").decode()
    assert attributes["nan"] is None and attributes["inf"] is None
    assert attributes["unset"] is None and attributes["missing"] is None
    assert attributes["nested"] == {"a": ["x", 1, {"deep": True}]}
    assert attributes["str"] == 'héllo "wörld"\n\t\x01 😀'
    assert json.loads(acme[1]["body"]) == {"error": "boom"}
    assert acme[1]["timestamp"] == acme[1]["observed_timestamp"], "falls back to observed time"
    assert acme[2]["trace_id"] is None and acme[2]["span_id"] is None, "all-zero and empty ids"
    assert acme[2]["body"] == '[1.5,""]'
    assert acme[3]["timestamp"] is None and acme[3]["severity_number"] is None
    assert groups["tenant-a"].num_rows == 2, "a record attribute names the tenant too"
    assert groups[None].to_pylist()[0]["body"] is None, "infinite doubles are null"


def test_settings_that_do_not_match_fail() -> None:
    pb = request().SerializeToString()
    with pytest.raises(MalformedPayloadError):
        flatten_logs(pb, encoding="json")
    with pytest.raises(MalformedPayloadError):
        flatten_logs(gzip.compress(pb), compression="none")
    with pytest.raises(MalformedPayloadError):
        flatten_logs(pb[:-5])


def test_decompression_is_limited() -> None:
    bomb = gzip.compress(b" " * (3 * 1024 * 1024))
    with pytest.raises(PayloadTooLargeError):
        flatten_logs(bomb, max_decompressed_mb=1)
    assert issubclass(PayloadTooLargeError, MalformedPayloadError)


async def test_mixed_batch_is_ingested_once_per_tenant(
    make_client: MakeClient, store: Bucket
) -> None:
    message = request()
    pb, js = message.SerializeToString(), otlp_json(message)
    bomb = zstd(b" " * (3 * 1024 * 1024))
    payloads: list[Any] = [pb, gzip.compress(js), zstd(pb), json.loads(js), b"\x0b", bomb]
    batch_id = ulid_now("4ZA8K2N4P6R8T0W2")
    c = client(make_client)
    outcomes = await c.ingest_otel_log_batch(
        payloads,
        contract=CONTRACT,
        default_tenant="tenant-b",
        batch_id=batch_id,
        max_decompressed_mb=1,
    )
    written = {o.report.tenant_id: o for o in outcomes if o.report is not None}
    assert set(written) == {"acme-001", "tenant-a", "tenant-b"}
    acme = written["acme-001"]
    assert acme.requests == [0, 1, 2, 3]
    assert acme.report is not None
    assert (acme.report.records_ok, acme.report.records_quarantined) == (12, 4)
    failed = {o.requests[0]: o.error for o in outcomes if o.error is not None}
    assert set(failed) == {4, 5}
    assert isinstance(failed[4], MalformedPayloadError)
    assert isinstance(failed[5], PayloadTooLargeError)
    keys = store.keys()
    assert len([k for k in keys if "tenant_id=acme-001/" in k and "/_" not in k]) >= 1
    again = await c.ingest_otel_log_batch(
        payloads,
        contract=CONTRACT,
        default_tenant="tenant-b",
        batch_id=batch_id,
        max_decompressed_mb=1,
    )
    assert store.keys() == keys, "a redelivered batch rewrites the same keys"
    assert {o.report.data_uri for o in again if o.report} == {
        o.report.data_uri for o in outcomes if o.report
    }
    table = store.parquet(acme.report.data_uri)
    assert table.column("timestamp").type == pa.timestamp("us", tz="UTC")
    assert table.num_rows == 12
