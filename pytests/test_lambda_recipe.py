"""The AWS Lambda recipe (examples/aws_lambda_handler.py) against synthetic events (moto S3)."""

from __future__ import annotations

import base64
import gzip
import importlib.util
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry, batch_id_for
from datalake_medallion_sdk.otel import logs_contract

from .conftest import SCHEMA, Bucket

MakeClient = Callable[..., BronzeIngestionClient]
LOGS = "otel/logs/1.0.0"
EVENTS = "security/login_event/1.0.0"
NOW_MS = int(time.time() * 1000)


def _recipe() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "examples" / "aws_lambda_handler.py"
    spec = importlib.util.spec_from_file_location("aws_lambda_handler", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


recipe = _recipe()


def client(make_client: MakeClient) -> BronzeIngestionClient:
    return make_client(registry=ContractRegistry.from_schemas([logs_contract(), SCHEMA]))


def otlp(tenant: str, n: int) -> bytes:
    records = [
        {"timeUnixNano": str(1_772_359_200_000_000_000 + i), "body": {"stringValue": f"line {i}"}}
        for i in range(n)
    ]
    attrs = [{"key": "tenant.id", "value": {"stringValue": tenant}}]
    body = {
        "resourceLogs": [
            {"resource": {"attributes": attrs}, "scopeLogs": [{"logRecords": records}]}
        ]
    }
    return json.dumps(body).encode()


def ok_by_tenant(result: Any) -> dict[str, int]:
    """Records written per tenant, over every report of the result."""
    out: dict[str, int] = {}
    for report in result.reports:
        out[report.tenant_id] = out.get(report.tenant_id, 0) + report.records_ok
    return out


def report_of(result: Any, tenant: str) -> Any:
    (report,) = [r for r in result.reports if r.tenant_id == tenant]
    return report


def kinesis(*datas: bytes) -> dict[str, Any]:
    return {
        "Records": [
            {
                "eventSource": "aws:kinesis",
                "kinesis": {
                    "sequenceNumber": f"4959{i:04d}",
                    "data": base64.b64encode(d).decode(),
                    "approximateArrivalTimestamp": NOW_MS / 1000,
                },
            }
            for i, d in enumerate(datas)
        ]
    }


def sqs(*bodies: str) -> dict[str, Any]:
    return {
        "Records": [
            {
                "eventSource": "aws:sqs",
                "messageId": f"msg-{i}",
                "body": b,
                "attributes": {"SentTimestamp": str(NOW_MS)},
            }
            for i, b in enumerate(bodies)
        ]
    }


def cloudwatch_payload(group: str, n: int) -> bytes:
    events = [
        {"id": str(i), "timestamp": NOW_MS - 1000 + i, "message": f"msg {i}"} for i in range(n)
    ]
    data = {
        "messageType": "DATA_MESSAGE",
        "logGroup": group,
        "logStream": "stream-1",
        "logEvents": events,
    }
    return gzip.compress(json.dumps(data).encode())


async def test_kinesis_otlp_batch(make_client: MakeClient, store: Bucket) -> None:
    ingest = recipe.LambdaIngestor(client(make_client), contract=LOGS, payload="otlp")
    event = kinesis(otlp("acme-001", 3), otlp("tenant-a", 2), b"not json", otlp("acme-001", 1))
    result = await ingest.ingest(event)
    assert ok_by_tenant(result) == {"acme-001": 4, "tenant-a": 2}
    assert result.failed == ["49590002"], "only the unreadable message is retried"
    assert result.batch_id == batch_id_for(
        ["49590000", "49590001", "49590002", "49590003"], time_ms=NOW_MS
    )
    keys = store.keys()
    assert len([k for k in keys if "tenant_id=acme-001/" in k]) == 1, "one object per tenant"
    again = await ingest.ingest(event)
    assert store.keys() == keys, "a redelivered batch rewrites the same objects"
    assert again.response() == {"batchItemFailures": [{"itemIdentifier": "49590002"}]}


async def test_sqs_json_with_tenant_field(make_client: MakeClient, store: Bucket) -> None:
    ingest = recipe.LambdaIngestor(
        client(make_client), contract=EVENTS, payload="json", tenant_field="tenant"
    )
    event = sqs(
        json.dumps({"tenant": "acme-001", "event_id": "a"}),
        json.dumps(
            [{"tenant": "tenant-a", "event_id": "b"}, {"tenant": "acme-001", "event_id": "c"}]
        ),
        json.dumps({"tenant": "unknown-tenant", "event_id": "d"}),
        json.dumps({"event_id": "no tenant"}),
    )
    result = await ingest.ingest(event)
    assert ok_by_tenant(result) == {"acme-001": 2, "tenant-a": 1}
    # The unknown tenant and the record without one fail alone.
    assert result.failed == ["msg-2", "msg-3"]
    table = store.parquet(report_of(result, "acme-001").data_uri)
    assert "tenant" not in table.column_names
    assert set(table.column("_dl_tenant_id").to_pylist()) == {"acme-001"}


async def test_cloudwatch_direct_and_through_kinesis(
    make_client: MakeClient, store: Bucket
) -> None:
    ingest = recipe.LambdaIngestor(
        client(make_client),
        contract=LOGS,
        cloudwatch_tenant=lambda group: group.removeprefix("/app/"),
    )
    direct = {
        "awslogs": {"data": base64.b64encode(cloudwatch_payload("/app/acme-001", 5)).decode()}
    }
    result = await ingest.ingest(direct)
    assert ok_by_tenant(result) == {"acme-001": 5} and result.failed == []
    table = store.parquet(report_of(result, "acme-001").data_uri)
    assert table.column("service_name").to_pylist() == ["/app/acme-001"] * 5
    assert table.column("body").to_pylist()[0] == "msg 0"

    through = await ingest.ingest(kinesis(cloudwatch_payload("/app/tenant-a", 3)))
    assert ok_by_tenant(through) == {"tenant-a": 3}


def test_json_payload_needs_a_tenant(make_client: MakeClient) -> None:
    with pytest.raises(Exception, match="tenant"):
        recipe.LambdaIngestor(client(make_client), contract=EVENTS, payload="json")


def protobuf(tenant: str, n: int) -> bytes:
    """The request of :func:`otlp`, serialized as OTLP/protobuf."""
    from google.protobuf.json_format import ParseDict
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

    message = ParseDict(json.loads(otlp(tenant, n)), ExportLogsServiceRequest())
    return bytes(message.SerializeToString())


def zstd(data: bytes) -> bytes:
    try:
        from compression import zstd as codec  # type: ignore[import-not-found]  # Python 3.14

        return bytes(codec.compress(data))
    except ImportError:
        import zstandard

        return zstandard.ZstdCompressor().compress(data)


def sqs_messages(*messages: tuple[str, dict[str, Any] | None]) -> dict[str, Any]:
    event = sqs(*(body for body, _ in messages))
    for record, (_, attributes) in zip(event["Records"], messages, strict=True):
        if attributes:
            record["messageAttributes"] = attributes
    return event


def s3_notification(bucket: str, key: str, etag: str, size: int) -> str:
    created = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(NOW_MS / 1000))
    record = {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "eventTime": created,
        "eventName": "ObjectCreated:Put",
        "s3": {
            "bucket": {"name": bucket},
            "object": {"key": key.replace(" ", "+"), "size": size, "eTag": etag},
        },
    }
    return json.dumps({"Records": [record]})


async def test_kinesis_protobuf_and_compressed(make_client: MakeClient) -> None:
    pytest.importorskip("opentelemetry.proto")
    ingest = recipe.LambdaIngestor(client(make_client), contract=LOGS)
    event = kinesis(
        protobuf("acme-001", 3),
        zstd(protobuf("tenant-a", 2)),
        gzip.compress(otlp("acme-001", 1)),
        gzip.compress(b"\x0b\x0c"),
    )
    result = await ingest.ingest(event)
    assert ok_by_tenant(result) == {"acme-001": 4, "tenant-a": 2}
    assert result.failed == ["49590003"]


async def test_sqs_protobuf_in_base64(make_client: MakeClient) -> None:
    pytest.importorskip("opentelemetry.proto")
    ingest = recipe.LambdaIngestor(client(make_client), contract=LOGS)
    protobuf_type = {
        "content-type": {"stringValue": "application/x-protobuf", "dataType": "String"}
    }
    event = sqs_messages(
        (base64.b64encode(protobuf("acme-001", 2)).decode(), protobuf_type),
        (base64.b64encode(gzip.compress(protobuf("tenant-a", 3))).decode(), None),
        (otlp("acme-001", 1).decode(), None),
        ("not base64 and not json", None),
    )
    result = await ingest.ingest(event)
    assert ok_by_tenant(result) == {"acme-001": 3, "tenant-a": 3}
    assert result.failed == ["msg-3"]


async def test_s3_objects_through_sqs(make_client: MakeClient, s3: Any, store: Bucket) -> None:
    pytest.importorskip("opentelemetry.proto")
    landing = f"landing-{store.name}"
    s3.create_bucket(Bucket=landing)
    objects = {
        "logs/part 1.binpb.gz": gzip.compress(protobuf("acme-001", 3)),
        "logs/part-2.json": otlp("tenant-a", 2),
        "logs/part-3.binpb.zst": zstd(protobuf("acme-001", 1)),
        "logs/huge.binpb": protobuf("acme-001", 1),
    }
    etags = {}
    for key, body in objects.items():
        etags[key] = s3.put_object(Bucket=landing, Key=key, Body=body)["ETag"].strip('"')

    def notice(key: str, size: int | None = None) -> str:
        actual = len(objects.get(key, b""))
        return s3_notification(landing, key, etags.get(key, "0"), actual if size is None else size)

    ingest = recipe.LambdaIngestor(client(make_client), contract=LOGS, s3=s3, max_object_mb=1)
    event = sqs(
        notice("logs/part 1.binpb.gz"),
        notice("logs/part-2.json"),
        notice("logs/part-3.binpb.zst"),
        notice("logs/missing.binpb"),
        notice("logs/huge.binpb", size=2 * 1024 * 1024),
        json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": landing}),
    )
    result = await ingest.ingest(event)
    assert ok_by_tenant(result) == {"acme-001": 4, "tenant-a": 2}
    assert result.failed == ["msg-3", "msg-4"], "missing and oversized objects are retried"
    assert result.batch_id == "", "no Kinesis or SQS payload of its own"
    # Each object is a batch named by its key and ETag, at the event time.
    first = f"s3://{landing}/logs/part 1.binpb.gz@{etags['logs/part 1.binpb.gz']}"
    object_batch = batch_id_for([first], time_ms=NOW_MS // 1000 * 1000)
    assert any(object_batch in r.data_uri for r in result.reports)
    written = store.keys()
    # One object announced again, alone and under a new message id (SQS groups
    # redeliveries differently): the same Bronze objects are rewritten.
    again = sqs(notice("logs/part-3.binpb.zst"))
    again["Records"][0]["messageId"] = "redelivered"
    second = await ingest.ingest(again)
    assert ok_by_tenant(second) == {"acme-001": 1} and second.failed == []
    assert store.keys() == written
