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
    assert {t: r.records_ok for t, r in result.reports.items()} == {"acme-001": 4, "tenant-a": 2}
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
    assert {t: r.records_ok for t, r in result.reports.items()} == {"acme-001": 2, "tenant-a": 1}
    # The unknown tenant and the record without one fail alone.
    assert result.failed == ["msg-2", "msg-3"]
    table = store.parquet(result.reports["acme-001"].data_uri)
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
    assert result.reports["acme-001"].records_ok == 5 and result.failed == []
    table = store.parquet(result.reports["acme-001"].data_uri)
    assert table.column("service_name").to_pylist() == ["/app/acme-001"] * 5
    assert table.column("body").to_pylist()[0] == "msg 0"

    through = await ingest.ingest(kinesis(cloudwatch_payload("/app/tenant-a", 3)))
    assert through.reports["tenant-a"].records_ok == 3


def test_json_payload_needs_a_tenant(make_client: MakeClient) -> None:
    with pytest.raises(Exception, match="tenant"):
        recipe.LambdaIngestor(client(make_client), contract=EVENTS, payload="json")
