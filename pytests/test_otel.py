"""OpenTelemetry logs: OTLP requests flattened and ingested per tenant (moto S3)."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    MalformedPayloadError,
    TenantViolationError,
)
from datalake_medallion_sdk.otel import flatten_logs, logs_contract

from .conftest import Bucket

pa = pytest.importorskip("pyarrow")

MakeClient = Callable[..., BronzeIngestionClient]
CONTRACT = "otel/logs/1.0.0"


def ulid_now(suffix: str = "0" * 16) -> str:
    """A ULID for the current time: the batch id sets the partition date and
    the reference time of the future-skew check."""
    import time

    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    ms, out = int(time.time() * 1000), ""
    for _ in range(10):
        ms, r = divmod(ms, 32)
        out = alphabet[r] + out
    return out + suffix


ULID = ulid_now("8K2N4P6R8T0W2Y4A")


def resource(tenant: str | None, records: list[dict[str, Any]]) -> dict[str, Any]:
    attributes = [{"key": "service.name", "value": {"stringValue": "checkout"}}]
    if tenant:
        attributes.append({"key": "tenant.id", "value": {"stringValue": tenant}})
    return {
        "resource": {"attributes": attributes},
        "scopeLogs": [{"scope": {"name": "app"}, "logRecords": records}],
    }


def record(i: int, **extra: Any) -> dict[str, Any]:
    return {
        "timeUnixNano": str(1_772_359_200_000_000_000 + i * 1_000_000_000),
        "severityNumber": 9,
        "severityText": "INFO",
        "body": {"stringValue": f"request {i} served"},
        "attributes": [{"key": "http.status", "value": {"intValue": "200"}}],
        "traceId": "5b8efff798038103d269b633813fc60c",
        "spanId": "eee19b7ec3c1b174",
        **extra,
    }


def request() -> dict[str, Any]:
    return {
        "resourceLogs": [
            resource("acme-001", [record(i) for i in range(3)]),
            resource("tenant-a", [record(10)]),
            resource(None, [record(20)]),
        ]
    }


def client(make_client: MakeClient) -> BronzeIngestionClient:
    return make_client(registry=ContractRegistry.from_schemas([logs_contract()]))


def test_contract_defaults() -> None:
    c = logs_contract()
    assert c["x-dl-compression"] == "zstd"
    assert "x-dl-partition_by" not in c
    assert c["properties"]["timestamp"]["x-dl-type"] == "timestamp"
    assert logs_contract(partition_by_event_time=True)["x-dl-partition_by"] == "timestamp"


def test_flatten_returns_one_batch_per_tenant() -> None:
    groups = flatten_logs(request())
    assert set(groups) == {None, "acme-001", "tenant-a"}
    acme = groups["acme-001"]
    assert isinstance(acme, pa.RecordBatch)
    assert acme.num_rows == 3
    assert acme.column("timestamp")[0].as_py() == "2026-03-01T10:00:00.000000Z"
    assert json.loads(acme.column("attributes")[0].as_py()) == {"http.status": 200}
    assert flatten_logs(json.dumps(request()).encode()).keys() == groups.keys()


async def test_every_tenant_is_ingested(make_client: MakeClient, store: Bucket) -> None:
    reports = await client(make_client).ingest_otel_logs(
        request(), contract=CONTRACT, default_tenant="tenant-b", batch_id=ULID
    )
    assert {t: r.records_ok for t, r in reports.items()} == {
        "acme-001": 3,
        "tenant-a": 1,
        "tenant-b": 1,
    }
    table = store.parquet(reports["acme-001"].data_uri)
    assert table.column("timestamp").type == pa.timestamp("us", tz="UTC")
    assert table.column("service_name").to_pylist() == ["checkout"] * 3
    assert "tenant_id=tenant-b/" in reports["tenant-b"].data_uri
    keys = store.keys()
    again = await client(make_client).ingest_otel_logs(
        request(), contract=CONTRACT, default_tenant="tenant-b", batch_id=ULID
    )
    assert {t: r.data_uri for t, r in again.items()} == {t: r.data_uri for t, r in reports.items()}
    assert store.keys() == keys, "a redelivered request rewrites the same keys"


async def test_records_without_tenant_need_a_default(
    make_client: MakeClient, store: Bucket
) -> None:
    with pytest.raises(TenantViolationError):
        await client(make_client).ingest_otel_logs(request(), contract=CONTRACT, batch_id=ULID)
    written = store.keys()
    assert written and not any("tenant-b" in k for k in written), "resolved tenants are written"
    # Calling again with a default tenant completes the batch without duplicates.
    await client(make_client).ingest_otel_logs(
        request(), contract=CONTRACT, default_tenant="tenant-b", batch_id=ULID
    )
    keys = store.keys()
    assert set(written) < set(keys)
    assert len([k for k in keys if "tenant_id=acme-001/" in k]) == 1
    with pytest.raises(TenantViolationError):
        bad = {"resourceLogs": [resource("unknown-tenant", [record(1)])]}
        await client(make_client).ingest_otel_logs(bad, contract=CONTRACT)


async def test_invalid_records_are_quarantined(make_client: MakeClient, store: Bucket) -> None:
    body = {
        "resourceLogs": [
            resource("acme-001", [record(1), record(2, traceId="not-hex"), {"severityNumber": 5}])
        ]
    }
    reports = await client(make_client).ingest_otel_logs(body, contract=CONTRACT)
    r = reports["acme-001"]
    assert (r.records_ok, r.records_quarantined) == (1, 2)


async def test_unreadable_requests_are_rejected(make_client: MakeClient) -> None:
    with pytest.raises(MalformedPayloadError, match="OTLP/protobuf"):
        await client(make_client).ingest_otel_logs(b"\x0a\x12\x08", contract=CONTRACT)
    with pytest.raises(MalformedPayloadError, match="JSON object"):
        await client(make_client).ingest_otel_logs(b"[1]", contract=CONTRACT, encoding="json")
    with pytest.raises(TypeError, match="JSON"):
        await client(make_client).ingest_otel_logs(
            request(), contract=CONTRACT, encoding="protobuf"
        )
    with pytest.raises(TypeError, match="bytes"):
        await client(make_client).ingest_otel_logs(42, contract=CONTRACT)  # type: ignore[arg-type]
    with pytest.raises(Exception, match="encoding"):
        await client(make_client).ingest_otel_logs(b"{}", contract=CONTRACT, encoding="xml")  # type: ignore[arg-type]


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
async def test_readme_log_query(
    make_client: MakeClient, s3_endpoint: str, bucket: str, engine: str
) -> None:
    """The reading example of the README's logs section."""
    from datalake_medallion_sdk import LakeReader, TableRef, TenantProfile, TenantRegistry

    errors = [record(i, severityNumber=17, severityText="ERROR") for i in range(3)]
    body = {"resourceLogs": [resource("acme-001", [record(i) for i in range(5)] + errors)]}
    await client(make_client).ingest_otel_logs(body, contract=CONTRACT)
    reader = LakeReader(
        TenantRegistry([TenantProfile.for_bucket("acme-001", bucket)]),
        engine=engine,
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        max_staleness=0,
    )
    try:
        table = reader.read(
            "acme-001",
            TableRef.bronze("logs"),
            columns=["timestamp", "service_name", "severity_text", "body", "trace_id"],
            where=[("timestamp", ">=", "2026-03-01T10:00:01Z"), ("severity_number", ">=", 17)],
            order_by=[("timestamp", "desc")],
            limit=100,
        )
    finally:
        reader.close()
    assert table.column("severity_text").to_pylist() == ["ERROR", "ERROR"]
    assert table.column("body").to_pylist() == ["request 2 served", "request 1 served"]
