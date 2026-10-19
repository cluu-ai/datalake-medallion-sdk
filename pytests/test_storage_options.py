"""Contract storage options: compression, typed timestamps and event-time partitions."""

from __future__ import annotations

import io
from collections.abc import Callable
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    ContractBootError,
    ContractRegistry,
    LakeReader,
    TableRef,
    TenantProfile,
    TenantRegistry,
)

from .conftest import Bucket

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

MakeClient = Callable[..., BronzeIngestionClient]
LOG = "app/log/1.0.0"


def log_schema(**options: Any) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "x-dl-domain": "app",
        "x-dl-datatype": "log",
        "x-dl-schema_version": "1.0.0",
        "required": ["id"],
        "properties": {
            "id": {"type": "string"},
            "at": {"type": "string", "format": "date-time", "x-dl-type": "timestamp"},
            "level": {"type": "string"},
            "msg": {"type": "string"},
        },
        **{f"x-dl-{k}": v for k, v in options.items()},
    }


def logs(start: int, n: int, days: int = 1) -> list[dict[str, Any]]:
    return [
        {
            "id": f"r{i:08d}",
            "at": f"2026-03-0{1 + i % days}T{i % 24:02d}:00:00Z",
            "level": "INFO" if i % 7 else "ERROR",
            "msg": f"request {i} served",
        }
        for i in range(start, start + n)
    ]


def client(make_client: MakeClient, **options: Any) -> BronzeIngestionClient:
    return make_client(registry=ContractRegistry.from_schemas([log_schema(**options)]))


def test_invalid_storage_options_are_rejected() -> None:
    for bad in (
        {"compression": "brotli"},
        {"compression": "zstd", "compression_level": 30},
        {"partition_by": "msg"},
    ):
        with pytest.raises(ContractBootError):
            ContractRegistry.from_schemas([log_schema(**bad)])


@pytest.mark.parametrize(
    ("codec", "written"),
    [("snappy", "SNAPPY"), ("zstd", "ZSTD"), ("lz4", "LZ4"), ("none", "UNCOMPRESSED")],
)
async def test_every_codec_is_built_into_the_wheel(
    make_client: MakeClient, store: Bucket, codec: str, written: str
) -> None:
    report = await client(make_client, compression=codec).ingest(
        logs(0, 20), tenant="acme-001", contract=LOG
    )
    body = store.body(report.data_uri)
    assert pq.ParquetFile(io.BytesIO(body)).metadata.row_group(0).column(0).compression == written
    assert pq.read_table(io.BytesIO(body)).num_rows == 20


async def test_zstd_and_typed_timestamps(make_client: MakeClient, store: Bucket) -> None:
    report = await client(make_client, compression="zstd", compression_level=3).ingest(
        logs(0, 50), tenant="acme-001", contract=LOG
    )
    assert report.records_ok == 50 and report.data_uris == [report.data_uri]
    body = store.body(report.data_uri)
    meta = pq.ParquetFile(io.BytesIO(body)).metadata
    assert meta.row_group(0).column(0).compression == "ZSTD"
    at = pq.read_table(io.BytesIO(body)).column("at")
    assert at.type == pa.timestamp("us", tz="UTC")
    assert at[0].as_py().isoformat() == "2026-03-01T00:00:00+00:00"


async def test_one_shot_ingest_partitions_by_event_day(
    make_client: MakeClient, store: Bucket
) -> None:
    c = client(make_client, partition_by="at")
    report = await c.ingest(logs(0, 30, days=3), tenant="acme-001", contract=LOG)
    assert len(report.data_uris) == 3
    for day, uri in zip(("01", "02", "03"), report.data_uris, strict=True):
        assert f"/year=2026/month=03/day={day}/" in uri
        assert uri.endswith(f"{report.batch_id}-202603{day}.parquet")
        days = {t.day for t in store.parquet(uri).column("at").to_pylist()}
        assert days == {int(day)}


async def test_stream_partitions_by_event_day_and_reads_back(
    make_client: MakeClient, store: Bucket, s3_endpoint: str, bucket: str
) -> None:
    c = client(make_client, partition_by="at", compression="zstd")
    chunks = (pa.Table.from_pylist(logs(i * 1_000, 1_000, days=3)) for i in range(6))
    report = await c.ingest_stream(
        chunks, tenant="acme-001", contract=LOG, chunk_rows=1_000, max_open_days=2
    )
    assert report.records_ok == 6_000
    total = 0
    for uri in report.data_uris:
        name = uri.rsplit("/", 1)[1]
        day = int(name.split("-")[1][-2:])
        assert f"/day={day:02d}/" in uri
        table = store.parquet(uri)
        assert {t.day for t in table.column("at").to_pylist()} == {day}
        total += table.num_rows
    assert total == 6_000
    reader = LakeReader(
        TenantRegistry([TenantProfile.for_bucket("acme-001", bucket)]),
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        max_staleness=0,
    )
    try:
        data = reader.read("acme-001", TableRef.bronze("log"), columns=["id", "at"])
        assert data.num_rows == 6_000
        assert data.column("at").type == pa.timestamp("us", tz="UTC")
    finally:
        reader.close()


async def test_old_objects_are_closed_between_chunks(make_client: MakeClient) -> None:
    c = client(make_client)
    chunks = (logs(i * 10, 10) for i in range(3))
    report = await c.ingest_stream(chunks, tenant="acme-001", contract=LOG, max_object_seconds=0)
    assert report.records_ok == 30
    assert len(report.data_uris) == 3
    with pytest.raises(ConfigurationError):
        await c.ingest_stream([], tenant="acme-001", contract=LOG, max_object_seconds=-1)
