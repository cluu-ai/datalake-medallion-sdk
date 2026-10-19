"""Streaming ingestion: Arrow streams, chunk iterables and files, on S3 (moto)."""

from __future__ import annotations

import gzip
import json
import secrets
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    LakeReader,
    StreamReport,
    TableRef,
    TenantRegistry,
)

from .conftest import CONTRACT, Bucket

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

MakeClient = Callable[..., BronzeIngestionClient]
EVENTS = TableRef.bronze("login_event")


def rows(start: int, n: int) -> list[dict[str, Any]]:
    """Every 10th record has a negative ``attempts`` and is quarantined."""
    return [
        {"event_id": f"e{i:08d}", "attempts": -1 if i % 10 == 9 else i, "user_email": f"u{i}@x.io"}
        for i in range(start, start + n)
    ]


def table(start: int, n: int) -> Any:
    return pa.Table.from_pylist(rows(start, n))


@pytest.fixture
def reader(s3_endpoint: str, bucket: str) -> Iterator[LakeReader]:
    from datalake_medallion_sdk import TenantProfile

    r = LakeReader(
        TenantRegistry([TenantProfile.for_bucket("acme-001", bucket)]),
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        max_staleness=0,
    )
    yield r
    r.close()


def check(report: StreamReport, received: int) -> None:
    assert report.records_received == received
    assert report.records_quarantined == received // 10
    assert report.records_ok == received - received // 10
    assert report.rejections == {"ERR_SCHEMA_VIOLATION": received // 10}


async def test_arrow_stream_is_read_batch_by_batch(
    make_client: MakeClient, store: Bucket, reader: LakeReader
) -> None:
    source = pa.RecordBatchReader.from_batches(
        table(0, 1).schema, (b for i in range(5) for b in table(i * 2_000, 2_000).to_batches())
    )
    report = await make_client().ingest_stream(source, tenant="acme-001", contract=CONTRACT)
    check(report, 10_000)
    assert len(report.data_uris) == 1 and report.data_uris[0].endswith("-00000.parquet")
    assert report.stream_id in report.data_uris[0]
    data = reader.read("acme-001", EVENTS, order_by=["event_id"])
    assert data.num_rows == 9_000
    assert set(data.column("_dl_batch_id").to_pylist()) == {report.stream_id}
    dead = [json.loads(line) for uri in report.dlq_uris for line in store.body(uri).splitlines()]
    assert sorted(d["record_index"] for d in dead) == list(range(9, 10_000, 10))


async def test_iterables_of_any_chunk_type(make_client: MakeClient, reader: LakeReader) -> None:
    polars = pytest.importorskip("polars")

    def chunks() -> Iterator[Any]:
        yield rows(0, 100)  # list of dicts
        yield json.dumps(rows(100, 100))  # JSON text
        yield table(200, 100)  # pyarrow table
        yield polars.DataFrame(rows(300, 100))  # Polars

    report = await make_client().ingest_stream(chunks(), tenant="acme-001", contract=CONTRACT)
    check(report, 400)
    ids = reader.read("acme-001", EVENTS, columns=["event_id"], order_by=["event_id"])
    assert ids.column("event_id").to_pylist() == [
        r["event_id"] for r in rows(0, 400) if r["attempts"] >= 0
    ]


async def test_large_input_spans_several_objects(make_client: MakeClient, store: Bucket) -> None:
    # Random text does not compress: about 20 MB of Parquet.
    def batches() -> Iterator[Any]:
        for c in range(10):
            n = 25_000
            yield pa.table(
                {
                    "event_id": [secrets.token_hex(40) for _ in range(n)],
                    "attempts": list(range(c * n, (c + 1) * n)),
                }
            )

    report = await make_client().ingest_stream(
        batches(),
        tenant="acme-001",
        contract=CONTRACT,
        target_object_mb=6,
        part_mb=5,
        row_group_mb=1,
    )
    assert report.records_ok == 250_000
    assert len(report.data_uris) >= 2
    total = sum(store.parquet(uri).num_rows for uri in report.data_uris)
    assert total == 250_000
    assert report.bytes_written >= 15 * 1024 * 1024


async def test_retry_with_the_same_stream_id_does_not_duplicate(
    make_client: MakeClient, store: Bucket, reader: LakeReader
) -> None:
    client = make_client()
    first = await client.ingest_stream(table(0, 500), tenant="acme-001", contract=CONTRACT)
    keys = store.keys()
    again = await client.ingest_stream(
        table(0, 500), tenant="acme-001", contract=CONTRACT, stream_id=first.stream_id
    )
    assert again.data_uris == first.data_uris and store.keys() == keys
    assert reader.read("acme-001", EVENTS).num_rows == 450
    with pytest.raises(ConfigurationError, match="ULID"):
        await client.ingest_stream([], tenant="acme-001", contract=CONTRACT, stream_id="x")


def write_files(tmp: Path) -> dict[str, Path]:
    data = rows(0, 3_000)
    csv_lines = ["event_id,attempts,user_email"]
    csv_lines += [f"{r['event_id']},{r['attempts']},{r['user_email']}" for r in data]
    csv_lines += ["bad-1,not-a-number,z@x.io", "only-id,,"]
    jsonl = [json.dumps(r) for r in data] + ["{broken", ""]
    files = {
        "csv": tmp / "events.csv",
        "tsv": tmp / "events.tsv",
        "jsonl": tmp / "events.jsonl",
        "gz": tmp / "events.jsonl.gz",
        "parquet": tmp / "events.parquet",
    }
    files["csv"].write_text("\n".join(csv_lines) + "\n")
    files["tsv"].write_text("\n".join(line.replace(",", "\t") for line in csv_lines) + "\n")
    files["jsonl"].write_text("\n".join(jsonl) + "\n")
    files["gz"].write_bytes(gzip.compress(("\n".join(jsonl) + "\n").encode()))
    pq.write_table(table(0, 3_000), files["parquet"], row_group_size=700)
    return files


async def test_files_of_every_format(make_client: MakeClient, tmp_path: Path) -> None:
    files = write_files(tmp_path)
    client = make_client()
    for kind in ("csv", "tsv"):
        report = await client.ingest_file(files[kind], tenant="acme-001", contract=CONTRACT)
        # 3,000 rows + a value that does not parse + a row with only the id.
        assert report.records_received == 3_002, kind
        assert report.records_ok == 2_701, kind
        assert report.rejections == {"ERR_SCHEMA_VIOLATION": 301}, kind
    for kind in ("jsonl", "gz"):
        report = await client.ingest_file(
            files[kind], tenant="acme-001", contract=CONTRACT, block_mb=1
        )
        assert report.records_received == 3_001, kind
        assert report.records_ok == 2_700, kind
        assert report.rejections == {"ERR_SCHEMA_VIOLATION": 300, "ERR_RECORD_NOT_OBJECT": 1}
    report = await client.ingest_file(files["parquet"], tenant="acme-001", contract=CONTRACT)
    check(report, 3_000)
    with pytest.raises(ConfigurationError, match="format"):
        await client.ingest_file(tmp_path / "events.bin", tenant="acme-001", contract=CONTRACT)


async def test_file_on_s3(make_client: MakeClient, store: Bucket, tmp_path: Path) -> None:
    files = write_files(tmp_path)
    store.s3.put_object(Bucket=store.name, Key="landing/events.csv", Body=files["csv"].read_bytes())
    report = await make_client().ingest_file(
        f"s3://{store.name}/landing/events.csv", tenant="acme-001", contract=CONTRACT
    )
    assert report.records_ok == 2_701
    assert all(uri.startswith(f"s3://{store.name}/bronze/") for uri in report.data_uris)
