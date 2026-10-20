"""Commit manifests and LakeReader.changes() (moto S3)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    LakeReader,
    TableRef,
    TenantProfile,
    TenantRegistry,
)

from .conftest import CONTRACT, Bucket

pa = pytest.importorskip("pyarrow")

MakeClient = Callable[..., BronzeIngestionClient]
EVENTS = TableRef.bronze("login_event")


def rows(start: int, n: int) -> list[dict[str, Any]]:
    """Every 10th record has a negative ``attempts`` and is quarantined."""
    return [
        {"event_id": f"e{i}", "attempts": -1 if i % 10 == 9 else i} for i in range(start, start + n)
    ]


@pytest.fixture
def reader(s3_endpoint: str, bucket: str) -> Iterator[LakeReader]:
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


async def test_changes_follow_commits_from_a_watermark(
    make_client: MakeClient, reader: LakeReader
) -> None:
    client = make_client(manifests=True)
    reports = [
        await client.ingest(rows(i * 20, 20), tenant="acme-001", contract=CONTRACT)
        for i in range(3)
    ]
    first = reader.changes("acme-001", EVENTS, settle_seconds=0)
    assert [e["batch_id"] for e in first.entries] == [r.batch_id for r in reports]
    assert first.data_uris == [r.data_uri for r in reports]
    entry = first.entries[0]
    assert (entry["records_received"], entry["records_ok"], entry["records_quarantined"]) == (
        20,
        18,
        2,
    )
    assert entry["dead_letters"] and entry["contract"] == CONTRACT
    assert first.watermark is not None

    none = reader.changes("acme-001", EVENTS, since=first.watermark, settle_seconds=0)
    assert none.entries == [] and none.watermark == first.watermark

    later = await client.ingest(rows(100, 5), tenant="acme-001", contract=CONTRACT)
    nxt = reader.changes("acme-001", EVENTS, since=first.watermark, settle_seconds=0)
    assert [e["batch_id"] for e in nxt.entries] == [later.batch_id]

    paged = reader.changes("acme-001", EVENTS, settle_seconds=0, limit=2)
    assert len(paged.entries) == 2
    rest = reader.changes("acme-001", EVENTS, since=paged.watermark, settle_seconds=0)
    assert len(rest.entries) == 2

    # The data readers do not see the manifests.
    assert reader.read("acme-001", EVENTS).num_rows == 18 * 3 + 5


async def test_young_entries_wait_for_the_next_call(
    make_client: MakeClient, reader: LakeReader
) -> None:
    await make_client(manifests=True).ingest(rows(0, 5), tenant="acme-001", contract=CONTRACT)
    waiting = reader.changes("acme-001", EVENTS, settle_seconds=3600)
    assert waiting.entries == [] and waiting.watermark is None


async def test_streams_and_files_write_entries(make_client: MakeClient, reader: LakeReader) -> None:
    client = make_client(manifests=True)
    table = pa.Table.from_pylist(rows(0, 2_000))
    report = await client.ingest_stream(table, tenant="acme-001", contract=CONTRACT)
    changes = reader.changes("acme-001", EVENTS, settle_seconds=0)
    assert [e["batch_id"] for e in changes.entries] == [report.stream_id]
    assert changes.data_uris == report.data_uris
    assert changes.entries[0]["records_ok"] == report.records_ok


async def test_no_manifests_by_default(
    make_client: MakeClient, reader: LakeReader, store: Bucket
) -> None:
    await make_client().ingest(rows(0, 5), tenant="acme-001", contract=CONTRACT)
    assert not any("_manifests" in k for k in store.keys())
    assert reader.changes("acme-001", EVENTS, settle_seconds=0).entries == []


async def test_read_changes_returns_the_new_rows_of_the_tenant(
    make_client: MakeClient, reader: LakeReader, bucket: str
) -> None:
    from datalake_medallion_sdk import TenantViolationError

    client = make_client(manifests=True)
    await client.ingest(rows(0, 20), tenant="acme-001", contract=CONTRACT)
    first = reader.changes("acme-001", EVENTS, settle_seconds=0)
    await client.ingest(rows(100, 10), tenant="acme-001", contract=CONTRACT)
    new = reader.changes("acme-001", EVENTS, since=first.watermark, settle_seconds=0)
    table = reader.read_changes("acme-001", EVENTS, new, columns=["event_id", "attempts"])
    # e109 is invalid: quarantined, not in Bronze.
    assert sorted(table.column("event_id").to_pylist()) == sorted(f"e{i}" for i in range(100, 109))
    assert table.column_names == ["event_id", "attempts"]
    other = TenantProfile.for_bucket("tenant-b", bucket)
    with pytest.raises(TenantViolationError, match="not in 'tenant-b'"):
        reader.read_changes(other, EVENTS, new)
