"""LakeReader: tenant-scoped Parquet and Iceberg reads on S3 (moto), for every engine."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    LakeReader,
    TableRef,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
)

from .conftest import CONTRACT

pa = pytest.importorskip("pyarrow")
pytest.importorskip("polars")
pytest.importorskip("duckdb")
pytest.importorskip("pyiceberg")

MakeClient = Callable[..., BronzeIngestionClient]
EVENTS = TableRef.bronze("login_event")
ENGINES = ["polars", "duckdb"]


@pytest.fixture
def tenants(bucket: str) -> TenantRegistry:
    return TenantRegistry(
        [TenantProfile.for_bucket(t, bucket) for t in ("acme-001", "tenant-a", "tenant-b")]
    )


@pytest.fixture(params=ENGINES)
def make_reader(
    request: pytest.FixtureRequest, s3_endpoint: str, tenants: TenantRegistry
) -> Iterator[Callable[..., LakeReader]]:
    readers: list[LakeReader] = []

    def make(**kwargs: Any) -> LakeReader:
        kwargs.setdefault("tenants", tenants)
        reader = LakeReader(
            engine=request.param,
            aws_region="us-east-1",
            aws_endpoint_url=s3_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            **kwargs,
        )
        readers.append(reader)
        return reader

    yield make
    for r in readers:
        r.close()


async def ingest(client: BronzeIngestionClient, tenant: str, n: int, start: int = 0) -> None:
    rows = [{"event_id": f"{tenant}-{i:03d}", "attempts": i} for i in range(start, start + n)]
    await client.ingest(rows, tenant=tenant, contract=CONTRACT)


async def test_bronze_reads_only_the_tenant(
    make_client: MakeClient, make_reader: Callable[..., LakeReader]
) -> None:
    client = make_client()
    await ingest(client, "acme-001", 5)
    await ingest(client, "acme-001", 5, start=5)
    await ingest(client, "tenant-a", 7)
    reader = make_reader()

    everything = reader.read("acme-001", EVENTS)
    assert everything.num_rows == 10
    assert set(everything.column("tenant_id").to_pylist()) == {"acme-001"}
    assert set(everything.column("_dl_tenant_id").to_pylist()) == {"acme-001"}
    assert all("/tenant_id=acme-001/" in f for f in reader.files("acme-001", EVENTS))
    assert reader.read("tenant-a", EVENTS).num_rows == 7
    assert reader.read("tenant-b", EVENTS).num_rows == 0


async def test_projection_filters_order_and_keyset_pages(
    make_client: MakeClient, make_reader: Callable[..., LakeReader]
) -> None:
    await ingest(make_client(), "acme-001", 20)
    reader = make_reader()
    page = reader.read(
        "acme-001",
        EVENTS,
        columns=["event_id", "attempts"],
        where=[("attempts", ">=", 5), ("attempts", "not in", [6, 7])],
        order_by=[("attempts", "desc")],
        limit=3,
    )
    assert page.column_names == ["event_id", "attempts"]
    assert page.column("attempts").to_pylist() == [19, 18, 17]

    pages, cursor = [], -1
    while True:
        batch = reader.read(
            "acme-001",
            EVENTS,
            columns=["attempts"],
            where=[("attempts", ">", cursor)],
            order_by=["attempts"],
            limit=8,
        )
        if batch.num_rows == 0:
            break
        pages.append(batch.column("attempts").to_pylist())
        cursor = pages[-1][-1]
    assert [len(p) for p in pages] == [8, 8, 4]
    assert [v for p in pages for v in p] == list(range(20))

    ids = ["acme-001-003", "acme-001-011", "missing"]
    point = reader.read("acme-001", EVENTS, columns=["event_id"], where=[("event_id", "in", ids)])
    assert sorted(point.column("event_id").to_pylist()) == ids[:2]


async def test_scan_runs_aggregations_in_the_engine(
    make_client: MakeClient, make_reader: Callable[..., LakeReader]
) -> None:
    client = make_client()
    await ingest(client, "acme-001", 10)
    await ingest(client, "tenant-a", 4)
    reader = make_reader()
    scan = reader.scan("acme-001", EVENTS)
    if reader.engine == "polars":
        import polars as pl

        total = scan.select(pl.len(), pl.col("attempts").sum()).collect().row(0)
    else:
        total = scan.aggregate("count(*), sum(attempts)").fetchone()
    assert tuple(total) == (10, 45)


async def test_tenant_guard_and_validation(
    make_client: MakeClient, make_reader: Callable[..., LakeReader]
) -> None:
    await ingest(make_client(), "acme-001", 2)
    reader = make_reader()
    with pytest.raises(TenantViolationError):
        reader.read("acme-001", EVENTS, where=[("tenant_id", "==", "tenant-a")])
    with pytest.raises(TenantViolationError):
        reader.read("not-a-tenant", EVENTS)
    for bad in (
        {"columns": ["event_id; DROP TABLE x"]},
        {"where": [("attempts", "LIKE", 1)]},
        {"where": [("attempts", "in", "12")]},
        {"order_by": [("attempts", "sideways")]},
        {"limit": -1},
    ):
        with pytest.raises(ConfigurationError):
            reader.read("acme-001", EVENTS, **bad)
    no_gold = TenantProfile("acme-001", bronze="s3://lake-a/b", quarantine="s3://lake-a/q")
    with pytest.raises(ConfigurationError, match="no gold location"):
        reader.read(no_gold, TableRef.parquet("gold", "summary"))


async def test_listings_are_cached_until_stale_or_refreshed(
    make_client: MakeClient, make_reader: Callable[..., LakeReader]
) -> None:
    client = make_client()
    await ingest(client, "acme-001", 3)
    cached = make_reader(max_staleness=600)
    fresh = make_reader(max_staleness=0)
    assert cached.read("acme-001", EVENTS).num_rows == 3
    await ingest(client, "acme-001", 2, start=3)
    assert cached.read("acme-001", EVENTS).num_rows == 3
    assert fresh.read("acme-001", EVENTS).num_rows == 5
    cached.refresh("acme-001")
    assert cached.read("acme-001", EVENTS).num_rows == 5


def test_reader_settings_are_validated(tenants: TenantRegistry) -> None:
    with pytest.raises(ConfigurationError, match="engine"):
        LakeReader(tenants, engine="athena")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="max_staleness"):
        LakeReader(tenants, max_staleness=-1)
    with pytest.raises(ConfigurationError, match="go together"):
        LakeReader(tenants, aws_access_key_id="k")
    with pytest.raises(ConfigurationError):
        TableRef("bronze", "x", "iceberg")
    with pytest.raises(ConfigurationError):
        TableRef.iceberg("gold", "x", location="/local/path")
    with pytest.raises(ConfigurationError):
        TableRef.iceberg("gold", "x", identifier="g.x", location="s3://b/x")


@pytest.fixture
def iceberg_catalog(s3_endpoint: str, bucket: str) -> Iterator[Any]:
    from pyiceberg.catalog.sql import SqlCatalog

    workdir = tempfile.mkdtemp()
    catalog = SqlCatalog(
        "lake",
        uri=f"sqlite:///{os.path.join(workdir, 'catalog.db')}",
        warehouse=f"s3://{bucket}/warehouse",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.region": "us-east-1",
            "s3.access-key-id": "test",
            "s3.secret-access-key": "test",
        },
    )
    catalog.create_namespace("gold")
    yield catalog
    catalog.engine.dispose()


def summary_rows(tenant: str, n: int) -> Any:
    return pa.table(
        {
            "tenant_id": [tenant] * n,
            "framework": [f"fw-{i % 3}" for i in range(n)],
            "total": list(range(n)),
        }
    )


def test_iceberg_shared_table_is_filtered_to_the_tenant(
    make_reader: Callable[..., LakeReader], iceberg_catalog: Any, bucket: str
) -> None:
    location = f"s3://{bucket}/shared/gold/compliance_summary"
    table = iceberg_catalog.create_table(
        "gold.compliance_summary", schema=summary_rows("x", 1).schema, location=location
    )
    with table.update_spec() as spec:
        spec.add_identity("tenant_id")
    table.append(summary_rows("acme-001", 6))
    table.append(summary_rows("tenant-a", 4))
    table.append(summary_rows("acme-001", 3))

    by_location = TableRef.iceberg("gold", "compliance_summary", location=location)
    by_identifier = TableRef.iceberg(
        "gold", "compliance_summary", identifier="gold.compliance_summary"
    )
    reader = make_reader(catalog=iceberg_catalog)
    for ref in (by_location, by_identifier):
        rows = reader.read("acme-001", ref, order_by=["total"])
        assert rows.num_rows == 9, ref
        assert set(rows.column("tenant_id").to_pylist()) == {"acme-001"}
        assert reader.files("acme-001", ref) == [
            iceberg_catalog.load_table("gold.compliance_summary").metadata_location
        ]
    summed = reader.read(
        "tenant-a", by_location, columns=["total"], where=[("framework", "==", "fw-0")]
    )
    assert sorted(summed.column("total").to_pylist()) == [0, 3]


def test_iceberg_table_in_the_tenant_location(
    make_reader: Callable[..., LakeReader], iceberg_catalog: Any, tenants: TenantRegistry
) -> None:
    location = tenants.get("acme-001").table_uri("gold", "scores")
    assert location is not None
    schema = pa.schema([("score", pa.int64())])
    table = iceberg_catalog.create_table(
        "gold.scores", schema=schema, location=location.rstrip("/")
    )
    table.append(pa.table({"score": [1, 2, 3]}, schema=schema))
    reader = make_reader()
    ref = TableRef.iceberg("gold", "scores")
    assert reader.read("acme-001", ref).column("score").to_pylist() == [1, 2, 3]
    assert reader.read("tenant-a", ref).num_rows == 0, "another tenant has no such table"


def test_shared_iceberg_table_needs_a_tenant_column(
    make_reader: Callable[..., LakeReader], iceberg_catalog: Any, bucket: str
) -> None:
    location = f"s3://{bucket}/shared/gold/no_tenant"
    schema = pa.schema([("score", pa.int64())])
    table = iceberg_catalog.create_table("gold.no_tenant", schema=schema, location=location)
    table.append(pa.table({"score": [1]}, schema=schema))
    with pytest.raises(TenantViolationError, match="tenant_id column"):
        make_reader().read("acme-001", TableRef.iceberg("gold", "no_tenant", location=location))
