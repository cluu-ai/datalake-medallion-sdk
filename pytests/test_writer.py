"""LakeWriter and LakeMaintenance: Iceberg Silver/Gold writes (moto S3, SQL catalog)."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from datalake_medallion_sdk import (
    LakeReader,
    TableRef,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
)

pa = pytest.importorskip("pyarrow")
pytest.importorskip("pyiceberg")

from datalake_medallion_sdk.writer import LakeMaintenance, LakeWriter  # noqa: E402

SHARED = TableRef.iceberg("silver", "items", identifier="silver.items")
OWN = TableRef.iceberg("silver", "events")
SCHEMA = pa.schema([("id", pa.int64()), ("day", pa.string()), ("v", pa.string())])


@pytest.fixture
def tenants(bucket: str) -> TenantRegistry:
    return TenantRegistry([TenantProfile.for_bucket(t, bucket) for t in ("acme-001", "tenant-b")])


@pytest.fixture
def catalog(s3_endpoint: str, bucket: str) -> Iterator[Any]:
    from pyiceberg.catalog.sql import SqlCatalog

    workdir = tempfile.mkdtemp()
    cat = SqlCatalog(
        "lake",
        uri=f"sqlite:///{Path(workdir, 'catalog.db').as_posix()}",
        warehouse=f"s3://{bucket}/warehouse",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.region": "us-east-1",
            "s3.access-key-id": "test",
            "s3.secret-access-key": "test",
        },
    )
    yield cat
    cat.engine.dispose()


@pytest.fixture
def writer(tenants: TenantRegistry, catalog: Any) -> LakeWriter:
    w = LakeWriter(tenants, catalog=catalog)
    w.create_table("acme-001", SHARED, SCHEMA, partition_by=["day"], keys=["id"])
    return w


@pytest.fixture
def reader(s3_endpoint: str, tenants: TenantRegistry, catalog: Any) -> Iterator[LakeReader]:
    r = LakeReader(
        tenants,
        catalog=catalog,
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        max_staleness=0,
    )
    yield r
    r.close()


def rows(ids: range | list[int], v: str, day: str = "2026-10-19") -> Any:
    ids = list(ids)
    return pa.table({"id": ids, "day": [day] * len(ids), "v": [v] * len(ids)})


def values(reader: LakeReader, tenant: str, table: TableRef = SHARED) -> dict[int, str]:
    t = reader.read(tenant, table, order_by=["id"])
    return dict(zip(t.column("id").to_pylist(), t.column("v").to_pylist(), strict=True))


def test_tenants_only_write_their_rows(writer: LakeWriter, reader: LakeReader) -> None:
    writer.append("acme-001", SHARED, rows(range(3), "a"))
    writer.append("tenant-b", SHARED, rows(range(2), "b"))
    assert values(reader, "acme-001") == {0: "a", 1: "a", 2: "a"}
    assert values(reader, "tenant-b") == {0: "b", 1: "b"}
    foreign = rows([9], "x").add_column(0, "tenant_id", pa.array(["tenant-b"]))
    with pytest.raises(TenantViolationError):
        writer.append("acme-001", SHARED, foreign)
    # replace and upsert are scoped to the tenant even with the same keys.
    writer.replace("acme-001", SHARED, rows([5], "a2"))
    writer.upsert("acme-001", SHARED, rows([0, 1], "a3"))
    assert values(reader, "acme-001") == {0: "a3", 1: "a3", 5: "a2"}
    assert values(reader, "tenant-b") == {0: "b", 1: "b"}


def test_batches_are_written_once(writer: LakeWriter, reader: LakeReader) -> None:
    first = writer.append("acme-001", SHARED, rows(range(4), "a"), batch_id="B-1")
    again = writer.append("acme-001", SHARED, rows(range(4), "a"), batch_id="B-1")
    assert not first.skipped and first.rows_inserted == 4
    assert again.skipped and again.snapshot_id == first.snapshot_id
    other = writer.append("tenant-b", SHARED, rows(range(4), "b"), batch_id="B-1")
    assert not other.skipped, "batch ids are per tenant"
    assert len(values(reader, "acme-001")) == 4


def test_upsert_and_replace_results(writer: LakeWriter, reader: LakeReader) -> None:
    writer.append("acme-001", SHARED, rows(range(3), "a"))
    up = writer.upsert("acme-001", SHARED, rows([2, 3], "new"))
    assert (up.operation, up.rows_inserted, up.rows_updated) == ("upsert", 1, 1)
    assert values(reader, "acme-001") == {0: "a", 1: "a", 2: "new", 3: "new"}
    writer.append("acme-001", SHARED, rows([10, 11], "other day", day="2026-10-20"))
    rep = writer.replace(
        "acme-001", SHARED, rows([7], "replaced"), where=[("day", "==", "2026-10-19")]
    )
    assert (rep.rows_deleted, rep.rows_inserted) == (4, 1)
    assert values(reader, "acme-001") == {7: "replaced", 10: "other day", 11: "other day"}


def test_a_stale_writer_applies_its_change_again(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    writer.append("acme-001", SHARED, rows(range(3), "a"))
    stale = catalog.load_table("silver.items")
    writer.upsert("acme-001", SHARED, rows([1], "from-other-writer"))
    loads = iter([stale])
    original = writer._load

    def load(identifier: str) -> Any:
        return next(loads, None) or original(identifier)

    writer._load = load  # type: ignore[method-assign]
    result = writer.upsert("acme-001", SHARED, rows([2], "from-stale-writer"))
    assert result.attempts == 2, "conflict detected, applied again on the new state"
    assert values(reader, "acme-001") == {0: "a", 1: "from-other-writer", 2: "from-stale-writer"}


def test_a_tenants_own_table_lives_in_its_location(
    writer: LakeWriter, reader: LakeReader, bucket: str
) -> None:
    writer.create_table("acme-001", OWN, SCHEMA)
    result = writer.append("acme-001", OWN, rows(range(2), "own"))
    assert result.table == "silver.events__acme_001"
    files = reader.files("acme-001", OWN)
    assert files and files[0].startswith(
        f"s3://{bucket}/silver/tenant_id=acme-001/data_type=events/"
    )
    assert values(reader, "acme-001", OWN) == {0: "own", 1: "own"}


def test_compaction_rewrites_small_files_only_for_the_tenant(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    for i in range(6):
        writer.append("acme-001", SHARED, rows([i], "a"))
    for i in range(3):
        writer.append("tenant-b", SHARED, rows([i], "b"))
    table = catalog.load_table("silver.items")
    before = len(table.inspect.files())
    result = LakeMaintenance(writer).compact("acme-001", SHARED, min_files=4)
    assert (result.partitions, result.files_removed, result.files_added) == (1, 6, 1)
    after = catalog.load_table("silver.items").inspect.files()
    assert len(after) == before - 5
    assert values(reader, "acme-001") == {i: "a" for i in range(6)}
    assert values(reader, "tenant-b") == {i: "b" for i in range(3)}
    assert LakeMaintenance(writer).compact("acme-001", SHARED).partitions == 0


def test_compaction_backs_off_when_a_writer_changed_the_files(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    for i in range(5):
        writer.append("acme-001", SHARED, rows([i], "a"))
    maintenance = LakeMaintenance(writer)
    stale = catalog.load_table("silver.items")
    writer.upsert("acme-001", SHARED, rows([0], "updated meanwhile"))
    loads = iter([stale, stale])
    original = writer._load
    writer._load = lambda identifier: next(loads, None) or original(identifier)  # type: ignore[method-assign]
    result = maintenance.compact("acme-001", SHARED, min_files=4)
    assert result.conflicts == 1 and result.partitions == 0
    writer._load = original  # type: ignore[method-assign]
    assert values(reader, "acme-001")[0] == "updated meanwhile", "the writer won"
    assert len(values(reader, "acme-001")) == 5


def test_expire_snapshots_keeps_the_latest(writer: LakeWriter, catalog: Any) -> None:
    for i in range(5):
        writer.append("acme-001", SHARED, rows([i], "a"), batch_id=f"B-{i}")
    expired = LakeMaintenance(writer).expire_snapshots(
        "acme-001", SHARED, older_than_hours=0, retain_last=2
    )
    assert expired == 3
    assert len(catalog.load_table("silver.items").snapshots()) == 2
    # Batches of expired snapshots are no longer recognized.
    assert not writer.append("acme-001", SHARED, rows([0], "a"), batch_id="B-0").skipped
    assert writer.append("acme-001", SHARED, rows([4], "a"), batch_id="B-4").skipped


def test_upserts_with_thousands_of_composite_keys(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    """PyIceberg's own upsert nests one Or per composite key and overflows the
    stack with thousands of keys; the writer builds a shallow filter."""
    multi = TableRef.iceberg("silver", "pairs", identifier="silver.pairs")
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("v", pa.string())])
    writer.create_table("acme-001", multi, schema, keys=["a", "b"])
    n = 3_000
    base = pa.table({"a": [i % 50 for i in range(n)], "b": list(range(n)), "v": ["old"] * n})
    writer.append("acme-001", multi, base)
    change = pa.table(
        {
            "a": [i % 50 for i in range(n - 1_000, n + 1_000)],
            "b": list(range(n - 1_000, n + 1_000)),
            "v": ["new"] * 2_000,
        }
    )
    result = writer.upsert("acme-001", multi, change)
    assert (result.rows_updated, result.rows_inserted) == (1_000, 1_000)
    t = reader.read("acme-001", multi)
    assert t.num_rows == n + 1_000
    assert t.column("v").to_pylist().count("new") == 2_000


def test_upsert_rejects_duplicate_keys(writer: LakeWriter) -> None:
    from datalake_medallion_sdk import ConfigurationError

    with pytest.raises(ConfigurationError, match="duplicate keys"):
        writer.upsert("acme-001", SHARED, rows([1, 1], "dup"))


async def test_incremental_bronze_to_silver_is_idempotent(
    make_client: Any, writer: LakeWriter, reader: LakeReader
) -> None:
    """The incremental job pattern: Bronze manifests -> changes -> Silver upserts
    keyed by the Bronze batch, safe to run again."""
    from .conftest import CONTRACT

    bronze = TableRef.bronze("login_event")
    silver = TableRef.iceberg("silver", "logins", identifier="silver.logins")
    writer.create_table(
        "acme-001",
        silver,
        pa.schema([("event_id", pa.string()), ("attempts", pa.int64())]),
        keys=["event_id"],
    )
    client = make_client(manifests=True)
    for start in (0, 100):
        batch = [{"event_id": f"e{i}", "attempts": i} for i in range(start, start + 10)]
        await client.ingest(batch, tenant="acme-001", contract=CONTRACT)

    def run(since: str | None) -> tuple[str | None, list[Any]]:
        changes = reader.changes("acme-001", bronze, since=since, settle_seconds=0)
        results = []
        for entry in changes.entries:
            one = type(changes)([entry], changes.watermark)
            rows = reader.read_changes("acme-001", bronze, one, columns=["event_id", "attempts"])
            results.append(writer.upsert("acme-001", silver, rows, batch_id=entry["batch_id"]))
        return changes.watermark, results

    watermark, first = run(None)
    assert [r.rows_inserted for r in first] == [10, 10]
    _, again = run(None)  # the job crashed before saving its watermark
    assert all(r.skipped for r in again), "batches already in Silver are skipped"
    _, nothing = run(watermark)
    assert nothing == []
    assert reader.read("acme-001", silver).num_rows == 20


def test_upsert_collapses_keys_stored_twice(writer: LakeWriter, reader: LakeReader) -> None:
    writer.append("acme-001", SHARED, rows([1, 2], "a"))
    writer.append("acme-001", SHARED, rows([1], "dup"))  # append does not enforce keys
    result = writer.upsert("acme-001", SHARED, rows([1, 3], "new"))
    assert (result.rows_updated, result.rows_inserted, result.rows_deleted) == (1, 1, 1)
    assert values(reader, "acme-001") == {1: "new", 2: "a", 3: "new"}


def test_commits_rejected_by_the_catalog_are_applied_again(
    writer: LakeWriter, reader: LakeReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import Table

    original = Table.append
    calls = {"n": 0}

    def flaky(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise CommitFailedException("concurrent update to table version 8")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Table, "append", flaky)
    result = writer.append("acme-001", SHARED, rows(range(3), "a"), batch_id="B-retry")
    assert result.attempts == 2
    assert values(reader, "acme-001") == {0: "a", 1: "a", 2: "a"}


def test_partition_scoped_upserts_leave_other_partitions_alone(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    writer.append("acme-001", SHARED, rows(range(5), "d1", day="2026-10-01"))
    writer.append("acme-001", SHARED, rows(range(5, 10), "d2", day="2026-10-02"))
    before = {
        f.file_path
        for f in catalog.load_table("silver.items").scan().plan_files()
        for f in [f.file]
        if "2026-10-02" in f.file_path
    }
    result = writer.upsert(
        "acme-001", SHARED, rows([1, 20], "new", day="2026-10-01"), partition_scoped=True
    )
    assert (result.rows_updated, result.rows_inserted) == (1, 1)
    after = {
        f.file.file_path
        for f in catalog.load_table("silver.items").scan().plan_files()
        if "2026-10-02" in f.file.file_path
    }
    assert after == before, "the other day's files were not rewritten"
    assert values(reader, "acme-001")[1] == "new"


def test_compaction_stops_within_its_budget_and_resumes(
    writer: LakeWriter, reader: LakeReader
) -> None:
    for day in ("2026-10-01", "2026-10-02", "2026-10-03"):
        for i in range(4):
            writer.append("acme-001", SHARED, rows([i], "a", day=day))
    maintenance = LakeMaintenance(writer)
    first = maintenance.compact("acme-001", SHARED, max_groups=1)
    assert (first.partitions, first.remaining) == (1, 2)
    rest = maintenance.compact("acme-001", SHARED)
    assert (rest.partitions, rest.remaining) == (2, 0)
    assert len(reader.read("acme-001", SHARED).column("id")) == 12


def test_iceberg_reads_are_served_from_the_object_cache(
    writer: LakeWriter, s3_endpoint: str, tenants: TenantRegistry, catalog: Any
) -> None:
    from datalake_medallion_sdk import reader as reader_module

    writer.append("acme-001", SHARED, rows(range(3), "a"))
    writer.append("tenant-b", SHARED, rows(range(2), "b"))
    loads: list[str] = []
    original = reader_module._ObjectCache._read

    def counting(self: Any, path: str, hive: bool) -> Any:
        loads.append(path)
        return original(self, path, hive)

    reader_module._ObjectCache._read = counting  # type: ignore[method-assign]
    try:
        cached = LakeReader(
            tenants,
            catalog=catalog,
            cache_mb=64,
            max_staleness=0,
            aws_region="us-east-1",
            aws_endpoint_url=s3_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        first = cached.read("acme-001", SHARED, order_by=["id"])
        assert first.column("v").to_pylist() == ["a"] * 3
        assert first.column_names == ["tenant_id", "id", "day", "v"], "no columns from paths"
        assert all("tenant_id=acme-001" in p for p in loads), "only the tenant's files"
        fetched = len(loads)
        assert cached.read("acme-001", SHARED).num_rows == 3
        assert len(loads) == fetched, "a warm read touches no object"
        writer.append("acme-001", SHARED, rows([9], "new"))
        assert cached.read("acme-001", SHARED).num_rows == 4
        assert len(loads) == fetched + 1, "only the new data file is fetched"
        cached.close()
    finally:
        reader_module._ObjectCache._read = original  # type: ignore[method-assign]


def test_large_upserts_never_duplicate_a_key_a_concurrent_writer_added(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    """The Arrow-matched path (> 1,000 keys) commits an exact file swap; under
    serializable isolation a file added meanwhile fails it and the upsert is
    applied again, so a key is never stored twice."""
    writer.append("acme-001", SHARED, rows(range(3_000), "a"))
    stale = catalog.load_table("silver.items")
    writer.append("acme-001", SHARED, rows([5_000], "concurrent"))  # a key the upsert also writes
    loads = iter([stale])
    original = writer._load
    writer._load = lambda identifier: next(loads, None) or original(identifier)  # type: ignore[method-assign]
    result = writer.upsert("acme-001", SHARED, rows(range(2_000, 5_001), "upserted"))
    writer._load = original  # type: ignore[method-assign]
    assert result.attempts == 2, "the stale commit was rejected and applied again"
    assert (result.rows_updated, result.rows_inserted) == (1_001, 2_000)
    t = reader.read("acme-001", SHARED)
    ids = t.column("id").to_pylist()
    assert len(ids) == len(set(ids)) == 5_001, "no key stored twice"
    assert values(reader, "acme-001")[5_000] == "upserted"


def test_orphan_files_are_removed_and_referenced_files_kept(
    writer: LakeWriter, catalog: Any, reader: LakeReader
) -> None:
    for i in range(5):
        writer.append("acme-001", SHARED, rows([i], "a"))
    LakeMaintenance(writer).compact("acme-001", SHARED)  # old files stay referenced by snapshots
    table = catalog.load_table("silver.items")
    stray = f"{table.location()}/data/stray-from-a-failed-commit.parquet"
    import pyarrow.parquet as pq

    with table.io.new_output(stray).create() as out:
        pq.write_table(pa.table({"x": [1]}), out)
    maintenance = LakeMaintenance(writer)
    preview = maintenance.remove_orphan_files("acme-001", SHARED, older_than_hours=0, dry_run=True)
    assert preview.paths == [stray] and preview.files == 1
    assert table.io.new_input(stray).exists(), "dry run deletes nothing"
    done = maintenance.remove_orphan_files("acme-001", SHARED, older_than_hours=0)
    assert done.files == 1 and not table.io.new_input(stray).exists()
    assert maintenance.remove_orphan_files("acme-001", SHARED, older_than_hours=0).files == 0
    assert values(reader, "acme-001") == {i: "a" for i in range(5)}
    recent = maintenance.remove_orphan_files("acme-001", SHARED, dry_run=True)
    assert recent.files == 0, "files newer than the safety window are never candidates"


def test_add_columns(writer: LakeWriter, reader: LakeReader) -> None:
    from datalake_medallion_sdk import ConfigurationError

    writer.append("acme-001", SHARED, rows([1], "a"))
    extended = rows([2], "b").append_column("score", pa.array([0.5]))
    with pytest.raises(ConfigurationError, match="add_columns"):
        writer.append("acme-001", SHARED, extended)
    writer.add_columns("acme-001", SHARED, pa.schema([("score", pa.float64())]))
    writer.append("acme-001", SHARED, extended)
    t = reader.read("acme-001", SHARED, columns=["id", "score"], order_by=["id"])
    assert t.column("score").to_pylist() == [None, 0.5]
    with pytest.raises(ConfigurationError, match="another type"):
        writer.add_columns("acme-001", SHARED, pa.schema([("score", pa.string())]))
