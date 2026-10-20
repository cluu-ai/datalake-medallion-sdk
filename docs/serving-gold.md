# Serving Gold tables to an API

Gold tables are small, pre-aggregated and read far more often than they are written: the same tenant asks for the same summaries and pages again and again. This guide shows how to serve them from an API with `LakeReader`, and what each choice costs. Numbers are from AWS Lambda against S3 with an AWS Glue catalog (see [performance.md](performance.md#serving-gold-on-aws)).

## The reader

Create one reader per process, with the object cache:

```python
from pyiceberg.catalog import load_catalog
from datalake_medallion_sdk import LakeReader, TableRef

reader = LakeReader(
    tenants,
    catalog=load_catalog("lake", type="glue"),
    engine="polars",
    cache_mb=2048,
    max_staleness=120,
)
checks = TableRef.iceberg("gold", "compliance_checks", identifier="gold.compliance_checks")
```

- `cache_mb` keeps the data files of the current snapshot in memory, only the tenant's partition of a shared table. Data files are never rewritten in place, so a warm request reads nothing from S3 and a new commit fetches only its new files. Size it for the hot tables of the tenants a process serves.
- `max_staleness` bounds how old a snapshot may be before the reader checks the catalog again. `reader.refresh(tenant)` makes a commit visible at once.
- The tenant filter is applied by the reader on every query; callers cannot remove it.

Measured: p50 6.3 ms, p95 36 ms, against 208 / 555 ms for DuckDB attached to the catalog with table metadata cached, and 823 ms without any cache.

## Per request

1. **Cache what does not change.** A completed scan, report or period never changes: resolve its identity (and its partition value) once and keep it in a process cache. Tenant configuration (locations, catalogs) changes rarely: cache it for minutes instead of reading a parameter store on every request.
2. **Filter on the partition column.** If a table is partitioned by month, add the month to every query. Without it a query plans over every month of the tenant, and gets slower as data accumulates.
3. **Page by key, not by offset.** Store a row number per entity (for example per scan and family) and read `ordinal BETWEEN first AND last` ordered by it.
4. **Combine small queries.** A total and its page can come from one query; every query is a round trip.
5. **Refresh with a bound.** When an entity is not found yet (data not committed), refresh that tenant once and retry, but no more than every few seconds: refreshing on every miss drops the cache for everyone under load.

```python
rows = reader.read(
    "acme-001",
    checks,
    columns=["requirement_id", "status", "resource_id"],
    where=[
        ("report_month", "==", month),
        ("scan_id", "==", scan_id),
        ("framework_family", "==", "cis"),
        ("ordinal", ">=", first),
        ("ordinal", "<=", last),
    ],
    order_by=["ordinal"],
    limit=page_size,
)
```

## Writing the tables

Gold tables written with `LakeWriter` are shared by tenants (a `tenant_id` column, partitioned by it), so one table serves every tenant and tenants never conflict on writes. Write them sorted by the columns requests filter on (entity id, then the page key) so Parquet statistics skip row groups, and run `LakeMaintenance.compact` on a schedule so each partition holds few files.
