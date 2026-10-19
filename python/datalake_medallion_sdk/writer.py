"""Silver and Gold writer for Iceberg tables, with a separate maintenance client.

:class:`LakeWriter` appends, upserts and replaces rows of a tenant in an
Iceberg table through a catalog (AWS Glue, REST, SQL...), with the guarantees
a multi-tenant lake needs:

* **Tenant isolation.** A tenant only writes its own rows: in a table shared
  by tenants, ``tenant_id`` is set or checked on every row and every
  ``upsert`` / ``replace`` is scoped to ``tenant_id = <tenant>``. Tables of
  one tenant live below its own location.
* **Concurrency.** Commits are optimistic. PyIceberg retries a commit that
  raced with an unrelated one; when the race is a real conflict (another
  writer changed the same rows), the writer reloads the table and applies the
  operation again on the new state, up to ``conflict_retries`` times.
  Partition shared tables by ``tenant_id`` so tenants never conflict.
* **Idempotency.** With ``batch_id=``, the batch id is stored in the snapshot
  summary; writing the same batch again (a retried job) is skipped. Snapshot
  expiry bounds how far back a batch is remembered.

:class:`LakeMaintenance` compacts small files and expires snapshots on a
schedule, outside the write path: it rewrites exact files and gives up on a
conflict (writers win), so it never slows down or races a write.

Needs ``pip install "datalake-medallion-sdk[write]"`` (PyIceberg and pyarrow).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from ._core import ConfigurationError, TenantProfile, TenantRegistry, TenantViolationError
from ._telemetry import traced
from .reader import TENANT_COLUMN, Predicate, TableRef, _predicates
from .tenancy import TenantResolver

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.table import Table

__all__ = ["CompactionResult", "LakeMaintenance", "LakeWriter", "WriteResult"]

logger = logging.getLogger(__name__)

BATCH_PROPERTY = "datalake.batch-id"
TENANT_PROPERTY = "datalake.tenant-id"
OPERATION_PROPERTY = "datalake.operation"

Operation = Literal["append", "upsert", "replace"]


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of one :class:`LakeWriter` operation."""

    operation: Operation
    tenant_id: str
    table: str
    """Catalog identifier of the table written."""
    snapshot_id: int | None
    """Snapshot committed (or the one that already holds the batch)."""
    batch_id: str | None
    rows_inserted: int
    """New rows."""
    rows_updated: int
    """Rows of the table changed by an upsert."""
    rows_deleted: int
    """Rows of the table removed by a replace."""
    files_added: int
    """Data files written, including unchanged rows rewritten copy-on-write."""
    skipped: bool = False
    """The batch was already committed; nothing was written."""
    attempts: int = 1
    """Commits tried, counting re-applications after a conflict."""


@dataclass(frozen=True, slots=True)
class CompactionResult:
    """Outcome of :meth:`LakeMaintenance.compact`."""

    table: str
    partitions: int
    """Partitions rewritten."""
    files_removed: int
    files_added: int
    bytes_rewritten: int
    conflicts: int
    """Partitions left for the next run because a writer changed them meanwhile."""


def _expressions() -> Any:
    """``pyiceberg.expressions``, typed loosely: its classes are pydantic
    models whose constructors type checkers cannot read (``EqualTo(term,
    literal)`` works at run time)."""
    import pyiceberg.expressions

    return pyiceberg.expressions


def _require_pyiceberg() -> None:
    try:
        import pyiceberg  # noqa: F401
    except ImportError as e:  # pragma: no cover - depends on the environment
        raise ConfigurationError(
            "LakeWriter needs PyIceberg: pip install 'datalake-medallion-sdk[write]'"
        ) from e


def _slug(text: str) -> str:
    """A catalog-safe name part (Glue: lowercase letters, digits, underscore)."""
    return "".join(c if c.isalnum() else "_" for c in text.lower())


def _expression(where: list[Predicate]) -> BooleanExpression:
    """PyIceberg expression of reader-style ``(column, op, value)`` filters."""
    ex = _expressions()
    operators: dict[str, Any] = {
        "==": ex.EqualTo,
        "!=": ex.NotEqualTo,
        "<": ex.LessThan,
        "<=": ex.LessThanOrEqual,
        ">": ex.GreaterThan,
        ">=": ex.GreaterThanOrEqual,
        "in": ex.In,
        "not in": ex.NotIn,
    }
    out: Any = ex.AlwaysTrue()
    for column, op, value in where:
        if value is None and op in ("==", "!="):
            term = ex.IsNull(column) if op == "==" else ex.NotNull(column)
        else:
            term = operators[op](column, value)
        out = term if isinstance(out, ex.AlwaysTrue) else ex.And(out, term)
    return cast("BooleanExpression", out)


def _arrow(data: Any) -> pa.Table:
    """Any Arrow-compatible input as a pyarrow table (PyIceberg needs one for
    partitioned writes)."""
    import pyarrow as pa

    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    if isinstance(data, pa.RecordBatchReader):
        return data.read_all()
    if hasattr(data, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(data).read_all()
    if hasattr(data, "to_arrow"):  # Polars, DuckDB relations
        return _arrow(data.to_arrow())
    if isinstance(data, list):
        return pa.Table.from_pylist(data)
    raise TypeError(
        "data must be Arrow (pyarrow Table/RecordBatch/RecordBatchReader, Polars, any "
        f"__arrow_c_stream__ object) or a list of dicts, got {type(data).__name__}"
    )


class LakeWriter:
    """Writes a tenant's rows to Silver or Gold Iceberg tables.

    Args:
        tenants: Where ``tenant="<id>"`` finds the profile (as for
            :class:`LakeReader`); not needed when calls pass a ``TenantProfile``.
        catalog: A PyIceberg catalog (``pyiceberg.catalog.load_catalog(...)``):
            AWS Glue, REST, SQL... Iceberg commits need one.
        namespace: Catalog namespace of tables that belong to one tenant;
            defaults to the layer (``silver`` / ``gold``). Shared tables are
            named by ``TableRef.iceberg(..., identifier="namespace.table")``.
        conflict_retries: Times an operation is applied again after a real
            conflict with another writer.
    """

    def __init__(
        self,
        tenants: TenantRegistry | TenantResolver | None = None,
        *,
        catalog: Catalog,
        namespace: str | None = None,
        conflict_retries: int = 3,
    ) -> None:
        _require_pyiceberg()
        if conflict_retries < 0:
            raise ConfigurationError("conflict_retries must not be negative")
        self._tenants = tenants
        self._catalog = catalog
        self._namespace = namespace
        self._conflict_retries = conflict_retries

    # -- tables -----------------------------------------------------------

    def _profile(self, tenant: TenantProfile | str) -> TenantProfile:
        if isinstance(tenant, TenantProfile):
            return tenant
        if isinstance(self._tenants, TenantRegistry):
            return self._tenants.get(tenant)
        if self._tenants is None:
            raise ConfigurationError("pass a TenantProfile, or create the writer with tenants=")
        profile = self._tenants(tenant)
        if not isinstance(profile, TenantProfile) or profile.tenant_id != tenant:
            raise TenantViolationError(f"tenant resolver returned a wrong profile for {tenant!r}")
        return profile

    def identifier(self, tenant: TenantProfile | str, table: TableRef) -> str:
        """Catalog identifier of ``table`` for ``tenant``: the shared table's
        ``identifier``, or ``<namespace>.<name>__<tenant>`` for a tenant's own table."""
        if table.format != "iceberg" or table.layer == "bronze":
            raise ConfigurationError("LakeWriter writes Silver and Gold Iceberg tables")
        if table.shared:
            if table.identifier is None:
                raise ConfigurationError(
                    "shared tables are written through the catalog: name them with identifier="
                )
            return table.identifier
        profile = self._profile(tenant)
        namespace = self._namespace or table.layer
        return f"{namespace}.{_slug(table.name)}__{_slug(profile.tenant_id)}"

    def create_table(
        self,
        tenant: TenantProfile | str,
        table: TableRef,
        schema: pa.Schema,
        *,
        partition_by: Sequence[str] = (),
        keys: Sequence[str] = (),
        properties: dict[str, str] | None = None,
    ) -> Table:
        """Creates ``table`` if it does not exist and returns it.

        A shared table gets a required ``tenant_id`` column and is partitioned
        by it first, so tenants never conflict; ``partition_by`` adds identity
        partitions. ``keys`` are the identifier fields used by :meth:`upsert`
        (``tenant_id`` is added for shared tables). A tenant's own table is
        created below its location for the layer.
        """
        import pyarrow as pa
        from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
        from pyiceberg.io.pyarrow import pyarrow_to_schema
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.table.name_mapping import MappedField, NameMapping
        from pyiceberg.transforms import IdentityTransform

        profile = self._profile(tenant)
        identifier = self.identifier(profile, table)
        fields = list(schema)
        if table.shared and TENANT_COLUMN not in schema.names:
            fields.insert(0, pa.field(TENANT_COLUMN, pa.string(), nullable=False))
        if table.shared:
            fields = [f.with_nullable(False) if f.name == TENANT_COLUMN else f for f in fields]
        key_names = list(keys)
        if table.shared and key_names and TENANT_COLUMN not in key_names:
            key_names.insert(0, TENANT_COLUMN)
        fields = [f.with_nullable(False) if f.name in key_names else f for f in fields]
        arrow_schema = pa.schema(fields)
        mapping = NameMapping(
            [
                cast(Any, MappedField)(field_id=i + 1, names=[f.name])
                for i, f in enumerate(arrow_schema)
            ]
        )
        iceberg_schema: Schema = pyarrow_to_schema(arrow_schema, name_mapping=mapping)
        if key_names:
            ids = [iceberg_schema.find_field(k).field_id for k in key_names]
            iceberg_schema = Schema(*iceberg_schema.fields, identifier_field_ids=ids)
        partition_names = ([TENANT_COLUMN] if table.shared else []) + [
            p for p in partition_by if p != TENANT_COLUMN
        ]
        spec = PartitionSpec(
            *(
                PartitionField(
                    source_id=iceberg_schema.find_field(name).field_id,
                    field_id=1000 + i,
                    transform=IdentityTransform(),
                    name=name,
                )
                for i, name in enumerate(partition_names)
            )
        )
        namespace = identifier.rsplit(".", 1)[0]
        try:
            self._catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        location = None if table.shared else profile.table_uri(table.layer, table.name)
        if not table.shared and location is None:
            raise ConfigurationError(f"tenant {profile.tenant_id!r} has no {table.layer} location")
        try:
            return self._catalog.create_table(
                identifier,
                schema=iceberg_schema,
                partition_spec=spec,
                location=location,
                properties=properties or {},
            )
        except TableAlreadyExistsError:
            return self._catalog.load_table(identifier)

    def _load(self, identifier: str) -> Table:
        return self._catalog.load_table(identifier)

    # -- rows ---------------------------------------------------------------

    def _tenant_rows(self, profile: TenantProfile, table: TableRef, data: Any) -> pa.Table:
        """The input as Arrow, with the tenant column set (shared tables) or
        checked: no row may belong to another tenant."""
        import pyarrow as pa
        import pyarrow.compute as pc

        rows = _arrow(data)
        tenant_id = profile.tenant_id
        if TENANT_COLUMN in rows.column_names:
            column = rows.column(TENANT_COLUMN)
            if column.null_count or (len(rows) and not pc.all(pc.equal(column, tenant_id)).as_py()):
                raise TenantViolationError(
                    f"rows for another tenant than {tenant_id!r} (or without tenant_id)"
                )
        elif table.shared:
            rows = rows.add_column(0, TENANT_COLUMN, pa.array([tenant_id] * len(rows), pa.string()))
        return rows

    @staticmethod
    def _cast(rows: pa.Table, iceberg: Table) -> pa.Table:
        """Rows in the table's column order and types (nullability included)."""
        target = iceberg.schema().as_arrow()
        names = [f.name for f in target]
        missing = [n for n in rows.column_names if n not in names]
        if missing:
            raise ConfigurationError(f"columns not in the table: {missing}")
        import pyarrow as pa

        columns = [
            rows.column(f.name).cast(f.type)
            if f.name in rows.column_names
            else pa.nulls(len(rows), f.type)
            for f in target
        ]
        return pa.Table.from_arrays(columns, schema=target)

    @staticmethod
    def _committed(iceberg: Table, batch_id: str, tenant_id: str) -> int | None:
        """Snapshot that already holds ``batch_id`` for ``tenant_id``."""
        for snapshot in reversed(iceberg.snapshots()):
            props = snapshot.summary.additional_properties if snapshot.summary else {}
            if props.get(BATCH_PROPERTY) == batch_id and props.get(TENANT_PROPERTY) == tenant_id:
                return snapshot.snapshot_id
        return None

    def _write(
        self,
        operation: Operation,
        tenant: TenantProfile | str,
        table: TableRef,
        data: Any,
        batch_id: str | None,
        apply: Callable[
            [Table, pa.Table, dict[str, str], BooleanExpression], tuple[int, int] | None
        ],
        where: list[Predicate] | None = None,
    ) -> WriteResult:
        from pyiceberg.exceptions import ValidationException

        ex = _expressions()

        profile = self._profile(tenant)
        identifier = self.identifier(profile, table)
        rows = self._tenant_rows(profile, table, data)
        scope: BooleanExpression = _expression(where or [])
        if table.shared:
            tenant_only = ex.EqualTo(TENANT_COLUMN, profile.tenant_id)
            scope = tenant_only if not where else ex.And(tenant_only, scope)
        props = {OPERATION_PROPERTY: operation, TENANT_PROPERTY: profile.tenant_id}
        if batch_id is not None:
            props[BATCH_PROPERTY] = batch_id
        attempts = 0
        while True:
            attempts += 1
            iceberg = self._load(identifier)
            if batch_id is not None:
                done = self._committed(iceberg, batch_id, profile.tenant_id)
                if done is not None:
                    return WriteResult(
                        operation,
                        profile.tenant_id,
                        identifier,
                        done,
                        batch_id,
                        0,
                        0,
                        0,
                        0,
                        skipped=True,
                        attempts=attempts,
                    )
            before = iceberg.current_snapshot()
            try:
                logical = apply(iceberg, self._cast(rows, iceberg), props, scope)
                break
            except ValidationException:
                if attempts > self._conflict_retries:
                    raise
                logger.info(
                    "conflict on %s for %s, applying %s again (attempt %d)",
                    identifier,
                    profile.tenant_id,
                    operation,
                    attempts + 1,
                )
                time.sleep(min(2.0, 0.1 * 2**attempts))
        iceberg = self._load(identifier)
        snapshot = iceberg.current_snapshot()
        if snapshot is None or (before is not None and snapshot.snapshot_id == before.snapshot_id):
            # An upsert with nothing to change commits no snapshot.
            return WriteResult(
                operation,
                profile.tenant_id,
                identifier,
                None if snapshot is None else snapshot.snapshot_id,
                batch_id,
                0,
                0,
                0,
                0,
                attempts=attempts,
            )
        # An operation may commit several snapshots (upsert: overwrite then
        # append): add up every snapshot after the one it started from.
        totals = {"added-records": 0, "deleted-records": 0, "added-data-files": 0}
        stop = None if before is None else before.snapshot_id
        current: Any = snapshot
        while current is not None and current.snapshot_id != stop:
            summary = current.summary.additional_properties if current.summary else {}
            for key in totals:
                totals[key] += int(summary.get(key, 0))
            parent = current.parent_snapshot_id
            current = None if parent is None else iceberg.snapshot_by_id(parent)
        inserted, updated = logical if logical is not None else (len(rows), 0)
        # Copy-on-write rewrites unchanged rows of touched files: they are in
        # both added and deleted records, and are not deletions.
        rewritten = totals["added-records"] - inserted - updated
        deleted = totals["deleted-records"] - rewritten - updated
        return WriteResult(
            operation,
            profile.tenant_id,
            identifier,
            snapshot.snapshot_id,
            batch_id,
            rows_inserted=inserted,
            rows_updated=updated,
            rows_deleted=max(deleted, 0),
            files_added=totals["added-data-files"],
            attempts=attempts,
        )

    @traced("write.append")
    def append(
        self,
        tenant: TenantProfile | str,
        table: TableRef,
        data: Any,
        *,
        batch_id: str | None = None,
    ) -> WriteResult:
        """Appends ``data`` (Arrow, Polars or a list of dicts) for ``tenant``.

        With ``batch_id``, a batch already committed is skipped. Two writers
        submitting the same batch at the very same time can both append it;
        use :meth:`upsert` when rows have keys.
        """

        def apply(t: Table, rows: pa.Table, props: dict[str, str], _: Any) -> None:
            t.append(rows, snapshot_properties=props)
            return None

        return self._write("append", tenant, table, data, batch_id, apply)

    @traced("write.upsert")
    def upsert(
        self,
        tenant: TenantProfile | str,
        table: TableRef,
        data: Any,
        *,
        keys: Sequence[str] | None = None,
        batch_id: str | None = None,
    ) -> WriteResult:
        """Updates rows of ``tenant`` matching ``keys`` and inserts the others.

        ``keys`` default to the table's identifier fields; in a shared table
        ``tenant_id`` is always part of them. Rows are rewritten copy-on-write:
        upsert batches that touch few partitions.
        """
        join = list(keys) if keys else None
        if join is not None and table.shared and TENANT_COLUMN not in join:
            join.insert(0, TENANT_COLUMN)

        def apply(t: Table, rows: pa.Table, props: dict[str, str], _: Any) -> tuple[int, int]:
            result = t.upsert(rows, join_cols=join, snapshot_properties=props)
            return result.rows_inserted, result.rows_updated

        return self._write("upsert", tenant, table, data, batch_id, apply)

    @traced("write.replace")
    def replace(
        self,
        tenant: TenantProfile | str,
        table: TableRef,
        data: Any,
        *,
        where: Iterable[Predicate] = (),
        batch_id: str | None = None,
    ) -> WriteResult:
        """Replaces ``tenant``'s rows matching ``where`` with ``data`` in one
        commit (for example a day: ``where=[("day", "==", "2026-10-19")]``).
        Without ``where`` all the tenant's rows are replaced."""
        predicates = _predicates(where)

        def apply(
            t: Table, rows: pa.Table, props: dict[str, str], scope: BooleanExpression
        ) -> None:
            t.overwrite(rows, overwrite_filter=scope, snapshot_properties=props)
            return None

        return self._write("replace", tenant, table, data, batch_id, apply, predicates)


class LakeMaintenance:
    """Scheduled maintenance of the writer's tables: compaction and snapshot
    expiry. Run it outside the write path (a scheduled job); it rewrites
    exact files and leaves a partition for the next run when a writer
    changed it meanwhile, so writers always win."""

    def __init__(self, writer: LakeWriter) -> None:
        self._writer = writer

    @traced("maintenance.compact")
    def compact(
        self,
        tenant: TenantProfile | str,
        table: TableRef,
        *,
        target_file_mb: int = 128,
        min_files: int = 4,
    ) -> CompactionResult:
        """Rewrites the tenant's partitions that have at least ``min_files``
        files smaller than ``target_file_mb`` into files of about that size."""
        from pyiceberg.exceptions import ValidationException
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        writer = self._writer
        profile = writer._profile(tenant)
        identifier = writer.identifier(profile, table)
        iceberg = writer._load(identifier)
        ex = _expressions()
        scope = ex.EqualTo(TENANT_COLUMN, profile.tenant_id) if table.shared else ex.AlwaysTrue()
        small = target_file_mb * 1024 * 1024 * 3 // 4
        by_partition: dict[Any, list[Any]] = {}
        for task in iceberg.scan(row_filter=scope).plan_files():
            if task.delete_files:
                continue  # merge-on-read files are left to engines that write them
            if task.file.file_size_in_bytes < small:
                by_partition.setdefault(task.file.partition, []).append(task.file)
        result = {"partitions": 0, "removed": 0, "added": 0, "bytes": 0, "conflicts": 0}
        for files in by_partition.values():
            if len(files) < min_files:
                continue
            iceberg = writer._load(identifier)
            import pyarrow.parquet as pq

            arrays = [pq.read_table(iceberg.io.new_input(f.file_path).open()) for f in files]
            import pyarrow as pa

            rows = pa.concat_tables(arrays, promote_options="default").cast(
                iceberg.schema().as_arrow()
            )
            try:
                tx = iceberg.transaction()
                with tx.update_snapshot(
                    snapshot_properties={OPERATION_PROPERTY: "compact"}
                ).overwrite() as rewrite:
                    for f in files:
                        rewrite.delete_data_file(f)
                    added = list(
                        _dataframe_to_data_files(
                            table_metadata=tx.table_metadata, df=rows, io=iceberg.io
                        )
                    )
                    for f in added:
                        rewrite.append_data_file(f)
                tx.commit_transaction()
            except ValidationException:
                result["conflicts"] += 1
                logger.info("compaction of %s skipped a partition changed meanwhile", identifier)
                continue
            result["partitions"] += 1
            result["removed"] += len(files)
            result["added"] += len(added)
            result["bytes"] += sum(f.file_size_in_bytes for f in files)
        return CompactionResult(
            identifier,
            result["partitions"],
            result["removed"],
            result["added"],
            result["bytes"],
            result["conflicts"],
        )

    @traced("maintenance.expire_snapshots")
    def expire_snapshots(
        self,
        tenant: TenantProfile | str,
        table: TableRef,
        *,
        older_than_hours: float = 168.0,
        retain_last: int = 10,
    ) -> int:
        """Expires snapshots older than ``older_than_hours`` except the last
        ``retain_last`` (and the current one). Returns how many. Batches whose
        snapshots expire are no longer recognized by ``batch_id``."""
        writer = self._writer
        identifier = writer.identifier(writer._profile(tenant), table)
        iceberg = writer._load(identifier)
        snapshots = sorted(iceberg.snapshots(), key=lambda s: s.timestamp_ms)
        current = iceberg.current_snapshot()
        keep = {s.snapshot_id for s in snapshots[-max(retain_last, 1) :]}
        if current is not None:
            keep.add(current.snapshot_id)
        cutoff = int((time.time() - older_than_hours * 3600) * 1000)
        expired = [
            s.snapshot_id
            for s in snapshots
            if s.timestamp_ms < cutoff and s.snapshot_id not in keep
        ]
        if expired:
            iceberg.maintenance.expire_snapshots().by_ids(expired).commit()
        return len(expired)
