"""End-to-end ingestion against an S3 emulator (moto): real uploads, real reads."""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import os
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    HookRegistry,
    PiiProtection,
    PreIngestHookResult,
    ProtectionError,
    SchemaViolationError,
    StorageError,
    TenantProfile,
)

from .conftest import CONTRACT, EMAIL, Bucket

TENANT = "acme-001"
MakeClient = Callable[..., BronzeIngestionClient]


async def test_valid_records_written_as_partitioned_parquet(
    make_client: MakeClient, store: Bucket
) -> None:
    report = await make_client().ingest(
        [{"event_id": "e1", "attempts": 2}, {"event_id": "e2"}],
        tenant=TENANT,
        contract=CONTRACT,
    )
    assert (report.records_received, report.records_ok, report.records_quarantined) == (2, 2, 0)
    assert report.dlq_uris == []
    assert report.data_uri is not None
    prefix = f"s3://{store.name}/bronze/tenant_id={TENANT}/data_type=login_event/year="
    assert report.data_uri.startswith(prefix)
    assert report.data_uri.endswith(f"/{report.batch_id}.parquet")

    table = store.parquet(report.data_uri)
    assert table.num_rows == 2
    assert table.column("event_id").to_pylist() == ["e1", "e2"]
    assert table.column("attempts").to_pylist() == [2, None]
    assert set(table.column("_dl_tenant_id").to_pylist()) == {TENANT}
    assert set(table.column("_dl_batch_id").to_pylist()) == {report.batch_id}
    assert len(set(table.column("_dl_ingest_id").to_pylist())) == 2
    assert str(table.schema.field("_dl_received_at").type) == "timestamp[us, tz=UTC]"


async def test_invalid_records_go_to_dead_letter_prefix(
    make_client: MakeClient, store: Bucket
) -> None:
    report = await make_client().ingest(
        [
            {"event_id": "ok"},
            {"attempts": 1},
            {"event_id": "x", "attempts": -3},
            {"event_id": "y", "undeclared": True},
            {"event_id": "z", "_dl_tenant_id": "someone-else"},
            {"event_id": "late", "occurred_at": "2999-01-01T00:00:00Z"},
        ],
        tenant=TENANT,
        contract=CONTRACT,
    )
    assert report.records_ok == 1
    assert report.records_quarantined == 5
    assert report.rejections == {
        "ERR_FUTURE_TIMESTAMP": 1,
        "ERR_RESERVED_FIELD": 1,
        "ERR_SCHEMA_VIOLATION": 2,
        "ERR_UNDECLARED_FIELD": 1,
    }
    assert len(report.dlq_uris) == 4
    for uri in report.dlq_uris:
        assert uri.startswith(
            f"s3://{store.name}/quarantine/tenant_id={TENANT}/data_type=login_event/"
        )
        assert uri.endswith(f"/{report.batch_id}.jsonl")

    schema_uri = next(u for u in report.dlq_uris if "error_code=ERR_SCHEMA_VIOLATION" in u)
    entries = store.dead_letters(schema_uri)
    assert [e["record_index"] for e in entries] == [1, 2]
    assert all(e["validation_level"] == 3 and e["batch_id"] == report.batch_id for e in entries)
    assert entries[1]["payload"] == {"event_id": "x", "attempts": -3}
    assert "-3" not in entries[1]["error_message"]


async def test_pii_is_stored_as_received_by_default(make_client: MakeClient, store: Bucket) -> None:
    report = await make_client().ingest(
        [
            {"event_id": "ok", "user_email": EMAIL},
            {"event_id": 7, "user_email": EMAIL},
        ],
        tenant=TENANT,
        contract=CONTRACT,
    )
    assert report.pii_protection == "none"
    assert report.tenant_id == TENANT
    assert (report.records_ok, report.records_quarantined) == (1, 1)
    table = store.parquet(report.data_uri)
    assert table.column("user_email").to_pylist() == [EMAIL]
    assert table.column("_dl_pii_protection").to_pylist() == ["none"]
    assert table.column("_dl_pii_key_ref").to_pylist() == [None]
    dead = store.dead_letters(report.dlq_uris[0])
    assert dead[0]["payload"]["user_email"] == EMAIL
    assert dead[0]["pii_protection"] == "none"
    assert EMAIL not in dead[0]["error_message"]


async def test_sha256_profile_hashes_everywhere(make_client: MakeClient, store: Bucket) -> None:
    report = await make_client(pii_protection=PiiProtection.sha256()).ingest(
        [
            {"event_id": "ok", "user_email": EMAIL},
            {"event_id": 7, "user_email": EMAIL},
        ],
        tenant=TENANT,
        contract=CONTRACT,
    )
    assert report.pii_protection == "sha256"
    assert EMAIL.encode() not in store.all_bytes()
    digest = hashlib.sha256(EMAIL.encode()).hexdigest()
    assert store.parquet(report.data_uri).column("user_email").to_pylist() == [digest]
    dead = store.dead_letters(report.dlq_uris[0])
    assert dead[0]["payload"]["user_email"] == digest
    assert dead[0]["pii_protection"] == "sha256"


async def test_hmac_pseudonymization(make_client: MakeClient, store: Bucket) -> None:
    key = b"k" * 32
    report = await make_client(pii_protection=PiiProtection.hmac_sha256(key)).ingest(
        {"event_id": "e", "user_email": EMAIL}, tenant=TENANT, contract=CONTRACT
    )
    expected = hmac.new(key, EMAIL.encode(), hashlib.sha256).hexdigest()
    assert store.parquet(report.data_uri).column("user_email").to_pylist() == [expected]


async def test_per_tenant_envelope_encryption_like_kms(
    make_client: MakeClient, store: Bucket
) -> None:
    # Stand-in for KMS GenerateDataKey with each tenant's key: a fresh data key
    # per tenant, and the "encrypted" key as key_ref.
    data_keys = {t: os.urandom(32) for t in ("tenant-a", "tenant-b")}
    protections = {
        t: PiiProtection.aes_256_gcm(k, key_ref=f"kms-blob-{t}") for t, k in data_keys.items()
    }

    def resolve(tenant_id: str) -> TenantProfile:
        return TenantProfile.for_bucket(
            tenant_id, store.name, pii_protection=protections[tenant_id]
        )

    client = make_client(tenants=resolve)
    batch = [{"event_id": "ok", "user_email": EMAIL}, {"event_id": 1, "user_email": EMAIL}]
    reports = {t: await client.ingest(batch, tenant=t, contract=CONTRACT) for t in protections}
    assert EMAIL.encode() not in store.all_bytes()

    for tenant, report in reports.items():
        assert report.pii_protection == "aes-256-gcm"
        table = store.parquet(report.data_uri)
        assert table.column("_dl_pii_key_ref").to_pylist() == [f"kms-blob-{tenant}"]
        token = table.column("user_email").to_pylist()[0]
        aes = protections[tenant]
        assert aes.decrypt(token, tenant_id=tenant, field="user_email") == EMAIL
        dead_token = store.dead_letters(report.dlq_uris[0])[0]["payload"]["user_email"]
        assert aes.decrypt(dead_token, tenant_id=tenant, field="user_email") == EMAIL
        other = "tenant-b" if tenant == "tenant-a" else "tenant-a"
        with pytest.raises(ProtectionError):
            protections[other].decrypt(token, tenant_id=tenant, field="user_email")
        with pytest.raises(ProtectionError):
            aes.decrypt(token, tenant_id=other, field="user_email")


class CountingProtector:
    """Custom scheme: one call per field and batch, reversible for the test."""

    scheme = "test-reverse"
    key_ref = "local-key-1"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    def protect_batch(self, tenant_id: str, field: str, values: list[str]) -> list[str]:
        self.calls.append((tenant_id, field, len(values)))
        return [v[::-1] for v in values]


async def test_custom_protector_runs_once_per_field(make_client: MakeClient, store: Bucket) -> None:
    protector = CountingProtector()
    client = make_client(pii_protection=PiiProtection.custom(protector))
    records = [{"event_id": f"e{i}", "user_email": f"u{i}@x.io"} for i in range(300)]
    report = await client.ingest(records, tenant=TENANT, contract=CONTRACT)
    assert protector.calls == [(TENANT, "user_email", 300)]
    table = store.parquet(report.data_uri)
    assert table.column("user_email").to_pylist()[:2] == ["oi.x@0u", "oi.x@1u"]
    assert table.column("_dl_pii_protection").to_pylist()[0] == "test-reverse"
    assert table.column("_dl_pii_key_ref").to_pylist()[0] == "local-key-1"


async def test_failing_protector_writes_nothing(make_client: MakeClient, store: Bucket) -> None:
    class Broken:
        scheme = "test-broken"

        def protect_batch(self, tenant_id: str, field: str, values: list[str]) -> list[str]:
            raise RuntimeError(f"key service down while protecting {values}")

    client = make_client(pii_protection=PiiProtection.custom(Broken()))
    with pytest.raises(ProtectionError, match="RuntimeError") as info:
        await client.ingest(
            {"event_id": "e", "user_email": EMAIL}, tenant=TENANT, contract=CONTRACT
        )
    assert EMAIL not in str(info.value)
    assert store.keys() == []


async def test_on_invalid_raise_writes_nothing(make_client: MakeClient, store: Bucket) -> None:
    with pytest.raises(SchemaViolationError, match="record 1"):
        await make_client(on_invalid="raise").ingest(
            [{"event_id": "ok"}, {"event_id": 1}], tenant=TENANT, contract=CONTRACT
        )
    assert store.keys() == []


async def test_hooks_accept_modify_reject_and_raise(make_client: MakeClient, store: Bucket) -> None:
    hooks = HookRegistry()

    @hooks.register(CONTRACT)
    def policy(record: dict[str, Any]) -> PreIngestHookResult:
        if record["event_id"] == "blocked":
            return PreIngestHookResult.reject("err-blocked", "blocked by policy")
        if record["event_id"] == "boom":
            raise ValueError(f"cannot handle {record}")
        if record["event_id"] == "invalid-rewrite":
            return PreIngestHookResult.accept({**record, "attempts": "many"})
        return PreIngestHookResult.accept({**record, "attempts": 99})

    report = await make_client(
        hooks=hooks.as_mapping(), pii_protection=PiiProtection.sha256()
    ).ingest(
        [
            {"event_id": "fine", "user_email": EMAIL},
            {"event_id": "blocked"},
            {"event_id": "boom", "user_email": EMAIL},
            {"event_id": "invalid-rewrite"},
        ],
        tenant=TENANT,
        contract=CONTRACT,
    )
    assert report.records_ok == 1
    assert report.rejections == {
        "ERR_BLOCKED": 1,
        "ERR_HOOK_EXCEPTION": 1,
        "ERR_SCHEMA_VIOLATION": 1,
    }
    assert store.parquet(report.data_uri).column("attempts").to_pylist() == [99]
    boom_uri = next(u for u in report.dlq_uris if "ERR_HOOK_EXCEPTION" in u)
    boom = store.dead_letters(boom_uri)[0]
    assert boom["error_message"] == "hook raised ValueError"
    assert EMAIL.encode() not in store.all_bytes()


async def test_json_text_and_bytes_inputs(make_client: MakeClient) -> None:
    client = make_client()
    r1 = await client.ingest(json.dumps([{"event_id": "a"}]), tenant=TENANT, contract=CONTRACT)
    r2 = await client.ingest(b'{"event_id": "b"}', tenant=TENANT, contract=CONTRACT)
    assert (r1.records_ok, r2.records_ok) == (1, 1)
    assert r1.batch_id != r2.batch_id


async def test_pyarrow_table_and_record_batch(make_client: MakeClient, store: Bucket) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "event_id": ["a", None, "c"],
            "attempts": pa.array([1, 2, -1], type=pa.int32()),
            "user_email": [EMAIL, EMAIL, None],
        }
    )
    client = make_client(pii_protection=PiiProtection.sha256())
    report = await client.ingest(table, tenant=TENANT, contract=CONTRACT)
    assert (report.records_ok, report.records_quarantined) == (1, 2)
    batch_report = await client.ingest(table.to_batches()[0], tenant=TENANT, contract=CONTRACT)
    assert batch_report.records_received == 3
    assert EMAIL.encode() not in store.all_bytes()


async def test_polars_dataframe(make_client: MakeClient, store: Bucket) -> None:
    pl = pytest.importorskip("polars")
    df = pl.DataFrame({"event_id": [f"e{i}" for i in range(1000)], "attempts": list(range(1000))})
    report = await make_client().ingest(df, tenant=TENANT, contract=CONTRACT)
    assert report.records_ok == 1000
    assert store.parquet(report.data_uri).num_rows == 1000


async def test_profile_per_call_sets_destination_and_layout(
    make_client: MakeClient, store: Bucket, s3: Any
) -> None:
    other = f"sdk-other-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=other)
    profile = TenantProfile(
        TENANT,
        bronze=f"s3://{other}/raw/connectors",
        quarantine=f"s3://{store.name}/raw/quarantine",
        partition_order="data_type_first",
    )
    client = make_client(tenants=None)
    report = await client.ingest([{"event_id": "ok"}, {}], tenant=profile, contract=CONTRACT)
    assert report.data_uri is not None
    assert report.data_uri.startswith(
        f"s3://{other}/raw/connectors/data_type=login_event/tenant_id={TENANT}/"
    )
    assert report.dlq_uris[0].startswith(f"s3://{store.name}/raw/quarantine/tenant_id={TENANT}/")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=other)["Contents"]]
    assert len(keys) == 1 and keys[0].endswith(".parquet")
    assert all(k.startswith("raw/quarantine/") for k in store.keys())


async def test_permissive_contract_keeps_extra_fields(
    make_client: MakeClient, store: Bucket, schema: dict[str, Any]
) -> None:
    schema["x-dl-flexibility_mode"] = "permissive"
    reg = ContractRegistry.from_schemas([schema])
    report = await make_client(registry=reg).ingest(
        {"event_id": "e", "source": {"ip": "10.0.0.1"}}, tenant=TENANT, contract=CONTRACT
    )
    extra = store.parquet(report.data_uri).column("_dl_extra").to_pylist()
    assert json.loads(extra[0]) == {"source": {"ip": "10.0.0.1"}}


async def test_record_size_limit(make_client: MakeClient, store: Bucket) -> None:
    report = await make_client(max_record_bytes=100).ingest(
        [{"event_id": "x" * 200}, {"event_id": "small"}], tenant=TENANT, contract=CONTRACT
    )
    assert report.rejections == {"ERR_RECORD_TOO_LARGE": 1}
    entry = store.dead_letters(report.dlq_uris[0])[0]
    assert entry["payload"] is None and entry["payload_bytes"] > 100


async def test_missing_bucket_raises_storage_error(
    s3_endpoint: str, registry: ContractRegistry
) -> None:
    client = BronzeIngestionClient(
        registry,
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    missing = TenantProfile.for_bucket(TENANT, "bucket-that-does-not-exist")
    with pytest.raises(StorageError, match="PutObject"):
        await client.ingest({"event_id": "e"}, tenant=missing, contract=CONTRACT)


def test_asyncio_run_without_running_loop(make_client: MakeClient) -> None:
    import asyncio

    client = make_client()
    report = asyncio.run(client.ingest({"event_id": "e"}, tenant=TENANT, contract=CONTRACT))
    assert report.records_ok == 1


async def test_concurrent_ingests_share_one_client(make_client: MakeClient) -> None:
    import asyncio

    client = make_client()
    reports = await asyncio.gather(
        *(
            client.ingest([{"event_id": f"e{i}"}] * 50, tenant=TENANT, contract=CONTRACT)
            for i in range(8)
        )
    )
    assert sum(r.records_ok for r in reports) == 400
    assert len({r.batch_id for r in reports}) == 8


async def test_client_is_reusable_across_tenants(make_client: MakeClient, store: Bucket) -> None:
    client = make_client()
    for tenant in ("tenant-a", "tenant-b"):
        await client.ingest({"event_id": "e"}, tenant=tenant, contract=CONTRACT)
    keys = store.keys()
    assert any("tenant_id=tenant-a/" in k for k in keys)
    assert any("tenant_id=tenant-b/" in k for k in keys)


VOLATILE_COLUMNS = {"_dl_ingest_id", "_dl_batch_id", "_dl_received_at"}


async def test_dict_pyarrow_and_polars_inputs_store_the_same_bronze_data(
    make_client: MakeClient, store: Bucket
) -> None:
    pa = pytest.importorskip("pyarrow")
    pl = pytest.importorskip("polars")
    rows: list[dict[str, Any]] = [
        {
            "event_id": f"e{i}" if i % 7 else None,
            "attempts": -1 if i % 11 == 0 else i,
            "occurred_at": "2999-01-01T00:00:00Z" if i % 13 == 0 else "2026-10-17T10:00:00Z",
            "user_email": f"user{i}@example.com" if i % 2 else None,
        }
        for i in range(600)
    ]
    as_dicts = [{k: v for k, v in r.items() if v is not None} for r in rows]
    columns = {k: [r[k] for r in rows] for k in rows[0]}
    client = make_client(pii_protection=PiiProtection.sha256())

    def outcome(report: Any) -> tuple[Any, ...]:
        table = store.parquet(report.data_uri)
        table = table.drop_columns([c for c in table.column_names if c in VOLATILE_COLUMNS])
        letters = [
            {k: v for k, v in entry.items() if k not in {"batch_id", "quarantined_at"}}
            for uri in report.dlq_uris
            for entry in store.dead_letters(uri)
        ]
        counts = (report.records_ok, report.records_quarantined, report.rejections)
        return counts, table.to_pylist(), letters

    from_dicts = outcome(await client.ingest(as_dicts, tenant=TENANT, contract=CONTRACT))
    from_arrow = outcome(await client.ingest(pa.table(columns), tenant=TENANT, contract=CONTRACT))
    from_polars = outcome(
        await client.ingest(pl.DataFrame(columns), tenant=TENANT, contract=CONTRACT)
    )
    assert from_dicts[0][0] > 0 and from_dicts[0][1] > 0
    assert from_arrow == from_dicts
    assert from_polars == from_dicts
    assert EMAIL.encode() not in store.all_bytes()
    assert b"user1@example.com" not in store.all_bytes()


async def test_parquet_reads_as_plain_types_in_pyarrow_and_polars(
    make_client: MakeClient, store: Bucket
) -> None:
    pa = pytest.importorskip("pyarrow")
    pl = pytest.importorskip("polars")
    report = await make_client().ingest(
        [{"event_id": "e", "attempts": 2, "user_email": EMAIL}], tenant=TENANT, contract=CONTRACT
    )
    schema = store.parquet(report.data_uri).schema
    for name in ("event_id", "_dl_tenant_id", "_dl_schema_hash", "_dl_pii_key_ref"):
        assert schema.field(name).type == pa.string(), name
    assert schema.field("attempts").type == pa.int64()
    assert schema.field("_dl_received_at").type == pa.timestamp("us", tz="UTC")
    df = pl.read_parquet(io.BytesIO(store.body(report.data_uri)))
    assert df.schema["_dl_tenant_id"] == pl.String
    assert df["_dl_tenant_id"].to_list() == [TENANT]
