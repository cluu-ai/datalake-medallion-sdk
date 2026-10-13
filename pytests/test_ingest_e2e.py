"""End-to-end ingestion against an S3 emulator (moto): real uploads, real reads."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    HookRegistry,
    PreIngestHookResult,
    SchemaViolationError,
    StorageError,
)

from .conftest import CONTRACT, EMAIL, Bucket

TENANT = "acme-001"
MakeClient = Callable[..., BronzeIngestionClient]


async def test_valid_records_written_as_partitioned_parquet(
    make_client: MakeClient, store: Bucket
) -> None:
    report = await make_client().ingest(
        [{"event_id": "e1", "attempts": 2}, {"event_id": "e2"}],
        tenant_id=TENANT,
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
        tenant_id=TENANT,
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
        assert uri.startswith(f"s3://{store.name}/dlq/tenant_id={TENANT}/data_type=login_event/")
        assert uri.endswith(f"/{report.batch_id}.jsonl")

    schema_uri = next(u for u in report.dlq_uris if "error_code=ERR_SCHEMA_VIOLATION" in u)
    entries = store.dead_letters(schema_uri)
    assert [e["record_index"] for e in entries] == [1, 2]
    assert all(e["validation_level"] == 3 and e["batch_id"] == report.batch_id for e in entries)
    assert entries[1]["payload"] == {"event_id": "x", "attempts": -3}
    assert "-3" not in entries[1]["error_message"]


async def test_pii_is_hashed_everywhere(make_client: MakeClient, store: Bucket) -> None:
    import hashlib

    report = await make_client().ingest(
        [
            {"event_id": "ok", "user_email": EMAIL},
            {"event_id": 7, "user_email": EMAIL},
        ],
        tenant_id=TENANT,
        contract=CONTRACT,
    )
    assert (report.records_ok, report.records_quarantined) == (1, 1)
    assert EMAIL.encode() not in store.all_bytes()

    table = store.parquet(report.data_uri)
    assert table.column("user_email").to_pylist() == [hashlib.sha256(EMAIL.encode()).hexdigest()]
    dead = store.dead_letters(report.dlq_uris[0])
    assert dead[0]["payload"]["user_email"] == hashlib.sha256(EMAIL.encode()).hexdigest()


async def test_masking_disabled_on_client_keeps_values(
    make_client: MakeClient, store: Bucket
) -> None:
    report = await make_client(pii_masking=False).ingest(
        [
            {"event_id": "ok", "user_email": EMAIL},
            {"event_id": 7, "user_email": EMAIL},
        ],
        tenant_id=TENANT,
        contract=CONTRACT,
    )
    assert report.pii_masking == "none"
    assert (report.records_ok, report.records_quarantined) == (1, 1)
    table = store.parquet(report.data_uri)
    assert table.column("user_email").to_pylist() == [EMAIL]
    assert table.column("_dl_pii_masking").to_pylist() == ["none"]
    dead = store.dead_letters(report.dlq_uris[0])
    assert dead[0]["payload"]["user_email"] == EMAIL
    assert EMAIL not in dead[0]["error_message"]


async def test_per_call_override_both_directions(make_client: MakeClient, store: Bucket) -> None:
    import hashlib

    record = {"event_id": "e", "user_email": EMAIL}
    masked_client = make_client()
    plain = await masked_client.ingest(
        record, tenant_id=TENANT, contract=CONTRACT, pii_masking=False
    )
    default = await masked_client.ingest(record, tenant_id=TENANT, contract=CONTRACT)
    assert (plain.pii_masking, default.pii_masking) == ("none", "sha256")
    assert store.parquet(plain.data_uri).column("user_email").to_pylist() == [EMAIL]
    assert store.parquet(default.data_uri).column("user_email").to_pylist() == [
        hashlib.sha256(EMAIL.encode()).hexdigest()
    ]

    plain_client = make_client(pii_masking=False)
    forced = await plain_client.ingest(
        record, tenant_id=TENANT, contract=CONTRACT, pii_masking=True
    )
    assert forced.pii_masking == "sha256"
    assert store.parquet(forced.data_uri).column("_dl_pii_masking").to_pylist() == ["sha256"]
    assert "pii=none" in repr(plain_client)


async def test_hmac_pseudonymization(make_client: MakeClient, store: Bucket) -> None:
    import hashlib
    import hmac

    key = b"k" * 32
    report = await make_client(pii_hmac_key=key).ingest(
        {"event_id": "e", "user_email": EMAIL}, tenant_id=TENANT, contract=CONTRACT
    )
    expected = hmac.new(key, EMAIL.encode(), hashlib.sha256).hexdigest()
    assert store.parquet(report.data_uri).column("user_email").to_pylist() == [expected]


async def test_on_invalid_raise_writes_nothing(make_client: MakeClient, store: Bucket) -> None:
    with pytest.raises(SchemaViolationError, match="record 1"):
        await make_client(on_invalid="raise").ingest(
            [{"event_id": "ok"}, {"event_id": 1}], tenant_id=TENANT, contract=CONTRACT
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

    report = await make_client(hooks=hooks.as_mapping()).ingest(
        [
            {"event_id": "fine", "user_email": EMAIL},
            {"event_id": "blocked"},
            {"event_id": "boom", "user_email": EMAIL},
            {"event_id": "invalid-rewrite"},
        ],
        tenant_id=TENANT,
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
    r1 = await client.ingest(json.dumps([{"event_id": "a"}]), tenant_id=TENANT, contract=CONTRACT)
    r2 = await client.ingest(b'{"event_id": "b"}', tenant_id=TENANT, contract=CONTRACT)
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
    client = make_client()
    report = await client.ingest(table, tenant_id=TENANT, contract=CONTRACT)
    assert (report.records_ok, report.records_quarantined) == (1, 2)
    batch_report = await client.ingest(table.to_batches()[0], tenant_id=TENANT, contract=CONTRACT)
    assert batch_report.records_received == 3
    assert EMAIL.encode() not in store.all_bytes()


async def test_polars_dataframe(make_client: MakeClient, store: Bucket) -> None:
    pl = pytest.importorskip("polars")
    df = pl.DataFrame({"event_id": [f"e{i}" for i in range(1000)], "attempts": list(range(1000))})
    report = await make_client().ingest(df, tenant_id=TENANT, contract=CONTRACT)
    assert report.records_ok == 1000
    assert store.parquet(report.data_uri).num_rows == 1000


async def test_data_type_first_layout_and_custom_prefixes(
    make_client: MakeClient, store: Bucket
) -> None:
    client = make_client(
        data_prefix="raw/connectors",
        dlq_prefix="raw/quarantine",
        partition_order="data_type_first",
    )
    report = await client.ingest([{"event_id": "ok"}, {}], tenant_id=TENANT, contract=CONTRACT)
    assert report.data_uri is not None
    key = report.data_uri.removeprefix(f"s3://{store.name}/")
    assert key.startswith(f"raw/connectors/data_type=login_event/tenant_id={TENANT}/")
    assert report.dlq_uris[0].startswith(f"s3://{store.name}/raw/quarantine/tenant_id={TENANT}/")


async def test_permissive_contract_keeps_extra_fields(
    make_client: MakeClient, store: Bucket, schema: dict[str, Any]
) -> None:
    schema["x-dl-flexibility_mode"] = "permissive"
    reg = ContractRegistry.from_schemas([schema])
    report = await make_client(registry=reg).ingest(
        {"event_id": "e", "source": {"ip": "10.0.0.1"}}, tenant_id=TENANT, contract=CONTRACT
    )
    extra = store.parquet(report.data_uri).column("_dl_extra").to_pylist()
    assert json.loads(extra[0]) == {"source": {"ip": "10.0.0.1"}}


async def test_record_size_limit(make_client: MakeClient, store: Bucket) -> None:
    report = await make_client(max_record_bytes=100).ingest(
        [{"event_id": "x" * 200}, {"event_id": "small"}], tenant_id=TENANT, contract=CONTRACT
    )
    assert report.rejections == {"ERR_RECORD_TOO_LARGE": 1}
    entry = store.dead_letters(report.dlq_uris[0])[0]
    assert entry["payload"] is None and entry["payload_bytes"] > 100


async def test_missing_bucket_raises_storage_error(
    s3_endpoint: str, registry: ContractRegistry
) -> None:
    client = BronzeIngestionClient(
        "bucket-that-does-not-exist",
        registry,
        aws_region="us-east-1",
        aws_endpoint_url=s3_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    with pytest.raises(StorageError, match="PutObject"):
        await client.ingest({"event_id": "e"}, tenant_id=TENANT, contract=CONTRACT)


def test_asyncio_run_without_running_loop(make_client: MakeClient) -> None:
    import asyncio

    client = make_client()
    report = asyncio.run(client.ingest({"event_id": "e"}, tenant_id=TENANT, contract=CONTRACT))
    assert report.records_ok == 1


async def test_concurrent_ingests_share_one_client(make_client: MakeClient) -> None:
    import asyncio

    client = make_client()
    reports = await asyncio.gather(
        *(
            client.ingest([{"event_id": f"e{i}"}] * 50, tenant_id=TENANT, contract=CONTRACT)
            for i in range(8)
        )
    )
    assert sum(r.records_ok for r in reports) == 400
    assert len({r.batch_id for r in reports}) == 8


async def test_client_is_reusable_across_tenants(make_client: MakeClient, store: Bucket) -> None:
    client = make_client()
    for tenant in ("tenant-a", "tenant-b"):
        await client.ingest({"event_id": "e"}, tenant_id=tenant, contract=CONTRACT)
    keys = store.keys()
    assert any("tenant_id=tenant-a/" in k for k in keys)
    assert any("tenant_id=tenant-b/" in k for k in keys)
