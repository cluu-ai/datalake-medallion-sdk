"""Envelope encryption with cached data keys per tenant (AWS KMS on moto and custom providers)."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import boto3
import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    DataKeyProvider,
    PiiDecryptor,
    PiiProtection,
    ProtectionError,
    TenantProfile,
    TenantRegistry,
)

from .conftest import CONTRACT, EMAIL, Bucket

MakeClient = Callable[..., BronzeIngestionClient]


@pytest.fixture
def kms_key(s3_endpoint: str, monkeypatch: pytest.MonkeyPatch) -> str:
    """A KMS key on the emulator; credentials for the SDK come from the environment."""
    for k, v in {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)
    kms = boto3.client(
        "kms",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    key_id = kms.create_key(Description="tenant key")["KeyMetadata"]["KeyId"]
    alias = f"alias/lake-{key_id[:8]}"
    kms.create_alias(AliasName=alias, TargetKeyId=key_id)
    return alias


def key_refs_and_tokens(store: Bucket, uri: str) -> tuple[list[str], list[str]]:
    table = store.parquet(uri)
    return table.column("_dl_pii_key_ref").to_pylist(), table.column("user_email").to_pylist()


async def test_kms_envelope_uses_one_data_key_per_tenant(
    make_client: MakeClient, store: Bucket, s3_endpoint: str, kms_key: str
) -> None:
    protection = PiiProtection.kms_envelope(kms_key, region="us-east-1", endpoint_url=s3_endpoint)
    assert protection.scheme == "aes-256-gcm" and protection.key_ref is None
    client = make_client(pii_protection=protection)
    refs: list[str] = []
    tokens: list[str] = []
    for i in range(3):
        report = await client.ingest(
            [{"event_id": f"e{i}", "user_email": EMAIL}], tenant="acme-001", contract=CONTRACT
        )
        r, t = key_refs_and_tokens(store, report.data_uri)
        refs += r
        tokens += t
    assert len(set(refs)) == 1, "three ingest calls share one data key"
    assert refs[0].startswith("kms1:arn:aws:kms:us-east-1:")
    assert EMAIL.encode() not in store.all_bytes()

    reader = PiiDecryptor.kms(region="us-east-1", endpoint_url=s3_endpoint)
    plain = reader.decrypt_many(
        [*tokens, None], tenant_id="acme-001", field="user_email", key_ref=refs[0]
    )
    assert plain == [EMAIL, EMAIL, EMAIL, None]
    with pytest.raises(ProtectionError):
        reader.decrypt(tokens[0], tenant_id="acme-001", field="phone", key_ref=refs[0])


async def test_kms_data_keys_are_bound_to_their_tenant(
    make_client: MakeClient, store: Bucket, s3_endpoint: str, kms_key: str
) -> None:
    client = make_client(
        pii_protection=PiiProtection.kms_envelope(
            kms_key, region="us-east-1", endpoint_url=s3_endpoint
        )
    )
    a = await client.ingest(
        {"event_id": "e", "user_email": EMAIL}, tenant="tenant-a", contract=CONTRACT
    )
    b = await client.ingest(
        {"event_id": "e", "user_email": EMAIL}, tenant="tenant-b", contract=CONTRACT
    )
    (ref_a,), (token_a,) = key_refs_and_tokens(store, a.data_uri)
    (ref_b,), _ = key_refs_and_tokens(store, b.data_uri)
    assert ref_a != ref_b, "each tenant has its own data key"
    reader = PiiDecryptor.kms(region="us-east-1", endpoint_url=s3_endpoint)
    assert reader.decrypt(token_a, tenant_id="tenant-a", field="user_email", key_ref=ref_a) == EMAIL
    with pytest.raises(ProtectionError):
        # The encryption context names tenant-a: KMS refuses the key for tenant-b.
        reader.decrypt(token_a, tenant_id="tenant-b", field="user_email", key_ref=ref_a)


class CountingProvider:
    """In-memory DataKeyProvider: key n has every byte equal to n + 1."""

    name = "counting"

    def __init__(self) -> None:
        self.generated = 0

    def generate(self, tenant_id: str) -> tuple[bytes, str]:
        self.generated += 1
        return bytes([self.generated]) * 32, f"count:{tenant_id}:{self.generated}"

    def decrypt(self, tenant_id: str, key_ref: str) -> bytes:
        owner, n = key_ref.removeprefix("count:").rsplit(":", 1)
        if owner != tenant_id:
            raise PermissionError("key of another tenant")
        return bytes([int(n)]) * 32


async def test_custom_provider_is_cached_and_rotates_by_nonce_budget(
    make_client: MakeClient, store: Bucket
) -> None:
    provider = CountingProvider()
    assert isinstance(provider, DataKeyProvider)
    cached = make_client(pii_protection=PiiProtection.envelope(provider))
    for i in range(4):
        await cached.ingest(
            {"event_id": f"e{i}", "user_email": EMAIL}, tenant="acme-001", contract=CONTRACT
        )
    assert provider.generated == 1

    rotating = CountingProvider()
    client = make_client(
        pii_protection=PiiProtection.envelope(rotating, max_nonce_prefixes_per_key=2)
    )
    refs = []
    for i in range(4):
        r = await client.ingest(
            {"event_id": f"e{i}", "user_email": EMAIL}, tenant="acme-001", contract=CONTRACT
        )
        refs += key_refs_and_tokens(store, r.data_uri)[0]
    assert rotating.generated == 2 and len(set(refs)) == 2

    reader = PiiDecryptor.from_provider(rotating)
    token = key_refs_and_tokens(store, r.data_uri)[1][0]
    assert (
        reader.decrypt(token, tenant_id="acme-001", field="user_email", key_ref=refs[-1]) == EMAIL
    )


async def test_failing_provider_writes_nothing(make_client: MakeClient, store: Bucket) -> None:
    class Down:
        def generate(self, tenant_id: str) -> tuple[bytes, str]:
            raise TimeoutError("key service down")

        def decrypt(self, tenant_id: str, key_ref: str) -> bytes:
            raise TimeoutError("key service down")

    client = make_client(pii_protection=PiiProtection.envelope(Down()))
    with pytest.raises(ProtectionError, match="TimeoutError"):
        await client.ingest(
            {"event_id": "e", "user_email": EMAIL}, tenant="acme-001", contract=CONTRACT
        )
    assert store.keys() == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"data_key_max_age": 5}, "between 60 seconds and 24 hours"),
        ({"data_key_max_age": 100_000}, "between 60 seconds and 24 hours"),
        ({"max_nonce_prefixes_per_key": 0}, "between 1 and 2"),
    ],
)
def test_cache_policy_is_validated(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        PiiProtection.envelope(CountingProvider(), **kwargs)


def test_registry_from_config_dict_and_file(
    tmp_path: Path, s3_endpoint: str, kms_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LAKE_HMAC_KEY", "k" * 32)
    config: dict[str, Any] = {
        "tenants": [
            {
                "tenant_id": "plain-001",
                "bronze": "s3://lake-a/bronze",
                "quarantine": "s3://lake-a/quarantine",
            },
            {
                "tenant_id": "hmac-002",
                "bronze": "s3://lake-b/b",
                "quarantine": "s3://lake-b/q",
                "pii_protection": {"scheme": "hmac-sha256", "key_env": "LAKE_HMAC_KEY"},
            },
            {
                "tenant_id": "kms-003",
                "bronze": "s3://lake-c/b",
                "quarantine": "s3://lake-c/q",
                "silver": "s3://lake-c/silver",
                "partition_order": "data_type_first",
                "pii_protection": {
                    "scheme": "aes-256-gcm",
                    "provider": "aws-kms",
                    "kms_key_id": kms_key,
                    "data_key_max_age_seconds": 900,
                    "region": "us-east-1",
                    "endpoint_url": s3_endpoint,
                },
            },
        ]
    }
    from_dict = TenantRegistry.from_config(config)
    path = tmp_path / "tenants.json"
    path.write_text(json.dumps(config))
    from_file = TenantRegistry.from_config(os.fspath(path))
    for reg in (from_dict, from_file):
        assert reg.tenant_ids() == ["hmac-002", "kms-003", "plain-001"]
        assert [reg.get(t).pii_protection for t in reg.tenant_ids()] == [
            "hmac-sha256",
            "aes-256-gcm",
            "none",
        ]
        assert reg.get("kms-003").location("silver") == "s3://lake-c/silver"


@pytest.mark.parametrize(
    "tenant",
    [
        {
            "tenant_id": "acme-001",
            "bronze": "s3://lake-a/b",
            "quarantine": "s3://lake-a/q",
            "pii_protecton": {},
        },
        {
            "tenant_id": "acme-001",
            "bronze": "s3://lake-a/b",
            "quarantine": "s3://lake-a/q",
            "pii_protection": {"scheme": "sha256", "salt": "x"},
        },
        {
            "tenant_id": "acme-001",
            "bronze": "s3://lake-a/b",
            "quarantine": "s3://lake-a/q",
            "pii_protection": {"scheme": "hmac-sha256", "key_env": "LAKE_KEY_THAT_IS_NOT_SET"},
        },
    ],
)
def test_invalid_configs_are_rejected(tenant: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        TenantRegistry.from_config({"tenants": [tenant]})


def test_profile_without_protection_never_needs_a_provider() -> None:
    profile = TenantProfile.for_bucket("acme-001", "acme-lake")
    assert profile.pii_protection == "none"


async def test_decrypt_column_takes_and_returns_arrow(
    make_client: MakeClient, store: Bucket, s3_endpoint: str, kms_key: str
) -> None:
    pa = pytest.importorskip("pyarrow")
    pl = pytest.importorskip("polars")
    client = make_client(
        pii_protection=PiiProtection.kms_envelope(
            kms_key, region="us-east-1", endpoint_url=s3_endpoint, max_nonce_prefixes_per_key=1
        )
    )
    tables = []
    for i in range(2):  # one nonce prefix per key: the second call uses a new data key
        rows = [{"event_id": f"e{i}{j}", "user_email": f"u{i}{j}@x.io"} for j in range(3)]
        rows.append({"event_id": f"e{i}n"})
        report = await client.ingest(rows, tenant="acme-001", contract=CONTRACT)
        tables.append(store.parquet(report.data_uri))
    table = pa.concat_tables(tables)
    assert len(set(table.column("_dl_pii_key_ref").to_pylist())) == 2
    reader = PiiDecryptor.kms(region="us-east-1", endpoint_url=s3_endpoint)
    expected = ["u00@x.io", "u01@x.io", "u02@x.io", None, "u10@x.io", "u11@x.io", "u12@x.io", None]
    clear = reader.decrypt_column(
        table.column("user_email"),
        tenant_id="acme-001",
        field="user_email",
        key_refs=table.column("_dl_pii_key_ref"),
    )
    assert isinstance(clear, pa.Array) and clear.to_pylist() == expected
    frame = pl.from_arrow(table)
    from_polars = reader.decrypt_column(
        frame["user_email"],
        tenant_id="acme-001",
        field="user_email",
        key_refs=frame["_dl_pii_key_ref"],
    )
    assert from_polars.to_pylist() == expected
    with pytest.raises(ProtectionError):
        reader.decrypt_column(
            table.column("user_email"),
            tenant_id="acme-001",
            field="user_email",
            key_refs=tables[0].column("_dl_pii_key_ref")[0].as_py(),
        )
