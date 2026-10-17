"""Client, tenant profile and protection checks that run before any I/O."""

from __future__ import annotations

from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    ContractBootError,
    ContractRegistry,
    FieldProtector,
    MalformedPayloadError,
    PiiProtection,
    PreIngestHookResult,
    ProtectionError,
    TenantProfile,
    TenantRegistry,
    TenantViolationError,
)

from .conftest import CONTRACT

# Port 9 (discard) is never an S3 endpoint: reaching storage would fail loudly.
UNREACHABLE = "http://127.0.0.1:9"
ACME = TenantProfile.for_bucket("acme-001", "acme-lake")


def client(registry: ContractRegistry, **kwargs: Any) -> BronzeIngestionClient:
    kwargs.setdefault("aws_endpoint_url", UNREACHABLE)
    kwargs.setdefault("aws_region", "us-east-1")
    kwargs.setdefault("tenants", TenantRegistry([ACME]))
    return BronzeIngestionClient(registry, **kwargs)


def test_defaults_and_repr(registry: ContractRegistry) -> None:
    c = client(registry)
    assert c.tenant_ids == ["acme-001"]
    assert c.hooked_contracts == []
    assert repr(c) == "BronzeIngestionClient(contracts=1, tenants=1)"


def test_repr_never_shows_credentials(registry: ContractRegistry) -> None:
    c = client(registry, aws_access_key_id="AKIAEXAMPLE", aws_secret_access_key="s3cr3t")
    assert "AKIAEXAMPLE" not in repr(c) and "s3cr3t" not in repr(c)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"envelope_prefix": "DL"},
        {"on_invalid": "ignore"},
        {"max_record_bytes": 0},
        {"max_future_skew_seconds": -1},
        {"aws_access_key_id": "only-key"},
        {"aws_session_token": "token-without-keys"},
    ],
)
def test_invalid_configuration(registry: ContractRegistry, kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        client(registry, **kwargs)


def test_tenants_must_be_a_registry_or_callable(registry: ContractRegistry) -> None:
    with pytest.raises(TypeError, match="tenants must be"):
        client(registry, tenants="acme-001")


def test_profile_defaults_every_layer_without_pii_protection() -> None:
    assert ACME.tenant_id == "acme-001"
    assert ACME.pii_protection == "none"
    assert ACME.partition_order == "tenant_first"
    for layer in ("bronze", "quarantine", "silver", "gold"):
        assert ACME.location(layer) == f"s3://acme-lake/{layer}"
    assert (
        ACME.table_uri("silver", "login_event")
        == "s3://acme-lake/silver/tenant_id=acme-001/data_type=login_event/"
    )
    assert "acme-lake" in repr(ACME)


def test_explicit_profile_with_dedicated_buckets() -> None:
    p = TenantProfile(
        "beta-002",
        bronze="s3://beta-raw/landing",
        quarantine="s3://beta-quarantine/dlq",
        gold="s3://beta-curated/gold",
        partition_order="data_type_first",
        pii_protection=PiiProtection.hmac_sha256(b"0123456789abcdef"),
    )
    assert p.location("silver") is None
    assert p.location("gold") == "s3://beta-curated/gold"
    assert p.pii_protection == "hmac-sha256"
    assert p.table_uri("silver", "x") is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"bronze": "s3://lake-a/data", "quarantine": "s3://lake-a/data"},
        {"bronze": "s3://lake-a/data", "quarantine": "s3://lake-a/data/dlq"},
        {"bronze": "s3://lake-a/b", "quarantine": "s3://lake-a/q", "silver": "s3://lake-a/b/s"},
        {"bronze": "lake-a/b", "quarantine": "s3://lake-a/q"},
        {"bronze": "s3://lake-a", "quarantine": "s3://lake-a/q"},
        {"bronze": "s3://Lake/b", "quarantine": "s3://lake-a/q"},
        {"bronze": "s3://lake-a/../b", "quarantine": "s3://lake-a/q"},
        {"bronze": "s3://lake-a/b", "quarantine": "s3://lake-a/q", "partition_order": "random"},
    ],
)
def test_invalid_profiles(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        TenantProfile("acme-001", **kwargs)


@pytest.mark.parametrize("bucket", ["ab", "Upper", "a..b", "-x"])
def test_invalid_bucket(bucket: str) -> None:
    with pytest.raises(ConfigurationError):
        TenantProfile.for_bucket("acme-001", bucket)


def test_invalid_tenant_id_in_profile() -> None:
    with pytest.raises(TenantViolationError):
        TenantProfile.for_bucket("Bad Tenant", "acme-lake")


def test_registry_lookup_and_duplicates() -> None:
    reg = TenantRegistry([ACME, TenantProfile.for_bucket("beta-002", "beta-lake")])
    assert len(reg) == 2 and "beta-002" in reg and "ghost-003" not in reg
    assert reg.tenant_ids() == ["acme-001", "beta-002"]
    assert reg.get("acme-001").location("bronze") == "s3://acme-lake/bronze"
    with pytest.raises(TenantViolationError):
        reg.get("ghost-003")
    with pytest.raises(ConfigurationError, match="registered twice"):
        TenantRegistry([ACME, ACME])


@pytest.mark.parametrize(
    ("factory", "scheme"),
    [
        (PiiProtection.sha256, "sha256"),
        (lambda: PiiProtection.hmac_sha256(b"k" * 16), "hmac-sha256"),
        (lambda: PiiProtection.aes_256_gcm(b"k" * 32, key_ref="blob"), "aes-256-gcm"),
    ],
)
def test_builtin_protection_schemes(factory: Any, scheme: str) -> None:
    p = factory()
    assert p.scheme == scheme
    assert repr(p) == f"PiiProtection(scheme={scheme!r})"


def test_protection_key_rules() -> None:
    with pytest.raises(ConfigurationError):
        PiiProtection.hmac_sha256(b"short")
    with pytest.raises(ConfigurationError):
        PiiProtection.aes_256_gcm(b"k" * 16, key_ref="blob")
    with pytest.raises(ConfigurationError):
        PiiProtection.aes_256_gcm(b"k" * 32, key_ref="")
    with pytest.raises(ConfigurationError, match="not reversible"):
        PiiProtection.sha256().decrypt("v1:x", tenant_id="acme-001", field="f")


def test_aes_decrypt_rejects_foreign_tokens() -> None:
    aes = PiiProtection.aes_256_gcm(b"k" * 32, key_ref="blob")
    assert aes.key_ref == "blob"
    with pytest.raises(ProtectionError):
        aes.decrypt("v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", tenant_id="acme-001", field="f")


class Upper:
    scheme = "test-upper"
    key_ref = None

    def protect_batch(self, tenant_id: str, field: str, values: list[str]) -> list[str]:
        return [v.upper() for v in values]


def test_custom_protectors_follow_the_protocol() -> None:
    assert isinstance(Upper(), FieldProtector)
    assert PiiProtection.custom(Upper()).scheme == "test-upper"

    class BadName(Upper):
        scheme = "none"

    class NotCallable:
        scheme = "x-1"
        protect_batch = "nope"

    with pytest.raises(ConfigurationError):
        PiiProtection.custom(BadName())
    with pytest.raises(TypeError, match="callable"):
        PiiProtection.custom(NotCallable())
    with pytest.raises(AttributeError):
        PiiProtection.custom(object())


def test_hook_for_unknown_contract_fails_boot(registry: ContractRegistry) -> None:
    with pytest.raises(ContractBootError, match="unknown contract"):
        client(registry, hooks={"other/contract/1": lambda r: PreIngestHookResult.accept(r)})


def test_non_callable_hook_is_rejected(registry: ContractRegistry) -> None:
    with pytest.raises(TypeError, match="not callable"):
        client(registry, hooks={CONTRACT: "not a function"})


def test_hooks_are_bound(registry: ContractRegistry) -> None:
    c = client(registry, hooks={CONTRACT: lambda r: PreIngestHookResult.accept(r)})
    assert c.hooked_contracts == [CONTRACT]


def test_contract_field_in_envelope_namespace_fails_boot(schema: dict[str, Any]) -> None:
    schema["properties"]["_dl_custom"] = {"type": "string"}
    reg = ContractRegistry.from_schemas([schema])
    with pytest.raises(ContractBootError, match="reserved envelope prefix"):
        client(reg)
    client(reg, envelope_prefix="_meta_")


@pytest.mark.parametrize("tenant", ["Bad Tenant", "ghost-003"])
async def test_invalid_or_unknown_tenant_raises_before_io(
    registry: ContractRegistry, tenant: str
) -> None:
    with pytest.raises(TenantViolationError):
        await client(registry).ingest({"event_id": "e"}, tenant=tenant, contract=CONTRACT)


async def test_tenant_id_needs_a_registry_or_resolver(registry: ContractRegistry) -> None:
    c = client(registry, tenants=None)
    with pytest.raises(ConfigurationError, match="no tenants registry"):
        await c.ingest({"event_id": "e"}, tenant="acme-001", contract=CONTRACT)
    with pytest.raises(TypeError, match="TenantProfile or a tenant id"):
        await c.ingest({"event_id": "e"}, tenant=42, contract=CONTRACT)


async def test_resolver_must_return_the_requested_tenant(registry: ContractRegistry) -> None:
    c = client(registry, tenants=lambda tenant_id: ACME)
    with pytest.raises(TenantViolationError, match="returned the profile of 'acme-001'"):
        await c.ingest({"event_id": "e"}, tenant="beta-002", contract=CONTRACT)
    bad = client(registry, tenants=lambda tenant_id: "s3://somewhere")
    with pytest.raises(TypeError, match="must return a TenantProfile"):
        await bad.ingest({"event_id": "e"}, tenant="acme-001", contract=CONTRACT)


async def test_unknown_contract_raises_before_io(registry: ContractRegistry) -> None:
    with pytest.raises(ConfigurationError):
        await client(registry).ingest({"event_id": "e"}, tenant=ACME, contract="a/b/1")


async def test_unsupported_batch_type(registry: ContractRegistry) -> None:
    with pytest.raises(TypeError, match="batch must be"):
        await client(registry).ingest(42, tenant=ACME, contract=CONTRACT)


async def test_non_serializable_batch(registry: ContractRegistry) -> None:
    with pytest.raises(MalformedPayloadError):
        await client(registry).ingest([{"event_id": float("nan")}], tenant=ACME, contract=CONTRACT)


@pytest.mark.parametrize("batch", ["not json", "42", b"\xff\xfe", "[{]"])
async def test_malformed_json_text(registry: ContractRegistry, batch: Any) -> None:
    with pytest.raises(MalformedPayloadError):
        await client(registry).ingest(batch, tenant="acme-001", contract=CONTRACT)
