"""Client construction and checks that run before any I/O."""

from __future__ import annotations

from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    ContractBootError,
    ContractRegistry,
    MalformedPayloadError,
    PreIngestHookResult,
    TenantViolationError,
)

from .conftest import CONTRACT

# Port 9 (discard) is never an S3 endpoint: reaching storage would fail loudly.
UNREACHABLE = "http://127.0.0.1:9"


def client(registry: ContractRegistry, **kwargs: Any) -> BronzeIngestionClient:
    kwargs.setdefault("aws_endpoint_url", UNREACHABLE)
    kwargs.setdefault("aws_region", "us-east-1")
    return BronzeIngestionClient("acme-lake", registry, **kwargs)


def test_defaults_and_repr(registry: ContractRegistry) -> None:
    c = client(registry)
    assert c.bucket == "acme-lake"
    assert c.hooked_contracts == []
    shown = repr(c)
    assert "acme-lake" in shown and "sha256" in shown


def test_repr_never_shows_credentials(registry: ContractRegistry) -> None:
    c = client(registry, aws_access_key_id="AKIAEXAMPLE", aws_secret_access_key="s3cr3t")
    assert "AKIAEXAMPLE" not in repr(c) and "s3cr3t" not in repr(c)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"data_prefix": "/bronze"},
        {"dlq_prefix": "bronze"},
        {"partition_order": "random"},
        {"envelope_prefix": "DL"},
        {"on_invalid": "ignore"},
        {"max_record_bytes": 0},
        {"max_future_skew_seconds": -1},
        {"pii_hmac_key": b"short"},
        {"aws_access_key_id": "only-key"},
        {"aws_session_token": "token-without-keys"},
    ],
)
def test_invalid_configuration(registry: ContractRegistry, kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        client(registry, **kwargs)


@pytest.mark.parametrize("bucket", ["ab", "Upper", "a..b", "-x"])
def test_invalid_bucket(registry: ContractRegistry, bucket: str) -> None:
    with pytest.raises(ConfigurationError):
        BronzeIngestionClient(bucket, registry)


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


async def test_invalid_tenant_raises_before_io(registry: ContractRegistry) -> None:
    with pytest.raises(TenantViolationError):
        await client(registry).ingest({"event_id": "e"}, tenant_id="Bad Tenant", contract=CONTRACT)


async def test_unknown_contract_raises_before_io(registry: ContractRegistry) -> None:
    with pytest.raises(ConfigurationError):
        await client(registry).ingest({"event_id": "e"}, tenant_id="acme-001", contract="a/b/1")


async def test_unsupported_batch_type(registry: ContractRegistry) -> None:
    with pytest.raises(TypeError, match="batch must be"):
        await client(registry).ingest(42, tenant_id="acme-001", contract=CONTRACT)


async def test_non_serializable_batch(registry: ContractRegistry) -> None:
    with pytest.raises(MalformedPayloadError):
        await client(registry).ingest(
            [{"event_id": float("nan")}], tenant_id="acme-001", contract=CONTRACT
        )


@pytest.mark.parametrize("batch", ["not json", "42", b"\xff\xfe", "[{]"])
async def test_malformed_json_text(registry: ContractRegistry, batch: Any) -> None:
    with pytest.raises(MalformedPayloadError):
        await client(registry).ingest(batch, tenant_id="acme-001", contract=CONTRACT)
