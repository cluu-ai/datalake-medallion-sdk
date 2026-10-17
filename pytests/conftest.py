from __future__ import annotations

import copy
import io
import json
import socket
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ContractRegistry,
    PiiProtection,
    TenantProfile,
    TenantRegistry,
)

CONTRACT = "security/login_event/1.0.0"
EMAIL = "alice@example.com"

SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "x-dl-domain": "security",
    "x-dl-datatype": "login_event",
    "x-dl-schema_version": "1.0.0",
    "required": ["event_id"],
    "properties": {
        "event_id": {"type": "string"},
        "attempts": {"type": "integer", "minimum": 0},
        "occurred_at": {"type": "string", "format": "date-time"},
        "user_email": {"type": "string", "x-dl-pii": True},
    },
}


@pytest.fixture
def schema() -> dict[str, Any]:
    return copy.deepcopy(SCHEMA)


@pytest.fixture
def registry(schema: dict[str, Any]) -> ContractRegistry:
    return ContractRegistry.from_schemas([schema])


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="session")
def s3_endpoint() -> Iterator[str]:
    """In-process S3 emulator (moto). Skipped when moto is not installed."""
    moto_server = pytest.importorskip("moto.server")
    port = _free_port()
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.stop()


@pytest.fixture
def s3(s3_endpoint: str) -> Any:
    boto3 = pytest.importorskip("boto3")
    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def bucket(s3: Any) -> str:
    name = f"sdk-test-{uuid.uuid4().hex[:12]}"
    s3.create_bucket(Bucket=name)
    return name


@pytest.fixture
def make_client(
    s3_endpoint: str, bucket: str, registry: ContractRegistry
) -> Callable[..., BronzeIngestionClient]:
    def make(**kwargs: Any) -> BronzeIngestionClient:
        """Client whose tenants (acme-001, tenant-a, tenant-b) all use the test
        bucket; ``pii_protection=`` applies to every one of them."""
        reg = kwargs.pop("registry", registry)
        protection: PiiProtection | None = kwargs.pop("pii_protection", None)
        kwargs.setdefault(
            "tenants",
            TenantRegistry(
                [
                    TenantProfile.for_bucket(t, bucket, pii_protection=protection)
                    for t in ("acme-001", "tenant-a", "tenant-b")
                ]
            ),
        )
        return BronzeIngestionClient(
            reg,
            aws_region="us-east-1",
            aws_endpoint_url=s3_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            **kwargs,
        )

    return make


class Bucket:
    """Read helpers over the emulated bucket."""

    def __init__(self, s3: Any, name: str) -> None:
        self.s3 = s3
        self.name = name

    def keys(self) -> list[str]:
        resp = self.s3.list_objects_v2(Bucket=self.name)
        return sorted(o["Key"] for o in resp.get("Contents", []))

    def body(self, uri_or_key: str) -> bytes:
        key = uri_or_key.removeprefix(f"s3://{self.name}/")
        return bytes(self.s3.get_object(Bucket=self.name, Key=key)["Body"].read())

    def all_bytes(self) -> bytes:
        return b"".join(self.body(k) for k in self.keys())

    def parquet(self, uri: str) -> Any:
        pq = pytest.importorskip("pyarrow.parquet")
        return pq.read_table(io.BytesIO(self.body(uri)))

    def dead_letters(self, uri: str) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.body(uri).decode().splitlines()]


@pytest.fixture
def store(s3: Any, bucket: str) -> Bucket:
    return Bucket(s3, bucket)
