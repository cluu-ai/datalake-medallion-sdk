"""Smoke test of an installed wheel: import, build the public objects, no I/O.

Run by the release workflow against each built wheel and by CI on every pull
request, so an API change cannot reach a release tag unnoticed.
"""

from __future__ import annotations

import datalake_medallion_sdk as sdk

registry = sdk.ContractRegistry.from_schemas(
    [
        {
            "type": "object",
            "x-dl-domain": "smoke",
            "x-dl-datatype": "event",
            "x-dl-schema_version": "1",
            "properties": {"email": {"type": "string", "x-dl-pii": True}},
        }
    ]
)
protection = sdk.PiiProtection.aes_256_gcm(bytes(32), key_ref="smoke-key-ref")
tenant = sdk.TenantProfile.for_bucket("smoke-001", "smoke-bucket", pii_protection=protection)
client = sdk.BronzeIngestionClient(registry, tenants=sdk.TenantRegistry([tenant]))

checks = {
    "tenant registry": client.tenant_ids == ["smoke-001"],
    "protection scheme": tenant.pii_protection == "aes-256-gcm",
    "quarantine location": tenant.location("quarantine") == "s3://smoke-bucket/quarantine",
    "protection off by default": sdk.TenantProfile.for_bucket(
        "smoke-002", "smoke-bucket"
    ).pii_protection
    == "none",
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise SystemExit(f"smoke test failed: {', '.join(failed)}")
print("ok", sdk.__version__)
