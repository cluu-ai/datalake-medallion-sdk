"""The example contracts shipped in the repository must compile and behave as documented."""

from __future__ import annotations

from pathlib import Path

from datalake_medallion_sdk import ContractRegistry

EXAMPLES = Path(__file__).resolve().parent.parent / "examples" / "contracts"


def test_example_contracts_compile() -> None:
    registry = ContractRegistry.from_directory(EXAMPLES)
    assert "security/login_event/1.0.0" in registry
    assert registry.pii_fields("security/login_event/1.0.0") == ["source_ip", "user_email"]
