from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from datalake_medallion_sdk import ConfigurationError, ContractBootError, ContractRegistry

from .conftest import CONTRACT


def test_from_schemas_dict_and_json_string(schema: dict[str, Any]) -> None:
    other = dict(schema, **{"x-dl-schema_version": "2.0.0"})
    reg = ContractRegistry.from_schemas([schema, json.dumps(other)])
    assert reg.keys() == [CONTRACT, "security/login_event/2.0.0"]
    assert len(reg) == 2
    assert CONTRACT in reg
    assert "nope/nope/1" not in reg


def test_metadata_accessors(registry: ContractRegistry) -> None:
    assert registry.pii_fields(CONTRACT) == ["user_email"]
    assert len(registry.schema_hash(CONTRACT)) == 64
    assert registry.flexibility_mode(CONTRACT) == "strict"
    assert registry.extension_prefix == "x-dl-"
    assert "contracts=1" in repr(registry)


def test_unknown_key_raises_configuration_error(registry: ContractRegistry) -> None:
    with pytest.raises(ConfigurationError, match="unknown contract"):
        registry.pii_fields("nope/nope/1")


def test_from_directory_recursive_and_skips_underscore(
    tmp_path: Path, schema: dict[str, Any]
) -> None:
    (tmp_path / "bronze" / "security").mkdir(parents=True)
    (tmp_path / "bronze" / "security" / "login.schema.json").write_text(json.dumps(schema))
    (tmp_path / "_examples").mkdir()
    (tmp_path / "_examples" / "broken.schema.json").write_text("{not json")
    (tmp_path / "notes.json").write_text("{}")
    reg = ContractRegistry.from_directory(tmp_path)
    assert reg.keys() == [CONTRACT]


def test_from_directory_missing_path_fails(tmp_path: Path) -> None:
    with pytest.raises(ContractBootError):
        ContractRegistry.from_directory(tmp_path / "missing")


def test_boot_is_all_or_nothing(schema: dict[str, Any]) -> None:
    with pytest.raises(ContractBootError, match="2 contract"):
        ContractRegistry.from_schemas([schema, {"type": "object"}, "[]"])


def test_duplicate_contract_fails(schema: dict[str, Any]) -> None:
    with pytest.raises(ContractBootError, match="more than once"):
        ContractRegistry.from_schemas([schema, schema])


def test_custom_extension_prefix() -> None:
    reg = ContractRegistry.from_schemas(
        [
            {
                "type": "object",
                "x-acme-domain": "security",
                "x-acme-datatype": "finding",
                "x-acme-schema_version": "1.0.0",
                "properties": {"email": {"type": "string", "x-acme-pii": True}},
            }
        ],
        extension_prefix="x-acme-",
    )
    assert reg.pii_fields("security/finding/1.0.0") == ["email"]


@pytest.mark.parametrize("prefix", ["", "dl-", "x-DL-", "acme:"])
def test_invalid_extension_prefix(prefix: str) -> None:
    with pytest.raises(ConfigurationError):
        ContractRegistry.from_schemas([], extension_prefix=prefix)


def test_remote_ref_is_not_fetched(schema: dict[str, Any]) -> None:
    schema["properties"]["event_id"] = {"$ref": "https://example.invalid/s.json"}
    with pytest.raises(ContractBootError):
        ContractRegistry.from_schemas([schema])
