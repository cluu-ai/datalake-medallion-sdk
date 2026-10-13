from __future__ import annotations

from typing import Any

import pytest
from datalake_medallion_sdk import (
    HookRegistry,
    HookStatus,
    PreIngestHookResult,
    contract_validator,
    default_hooks,
)


def test_result_constructors() -> None:
    ok = PreIngestHookResult.accept({"a": 1})
    assert ok.status is HookStatus.ACCEPT and ok.payload == {"a": 1}
    bad = PreIngestHookResult.reject("ERR_X", "why")
    assert bad.status is HookStatus.REJECT
    assert (bad.error_code, bad.reason, bad.payload) == ("ERR_X", "why", None)


def test_result_is_immutable() -> None:
    result = PreIngestHookResult.accept({})
    with pytest.raises(AttributeError):
        result.status = HookStatus.REJECT  # type: ignore[misc]


def test_registry_register_and_mapping() -> None:
    hooks = HookRegistry()

    @hooks.register("d/t/1")
    def hook(record: dict[str, Any]) -> PreIngestHookResult:
        return PreIngestHookResult.accept(record)

    assert "d/t/1" in hooks and len(hooks) == 1
    assert list(hooks) == ["d/t/1"]
    mapping = hooks.as_mapping()
    assert mapping == {"d/t/1": hook}
    mapping.clear()
    assert len(hooks) == 1


def test_registry_rejects_duplicates() -> None:
    hooks = HookRegistry()
    hooks.register("d/t/1")(lambda r: PreIngestHookResult.accept(r))
    with pytest.raises(ValueError, match="already registered"):
        hooks.register("d/t/1")(lambda r: PreIngestHookResult.accept(r))


def test_contract_validator_uses_given_or_default_registry() -> None:
    local = HookRegistry()
    contract_validator("d/t/1", registry=local)(lambda r: PreIngestHookResult.accept(r))
    assert "d/t/1" in local

    default_hooks.clear()
    try:
        contract_validator("d/t/2")(lambda r: PreIngestHookResult.accept(r))
        assert "d/t/2" in default_hooks
    finally:
        default_hooks.clear()
