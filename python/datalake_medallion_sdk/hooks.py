"""Level 4 pre-ingest hooks.

A hook receives one record (a ``dict``) that already passed its contract and
returns :class:`PreIngestHookResult`. Accepted payloads are validated against
the contract again, so a hook cannot introduce invalid data. Exceptions raised
by a hook quarantine the record with ``ERR_HOOK_EXCEPTION``; only the exception
type is recorded, never its message.

Bind hooks to a client explicitly::

    hooks = HookRegistry()

    @hooks.register("security/login_event/1.0.0")
    def drop_test_accounts(record: dict) -> PreIngestHookResult:
        if record.get("user_email", "").endswith("@test.invalid"):
            return PreIngestHookResult.reject("ERR_TEST_ACCOUNT", "test account")
        return PreIngestHookResult.accept(record)

    client = BronzeIngestionClient(bucket, registry, hooks=hooks.as_mapping())
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = [
    "Hook",
    "HookRegistry",
    "HookStatus",
    "PreIngestHookResult",
    "contract_validator",
    "default_hooks",
]


class HookStatus(Enum):
    """Decision of a hook."""

    ACCEPT = "ACCEPT"
    REJECT = "REJECT"


@dataclass(frozen=True, slots=True)
class PreIngestHookResult:
    """Result returned by a hook.

    Use :meth:`accept` or :meth:`reject` instead of the constructor.
    """

    status: HookStatus
    payload: dict[str, Any] | None = None
    error_code: str | None = None
    reason: str | None = None

    @classmethod
    def accept(cls, payload: dict[str, Any]) -> PreIngestHookResult:
        """Keep the record, using ``payload`` (possibly modified)."""
        return cls(status=HookStatus.ACCEPT, payload=payload)

    @classmethod
    def reject(cls, error_code: str, reason: str) -> PreIngestHookResult:
        """Quarantine the record.

        ``error_code`` becomes a storage partition and is normalized to
        ``[A-Z0-9_]{1,64}``. ``reason`` is stored in the dead-letter entry and
        must not contain personal data.
        """
        return cls(status=HookStatus.REJECT, error_code=error_code, reason=reason)


Hook = Callable[[dict[str, Any]], PreIngestHookResult]


class HookRegistry:
    """Maps contract keys (``domain/datatype/version``) to hooks."""

    def __init__(self) -> None:
        self._hooks: dict[str, Hook] = {}

    def register(self, contract: str) -> Callable[[Hook], Hook]:
        """Decorator binding a hook to ``contract``. One hook per contract."""

        def decorator(func: Hook) -> Hook:
            if contract in self._hooks:
                raise ValueError(f"a hook is already registered for contract {contract!r}")
            self._hooks[contract] = func
            return func

        return decorator

    def as_mapping(self) -> dict[str, Hook]:
        """Copy suitable for ``BronzeIngestionClient(hooks=...)``."""
        return dict(self._hooks)

    def clear(self) -> None:
        """Remove every hook."""
        self._hooks.clear()

    def __contains__(self, contract: object) -> bool:
        return contract in self._hooks

    def __len__(self) -> int:
        return len(self._hooks)

    def __iter__(self) -> Iterator[str]:
        return iter(self._hooks)


default_hooks = HookRegistry()
"""Process-wide registry used by :func:`contract_validator`."""


def contract_validator(
    contract: str, *, registry: HookRegistry | None = None
) -> Callable[[Hook], Hook]:
    """Register a hook in ``registry`` (default: :data:`default_hooks`)."""
    return (registry if registry is not None else default_hooks).register(contract)
