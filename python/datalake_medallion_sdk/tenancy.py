"""Typing helpers for tenant resolution and custom PII protection."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from ._core import TenantProfile

__all__ = ["FieldProtector", "TenantResolver"]

TenantResolver = Callable[[str], TenantProfile]
"""Host function returning the :class:`TenantProfile` of a tenant id.

Use it to back tenant profiles with your own configuration store. It is
called once per ``ingest`` and must return the profile of that same tenant.
"""


@runtime_checkable
class FieldProtector(Protocol):
    """A custom PII protection scheme, wrapped with ``PiiProtection.custom``.

    ``protect_batch`` receives every non-null value of one PII field for one
    call (strings as-is, other values as compact JSON) and must return one
    string per value, in order. It runs once per field and batch, so it can
    reach a key service or encrypt in bulk. Raise to fail the call: nothing
    is written, and only the exception type is reported.
    """

    @property
    def scheme(self) -> str:
        """Name recorded in ``_dl_pii_protection``: 1-32 chars of ``[a-z0-9-]``, not ``none``."""
        ...

    def protect_batch(self, tenant_id: str, field: str, values: list[str]) -> list[str]:
        """Protect ``values`` of ``field`` for ``tenant_id``."""
        ...
