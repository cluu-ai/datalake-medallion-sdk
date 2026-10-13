"""Decorators for async ingestion functions."""

from __future__ import annotations

import asyncio
import functools
import random
import time
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from ._core import StorageError
from .metrics import LoggingMetricsSink, MetricsSink

__all__ = ["datalake_retry", "emit_metrics"]

P = ParamSpec("P")
R = TypeVar("R")


def datalake_retry(
    attempts: int = 3,
    *,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    retry_on: tuple[type[BaseException], ...] = (StorageError,),
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Retry an async call on transient errors with exponential backoff and full jitter.

    Only :class:`StorageError` is retried by default. Contract, tenant and
    payload errors are deterministic and are raised immediately. Retrying an
    ingest is safe for Bronze data because dead letters are written before the
    data object; dead-letter entries of the failed attempt may repeat.

    Args:
        attempts: Total attempts, at least 1.
        base_delay: Backoff base in seconds.
        max_delay: Upper bound of a single wait in seconds.
        retry_on: Exception types considered transient.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if base_delay < 0 or max_delay < 0:
        raise ValueError("delays must not be negative")

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except retry_on:
                    if attempt == attempts - 1:
                        raise
                    cap = min(max_delay, base_delay * (2**attempt))
                    await asyncio.sleep(random.uniform(0, cap))
            raise AssertionError("unreachable")

        return wrapper

    return decorator


def emit_metrics(
    sink: MetricsSink | None = None,
    *,
    operation: str = "ingest",
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Emit duration, outcome and record counts of an async ingest call.

    Metrics (dimension ``operation``):

    * ``DurationMs`` (Milliseconds) on every call.
    * ``Failures`` (Count) when the call raises.
    * ``RecordsOk`` and ``RecordsQuarantined`` (Count) when the call returns an
      object with ``records_ok`` / ``records_quarantined`` such as
      :class:`IngestReport`.
    """
    target: MetricsSink = sink if sink is not None else LoggingMetricsSink()
    dims = {"operation": operation}

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
            except BaseException:
                target.emit("Failures", 1, "Count", dims)
                raise
            finally:
                elapsed = (time.perf_counter() - start) * 1000
                target.emit("DurationMs", elapsed, "Milliseconds", dims)
            for attr, name in (
                ("records_ok", "RecordsOk"),
                ("records_quarantined", "RecordsQuarantined"),
            ):
                value = getattr(result, attr, None)
                if isinstance(value, int):
                    target.emit(name, value, "Count", dims)
            return result

        return wrapper

    return decorator
