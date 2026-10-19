"""Optional OpenTelemetry instrumentation of the SDK's own operations.

With ``opentelemetry-api`` installed (``pip install "datalake-medallion-sdk[otel]"``)
each ingest and read is a span, and three instruments record what the SDK
did. The SDK ships no exporter and configures nothing: the application's
OpenTelemetry SDK (or none, which makes every call a no-op) decides where
the data goes. Without the package every call here is a no-op.

Spans (kind ``INTERNAL``): ``datalake.ingest``, ``datalake.ingest_stream``,
``datalake.ingest_file``, ``datalake.ingest_otel_logs``, ``datalake.read``,
``datalake.changes``; attributes ``tenant.id``, ``datalake.contract``,
``datalake.data_type``, ``datalake.layer``, ``datalake.batch_id`` and the
record counts.

Metrics: ``datalake.records`` (counter, ``{record}``, attribute
``datalake.outcome`` = ``ok`` or ``quarantined``), ``datalake.bytes``
(counter, ``By``, bytes uploaded by streams) and
``datalake.operation.duration`` (histogram, ``s``). Metric attributes are
``datalake.operation``, ``tenant.id``, ``datalake.data_type`` and, on
failure, ``error.type``; drop ``tenant.id`` with a View if you have many
tenants.
"""

from __future__ import annotations

import functools
import inspect
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from importlib.metadata import version as _version
from typing import Any, ParamSpec, TypeVar, cast

try:
    from opentelemetry import metrics as _metrics
    from opentelemetry import trace as _trace
except ImportError:  # the [otel] extra is not installed
    _trace = None  # type: ignore[assignment]
    _metrics = None  # type: ignore[assignment]

__all__ = ["enabled", "operation", "traced"]

P = ParamSpec("P")
R = TypeVar("R")

_SCOPE = "datalake_medallion_sdk"


class _Instruments:
    def __init__(self) -> None:
        if _trace is None or _metrics is None:
            raise RuntimeError("opentelemetry-api is not installed")
        version = _version("datalake-medallion-sdk")
        # Proxies: they follow the provider the application sets, even later.
        self.tracer = _trace.get_tracer(_SCOPE, version)
        meter = _metrics.get_meter(_SCOPE, version)
        self.records = meter.create_counter(
            "datalake.records",
            unit="{record}",
            description="Records processed by the SDK, by outcome (ok or quarantined).",
        )
        self.bytes = meter.create_counter(
            "datalake.bytes", unit="By", description="Bytes uploaded by streams."
        )
        self.duration = meter.create_histogram(
            "datalake.operation.duration",
            unit="s",
            description="Duration of SDK operations.",
        )


_instruments = _Instruments() if _trace is not None else None


def _configured() -> bool:
    """Whether the application set a tracer or meter provider; until then the
    API is a no-op and the SDK skips the instrumentation entirely."""
    if _trace is None or _metrics is None:
        return False
    return not isinstance(_trace.get_tracer_provider(), _trace.ProxyTracerProvider) or (
        type(_metrics.get_meter_provider()).__name__ != "_ProxyMeterProvider"
    )


def enabled() -> bool:
    """Whether ``opentelemetry-api`` is installed (spans may still be no-ops)."""
    return _instruments is not None


Done = Callable[[Any], None]


def _noop(_: Any) -> None:
    return None


def _reports(result: Any) -> list[Any]:
    """Reports in an SDK result: a report, a dict or list of them, or
    ``TenantOutcome``s."""
    if isinstance(result, dict):
        return list(result.values())
    if isinstance(result, list):
        return [getattr(r, "report", r) for r in result]
    return [result]


def traced(
    name: str, *, layer: str | None = None, count: bool = True
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Wraps an SDK method in :func:`operation`, reading ``tenant``,
    ``contract`` and ``table`` from its arguments. Returns the method
    unchanged when OpenTelemetry is not installed. ``count=False`` records
    the span and duration only (for calls that delegate to a counted one)."""

    def wrap(fn: Callable[P, R]) -> Callable[P, R]:
        if _instruments is None:
            return fn
        # Positions of the arguments read, computed once (binding a signature
        # on every call costs microseconds).
        params = list(inspect.signature(fn).parameters)
        positions = {
            n: params.index(n)
            for n in ("tenant", "default_tenant", "contract", "table")
            if n in params
        }

        def argument(args: Any, kwargs: Any, key: str) -> Any:
            if key in kwargs:
                return kwargs[key]
            i = positions.get(key)
            return args[i] if i is not None and i < len(args) else None

        def attributes(args: Any, kwargs: Any) -> dict[str, Any]:
            table = argument(args, kwargs, "table")
            return {
                "tenant": argument(args, kwargs, "tenant")
                or argument(args, kwargs, "default_tenant"),
                "contract": argument(args, kwargs, "contract"),
                "layer": layer or getattr(table, "layer", None),
                "data_type": getattr(table, "name", None),
            }

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def run_async(*args: P.args, **kwargs: P.kwargs) -> Any:
                if not _configured():
                    return await cast("Callable[P, Awaitable[Any]]", fn)(*args, **kwargs)
                with operation(name, **attributes(args, kwargs)) as done:
                    result = await cast("Callable[P, Awaitable[Any]]", fn)(*args, **kwargs)
                    if count:
                        done(_reports(result))
                    return result

            return cast("Callable[P, R]", run_async)

        @functools.wraps(fn)
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            if not _configured():
                return fn(*args, **kwargs)
            with operation(name, **attributes(args, kwargs)) as done:
                result = fn(*args, **kwargs)
                if count:
                    done(_reports(result))
                return result

        return run

    return wrap


def _data_type(contract: str | None) -> str | None:
    parts = contract.split("/") if contract else []
    return parts[1] if len(parts) == 3 else None


@contextmanager
def operation(
    name: str,
    *,
    tenant: Any = None,
    contract: str | None = None,
    layer: str | None = None,
    data_type: str | None = None,
) -> Iterator[Done]:
    """A span and a duration measurement around one SDK call.

    Yields ``done(result)``: call it with the report(s) to record counts.
    """
    if _instruments is None:
        yield _noop
        return
    tenant_id = tenant if isinstance(tenant, str) else getattr(tenant, "tenant_id", None)
    data_type = data_type or _data_type(contract)
    attributes: dict[str, Any] = {"datalake.operation": name}
    for key, value in (
        ("tenant.id", tenant_id),
        ("datalake.data_type", data_type),
        ("datalake.layer", layer),
    ):
        if value is not None:
            attributes[key] = value
    span_attributes = dict(attributes)
    if contract:
        span_attributes["datalake.contract"] = contract
    metric_attributes = dict(attributes)
    start = time.perf_counter()
    with _instruments.tracer.start_as_current_span(
        f"datalake.{name}", attributes=span_attributes
    ) as span:

        def done(result: Any) -> None:
            reports = result if isinstance(result, list | tuple) else [result]
            for report in reports:
                if report is None:
                    continue
                ok = getattr(report, "records_ok", None)
                quarantined = getattr(report, "records_quarantined", None)
                tenant_attrs = dict(attributes)
                report_tenant = getattr(report, "tenant_id", None)
                if report_tenant:
                    tenant_attrs["tenant.id"] = report_tenant
                if ok is not None:
                    _instruments.records.add(ok, {**tenant_attrs, "datalake.outcome": "ok"})
                if quarantined:
                    _instruments.records.add(
                        quarantined, {**tenant_attrs, "datalake.outcome": "quarantined"}
                    )
                written = getattr(report, "bytes_written", None)
                if written:
                    _instruments.bytes.add(written, tenant_attrs)
                entries = getattr(report, "entries", None)
                if entries is not None:
                    span.set_attribute("datalake.entries", len(entries))
            if len(reports) == 1 and reports[0] is not None:
                report = reports[0]
                for attr, key in (
                    ("batch_id", "datalake.batch_id"),
                    ("stream_id", "datalake.batch_id"),
                    ("records_ok", "datalake.records.ok"),
                    ("records_quarantined", "datalake.records.quarantined"),
                    ("num_rows", "datalake.rows"),
                ):
                    value = getattr(report, attr, None)
                    if value is not None:
                        span.set_attribute(key, value)

        try:
            yield done
        except BaseException as error:
            metric_attributes["error.type"] = type(error).__name__
            raise
        finally:
            _instruments.duration.record(time.perf_counter() - start, metric_attributes)
