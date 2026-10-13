"""Metrics sinks for :func:`datalake_medallion_sdk.emit_metrics`.

The SDK does not depend on any monitoring client. Pick a sink:

* :class:`LoggingMetricsSink` (default): standard :mod:`logging`.
* :class:`EmfMetricsSink`: CloudWatch Embedded Metric Format lines on stdout,
  picked up by AWS Lambda and the CloudWatch agent without an AWS SDK.
* :class:`InMemoryMetricsSink`: keeps metrics in a list, for tests.

Or implement :class:`MetricsSink` for StatsD, OpenTelemetry, Prometheus, etc.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol, TextIO, runtime_checkable

__all__ = [
    "EmfMetricsSink",
    "InMemoryMetricsSink",
    "LoggingMetricsSink",
    "Metric",
    "MetricsSink",
]


@runtime_checkable
class MetricsSink(Protocol):
    """Receives one metric data point."""

    def emit(self, name: str, value: float, unit: str, dimensions: Mapping[str, str]) -> None:
        """Record ``value`` for ``name``."""
        ...


@dataclass(frozen=True, slots=True)
class Metric:
    """A recorded data point."""

    name: str
    value: float
    unit: str
    dimensions: dict[str, str] = field(default_factory=dict)


class LoggingMetricsSink:
    """Logs each metric at INFO on the ``datalake_medallion_sdk.metrics`` logger."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("datalake_medallion_sdk.metrics")

    def emit(self, name: str, value: float, unit: str, dimensions: Mapping[str, str]) -> None:
        self._logger.info(
            "metric %s=%s %s",
            name,
            value,
            unit,
            extra={"metric": {"name": name, "value": value, "unit": unit, **dimensions}},
        )


class EmfMetricsSink:
    """Writes CloudWatch Embedded Metric Format documents, one JSON line each."""

    def __init__(
        self, namespace: str = "DatalakeMedallionSdk", stream: TextIO | None = None
    ) -> None:
        if not namespace or len(namespace) > 255:
            raise ValueError("namespace must be 1 to 255 characters")
        self._namespace = namespace
        self._stream = stream
        self._lock = threading.Lock()

    def emit(self, name: str, value: float, unit: str, dimensions: Mapping[str, str]) -> None:
        document = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self._namespace,
                        "Dimensions": [sorted(dimensions)],
                        "Metrics": [{"Name": name, "Unit": unit}],
                    }
                ],
            },
            name: value,
            **dimensions,
        }
        line = json.dumps(document, separators=(",", ":"))
        with self._lock:
            stream = self._stream if self._stream is not None else sys.stdout
            stream.write(line + "\n")
            stream.flush()


class InMemoryMetricsSink:
    """Keeps every data point in :attr:`metrics`."""

    def __init__(self) -> None:
        self.metrics: list[Metric] = []
        self._lock = threading.Lock()

    def emit(self, name: str, value: float, unit: str, dimensions: Mapping[str, str]) -> None:
        with self._lock:
            self.metrics.append(Metric(name, value, unit, dict(dimensions)))

    def values(self, name: str) -> list[float]:
        """Values recorded for ``name``."""
        return [m.value for m in self.metrics if m.name == name]
