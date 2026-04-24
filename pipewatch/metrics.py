"""Core metrics collection module for pipewatch."""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineMetric:
    """Represents a single pipeline health metric snapshot."""

    name: str
    value: float
    unit: str
    timestamp: float = field(default_factory=time.time)
    tags: dict = field(default_factory=dict)

    def is_stale(self, max_age_seconds: float = 60.0) -> bool:
        """Return True if the metric is older than max_age_seconds."""
        return (time.time() - self.timestamp) > max_age_seconds

    def to_dict(self) -> dict:
        """Serialize metric to a plain dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "unit": self.unit,
            "timestamp": self.timestamp,
            "tags": self.tags,
        }


class MetricsCollector:
    """Collects and stores pipeline metrics in memory."""

    def __init__(self) -> None:
        self._metrics: dict[str, PipelineMetric] = {}

    def record(self, name: str, value: float, unit: str = "", tags: Optional[dict] = None) -> PipelineMetric:
        """Record a new metric value, overwriting any previous entry for the same name."""
        metric = PipelineMetric(
            name=name,
            value=value,
            unit=unit,
            tags=tags or {},
        )
        self._metrics[name] = metric
        return metric

    def get(self, name: str) -> Optional[PipelineMetric]:
        """Retrieve the latest metric by name."""
        return self._metrics.get(name)

    def all_metrics(self) -> list[PipelineMetric]:
        """Return all stored metrics as a list."""
        return list(self._metrics.values())

    def clear(self) -> None:
        """Remove all stored metrics."""
        self._metrics.clear()

    def summary(self) -> dict:
        """Return a summary dict of all current metric values."""
        return {name: m.to_dict() for name, m in self._metrics.items()}
