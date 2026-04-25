"""Metric history tracking for pipewatch pipelines."""

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from pipewatch.metrics import PipelineMetric


class HistoryError(Exception):
    """Raised when history operations fail."""


@dataclass
class MetricHistory:
    """Stores a bounded history of PipelineMetric entries per pipeline."""

    max_size: int = 100
    _store: Dict[str, Deque[PipelineMetric]] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_size < 1:
            raise HistoryError(f"max_size must be at least 1, got {self.max_size}")

    def push(self, metric: PipelineMetric) -> None:
        """Append a metric to the history for its pipeline."""
        name = metric.pipeline_name
        if name not in self._store:
            self._store[name] = deque(maxlen=self.max_size)
        self._store[name].append(metric)

    def push_many(self, metrics: List[PipelineMetric]) -> None:
        """Append multiple metrics at once."""
        for metric in metrics:
            self.push(metric)

    def get(self, pipeline_name: str) -> List[PipelineMetric]:
        """Return a list of stored metrics for the given pipeline."""
        return list(self._store.get(pipeline_name, []))

    def latest(self, pipeline_name: str) -> Optional[PipelineMetric]:
        """Return the most recently added metric for a pipeline, or None."""
        entries = self._store.get(pipeline_name)
        if not entries:
            return None
        return entries[-1]

    def pipelines(self) -> List[str]:
        """Return sorted list of pipeline names that have history."""
        return sorted(self._store.keys())

    def clear(self, pipeline_name: Optional[str] = None) -> None:
        """Clear history for one pipeline or all pipelines."""
        if pipeline_name is not None:
            self._store.pop(pipeline_name, None)
        else:
            self._store.clear()

    def size(self, pipeline_name: str) -> int:
        """Return the number of stored entries for a pipeline."""
        return len(self._store.get(pipeline_name, []))
