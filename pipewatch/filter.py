"""Filtering utilities for pipeline metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MetricFilter:
    """Composable filter for PipelineMetric objects."""

    pipeline_name: Optional[str] = None
    metric_name: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    tags: dict = field(default_factory=dict)
    custom_predicates: List[Callable[[PipelineMetric], bool]] = field(
        default_factory=list
    )

    def matches(self, metric: PipelineMetric) -> bool:
        """Return True if *metric* satisfies all filter criteria."""
        if self.pipeline_name and metric.pipeline_name != self.pipeline_name:
            return False
        if self.metric_name and metric.name != self.metric_name:
            return False
        if self.min_value is not None and metric.value < self.min_value:
            return False
        if self.max_value is not None and metric.value > self.max_value:
            return False
        for tag_key, tag_val in self.tags.items():
            if metric.tags.get(tag_key) != tag_val:
                return False
        for predicate in self.custom_predicates:
            if not predicate(metric):
                return False
        return True

    def add_predicate(self, fn: Callable[[PipelineMetric], bool]) -> "MetricFilter":
        """Attach a custom predicate and return *self* for chaining."""
        self.custom_predicates.append(fn)
        return self


def apply_filter(
    metrics: List[PipelineMetric], metric_filter: MetricFilter
) -> List[PipelineMetric]:
    """Return the subset of *metrics* that match *metric_filter*."""
    return [m for m in metrics if metric_filter.matches(m)]
