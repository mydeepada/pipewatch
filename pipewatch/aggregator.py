"""Metric aggregation utilities for pipewatch pipelines."""

from __future__ import annotations

from statistics import mean, median, stdev
from typing import Callable, Dict, List, Optional

from pipewatch.metrics import PipelineMetric


class AggregationError(Exception):
    """Raised when aggregation cannot be performed."""


AGGREGATORS: Dict[str, Callable[[List[float]], float]] = {
    "mean": mean,
    "median": median,
    "min": min,
    "max": max,
    "sum": sum,
    "count": lambda values: float(len(values)),
    "stdev": stdev,
}


class MetricAggregator:
    """Aggregates a collection of PipelineMetric values by name."""

    def __init__(self, metrics: List[PipelineMetric]) -> None:
        if not isinstance(metrics, list):
            raise TypeError("metrics must be a list")
        self._metrics = metrics

    def _values_for(self, metric_name: str) -> List[float]:
        return [
            m.value for m in self._metrics if m.name == metric_name
        ]

    def aggregate(
        self,
        metric_name: str,
        method: str = "mean",
    ) -> Optional[float]:
        """Return aggregated value for *metric_name* using *method*.

        Returns ``None`` when no matching metrics exist.
        Raises :class:`AggregationError` for an unknown method.
        """
        if method not in AGGREGATORS:
            raise AggregationError(
                f"Unknown aggregation method '{method}'. "
                f"Choose from: {sorted(AGGREGATORS)}"
            )
        values = self._values_for(metric_name)
        if not values:
            return None
        if method == "stdev" and len(values) < 2:
            raise AggregationError(
                "stdev requires at least 2 data points"
            )
        return AGGREGATORS[method](values)

    def summary(self, metric_name: str) -> Dict[str, Optional[float]]:
        """Return a dict with all supported aggregations for *metric_name*."""
        result: Dict[str, Optional[float]] = {}
        for method in AGGREGATORS:
            try:
                result[method] = self.aggregate(metric_name, method)
            except AggregationError:
                result[method] = None
        return result

    def metric_names(self) -> List[str]:
        """Return a deduplicated list of metric names present."""
        seen = set()
        names = []
        for m in self._metrics:
            if m.name not in seen:
                seen.add(m.name)
                names.append(m.name)
        return names
