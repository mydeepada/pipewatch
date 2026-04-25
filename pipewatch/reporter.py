"""High-level reporting built on top of MetricAggregator."""

from __future__ import annotations

from typing import Dict, List, Optional

from pipewatch.aggregator import MetricAggregator
from pipewatch.pipeline import PipelineMonitor


class PipelineReport:
    """Generates a structured report from a :class:`PipelineMonitor`."""

    DEFAULT_METHODS = ("mean", "min", "max", "count")

    def __init__(
        self,
        monitor: PipelineMonitor,
        methods: Optional[tuple] = None,
    ) -> None:
        self._monitor = monitor
        self._methods = methods or self.DEFAULT_METHODS

    def _build_aggregator(self) -> MetricAggregator:
        all_metrics = list(self._monitor.collector.metrics)
        return MetricAggregator(all_metrics)

    def generate(self) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        """Return a nested dict: pipeline -> metric_name -> aggregation."""
        agg = self._build_aggregator()
        result: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}

        for metric in self._monitor.collector.metrics:
            pipeline = metric.pipeline
            name = metric.name
            if pipeline not in result:
                result[pipeline] = {}
            if name not in result[pipeline]:
                pipeline_metrics = [
                    m
                    for m in self._monitor.collector.metrics
                    if m.pipeline == pipeline and m.name == name
                ]
                pipe_agg = MetricAggregator(pipeline_metrics)
                result[pipeline][name] = {
                    method: pipe_agg.aggregate(name, method)
                    for method in self._methods
                }
        return result

    def pipelines(self) -> List[str]:
        """Return list of unique pipeline names in the monitor."""
        seen = set()
        names = []
        for m in self._monitor.collector.metrics:
            if m.pipeline not in seen:
                seen.add(m.pipeline)
                names.append(m.pipeline)
        return names

    def metric_names(self) -> List[str]:
        """Return list of unique metric names across all pipelines."""
        agg = self._build_aggregator()
        return agg.metric_names()
