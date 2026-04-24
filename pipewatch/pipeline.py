"""Pipeline monitor: collects metrics and evaluates alert rules."""

from __future__ import annotations

from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertRule
from pipewatch.filter import MetricFilter, apply_filter
from pipewatch.metrics import MetricsCollector, PipelineMetric


class PipelineMonitor:
    """Central object that ties together metric collection and alerting."""

    def __init__(self, pipeline_name: str) -> None:
        self.pipeline_name = pipeline_name
        self._collector: MetricsCollector = MetricsCollector()
        self._rules: List[AlertRule] = []
        self._alerts: List[Alert] = []

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------

    def add_rule(self, rule: AlertRule) -> None:
        """Register an *AlertRule* to be evaluated on each recorded metric."""
        self._rules.append(rule)

    # ------------------------------------------------------------------
    # Metric recording
    # ------------------------------------------------------------------

    def record(self, name: str, value: float, tags: Optional[Dict] = None) -> PipelineMetric:
        """Record a single metric and evaluate all registered rules."""
        metric = PipelineMetric(
            pipeline_name=self.pipeline_name,
            name=name,
            value=value,
            tags=tags or {},
        )
        self._collector.add(metric)
        self._evaluate_rules(metric)
        return metric

    def record_many(self, entries: List[Dict]) -> List[PipelineMetric]:
        """Record multiple metrics from a list of dicts with keys name/value/tags."""
        return [self.record(**entry) for entry in entries]

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def query(self, metric_filter: MetricFilter) -> List[PipelineMetric]:
        """Return metrics that satisfy *metric_filter*."""
        return apply_filter(self._collector.metrics, metric_filter)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _evaluate_rules(self, metric: PipelineMetric) -> None:
        for rule in self._rules:
            alert = rule.evaluate(metric)
            if alert is not None:
                self._alerts.append(alert)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def summary(self) -> Dict:
        """Return a high-level summary dict for the monitored pipeline."""
        metrics = self._collector.metrics
        return {
            "pipeline": self.pipeline_name,
            "total_metrics": len(metrics),
            "total_alerts": len(self._alerts),
            "alerts": [a.to_dict() for a in self._alerts],
        }
