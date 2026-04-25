"""Pipeline monitor — core orchestration layer for pipewatch."""
from __future__ import annotations

from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertRule
from pipewatch.metrics import MetricsCollector, PipelineMetric
from pipewatch.sampler import MetricSampler


class PipelineMonitor:
    """Collects metrics, evaluates alert rules, and optionally samples data.

    Args:
        name:    Human-readable name for this monitor.
        sampler: Optional :class:`~pipewatch.sampler.MetricSampler` applied
                 when :meth:`record_many` is called.
    """

    def __init__(
        self,
        name: str = "default",
        sampler: Optional[MetricSampler] = None,
    ) -> None:
        self.name = name
        self._collector: MetricsCollector = MetricsCollector()
        self._rules: List[AlertRule] = []
        self._alerts: List[Alert] = []
        self._sampler: Optional[MetricSampler] = sampler

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------

    def add_rule(self, rule: AlertRule) -> None:
        """Register an :class:`~pipewatch.alerts.AlertRule`."""
        self._rules.append(rule)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(self, metric: PipelineMetric) -> List[Alert]:
        """Record a single metric and return any triggered alerts."""
        self._collector.add(metric)
        triggered = [
            alert
            for rule in self._rules
            for alert in [rule.evaluate(metric)]
            if alert is not None
        ]
        self._alerts.extend(triggered)
        return triggered

    def record_many(self, metrics: List[PipelineMetric]) -> List[Alert]:
        """Record multiple metrics, applying the sampler when configured."""
        source = (
            self._sampler.sample(metrics)
            if self._sampler is not None
            else metrics
        )
        all_alerts: List[Alert] = []
        for metric in source:
            all_alerts.extend(self.record(metric))
        return all_alerts

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def metrics(self) -> List[PipelineMetric]:
        """All recorded metrics."""
        return self._collector.all()

    @property
    def alerts(self) -> List[Alert]:
        """All alerts triggered so far."""
        return list(self._alerts)

    def metrics_for(self, pipeline_name: str) -> List[PipelineMetric]:
        """Return metrics belonging to *pipeline_name*."""
        return [
            m for m in self._collector.all()
            if m.pipeline_name == pipeline_name
        ]

    def pipeline_names(self) -> List[str]:
        """Unique pipeline names seen so far."""
        return list({m.pipeline_name for m in self._collector.all()})
