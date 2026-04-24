"""High-level pipeline monitor that ties metrics, alerts, and notifications."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertRule
from pipewatch.metrics import MetricsCollector, PipelineMetric
from pipewatch.notifier import NotificationDispatcher, StdoutNotifier


@dataclass
class PipelineMonitor:
    """Orchestrates metric collection, alert evaluation, and notifications."""

    name: str
    rules: List[AlertRule] = field(default_factory=list)
    dispatcher: NotificationDispatcher = field(
        default_factory=lambda: NotificationDispatcher(channels=[StdoutNotifier()])
    )
    _collector: MetricsCollector = field(
        default_factory=MetricsCollector, repr=False
    )

    def add_rule(self, rule: AlertRule) -> None:
        """Register an alert rule for this pipeline."""
        self.rules.append(rule)

    def record(self, metric: PipelineMetric) -> List[Alert]:
        """Record a metric, evaluate all rules, dispatch any alerts.

        Returns the list of alerts that were triggered.
        """
        self._collector.add(metric)
        triggered: List[Alert] = []
        for rule in self.rules:
            alert: Optional[Alert] = rule.evaluate(metric)
            if alert is not None:
                alert.pipeline_name = self.name
                triggered.append(alert)
        if triggered:
            self.dispatcher.dispatch_all(triggered)
        return triggered

    def record_many(self, metrics: List[PipelineMetric]) -> List[Alert]:
        """Record multiple metrics and return all triggered alerts."""
        all_alerts: List[Alert] = []
        for metric in metrics:
            all_alerts.extend(self.record(metric))
        return all_alerts

    def summary(self) -> dict:
        """Return a summary dict of the current collector state."""
        return {
            "pipeline": self.name,
            "metrics_count": len(self._collector.metrics),
            "rules_count": len(self.rules),
            "latest": (
                self._collector.metrics[-1].to_dict()
                if self._collector.metrics
                else None
            ),
        }
