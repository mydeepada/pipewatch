"""Produces a threshold-breach summary report from a set of metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.threshold import ThresholdRegistry


@dataclass
class BreachRecord:
    """A single threshold breach detected for a metric."""

    metric: PipelineMetric
    level: str  # 'warning' or 'critical'

    def to_dict(self) -> dict:
        return {
            "pipeline": self.metric.pipeline,
            "metric_name": self.metric.name,
            "value": self.metric.value,
            "level": self.level,
            "timestamp": self.metric.timestamp.isoformat(),
        }


@dataclass
class ThresholdReport:
    """Aggregated breach report for a collection of metrics."""

    breaches: List[BreachRecord] = field(default_factory=list)

    @property
    def warning_count(self) -> int:
        return sum(1 for b in self.breaches if b.level == "warning")

    @property
    def critical_count(self) -> int:
        return sum(1 for b in self.breaches if b.level == "critical")

    @property
    def has_critical(self) -> bool:
        return self.critical_count > 0

    def by_pipeline(self) -> Dict[str, List[BreachRecord]]:
        """Group breach records by pipeline name."""
        result: Dict[str, List[BreachRecord]] = {}
        for breach in self.breaches:
            result.setdefault(breach.metric.pipeline, []).append(breach)
        return result

    def to_dict(self) -> dict:
        return {
            "total_breaches": len(self.breaches),
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "breaches": [b.to_dict() for b in self.breaches],
        }


class ThresholdReporter:
    """Evaluates a list of metrics against a ThresholdRegistry and
    returns a ThresholdReport."""

    def __init__(self, registry: ThresholdRegistry) -> None:
        self._registry = registry

    def evaluate(self, metrics: List[PipelineMetric]) -> ThresholdReport:
        """Check every metric and collect breaches."""
        report = ThresholdReport()
        for metric in metrics:
            level: Optional[str] = self._registry.evaluate_metric(metric)
            if level is not None:
                report.breaches.append(BreachRecord(metric=metric, level=level))
        return report
