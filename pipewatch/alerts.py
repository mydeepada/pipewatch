"""Alert definitions and notification logic for pipewatch.

This module provides threshold-based alerting for pipeline metrics,
allowing users to define rules and receive notifications when metrics
exceed acceptable bounds.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, List, Optional

from pipewatch.metrics import PipelineMetric, is_stale


class AlertSeverity(Enum):
    """Severity levels for pipeline alerts."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """Defines a condition that triggers an alert when a metric breaches a threshold.

    Attributes:
        name: Human-readable name for this alert rule.
        metric_name: The pipeline metric name this rule applies to.
        threshold: The numeric value that triggers the alert when breached.
        comparator: A callable that takes (metric_value, threshold) and returns True if alert should fire.
        severity: How severe this alert is considered.
        message_template: Optional custom message; supports {name}, {value}, {threshold} placeholders.
    """
    name: str
    metric_name: str
    threshold: float
    comparator: Callable[[float, float], bool]
    severity: AlertSeverity = AlertSeverity.WARNING
    message_template: str = "Alert '{name}': value {value} breached threshold {threshold}"

    def evaluate(self, metric: PipelineMetric) -> Optional["Alert"]:
        """Evaluate this rule against a metric, returning an Alert if the condition is met."""
        if metric.name != self.metric_name:
            return None
        if self.comparator(metric.value, self.threshold):
            message = self.message_template.format(
                name=self.name,
                value=metric.value,
                threshold=self.threshold,
            )
            return Alert(
                rule_name=self.name,
                metric=metric,
                severity=self.severity,
                message=message,
            )
        return None


@dataclass
class Alert:
    """Represents a fired alert for a pipeline metric breach.

    Attributes:
        rule_name: The name of the rule that triggered this alert.
        metric: The metric that caused the alert.
        severity: Severity level of the alert.
        message: Human-readable description of the alert condition.
        fired_at: Timestamp when the alert was generated.
    """
    rule_name: str
    metric: PipelineMetric
    severity: AlertSeverity
    message: str
    fired_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        """Serialize the alert to a plain dictionary for output or logging."""
        return {
            "rule_name": self.rule_name,
            "severity": self.severity.value,
            "message": self.message,
            "fired_at": self.fired_at.isoformat(),
            "metric": {
                "name": self.metric.name,
                "value": self.metric.value,
                "pipeline": self.metric.pipeline,
                "timestamp": self.metric.timestamp.isoformat(),
            },
        }


class AlertEngine:
    """Evaluates a set of alert rules against incoming metrics and collects fired alerts."""

    def __init__(self, stale_threshold_seconds: float = 300.0):
        """Initialize the engine with an optional staleness threshold.

        Args:
            stale_threshold_seconds: Metrics older than this many seconds are considered stale
                                     and will trigger an automatic INFO-level alert.
        """
        self.rules: List[AlertRule] = []
        self.stale_threshold_seconds = stale_threshold_seconds

    def add_rule(self, rule: AlertRule) -> None:
        """Register an alert rule with the engine."""
        self.rules.append(rule)

    def evaluate(self, metric: PipelineMetric) -> List[Alert]:
        """Run all registered rules against a metric and return any fired alerts.

        Also checks for metric staleness and appends a staleness alert if needed.

        Args:
            metric: The pipeline metric to evaluate.

        Returns:
            A list of Alert objects for every rule that fired.
        """
        fired: List[Alert] = []

        # Check staleness first
        if is_stale(metric, self.stale_threshold_seconds):
            fired.append(Alert(
                rule_name="__stale_metric__",
                metric=metric,
                severity=AlertSeverity.WARNING,
                message=(
                    f"Metric '{metric.name}' on pipeline '{metric.pipeline}' "
                    f"has not been updated in over {self.stale_threshold_seconds}s."
                ),
            ))

        for rule in self.rules:
            alert = rule.evaluate(metric)
            if alert is not None:
                fired.append(alert)

        return fired
