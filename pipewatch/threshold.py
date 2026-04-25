"""Threshold configuration and evaluation for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.metrics import PipelineMetric


class ThresholdError(Exception):
    """Raised when threshold configuration is invalid."""


@dataclass
class Threshold:
    """Defines warning and critical boundaries for a named metric field."""

    metric_name: str
    warning: Optional[float] = None
    critical: Optional[float] = None
    # Direction: 'above' triggers when value exceeds bound; 'below' when under.
    direction: str = "above"

    def __post_init__(self) -> None:
        if self.direction not in ("above", "below"):
            raise ThresholdError(
                f"direction must be 'above' or 'below', got '{self.direction}'"
            )
        if self.warning is None and self.critical is None:
            raise ThresholdError(
                "At least one of 'warning' or 'critical' must be set."
            )
        if (
            self.warning is not None
            and self.critical is not None
            and self.direction == "above"
            and self.warning > self.critical
        ):
            raise ThresholdError(
                "For direction='above', warning must be <= critical."
            )
        if (
            self.warning is not None
            and self.critical is not None
            and self.direction == "below"
            and self.warning < self.critical
        ):
            raise ThresholdError(
                "For direction='below', warning must be >= critical."
            )

    def evaluate(self, metric: PipelineMetric) -> Optional[str]:
        """Return 'critical', 'warning', or None based on the metric value."""
        value: float = metric.value

        def _exceeds(bound: float) -> bool:
            return value > bound if self.direction == "above" else value < bound

        if self.critical is not None and _exceeds(self.critical):
            return "critical"
        if self.warning is not None and _exceeds(self.warning):
            return "warning"
        return None


@dataclass
class ThresholdRegistry:
    """Holds multiple Threshold definitions keyed by metric_name."""

    _thresholds: Dict[str, Threshold] = field(default_factory=dict, init=False)

    def register(self, threshold: Threshold) -> None:
        """Add or replace a threshold for its metric_name."""
        self._thresholds[threshold.metric_name] = threshold

    def get(self, metric_name: str) -> Optional[Threshold]:
        """Return the Threshold for *metric_name*, or None if not registered."""
        return self._thresholds.get(metric_name)

    def evaluate_metric(self, metric: PipelineMetric) -> Optional[str]:
        """Evaluate a metric against its registered threshold, if any."""
        threshold = self.get(metric.name)
        if threshold is None:
            return None
        return threshold.evaluate(metric)

    def __len__(self) -> int:
        return len(self._thresholds)
