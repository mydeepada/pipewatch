"""Baseline deviation detection for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean, stdev
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


class BaselineError(Exception):
    """Raised when baseline operations fail."""


@dataclass
class Baseline:
    """Tracks the rolling mean and standard deviation for a named metric."""

    pipeline: str
    metric_name: str
    min_samples: int = 5
    _samples: List[float] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.min_samples < 2:
            raise BaselineError("min_samples must be at least 2")

    def add(self, value: float) -> None:
        """Record a new sample value."""
        self._samples.append(value)

    @property
    def is_ready(self) -> bool:
        """Return True once enough samples have been collected."""
        return len(self._samples) >= self.min_samples

    @property
    def mean(self) -> float:
        if not self._samples:
            raise BaselineError("No samples available")
        return mean(self._samples)

    @property
    def std(self) -> float:
        if len(self._samples) < 2:
            raise BaselineError("Need at least 2 samples for std")
        return stdev(self._samples)

    def z_score(self, value: float) -> Optional[float]:
        """Return the z-score of *value* relative to the current baseline."""
        if not self.is_ready:
            return None
        s = self.std
        if s == 0.0:
            return 0.0
        return (value - self.mean) / s

    def is_anomaly(self, value: float, threshold: float = 2.0) -> bool:
        """Return True if *value* deviates more than *threshold* std devs."""
        z = self.z_score(value)
        if z is None:
            return False
        return abs(z) > threshold


class BaselineTracker:
    """Maintains one :class:`Baseline` per (pipeline, metric_name) pair."""

    def __init__(self, min_samples: int = 5) -> None:
        if min_samples < 2:
            raise BaselineError("min_samples must be at least 2")
        self._min_samples = min_samples
        self._baselines: Dict[tuple, Baseline] = {}

    def _key(self, pipeline: str, metric_name: str) -> tuple:
        return (pipeline, metric_name)

    def record(self, metric: PipelineMetric) -> None:
        """Feed a metric reading into its baseline."""
        key = self._key(metric.pipeline_name, metric.name)
        if key not in self._baselines:
            self._baselines[key] = Baseline(
                pipeline=metric.pipeline_name,
                metric_name=metric.name,
                min_samples=self._min_samples,
            )
        self._baselines[key].add(metric.value)

    def get(self, pipeline: str, metric_name: str) -> Optional[Baseline]:
        return self._baselines.get(self._key(pipeline, metric_name))

    def is_anomaly(
        self, metric: PipelineMetric, threshold: float = 2.0
    ) -> bool:
        baseline = self.get(metric.pipeline_name, metric.name)
        if baseline is None:
            return False
        return baseline.is_anomaly(metric.value, threshold)
