"""Tracks the rate of change for pipeline metrics over a sliding window."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple

from pipewatch.metrics import PipelineMetric


class RateTrackerError(ValueError):
    """Raised when RateTracker is misconfigured or misused."""


@dataclass
class RateTracker:
    """Computes per-second rate of change for a named metric field.

    Maintains a fixed-size window of (timestamp, value) observations and
    returns the average rate across consecutive pairs in that window.
    """

    pipeline_name: str
    metric_name: str
    window_size: int = 10
    _window: Deque[Tuple[float, float]] = field(default_factory=deque, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.window_size < 2:
            raise RateTrackerError(
                f"window_size must be at least 2, got {self.window_size}"
            )
        self._window: Deque[Tuple[float, float]] = deque(maxlen=self.window_size)

    def observe(self, metric: PipelineMetric) -> None:
        """Record a new observation from *metric*.

        Only metrics whose pipeline_name and metric_name match are accepted.
        """
        if metric.pipeline_name != self.pipeline_name:
            raise RateTrackerError(
                f"Expected pipeline '{self.pipeline_name}', got '{metric.pipeline_name}'"
            )
        if metric.metric_name != self.metric_name:
            raise RateTrackerError(
                f"Expected metric '{self.metric_name}', got '{metric.metric_name}'"
            )
        self._window.append((metric.timestamp, metric.value))

    def rate(self) -> Optional[float]:
        """Return the average per-second rate of change across the window.

        Returns *None* if fewer than two observations have been recorded.
        """
        if len(self._window) < 2:
            return None

        deltas: list[float] = []
        pairs = list(self._window)
        for (t0, v0), (t1, v1) in zip(pairs, pairs[1:]):
            dt = t1 - t0
            if dt <= 0:
                continue
            deltas.append((v1 - v0) / dt)

        if not deltas:
            return None
        return sum(deltas) / len(deltas)

    @property
    def observation_count(self) -> int:
        """Number of observations currently in the window."""
        return len(self._window)

    def reset(self) -> None:
        """Clear all observations."""
        self._window.clear()
