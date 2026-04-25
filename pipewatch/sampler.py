"""Metric sampling utilities for pipewatch.

Provides reservoir and rate-limited sampling strategies
for controlling the volume of metrics processed.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


class SamplerError(ValueError):
    """Raised when a sampler is misconfigured."""


@dataclass
class MetricSampler:
    """Down-samples a stream of PipelineMetrics.

    Args:
        rate: Fraction of metrics to keep (0.0 < rate <= 1.0).
        seed:  Optional random seed for reproducibility.
    """

    rate: float
    seed: Optional[int] = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not (0.0 < self.rate <= 1.0):
            raise SamplerError(
                f"rate must be in (0.0, 1.0]; got {self.rate}"
            )
        self._rng = random.Random(self.seed)

    def sample(self, metrics: List[PipelineMetric]) -> List[PipelineMetric]:
        """Return a random subset of *metrics* according to *rate*."""
        if self.rate == 1.0:
            return list(metrics)
        return [
            m for m in metrics if self._rng.random() < self.rate
        ]

    def reservoir(self, metrics: List[PipelineMetric], k: int) -> List[PipelineMetric]:
        """Return at most *k* metrics using reservoir sampling.

        Args:
            metrics: Source list of metrics.
            k:       Maximum number of items to return.

        Returns:
            A list of at most *k* sampled metrics.
        """
        if k <= 0:
            raise SamplerError(f"k must be a positive integer; got {k}")
        return self._rng.sample(metrics, min(k, len(metrics)))
