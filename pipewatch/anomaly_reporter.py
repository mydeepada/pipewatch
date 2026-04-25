"""Report anomalies discovered by :class:`BaselineTracker`."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.baseline import BaselineTracker
from pipewatch.metrics import PipelineMetric


@dataclass
class AnomalyRecord:
    """Captures a single anomaly detection event."""

    pipeline: str
    metric_name: str
    value: float
    z_score: Optional[float]
    detected_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "value": self.value,
            "z_score": round(self.z_score, 4) if self.z_score is not None else None,
            "detected_at": self.detected_at.isoformat(),
        }


class AnomalyReporter:
    """Wraps a :class:`BaselineTracker` and accumulates anomaly records."""

    def __init__(
        self,
        tracker: Optional[BaselineTracker] = None,
        z_threshold: float = 2.0,
    ) -> None:
        self._tracker = tracker or BaselineTracker()
        self._z_threshold = z_threshold
        self._records: List[AnomalyRecord] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def observe(self, metric: PipelineMetric) -> bool:
        """Feed *metric* into the tracker and record anomaly if detected.

        Returns True if the metric was flagged as an anomaly.
        """
        self._tracker.record(metric)
        baseline = self._tracker.get(metric.pipeline_name, metric.name)
        if baseline is None or not baseline.is_ready:
            return False

        z = baseline.z_score(metric.value)
        if z is not None and abs(z) > self._z_threshold:
            self._records.append(
                AnomalyRecord(
                    pipeline=metric.pipeline_name,
                    metric_name=metric.name,
                    value=metric.value,
                    z_score=z,
                )
            )
            return True
        return False

    def observe_many(self, metrics: List[PipelineMetric]) -> List[bool]:
        """Observe a batch of metrics; returns a list of anomaly flags."""
        return [self.observe(m) for m in metrics]

    @property
    def anomalies(self) -> List[AnomalyRecord]:
        """Return a copy of all recorded anomalies."""
        return list(self._records)

    def summary(self) -> Dict:
        """Return a high-level summary dict suitable for reporting."""
        by_pipeline: Dict[str, int] = {}
        for rec in self._records:
            by_pipeline[rec.pipeline] = by_pipeline.get(rec.pipeline, 0) + 1
        return {
            "total_anomalies": len(self._records),
            "by_pipeline": by_pipeline,
            "records": [r.to_dict() for r in self._records],
        }
