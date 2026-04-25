"""Tests for pipewatch.anomaly_reporter module."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from pipewatch.metrics import PipelineMetric
from pipewatch.baseline import Baseline
from pipewatch.anomaly_reporter import AnomalyRecord, AnomalyReporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metric(pipeline: str = "etl", value: float = 1.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        metric_name="row_count",
        value=value,
    )


def _ready_baseline(mean: float = 100.0, std: float = 10.0) -> MagicMock:
    """Return a mock Baseline that is ready and returns controlled stats."""
    bl = MagicMock(spec=Baseline)
    bl.is_ready.return_value = True
    bl.mean.return_value = mean
    bl.stddev.return_value = std
    return bl


def _unready_baseline() -> MagicMock:
    bl = MagicMock(spec=Baseline)
    bl.is_ready.return_value = False
    return bl


# ---------------------------------------------------------------------------
# AnomalyRecord
# ---------------------------------------------------------------------------

class TestAnomalyRecord:
    def test_to_dict_contains_expected_keys(self):
        metric = _make_metric(value=150.0)
        record = AnomalyRecord(
            metric=metric,
            z_score=5.0,
            baseline_mean=100.0,
            baseline_stddev=10.0,
        )
        d = record.to_dict()
        assert "pipeline_name" in d
        assert "metric_name" in d
        assert "value" in d
        assert "z_score" in d
        assert "baseline_mean" in d
        assert "baseline_stddev" in d
        assert "timestamp" in d

    def test_to_dict_values_match(self):
        metric = _make_metric(value=150.0)
        record = AnomalyRecord(
            metric=metric,
            z_score=5.0,
            baseline_mean=100.0,
            baseline_stddev=10.0,
        )
        d = record.to_dict()
        assert d["pipeline_name"] == "etl"
        assert d["value"] == 150.0
        assert d["z_score"] == pytest.approx(5.0)
        assert d["baseline_mean"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# AnomalyReporter — initialisation
# ---------------------------------------------------------------------------

class TestAnomalyReporterInit:
    def test_default_threshold(self):
        reporter = AnomalyReporter()
        assert reporter.z_threshold > 0

    def test_custom_threshold(self):
        reporter = AnomalyReporter(z_threshold=3.0)
        assert reporter.z_threshold == pytest.approx(3.0)

    def test_zero_threshold_raises(self):
        with pytest.raises(ValueError):
            AnomalyReporter(z_threshold=0.0)

    def test_negative_threshold_raises(self):
        with pytest.raises(ValueError):
            AnomalyReporter(z_threshold=-1.5)


# ---------------------------------------------------------------------------
# AnomalyReporter — observe
# ---------------------------------------------------------------------------

class TestAnomalyReporterObserve:
    def test_no_anomaly_within_threshold(self):
        reporter = AnomalyReporter(z_threshold=3.0)
        baseline = _ready_baseline(mean=100.0, std=10.0)
        metric = _make_metric(value=105.0)  # z = 0.5
        result = reporter.observe(metric, baseline)
        assert result is None

    def test_anomaly_detected_above_mean(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _ready_baseline(mean=100.0, std=10.0)
        metric = _make_metric(value=130.0)  # z = 3.0
        result = reporter.observe(metric, baseline)
        assert result is not None
        assert isinstance(result, AnomalyRecord)
        assert result.z_score == pytest.approx(3.0)

    def test_anomaly_detected_below_mean(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _ready_baseline(mean=100.0, std=10.0)
        metric = _make_metric(value=70.0)  # z = -3.0, |z| = 3.0
        result = reporter.observe(metric, baseline)
        assert result is not None
        assert abs(result.z_score) == pytest.approx(3.0)

    def test_unready_baseline_returns_none(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _unready_baseline()
        metric = _make_metric(value=999.0)
        result = reporter.observe(metric, baseline)
        assert result is None

    def test_zero_stddev_returns_none(self):
        """A perfectly flat baseline should not produce a division-by-zero error."""
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _ready_baseline(mean=100.0, std=0.0)
        metric = _make_metric(value=200.0)
        result = reporter.observe(metric, baseline)
        assert result is None


# ---------------------------------------------------------------------------
# AnomalyReporter — summary
# ---------------------------------------------------------------------------

class TestAnomalyReporterSummary:
    def test_anomalies_accumulate(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _ready_baseline(mean=100.0, std=10.0)
        for v in [130.0, 140.0, 150.0]:
            reporter.observe(_make_metric(value=v), baseline)
        assert len(reporter.anomalies) == 3

    def test_non_anomalies_not_stored(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _ready_baseline(mean=100.0, std=10.0)
        reporter.observe(_make_metric(value=101.0), baseline)
        assert len(reporter.anomalies) == 0

    def test_summary_returns_dict(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        summary = reporter.summary()
        assert isinstance(summary, dict)
        assert "total_anomalies" in summary
        assert "z_threshold" in summary

    def test_summary_count_matches_anomalies(self):
        reporter = AnomalyReporter(z_threshold=2.0)
        baseline = _ready_baseline(mean=100.0, std=10.0)
        reporter.observe(_make_metric(value=130.0), baseline)
        reporter.observe(_make_metric(value=135.0), baseline)
        summary = reporter.summary()
        assert summary["total_anomalies"] == 2
