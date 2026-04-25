"""Tests for pipewatch.baseline."""
from __future__ import annotations

import pytest

from pipewatch.baseline import Baseline, BaselineError, BaselineTracker
from pipewatch.metrics import PipelineMetric


def _make_metric(value: float, name: str = "latency", pipeline: str = "etl") -> PipelineMetric:
    return PipelineMetric(pipeline_name=pipeline, name=name, value=value)


# ---------------------------------------------------------------------------
# Baseline unit tests
# ---------------------------------------------------------------------------

class TestBaselineInit:
    def test_valid_construction(self):
        b = Baseline(pipeline="etl", metric_name="latency", min_samples=5)
        assert b.pipeline == "etl"
        assert b.metric_name == "latency"

    def test_min_samples_below_two_raises(self):
        with pytest.raises(BaselineError):
            Baseline(pipeline="etl", metric_name="latency", min_samples=1)


class TestBaselineReadiness:
    def test_not_ready_before_min_samples(self):
        b = Baseline(pipeline="etl", metric_name="latency", min_samples=3)
        b.add(1.0)
        b.add(2.0)
        assert not b.is_ready

    def test_ready_at_min_samples(self):
        b = Baseline(pipeline="etl", metric_name="latency", min_samples=3)
        for v in (1.0, 2.0, 3.0):
            b.add(v)
        assert b.is_ready


class TestBaselineStats:
    @pytest.fixture()
    def baseline(self):
        b = Baseline(pipeline="etl", metric_name="latency", min_samples=5)
        for v in (10.0, 20.0, 30.0, 40.0, 50.0):
            b.add(v)
        return b

    def test_mean(self, baseline):
        assert baseline.mean == pytest.approx(30.0)

    def test_std_positive(self, baseline):
        assert baseline.std > 0

    def test_z_score_none_when_not_ready(self):
        b = Baseline(pipeline="etl", metric_name="latency", min_samples=5)
        b.add(10.0)
        assert b.z_score(10.0) is None

    def test_z_score_zero_for_mean(self, baseline):
        assert baseline.z_score(30.0) == pytest.approx(0.0)

    def test_is_anomaly_false_for_normal_value(self, baseline):
        assert not baseline.is_anomaly(30.0)

    def test_is_anomaly_true_for_extreme_value(self, baseline):
        assert baseline.is_anomaly(1000.0)

    def test_mean_raises_with_no_samples(self):
        b = Baseline(pipeline="etl", metric_name="latency", min_samples=2)
        with pytest.raises(BaselineError):
            _ = b.mean


# ---------------------------------------------------------------------------
# BaselineTracker tests
# ---------------------------------------------------------------------------

class TestBaselineTracker:
    def test_invalid_min_samples_raises(self):
        with pytest.raises(BaselineError):
            BaselineTracker(min_samples=1)

    def test_get_returns_none_for_unknown(self):
        tracker = BaselineTracker()
        assert tracker.get("etl", "latency") is None

    def test_record_creates_baseline(self):
        tracker = BaselineTracker(min_samples=3)
        tracker.record(_make_metric(1.0))
        assert tracker.get("etl", "latency") is not None

    def test_is_anomaly_false_before_ready(self):
        tracker = BaselineTracker(min_samples=5)
        tracker.record(_make_metric(100.0))
        assert not tracker.is_anomaly(_make_metric(100.0))

    def test_is_anomaly_detects_spike(self):
        tracker = BaselineTracker(min_samples=5)
        for v in (10.0, 10.0, 10.0, 10.0, 10.0):
            tracker.record(_make_metric(v))
        assert tracker.is_anomaly(_make_metric(9999.0))

    def test_separate_baselines_per_pipeline(self):
        tracker = BaselineTracker(min_samples=3)
        tracker.record(_make_metric(1.0, pipeline="etl"))
        tracker.record(_make_metric(1.0, pipeline="ml"))
        assert tracker.get("etl", "latency") is not tracker.get("ml", "latency")
