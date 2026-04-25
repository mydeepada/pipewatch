"""Tests for pipewatch.rate_tracker."""

import time

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.rate_tracker import RateTracker, RateTrackerError


def _make_metric(pipeline: str, name: str, value: float, timestamp: float) -> PipelineMetric:
    m = PipelineMetric(pipeline_name=pipeline, metric_name=name, value=value)
    object.__setattr__(m, "timestamp", timestamp)
    return m


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestRateTrackerInit:
    def test_valid_construction(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows", window_size=5)
        assert rt.pipeline_name == "etl"
        assert rt.metric_name == "rows"
        assert rt.window_size == 5

    def test_default_window_size(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        assert rt.window_size == 10

    def test_window_size_below_two_raises(self):
        with pytest.raises(RateTrackerError, match="window_size"):
            RateTracker(pipeline_name="etl", metric_name="rows", window_size=1)

    def test_zero_window_size_raises(self):
        with pytest.raises(RateTrackerError):
            RateTracker(pipeline_name="etl", metric_name="rows", window_size=0)


# ---------------------------------------------------------------------------
# observe()
# ---------------------------------------------------------------------------

class TestRateTrackerObserve:
    def test_observe_increments_count(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        m = _make_metric("etl", "rows", 100.0, time.time())
        rt.observe(m)
        assert rt.observation_count == 1

    def test_observe_wrong_pipeline_raises(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        m = _make_metric("other", "rows", 100.0, time.time())
        with pytest.raises(RateTrackerError, match="pipeline"):
            rt.observe(m)

    def test_observe_wrong_metric_raises(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        m = _make_metric("etl", "bytes", 100.0, time.time())
        with pytest.raises(RateTrackerError, match="metric"):
            rt.observe(m)

    def test_window_evicts_oldest(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows", window_size=3)
        for i in range(5):
            rt.observe(_make_metric("etl", "rows", float(i), float(i)))
        assert rt.observation_count == 3


# ---------------------------------------------------------------------------
# rate()
# ---------------------------------------------------------------------------

class TestRateTrackerRate:
    def test_rate_none_with_single_observation(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        rt.observe(_make_metric("etl", "rows", 0.0, 0.0))
        assert rt.rate() is None

    def test_rate_none_with_no_observations(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        assert rt.rate() is None

    def test_constant_rate(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        for i in range(5):
            rt.observe(_make_metric("etl", "rows", float(i * 10), float(i)))
        # Each step: +10 value over +1 second => rate = 10.0/s
        assert rt.rate() == pytest.approx(10.0)

    def test_negative_rate(self):
        rt = RateTracker(pipeline_name="etl", metric_name="errors")
        rt.observe(_make_metric("etl", "errors", 50.0, 0.0))
        rt.observe(_make_metric("etl", "errors", 40.0, 2.0))
        assert rt.rate() == pytest.approx(-5.0)

    def test_reset_clears_observations(self):
        rt = RateTracker(pipeline_name="etl", metric_name="rows")
        rt.observe(_make_metric("etl", "rows", 0.0, 0.0))
        rt.observe(_make_metric("etl", "rows", 10.0, 1.0))
        rt.reset()
        assert rt.observation_count == 0
        assert rt.rate() is None
