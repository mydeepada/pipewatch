"""Tests for pipewatch.threshold."""
import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.threshold import Threshold, ThresholdError, ThresholdRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metric(name: str, value: float, pipeline: str = "pipe") -> PipelineMetric:
    return PipelineMetric(name=name, value=value, pipeline=pipeline)


# ---------------------------------------------------------------------------
# Threshold construction
# ---------------------------------------------------------------------------

class TestThresholdInit:
    def test_valid_above_threshold(self):
        t = Threshold(metric_name="latency", warning=100.0, critical=200.0)
        assert t.direction == "above"

    def test_valid_below_threshold(self):
        t = Threshold(metric_name="throughput", warning=50.0, critical=20.0, direction="below")
        assert t.direction == "below"

    def test_invalid_direction_raises(self):
        with pytest.raises(ThresholdError, match="direction"):
            Threshold(metric_name="x", warning=1.0, direction="sideways")

    def test_no_bounds_raises(self):
        with pytest.raises(ThresholdError, match="At least one"):
            Threshold(metric_name="x")

    def test_above_warning_greater_than_critical_raises(self):
        with pytest.raises(ThresholdError, match="warning must be <= critical"):
            Threshold(metric_name="x", warning=300.0, critical=100.0, direction="above")

    def test_below_warning_less_than_critical_raises(self):
        with pytest.raises(ThresholdError, match="warning must be >= critical"):
            Threshold(metric_name="x", warning=10.0, critical=50.0, direction="below")

    def test_only_critical_is_valid(self):
        t = Threshold(metric_name="errors", critical=500.0)
        assert t.warning is None


# ---------------------------------------------------------------------------
# Threshold.evaluate
# ---------------------------------------------------------------------------

class TestThresholdEvaluate:
    def test_below_warning_returns_none(self):
        t = Threshold(metric_name="latency", warning=100.0, critical=200.0)
        assert t.evaluate(_metric("latency", 50.0)) is None

    def test_above_warning_returns_warning(self):
        t = Threshold(metric_name="latency", warning=100.0, critical=200.0)
        assert t.evaluate(_metric("latency", 150.0)) == "warning"

    def test_above_critical_returns_critical(self):
        t = Threshold(metric_name="latency", warning=100.0, critical=200.0)
        assert t.evaluate(_metric("latency", 250.0)) == "critical"

    def test_direction_below_ok(self):
        t = Threshold(metric_name="throughput", warning=50.0, critical=20.0, direction="below")
        assert t.evaluate(_metric("throughput", 80.0)) is None

    def test_direction_below_warning(self):
        t = Threshold(metric_name="throughput", warning=50.0, critical=20.0, direction="below")
        assert t.evaluate(_metric("throughput", 30.0)) == "warning"

    def test_direction_below_critical(self):
        t = Threshold(metric_name="throughput", warning=50.0, critical=20.0, direction="below")
        assert t.evaluate(_metric("throughput", 10.0)) == "critical"


# ---------------------------------------------------------------------------
# ThresholdRegistry
# ---------------------------------------------------------------------------

class TestThresholdRegistry:
    def test_register_and_get(self):
        reg = ThresholdRegistry()
        t = Threshold(metric_name="latency", warning=100.0, critical=200.0)
        reg.register(t)
        assert reg.get("latency") is t

    def test_get_missing_returns_none(self):
        reg = ThresholdRegistry()
        assert reg.get("nonexistent") is None

    def test_len(self):
        reg = ThresholdRegistry()
        reg.register(Threshold(metric_name="a", warning=1.0))
        reg.register(Threshold(metric_name="b", critical=2.0))
        assert len(reg) == 2

    def test_evaluate_metric_no_threshold(self):
        reg = ThresholdRegistry()
        assert reg.evaluate_metric(_metric("latency", 999.0)) is None

    def test_evaluate_metric_with_threshold(self):
        reg = ThresholdRegistry()
        reg.register(Threshold(metric_name="latency", warning=100.0, critical=200.0))
        assert reg.evaluate_metric(_metric("latency", 150.0)) == "warning"
        assert reg.evaluate_metric(_metric("latency", 250.0)) == "critical"
        assert reg.evaluate_metric(_metric("latency", 50.0)) is None

    def test_register_overwrites_existing(self):
        reg = ThresholdRegistry()
        reg.register(Threshold(metric_name="latency", warning=100.0))
        reg.register(Threshold(metric_name="latency", critical=50.0))
        assert reg.get("latency").warning is None
        assert len(reg) == 1
