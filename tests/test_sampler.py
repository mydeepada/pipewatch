"""Tests for pipewatch.sampler."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.sampler import MetricSampler, SamplerError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metrics(n: int, pipeline: str = "pipe") -> list[PipelineMetric]:
    return [
        PipelineMetric(pipeline_name=pipeline, metric_name="latency", value=float(i))
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestMetricSamplerInit:
    def test_valid_rate(self):
        s = MetricSampler(rate=0.5)
        assert s.rate == 0.5

    def test_rate_of_one_is_valid(self):
        s = MetricSampler(rate=1.0)
        assert s.rate == 1.0

    def test_zero_rate_raises(self):
        with pytest.raises(SamplerError, match="rate"):
            MetricSampler(rate=0.0)

    def test_negative_rate_raises(self):
        with pytest.raises(SamplerError):
            MetricSampler(rate=-0.1)

    def test_rate_above_one_raises(self):
        with pytest.raises(SamplerError):
            MetricSampler(rate=1.1)


# ---------------------------------------------------------------------------
# sample()
# ---------------------------------------------------------------------------

class TestMetricSamplerSample:
    def test_rate_one_returns_all(self):
        metrics = _make_metrics(20)
        s = MetricSampler(rate=1.0)
        assert s.sample(metrics) == metrics

    def test_empty_input_returns_empty(self):
        s = MetricSampler(rate=0.5)
        assert s.sample([]) == []

    def test_sample_returns_subset(self):
        metrics = _make_metrics(1000)
        s = MetricSampler(rate=0.1, seed=42)
        result = s.sample(metrics)
        assert len(result) < len(metrics)

    def test_seed_makes_deterministic(self):
        metrics = _make_metrics(100)
        r1 = MetricSampler(rate=0.3, seed=7).sample(metrics)
        r2 = MetricSampler(rate=0.3, seed=7).sample(metrics)
        assert r1 == r2


# ---------------------------------------------------------------------------
# reservoir()
# ---------------------------------------------------------------------------

class TestMetricSamplerReservoir:
    def test_reservoir_returns_at_most_k(self):
        metrics = _make_metrics(50)
        s = MetricSampler(rate=1.0, seed=0)
        result = s.reservoir(metrics, k=10)
        assert len(result) == 10

    def test_reservoir_k_larger_than_input(self):
        metrics = _make_metrics(5)
        s = MetricSampler(rate=1.0)
        result = s.reservoir(metrics, k=100)
        assert len(result) == 5

    def test_reservoir_zero_k_raises(self):
        s = MetricSampler(rate=1.0)
        with pytest.raises(SamplerError, match="k"):
            s.reservoir(_make_metrics(10), k=0)

    def test_reservoir_negative_k_raises(self):
        s = MetricSampler(rate=1.0)
        with pytest.raises(SamplerError):
            s.reservoir(_make_metrics(10), k=-3)

    def test_reservoir_results_are_pipeline_metrics(self):
        metrics = _make_metrics(20)
        s = MetricSampler(rate=1.0, seed=1)
        for m in s.reservoir(metrics, k=5):
            assert isinstance(m, PipelineMetric)
