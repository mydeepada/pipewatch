"""Tests for pipewatch.aggregator."""

import pytest

from pipewatch.aggregator import AggregationError, MetricAggregator
from pipewatch.metrics import PipelineMetric


def _make_metric(name: str, value: float, pipeline: str = "pipe") -> PipelineMetric:
    return PipelineMetric(name=name, value=value, pipeline=pipeline)


@pytest.fixture()
def sample_metrics():
    return [
        _make_metric("latency", 10.0),
        _make_metric("latency", 20.0),
        _make_metric("latency", 30.0),
        _make_metric("error_rate", 0.5),
        _make_metric("error_rate", 1.5),
    ]


class TestMetricAggregatorBasic:
    def test_mean(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("latency", "mean") == pytest.approx(20.0)

    def test_median(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("latency", "median") == pytest.approx(20.0)

    def test_min(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("latency", "min") == pytest.approx(10.0)

    def test_max(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("latency", "max") == pytest.approx(30.0)

    def test_sum(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("latency", "sum") == pytest.approx(60.0)

    def test_count(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("latency", "count") == 3.0

    def test_stdev(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        result = agg.aggregate("latency", "stdev")
        assert result == pytest.approx(10.0)


class TestMetricAggregatorEdgeCases:
    def test_unknown_method_raises(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        with pytest.raises(AggregationError, match="Unknown aggregation method"):
            agg.aggregate("latency", "variance")

    def test_missing_metric_returns_none(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        assert agg.aggregate("nonexistent", "mean") is None

    def test_stdev_single_value_raises(self):
        agg = MetricAggregator([_make_metric("x", 1.0)])
        with pytest.raises(AggregationError, match="at least 2"):
            agg.aggregate("x", "stdev")

    def test_invalid_metrics_type_raises(self):
        with pytest.raises(TypeError):
            MetricAggregator("not-a-list")  # type: ignore

    def test_empty_metrics_list(self):
        agg = MetricAggregator([])
        assert agg.aggregate("latency", "mean") is None


class TestMetricAggregatorSummary:
    def test_summary_contains_all_methods(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        summary = agg.summary("latency")
        for key in ("mean", "median", "min", "max", "sum", "count", "stdev"):
            assert key in summary

    def test_summary_missing_metric_returns_nones(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        summary = agg.summary("ghost")
        assert all(v is None for v in summary.values())

    def test_metric_names_deduped(self, sample_metrics):
        agg = MetricAggregator(sample_metrics)
        names = agg.metric_names()
        assert sorted(names) == ["error_rate", "latency"]
        assert len(names) == len(set(names))
