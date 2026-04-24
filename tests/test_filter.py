"""Tests for pipewatch.filter."""

from __future__ import annotations

import pytest

from pipewatch.filter import MetricFilter, apply_filter
from pipewatch.metrics import PipelineMetric


def _make_metric(
    pipeline_name: str = "etl",
    name: str = "row_count",
    value: float = 100.0,
    tags: dict | None = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline_name,
        name=name,
        value=value,
        tags=tags or {},
    )


@pytest.fixture
def sample_metrics():
    return [
        _make_metric(pipeline_name="etl", name="row_count", value=500, tags={"env": "prod"}),
        _make_metric(pipeline_name="etl", name="error_rate", value=0.02, tags={"env": "prod"}),
        _make_metric(pipeline_name="ingest", name="row_count", value=200, tags={"env": "staging"}),
        _make_metric(pipeline_name="ingest", name="latency_ms", value=350, tags={"env": "prod"}),
    ]


class TestMetricFilterMatches:
    def test_no_criteria_matches_all(self, sample_metrics):
        f = MetricFilter()
        assert all(f.matches(m) for m in sample_metrics)

    def test_pipeline_name_filter(self, sample_metrics):
        f = MetricFilter(pipeline_name="etl")
        result = apply_filter(sample_metrics, f)
        assert len(result) == 2
        assert all(m.pipeline_name == "etl" for m in result)

    def test_metric_name_filter(self, sample_metrics):
        f = MetricFilter(metric_name="row_count")
        result = apply_filter(sample_metrics, f)
        assert len(result) == 2
        assert all(m.name == "row_count" for m in result)

    def test_min_value_filter(self, sample_metrics):
        f = MetricFilter(min_value=300)
        result = apply_filter(sample_metrics, f)
        assert all(m.value >= 300 for m in result)

    def test_max_value_filter(self, sample_metrics):
        f = MetricFilter(max_value=300)
        result = apply_filter(sample_metrics, f)
        assert all(m.value <= 300 for m in result)

    def test_tag_filter(self, sample_metrics):
        f = MetricFilter(tags={"env": "prod"})
        result = apply_filter(sample_metrics, f)
        assert len(result) == 3
        assert all(m.tags.get("env") == "prod" for m in result)

    def test_combined_filters(self, sample_metrics):
        f = MetricFilter(pipeline_name="ingest", tags={"env": "prod"})
        result = apply_filter(sample_metrics, f)
        assert len(result) == 1
        assert result[0].name == "latency_ms"

    def test_custom_predicate(self, sample_metrics):
        f = MetricFilter()
        f.add_predicate(lambda m: m.value > 100)
        result = apply_filter(sample_metrics, f)
        assert all(m.value > 100 for m in result)

    def test_chained_predicates(self, sample_metrics):
        f = (
            MetricFilter()
            .add_predicate(lambda m: m.value > 100)
            .add_predicate(lambda m: m.pipeline_name == "etl")
        )
        result = apply_filter(sample_metrics, f)
        assert len(result) == 1
        assert result[0].name == "row_count"

    def test_no_match_returns_empty(self, sample_metrics):
        f = MetricFilter(pipeline_name="nonexistent")
        assert apply_filter(sample_metrics, f) == []
