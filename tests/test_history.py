"""Tests for pipewatch.history module."""

import pytest

from pipewatch.history import HistoryError, MetricHistory
from pipewatch.metrics import PipelineMetric


def _make_metric(pipeline: str, value: float = 1.0) -> PipelineMetric:
    return PipelineMetric(pipeline_name=pipeline, metric_name="latency", value=value)


@pytest.fixture()
def history() -> MetricHistory:
    return MetricHistory(max_size=5)


class TestMetricHistoryInit:
    def test_default_max_size(self):
        h = MetricHistory()
        assert h.max_size == 100

    def test_custom_max_size(self):
        h = MetricHistory(max_size=10)
        assert h.max_size == 10

    def test_zero_max_size_raises(self):
        with pytest.raises(HistoryError):
            MetricHistory(max_size=0)

    def test_negative_max_size_raises(self):
        with pytest.raises(HistoryError):
            MetricHistory(max_size=-3)


class TestMetricHistoryPush:
    def test_push_single_metric(self, history):
        m = _make_metric("pipe_a")
        history.push(m)
        assert history.size("pipe_a") == 1

    def test_push_many(self, history):
        metrics = [_make_metric("pipe_a", float(i)) for i in range(3)]
        history.push_many(metrics)
        assert history.size("pipe_a") == 3

    def test_bounded_by_max_size(self, history):
        for i in range(10):
            history.push(_make_metric("pipe_a", float(i)))
        assert history.size("pipe_a") == 5

    def test_oldest_entry_evicted(self, history):
        for i in range(6):
            history.push(_make_metric("pipe_a", float(i)))
        entries = history.get("pipe_a")
        assert entries[0].value == 1.0  # 0 was evicted


class TestMetricHistoryGet:
    def test_get_unknown_pipeline_returns_empty(self, history):
        assert history.get("nonexistent") == []

    def test_latest_returns_last_pushed(self, history):
        history.push(_make_metric("pipe_a", 10.0))
        history.push(_make_metric("pipe_a", 99.0))
        assert history.latest("pipe_a").value == 99.0

    def test_latest_unknown_pipeline_returns_none(self, history):
        assert history.latest("ghost") is None

    def test_pipelines_sorted(self, history):
        history.push(_make_metric("zebra"))
        history.push(_make_metric("alpha"))
        history.push(_make_metric("mango"))
        assert history.pipelines() == ["alpha", "mango", "zebra"]


class TestMetricHistoryClear:
    def test_clear_single_pipeline(self, history):
        history.push(_make_metric("pipe_a"))
        history.push(_make_metric("pipe_b"))
        history.clear("pipe_a")
        assert history.size("pipe_a") == 0
        assert history.size("pipe_b") == 1

    def test_clear_all_pipelines(self, history):
        history.push(_make_metric("pipe_a"))
        history.push(_make_metric("pipe_b"))
        history.clear()
        assert history.pipelines() == []
