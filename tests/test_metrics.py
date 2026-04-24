"""Tests for pipewatch.metrics module."""

import time
import pytest
from pipewatch.metrics import PipelineMetric, MetricsCollector


class TestPipelineMetric:
    def test_default_timestamp_is_recent(self):
        metric = PipelineMetric(name="lag", value=3.5, unit="seconds")
        assert abs(metric.timestamp - time.time()) < 1.0

    def test_is_stale_fresh_metric(self):
        metric = PipelineMetric(name="lag", value=3.5, unit="seconds")
        assert metric.is_stale(max_age_seconds=60) is False

    def test_is_stale_old_metric(self):
        metric = PipelineMetric(name="lag", value=3.5, unit="seconds", timestamp=time.time() - 120)
        assert metric.is_stale(max_age_seconds=60) is True

    def test_to_dict_contains_expected_keys(self):
        metric = PipelineMetric(name="throughput", value=100.0, unit="msg/s", tags={"pipeline": "etl"})
        d = metric.to_dict()
        assert set(d.keys()) == {"name", "value", "unit", "timestamp", "tags"}
        assert d["name"] == "throughput"
        assert d["tags"] == {"pipeline": "etl"}


class TestMetricsCollector:
    def setup_method(self):
        self.collector = MetricsCollector()

    def test_record_and_get(self):
        self.collector.record("error_rate", 0.02, unit="ratio")
        metric = self.collector.get("error_rate")
        assert metric is not None
        assert metric.value == 0.02
        assert metric.unit == "ratio"

    def test_record_overwrites_previous(self):
        self.collector.record("lag", 5.0, unit="s")
        self.collector.record("lag", 2.0, unit="s")
        assert self.collector.get("lag").value == 2.0

    def test_get_missing_returns_none(self):
        assert self.collector.get("nonexistent") is None

    def test_all_metrics_returns_all(self):
        self.collector.record("lag", 1.0)
        self.collector.record("throughput", 200.0)
        assert len(self.collector.all_metrics()) == 2

    def test_clear_removes_all(self):
        self.collector.record("lag", 1.0)
        self.collector.clear()
        assert self.collector.all_metrics() == []

    def test_summary_structure(self):
        self.collector.record("lag", 1.5, unit="s", tags={"env": "prod"})
        summary = self.collector.summary()
        assert "lag" in summary
        assert summary["lag"]["value"] == 1.5
        assert summary["lag"]["tags"] == {"env": "prod"}
