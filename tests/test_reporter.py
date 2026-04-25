"""Tests for pipewatch.reporter."""

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.pipeline import PipelineMonitor
from pipewatch.reporter import PipelineReport


@pytest.fixture()
def populated_monitor():
    monitor = PipelineMonitor(name="test-monitor")
    metrics = [
        PipelineMetric(name="latency", value=100.0, pipeline="pipe-a"),
        PipelineMetric(name="latency", value=200.0, pipeline="pipe-a"),
        PipelineMetric(name="latency", value=150.0, pipeline="pipe-b"),
        PipelineMetric(name="error_rate", value=0.1, pipeline="pipe-a"),
        PipelineMetric(name="error_rate", value=0.3, pipeline="pipe-b"),
    ]
    monitor.record_many(metrics)
    return monitor


@pytest.fixture()
def report(populated_monitor):
    return PipelineReport(populated_monitor)


class TestPipelineReportGenerate:
    def test_generate_returns_dict(self, report):
        result = report.generate()
        assert isinstance(result, dict)

    def test_generate_contains_expected_pipelines(self, report):
        result = report.generate()
        assert "pipe-a" in result
        assert "pipe-b" in result

    def test_generate_contains_expected_metrics(self, report):
        result = report.generate()
        assert "latency" in result["pipe-a"]
        assert "error_rate" in result["pipe-a"]

    def test_generate_mean_latency_pipe_a(self, report):
        result = report.generate()
        assert result["pipe-a"]["latency"]["mean"] == pytest.approx(150.0)

    def test_generate_count_latency_pipe_b(self, report):
        result = report.generate()
        assert result["pipe-b"]["latency"]["count"] == 1.0

    def test_generate_min_max_latency_pipe_a(self, report):
        result = report.generate()
        assert result["pipe-a"]["latency"]["min"] == pytest.approx(100.0)
        assert result["pipe-a"]["latency"]["max"] == pytest.approx(200.0)


class TestPipelineReportCustomMethods:
    def test_custom_methods_respected(self, populated_monitor):
        report = PipelineReport(populated_monitor, methods=("sum", "count"))
        result = report.generate()
        for pipeline in result.values():
            for metric in pipeline.values():
                assert set(metric.keys()) == {"sum", "count"}


class TestPipelineReportHelpers:
    def test_pipelines_returns_list(self, report):
        assert sorted(report.pipelines()) == ["pipe-a", "pipe-b"]

    def test_metric_names_returns_list(self, report):
        assert sorted(report.metric_names()) == ["error_rate", "latency"]

    def test_empty_monitor_pipelines(self):
        monitor = PipelineMonitor(name="empty")
        report = PipelineReport(monitor)
        assert report.pipelines() == []

    def test_empty_monitor_generate(self):
        monitor = PipelineMonitor(name="empty")
        report = PipelineReport(monitor)
        assert report.generate() == {}
