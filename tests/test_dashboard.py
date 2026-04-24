"""Tests for the Dashboard rendering module."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from pipewatch.dashboard import Dashboard, _colorize, RESET
from pipewatch.alerts import AlertSeverity
from pipewatch.pipeline import PipelineMonitor


@pytest.fixture
def empty_monitor():
    return PipelineMonitor(pipeline_name="test-pipe")


@pytest.fixture
def populated_monitor():
    from pipewatch.alerts import AlertRule
    monitor = PipelineMonitor(pipeline_name="test-pipe")
    rule = AlertRule(
        name="high_latency",
        metric_name="latency",
        threshold=100.0,
        severity=AlertSeverity.WARNING,
    )
    monitor.add_rule(rule)
    monitor.record("latency", 150.0)
    monitor.record("throughput", 42.0)
    return monitor


class TestColorize:
    def test_colorize_with_color_enabled(self):
        result = _colorize("hello", "\033[91m", use_color=True)
        assert "hello" in result
        assert RESET in result

    def test_colorize_with_color_disabled(self):
        result = _colorize("hello", "\033[91m", use_color=False)
        assert result == "hello"


class TestDashboard:
    def test_render_returns_string(self, empty_monitor):
        dash = Dashboard(empty_monitor, use_color=False)
        output = dash.render()
        assert isinstance(output, str)

    def test_render_contains_pipeline_name(self, empty_monitor):
        dash = Dashboard(empty_monitor, use_color=False)
        output = dash.render()
        assert "PipeWatch Dashboard" in output

    def test_render_no_metrics_message(self, empty_monitor):
        dash = Dashboard(empty_monitor, use_color=False)
        output = dash.render()
        assert "no metrics recorded" in output

    def test_render_no_alerts_message(self, empty_monitor):
        dash = Dashboard(empty_monitor, use_color=False)
        output = dash.render()
        assert "no active alerts" in output

    def test_render_shows_metric_names(self, populated_monitor):
        dash = Dashboard(populated_monitor, use_color=False)
        output = dash.render()
        assert "latency" in output
        assert "throughput" in output

    def test_render_shows_alert_rule_name(self, populated_monitor):
        dash = Dashboard(populated_monitor, use_color=False)
        output = dash.render()
        assert "high_latency" in output

    def test_render_shows_total_alerts(self, populated_monitor):
        dash = Dashboard(populated_monitor, use_color=False)
        output = dash.render()
        assert "Total alerts fired" in output

    def test_print_calls_builtin_print(self, empty_monitor):
        dash = Dashboard(empty_monitor, use_color=False)
        with patch("builtins.print") as mock_print:
            dash.print()
            mock_print.assert_called_once()

    def test_render_with_color_enabled(self, populated_monitor):
        dash = Dashboard(populated_monitor, use_color=True)
        output = dash.render()
        assert "\033[" in output
