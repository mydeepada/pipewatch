"""Tests for pipewatch.notifier module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.notifier import (
    LogNotifier,
    NotificationDispatcher,
    StdoutNotifier,
)


@pytest.fixture()
def sample_alert() -> Alert:
    return Alert(
        pipeline_name="etl_daily",
        metric_name="error_rate",
        metric_value=0.42,
        severity=AlertSeverity.WARNING,
        message="error_rate exceeded threshold of 0.10",
    )


class TestLogNotifier:
    def test_send_returns_true(self, sample_alert):
        notifier = LogNotifier()
        assert notifier.send(sample_alert) is True

    def test_send_logs_at_warning_level(self, sample_alert):
        notifier = LogNotifier()
        with patch("pipewatch.notifier.logger") as mock_logger:
            notifier.send(sample_alert)
            mock_logger.log.assert_called_once()
            args = mock_logger.log.call_args[0]
            import logging
            assert args[0] == logging.WARNING

    def test_send_critical_logs_at_critical_level(self, sample_alert):
        sample_alert.severity = AlertSeverity.CRITICAL
        notifier = LogNotifier()
        with patch("pipewatch.notifier.logger") as mock_logger:
            notifier.send(sample_alert)
            import logging
            assert mock_logger.log.call_args[0][0] == logging.CRITICAL


class TestStdoutNotifier:
    def test_send_returns_true(self, sample_alert, capsys):
        notifier = StdoutNotifier()
        result = notifier.send(sample_alert)
        assert result is True

    def test_send_prints_pipeline_name(self, sample_alert, capsys):
        notifier = StdoutNotifier()
        notifier.send(sample_alert)
        captured = capsys.readouterr()
        assert "etl_daily" in captured.out
        assert "WARNING" in captured.out


class TestNotificationDispatcher:
    def test_register_adds_channel(self):
        dispatcher = NotificationDispatcher()
        ch = StdoutNotifier()
        dispatcher.register(ch)
        assert ch in dispatcher.channels

    def test_dispatch_calls_all_channels(self, sample_alert):
        ch1 = MagicMock()
        ch1.__class__.__name__ = "MockA"
        ch2 = MagicMock()
        ch2.__class__.__name__ = "MockB"
        dispatcher = NotificationDispatcher(channels=[ch1, ch2])
        dispatcher.dispatch(sample_alert)
        ch1.send.assert_called_once_with(sample_alert)
        ch2.send.assert_called_once_with(sample_alert)

    def test_dispatch_returns_results_dict(self, sample_alert):
        ch = MagicMock(return_value=True)
        ch.send.return_value = True
        dispatcher = NotificationDispatcher(channels=[ch])
        results = dispatcher.dispatch(sample_alert)
        assert isinstance(results, dict)

    def test_dispatch_handles_channel_exception(self, sample_alert):
        ch = MagicMock()
        ch.__class__.__name__ = "BrokenChannel"
        ch.send.side_effect = RuntimeError("connection refused")
        dispatcher = NotificationDispatcher(channels=[ch])
        results = dispatcher.dispatch(sample_alert)
        assert results["BrokenChannel"] is False

    def test_dispatch_all_returns_list(self, sample_alert):
        dispatcher = NotificationDispatcher(channels=[StdoutNotifier()])
        results = dispatcher.dispatch_all([sample_alert, sample_alert])
        assert len(results) == 2
