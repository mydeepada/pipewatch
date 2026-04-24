"""Tests for pipewatch.scheduler."""

import time
import threading
import pytest
from unittest.mock import MagicMock, patch

from pipewatch.scheduler import MetricsScheduler, SchedulerError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scheduler(interval=0.05, callback=None, max_errors=3):
    cb = callback or MagicMock()
    return MetricsScheduler(interval=interval, callback=cb, max_errors=max_errors), cb


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestMetricsSchedulerInit:
    def test_valid_construction(self):
        s, _ = _make_scheduler()
        assert s.interval == 0.05
        assert s.max_errors == 3

    def test_invalid_interval_raises(self):
        with pytest.raises(ValueError, match="positive"):
            MetricsScheduler(interval=0, callback=MagicMock())

    def test_negative_interval_raises(self):
        with pytest.raises(ValueError):
            MetricsScheduler(interval=-1, callback=MagicMock())


# ---------------------------------------------------------------------------
# Start / stop
# ---------------------------------------------------------------------------

class TestMetricsSchedulerLifecycle:
    def test_is_running_after_start(self):
        s, _ = _make_scheduler()
        s.start()
        assert s.is_running
        s.stop()

    def test_not_running_before_start(self):
        s, _ = _make_scheduler()
        assert not s.is_running

    def test_not_running_after_stop(self):
        s, _ = _make_scheduler()
        s.start()
        s.stop()
        assert not s.is_running

    def test_double_start_raises(self):
        s, _ = _make_scheduler()
        s.start()
        with pytest.raises(SchedulerError, match="already running"):
            s.start()
        s.stop()


# ---------------------------------------------------------------------------
# Tick behaviour
# ---------------------------------------------------------------------------

class TestMetricsSchedulerTicks:
    def test_callback_called_multiple_times(self):
        s, cb = _make_scheduler(interval=0.02)
        s.start()
        time.sleep(0.12)
        s.stop()
        assert cb.call_count >= 3

    def test_tick_count_increments(self):
        s, _ = _make_scheduler(interval=0.02)
        s.start()
        time.sleep(0.12)
        s.stop()
        assert s.tick_count >= 3

    def test_error_does_not_stop_scheduler_immediately(self):
        cb = MagicMock(side_effect=[Exception("boom"), None, None, None, None])
        s = MetricsScheduler(interval=0.02, callback=cb, max_errors=3)
        s.start()
        time.sleep(0.15)
        s.stop()
        assert s.tick_count >= 2

    def test_max_errors_stops_scheduler(self):
        cb = MagicMock(side_effect=Exception("always fails"))
        s = MetricsScheduler(interval=0.02, callback=cb, max_errors=3)
        s.start()
        time.sleep(0.3)
        assert not s.is_running
