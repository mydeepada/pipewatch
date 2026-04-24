"""Scheduler for periodic pipeline metric collection and alerting."""

import time
import logging
import threading
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class SchedulerError(Exception):
    """Raised when the scheduler encounters a fatal error."""


class MetricsScheduler:
    """Runs a collection callback at a fixed interval in a background thread."""

    def __init__(self, interval: float, callback: Callable[[], None], max_errors: int = 5):
        """
        Args:
            interval: Seconds between each callback invocation.
            callback: Callable executed on each tick.
            max_errors: Consecutive errors allowed before the scheduler stops.
        """
        if interval <= 0:
            raise ValueError("interval must be a positive number")
        self.interval = interval
        self.callback = callback
        self.max_errors = max_errors

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._consecutive_errors = 0
        self._tick_count = 0

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def tick_count(self) -> int:
        return self._tick_count

    def start(self) -> None:
        """Start the scheduler in a daemon background thread."""
        if self.is_running:
            raise SchedulerError("Scheduler is already running")
        self._stop_event.clear()
        self._tick_count = 0
        self._consecutive_errors = 0
        self._thread = threading.Thread(target=self._run, daemon=True, name="MetricsScheduler")
        self._thread.start()
        logger.info("MetricsScheduler started (interval=%.1fs)", self.interval)

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the scheduler to stop and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("MetricsScheduler stopped after %d ticks", self._tick_count)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            start = time.monotonic()
            try:
                self.callback()
                self._tick_count += 1
                self._consecutive_errors = 0
            except Exception as exc:  # noqa: BLE001
                self._consecutive_errors += 1
                logger.error(
                    "Scheduler callback error (%d/%d): %s",
                    self._consecutive_errors,
                    self.max_errors,
                    exc,
                )
                if self._consecutive_errors >= self.max_errors:
                    logger.critical("Max consecutive errors reached — stopping scheduler")
                    break
            elapsed = time.monotonic() - start
            sleep_for = max(0.0, self.interval - elapsed)
            self._stop_event.wait(timeout=sleep_for)
