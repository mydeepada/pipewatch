"""Notification dispatch for pipeline alerts."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List

from pipewatch.alerts import Alert, AlertSeverity

logger = logging.getLogger(__name__)


class NotificationChannel(ABC):
    """Abstract base class for notification channels."""

    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """Send an alert notification. Returns True on success."""
        ...


class LogNotifier(NotificationChannel):
    """Logs alerts using Python's logging module."""

    LEVEL_MAP = {
        AlertSeverity.INFO: logging.INFO,
        AlertSeverity.WARNING: logging.WARNING,
        AlertSeverity.CRITICAL: logging.CRITICAL,
    }

    def send(self, alert: Alert) -> bool:
        level = self.LEVEL_MAP.get(alert.severity, logging.INFO)
        logger.log(
            level,
            "[pipewatch] %s | pipeline=%s metric=%s value=%.4f",
            alert.message,
            alert.pipeline_name,
            alert.metric_name,
            alert.metric_value,
        )
        return True


class StdoutNotifier(NotificationChannel):
    """Prints alerts to stdout (useful for CLI output)."""

    def send(self, alert: Alert) -> bool:
        severity_label = alert.severity.value.upper()
        print(
            f"[{severity_label}] {alert.pipeline_name} — {alert.message} "
            f"({alert.metric_name}={alert.metric_value:.4f})"
        )
        return True


@dataclass
class NotificationDispatcher:
    """Dispatches alerts to one or more registered channels."""

    channels: List[NotificationChannel] = field(default_factory=list)

    def register(self, channel: NotificationChannel) -> None:
        """Register a notification channel."""
        self.channels.append(channel)

    def dispatch(self, alert: Alert) -> dict:
        """Send alert to all channels. Returns a results summary."""
        results = {}
        for channel in self.channels:
            name = type(channel).__name__
            try:
                results[name] = channel.send(alert)
            except Exception as exc:  # noqa: BLE001
                logger.error("Channel %s failed: %s", name, exc)
                results[name] = False
        return results

    def dispatch_all(self, alerts: List[Alert]) -> List[dict]:
        """Dispatch a list of alerts and return per-alert results."""
        return [self.dispatch(alert) for alert in alerts]
