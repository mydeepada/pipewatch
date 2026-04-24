"""Terminal dashboard for real-time pipeline health display."""

from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import AlertSeverity
from pipewatch.pipeline import PipelineMonitor


SEVERITY_SYMBOLS = {
    AlertSeverity.INFO: "[INFO]",
    AlertSeverity.WARNING: "[WARN]",
    AlertSeverity.CRITICAL: "[CRIT]",
}

SEVERITY_COLORS = {
    AlertSeverity.INFO: "\033[94m",
    AlertSeverity.WARNING: "\033[93m",
    AlertSeverity.CRITICAL: "\033[91m",
}

RESET = "\033[0m"
BOLD = "\033[1m"


def _colorize(text: str, color: str, use_color: bool = True) -> str:
    if not use_color:
        return text
    return f"{color}{text}{RESET}"


class Dashboard:
    """Renders a summary of pipeline health to stdout."""

    def __init__(self, monitor: PipelineMonitor, use_color: bool = True):
        self.monitor = monitor
        self.use_color = use_color

    def _header(self) -> str:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        title = f"  PipeWatch Dashboard — {now}  "
        border = "=" * len(title)
        return f"{BOLD}{border}\n{title}\n{border}{RESET}" if self.use_color else f"{border}\n{title}\n{border}"

    def _metrics_section(self, summary: dict) -> List[str]:
        lines = [f"{BOLD}Metrics:{RESET}" if self.use_color else "Metrics:"]
        metrics = summary.get("metrics", {})
        if not metrics:
            lines.append("  (no metrics recorded)")
        for name, info in metrics.items():
            value = info.get("latest_value", "N/A")
            count = info.get("count", 0)
            stale = " [STALE]" if info.get("stale") else ""
            lines.append(f"  {name}: {value} (samples: {count}){stale}")
        return lines

    def _alerts_section(self, summary: dict) -> List[str]:
        lines = [f"{BOLD}Active Alerts:{RESET}" if self.use_color else "Active Alerts:"]
        alerts = summary.get("active_alerts", [])
        if not alerts:
            lines.append("  (no active alerts)")
        for alert in alerts:
            severity = AlertSeverity(alert.get("severity", "info"))
            symbol = SEVERITY_SYMBOLS.get(severity, "[INFO]")
            color = SEVERITY_COLORS.get(severity, "")
            msg = f"  {symbol} [{alert.get('rule_name')}] {alert.get('message')}"
            lines.append(_colorize(msg, color, self.use_color))
        return lines

    def render(self) -> str:
        summary = self.monitor.summary()
        sections = [
            self._header(),
            "",
            *self._metrics_section(summary),
            "",
            *self._alerts_section(summary),
            "",
            f"Total alerts fired: {summary.get('total_alerts_fired', 0)}",
        ]
        return "\n".join(sections)

    def print(self) -> None:
        print(self.render())
