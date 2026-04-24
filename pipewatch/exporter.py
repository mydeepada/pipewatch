"""Metrics exporter for pipewatch — serializes pipeline health snapshots to JSON or CSV."""

from __future__ import annotations

import csv
import io
import json
from typing import List

from pipewatch.pipeline import PipelineMonitor


class MetricsExporter:
    """Exports collected pipeline metrics to various output formats."""

    def __init__(self, monitor: PipelineMonitor) -> None:
        self.monitor = monitor

    def to_json(self, indent: int = 2) -> str:
        """Serialize the current pipeline summary to a JSON string."""
        data = self.monitor.summary()
        return json.dumps(data, indent=indent, default=str)

    def to_csv(self) -> str:
        """Serialize recorded metrics to CSV format.

        Each row represents one metric observation with columns:
        name, value, timestamp, labels.
        """
        output = io.StringIO()
        fieldnames = ["name", "value", "timestamp", "labels"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for metric in self.monitor.metrics:
            row = {
                "name": metric.name,
                "value": metric.value,
                "timestamp": metric.timestamp.isoformat(),
                "labels": json.dumps(metric.labels),
            }
            writer.writerow(row)

        return output.getvalue()

    def save_json(self, filepath: str, indent: int = 2) -> None:
        """Write the JSON export to *filepath*."""
        with open(filepath, "w", encoding="utf-8") as fh:
            fh.write(self.to_json(indent=indent))

    def save_csv(self, filepath: str) -> None:
        """Write the CSV export to *filepath*."""
        with open(filepath, "w", encoding="utf-8", newline="") as fh:
            fh.write(self.to_csv())
