"""Tests for pipewatch.exporter."""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

from pipewatch.exporter import MetricsExporter
from pipewatch.metrics import PipelineMetric
from pipewatch.pipeline import PipelineMonitor


@pytest.fixture()
def populated_monitor() -> PipelineMonitor:
    monitor = PipelineMonitor(name="test-pipeline")
    monitor.record(PipelineMetric(name="row_count", value=100, labels={"stage": "ingest"}))
    monitor.record(PipelineMetric(name="error_rate", value=0.02, labels={"stage": "transform"}))
    return monitor


@pytest.fixture()
def exporter(populated_monitor: PipelineMonitor) -> MetricsExporter:
    return MetricsExporter(monitor=populated_monitor)


class TestMetricsExporterJSON:
    def test_to_json_returns_string(self, exporter: MetricsExporter) -> None:
        result = exporter.to_json()
        assert isinstance(result, str)

    def test_to_json_is_valid_json(self, exporter: MetricsExporter) -> None:
        result = exporter.to_json()
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_to_json_contains_pipeline_name(self, exporter: MetricsExporter) -> None:
        parsed = json.loads(exporter.to_json())
        assert parsed.get("pipeline") == "test-pipeline"

    def test_save_json_creates_file(self, exporter: MetricsExporter) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            path = tmp.name
        try:
            exporter.save_json(path)
            assert os.path.exists(path)
            with open(path, encoding="utf-8") as fh:
                parsed = json.load(fh)
            assert "pipeline" in parsed
        finally:
            os.unlink(path)


class TestMetricsExporterCSV:
    def test_to_csv_returns_string(self, exporter: MetricsExporter) -> None:
        result = exporter.to_csv()
        assert isinstance(result, str)

    def test_to_csv_has_header_row(self, exporter: MetricsExporter) -> None:
        result = exporter.to_csv()
        reader = csv.DictReader(io.StringIO(result))
        assert set(reader.fieldnames or []) == {"name", "value", "timestamp", "labels"}

    def test_to_csv_row_count_matches_metrics(self, exporter: MetricsExporter) -> None:
        result = exporter.to_csv()
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 2

    def test_to_csv_values_are_correct(self, exporter: MetricsExporter) -> None:
        result = exporter.to_csv()
        reader = csv.DictReader(io.StringIO(result))
        rows = {row["name"]: row for row in reader}
        assert float(rows["row_count"]["value"]) == pytest.approx(100)
        assert float(rows["error_rate"]["value"]) == pytest.approx(0.02)

    def test_save_csv_creates_file(self, exporter: MetricsExporter) -> None:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            path = tmp.name
        try:
            exporter.save_csv(path)
            assert os.path.exists(path)
        finally:
            os.unlink(path)
