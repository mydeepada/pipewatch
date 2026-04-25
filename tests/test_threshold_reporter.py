"""Tests for pipewatch.threshold_reporter module."""

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric
from pipewatch.threshold import Threshold
from pipewatch.threshold_reporter import BreachRecord, ThresholdReport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metric(pipeline: str = "etl", name: str = "row_count", value: float = 100.0) -> PipelineMetric:
    return PipelineMetric(pipeline_name=pipeline, metric_name=name, value=value)


def _above_threshold(warn: float = 80.0, critical: float = 95.0) -> Threshold:
    return Threshold(direction="above", warning_level=warn, critical_level=critical)


def _below_threshold(warn: float = 20.0, critical: float = 5.0) -> Threshold:
    return Threshold(direction="below", warning_level=warn, critical_level=critical)


# ---------------------------------------------------------------------------
# BreachRecord
# ---------------------------------------------------------------------------

class TestBreachRecord:
    def test_to_dict_contains_expected_keys(self):
        metric = _make_metric(value=90.0)
        threshold = _above_threshold()
        record = BreachRecord(metric=metric, threshold=threshold, severity="warning")
        d = record.to_dict()
        assert "pipeline_name" in d
        assert "metric_name" in d
        assert "value" in d
        assert "severity" in d
        assert "direction" in d
        assert "warning_level" in d
        assert "critical_level" in d
        assert "timestamp" in d

    def test_to_dict_values_match_metric(self):
        metric = _make_metric(pipeline="pipeline_a", name="latency", value=90.0)
        threshold = _above_threshold()
        record = BreachRecord(metric=metric, threshold=threshold, severity="warning")
        d = record.to_dict()
        assert d["pipeline_name"] == "pipeline_a"
        assert d["metric_name"] == "latency"
        assert d["value"] == 90.0
        assert d["severity"] == "warning"

    def test_to_dict_critical_severity(self):
        metric = _make_metric(value=99.0)
        threshold = _above_threshold()
        record = BreachRecord(metric=metric, threshold=threshold, severity="critical")
        assert record.to_dict()["severity"] == "critical"


# ---------------------------------------------------------------------------
# ThresholdReport — construction
# ---------------------------------------------------------------------------

class TestThresholdReportInit:
    def test_empty_report_has_zero_counts(self):
        report = ThresholdReport()
        assert report.warning_count == 0
        assert report.critical_count == 0

    def test_empty_report_has_no_breaches(self):
        report = ThresholdReport()
        assert report.breaches == []


# ---------------------------------------------------------------------------
# ThresholdReport — evaluate_metric
# ---------------------------------------------------------------------------

class TestThresholdReportEvaluate:
    def test_no_breach_not_recorded(self):
        report = ThresholdReport()
        metric = _make_metric(value=50.0)
        threshold = _above_threshold(warn=80.0, critical=95.0)
        report.evaluate_metric(metric, threshold)
        assert len(report.breaches) == 0

    def test_warning_breach_recorded(self):
        report = ThresholdReport()
        metric = _make_metric(value=85.0)
        threshold = _above_threshold(warn=80.0, critical=95.0)
        report.evaluate_metric(metric, threshold)
        assert report.warning_count == 1
        assert report.critical_count == 0

    def test_critical_breach_recorded(self):
        report = ThresholdReport()
        metric = _make_metric(value=98.0)
        threshold = _above_threshold(warn=80.0, critical=95.0)
        report.evaluate_metric(metric, threshold)
        assert report.critical_count == 1
        assert report.warning_count == 0

    def test_multiple_evaluations_accumulate(self):
        report = ThresholdReport()
        threshold = _above_threshold(warn=80.0, critical=95.0)
        report.evaluate_metric(_make_metric(value=82.0), threshold)
        report.evaluate_metric(_make_metric(value=96.0), threshold)
        report.evaluate_metric(_make_metric(value=50.0), threshold)
        assert report.warning_count == 1
        assert report.critical_count == 1
        assert len(report.breaches) == 2

    def test_below_threshold_warning_breach(self):
        report = ThresholdReport()
        metric = _make_metric(value=15.0)
        threshold = _below_threshold(warn=20.0, critical=5.0)
        report.evaluate_metric(metric, threshold)
        assert report.warning_count == 1

    def test_below_threshold_critical_breach(self):
        report = ThresholdReport()
        metric = _make_metric(value=3.0)
        threshold = _below_threshold(warn=20.0, critical=5.0)
        report.evaluate_metric(metric, threshold)
        assert report.critical_count == 1


# ---------------------------------------------------------------------------
# ThresholdReport — summary
# ---------------------------------------------------------------------------

class TestThresholdReportSummary:
    def test_summary_keys(self):
        report = ThresholdReport()
        summary = report.summary()
        assert "warning_count" in summary
        assert "critical_count" in summary
        assert "total_breaches" in summary
        assert "breaches" in summary

    def test_summary_total_breaches(self):
        report = ThresholdReport()
        threshold = _above_threshold()
        report.evaluate_metric(_make_metric(value=85.0), threshold)
        report.evaluate_metric(_make_metric(value=98.0), threshold)
        summary = report.summary()
        assert summary["total_breaches"] == 2
        assert summary["warning_count"] == 1
        assert summary["critical_count"] == 1

    def test_summary_breaches_are_dicts(self):
        report = ThresholdReport()
        threshold = _above_threshold()
        report.evaluate_metric(_make_metric(value=85.0), threshold)
        summary = report.summary()
        assert isinstance(summary["breaches"], list)
        assert isinstance(summary["breaches"][0], dict)
