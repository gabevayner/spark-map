"""Core analysis functionality for Spark Map."""

from spark_map.core.analyzer import analyze
from spark_map.core.findings import Finding, Severity
from spark_map.core.report import Report

__all__ = ["analyze", "Report", "Finding", "Severity"]
