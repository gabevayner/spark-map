"""
Spark Map - Analyze Spark event logs to identify performance bottlenecks.

Basic usage:
    >>> from spark_map import analyze
    >>> report = analyze("path/to/eventlog.json")
    >>> report.to_html("report.html")

For more details, see: https://github.com/yourusername/spark-map
"""

from spark_map.core.analyzer import analyze
from spark_map.core.findings import Finding, Severity
from spark_map.core.report import Report
from spark_map.models.schemas import SparkMetrics, StageMetrics, TaskMetrics

__version__ = "0.1.0"

__all__ = [
    # Main entry point
    "analyze",
    # Core classes
    "Report",
    "Finding",
    "Severity",
    # Data models
    "SparkMetrics",
    "StageMetrics",
    "TaskMetrics",
    # Version
    "__version__",
]
