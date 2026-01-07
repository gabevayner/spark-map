"""
Main analysis entry point.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from spark_map.core.findings import FindingCollection
from spark_map.core.parser import parse_eventlog
from spark_map.core.report import Report
from spark_map.models.schemas import ThresholdConfig

if TYPE_CHECKING:
    from spark_map.explain.base import LLMProvider


def analyze(
    eventlog_path: str | Path,
    *,
    thresholds: ThresholdConfig | None = None,
    llm_provider: LLMProvider | None = None,
) -> Report:
    """
    Analyze a Spark event log and return a report with findings.

    Args:
        eventlog_path: Path to the Spark event log JSON file.
        thresholds: Optional custom thresholds for detection. Uses defaults if None.
        llm_provider: Optional LLM provider for generating explanations.

    Returns:
        Report containing metrics, findings, and optional LLM summary.

    Example:
        >>> from spark_map import analyze
        >>> report = analyze("path/to/eventlog.json")
        >>> print(report.summary())
        >>> report.to_html("report.html")

        # With custom thresholds
        >>> from spark_map.models.schemas import ThresholdConfig
        >>> thresholds = ThresholdConfig(skew_ratio=5.0)
        >>> report = analyze("eventlog.json", thresholds=thresholds)

        # With LLM explanations
        >>> from spark_map.explain.ollama import OllamaProvider
        >>> llm = OllamaProvider(model="codellama:7b-instruct")
        >>> report = analyze("eventlog.json", llm_provider=llm)
    """
    eventlog_path = Path(eventlog_path)
    thresholds = thresholds or ThresholdConfig()

    # Parse the event log
    metrics = parse_eventlog(eventlog_path)

    # Run all detectors
    findings = _run_detectors(metrics, thresholds)

    # Create the report
    report = Report(
        source_path=str(eventlog_path),
        analysis_timestamp=datetime.now(timezone.utc).isoformat(),
        metrics=metrics,
        findings=findings,
    )

    # Generate LLM explanations if provider is configured
    if llm_provider is not None:
        _add_llm_explanations(report, llm_provider)

    return report


def _run_detectors(metrics, thresholds: ThresholdConfig) -> FindingCollection:
    """Run all bottleneck detectors and collect findings."""
    from spark_map.core.detectors import get_all_detectors

    findings = FindingCollection()

    for detector_cls in get_all_detectors():
        detector = detector_cls(thresholds=thresholds)
        detector_findings = detector.detect(metrics)
        for finding in detector_findings:
            findings.add(finding)

    return findings


def _add_llm_explanations(report: Report, llm_provider: LLMProvider) -> None:
    """Add LLM-generated explanations to findings and overall summary."""
    # Generate individual explanations for each finding
    for finding in report.findings:
        explanation = llm_provider.explain_finding(finding.to_summary_dict())
        finding.llm_explanation = explanation

    # Generate overall summary
    if report.findings:
        summary_input = {
            "app_id": report.metrics.app_id,
            "app_name": report.metrics.app_name,
            "duration_ms": report.metrics.total_duration_ms,
            "num_stages": report.metrics.num_stages,
            "findings": report.get_findings_for_llm(),
        }
        report.llm_summary = llm_provider.summarize(summary_input)
        report.llm_provider = llm_provider.name
