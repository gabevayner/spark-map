"""
Report class that holds analysis results and supports multiple output formats.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from spark_map.core.findings import Finding, FindingCollection

if TYPE_CHECKING:
    from spark_map.models.schemas import SparkMetrics


class Report(BaseModel):
    """
    Analysis report containing metrics, findings, and optional LLM explanations.

    Usage:
        >>> report = analyze("eventlog.json")
        >>> print(report.summary())
        >>> report.to_html("report.html")
        >>> report.to_json("report.json")
    """

    # Metadata
    source_path: str = Field(description="Path to the analyzed event log")
    spark_version: str | None = Field(default=None, description="Spark version if detected")
    analysis_timestamp: str = Field(description="When this analysis was run")

    # Core data
    metrics: Any = Field(description="Extracted SparkMetrics")  # SparkMetrics, but Any for flexibility
    findings: FindingCollection = Field(default_factory=FindingCollection)

    # LLM summary (optional)
    llm_summary: str | None = Field(default=None, description="Overall LLM-generated summary")
    llm_provider: str | None = Field(default=None, description="LLM provider used (if any)")

    model_config = {"arbitrary_types_allowed": True}

    def summary(self) -> str:
        """Generate a text summary of findings."""
        lines = [
            f"Spark Map Analysis: {self.source_path}",
            f"{'=' * 50}",
            "",
            f"Application: {self.metrics.app_name or self.metrics.app_id}",
            f"Duration: {self.metrics.total_duration_ms / 1000:.1f}s",
            f"Stages: {self.metrics.num_stages} ({self.metrics.num_failed_stages} failed)",
            f"Tasks: {self.metrics.num_tasks} ({self.metrics.num_failed_tasks} failed)",
            "",
            f"Findings: {len(self.findings)} total",
            f"  Critical: {len(self.findings.critical)}",
            f"  Warnings: {len(self.findings.warnings)}",
            f"  Info: {len(self.findings.info)}",
            "",
        ]

        if self.findings:
            lines.append("Top Issues:")
            lines.append("-" * 30)
            for finding in self.findings.sorted_by_severity()[:5]:
                lines.append(f"  [{finding.severity}] {finding.title}")
                if finding.stage_ids:
                    lines.append(f"    Stages: {finding.stage_ids}")
                lines.append(f"    {finding.description[:100]}...")
                lines.append("")

        if self.llm_summary:
            lines.extend([
                "",
                "AI Summary:",
                "-" * 30,
                self.llm_summary,
            ])

        return "\n".join(lines)

    def to_dict(self, include_raw_metrics: bool = True) -> dict[str, Any]:
        """Convert report to dictionary."""
        data: dict[str, Any] = {
            "source_path": self.source_path,
            "spark_version": self.spark_version,
            "analysis_timestamp": self.analysis_timestamp,
            "summary": {
                "app_id": self.metrics.app_id,
                "app_name": self.metrics.app_name,
                "duration_ms": self.metrics.total_duration_ms,
                "num_stages": self.metrics.num_stages,
                "num_tasks": self.metrics.num_tasks,
                "num_findings": len(self.findings),
                "num_critical": len(self.findings.critical),
                "num_warnings": len(self.findings.warnings),
            },
            "findings": [f.model_dump() for f in self.findings.sorted_by_severity()],
        }

        if include_raw_metrics:
            data["metrics"] = self.metrics.model_dump()

        if self.llm_summary:
            data["llm_summary"] = self.llm_summary
            data["llm_provider"] = self.llm_provider

        return data

    def to_json(self, path: str | Path, indent: int = 2, include_raw_metrics: bool = True) -> None:
        """Export report as JSON file."""
        path = Path(path)
        data = self.to_dict(include_raw_metrics=include_raw_metrics)
        path.write_text(json.dumps(data, indent=indent, default=str))

    def to_html(self, path: str | Path) -> None:
        """Export report as HTML file."""
        from spark_map.render.html import render_html

        path = Path(path)
        html_content = render_html(self)
        path.write_text(html_content)

    def to_markdown(self, path: str | Path) -> None:
        """Export report as Markdown file."""
        from spark_map.render.markdown import render_markdown

        path = Path(path)
        md_content = render_markdown(self)
        path.write_text(md_content)

    def get_findings_for_llm(self) -> list[dict[str, Any]]:
        """Get findings in a format suitable for LLM consumption."""
        return [f.to_summary_dict() for f in self.findings.sorted_by_severity()]

    def add_finding(self, finding: Finding) -> None:
        """Add a finding to the report."""
        self.findings.add(finding)
