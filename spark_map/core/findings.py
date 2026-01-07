"""
Finding and severity models for detected bottlenecks.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Severity(str, Enum):
    """Severity levels for findings."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"

    def __str__(self) -> str:
        return self.value


class MitigationTag(str, Enum):
    """Suggested mitigation strategies."""

    REPARTITION = "repartition"
    COALESCE = "coalesce"
    BROADCAST_JOIN = "broadcast-join"
    INCREASE_MEMORY = "increase-memory"
    INCREASE_PARALLELISM = "increase-parallelism"
    REDUCE_PARALLELISM = "reduce-parallelism"
    ENABLE_AQE = "enable-aqe"
    CACHE_DATA = "cache-data"
    FILTER_EARLY = "filter-early"
    SALTING = "salting"
    OPTIMIZE_SHUFFLE = "optimize-shuffle"
    CHECK_DATA_SOURCE = "check-data-source"
    REDUCE_COLLECT = "reduce-collect"

    def __str__(self) -> str:
        return self.value


class Finding(BaseModel):
    """A detected performance issue with supporting evidence."""

    # Identity
    id: str = Field(description="Unique finding identifier (e.g., 'skew-stage-5')")
    detector: str = Field(description="Name of detector that found this (e.g., 'skew')")

    # Classification
    title: str = Field(description="Short, human-readable title")
    severity: Severity = Field(description="How serious is this issue")

    # Location
    stage_ids: list[int] = Field(default_factory=list, description="Affected stage IDs")

    # Evidence
    description: str = Field(description="Detailed explanation of the finding")
    metrics: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw metrics that triggered this finding",
    )

    # Suggestions
    mitigation_tags: list[MitigationTag] = Field(
        default_factory=list,
        description="Suggested mitigation strategies",
    )
    mitigation_hint: str = Field(
        default="",
        description="Brief human-readable suggestion",
    )

    # LLM explanation (populated later if LLM is enabled)
    llm_explanation: str | None = Field(
        default=None,
        description="LLM-generated explanation (if enabled)",
    )

    def to_summary_dict(self) -> dict[str, Any]:
        """Return a dict suitable for LLM consumption (no raw metrics)."""
        return {
            "id": self.id,
            "detector": self.detector,
            "title": self.title,
            "severity": str(self.severity),
            "stage_ids": self.stage_ids,
            "description": self.description,
            "mitigation_tags": [str(t) for t in self.mitigation_tags],
            "mitigation_hint": self.mitigation_hint,
        }


class FindingCollection(BaseModel):
    """Collection of findings with helper methods."""

    findings: list[Finding] = Field(default_factory=list)

    def add(self, finding: Finding) -> None:
        """Add a finding to the collection."""
        self.findings.append(finding)

    def by_severity(self, severity: Severity) -> list[Finding]:
        """Get findings filtered by severity."""
        return [f for f in self.findings if f.severity == severity]

    def by_detector(self, detector: str) -> list[Finding]:
        """Get findings from a specific detector."""
        return [f for f in self.findings if f.detector == detector]

    def by_stage(self, stage_id: int) -> list[Finding]:
        """Get findings affecting a specific stage."""
        return [f for f in self.findings if stage_id in f.stage_ids]

    @property
    def critical(self) -> list[Finding]:
        """Get all critical findings."""
        return self.by_severity(Severity.CRITICAL)

    @property
    def warnings(self) -> list[Finding]:
        """Get all warning findings."""
        return self.by_severity(Severity.WARNING)

    @property
    def info(self) -> list[Finding]:
        """Get all info findings."""
        return self.by_severity(Severity.INFO)

    def sorted_by_severity(self) -> list[Finding]:
        """Return findings sorted by severity (critical first)."""
        order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
        return sorted(self.findings, key=lambda f: order[f.severity])

    def __len__(self) -> int:
        return len(self.findings)

    def __iter__(self):
        return iter(self.findings)
