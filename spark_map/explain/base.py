"""
Base LLM provider interface.

All LLM providers must implement this interface to ensure consistent
behavior and output.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class LLMProvider(ABC):
    """
    Abstract base class for LLM providers.

    LLMs are used only to explain findings - they never detect problems.
    All providers receive structured data (not raw logs) and return
    human-readable explanations.
    """

    name: str = "base"

    @abstractmethod
    def explain_finding(self, finding_summary: dict[str, Any]) -> str:
        """
        Generate an explanation for a single finding.

        Args:
            finding_summary: Dict containing:
                - id: Finding identifier
                - detector: Which detector found this
                - title: Short title
                - severity: critical/warning/info
                - stage_ids: Affected stages
                - description: Technical description
                - mitigation_tags: Suggested fixes
                - mitigation_hint: Brief suggestion

        Returns:
            Human-readable explanation of the finding.
        """
        pass

    @abstractmethod
    def summarize(self, analysis_summary: dict[str, Any]) -> str:
        """
        Generate an overall summary of the analysis.

        Args:
            analysis_summary: Dict containing:
                - app_id: Spark application ID
                - app_name: Application name
                - duration_ms: Total duration
                - num_stages: Number of stages
                - findings: List of finding summaries

        Returns:
            Human-readable summary with prioritized recommendations.
        """
        pass


FINDING_EXPLANATION_PROMPT = """You are a Spark performance expert helping engineers understand performance issues.

Given this detected performance issue:

Title: {title}
Severity: {severity}
Detector: {detector}
Affected Stages: {stage_ids}

Technical Description:
{description}

Suggested Mitigations: {mitigation_tags}
Hint: {mitigation_hint}

Provide a brief (2-3 sentences) explanation that:
1. Explains WHY this is a problem in plain English
2. Suggests the most impactful fix

Do NOT invent new facts. Only explain what was detected.
Keep your response concise and actionable."""


SUMMARY_PROMPT = """You are a Spark performance expert summarizing an analysis report.

Application: {app_name} ({app_id})
Duration: {duration_ms}ms
Stages: {num_stages}

Detected Issues:
{findings_text}

Provide a brief summary (3-5 sentences) that:
1. Identifies the top 1-2 most impactful issues
2. Gives a prioritized recommendation
3. Notes any patterns across findings

Be concise and actionable. Do not repeat the full technical details."""
