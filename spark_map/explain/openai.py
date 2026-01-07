"""
OpenAI LLM provider.

Requires an API key. Set OPENAI_API_KEY environment variable
or pass to --llm-api-key.
"""

from __future__ import annotations

from typing import Any

from spark_map.explain.base import (
    FINDING_EXPLANATION_PROMPT,
    SUMMARY_PROMPT,
    LLMProvider,
)


class OpenAIProvider(LLMProvider):
    """
    OpenAI provider for cloud LLM inference.

    Usage:
        provider = OpenAIProvider(api_key="sk-...", model="gpt-4o")
        explanation = provider.explain_finding(finding_summary)
    """

    name = "openai"

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o",
    ) -> None:
        """
        Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key
            model: Model name (gpt-4o, gpt-4-turbo, gpt-3.5-turbo)
        """
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError(
                "OpenAI package not installed. Run: pip install spark-map[openai]"
            )

        self.model = model
        self._client = OpenAI(api_key=api_key)
        self.name = f"openai/{model}"

    def explain_finding(self, finding_summary: dict[str, Any]) -> str:
        """Generate explanation for a finding using OpenAI."""
        prompt = FINDING_EXPLANATION_PROMPT.format(
            title=finding_summary.get("title", ""),
            severity=finding_summary.get("severity", ""),
            detector=finding_summary.get("detector", ""),
            stage_ids=finding_summary.get("stage_ids", []),
            description=finding_summary.get("description", ""),
            mitigation_tags=finding_summary.get("mitigation_tags", []),
            mitigation_hint=finding_summary.get("mitigation_hint", ""),
        )

        try:
            response = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a Spark performance expert."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=256,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"(OpenAI error: {e})"

    def summarize(self, analysis_summary: dict[str, Any]) -> str:
        """Generate overall summary using OpenAI."""
        findings = analysis_summary.get("findings", [])
        findings_text = "\n".join(
            f"- [{f.get('severity', 'info')}] {f.get('title', 'Unknown')}"
            for f in findings[:10]
        )

        prompt = SUMMARY_PROMPT.format(
            app_name=analysis_summary.get("app_name", "Unknown"),
            app_id=analysis_summary.get("app_id", "Unknown"),
            duration_ms=analysis_summary.get("duration_ms", 0),
            num_stages=analysis_summary.get("num_stages", 0),
            findings_text=findings_text or "No significant issues detected.",
        )

        try:
            response = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a Spark performance expert."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=512,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"(OpenAI error: {e})"
