"""
Anthropic (Claude) LLM provider.

Requires an API key. Set ANTHROPIC_API_KEY environment variable
or pass to --llm-api-key.
"""

from __future__ import annotations

from typing import Any

from spark_map.explain.base import (
    FINDING_EXPLANATION_PROMPT,
    SUMMARY_PROMPT,
    LLMProvider,
)


class AnthropicProvider(LLMProvider):
    """
    Anthropic provider for Claude inference.

    Usage:
        provider = AnthropicProvider(api_key="sk-ant-...", model="claude-3-sonnet-20240229")
        explanation = provider.explain_finding(finding_summary)
    """

    name = "anthropic"

    def __init__(
        self,
        api_key: str,
        model: str = "claude-3-sonnet-20240229",
    ) -> None:
        """
        Initialize Anthropic provider.

        Args:
            api_key: Anthropic API key
            model: Model name (claude-3-opus-20240229, claude-3-sonnet-20240229, etc.)
        """
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "Anthropic package not installed. Run: pip install spark-map[anthropic]"
            )

        self.model = model
        self._client = anthropic.Anthropic(api_key=api_key)
        self.name = f"anthropic/{model}"

    def explain_finding(self, finding_summary: dict[str, Any]) -> str:
        """Generate explanation for a finding using Claude."""
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
            response = self._client.messages.create(
                model=self.model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            return f"(Anthropic error: {e})"

    def summarize(self, analysis_summary: dict[str, Any]) -> str:
        """Generate overall summary using Claude."""
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
            response = self._client.messages.create(
                model=self.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            return f"(Anthropic error: {e})"
