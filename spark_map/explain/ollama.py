"""
Ollama LLM provider for local inference.

Ollama provides free, local LLM inference. No API key required.
Install from: https://ollama.ai
"""

from __future__ import annotations

from typing import Any

from spark_map.explain.base import (
    FINDING_EXPLANATION_PROMPT,
    SUMMARY_PROMPT,
    LLMProvider,
)


class OllamaProvider(LLMProvider):
    """
    Ollama provider for local LLM inference.

    Usage:
        provider = OllamaProvider(model="codellama:7b-instruct")
        explanation = provider.explain_finding(finding_summary)
    """

    name = "ollama"

    def __init__(
        self,
        model: str = "codellama:7b-instruct",
        host: str | None = None,
    ) -> None:
        """
        Initialize Ollama provider.

        Args:
            model: Ollama model name (run `ollama list` to see available models)
            host: Ollama host URL (default: http://localhost:11434)
        """
        try:
            import ollama
        except ImportError:
            raise ImportError(
                "Ollama package not installed. Run: pip install spark-map[ollama]"
            )

        self.model = model
        self._client = ollama.Client(host=host) if host else ollama.Client()
        self.name = f"ollama/{model}"

    def explain_finding(self, finding_summary: dict[str, Any]) -> str:
        """Generate explanation for a finding using Ollama."""
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
            response = self._client.generate(
                model=self.model,
                prompt=prompt,
                options={"temperature": 0.3, "num_predict": 256},
            )
            return response["response"].strip()
        except Exception as e:
            return f"(Ollama error: {e})"

    def summarize(self, analysis_summary: dict[str, Any]) -> str:
        """Generate overall summary using Ollama."""
        findings = analysis_summary.get("findings", [])
        findings_text = "\n".join(
            f"- [{f.get('severity', 'info')}] {f.get('title', 'Unknown')}"
            for f in findings[:10]  # Limit to top 10
        )

        prompt = SUMMARY_PROMPT.format(
            app_name=analysis_summary.get("app_name", "Unknown"),
            app_id=analysis_summary.get("app_id", "Unknown"),
            duration_ms=analysis_summary.get("duration_ms", 0),
            num_stages=analysis_summary.get("num_stages", 0),
            findings_text=findings_text or "No significant issues detected.",
        )

        try:
            response = self._client.generate(
                model=self.model,
                prompt=prompt,
                options={"temperature": 0.3, "num_predict": 512},
            )
            return response["response"].strip()
        except Exception as e:
            return f"(Ollama error: {e})"
