"""
Shuffle explosion detector.

Detects when shuffle data is disproportionately large compared to input size.
"""

from __future__ import annotations

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics


class ShuffleExplosionDetector(BaseDetector):
    """Detect shuffle explosion by comparing shuffle vs input bytes."""

    name = "shuffle"
    description = "Detects excessive shuffle data relative to input"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings: list[Finding] = []

        for stage in metrics.stages:
            # Need input data to compare against
            if stage.input_bytes == 0:
                continue

            # Check shuffle write explosion
            if stage.shuffle_write_bytes > 0:
                ratio = stage.shuffle_write_bytes / stage.input_bytes

                if ratio >= self.thresholds.shuffle_explosion_ratio:
                    severity = Severity.CRITICAL if ratio > self.thresholds.shuffle_explosion_ratio * 2 else Severity.WARNING

                    findings.append(
                        Finding(
                            id=f"shuffle-explosion-stage-{stage.stage_id}",
                            detector=self.name,
                            title=f"Shuffle explosion in stage {stage.stage_id}",
                            severity=severity,
                            stage_ids=[stage.stage_id],
                            description=(
                                f"Stage {stage.stage_id} ({stage.stage_name}) wrote {_format_bytes(stage.shuffle_write_bytes)} "
                                f"to shuffle while reading only {_format_bytes(stage.input_bytes)} input "
                                f"(ratio: {ratio:.1f}x). This often indicates an exploding join or inefficient aggregation."
                            ),
                            metrics={
                                "input_bytes": stage.input_bytes,
                                "shuffle_write_bytes": stage.shuffle_write_bytes,
                                "explosion_ratio": round(ratio, 2),
                            },
                            mitigation_tags=[
                                MitigationTag.BROADCAST_JOIN,
                                MitigationTag.FILTER_EARLY,
                                MitigationTag.OPTIMIZE_SHUFFLE,
                            ],
                            mitigation_hint=(
                                "Consider using broadcast joins for small tables, "
                                "filtering data earlier in the pipeline, or reviewing join conditions."
                            ),
                        )
                    )

        # Also check application-wide shuffle
        if metrics.total_input_bytes > 0:
            total_shuffle = metrics.total_shuffle_read_bytes + metrics.total_shuffle_write_bytes
            total_ratio = total_shuffle / metrics.total_input_bytes

            if total_ratio >= self.thresholds.shuffle_explosion_ratio * 2:
                findings.append(
                    Finding(
                        id="shuffle-explosion-global",
                        detector=self.name,
                        title="High overall shuffle volume",
                        severity=Severity.WARNING,
                        stage_ids=[],
                        description=(
                            f"Application shuffled {_format_bytes(total_shuffle)} total "
                            f"while input was {_format_bytes(metrics.total_input_bytes)} "
                            f"(ratio: {total_ratio:.1f}x). This may indicate multiple expensive shuffles."
                        ),
                        metrics={
                            "total_input_bytes": metrics.total_input_bytes,
                            "total_shuffle_bytes": total_shuffle,
                            "shuffle_ratio": round(total_ratio, 2),
                        },
                        mitigation_tags=[
                            MitigationTag.CACHE_DATA,
                            MitigationTag.OPTIMIZE_SHUFFLE,
                            MitigationTag.ENABLE_AQE,
                        ],
                        mitigation_hint=(
                            "Consider caching intermediate results, enabling AQE, "
                            "or restructuring the query to reduce shuffles."
                        ),
                    )
                )

        return findings


def _format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"
