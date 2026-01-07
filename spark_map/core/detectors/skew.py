"""
Stage skew detector.

Detects when some tasks in a stage take significantly longer than others,
indicating data skew or uneven partitioning.
"""

from __future__ import annotations

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics


class SkewDetector(BaseDetector):
    """Detect data skew by comparing max vs median task duration."""

    name = "skew"
    description = "Detects data skew causing uneven task distribution"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings: list[Finding] = []

        for stage in metrics.stages:
            # Skip stages with few tasks (not meaningful)
            if stage.num_tasks < 10:
                continue

            # Skip stages where median is 0 (avoid division by zero)
            if stage.task_duration_median_ms == 0:
                continue

            ratio = stage.task_duration_max_ms / stage.task_duration_median_ms

            if ratio >= self.thresholds.skew_ratio:
                severity = Severity.CRITICAL if ratio > self.thresholds.skew_ratio * 2 else Severity.WARNING

                findings.append(
                    Finding(
                        id=f"skew-stage-{stage.stage_id}",
                        detector=self.name,
                        title=f"Data skew detected in stage {stage.stage_id}",
                        severity=severity,
                        stage_ids=[stage.stage_id],
                        description=(
                            f"Stage {stage.stage_id} ({stage.stage_name}) has significant task duration skew. "
                            f"Max task took {stage.task_duration_max_ms}ms while median was {stage.task_duration_median_ms}ms "
                            f"(ratio: {ratio:.1f}x). This typically indicates data skew where some partitions "
                            f"have much more data than others."
                        ),
                        metrics={
                            "max_task_duration_ms": stage.task_duration_max_ms,
                            "median_task_duration_ms": stage.task_duration_median_ms,
                            "p90_task_duration_ms": stage.task_duration_p90_ms,
                            "p99_task_duration_ms": stage.task_duration_p99_ms,
                            "skew_ratio": round(ratio, 2),
                            "num_tasks": stage.num_tasks,
                        },
                        mitigation_tags=[
                            MitigationTag.SALTING,
                            MitigationTag.REPARTITION,
                            MitigationTag.BROADCAST_JOIN,
                        ],
                        mitigation_hint=(
                            "Consider salting skewed keys, repartitioning data, "
                            "or using broadcast joins for small tables."
                        ),
                    )
                )

        return findings
