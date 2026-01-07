"""
Spill detector.

Detects memory pressure indicated by spilling data to disk.
"""

from __future__ import annotations

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics

MB = 1024 * 1024


class SpillDetector(BaseDetector):
    """Detect memory pressure via spill metrics."""

    name = "spill"
    description = "Detects memory pressure causing disk spill"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings: list[Finding] = []

        # Check per-stage spill
        for stage in metrics.stages:
            disk_spill_mb = stage.disk_bytes_spilled / MB

            if disk_spill_mb >= self.thresholds.min_spill_mb:
                # Severity based on spill amount
                if disk_spill_mb > self.thresholds.min_spill_mb * 10:
                    severity = Severity.CRITICAL
                elif disk_spill_mb > self.thresholds.min_spill_mb * 3:
                    severity = Severity.WARNING
                else:
                    severity = Severity.INFO

                findings.append(
                    Finding(
                        id=f"spill-stage-{stage.stage_id}",
                        detector=self.name,
                        title=f"Disk spill in stage {stage.stage_id}",
                        severity=severity,
                        stage_ids=[stage.stage_id],
                        description=(
                            f"Stage {stage.stage_id} ({stage.stage_name}) spilled {disk_spill_mb:.1f} MB to disk. "
                            f"Memory spill was {stage.memory_bytes_spilled / MB:.1f} MB. "
                            f"This indicates memory pressure and can significantly slow down execution."
                        ),
                        metrics={
                            "disk_bytes_spilled": stage.disk_bytes_spilled,
                            "memory_bytes_spilled": stage.memory_bytes_spilled,
                            "disk_spill_mb": round(disk_spill_mb, 2),
                        },
                        mitigation_tags=[
                            MitigationTag.INCREASE_MEMORY,
                            MitigationTag.REPARTITION,
                            MitigationTag.REDUCE_PARALLELISM,
                        ],
                        mitigation_hint=(
                            "Consider increasing executor memory (spark.executor.memory), "
                            "reducing partition count, or increasing spark.memory.fraction."
                        ),
                    )
                )

        # Check total application spill
        total_spill_mb = metrics.total_disk_bytes_spilled / MB
        if total_spill_mb >= self.thresholds.min_spill_mb * 5:
            findings.append(
                Finding(
                    id="spill-total",
                    detector=self.name,
                    title="High total disk spill across application",
                    severity=Severity.WARNING,
                    stage_ids=[s.stage_id for s in metrics.stages if s.disk_bytes_spilled > 0],
                    description=(
                        f"Application spilled {total_spill_mb:.1f} MB to disk in total across all stages. "
                        f"This represents significant memory pressure that's impacting performance."
                    ),
                    metrics={
                        "total_disk_spill_mb": round(total_spill_mb, 2),
                        "stages_with_spill": sum(1 for s in metrics.stages if s.disk_bytes_spilled > 0),
                    },
                    mitigation_tags=[
                        MitigationTag.INCREASE_MEMORY,
                        MitigationTag.ENABLE_AQE,
                    ],
                    mitigation_hint=(
                        "Consider increasing cluster memory or enabling Adaptive Query Execution (AQE) "
                        "to dynamically optimize execution."
                    ),
                )
            )

        return findings
