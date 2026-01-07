"""
Partition inefficiency detector.

Detects when there are too many small tasks, suggesting over-partitioning.
"""

from __future__ import annotations

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics


class PartitionInefficiencyDetector(BaseDetector):
    """Detect partition inefficiency via task count and runtime analysis."""

    name = "partition"
    description = "Detects partition inefficiency (too many or too few partitions)"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings: list[Finding] = []

        for stage in metrics.stages:
            # Check for too many small tasks (over-partitioning)
            if stage.num_tasks >= self.thresholds.min_tasks_for_inefficiency:
                if stage.task_duration_median_ms <= self.thresholds.max_task_runtime_ms_for_inefficiency:
                    # Calculate overhead ratio
                    # If median task is very short but we have many tasks, scheduling overhead dominates
                    overhead_indicator = stage.num_tasks * stage.task_duration_median_ms

                    findings.append(
                        Finding(
                            id=f"partition-inefficiency-stage-{stage.stage_id}",
                            detector=self.name,
                            title=f"Too many partitions in stage {stage.stage_id}",
                            severity=Severity.WARNING,
                            stage_ids=[stage.stage_id],
                            description=(
                                f"Stage {stage.stage_id} ({stage.stage_name}) has {stage.num_tasks} tasks "
                                f"with median runtime of only {stage.task_duration_median_ms}ms. "
                                f"Tasks this short spend more time in scheduling overhead than actual work. "
                                f"Consider using coalesce() to reduce partition count."
                            ),
                            metrics={
                                "num_tasks": stage.num_tasks,
                                "median_task_duration_ms": stage.task_duration_median_ms,
                                "min_task_duration_ms": stage.task_duration_min_ms,
                                "overhead_indicator": overhead_indicator,
                            },
                            mitigation_tags=[
                                MitigationTag.COALESCE,
                                MitigationTag.REDUCE_PARALLELISM,
                            ],
                            mitigation_hint=(
                                f"Use .coalesce({max(stage.num_tasks // 10, 1)}) to reduce partitions, "
                                "or set spark.sql.shuffle.partitions to a lower value."
                            ),
                        )
                    )

            # Check for too few partitions (under-partitioning)
            # This is harder to detect definitively, but we can flag very long median task times
            if stage.num_tasks < 10 and stage.task_duration_median_ms > 60000:  # > 1 minute
                findings.append(
                    Finding(
                        id=f"under-partitioned-stage-{stage.stage_id}",
                        detector=self.name,
                        title=f"Potentially under-partitioned stage {stage.stage_id}",
                        severity=Severity.INFO,
                        stage_ids=[stage.stage_id],
                        description=(
                            f"Stage {stage.stage_id} ({stage.stage_name}) has only {stage.num_tasks} tasks "
                            f"with median runtime of {stage.task_duration_median_ms / 1000:.1f}s. "
                            f"If you have more executors available, increasing partitions could improve parallelism."
                        ),
                        metrics={
                            "num_tasks": stage.num_tasks,
                            "median_task_duration_ms": stage.task_duration_median_ms,
                            "num_executors": metrics.num_executors,
                        },
                        mitigation_tags=[
                            MitigationTag.REPARTITION,
                            MitigationTag.INCREASE_PARALLELISM,
                        ],
                        mitigation_hint=(
                            f"Consider using .repartition({metrics.num_executors * 2}) "
                            "to increase parallelism."
                        ),
                    )
                )

        return findings
