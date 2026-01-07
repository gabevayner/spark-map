"""
I/O bottleneck detector.

Detects when I/O operations dominate stage execution time.
"""

from __future__ import annotations

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics


class IOBottleneckDetector(BaseDetector):
    """Detect I/O bottlenecks where read time dominates execution."""

    name = "io"
    description = "Detects I/O-bound stages"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings: list[Finding] = []

        for stage in metrics.stages:
            # Skip stages with no duration
            if stage.duration_ms == 0:
                continue

            # Check shuffle read wait time as percentage of stage time
            # Note: This is imperfect since shuffle read time is per-task but we're looking at stage level
            if stage.shuffle_read_bytes > 0:
                # Estimate if shuffle is likely the bottleneck
                # High shuffle read with long stage duration suggests I/O bound
                shuffle_per_task_mb = (stage.shuffle_read_bytes / stage.num_tasks) / (1024 * 1024) if stage.num_tasks > 0 else 0

                # If reading > 100MB per task and stage is slow, likely I/O bound
                if shuffle_per_task_mb > 100 and stage.task_duration_median_ms > 10000:
                    findings.append(
                        Finding(
                            id=f"io-shuffle-stage-{stage.stage_id}",
                            detector=self.name,
                            title=f"Shuffle-bound stage {stage.stage_id}",
                            severity=Severity.WARNING,
                            stage_ids=[stage.stage_id],
                            description=(
                                f"Stage {stage.stage_id} ({stage.stage_name}) reads "
                                f"{shuffle_per_task_mb:.1f} MB shuffle data per task on average. "
                                f"High shuffle read volume can cause network I/O bottlenecks."
                            ),
                            metrics={
                                "shuffle_read_bytes": stage.shuffle_read_bytes,
                                "shuffle_per_task_mb": round(shuffle_per_task_mb, 2),
                                "num_tasks": stage.num_tasks,
                                "median_task_duration_ms": stage.task_duration_median_ms,
                            },
                            mitigation_tags=[
                                MitigationTag.BROADCAST_JOIN,
                                MitigationTag.OPTIMIZE_SHUFFLE,
                                MitigationTag.ENABLE_AQE,
                            ],
                            mitigation_hint=(
                                "Consider broadcasting smaller tables to avoid shuffle, "
                                "or using more partitions to reduce per-task shuffle size."
                            ),
                        )
                    )

            # Check input I/O
            if stage.input_bytes > 0:
                input_per_task_mb = (stage.input_bytes / stage.num_tasks) / (1024 * 1024) if stage.num_tasks > 0 else 0

                # If reading > 500MB per task from input, may be I/O bound
                if input_per_task_mb > 500 and stage.task_duration_median_ms > 30000:
                    findings.append(
                        Finding(
                            id=f"io-input-stage-{stage.stage_id}",
                            detector=self.name,
                            title=f"Input I/O bottleneck in stage {stage.stage_id}",
                            severity=Severity.INFO,
                            stage_ids=[stage.stage_id],
                            description=(
                                f"Stage {stage.stage_id} ({stage.stage_name}) reads "
                                f"{input_per_task_mb:.1f} MB input data per task. "
                                f"Large input per task may indicate I/O-bound processing."
                            ),
                            metrics={
                                "input_bytes": stage.input_bytes,
                                "input_per_task_mb": round(input_per_task_mb, 2),
                                "num_tasks": stage.num_tasks,
                            },
                            mitigation_tags=[
                                MitigationTag.REPARTITION,
                                MitigationTag.CHECK_DATA_SOURCE,
                                MitigationTag.FILTER_EARLY,
                            ],
                            mitigation_hint=(
                                "Consider repartitioning input data, using predicate pushdown, "
                                "or filtering earlier in the pipeline."
                            ),
                        )
                    )

        return findings
