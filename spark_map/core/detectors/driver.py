"""
Driver bottleneck detector.

Detects when the driver is the limiting factor due to large results
or scheduling delays.
"""

from __future__ import annotations

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics

MB = 1024 * 1024


class DriverBottleneckDetector(BaseDetector):
    """Detect driver bottlenecks from result size and scheduling patterns."""

    name = "driver"
    description = "Detects driver-side bottlenecks"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings: list[Finding] = []

        # Check for stages with large scheduling delays
        # This is approximated by looking at stage duration vs sum of task times
        for stage in metrics.stages:
            if stage.num_tasks == 0:
                continue

            # Estimate scheduling delay
            # If stage duration >> sum of parallel task durations, there's scheduling overhead
            # This is a rough heuristic since we don't have exact scheduler timing
            expected_parallel_time = stage.task_duration_max_ms  # Ideal case: all tasks run in parallel
            actual_time = stage.duration_ms

            if expected_parallel_time > 0 and actual_time > 0:
                scheduling_overhead_ratio = actual_time / expected_parallel_time

                # If stage takes much longer than the slowest task, scheduling is an issue
                if scheduling_overhead_ratio > 5 and actual_time > self.thresholds.max_scheduling_delay_ms:
                    findings.append(
                        Finding(
                            id=f"driver-scheduling-stage-{stage.stage_id}",
                            detector=self.name,
                            title=f"Scheduling delay in stage {stage.stage_id}",
                            severity=Severity.WARNING,
                            stage_ids=[stage.stage_id],
                            description=(
                                f"Stage {stage.stage_id} ({stage.stage_name}) took {actual_time}ms "
                                f"but the longest task was only {expected_parallel_time}ms "
                                f"(ratio: {scheduling_overhead_ratio:.1f}x). This suggests tasks weren't "
                                f"running in parallel, possibly due to insufficient executors or driver scheduling delays."
                            ),
                            metrics={
                                "stage_duration_ms": actual_time,
                                "max_task_duration_ms": expected_parallel_time,
                                "scheduling_overhead_ratio": round(scheduling_overhead_ratio, 2),
                                "num_tasks": stage.num_tasks,
                                "num_executors": metrics.num_executors,
                            },
                            mitigation_tags=[
                                MitigationTag.INCREASE_PARALLELISM,
                                MitigationTag.COALESCE,
                            ],
                            mitigation_hint=(
                                "Consider adding more executors to increase parallelism, "
                                "or reducing task count if executors are bottlenecked."
                            ),
                        )
                    )

        # Check for potential collect() abuse by looking at output patterns
        # This is heuristic - large output at the end of job may indicate collect()
        if metrics.stages:
            last_stages = sorted(metrics.stages, key=lambda s: s.stage_id)[-3:]
            for stage in last_stages:
                output_mb = stage.output_bytes / MB
                if output_mb > self.thresholds.max_result_size_mb:
                    findings.append(
                        Finding(
                            id=f"driver-large-result-stage-{stage.stage_id}",
                            detector=self.name,
                            title=f"Large result in stage {stage.stage_id}",
                            severity=Severity.WARNING,
                            stage_ids=[stage.stage_id],
                            description=(
                                f"Stage {stage.stage_id} ({stage.stage_name}) outputs {output_mb:.1f} MB. "
                                f"If this data is being collected to the driver, it may cause memory pressure "
                                f"or OOM errors. Consider writing results to storage instead of collecting."
                            ),
                            metrics={
                                "output_bytes": stage.output_bytes,
                                "output_mb": round(output_mb, 2),
                            },
                            mitigation_tags=[
                                MitigationTag.REDUCE_COLLECT,
                            ],
                            mitigation_hint=(
                                "Avoid collect() on large datasets. Use .write() to save results to storage, "
                                "or use .take(n) to limit collected rows."
                            ),
                        )
                    )

        return findings
