"""
Spark event log parser.

Handles parsing of Spark event log JSON files and extraction of metrics.

SCALING APPROACH:
- We do NOT load all events/tasks into memory
- We stream through the file once, aggregating metrics on the fly
- For task duration percentiles, we use reservoir sampling
- Memory usage is O(num_stages), not O(num_tasks)

This allows analyzing logs with millions of tasks without running out of memory.
"""

from __future__ import annotations

import json
import random
import statistics
from pathlib import Path
from typing import Any, Iterator

from spark_map.models.schemas import (
    ShuffleMetrics,
    SparkMetrics,
    SpillMetrics,
    StageMetrics,
    TaskMetrics,
)


def parse_eventlog(path: Path, streaming: bool = True) -> SparkMetrics:
    """
    Parse a Spark event log and extract metrics.

    Args:
        path: Path to the event log JSON file.
        streaming: If True (default), use memory-efficient streaming parser.
                   If False, load all events into memory (only for small logs/testing).

    Returns:
        SparkMetrics containing all extracted metrics.

    Note:
        Spark event logs are newline-delimited JSON (one event per line).
        For large logs (millions of tasks), streaming=True is required.

    Memory Usage:
        - streaming=True: O(num_stages * RESERVOIR_SIZE) ≈ few MB
        - streaming=False: O(num_tasks) - can be GB for large jobs
    """
    if streaming:
        return _extract_metrics_streaming(path)
    else:
        events = list(_read_events_streaming(path))
        return _extract_metrics_legacy(events)


def _read_events_streaming(path: Path) -> Iterator[dict[str, Any]]:
    """
    Stream events from a Spark event log file one at a time.

    Memory: O(1) - only one event in memory at a time.
    """
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # Skip malformed lines
                    continue


def _extract_metrics_streaming(path: Path) -> SparkMetrics:
    """
    Extract metrics with O(num_stages) memory, not O(num_tasks).

    For percentiles, we use reservoir sampling - keeps a fixed-size
    random sample of task durations per stage.
    """
    app_id = ""
    app_name = ""
    start_time: int | None = None
    end_time: int | None = None

    # Stage-level aggregators (not individual tasks)
    stages: dict[int, _StageAggregator] = {}
    executors: set[str] = set()

    for event in _read_events_streaming(path):
        event_type = event.get("Event", "")

        if event_type == "SparkListenerApplicationStart":
            app_id = event.get("App ID", "")
            app_name = event.get("App Name", "")
            start_time = event.get("Timestamp")

        elif event_type == "SparkListenerApplicationEnd":
            end_time = event.get("Timestamp")

        elif event_type == "SparkListenerStageSubmitted":
            stage_info = event.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID", 0)
            stages[stage_id] = _StageAggregator(
                stage_id=stage_id,
                stage_name=stage_info.get("Stage Name", ""),
                expected_tasks=stage_info.get("Number of Tasks", 0),
                submission_time_ms=event.get("Timestamp"),
            )

        elif event_type == "SparkListenerStageCompleted":
            stage_info = event.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID", 0)
            if stage_id in stages:
                stages[stage_id].completion_time_ms = event.get("Timestamp")
                stages[stage_id].num_failed_tasks = stage_info.get("Number of Failed Tasks", 0)

        elif event_type == "SparkListenerTaskEnd":
            task_info = event.get("Task Info", {})
            task_metrics = event.get("Task Metrics", {})
            stage_id = event.get("Stage ID", 0)

            executor_id = task_info.get("Executor ID", "")
            executors.add(executor_id)

            # Aggregate into stage - DON'T store individual task
            if stage_id in stages:
                stages[stage_id].add_task(task_info, task_metrics)

        elif event_type == "SparkListenerExecutorAdded":
            executors.add(event.get("Executor ID", ""))

    # Build final metrics from aggregators
    stage_metrics_list = [agg.to_stage_metrics() for agg in stages.values()]
    stage_metrics_list.sort(key=lambda s: s.stage_id)

    total_duration = 0
    if start_time is not None and end_time is not None:
        total_duration = end_time - start_time

    return SparkMetrics(
        app_id=app_id,
        app_name=app_name,
        start_time_ms=start_time,
        end_time_ms=end_time,
        total_duration_ms=total_duration,
        num_stages=len(stages),
        num_completed_stages=sum(1 for s in stage_metrics_list if s.num_failed_tasks == 0),
        num_failed_stages=sum(1 for s in stage_metrics_list if s.num_failed_tasks > 0),
        stages=stage_metrics_list,
        num_tasks=sum(s.num_tasks for s in stage_metrics_list),
        num_completed_tasks=sum(s.num_tasks - s.num_failed_tasks for s in stage_metrics_list),
        num_failed_tasks=sum(s.num_failed_tasks for s in stage_metrics_list),
        num_executors=len(executors),
        executor_ids=list(executors),
        total_input_bytes=sum(s.input_bytes for s in stage_metrics_list),
        total_output_bytes=sum(s.output_bytes for s in stage_metrics_list),
        total_shuffle_read_bytes=sum(s.shuffle_read_bytes for s in stage_metrics_list),
        total_shuffle_write_bytes=sum(s.shuffle_write_bytes for s in stage_metrics_list),
        total_disk_bytes_spilled=sum(s.disk_bytes_spilled for s in stage_metrics_list),
    )


class _StageAggregator:
    """
    Aggregates task metrics for a stage without storing all tasks.

    Uses reservoir sampling for percentile estimation to keep memory bounded.
    With RESERVOIR_SIZE=1000, percentile estimates are typically within 1-2%
    of true values even for millions of tasks.
    """

    RESERVOIR_SIZE = 1000  # Max samples for percentiles - ~8KB per stage

    def __init__(
        self,
        stage_id: int,
        stage_name: str,
        expected_tasks: int,
        submission_time_ms: int | None,
    ):
        self.stage_id = stage_id
        self.stage_name = stage_name
        self.expected_tasks = expected_tasks
        self.submission_time_ms = submission_time_ms
        self.completion_time_ms: int | None = None
        self.num_failed_tasks = 0

        # Running aggregates - O(1) memory
        self.task_count = 0
        self.duration_min = float("inf")
        self.duration_max = 0
        self.duration_sum = 0

        # Reservoir sample for percentiles - O(RESERVOIR_SIZE) memory
        self._duration_reservoir: list[int] = []
        self._reservoir_count = 0

        # Sum aggregates - O(1) memory
        self.input_bytes = 0
        self.input_records = 0
        self.output_bytes = 0
        self.output_records = 0
        self.shuffle_read_bytes = 0
        self.shuffle_write_bytes = 0
        self.memory_bytes_spilled = 0
        self.disk_bytes_spilled = 0

    def add_task(self, task_info: dict[str, Any], task_metrics: dict[str, Any]) -> None:
        """
        Add a task's metrics to the aggregation.

        This does NOT store the task - it updates running aggregates.
        """
        self.task_count += 1

        # Duration tracking
        launch = task_info.get("Launch Time", 0)
        finish = task_info.get("Finish Time", 0)
        duration = finish - launch if finish > launch else 0

        self.duration_min = min(self.duration_min, duration)
        self.duration_max = max(self.duration_max, duration)
        self.duration_sum += duration

        # Reservoir sampling for percentiles (Algorithm R)
        # Keeps a uniform random sample as we stream through tasks
        self._reservoir_count += 1
        if len(self._duration_reservoir) < self.RESERVOIR_SIZE:
            self._duration_reservoir.append(duration)
        else:
            # Replace element j with probability RESERVOIR_SIZE / count
            j = random.randint(0, self._reservoir_count - 1)
            if j < self.RESERVOIR_SIZE:
                self._duration_reservoir[j] = duration

        # Sum aggregates
        input_m = task_metrics.get("Input Metrics", {})
        output_m = task_metrics.get("Output Metrics", {})
        shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
        shuffle_write = task_metrics.get("Shuffle Write Metrics", {})

        self.input_bytes += input_m.get("Bytes Read", 0)
        self.input_records += input_m.get("Records Read", 0)
        self.output_bytes += output_m.get("Bytes Written", 0)
        self.output_records += output_m.get("Records Written", 0)

        self.shuffle_read_bytes += shuffle_read.get("Remote Bytes Read", 0) + shuffle_read.get(
            "Local Bytes Read", 0
        )
        self.shuffle_write_bytes += shuffle_write.get("Shuffle Bytes Written", 0)

        self.memory_bytes_spilled += task_metrics.get("Memory Bytes Spilled", 0)
        self.disk_bytes_spilled += task_metrics.get("Disk Bytes Spilled", 0)

        if task_info.get("Failed", False):
            self.num_failed_tasks += 1

    def to_stage_metrics(self) -> StageMetrics:
        """Convert aggregated data to StageMetrics."""
        # Calculate percentiles from reservoir sample
        if self._duration_reservoir:
            sorted_durations = sorted(self._duration_reservoir)
            duration_median = int(_percentile(sorted_durations, 50))
            duration_p75 = int(_percentile(sorted_durations, 75))
            duration_p90 = int(_percentile(sorted_durations, 90))
            duration_p99 = int(_percentile(sorted_durations, 99))
        else:
            duration_median = duration_p75 = duration_p90 = duration_p99 = 0

        stage_duration = 0
        if self.submission_time_ms and self.completion_time_ms:
            stage_duration = self.completion_time_ms - self.submission_time_ms

        return StageMetrics(
            stage_id=self.stage_id,
            stage_name=self.stage_name,
            num_tasks=self.task_count,
            submission_time_ms=self.submission_time_ms,
            completion_time_ms=self.completion_time_ms,
            duration_ms=stage_duration,
            task_duration_min_ms=int(self.duration_min) if self.duration_min != float("inf") else 0,
            task_duration_max_ms=self.duration_max,
            task_duration_median_ms=duration_median,
            task_duration_p75_ms=duration_p75,
            task_duration_p90_ms=duration_p90,
            task_duration_p99_ms=duration_p99,
            input_bytes=self.input_bytes,
            input_records=self.input_records,
            output_bytes=self.output_bytes,
            output_records=self.output_records,
            shuffle_read_bytes=self.shuffle_read_bytes,
            shuffle_write_bytes=self.shuffle_write_bytes,
            memory_bytes_spilled=self.memory_bytes_spilled,
            disk_bytes_spilled=self.disk_bytes_spilled,
            num_failed_tasks=self.num_failed_tasks,
        )


# Legacy non-streaming parser (kept for testing small logs)
def _extract_metrics_legacy(events: list[dict[str, Any]]) -> SparkMetrics:
    """Extract SparkMetrics from parsed events (loads all into memory)."""
    app_id = ""
    app_name = ""
    start_time: int | None = None
    end_time: int | None = None

    stages: dict[int, dict[str, Any]] = {}
    tasks: dict[int, list[TaskMetrics]] = {}
    executors: set[str] = set()

    for event in events:
        event_type = event.get("Event", "")

        if event_type == "SparkListenerApplicationStart":
            app_id = event.get("App ID", "")
            app_name = event.get("App Name", "")
            start_time = event.get("Timestamp")

        elif event_type == "SparkListenerApplicationEnd":
            end_time = event.get("Timestamp")

        elif event_type == "SparkListenerStageSubmitted":
            stage_info = event.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID", 0)
            stages[stage_id] = {
                "stage_id": stage_id,
                "stage_name": stage_info.get("Stage Name", ""),
                "num_tasks": stage_info.get("Number of Tasks", 0),
                "submission_time_ms": event.get("Timestamp"),
            }

        elif event_type == "SparkListenerStageCompleted":
            stage_info = event.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID", 0)
            if stage_id in stages:
                stages[stage_id]["completion_time_ms"] = event.get("Timestamp")
                stages[stage_id]["num_failed_tasks"] = stage_info.get("Number of Failed Tasks", 0)

        elif event_type == "SparkListenerTaskEnd":
            task_info = event.get("Task Info", {})
            task_metrics = event.get("Task Metrics", {})
            stage_id = event.get("Stage ID", 0)

            executor_id = task_info.get("Executor ID", "")
            executors.add(executor_id)

            task = _parse_task_metrics(event, task_info, task_metrics, stage_id)

            if stage_id not in tasks:
                tasks[stage_id] = []
            tasks[stage_id].append(task)

        elif event_type == "SparkListenerExecutorAdded":
            executors.add(event.get("Executor ID", ""))

    stage_metrics_list = []
    for stage_id, stage_data in stages.items():
        stage_tasks = tasks.get(stage_id, [])
        stage_metrics = _build_stage_metrics(stage_data, stage_tasks)
        stage_metrics_list.append(stage_metrics)

    stage_metrics_list.sort(key=lambda s: s.stage_id)

    total_duration = 0
    if start_time is not None and end_time is not None:
        total_duration = end_time - start_time

    total_tasks = sum(len(t) for t in tasks.values())
    total_failed_tasks = sum(s.num_failed_tasks for s in stage_metrics_list)
    total_failed_stages = sum(1 for s in stages.values() if s.get("num_failed_tasks", 0) > 0)

    return SparkMetrics(
        app_id=app_id,
        app_name=app_name,
        start_time_ms=start_time,
        end_time_ms=end_time,
        total_duration_ms=total_duration,
        num_stages=len(stages),
        num_completed_stages=len(stages) - total_failed_stages,
        num_failed_stages=total_failed_stages,
        stages=stage_metrics_list,
        num_tasks=total_tasks,
        num_completed_tasks=total_tasks - total_failed_tasks,
        num_failed_tasks=total_failed_tasks,
        num_executors=len(executors),
        executor_ids=list(executors),
        total_input_bytes=sum(s.input_bytes for s in stage_metrics_list),
        total_output_bytes=sum(s.output_bytes for s in stage_metrics_list),
        total_shuffle_read_bytes=sum(s.shuffle_read_bytes for s in stage_metrics_list),
        total_shuffle_write_bytes=sum(s.shuffle_write_bytes for s in stage_metrics_list),
        total_disk_bytes_spilled=sum(s.disk_bytes_spilled for s in stage_metrics_list),
    )


def _parse_task_metrics(
    event: dict[str, Any],
    task_info: dict[str, Any],
    task_metrics: dict[str, Any],
    stage_id: int,
) -> TaskMetrics:
    """Parse a single task's metrics from event data."""
    shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
    shuffle_write = task_metrics.get("Shuffle Write Metrics", {})

    shuffle = ShuffleMetrics(
        read_bytes=shuffle_read.get("Remote Bytes Read", 0)
        + shuffle_read.get("Local Bytes Read", 0),
        read_records=shuffle_read.get("Total Records Read", 0),
        write_bytes=shuffle_write.get("Shuffle Bytes Written", 0),
        write_records=shuffle_write.get("Shuffle Records Written", 0),
        read_time_ms=shuffle_read.get("Fetch Wait Time", 0),
        write_time_ms=shuffle_write.get("Shuffle Write Time", 0) // 1_000_000,
    )

    spill = SpillMetrics(
        memory_bytes_spilled=task_metrics.get("Memory Bytes Spilled", 0),
        disk_bytes_spilled=task_metrics.get("Disk Bytes Spilled", 0),
    )

    input_metrics = task_metrics.get("Input Metrics", {})
    output_metrics = task_metrics.get("Output Metrics", {})

    launch_time = task_info.get("Launch Time", 0)
    finish_time = task_info.get("Finish Time", 0)

    return TaskMetrics(
        task_id=task_info.get("Task ID", 0),
        stage_id=stage_id,
        executor_id=task_info.get("Executor ID", ""),
        host=task_info.get("Host", ""),
        launch_time_ms=launch_time,
        finish_time_ms=finish_time,
        duration_ms=finish_time - launch_time if finish_time > launch_time else 0,
        executor_run_time_ms=task_metrics.get("Executor Run Time", 0),
        executor_cpu_time_ns=task_metrics.get("Executor CPU Time", 0),
        gc_time_ms=task_metrics.get("JVM GC Time", 0),
        input_bytes=input_metrics.get("Bytes Read", 0),
        input_records=input_metrics.get("Records Read", 0),
        output_bytes=output_metrics.get("Bytes Written", 0),
        output_records=output_metrics.get("Records Written", 0),
        shuffle=shuffle,
        spill=spill,
        result_size_bytes=task_metrics.get("Result Size", 0),
        failed=task_info.get("Failed", False),
        failure_reason=event.get("Task End Reason", {}).get("Reason"),
    )


def _build_stage_metrics(stage_data: dict[str, Any], tasks: list[TaskMetrics]) -> StageMetrics:
    """Build StageMetrics from stage data and associated tasks."""
    durations = [t.duration_ms for t in tasks]
    if durations:
        durations_sorted = sorted(durations)
        duration_min = durations_sorted[0]
        duration_max = durations_sorted[-1]
        duration_median = int(statistics.median(durations))
        duration_p75 = int(_percentile(durations_sorted, 75))
        duration_p90 = int(_percentile(durations_sorted, 90))
        duration_p99 = int(_percentile(durations_sorted, 99))
    else:
        duration_min = duration_max = duration_median = 0
        duration_p75 = duration_p90 = duration_p99 = 0

    submission = stage_data.get("submission_time_ms")
    completion = stage_data.get("completion_time_ms")
    stage_duration = 0
    if submission is not None and completion is not None:
        stage_duration = completion - submission

    return StageMetrics(
        stage_id=stage_data["stage_id"],
        stage_name=stage_data.get("stage_name", ""),
        num_tasks=len(tasks),
        submission_time_ms=submission,
        completion_time_ms=completion,
        duration_ms=stage_duration,
        task_duration_min_ms=duration_min,
        task_duration_max_ms=duration_max,
        task_duration_median_ms=duration_median,
        task_duration_p75_ms=duration_p75,
        task_duration_p90_ms=duration_p90,
        task_duration_p99_ms=duration_p99,
        input_bytes=sum(t.input_bytes for t in tasks),
        input_records=sum(t.input_records for t in tasks),
        output_bytes=sum(t.output_bytes for t in tasks),
        output_records=sum(t.output_records for t in tasks),
        shuffle_read_bytes=sum(t.shuffle.read_bytes for t in tasks),
        shuffle_write_bytes=sum(t.shuffle.write_bytes for t in tasks),
        memory_bytes_spilled=sum(t.spill.memory_bytes_spilled for t in tasks),
        disk_bytes_spilled=sum(t.spill.disk_bytes_spilled for t in tasks),
        num_failed_tasks=sum(1 for t in tasks if t.failed),
    )


def _percentile(sorted_data: list[int], p: float) -> float:
    """Calculate percentile from sorted data using linear interpolation."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    if f == c:
        return float(sorted_data[f])
    return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)
