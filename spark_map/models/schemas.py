"""
Pydantic models for Spark metrics and findings.

These models provide type-safe representations of Spark event log data
and analysis results.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ShuffleMetrics(BaseModel):
    """Shuffle read/write metrics for a task or stage."""

    read_bytes: int = Field(default=0, ge=0, description="Bytes read during shuffle")
    read_records: int = Field(default=0, ge=0, description="Records read during shuffle")
    write_bytes: int = Field(default=0, ge=0, description="Bytes written during shuffle")
    write_records: int = Field(default=0, ge=0, description="Records written during shuffle")
    read_time_ms: int = Field(default=0, ge=0, description="Time spent reading shuffle data")
    write_time_ms: int = Field(default=0, ge=0, description="Time spent writing shuffle data")


class SpillMetrics(BaseModel):
    """Memory spill metrics indicating memory pressure."""

    memory_bytes_spilled: int = Field(default=0, ge=0, description="Bytes spilled to memory")
    disk_bytes_spilled: int = Field(default=0, ge=0, description="Bytes spilled to disk")


class TaskMetrics(BaseModel):
    """Metrics for a single Spark task."""

    task_id: int = Field(description="Unique task identifier")
    stage_id: int = Field(description="Stage this task belongs to")
    executor_id: str = Field(description="Executor that ran this task")
    host: str = Field(description="Host where task executed")

    # Timing
    launch_time_ms: int = Field(description="Task launch timestamp")
    finish_time_ms: int = Field(description="Task finish timestamp")
    duration_ms: int = Field(ge=0, description="Total task duration")
    executor_run_time_ms: int = Field(default=0, ge=0, description="Time spent in executor")
    executor_cpu_time_ns: int = Field(default=0, ge=0, description="CPU time in nanoseconds")
    gc_time_ms: int = Field(default=0, ge=0, description="Time spent in garbage collection")

    # I/O
    input_bytes: int = Field(default=0, ge=0, description="Bytes read from input")
    input_records: int = Field(default=0, ge=0, description="Records read from input")
    output_bytes: int = Field(default=0, ge=0, description="Bytes written to output")
    output_records: int = Field(default=0, ge=0, description="Records written to output")

    # Shuffle and spill
    shuffle: ShuffleMetrics = Field(default_factory=ShuffleMetrics)
    spill: SpillMetrics = Field(default_factory=SpillMetrics)

    # Result
    result_size_bytes: int = Field(default=0, ge=0, description="Size of task result")
    failed: bool = Field(default=False, description="Whether task failed")
    failure_reason: str | None = Field(default=None, description="Failure reason if failed")


class StageMetrics(BaseModel):
    """Aggregated metrics for a Spark stage."""

    stage_id: int = Field(description="Unique stage identifier")
    stage_name: str = Field(default="", description="Human-readable stage name")
    num_tasks: int = Field(ge=0, description="Number of tasks in stage")

    # Timing
    submission_time_ms: int | None = Field(default=None, description="Stage submission timestamp")
    completion_time_ms: int | None = Field(default=None, description="Stage completion timestamp")
    duration_ms: int = Field(default=0, ge=0, description="Total stage duration")

    # Task duration statistics
    task_duration_min_ms: int = Field(default=0, ge=0, description="Minimum task duration")
    task_duration_max_ms: int = Field(default=0, ge=0, description="Maximum task duration")
    task_duration_median_ms: int = Field(default=0, ge=0, description="Median task duration")
    task_duration_p75_ms: int = Field(default=0, ge=0, description="75th percentile task duration")
    task_duration_p90_ms: int = Field(default=0, ge=0, description="90th percentile task duration")
    task_duration_p99_ms: int = Field(default=0, ge=0, description="99th percentile task duration")

    # Aggregated I/O
    input_bytes: int = Field(default=0, ge=0, description="Total bytes read from input")
    input_records: int = Field(default=0, ge=0, description="Total records read from input")
    output_bytes: int = Field(default=0, ge=0, description="Total bytes written to output")
    output_records: int = Field(default=0, ge=0, description="Total records written to output")

    # Aggregated shuffle
    shuffle_read_bytes: int = Field(default=0, ge=0, description="Total shuffle read bytes")
    shuffle_write_bytes: int = Field(default=0, ge=0, description="Total shuffle write bytes")

    # Aggregated spill
    memory_bytes_spilled: int = Field(default=0, ge=0, description="Total memory spill")
    disk_bytes_spilled: int = Field(default=0, ge=0, description="Total disk spill")

    # Failure info
    num_failed_tasks: int = Field(default=0, ge=0, description="Number of failed tasks")


class SparkMetrics(BaseModel):
    """Top-level metrics for an entire Spark application."""

    app_id: str = Field(description="Spark application ID")
    app_name: str = Field(default="", description="Application name")

    # Timing
    start_time_ms: int | None = Field(default=None, description="Application start timestamp")
    end_time_ms: int | None = Field(default=None, description="Application end timestamp")
    total_duration_ms: int = Field(default=0, ge=0, description="Total application duration")

    # Stages
    num_stages: int = Field(default=0, ge=0, description="Total number of stages")
    num_completed_stages: int = Field(default=0, ge=0, description="Completed stages")
    num_failed_stages: int = Field(default=0, ge=0, description="Failed stages")
    stages: list[StageMetrics] = Field(default_factory=list, description="Per-stage metrics")

    # Tasks
    num_tasks: int = Field(default=0, ge=0, description="Total number of tasks")
    num_completed_tasks: int = Field(default=0, ge=0, description="Completed tasks")
    num_failed_tasks: int = Field(default=0, ge=0, description="Failed tasks")

    # Executors
    num_executors: int = Field(default=0, ge=0, description="Number of executors used")
    executor_ids: list[str] = Field(default_factory=list, description="List of executor IDs")

    # Aggregated I/O
    total_input_bytes: int = Field(default=0, ge=0, description="Total input bytes")
    total_output_bytes: int = Field(default=0, ge=0, description="Total output bytes")
    total_shuffle_read_bytes: int = Field(default=0, ge=0, description="Total shuffle read")
    total_shuffle_write_bytes: int = Field(default=0, ge=0, description="Total shuffle write")
    total_disk_bytes_spilled: int = Field(default=0, ge=0, description="Total disk spill")


class ThresholdConfig(BaseModel):
    """Configurable thresholds for bottleneck detection."""

    # Skew detection
    skew_ratio: float = Field(
        default=10.0,
        gt=1.0,
        description="Max/median task duration ratio to flag as skew",
    )

    # Shuffle explosion
    shuffle_explosion_ratio: float = Field(
        default=5.0,
        gt=1.0,
        description="Shuffle/input ratio to flag as explosion",
    )

    # Spill thresholds
    min_spill_mb: int = Field(
        default=100,
        ge=0,
        description="Minimum MB spilled to flag as issue",
    )

    # Partition inefficiency
    min_tasks_for_inefficiency: int = Field(
        default=200,
        ge=1,
        description="Minimum tasks to check for partition inefficiency",
    )
    max_task_runtime_ms_for_inefficiency: int = Field(
        default=100,
        ge=1,
        description="Task runtime below this suggests too many partitions",
    )

    # I/O bottleneck
    io_dominant_ratio: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Ratio of I/O time to total time to flag as I/O bottleneck",
    )

    # Driver bottleneck
    max_result_size_mb: int = Field(
        default=50,
        ge=0,
        description="Result size above this suggests driver bottleneck",
    )
    max_scheduling_delay_ms: int = Field(
        default=1000,
        ge=0,
        description="Scheduling delay above this suggests driver bottleneck",
    )
