"""Tests for event log parser."""

import json
import tempfile
from pathlib import Path

import pytest

from spark_map.core.parser import parse_eventlog


@pytest.fixture
def sample_eventlog():
    """Create a minimal sample event log for testing."""
    events = [
        {
            "Event": "SparkListenerApplicationStart",
            "App ID": "app-123",
            "App Name": "Test App",
            "Timestamp": 1000000,
        },
        {
            "Event": "SparkListenerStageSubmitted",
            "Stage Info": {
                "Stage ID": 0,
                "Stage Name": "map at test.py:10",
                "Number of Tasks": 4,
            },
            "Timestamp": 1001000,
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 0,
            "Task Info": {
                "Task ID": 0,
                "Executor ID": "1",
                "Host": "worker-1",
                "Launch Time": 1001100,
                "Finish Time": 1001500,
                "Failed": False,
            },
            "Task Metrics": {
                "Executor Run Time": 350,
                "Executor CPU Time": 300000000,
                "JVM GC Time": 10,
                "Result Size": 1000,
                "Memory Bytes Spilled": 0,
                "Disk Bytes Spilled": 0,
                "Input Metrics": {"Bytes Read": 10000, "Records Read": 100},
                "Output Metrics": {"Bytes Written": 0, "Records Written": 0},
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 0,
                    "Local Bytes Read": 0,
                    "Total Records Read": 0,
                    "Fetch Wait Time": 0,
                },
                "Shuffle Write Metrics": {
                    "Shuffle Bytes Written": 5000,
                    "Shuffle Records Written": 50,
                    "Shuffle Write Time": 1000000,
                },
            },
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 0,
            "Task Info": {
                "Task ID": 1,
                "Executor ID": "1",
                "Host": "worker-1",
                "Launch Time": 1001100,
                "Finish Time": 1001600,
                "Failed": False,
            },
            "Task Metrics": {
                "Executor Run Time": 450,
                "Input Metrics": {"Bytes Read": 12000, "Records Read": 120},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 6000},
            },
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {"Stage ID": 0, "Number of Failed Tasks": 0},
            "Timestamp": 1002000,
        },
        {
            "Event": "SparkListenerApplicationEnd",
            "Timestamp": 1010000,
        },
    ]

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
        return Path(f.name)


def test_parse_eventlog(sample_eventlog):
    """Test basic parsing of event log."""
    metrics = parse_eventlog(sample_eventlog)

    assert metrics.app_id == "app-123"
    assert metrics.app_name == "Test App"
    assert metrics.total_duration_ms == 1010000 - 1000000
    assert metrics.num_stages == 1
    assert metrics.num_tasks == 2


def test_parse_stage_metrics(sample_eventlog):
    """Test stage metrics extraction."""
    metrics = parse_eventlog(sample_eventlog)

    assert len(metrics.stages) == 1
    stage = metrics.stages[0]

    assert stage.stage_id == 0
    assert stage.num_tasks == 2
    assert stage.input_bytes == 10000 + 12000
    assert stage.shuffle_write_bytes == 5000 + 6000


def test_parse_task_duration_stats(sample_eventlog):
    """Test task duration percentile calculations."""
    metrics = parse_eventlog(sample_eventlog)
    stage = metrics.stages[0]

    # Task 0: 400ms, Task 1: 500ms
    assert stage.task_duration_min_ms == 400
    assert stage.task_duration_max_ms == 500
    # Median of [400, 500] = 450
    assert stage.task_duration_median_ms == 450


def test_parse_empty_eventlog():
    """Test handling of empty event log."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("")
        path = Path(f.name)

    metrics = parse_eventlog(path)
    assert metrics.app_id == ""
    assert metrics.num_stages == 0


def test_parse_malformed_lines():
    """Test that malformed JSON lines are skipped."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write('{"Event": "SparkListenerApplicationStart", "App ID": "test"}\n')
        f.write("this is not json\n")
        f.write('{"Event": "SparkListenerApplicationEnd"}\n')
        path = Path(f.name)

    # Should not raise, should skip bad line
    metrics = parse_eventlog(path)
    assert metrics.app_id == "test"
