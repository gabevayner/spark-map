"""Tests for bottleneck detectors."""

import pytest

from spark_map.core.detectors.skew import SkewDetector
from spark_map.core.detectors.shuffle import ShuffleExplosionDetector
from spark_map.core.detectors.spill import SpillDetector
from spark_map.core.findings import Severity
from spark_map.models.schemas import SparkMetrics, StageMetrics, ThresholdConfig


@pytest.fixture
def thresholds():
    return ThresholdConfig()


class TestSkewDetector:
    def test_detects_skew(self, thresholds):
        """Test that skew is detected when max >> median."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="test stage",
                    num_tasks=100,
                    task_duration_min_ms=100,
                    task_duration_median_ms=200,
                    task_duration_max_ms=5000,  # 25x median = severe skew
                    task_duration_p75_ms=300,
                    task_duration_p90_ms=400,
                    task_duration_p99_ms=2000,
                )
            ],
        )

        detector = SkewDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        assert len(findings) == 1
        assert findings[0].severity == Severity.CRITICAL
        assert "skew" in findings[0].title.lower()
        assert findings[0].stage_ids == [0]

    def test_no_skew_when_balanced(self, thresholds):
        """Test that no skew is detected when tasks are balanced."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="balanced stage",
                    num_tasks=100,
                    task_duration_min_ms=90,
                    task_duration_median_ms=100,
                    task_duration_max_ms=150,  # 1.5x median = normal
                    task_duration_p75_ms=120,
                    task_duration_p90_ms=130,
                    task_duration_p99_ms=145,
                )
            ],
        )

        detector = SkewDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        assert len(findings) == 0

    def test_skips_small_stages(self, thresholds):
        """Test that stages with few tasks are skipped."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="small stage",
                    num_tasks=5,  # Too few tasks
                    task_duration_median_ms=100,
                    task_duration_max_ms=10000,  # Would be skew if checked
                )
            ],
        )

        detector = SkewDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        assert len(findings) == 0


class TestShuffleExplosionDetector:
    def test_detects_shuffle_explosion(self, thresholds):
        """Test that shuffle explosion is detected."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="exploding stage",
                    num_tasks=100,
                    input_bytes=1_000_000,  # 1 MB input
                    shuffle_write_bytes=100_000_000,  # 100 MB shuffle = 100x
                )
            ],
        )

        detector = ShuffleExplosionDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        assert len(findings) >= 1
        assert any("shuffle" in f.title.lower() for f in findings)

    def test_no_explosion_when_normal(self, thresholds):
        """Test no finding when shuffle is proportional."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="normal stage",
                    num_tasks=100,
                    input_bytes=100_000_000,  # 100 MB input
                    shuffle_write_bytes=50_000_000,  # 50 MB shuffle = 0.5x
                )
            ],
        )

        detector = ShuffleExplosionDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        # Should not flag per-stage explosion
        stage_findings = [f for f in findings if "stage-0" in f.id]
        assert len(stage_findings) == 0


class TestSpillDetector:
    def test_detects_spill(self, thresholds):
        """Test that disk spill is detected."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="spilling stage",
                    num_tasks=100,
                    disk_bytes_spilled=500 * 1024 * 1024,  # 500 MB
                    memory_bytes_spilled=1000 * 1024 * 1024,
                )
            ],
        )

        detector = SpillDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        assert len(findings) >= 1
        assert any("spill" in f.title.lower() for f in findings)

    def test_no_spill_when_low(self, thresholds):
        """Test no finding when spill is minimal."""
        metrics = SparkMetrics(
            app_id="test-app",
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="clean stage",
                    num_tasks=100,
                    disk_bytes_spilled=10 * 1024 * 1024,  # 10 MB - below threshold
                )
            ],
        )

        detector = SpillDetector(thresholds=thresholds)
        findings = detector.detect(metrics)

        assert len(findings) == 0
