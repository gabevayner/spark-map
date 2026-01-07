"""
Bottleneck detectors for Spark performance analysis.

Each detector examines SparkMetrics and produces Finding objects
for any issues it detects.
"""

from spark_map.core.detectors.base import BaseDetector
from spark_map.core.detectors.driver import DriverBottleneckDetector
from spark_map.core.detectors.io import IOBottleneckDetector
from spark_map.core.detectors.partition import PartitionInefficiencyDetector
from spark_map.core.detectors.shuffle import ShuffleExplosionDetector
from spark_map.core.detectors.skew import SkewDetector
from spark_map.core.detectors.spill import SpillDetector


def get_all_detectors() -> list[type[BaseDetector]]:
    """Return all available detector classes."""
    return [
        SkewDetector,
        ShuffleExplosionDetector,
        SpillDetector,
        PartitionInefficiencyDetector,
        IOBottleneckDetector,
        DriverBottleneckDetector,
    ]


__all__ = [
    "BaseDetector",
    "SkewDetector",
    "ShuffleExplosionDetector",
    "SpillDetector",
    "PartitionInefficiencyDetector",
    "IOBottleneckDetector",
    "DriverBottleneckDetector",
    "get_all_detectors",
]
