"""
Base detector interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from spark_map.core.findings import Finding
from spark_map.models.schemas import SparkMetrics, ThresholdConfig


class BaseDetector(ABC):
    """
    Abstract base class for all bottleneck detectors.

    Each detector examines SparkMetrics and returns a list of Finding
    objects for any issues detected.
    """

    name: str = "base"
    description: str = "Base detector"

    def __init__(self, thresholds: ThresholdConfig) -> None:
        self.thresholds = thresholds

    @abstractmethod
    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        """
        Analyze metrics and return any findings.

        Args:
            metrics: The SparkMetrics to analyze.

        Returns:
            List of Finding objects, empty if no issues detected.
        """
        pass
