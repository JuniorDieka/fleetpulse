"""Tests for reliability metrics (MTBF, MTTR)."""

import pytest
from fleetpulse.analytics.reliability import calculate_mtbf, calculate_mttr


class TestMTBF:
    """Test MTBF calculations."""

    def test_mtbf_normal_case(self) -> None:
        """Test MTBF with normal inputs."""
        mtbf = calculate_mtbf("TRUCK-001", 10000.0, 5)
        assert mtbf == 2000.0

    def test_mtbf_no_failures(self) -> None:
        """Test MTBF with zero failures."""
        mtbf = calculate_mtbf("TRUCK-001", 10000.0, 0)
        assert mtbf == float("inf")

    def test_mtbf_single_failure(self) -> None:
        """Test MTBF with single failure."""
        mtbf = calculate_mtbf("TRUCK-001", 5000.0, 1)
        assert mtbf == 5000.0

    def test_mtbf_high_failure_rate(self) -> None:
        """Test MTBF with high failure rate."""
        mtbf = calculate_mtbf("TRUCK-001", 1000.0, 10)
        assert mtbf == 100.0


class TestMTTR:
    """Test MTTR calculations."""

    def test_mttr_normal_case(self) -> None:
        """Test MTTR with normal inputs."""
        mttr = calculate_mttr("TRUCK-001", 50.0, 5)
        assert mttr == 10.0

    def test_mttr_no_repairs(self) -> None:
        """Test MTTR with zero repairs."""
        mttr = calculate_mttr("TRUCK-001", 0.0, 0)
        assert mttr == 0.0

    def test_mttr_single_repair(self) -> None:
        """Test MTTR with single repair."""
        mttr = calculate_mttr("TRUCK-001", 12.5, 1)
        assert mttr == 12.5

    def test_mttr_varying_repair_times(self) -> None:
        """Test MTTR with varying repair durations."""
        mttr = calculate_mttr("TRUCK-001", 100.0, 8)
        assert mttr == 12.5
