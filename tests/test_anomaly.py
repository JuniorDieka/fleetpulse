"""Tests for Z-score anomaly detection."""

import pytest
import numpy as np
from fleetpulse.analytics.anomaly import calculate_z_score


class TestZScore:
    """Test Z-score calculations."""

    def test_z_score_at_mean(self) -> None:
        """Test Z-score when value equals mean."""
        z = calculate_z_score(value=100.0, mean=100.0, std=10.0)
        assert z == 0.0

    def test_z_score_one_sigma_above(self) -> None:
        """Test Z-score one standard deviation above mean."""
        z = calculate_z_score(value=110.0, mean=100.0, std=10.0)
        assert z == 1.0

    def test_z_score_one_sigma_below(self) -> None:
        """Test Z-score one standard deviation below mean."""
        z = calculate_z_score(value=90.0, mean=100.0, std=10.0)
        assert z == -1.0

    def test_z_score_three_sigma_above(self) -> None:
        """Test Z-score three standard deviations above mean."""
        z = calculate_z_score(value=130.0, mean=100.0, std=10.0)
        assert z == 3.0

    def test_z_score_zero_std(self) -> None:
        """Test Z-score with zero standard deviation."""
        z = calculate_z_score(value=100.0, mean=100.0, std=0.0)
        assert z == 0.0

    def test_z_score_negative_values(self) -> None:
        """Test Z-score with negative values."""
        z = calculate_z_score(value=-50.0, mean=-100.0, std=25.0)
        assert z == 2.0

    def test_z_score_large_deviation(self) -> None:
        """Test Z-score with large deviation."""
        z = calculate_z_score(value=200.0, mean=100.0, std=10.0)
        assert z == 10.0

    def test_z_score_precision(self) -> None:
        """Test Z-score calculation precision."""
        z = calculate_z_score(value=105.5, mean=100.0, std=3.0)
        assert abs(z - 1.8333333) < 0.0001
