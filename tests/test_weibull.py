"""Tests for Weibull distribution analysis."""

import pytest
import numpy as np
from fleetpulse.analytics.weibull import (
    fit_weibull_distribution,
    predict_failure_probability,
)


class TestWeibullFitting:
    """Test Weibull distribution fitting."""

    def test_weibull_fit_sufficient_data(self) -> None:
        """Test Weibull fitting with sufficient data."""
        np.random.seed(42)
        intervals = np.random.weibull(2.0, 10) * 1000

        shape, scale = fit_weibull_distribution(intervals, "TRUCK-001")

        assert shape is not None
        assert scale is not None
        assert shape > 0
        assert scale > 0

    def test_weibull_fit_insufficient_data(self) -> None:
        """Test Weibull fitting with insufficient data."""
        intervals = np.array([100.0, 200.0])

        shape, scale = fit_weibull_distribution(intervals, "TRUCK-001")

        assert shape is None
        assert scale is None

    def test_weibull_fit_minimum_data(self) -> None:
        """Test Weibull fitting with minimum required data."""
        intervals = np.array([100.0, 200.0, 150.0])

        shape, scale = fit_weibull_distribution(intervals, "TRUCK-001")

        assert shape is not None
        assert scale is not None


class TestFailureProbability:
    """Test failure probability predictions."""

    def test_failure_probability_wear_out(self) -> None:
        """Test failure probability with wear-out mode (β > 1)."""
        prob = predict_failure_probability(
            shape=2.5, scale=10000.0, current_hours=8000.0, horizon_hours=50.0, truck_id="TRUCK-001"
        )

        assert 0.0 <= prob <= 1.0
        assert prob > 0

    def test_failure_probability_random(self) -> None:
        """Test failure probability with random failures (β ≈ 1)."""
        prob = predict_failure_probability(
            shape=1.0, scale=10000.0, current_hours=5000.0, horizon_hours=50.0, truck_id="TRUCK-001"
        )

        assert 0.0 <= prob <= 1.0

    def test_failure_probability_infant_mortality(self) -> None:
        """Test failure probability with infant mortality (β < 1)."""
        prob = predict_failure_probability(
            shape=0.5, scale=10000.0, current_hours=1000.0, horizon_hours=50.0, truck_id="TRUCK-001"
        )

        assert 0.0 <= prob <= 1.0

    def test_failure_probability_bounds(self) -> None:
        """Test that probability is always between 0 and 1."""
        prob = predict_failure_probability(
            shape=3.0, scale=5000.0, current_hours=4900.0, horizon_hours=200.0, truck_id="TRUCK-001"
        )

        assert 0.0 <= prob <= 1.0

    def test_failure_probability_zero_horizon(self) -> None:
        """Test failure probability with zero horizon."""
        prob = predict_failure_probability(
            shape=2.0, scale=10000.0, current_hours=5000.0, horizon_hours=0.0, truck_id="TRUCK-001"
        )

        assert prob == 0.0
