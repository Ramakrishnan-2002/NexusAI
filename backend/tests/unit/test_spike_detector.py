import pytest
from workers.analytics.detector import SpikeDetector


def test_baseline_activity_scoring():
    # Normal low baseline: 1 edit in 1m, 2 edits in 5m, 4 edits in 15m
    score, velocity, baseline_vel, spike_mult = SpikeDetector.calculate_activity_score(
        edits_1m=1,
        edits_5m=2,
        edits_15m=4,
        unique_editors_5m=2,
        total_byte_delta=150,
    )
    assert velocity > 0
    assert baseline_vel >= 0.2
    assert spike_mult < 3.0  # Should not trigger spike multiplier threshold


def test_unusual_activity_spike_detection():
    # Sudden burst: 15 edits in 1m, 35 edits in 5m, 35 edits in 15m (sudden 10x surge from baseline)
    score, velocity, baseline_vel, spike_mult = SpikeDetector.calculate_activity_score(
        edits_1m=15,
        edits_5m=35,
        edits_15m=35,
        unique_editors_5m=12,
        total_byte_delta=5400,
    )
    assert velocity >= 9.0
    assert spike_mult >= 3.0
    assert score > 20.0
