from typing import Dict, Tuple
from app.core.config import settings


class SpikeDetector:
    """
    Transparent Trend & Unusual Activity Detection Engine.
    Formula:
      activity_score =
          (w_1m * velocity_1m)
        + (w_5m * velocity_5m)
        + (w_15m * velocity_15m)
        + (baseline_multiplier * deviation_factor)
        + (unique_editor_ratio * editor_weight)
    """

    @staticmethod
    def calculate_activity_score(
        edits_1m: int,
        edits_5m: int,
        edits_15m: int,
        unique_editors_5m: int,
        total_byte_delta: int,
    ) -> Tuple[float, float, float, float]:
        """
        Returns (activity_score, edits_per_minute, baseline_velocity, spike_multiplier)
        """
        # Velocities in edits per minute
        v_1m = float(edits_1m)
        v_5m = float(edits_5m) / 5.0
        v_15m = float(edits_15m) / 15.0

        # Current weighted velocity
        current_velocity = (
            settings.TREND_WINDOW_1M_WEIGHT * v_1m +
            settings.TREND_WINDOW_5M_WEIGHT * v_5m +
            settings.TREND_WINDOW_15M_WEIGHT * v_15m
        )

        # Baseline velocity (represented by the longer trailing 15m rate, min floor 0.2)
        baseline_velocity = max(0.2, v_15m)

        # Spike multiplier
        spike_multiplier = max(1.0, current_velocity / baseline_velocity)

        # Editor diversity bonus (spikes by multiple independent editors are more meaningful than 1 bot)
        editor_diversity = min(3.0, float(unique_editors_5m) / max(1.0, float(edits_5m)))

        # Magnitude factor
        magnitude_bonus = min(2.0, abs(total_byte_delta) / 1000.0)

        # Transparent combined score
        score = (current_velocity * 2.0) + (spike_multiplier * 1.5) + (editor_diversity * 1.2) + magnitude_bonus

        return round(score, 2), round(current_velocity, 2), round(baseline_velocity, 2), round(spike_multiplier, 2)
