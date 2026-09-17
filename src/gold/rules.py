"""Pure business rules used by the Gold investigation queue."""

from __future__ import annotations


PRIORITY_TIERS = ("critical", "high", "medium", "standard")


def validate_priority_fractions(
    priority_fractions: list[float],
    queue_fraction: float,
) -> None:
    """Validate cumulative population fractions for investigation tiers."""
    if not 0 < queue_fraction <= 1:
        raise ValueError("queue_fraction must be in (0, 1].")
    if len(priority_fractions) != 3:
        raise ValueError("priority_fractions must contain critical, high, and medium cutoffs.")
    if (
        any(not 0 < value <= 1 for value in priority_fractions)
        or priority_fractions != sorted(set(priority_fractions))
        or priority_fractions[-1] > queue_fraction
    ):
        raise ValueError("Priority fractions must be unique, ascending, and inside the queue.")


def priority_tier_for_fraction(
    rank_fraction: float,
    priority_fractions: list[float],
) -> str:
    """Return the deterministic priority tier for a population rank fraction."""
    if not 0 < rank_fraction <= 1:
        raise ValueError("rank_fraction must be in (0, 1].")
    validate_priority_fractions(priority_fractions, 1.0)
    if rank_fraction <= priority_fractions[0]:
        return "critical"
    if rank_fraction <= priority_fractions[1]:
        return "high"
    if rank_fraction <= priority_fractions[2]:
        return "medium"
    return "standard"
