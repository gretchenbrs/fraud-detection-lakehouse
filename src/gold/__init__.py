"""Business-facing risk analytics and investigation outputs."""

from src.gold.pipeline import run_gold_pipeline
from src.gold.reporting import (
    build_executive_summary,
    build_monthly_risk_trend,
    build_queue_segment_summary,
)

__all__ = [
    "build_executive_summary",
    "build_monthly_risk_trend",
    "build_queue_segment_summary",
    "run_gold_pipeline",
]
