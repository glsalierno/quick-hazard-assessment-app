"""Unified hazard report: legacy v1.4 assessment + REACH IUCLID offline rows (separate package)."""

from .report_generator import generate_report
from .unified_lookup import unified_lookup

__all__ = ["unified_lookup", "generate_report"]
