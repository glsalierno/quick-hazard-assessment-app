"""
P2OASys category score aggregation.

Convert per-category max scores to an overall 0–10 evaluation.
Used with utils.p2oasys_scorer.compute_p2oasys_scores().
"""

from __future__ import annotations

from typing import Any


def aggregate_category_scores(scores: dict[str, Any], method: str = "max") -> float:
    """
    Convert per-category max scores to overall 0–10 evaluation.

    Methods:
      - max: overall = max of category maxes
      - mean: overall = mean of category maxes
      - weighted_mean: Acute/Chronic/Physical weighted higher (typical P2OASys emphasis)
    """
    cat_maxes = []
    for cat, data in scores.items():
        if cat.startswith("_"):
            continue
        if isinstance(data, dict):
            cmax = data.get("_category_max")
            if cmax is not None:
                cat_maxes.append(cmax)

    if not cat_maxes:
        return float("nan")

    if method == "max":
        return max(cat_maxes)
    if method == "mean":
        return sum(cat_maxes) / len(cat_maxes)
    if method == "weighted_mean":
        weights = {"Acute Human Effects": 1.5, "Chronic Human Effects": 1.5, "Physical Properties": 1.2}
        wsum = 0.0
        wtotal = 0.0
        for cat, data in scores.items():
            if cat.startswith("_") or not isinstance(data, dict):
                continue
            cmax = data.get("_category_max")
            if cmax is not None:
                w = weights.get(cat, 1.0)
                wsum += cmax * w
                wtotal += w
        return wsum / wtotal if wtotal else float("nan")
    return float("nan")


def count_scored_categories(scores: dict[str, Any]) -> tuple[int, list[str]]:
    """Return (number of categories with a score, list of category names)."""
    n = 0
    names = []
    for cat, data in scores.items():
        if cat.startswith("_"):
            continue
        if isinstance(data, dict) and data.get("_category_max") is not None:
            n += 1
            names.append(cat)
    return n, names
