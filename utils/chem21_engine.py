"""
Reconstructed CHEM21 Safety / Health / Environment scoring engine.

Based on the CHEM21 solvent selection guide methodology (Prat et al., Green Chem. 2016;
ACS GCI CHEM21 learning platform). This is an approximate reconstruction for screening —
not the official CHEM21 spreadsheet tool.

Scores are 1 (lowest hazard) to 10 (highest hazard).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Health: Table 4 (H3xx / EUH → base score)
# ---------------------------------------------------------------------------

HEALTH_CODE_SCORES: dict[str, int] = {
    # Irritation / minor (score 2)
    "H315": 2,
    "H317": 2,
    "H319": 2,
    "H335": 2,
    "EUH066": 2,
    # Toxic, non-fatal acute (score 4)
    "H301": 4,
    "H311": 4,
    "H331": 4,
    "H318": 4,
    # Harmful acute (score 6)
    "H302": 6,
    "H312": 6,
    "H332": 6,
    "H336": 6,
    "EUH070": 6,
    # STOT / sensitization (score 7)
    "H304": 7,
    "H334": 7,
    "H370": 7,
    "H371": 7,
    "H372": 7,
    "H373": 7,
    # CMR category 2 (score 7 in guide grouping)
    "H341": 7,
    "H351": 7,
    "H361": 7,
    # CMR category 1 (score 9)
    "H340": 9,
    "H350": 9,
    "H360": 9,
    # Fatal / severe damage (score 10)
    "H300": 10,
    "H310": 10,
    "H330": 10,
    "H314": 10,
}

# ---------------------------------------------------------------------------
# Environment: Table 5
# ---------------------------------------------------------------------------

ENV_CODE_SCORES: dict[str, int] = {
    "H412": 3,
    "H413": 3,
    "H400": 7,
    "H410": 7,
    "H411": 7,
    "H401": 7,
    "H402": 7,
    "H420": 10,
}

# ---------------------------------------------------------------------------
# Safety: Table 2 flash-point bands
# ---------------------------------------------------------------------------

SAFETY_FP_BANDS: list[tuple[float, float, int]] = [
    (60.0, 10_000.0, 1),
    (24.0, 60.0, 3),
    (0.0, 24.0, 4),
    (-20.0, 0.0, 5),
    (-10_000.0, -20.0, 7),
]

SAFETY_H2_SCORES: dict[str, int] = {
    "H226": 3,
    "H225": 7,
    "H224": 7,
}

# Missing-data defaults (CHEM21 paper: env default 5 without full REACH data)
CONSERVATIVE_MISSING_HEALTH = 5
CONSERVATIVE_MISSING_ENV = 5
CONSERVATIVE_MISSING_SAFETY = 5
BEST_AVAILABLE_MISSING_DEFAULT = 3


@dataclass
class ScoreResult:
    score: int | None
    mode: str
    components: list[str] = field(default_factory=list)
    missing_input: bool = False
    uncertainty: bool = False


def _base_h(code: str) -> str:
    match = re.match(r"(H\d{3}|EUH\d{3})", code.upper())
    return match.group(1) if match else code.upper()


def _codes_set(codes: list[str] | None) -> set[str]:
    if not codes:
        return set()
    return {_base_h(c) for c in codes}


def _score_from_code_map(codes: list[str] | None, mapping: dict[str, int]) -> int | None:
    if not codes:
        return None
    bases = _codes_set(codes)
    matched = [mapping[c] for c in bases if c in mapping]
    return max(matched) if matched else None


def _bp_health_modifier(bp: float | None) -> int:
    """+1 to health if BP < 85 °C (CHEM21 Table 4 footnote)."""
    if bp is None:
        return 0
    return 1 if bp < 85.0 else 0


def _bp_env_score(bp: float | None) -> int | None:
    """Environment score component from boiling point volatility (Table 5)."""
    if bp is None:
        return None
    if bp < 50:
        return 7
    if bp < 70:
        return 5
    if bp <= 200:
        return 3
    return 3


def _fp_safety_score(fp: float | None) -> int | None:
    if fp is None:
        return None
    for lo, hi, score in SAFETY_FP_BANDS:
        if lo <= fp <= hi:
            return score
    return None


def score_safety(
    flash_point: float | None,
    autoignition_temp: float | None,
    h2xx_codes: list[str] | None,
    peroxide_flag: bool | None = None,
    resistivity: bool | None = None,
    high_decomp_energy: bool | None = None,
) -> ScoreResult:
    """
    CHEM21 safety score from flash point + modifiers (Table 2).

    Adds +1 for AIT < 200 °C, resistivity > 1e8 Ω·m, or EUH019 peroxides.
    """
    components: list[str] = []
    missing = flash_point is None and not h2xx_codes

    if high_decomp_energy:
        return ScoreResult(10, "safety", ["High decomposition energy (>500 J/g) → 10"])

    base = _fp_safety_score(flash_point)
    if base is not None:
        components.append(f"Flash point {flash_point:g} °C → base {base}")
    else:
        h2_score = _score_from_code_map(h2xx_codes, SAFETY_H2_SCORES)
        if h2_score is not None:
            base = h2_score
            components.append(f"H2xx flammability → base {base}")
        else:
            base = None
            components.append("Flash point and H2xx unavailable")

    if base is None:
        return ScoreResult(
            None,
            "safety",
            components,
            missing_input=True,
            uncertainty=True,
        )

    total = base
    if autoignition_temp is not None and autoignition_temp < 200:
        total += 1
        components.append(f"AIT {autoignition_temp:g} °C < 200 → +1")
    elif autoignition_temp is None:
        components.append("AIT unknown (not incremented)")

    if peroxide_flag:
        total += 1
        components.append("Peroxide former (EUH019) → +1")

    if resistivity:
        total += 1
        components.append("High resistivity (>1e8 Ω·m) → +1")

    return ScoreResult(
        min(total, 10),
        "safety",
        components,
        missing_input=missing,
        uncertainty=missing,
    )


def score_health(
    h3xx_codes: list[str] | None,
    boiling_point: float | None = None,
    reach_status: str | None = None,
    euh_codes: list[str] | None = None,
) -> ScoreResult:
    """
    CHEM21 health score from worst H3xx (Table 4) + BP modifier.

    reach_status: 'full', 'partial', 'unknown' — if full and no H3xx with BP >= 85,
    health can be 1 (water-like case).
    """
    components: list[str] = []
    all_health_codes = list(h3xx_codes or []) + list(euh_codes or [])
    base = _score_from_code_map(all_health_codes, HEALTH_CODE_SCORES)

    if base is not None:
        worst = max(
            (c for c in _codes_set(all_health_codes) if c in HEALTH_CODE_SCORES),
            key=lambda c: HEALTH_CODE_SCORES[c],
        )
        components.append(f"Worst H-code {worst} → base {base}")
    elif reach_status == "full" and boiling_point is not None and boiling_point >= 85:
        base = 1
        components.append("Full REACH, no H3xx, BP ≥ 85 °C → 1")
    else:
        base = None
        components.append("No mapped H3xx/EUH health codes")

    missing_h = not all_health_codes
    if base is None:
        return ScoreResult(
            None,
            "health",
            components,
            missing_input=missing_h,
            uncertainty=True,
        )

    modifier = _bp_health_modifier(boiling_point)
    if modifier:
        components.append(f"BP {boiling_point:g} °C < 85 → +{modifier}")
    elif boiling_point is None:
        components.append("BP unknown (volatility modifier not applied)")

    total = min(base + modifier, 10)
    return ScoreResult(
        total,
        "health",
        components,
        missing_input=missing_h,
        uncertainty=missing_h or boiling_point is None,
    )


def score_environment(
    h4xx_codes: list[str] | None,
    boiling_point: float | None = None,
    reach_status: str | None = None,
) -> ScoreResult:
    """CHEM21 environment score — max of H4xx, BP volatility, REACH default (Table 5)."""
    components: list[str] = []
    candidates: list[int] = []

    code_score = _score_from_code_map(h4xx_codes, ENV_CODE_SCORES)
    if code_score is not None:
        candidates.append(code_score)
        components.append(f"H4xx → {code_score}")

    bp_score = _bp_env_score(boiling_point)
    if bp_score is not None:
        candidates.append(bp_score)
        components.append(f"BP volatility → {bp_score}")
    elif boiling_point is None:
        components.append("BP unknown")

    if not h4xx_codes and reach_status == "full":
        candidates.append(3)
        components.append("Full REACH, no H4xx → 3")
    elif not h4xx_codes and reach_status in {None, "unknown", "partial"}:
        candidates.append(5)
        components.append("REACH incomplete / unknown, no H4xx → default 5")

    missing_h = not h4xx_codes
    if not candidates:
        return ScoreResult(
            None,
            "environment",
            components,
            missing_input=missing_h,
            uncertainty=True,
        )

    total = max(candidates)
    return ScoreResult(
        total,
        "environment",
        components,
        missing_input=missing_h,
        uncertainty=missing_h or boiling_point is None,
    )


def assign_chem21_category(
    safety_score: int | None,
    health_score: int | None,
    environment_score: int | None,
) -> str | None:
    """
    Ranking by default (Table 6): driven by the most stringent SHE axis.

    1–3 Recommended, 4–6 Problematic, 7–10 Hazardous.
    """
    scores = [s for s in (safety_score, health_score, environment_score) if s is not None]
    if not scores:
        return None
    worst = max(scores)
    if worst <= 3:
        return "Recommended"
    if worst <= 6:
        return "Problematic"
    return "Hazardous"


def apply_scoring_mode(
    safety: ScoreResult,
    health: ScoreResult,
    env: ScoreResult,
    mode: str,
    peroxide_flag: bool | None = None,
) -> tuple[int | None, int | None, int | None, list[str]]:
    """
    Apply best-available or conservative handling for missing inputs.

    Conservative mode raises scores when critical hazard data are absent.
    """
    notes: list[str] = []

    s = safety.score
    h = health.score
    e = env.score

    if mode == "conservative":
        if s is None:
            s = CONSERVATIVE_MISSING_SAFETY
            notes.append(f"Safety missing → conservative default {s}")
        if h is None:
            h = CONSERVATIVE_MISSING_HEALTH
            notes.append(f"Health missing → conservative default {h}")
        elif health.missing_input:
            h = max(h, CONSERVATIVE_MISSING_HEALTH)
            notes.append(f"Health H-codes missing → floor {h}")
        if e is None:
            e = CONSERVATIVE_MISSING_ENV
            notes.append(f"Environment missing → conservative default {e}")
        elif env.missing_input:
            e = max(e, CONSERVATIVE_MISSING_ENV)
            notes.append(f"Environment H-codes missing → floor {e}")
    else:
        if s is None:
            s = BEST_AVAILABLE_MISSING_DEFAULT
            notes.append(f"Safety missing → best-available default {s}")
        if h is None and health.missing_input:
            h = BEST_AVAILABLE_MISSING_DEFAULT
            notes.append("Health: no H-codes, best-available default 3")
        if e is None and env.missing_input:
            e = BEST_AVAILABLE_MISSING_DEFAULT
            notes.append("Environment: no H4xx, best-available default 3")

    return s, h, e, notes


def score_row(
    features: dict[str, Any],
    mode: str = "best_available",
    reach_status: str | None = "unknown",
    peroxide_flag: bool | None = None,
    resistivity: bool | None = None,
) -> dict[str, Any]:
    """Score one compound from PubChem-derived features."""
    euh = features.get("euH_codes") or [
        c for c in features.get("h_codes_all", []) if str(c).startswith("EUH")
    ]
    peroxide = peroxide_flag
    if peroxide is None:
        peroxide = "EUH019" in {_base_h(c) for c in euh}

    safety = score_safety(
        features.get("flash_point_c"),
        features.get("autoignition_temp_c"),
        features.get("h2xx_codes"),
        peroxide_flag=peroxide,
        resistivity=resistivity,
    )
    health = score_health(
        features.get("h3xx_codes"),
        features.get("boiling_point_c"),
        reach_status=reach_status,
        euh_codes=euh,
    )
    env = score_environment(
        features.get("h4xx_codes"),
        features.get("boiling_point_c"),
        reach_status=reach_status,
    )

    s, h, e, mode_notes = apply_scoring_mode(safety, health, env, mode, peroxide)
    category = assign_chem21_category(s, h, e)

    missing_pubchem = features.get("pubchem_status") in {"cas_not_found", "invalid_cas"}
    missing_h = not features.get("h_codes_all")
    missing_phys = (
        features.get("boiling_point_c") is None
        and features.get("flash_point_c") is None
        and features.get("autoignition_temp_c") is None
    )

    uncertainty = sum(
        [
            safety.uncertainty,
            health.uncertainty,
            env.uncertainty,
            missing_pubchem,
            missing_h,
            missing_phys,
        ]
    )

    return {
        "chem21_safety_score": s,
        "chem21_health_score": h,
        "chem21_environment_score": e,
        f"chem21_{mode}_category": category,
        "missing_pubchem_data": missing_pubchem,
        "missing_h_codes": missing_h,
        "missing_physical_properties": missing_phys,
        "uncertainty_score": uncertainty,
        "scoring_notes": explain_score(safety, health, env, mode_notes),
    }


def explain_score(
    safety: ScoreResult,
    health: ScoreResult,
    env: ScoreResult,
    extra_notes: list[str] | None = None,
) -> str:
    """Human-readable scoring breakdown."""
    parts = [
        f"Safety: {'; '.join(safety.components)}",
        f"Health: {'; '.join(health.components)}",
        f"Environment: {'; '.join(env.components)}",
    ]
    if extra_notes:
        parts.extend(extra_notes)
    return " | ".join(parts)
