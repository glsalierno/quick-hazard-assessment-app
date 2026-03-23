"""
GHS Hazard (H) and Precautionary (P) phrase formatting for display.
Based on UN GHS Rev.10/11. Source: Wikipedia Module:GHS_phrases/data.
"""

from __future__ import annotations

# Subset of H-phrases commonly returned by PubChem (full set in quick_hazard_assessment/ghs_phrases.py)
GHS_H_PHRASES: dict[str, str] = {
    "H220": "Extremely flammable gas.",
    "H225": "Highly flammable liquid and vapour.",
    "H226": "Flammable liquid and vapour.",
    "H302": "Harmful if swallowed.",
    "H312": "Harmful in contact with skin.",
    "H314": "Causes severe skin burns and eye damage.",
    "H315": "Causes skin irritation.",
    "H319": "Causes serious eye irritation.",
    "H332": "Harmful if inhaled.",
    "H335": "May cause respiratory irritation.",
    "H340": "May cause genetic defects.",
    "H350": "May cause cancer.",
    "H351": "Suspected of causing cancer.",
    "H360": "May damage fertility or the unborn child.",
    "H373": "May cause damage to organs through prolonged or repeated exposure.",
    "H400": "Very toxic to aquatic life.",
    "H410": "Very toxic to aquatic life with long lasting effects.",
    "H411": "Toxic to aquatic life with long lasting effects.",
}

# Subset of P-phrases (expand as needed; full set in repo ghs_phrases.py)
GHS_P_PHRASES: dict[str, str] = {
    "P210": "Keep away from heat, hot surfaces, sparks, open flames and other ignition sources. No smoking.",
    "P261": "Avoid breathing dust/fume/gas/mist/vapours/spray.",
    "P273": "Avoid release to the environment.",
    "P280": "Wear protective gloves/protective clothing/eye protection/face protection/hearing protection/...",
    "P305+P351+P338": "IF IN EYES: Rinse continuously with water for several minutes. Remove contact lenses, if present and easy to do. Continue rinsing.",
    "P310": "Immediately call a POISON CENTER or doctor/physician.",
    "P501": "Dispose of contents/container to ...",
}


def get_h_phrase(code: str) -> str:
    """Return H-code phrase, or 'code: (phrase not found)' if unknown."""
    if not code:
        return ""
    c = code.strip()
    return GHS_H_PHRASES.get(c, f"{c}: (phrase not found)")


def get_p_phrase(code: str) -> str:
    """Return P-code phrase, or 'code: (phrase not found)' if unknown."""
    if not code:
        return ""
    c = code.strip()
    return GHS_P_PHRASES.get(c, f"{c}: (phrase not found)")


def expand_h_codes_with_phrases(codes: list[str] | None) -> list[str]:
    """Convert ['H302','H312'] -> ['H302: Harmful if swallowed', ...]."""
    if not codes:
        return []
    return [f"{c}: {get_h_phrase(c)}" for c in codes if (c or "").strip()]


def expand_p_codes_with_phrases(codes: list[str] | None) -> list[str]:
    """Convert ['P264','P280'] -> ['P264: Wash hands...', ...]."""
    if not codes:
        return []
    return [f"{c}: {get_p_phrase(c)}" for c in codes if (c or "").strip()]
