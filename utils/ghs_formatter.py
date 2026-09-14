"""
GHS Hazard (H) and Precautionary (P) phrase formatting for display.
Based on UN GHS Rev.10/11. Lexicon version: ghs_formatter_v6.
"""

from __future__ import annotations

import re
from typing import Any

LEXICON_VERSION = "ghs_formatter_v6"

# Subsumption: secondary code → (primary_code, reason)
# H318 eye damage is largely covered by H314 (skin burns and eye damage).
H_SUBSUMPTION: dict[str, tuple[str, str]] = {
    "H318": (
        "H314",
        "H318 (serious eye damage) is often co-listed with H314; "
        "H314 already covers severe skin burns and eye damage.",
    ),
}

# Common UN GHS H-phrases returned by PubChem (Rev.10/11 wording).
GHS_H_PHRASES: dict[str, str] = {
    # Physical
    "H200": "Unstable explosive.",
    "H201": "Explosive; mass explosion hazard.",
    "H202": "Explosive; severe projection hazard.",
    "H203": "Explosive; fire, blast or projection hazard.",
    "H204": "Fire or projection hazard.",
    "H205": "May mass explode in fire.",
    "H220": "Extremely flammable gas.",
    "H221": "Flammable gas.",
    "H222": "Extremely flammable aerosol.",
    "H223": "Flammable aerosol.",
    "H224": "Extremely flammable liquid and vapour.",
    "H225": "Highly flammable liquid and vapour.",
    "H226": "Flammable liquid and vapour.",
    "H227": "Combustible liquid.",
    "H228": "Flammable solid.",
    "H229": "Pressurized container: may burst if heated.",
    "H230": "May react explosively even in the absence of air.",
    "H231": "May react explosively even in the absence of air at elevated pressure and/or temperature.",
    "H240": "Heating may cause an explosion.",
    "H241": "Heating may cause a fire or explosion.",
    "H242": "Heating may cause a fire.",
    "H250": "Catches fire spontaneously if exposed to air.",
    "H251": "Self-heating; may catch fire.",
    "H252": "Self-heating in large quantities; may catch fire.",
    "H260": "In contact with water releases flammable gases which may ignite spontaneously.",
    "H261": "In contact with water releases flammable gas.",
    "H270": "May cause or intensify fire; oxidizer.",
    "H271": "May cause fire or explosion; strong oxidizer.",
    "H272": "May intensify fire; oxidizer.",
    "H280": "Contains gas under pressure; may explode if heated.",
    "H281": "Contains refrigerated gas; may cause cryogenic burns or injury.",
    "H290": "May be corrosive to metals.",
    # Health
    "H300": "Fatal if swallowed.",
    "H301": "Toxic if swallowed.",
    "H302": "Harmful if swallowed.",
    "H304": "May be fatal if swallowed and enters airways.",
    "H310": "Fatal in contact with skin.",
    "H311": "Toxic in contact with skin.",
    "H312": "Harmful in contact with skin.",
    "H314": "Causes severe skin burns and eye damage.",
    "H315": "Causes skin irritation.",
    "H317": "May cause an allergic skin reaction.",
    "H318": "Causes serious eye damage.",
    "H319": "Causes serious eye irritation.",
    "H330": "Fatal if inhaled.",
    "H331": "Toxic if inhaled.",
    "H332": "Harmful if inhaled.",
    "H334": "May cause allergy or asthma symptoms or breathing difficulties if inhaled.",
    "H335": "May cause respiratory irritation.",
    "H336": "May cause drowsiness or dizziness.",
    "H340": "May cause genetic defects.",
    "H341": "Suspected of causing genetic defects.",
    "H350": "May cause cancer.",
    "H351": "Suspected of causing cancer.",
    "H360": "May damage fertility or the unborn child.",
    "H361": "Suspected of damaging fertility or the unborn child.",
    "H362": "May cause harm to breast-fed children.",
    "H370": "Causes damage to organs.",
    "H371": "May cause damage to organs.",
    "H372": "Causes damage to organs through prolonged or repeated exposure.",
    "H373": "May cause damage to organs through prolonged or repeated exposure.",
    # Environmental
    "H400": "Very toxic to aquatic life.",
    "H401": "Toxic to aquatic life.",
    "H402": "Harmful to aquatic life.",
    "H410": "Very toxic to aquatic life with long lasting effects.",
    "H411": "Toxic to aquatic life with long lasting effects.",
    "H412": "Harmful to aquatic life with long lasting effects.",
    "H413": "May cause long lasting harmful effects to aquatic life.",
    "H420": "Harms public health and the environment by destroying ozone in the upper atmosphere.",
}

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
    if c in GHS_H_PHRASES:
        return GHS_H_PHRASES[c]
    # PubChem sometimes returns route/target variants (H350i, H360D, H360Fd).
    base = re.match(r"^(H\d{3})", c, re.I)
    if base:
        key = base.group(1).upper()
        if key in GHS_H_PHRASES:
            return GHS_H_PHRASES[key]
    return f"{c}: (phrase not found)"


def get_p_phrase(code: str) -> str:
    """Return P-code phrase, or 'code: (phrase not found)' if unknown."""
    if not code:
        return ""
    c = code.strip()
    return GHS_P_PHRASES.get(c, f"{c}: (phrase not found)")


def phrase_is_found(phrase: str) -> bool:
    p = (phrase or "").strip().lower()
    return bool(p) and "(phrase not found)" not in p


def _normalize_codes(codes: list[str] | None) -> list[str]:
    out: list[str] = []
    for c in codes or []:
        s = (c or "").strip()
        if s and s not in out:
            out.append(s)
    return out


def account_ghs_codes(
    h_codes: list[str] | None,
    p_codes: list[str] | None,
    *,
    apply_h318_subsumption: bool = True,
) -> dict[str, Any]:
    """
    Partition retrieved GHS codes into displayed / unmapped / suppressed_or_subsumed.

    Acceptance:
        set(retrieved) == set(displayed) | set(unmapped) | set(suppressed codes)
    """
    retrieved_h = _normalize_codes(h_codes)
    retrieved_p = _normalize_codes(p_codes)

    displayed_h: list[str] = []
    displayed_h_phrases: dict[str, str] = {}
    unmapped_h: list[str] = []
    suppressed_h: list[dict[str, str]] = []

    for code in retrieved_h:
        if apply_h318_subsumption and code in H_SUBSUMPTION:
            primary, reason = H_SUBSUMPTION[code]
            if primary in retrieved_h:
                phrase = get_h_phrase(code)
                suppressed_h.append(
                    {
                        "code": code,
                        "primary": primary,
                        "reason": reason,
                        "phrase": phrase if phrase_is_found(phrase) else "",
                    }
                )
                continue
        phrase = get_h_phrase(code)
        if phrase_is_found(phrase):
            # Strip accidental "code: " prefix from dictionary values; get_h_phrase
            # returns bare phrase for known codes.
            clean = phrase
            if clean.lower().startswith(code.lower() + ":"):
                clean = clean.split(":", 1)[1].strip()
            displayed_h.append(code)
            displayed_h_phrases[code] = clean
        else:
            unmapped_h.append(code)

    displayed_p: list[str] = []
    displayed_p_phrases: dict[str, str] = {}
    unmapped_p: list[str] = []
    suppressed_p: list[dict[str, str]] = []

    for code in retrieved_p:
        phrase = get_p_phrase(code)
        if phrase_is_found(phrase):
            clean = phrase
            if clean.lower().startswith(code.lower() + ":"):
                clean = clean.split(":", 1)[1].strip()
            displayed_p.append(code)
            displayed_p_phrases[code] = clean
        else:
            unmapped_p.append(code)

    accounted_h = set(displayed_h) | set(unmapped_h) | {x["code"] for x in suppressed_h}
    accounted_p = set(displayed_p) | set(unmapped_p) | {x["code"] for x in suppressed_p}

    return {
        "lexicon_version": LEXICON_VERSION,
        "retrieved_h_codes": retrieved_h,
        "retrieved_p_codes": retrieved_p,
        "displayed_h_codes": displayed_h,
        "displayed_p_codes": displayed_p,
        "displayed_h_phrases": displayed_h_phrases,
        "displayed_p_phrases": displayed_p_phrases,
        "unmapped_h_codes": unmapped_h,
        "unmapped_p_codes": unmapped_p,
        "suppressed_or_subsumed_h_codes": suppressed_h,
        "suppressed_or_subsumed_p_codes": suppressed_p,
        "h_complete": set(retrieved_h) == accounted_h,
        "p_complete": set(retrieved_p) == accounted_p,
    }


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
