"""Build CHEM21 S/H/E scores from GHaz7 PubChem / hazard payloads.

Uses the reconstructed Prat et al. CHEM21 engine (vendored from
fastP2OASys/CHEM21/analysis/chem21_engine.py) for automatic screening rankings.
"""

from __future__ import annotations

import re
from typing import Any

from utils import chem21_engine


def _base_h(code: str) -> str:
    m = re.match(r"(H\d{3}|EUH\d{3})", str(code).upper())
    return m.group(1) if m else str(code).upper()


def _collect_h_codes(pubchem: dict[str, Any] | None, hazard_data: dict[str, Any] | None = None) -> list[str]:
    codes: list[str] = []
    for blob in (pubchem or {}, hazard_data or {}):
        ghs = blob.get("ghs") if isinstance(blob.get("ghs"), dict) else {}
        for key in ("h_codes", "h_phrases", "hazard_statements"):
            val = ghs.get(key) or blob.get(key)
            if isinstance(val, list):
                codes.extend(str(x) for x in val)
            elif isinstance(val, str):
                codes.append(val)
        # free-text scan
        for key in ("ghs_classification", "hazard_class"):
            text = blob.get(key)
            if isinstance(text, str):
                codes.extend(re.findall(r"H\d{3}|EUH\d{3}", text, flags=re.I))
    # Also scan toxicity / designation strings lightly
    hm = (hazard_data or {}).get("hazard_metrics") or {}
    for arr in hm.values() if isinstance(hm, dict) else []:
        if isinstance(arr, list):
            for item in arr:
                codes.extend(re.findall(r"H\d{3}|EUH\d{3}", str(item), flags=re.I))
    out: list[str] = []
    seen: set[str] = set()
    for c in codes:
        b = _base_h(c)
        if b not in seen and re.match(r"(H\d{3}|EUH\d{3})$", b):
            seen.add(b)
            out.append(b)
    return out


def _split_families(h_codes: list[str]) -> tuple[list[str], list[str], list[str]]:
    h2, h3, h4 = [], [], []
    for c in h_codes:
        if c.startswith("H2"):
            h2.append(c)
        elif c.startswith("H3") or c.startswith("EUH"):
            h3.append(c)
        elif c.startswith("H4"):
            h4.append(c)
    return h2, h3, h4


def _first_temp_c(values: Any) -> float | None:
    if values is None:
        return None
    if isinstance(values, (int, float)) and values == values:
        return float(values)
    seq = values if isinstance(values, list) else [values]
    for item in seq:
        text = str(item)
        # Prefer explicit °C
        m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*°?\s*C\b", text, flags=re.I)
        if m:
            return float(m.group(1))
        m_f = re.search(r"([+-]?\d+(?:\.\d+)?)\s*°?\s*F\b", text, flags=re.I)
        if m_f:
            return (float(m_f.group(1)) - 32.0) * 5.0 / 9.0
        m_n = re.search(r"([+-]?\d+(?:\.\d+)?)", text)
        if m_n and "flash" not in text.lower()[:20]:
            # bare number — only trust if short
            if len(text.strip()) < 12:
                try:
                    return float(m_n.group(1))
                except ValueError:
                    pass
    return None


def features_from_pubchem(
    pubchem: dict[str, Any] | None,
    *,
    hazard_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Flatten GHaz7 PubChem / hazard dicts into chem21_engine.score_row features."""
    pc = pubchem or {}
    hd = hazard_data or {}
    h_codes = _collect_h_codes(pc, hd)
    h2, h3, h4 = _split_families(h_codes)
    euh = [c for c in h_codes if c.startswith("EUH")]

    bp = _first_temp_c(pc.get("boiling_point") or pc.get("boiling_point_c"))
    fp = _first_temp_c(pc.get("flash_point") or pc.get("flash_point_c"))
    if bp is None:
        bp = _first_temp_c((hd.get("hazard_metrics") or {}).get("boiling_point"))
    if fp is None:
        fp = _first_temp_c((hd.get("hazard_metrics") or {}).get("flash_point"))
    ait = _first_temp_c(pc.get("autoignition_temperature") or pc.get("autoignition_temp_c"))

    status = "ok"
    if not pc:
        status = "cas_not_found"

    return {
        "h_codes_all": h_codes,
        "h2xx_codes": h2,
        "h3xx_codes": h3,
        "h4xx_codes": h4,
        "euH_codes": euh,
        "boiling_point_c": bp,
        "flash_point_c": fp,
        "autoignition_temp_c": ait,
        "pubchem_status": status,
    }


def estimate_chem21(
    pubchem: dict[str, Any] | None,
    *,
    hazard_data: dict[str, Any] | None = None,
    mode: str = "conservative",
) -> dict[str, Any]:
    """Return CHEM21 S/H/E + ranking via the Prat-style reconstructed engine."""
    features = features_from_pubchem(pubchem, hazard_data=hazard_data)
    scored = chem21_engine.score_row(features, mode=mode, reach_status="unknown")
    category = (
        scored.get(f"chem21_{mode}_category")
        or scored.get("chem21_best_available_category")
        or scored.get("chem21_conservative_category")
    )
    return {
        "status": "estimated",
        "message": "CHEM21 reconstructed ranking (Prat et al. algorithm via PubChem GHS/physchem).",
        "safety": scored.get("chem21_safety_score"),
        "health": scored.get("chem21_health_score"),
        "env": scored.get("chem21_environment_score"),
        "ranking_default": category,
        "ranking_discussion": None,
        "scoring_notes": scored.get("scoring_notes"),
        "mode": mode,
        "source": "chem21_engine",
        "features": features,
    }
