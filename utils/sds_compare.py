"""
Compare SDS-extracted fields against v1.3 PubChem-derived fields.

Phase 1 focus:
- GHS H/P codes + signal word
- Quantitative values:
  - Flash point
  - Vapor pressure
  - Aquatic LC50/EC50 (mg/L)

This intentionally ignores empty SDS fields (so "empty mandatory fields"
do not create false mismatches).
"""

from __future__ import annotations

import math
import re
from typing import Any


_NUM_RE = re.compile(r"(?P<op>[<>~])?\s*(?P<val>\d+(?:[.,]\d+)?)")
_FLASH_F_RE = re.compile(r"\bF\b|Fahrenheit|°\s*F", re.IGNORECASE)


def _as_float(s: Any) -> float | None:
    if s is None:
        return None
    try:
        ss = str(s).strip().replace(",", ".")
        return float(ss)
    except ValueError:
        return None


def _parse_ghs_tokens(codes: list[str]) -> set[str]:
    """
    Split combined codes (e.g., P305+P351+P338) into individual tokens.
    """
    toks: set[str] = set()
    for c in codes or []:
        if not c:
            continue
        for m in re.findall(r"\b([HP]\d{3})\b", str(c), flags=re.IGNORECASE):
            toks.add(m.upper())
    return toks


def _parse_flash_point_value_c(fp_str: str) -> tuple[float | None, str | None]:
    """
    Parse a PubChem flash_point entry like:
    - '12 °C'
    - '> 50 °F'

    Returns (value_c, operator).
    """
    if not fp_str:
        return None, None
    s = str(fp_str)
    m = _NUM_RE.search(s)
    if not m:
        return None, None
    op = (m.group("op") or "").strip() or None
    val = _as_float(m.group("val"))
    if val is None:
        return None, op

    is_f = bool(_FLASH_F_RE.search(s))
    if is_f:
        val = (val - 32.0) * 5.0 / 9.0
    return float(val), op


_VAPOR_UNIT_TO_MMHG = {
    # 1 mmHg = 133.322 Pa = 0.133322 kPa = 1.33322 mbar
    "mmhg": 1.0,
    "torr": 1.0,
    "pa": 1.0 / 133.322,
    "kpa": 1.0 / 0.133322,
    "hpa": 1.0 / 0.133322,
    "mbar": 1.0 / 1.33322,
    "bar": 1.0 / 0.00133322,
}


def _to_mmhg(value: float, unit: str) -> float | None:
    if value is None or not unit:
        return None
    u = str(unit).strip().lower().replace("°", "")
    if u not in _VAPOR_UNIT_TO_MMHG:
        return None
    return float(value) * _VAPOR_UNIT_TO_MMHG[u]


def _parse_vapor_pressure_pubchem(vp_str: str) -> tuple[float | None, float | None]:
    """
    Returns (value_mmHg, temperature_c_or_none).
    """
    if not vp_str:
        return None, None
    s = str(vp_str)

    # Extract operator + number first.
    m = _NUM_RE.search(s)
    if not m:
        return None, None
    val = _as_float(m.group("val"))
    if val is None:
        return None, None

    unit_m = re.search(r"(mmHg|kPa|hPa|Pa|mbar|bar|Torr)\b", s, flags=re.IGNORECASE)
    if not unit_m:
        return None, None
    unit = unit_m.group(1)
    value_mmHg = _to_mmhg(val, unit)

    # Optional temperature: e.g. '(20 °C)'
    temp_c = None
    t_m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:°?\s*([CF])|deg\s*([CF]))", s, flags=re.IGNORECASE)
    if t_m:
        t_val = _as_float(t_m.group(1))
        unit_letter = t_m.group(2) or t_m.group(3)
        if t_val is not None and unit_letter:
            if unit_letter.upper() == "F":
                temp_c = (t_val - 32.0) * 5.0 / 9.0
            else:
                temp_c = float(t_val)

    return value_mmHg, temp_c


def _parse_sds_mg_l_to_value_mg_l(value: float, unit: str) -> float | None:
    if value is None:
        return None
    if not unit:
        return None
    u = str(unit).strip().lower().replace("µ", "u")
    if u in ("mg/l", "mg l"):
        return float(value)
    if u in ("ug/l", "ug l", "u/l"):
        return float(value) / 1000.0
    return None


def compare_sds_to_pubchem(sds: dict[str, Any], pubchem_data: dict[str, Any]) -> dict[str, Any]:
    """
    Returns a report dict meant for UI display.
    """
    pubchem_ghs = pubchem_data.get("ghs") or {}
    pub_h = pubchem_ghs.get("h_codes") or []
    pub_p = pubchem_ghs.get("p_codes") or []
    pub_signal = (pubchem_ghs.get("signal_word") or "").strip()

    sds_ghs = (sds.get("ghs") or {}) if isinstance(sds.get("ghs"), dict) else {}
    sds_h = sds_ghs.get("h_codes") or []
    sds_p = sds_ghs.get("p_codes") or []
    sds_signal = (sds_ghs.get("signal_word") or "").strip()

    sds_h_toks = _parse_ghs_tokens(list(sds_h))
    sds_p_toks = _parse_ghs_tokens(list(sds_p))
    pub_h_toks = _parse_ghs_tokens(list(pub_h))
    pub_p_toks = _parse_ghs_tokens(list(pub_p))

    report: dict[str, Any] = {
        "match_summary": {
            "ghs": {"h_overlap": len(sds_h_toks & pub_h_toks), "p_overlap": len(sds_p_toks & pub_p_toks)},
            "signal_word": {"match": bool(sds_signal and pub_signal and sds_signal.lower() == pub_signal.lower())},
        },
        "ghs_comparison": {
            "sds": {"h_codes": sorted(sds_h_toks), "p_codes": sorted(sds_p_toks), "signal_word": sds_signal or None},
            "pubchem": {"h_codes": sorted(pub_h_toks), "p_codes": sorted(pub_p_toks), "signal_word": pub_signal or None},
            "overlap": {
                "h_codes_overlap": sorted(sds_h_toks & pub_h_toks),
                "h_codes_only_in_sds": sorted(sds_h_toks - pub_h_toks),
                "h_codes_missing_in_sds": sorted(pub_h_toks - sds_h_toks),
                "p_codes_overlap": sorted(sds_p_toks & pub_p_toks),
                "p_codes_only_in_sds": sorted(sds_p_toks - pub_p_toks),
                "p_codes_missing_in_sds": sorted(pub_p_toks - sds_p_toks),
            },
        },
        "quantitative_comparison": {},
        "notes": {
            # We use "meaningful-only" comparison: if SDS has empty values, do not report mismatch.
            "quant_comparison_rule": "Only compare a quantitative metric if SDS extracted at least one value.",
        },
    }

    quant = sds.get("quantitative") or {}
    if not isinstance(quant, dict):
        quant = {}

    # Flash point
    sds_flash = quant.get("flash_point") or []
    if sds_flash:
        pub_flash_strs = pubchem_data.get("flash_point") or []
        pub_flash_parsed = [_parse_flash_point_value_c(x) for x in pub_flash_strs]
        sds_flash_parsed = []
        for x in sds_flash:
            if not isinstance(x, dict):
                continue
            val_c = x.get("value_c")
            sds_flash_parsed.append(
                {
                    "value_c": val_c,
                    "operator": (x.get("operator") or "").strip() or None,
                    "raw_text": x.get("raw_text") or "",
                }
            )

        matches: list[dict[str, Any]] = []
        mismatches: list[dict[str, Any]] = []
        tol_c = 10.0
        for s in sds_flash_parsed:
            sv = s.get("value_c")
            sop = s.get("operator")
            if sv is None:
                continue
            matched = False
            # Check against all pubchem flash points we parsed.
            for pv, pop in pub_flash_parsed:
                if pv is None:
                    continue
                # Operator-aware inequality
                if sop == "<" and sv is not None and pv is not None:
                    if pv < sv + tol_c:
                        matched = True
                        break
                if sop == ">" and sv is not None and pv is not None:
                    if pv > sv - tol_c:
                        matched = True
                        break

                if abs(pv - sv) <= tol_c:
                    matched = True
                    break
                if sv != 0 and abs(pv - sv) / abs(sv) <= 0.25:
                    matched = True
                    break

            if matched:
                matches.append(s)
            else:
                mismatches.append(s)

        report["quantitative_comparison"]["flash_point"] = {
            "sds_values": sds_flash_parsed,
            "pubchem_values_raw": pub_flash_strs,
            "pubchem_values_parsed": [{"value_c": pv, "operator": pop} for (pv, pop) in pub_flash_parsed],
            "matches_count": len(matches),
            "mismatches_count": len(mismatches),
            "matches": matches,
            "mismatches": mismatches,
        }

    # Vapor pressure
    sds_vp = quant.get("vapor_pressure") or []
    if sds_vp:
        pub_vp_strs = pubchem_data.get("vapor_pressure") or []
        pub_vp_parsed = [_parse_vapor_pressure_pubchem(x) for x in pub_vp_strs]  # (mmHg, temp)

        # Convert SDS values to mmHg where possible.
        sds_vp_parsed: list[dict[str, Any]] = []
        for x in sds_vp:
            if not isinstance(x, dict):
                continue
            val = x.get("value")
            unit = x.get("unit")
            mmhg = _to_mmhg(float(val), str(unit)) if val is not None and unit else None
            sds_vp_parsed.append(
                {
                    "value_mmHg": mmhg,
                    "temperature_c": x.get("temperature_c"),
                    "operator": (x.get("operator") or "").strip() or None,
                    "raw_text": x.get("raw_text") or "",
                    "sds_unit": unit,
                }
            )

        # Match rule: allow 1 order of magnitude disagreement (factor 10),
        # but require numeric parsing success.
        matches = []
        mismatches = []
        for s in sds_vp_parsed:
            sv = s.get("value_mmHg")
            if sv is None:
                mismatches.append(s)
                continue
            matched = False
            for pv, _pt in pub_vp_parsed:
                if pv is None:
                    continue
                if sv == 0 or pv == 0:
                    continue
                factor = max(sv / pv, pv / sv)
                if factor <= 10.0:
                    matched = True
                    break
            if matched:
                matches.append(s)
            else:
                mismatches.append(s)

        report["quantitative_comparison"]["vapor_pressure"] = {
            "sds_values_parsed": sds_vp_parsed,
            "pubchem_values_raw": pub_vp_strs,
            "pubchem_values_parsed_mmHg": [{"value_mmHg": pv, "temperature_c": pt} for (pv, pt) in pub_vp_parsed],
            "matches_count": len(matches),
            "mismatches_count": len(mismatches),
            "matches": matches,
            "mismatches": mismatches,
        }

    # Aquatic LC50/EC50
    sds_aq = quant.get("aquatic_toxicity") or []
    if sds_aq:
        # Compare against PubChem derived single values (may be None).
        pub_lc50 = pubchem_data.get("ecotoxicity", {}).get("aquatic_lc50_mg_l")
        pub_ec50 = pubchem_data.get("ecotoxicity", {}).get("aquatic_ec50_mg_l")

        sds_aq_parsed: list[dict[str, Any]] = []
        for x in sds_aq:
            if not isinstance(x, dict):
                continue
            endpoint = x.get("endpoint")
            unit = x.get("unit")
            value = x.get("value")
            mg_l = _parse_sds_mg_l_to_value_mg_l(value, unit)
            sds_aq_parsed.append(
                {
                    "endpoint": endpoint,
                    "value_mg_l": mg_l,
                    "raw_text": x.get("raw_text") or "",
                    "duration": x.get("duration"),
                    "species": x.get("species"),
                    "operator": (x.get("operator") or "").strip() or None,
                    "unit": unit,
                }
            )

        matches = []
        mismatches = []
        for s in sds_aq_parsed:
            endpoint = (s.get("endpoint") or "").upper()
            sv = s.get("value_mg_l")
            if sv is None:
                mismatches.append(s)
                continue
            if endpoint.startswith("LC50"):
                pv = pub_lc50
            elif endpoint.startswith("EC50"):
                pv = pub_ec50
            else:
                pv = None

            if pv is None:
                mismatches.append({**s, "pubchem_value_mg_l": None})
                continue

            if pv == 0 or sv == 0:
                continue
            factor = max(sv / float(pv), float(pv) / sv)
            if factor <= 10.0:
                matches.append(s)
            else:
                mismatches.append({**s, "pubchem_value_mg_l": pv})

        report["quantitative_comparison"]["aquatic_toxicity"] = {
            "sds_values_parsed": sds_aq_parsed,
            "pubchem_aquatic_lc50_mg_l": pub_lc50,
            "pubchem_aquatic_ec50_mg_l": pub_ec50,
            "matches_count": len(matches),
            "mismatches_count": len(mismatches),
            "matches": matches,
            "mismatches": mismatches,
        }

    return report

