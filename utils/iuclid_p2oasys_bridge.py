"""
Map offline IUCLID (unified_lookup payload) into ``extra_sources`` for P2OASys.

Uses normalized endpoint rows when present; falls back to cached ``iuclid_endpoints`` snippets.
For duplicate-type toxicities (e.g. several oral LD50s), the **lowest** numeric value in
mg/kg (oral/dermal) or ppm / mg/m³ (inhalation LC50) or mg/L (aquatic LC50/EC50) is kept —
lowest dose = most conservative (highest hazard) for acute endpoints.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _parse_first_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and val != val:  # NaN
            return None
        return float(val)
    s = str(val).strip().replace(",", ".")
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _text_blob(row: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in (
        "endpoint_name",
        "endpoint_name_label",
        "study_result_type_label",
        "administration_exposure_label",
        "effect_endpoint_label",
        "species_label",
    ):
        parts.append(str(row.get(k) or ""))
    return " ".join(parts).lower()


def _normalize_mg_per_kg(val: float, unit_raw: str) -> Optional[float]:
    u = (unit_raw or "").lower().replace("μ", "u").replace(" ", "")
    if not u:
        return None
    if "mg/kg" in u or "mgkg-1" in u or "mg.kg" in u:
        return val
    if "g/kg" in u and "mg" not in u:
        return val * 1000.0
    if "ug/kg" in u or "µg/kg" in u or "microg/kg" in u:
        return val / 1000.0
    if u == "kg/kg" or "/kg" in u:
        return None
    return None


def _normalize_inhal_lc(val: float, unit_raw: str) -> tuple[Optional[float], Optional[float]]:
    """
    Returns (ppm_value, mg_m3_value) — at most one non-None when unit is recognized.
    """
    u = (unit_raw or "").lower().replace("μ", "u").replace(" ", "")
    if "ppm" in u:
        return val, None
    if "mg/m" in u or "mgm-3" in u or "mg/m3" in u:
        return None, val
    if "g/m" in u:
        return None, val * 1000.0
    return None, None


def _normalize_aquatic_mg_l(val: float, unit_raw: str) -> Optional[float]:
    u = (unit_raw or "").lower().replace("μ", "u").replace(" ", "")
    if "mg/l" in u or "mgl-1" in u:
        return val
    if "g/l" in u and "mg" not in u:
        return val * 1000.0
    if "µg/l" in u or "ug/l" in u:
        return val / 1000.0
    return None


def _classify_acute_route(blob: str) -> str:
    if "dermal" in blob or "skin" in blob and "absorption" in blob:
        return "dermal"
    if "inhalation" in blob or "inhalative" in blob or "respiratory" in blob or "nose only" in blob or "whole body" in blob:
        return "inhalation"
    if (
        "oral" in blob
        or "gavage" in blob
        or "feed" in blob
        or "drinking water" in blob
        or "intraperitoneal" in blob
        or re.search(r"\bi\.?p\.?\b", blob)
        or re.search(r"\bip\b", blob)
    ):
        return "oral"
    return ""


def _is_reliable_klimisch(row: dict[str, Any]) -> bool:
    """
    Prefer Klimisch 1/2 when coded; if reliability is blank, accept the row.

    Hard-reject only when an explicit unreliable code/label is present (3/4 or
    "not reliable" / "not assignable"). Blank reliability is common in snippet
    caches and was discarding usable acute/aquatic rows.
    """
    code = str(row.get("reliability_code") or "").strip()
    label = str(row.get("reliability_label") or "").lower()
    if not code and not label:
        return True
    if code in {"1", "2"}:
        return True
    if code in {"3", "4"}:
        return False
    if "reliable without restriction" in label or "reliable with restrictions" in label:
        return True
    if "not reliable" in label or "not assignable" in label:
        return False
    # Unknown non-empty label/code → keep (downstream still takes min dose).
    return True


def _is_noael(blob: str) -> bool:
    return "noael" in blob or "noec" in blob or "noec " in blob


def _is_loael(blob: str) -> bool:
    return "loael" in blob or "loec" in blob or "loec " in blob


def _is_ld50_row(blob: str) -> bool:
    return "ld50" in blob or "lethal dose fifty" in blob or ("acute toxicity" in blob and "mortality" in blob and "ld" in blob)


def _is_lc50_row(blob: str) -> bool:
    return "lc50" in blob or ("lethal concentration" in blob and "50" in blob) or "lethal concentration fifty" in blob


def _is_ec50_aquatic(blob: str) -> bool:
    return ("ec50" in blob or "el50" in blob or "ic50" in blob) and (
        "fish" in blob
        or "daphnia" in blob
        or "algae" in blob
        or "aquatic" in blob
        or "crustace" in blob
        or "pimephales" in blob
        or "oncorhynchus" in blob
        or "cyprinus" in blob
        or "danio" in blob
    )


def _is_lc50_aquatic(blob: str) -> bool:
    return _is_lc50_row(blob) and (
        "fish" in blob
        or "daphnia" in blob
        or "algae" in blob
        or "aquatic" in blob
        or "crustace" in blob
        or "pimephales" in blob
        or "oncorhynchus" in blob
        or "short-term toxicity to fish" in blob
        or "long-term toxicity to fish" in blob
    )


def _collect_from_normalized_row(
    row: dict[str, Any],
    oral: list[float],
    dermal: list[float],
    ppm: list[float],
    mgm3: list[float],
    aqua: list[float],
    rep_noael_oral: list[float],
    rep_loael_oral: list[float],
    rep_noael_inh_ppm: list[float],
    rep_loael_inh_ppm: list[float],
    rep_noael_inh_mgm3: list[float],
    rep_loael_inh_mgm3: list[float],
) -> None:
    if not _is_reliable_klimisch(row):
        return
    val = _parse_first_float(row.get("effect_level_value"))
    if val is None:
        return
    unit_raw = str(row.get("effect_level_unit") or row.get("effect_level_unit_label") or "")
    blob = _text_blob(row)
    adm = str(row.get("administration_exposure_label") or "").lower()
    route = _classify_acute_route(blob) or _classify_acute_route(adm)

    mgkg = _normalize_mg_per_kg(val, unit_raw)
    if mgkg is not None and _is_ld50_row(blob):
        if "dermal" in adm or route == "dermal":
            dermal.append(mgkg)
        elif "inhalation" in adm or route == "inhalation":
            pass
        else:
            oral.append(mgkg)
        return

    p, m3 = _normalize_inhal_lc(val, unit_raw)
    if (p is not None or m3 is not None) and _is_lc50_row(blob) and (
        route == "inhalation" or "inhalation" in adm or "inhalation" in blob or "nose only" in blob
    ):
        if p is not None:
            ppm.append(p)
        if m3 is not None:
            mgm3.append(m3)
        return

    aq = _normalize_aquatic_mg_l(val, unit_raw)
    if aq is not None and (_is_lc50_aquatic(blob) or _is_ec50_aquatic(blob)):
        aqua.append(aq)
        return

    # Repeated-dose / chronic-like effect levels (prefer NOAEL over LOAEL downstream).
    is_rep = (
        "repeated dose toxicity" in blob
        or "subchronic" in blob
        or "chronic" in blob
        or "noael" in blob
        or "loael" in blob
    )
    if not is_rep:
        return
    if route in ("oral", "dermal") or "oral" in adm or "gavage" in blob or "drinking water" in blob:
        mgkg = _normalize_mg_per_kg(val, unit_raw)
        if mgkg is not None:
            if _is_noael(blob):
                rep_noael_oral.append(mgkg)
            elif _is_loael(blob):
                rep_loael_oral.append(mgkg)
            return
    p, m3 = _normalize_inhal_lc(val, unit_raw)
    if route == "inhalation" or "inhalation" in adm or "inhalation" in blob or "nose only" in blob:
        if p is not None:
            if _is_noael(blob):
                rep_noael_inh_ppm.append(p)
            elif _is_loael(blob):
                rep_loael_inh_ppm.append(p)
        if m3 is not None:
            if _is_noael(blob):
                rep_noael_inh_mgm3.append(m3)
            elif _is_loael(blob):
                rep_loael_inh_mgm3.append(m3)


def _collect_from_raw_endpoint_row(
    row: dict[str, Any],
    oral: list[float],
    dermal: list[float],
    ppm: list[float],
    mgm3: list[float],
    aqua: list[float],
) -> None:
    """Cached ``iuclid_endpoints`` rows use result_value / unit / labels."""
    val = _parse_first_float(row.get("result") or row.get("result_value"))
    if val is None:
        return
    unit_raw = str(row.get("units") or row.get("unit") or "")
    parts = [
        str(row.get("endpoint_name") or ""),
        str(row.get("endpoint_name_label") or ""),
        str(row.get("species") or ""),
        str(row.get("species_label") or ""),
        str(row.get("raw_text") or ""),
    ]
    blob = " ".join(parts).lower()
    route = _classify_acute_route(blob)

    mgkg = _normalize_mg_per_kg(val, unit_raw)
    if mgkg is not None and _is_ld50_row(blob):
        if route == "dermal":
            dermal.append(mgkg)
        elif route != "inhalation":
            oral.append(mgkg)
        return

    p, m3 = _normalize_inhal_lc(val, unit_raw)
    if (p is not None or m3 is not None) and _is_lc50_row(blob) and (route == "inhalation" or "inhalation" in blob):
        if p is not None:
            ppm.append(p)
        if m3 is not None:
            mgm3.append(m3)
        return

    aq = _normalize_aquatic_mg_l(val, unit_raw)
    if aq is not None and (_is_lc50_aquatic(blob) or _is_ec50_aquatic(blob)):
        aqua.append(aq)


def _h_codes_from_cl_rows(cl_rows: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for r in cl_rows:
        for key in ("h_statement_code", "hazard_code", "h_statement_code_label"):
            s = str(r.get(key) or "").strip().upper()
            for m in re.finditer(r"\bH\d{3}(?:\s*\+\s*H\d{3})?\b", s):
                code = m.group(0).replace(" ", "")
                if code not in seen:
                    seen.add(code)
                    out.append(code)
    return out


def _flash_vp_from_rows(norm_rows: list[dict[str, Any]], raw_rows: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    flash: list[str] = []
    vp: list[str] = []
    for row in norm_rows + raw_rows:
        if not isinstance(row, dict):
            continue
        blob = _text_blob(row) if "endpoint_name_label" in row or "endpoint_name" in row else ""
        if not blob:
            blob = " ".join(str(row.get(k) or "") for k in row).lower()
        val = _parse_first_float(row.get("effect_level_value") or row.get("result") or row.get("result_value"))
        if val is None:
            continue
        unit_raw = str(row.get("effect_level_unit") or row.get("unit") or row.get("units") or "")
        ulow = unit_raw.lower()
        if "flash" in blob and "point" in blob and ("°c" in ulow or " deg c" in ulow or ulow.endswith("c") or "celsius" in ulow):
            flash.append(f"{val} °C")
        if "vapour pressure" in blob or "vapor pressure" in blob:
            u = ulow
            if "mm hg" in u or "mmhg" in u or "mm mercury" in u:
                vp.append(f"{val} mmHg")
            elif "pa" in u and "mpa" not in u and "kpa" not in u:
                mmhg = val / 133.322
                vp.append(f"{mmhg:.6g} mmHg")
            elif "kpa" in u:
                mmhg = val * 7.50062
                vp.append(f"{mmhg:.6g} mmHg")
    return flash, vp


def build_extra_sources_from_iuclid_unified(data: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Build ``extra_sources`` from ``unified_lookup`` output.

    Returns None when there is nothing to contribute (no UUIDs / no usable rows).
    """
    if not data:
        return None
    uuids = data.get("iuclid_uuids") or []
    if not uuids:
        return None

    oral: list[float] = []
    dermal: list[float] = []
    ppm: list[float] = []
    mgm3: list[float] = []
    aqua: list[float] = []
    rep_noael_oral: list[float] = []
    rep_loael_oral: list[float] = []
    rep_noael_inh_ppm: list[float] = []
    rep_loael_inh_ppm: list[float] = []
    rep_noael_inh_mgm3: list[float] = []
    rep_loael_inh_mgm3: list[float] = []

    for row in data.get("iuclid_endpoints_normalized") or []:
        if isinstance(row, dict):
            _collect_from_normalized_row(
                row,
                oral,
                dermal,
                ppm,
                mgm3,
                aqua,
                rep_noael_oral,
                rep_loael_oral,
                rep_noael_inh_ppm,
                rep_loael_inh_ppm,
                rep_noael_inh_mgm3,
                rep_loael_inh_mgm3,
            )

    for row in data.get("iuclid_endpoints") or []:
        if isinstance(row, dict):
            _collect_from_raw_endpoint_row(row, oral, dermal, ppm, mgm3, aqua)

    cl_rows = [r for r in (data.get("iuclid_cl_rows") or []) if isinstance(r, dict)]
    h_codes = _h_codes_from_cl_rows(cl_rows)

    norm = [r for r in (data.get("iuclid_endpoints_normalized") or []) if isinstance(r, dict)]
    raw_eps = [r for r in (data.get("iuclid_endpoints") or []) if isinstance(r, dict)]
    flash_list, vp_list = _flash_vp_from_rows(norm, raw_eps)

    toxicities: list[dict[str, Any]] = []
    if oral:
        v = min(oral)
        toxicities.append(
            {
                "value": f"LD50 {v} mg/kg bw",
                "unit": "mg/kg",
                "species_route": ["oral", "IUCLID"],
            }
        )
    if dermal:
        v = min(dermal)
        toxicities.append(
            {
                "value": f"LD50 {v} mg/kg bw (dermal)",
                "unit": "mg/kg",
                "species_route": ["dermal", "IUCLID"],
            }
        )
    if ppm:
        v = min(ppm)
        toxicities.append(
            {
                "value": f"LC50 {v} ppm inhalation",
                "unit": "ppm",
                "species_route": ["inhalation", "IUCLID"],
            }
        )
    if mgm3:
        v = min(mgm3)
        toxicities.append(
            {
                "value": f"LC50 {v} mg/m³ inhalation",
                "unit": "mg/m³",
                "species_route": ["inhalation", "IUCLID"],
            }
        )
    if aqua:
        v = min(aqua)
        toxicities.append(
            {
                "value": f"LC50/EC50 {v} mg/L fish",
                "unit": "mg/L",
                "species_route": ["aquatic", "IUCLID"],
            }
        )
    # Repeated-dose conservative values: prefer NOAEL; use LOAEL as fallback.
    if rep_noael_oral or rep_loael_oral:
        if rep_noael_oral:
            v = min(rep_noael_oral)
            tag = "NOAEL"
        else:
            v = min(rep_loael_oral)
            tag = "LOAEL"
        toxicities.append(
            {
                "value": f"{tag} {v} mg/kg/day oral",
                "unit": "mg/kg/day",
                "species_route": ["oral", "IUCLID", "repeated-dose"],
            }
        )
    if rep_noael_inh_ppm or rep_loael_inh_ppm:
        if rep_noael_inh_ppm:
            v = min(rep_noael_inh_ppm)
            tag = "NOAEC"
        else:
            v = min(rep_loael_inh_ppm)
            tag = "LOAEC"
        toxicities.append(
            {
                "value": f"{tag} {v} ppm inhalation",
                "unit": "ppm",
                "species_route": ["inhalation", "IUCLID", "repeated-dose"],
            }
        )
    if rep_noael_inh_mgm3 or rep_loael_inh_mgm3:
        if rep_noael_inh_mgm3:
            v = min(rep_noael_inh_mgm3)
            tag = "NOAEC"
        else:
            v = min(rep_loael_inh_mgm3)
            tag = "LOAEC"
        toxicities.append(
            {
                "value": f"{tag} {v} mg/m³ inhalation",
                "unit": "mg/m³",
                "species_route": ["inhalation", "IUCLID", "repeated-dose"],
            }
        )

    extra: dict[str, Any] = {"toxicities": [], "ghs": {"h_codes": [], "p_codes": []}, "hazard_metrics": {}}
    extra["toxicities"] = toxicities
    if h_codes:
        extra["ghs"]["h_codes"] = h_codes
    if flash_list:
        extra["hazard_metrics"]["flash_point"] = flash_list[:3]
    if vp_list:
        extra["hazard_metrics"].setdefault("other_designations", []).extend(vp_list[:3])

    if not toxicities and not h_codes and not flash_list and not vp_list:
        logger.debug("IUCLID unified data had UUIDs but no P2OASys-mappable fields for CAS %s", data.get("cas"))
        return None

    return extra
