"""
PubChem API client for compound data: identifiers, properties, GHS.
Uses pubchempy and PUG REST for CID resolution and PUG View for GHS/hazard.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Optional
from urllib.parse import quote

import pubchempy as pcp
import requests

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
PUG_VIEW_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view"
REQUEST_DELAY = 0.25
MAX_RETRIES = 3

GHS_H_CODE = re.compile(r"H\d+(?:\+\d+)?(?:\s*\([^)]+\))?")
GHS_P_CODE = re.compile(r"P\d+(?:\+\d+)?(?:\s*\([^)]+\))?")


def get_cid(identifier: str, input_type: str = "name") -> Optional[int]:
    """Resolve chemical identifier (CAS or name) to PubChem CID."""
    try:
        if input_type.lower() == "cid":
            return int(identifier)
        if input_type.lower() == "cas":
            url = f"{PUBCHEM_BASE}/compound/xref/RegistryID/{quote(identifier)}/cids/JSON"
            time.sleep(REQUEST_DELAY)
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            cids = data.get("IdentifierList", {}).get("CID", [])
            return int(cids[0]) if cids else None
        cids = pcp.get_cids(identifier, input_type, "compound")
        return cids[0] if cids else None
    except (pcp.BadRequestError, pcp.NotFoundError, ValueError, requests.RequestException):
        return None


def _fetch_pug_view(cid: int) -> Optional[dict]:
    """Fetch full PUG View compound record for GHS and hazard extraction."""
    url = f"{PUG_VIEW_BASE}/data/compound/{cid}/JSON"
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 503:
                time.sleep(2**attempt)
                continue
            r.raise_for_status()
        except (requests.RequestException, json.JSONDecodeError):
            if attempt == MAX_RETRIES - 1:
                return None
            time.sleep(2**attempt)
    return None


def _get_string_from_value(val: Any) -> str:
    """Extract display string from PUG View Value (StringWithMarkup)."""
    if val is None:
        return ""
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dict):
        swm = val.get("StringWithMarkup", [])
        if isinstance(swm, dict):
            swm = [swm]
        parts = [item["String"] for item in (swm or []) if isinstance(item, dict) and "String" in item]
        return " ".join(parts).strip() if parts else val.get("String", "")
    return ""


def _get_reference_urls(val: Any) -> list[str]:
    """Extract reference URLs from PUG View Value markup."""
    urls = []
    if not isinstance(val, dict):
        return urls
    swm = val.get("StringWithMarkup", [])
    if isinstance(swm, dict):
        swm = [swm]
    for item in (swm or []):
        if isinstance(item, dict):
            for m in (item.get("Markup") or []):
                if isinstance(m, dict) and m.get("URL"):
                    urls.append(m["URL"])
    return urls


def _extract_ghs_codes(data: dict) -> dict[str, Any]:
    """Extract GHS H/P codes and related from PUG View record."""
    result = {"h_codes": [], "p_codes": [], "signal_word": "", "pictograms": []}
    if not isinstance(data, dict):
        return result

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            h = obj.get("TOCHeading", "")
            if "GHS" in str(h) or "Classification" in str(h):
                for info in obj.get("Information", []) or []:
                    name = info.get("Name", "")
                    val = info.get("Value", {})
                    text = _get_string_from_value(val)
                    if "pictogram" in name.lower():
                        for x in val.get("StringWithMarkup") or []:
                            if isinstance(x, dict):
                                for m in (x.get("Markup") or []):
                                    extra = (m or {}).get("Extra", "").strip()
                                    if extra and extra not in result["pictograms"]:
                                        result["pictograms"].append(extra)
                    elif "signal" in name.lower():
                        result["signal_word"] = text or name
                    elif "hazard" in name.lower() and text:
                        for m in GHS_H_CODE.findall(text):
                            code = re.sub(r"\s*\([^)]+\)", "", m).strip()
                            if code and code not in result["h_codes"]:
                                result["h_codes"].append(code)
                    elif "precautionary" in name.lower() and text:
                        for m in GHS_P_CODE.findall(text):
                            code = re.sub(r"\s*\([^)]+\)", "", m).strip()
                            if code and code not in result["p_codes"]:
                                result["p_codes"].append(code)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    result["h_codes"] = list(dict.fromkeys(result["h_codes"]))
    result["p_codes"] = list(dict.fromkeys(result["p_codes"]))
    return result


def _extract_hazard_metrics(data: dict) -> dict[str, Any]:
    """Extract flash point, vapor pressure, etc. from PUG View."""
    result = {"flash_point": [], "vapor_pressure": [], "nfpa": [], "iarc": [], "prop65": []}
    if not isinstance(data, dict):
        return result

    def process_info(name: str, val: Any, heading: str) -> None:
        text = _get_string_from_value(val)
        if not text:
            return
        name_l = (name or "").lower()
        head_l = (heading or "").lower()
        if "flash point" in name_l or "flash point" in head_l:
            if text not in result["flash_point"]:
                result["flash_point"].append(text)
        if "vapor pressure" in name_l or "vapor pressure" in head_l:
            if text not in result["vapor_pressure"]:
                result["vapor_pressure"].append(text)
        if "nfpa" in name_l or "nfpa" in head_l:
            if text not in result["nfpa"]:
                result["nfpa"].append(text)
        if "iarc" in name_l or "iarc" in head_l:
            if text not in result["iarc"]:
                result["iarc"].append(text)
        if "proposition 65" in name_l or "prop 65" in name_l:
            if text not in result["prop65"]:
                result["prop65"].append(text)

    def walk_section(section: dict, heading: str = "") -> None:
        if not isinstance(section, dict):
            return
        h = section.get("TOCHeading", "") or heading
        for info in section.get("Information", []) or []:
            process_info(info.get("Name", ""), info.get("Value", {}), h)
        for sub in section.get("Section", []) or []:
            walk_section(sub, h)

    record = data.get("Record", {}) or {}
    for section in record.get("Section", []) or []:
        walk_section(section)
    return result


# Patterns for structured toxicity extraction (from quick_hazard_assessment/hazard_query_structured)
_UNIT_PATTERN = re.compile(r"(mg/kg|mg/m³|ppm|g/kg|mL/kg|mg/L|µg/kg|mg/m3|ppb)\b", re.I)
_SPECIES_ROUTE_PATTERN = re.compile(
    r"\b(rat|mouse|rabbit|dog|guinea pig|human|oral|dermal|inhalation|ip|iv|sc|ld50|lc50|fish|trout|daphnia|algae|aquatic)\b",
    re.I,
)


def _extract_toxicities(data: dict) -> list[dict[str, Any]]:
    """Extract toxicity entries with type, value, unit, species_route, source_section (full QHA-style)."""
    tox_entries: list[dict[str, Any]] = []
    tox_keywords = ["tox", "safety", "hazard", "health", "exposure", "pharmacokinetics", "carcinogen"]

    def process_section(section: dict, parent_heading: str = "") -> None:
        if not isinstance(section, dict):
            return
        heading = section.get("TOCHeading", "") or parent_heading
        if not any(kw in str(heading).lower() for kw in tox_keywords):
            for sub in section.get("Section", []) or []:
                process_section(sub, heading)
            return
        for info in section.get("Information", []) or []:
            name = info.get("Name", "")
            val = info.get("Value", {})
            text = _get_string_from_value(val)
            if not text:
                continue
            units = _UNIT_PATTERN.findall(text)
            species_route = _SPECIES_ROUTE_PATTERN.findall(text)
            refs = _get_reference_urls(val)
            entry = {
                "type": name or "Toxicity",
                "value": text[:400],
                "unit": units[0] if units else None,
                "species_route": list(dict.fromkeys(species_route)) if species_route else None,
                "source_section": heading or parent_heading,
                "reference_urls": refs[:5] if refs else None,
            }
            tox_entries.append(entry)
        for sub in section.get("Section", []) or []:
            process_section(sub, heading)

    record = data.get("Record", {}) or {}
    for section in record.get("Section", []) or []:
        process_section(section)
    return tox_entries


_SPECIES_TOKENS = {"rat", "mouse", "rabbit", "dog", "guinea pig", "human", "fish", "trout", "daphnia", "algae"}
_ROUTE_TOKENS = {"oral", "dermal", "inhalation", "ip", "iv", "sc"}

# Ecotoxicity parsing patterns
_ECOTOX_ENDPOINT_RE = re.compile(r"\b(LC50|EC50|LC10|LC20|LC90|EC10|EC20|EC90|NOEC|LOEC)\b", re.I)
_ECOTOX_DURATION_RE = re.compile(r"(\d+\s*(?:h|hr|hrs|hour|hours|d|day|days))\b", re.I)
_ECOTOX_VALUE_UNIT_RE = re.compile(
    r"([<>~]?\s*\d+(?:[.,]\d+)?)\s*(mg/L|µg/L|ug/L|mg/kg|g/L|mg/l)\b", re.I
)
_ECOTOX_CI_RE = re.compile(
    r"(?:CI|confidence interval)[^0-9]*([0-9]+(?:\.\d+)?)\s*[–-]\s*([0-9]+(?:\.\d+)?)",
    re.I,
)


def _classify_route_and_species(entry: dict[str, Any]) -> tuple[str, str]:
    """Infer exposure pathway and species from toxicity entry. Returns (route, species)."""
    val = (entry.get("value") or "").lower()
    sr = [s.strip().lower() for s in (entry.get("species_route") or []) if s]
    # Split tokens into species vs route
    species_parts = [x for x in sr if x in _SPECIES_TOKENS or (x not in _ROUTE_TOKENS and x not in ("ld50", "lc50"))]
    route_parts = [x for x in sr if x in _ROUTE_TOKENS]
    species = ", ".join(dict.fromkeys(species_parts)) if species_parts else "—"
    route = "Other"

    if "fish" in val or "trout" in val or "daphnia" in val or "algae" in val or "aquatic" in val:
        route = "Ecotoxicity (aquatic)"
        if not species_parts:
            species = "fish" if "fish" in val or "trout" in val else "Daphnia" if "daphnia" in val else "algae" if "algae" in val else "aquatic"
    elif "dermal" in val or "skin" in val or "dermal" in route_parts:
        route = "Dermal"
        if not species_parts and "rabbit" in val:
            species = "rabbit"
        elif not species_parts and "rat" in val:
            species = "rat"
    elif "inhalation" in val or "inhaled" in val or "mg/m" in val or "ppm" in val or "inhalation" in route_parts:
        route = "Inhalation"
        if not species_parts and "rat" in val:
            species = "rat"
    elif "oral" in val or "po " in val or "oral" in route_parts or ("rat" in val and "dermal" not in val and "inhalation" not in val):
        route = "Oral"
        if not species_parts and "rat" in val:
            species = "rat"
        elif not species_parts and "mouse" in val:
            species = "mouse"

    if species == "—" and species_parts:
        species = ", ".join(dict.fromkeys(species_parts))
    return (route, species)


def _parse_ecotox_text(raw: str) -> dict[str, Any]:
    """
    Heuristic parser for PubChem ecotoxicity text.
    Returns endpoint, duration, numeric value, units, CI bounds, and leftover conditions.
    """
    if not raw:
        return {}
    text = raw.strip()
    out: dict[str, Any] = {}

    # Endpoint (LC50, EC50, NOEC, LOEC, etc.)
    m_ep = _ECOTOX_ENDPOINT_RE.search(text)
    if m_ep:
        out["endpoint"] = m_ep.group(1).upper()

    # Duration (e.g. 96 h, 21 d)
    m_dur = _ECOTOX_DURATION_RE.search(text)
    if m_dur:
        out["duration"] = m_dur.group(1)

    # Primary numeric value + units (mg/L etc.)
    m_val = _ECOTOX_VALUE_UNIT_RE.search(text)
    if m_val:
        val_str = m_val.group(1).replace(",", "")
        units = m_val.group(2)
        out["unit"] = units
        try:
            out["value_num"] = float(re.sub(r"[<>~]\s*", "", val_str))
            out["quantitative"] = True
        except ValueError:
            out["value_num"] = None
            out["quantitative"] = False
    else:
        out["quantitative"] = False

    # Confidence interval (if present)
    m_ci = _ECOTOX_CI_RE.search(text)
    if m_ci:
        try:
            out["ci_low"] = float(m_ci.group(1))
            out["ci_high"] = float(m_ci.group(2))
        except ValueError:
            pass

    # Conditions: for now, keep the full text for display/filtering
    out["conditions"] = text
    return out


def _extract_ecotoxicity(ghs: dict, toxicities: list[dict]) -> dict[str, Any]:
    """Extract ecotoxicity endpoints (aquatic LC50/EC50, species, H4xx codes)."""
    out = {
        "aquatic_lc50_mg_l": None,
        "aquatic_ec50_mg_l": None,
        "aquatic_species": None,
        "aquatic_value_raw": None,
        "h_codes_aquatic": [],
        "entries": [],  # list of {value, species, unit} for display
    }
    h_codes = ghs.get("h_codes") or []
    out["h_codes_aquatic"] = [h for h in h_codes if h.startswith("H4")]
    for t in toxicities:
        val = (t.get("value") or "").lower()
        if "fish" in val or "trout" in val or "daphnia" in val or "algae" in val or "aquatic" in val:
            raw = t.get("value", "")
            species = "fish" if "fish" in val or "trout" in val else "Daphnia" if "daphnia" in val else "algae" if "algae" in val else "aquatic"
            parsed = _parse_ecotox_text(raw)
            num = parsed.get("value_num")
            if num is not None:
                if (parsed.get("endpoint") or "").upper().startswith("EC"):
                    out["aquatic_ec50_mg_l"] = num
                else:
                    out["aquatic_lc50_mg_l"] = out["aquatic_lc50_mg_l"] or num
                out["aquatic_value_raw"] = raw[:250]
                out["aquatic_species"] = out["aquatic_species"] or species
            entry = {
                "value": raw[:400],
                "species": species,
                "unit": parsed.get("unit") or "mg/L",
            }
            entry.update(parsed)
            out["entries"].append(entry)
    if not out["entries"]:
        for t in toxicities:
            if "LC50" in (t.get("value") or "").upper() and "mg" in (t.get("value") or "").lower() and "L" in (t.get("value") or ""):
                raw = t.get("value") or ""
                parsed = _parse_ecotox_text(raw)
                entry = {
                    "value": raw[:400],
                    "species": "—",
                    "unit": parsed.get("unit") or t.get("unit") or "mg/L",
                }
                entry.update(parsed)
                out["entries"].append(entry)
                if out["aquatic_lc50_mg_l"] is None:
                    out["aquatic_value_raw"] = (t.get("value") or "")[:250]
                break
    return out


def _compute_exposure_bands(toxicities: list[dict]) -> dict[str, Any]:
    """Compute GHS-style exposure bands for oral, dermal, inhalation (from QHA exposure_bands.py)."""
    oral_bands = [(5, 1), (50, 2), (300, 3), (2000, 4), (5000, 5)]
    dermal_bands = [(50, 1), (200, 2), (1000, 3), (2000, 4), (5000, 5)]

    def band_from_value(value: float, bands: list[tuple[float, int]]) -> int:
        for threshold, band in bands:
            if value <= threshold:
                return band
        return 5

    def extract_ld50_oral():
        for t in toxicities or []:
            v = (t.get("value") or "").lower()
            if "ld50" not in v or "mg" not in v or "kg" not in v:
                continue
            if "dermal" in v or "skin" in v:
                continue
            m = re.search(r"(\d+(?:\.\d+)?)\s*mg\s*/\s*kg", v)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
        return None

    def extract_ld50_dermal():
        for t in toxicities or []:
            v = (t.get("value") or "").lower()
            if "dermal" not in v and "skin" not in v:
                continue
            if "ld50" not in v:
                continue
            m = re.search(r"(\d+(?:\.\d+)?)\s*mg\s*/\s*kg", v)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
        return None

    def extract_lc50_inhalation():
        for t in toxicities or []:
            v = (t.get("value") or "").lower()
            if "lc50" not in v:
                continue
            m = re.search(r"(\d+(?:[.,]\d+)*)\s*mg\s*/\s*m", v) or re.search(r"(\d+(?:[.,]\d+)*)\s*mg/m3", v, re.I)
            if m:
                try:
                    return float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
        return None

    out = {"oral": {}, "dermal": {}, "inhalation": {}}
    lo = extract_ld50_oral()
    if lo is not None:
        out["oral"] = {"ld50_mg_kg": lo, "band": band_from_value(lo, oral_bands), "source": "PubChem"}
    ld_ = extract_ld50_dermal()
    if ld_ is not None:
        out["dermal"] = {"ld50_mg_kg": ld_, "band": band_from_value(ld_, dermal_bands), "source": "PubChem"}
    li = extract_lc50_inhalation()
    if li is not None:
        b = 1 if li <= 100 else 2 if li <= 500 else 3 if li <= 2500 else 4 if li <= 20000 else 5
        out["inhalation"] = {"lc50_mg_m3": li, "band": b, "source": "PubChem"}
    return out


def get_compound_data(identifier: str, input_type: str = "auto") -> Optional[dict[str, Any]]:
    """
    Fetch compound data for display: SMILES, formula, MW, IUPAC name, GHS, flash point, etc.
    identifier: CAS number (e.g. 67-64-1) or chemical name.
    input_type: 'cas', 'name', or 'auto' (guess from format).
    Returns dict with keys: cid, smiles, formula, mw, iupac_name, ghs, flash_point, vapor_pressure, etc.
    Returns None if compound not found.
    """
    if input_type == "auto":
        # CAS-like: digits-digits-digits
        if re.match(r"^\d{1,9}-\d{2}-\d$", (identifier or "").strip()):
            input_type = "cas"
        else:
            input_type = "name"
    cid = get_cid(identifier, input_type)
    if not cid:
        return None
    try:
        comp = pcp.Compound.from_cid(cid)
    except Exception:
        return None
    # Basic props from pubchempy
    smiles = getattr(comp, "smiles", None) or getattr(comp, "isomeric_smiles", None) or getattr(comp, "canonical_smiles", None)
    formula = getattr(comp, "molecular_formula", None)
    mw = getattr(comp, "molecular_weight", None)
    iupac_name = getattr(comp, "iupac_name", None) or getattr(comp, "iupac_name_legacy", None)
    if mw is not None:
        mw = f"{mw:.2f}" if isinstance(mw, (int, float)) else str(mw)
    out = {
        "cid": cid,
        "smiles": smiles,
        "formula": formula,
        "mw": mw,
        "iupac_name": iupac_name,
        "ghs": {"h_codes": [], "p_codes": [], "signal_word": "", "pictograms": []},
        "flash_point": [],  # list of strings, one per value
        "vapor_pressure": [],  # list of strings
        "toxicities": [],  # list of {type, value, unit, species_route, route, species} for LD50/LC50 etc.
        "ld50": [],  # subset of toxicities containing LD50
        "lc50": [],  # subset of toxicities containing LC50
        "ecotoxicity": {"aquatic_lc50_mg_l": None, "aquatic_ec50_mg_l": None, "aquatic_species": None, "h_codes_aquatic": [], "entries": []},
        "exposure_bands": {"oral": {}, "dermal": {}, "inhalation": {}},
        "nfpa": None,
        "iarc": None,
        "prop65": None,
    }
    pug = _fetch_pug_view(cid)
    if pug:
        out["ghs"] = _extract_ghs_codes(pug)
        hazards = _extract_hazard_metrics(pug)
        out["flash_point"] = list(hazards["flash_point"]) if hazards["flash_point"] else []
        out["vapor_pressure"] = list(hazards["vapor_pressure"]) if hazards["vapor_pressure"] else []
        out["nfpa"] = "; ".join(hazards["nfpa"]) if hazards["nfpa"] else None
        out["iarc"] = "; ".join(hazards["iarc"]) if hazards["iarc"] else None
        out["prop65"] = "; ".join(hazards["prop65"]) if hazards["prop65"] else None
        tox = _extract_toxicities(pug)
        out["toxicities"] = tox
        out["ld50"] = [t["value"] for t in tox if "LD50" in (t.get("value") or "").upper()]
        out["lc50"] = [t["value"] for t in tox if "LC50" in (t.get("value") or "").upper()]
        out["ecotoxicity"] = _extract_ecotoxicity(out["ghs"], tox)
        out["exposure_bands"] = _compute_exposure_bands(tox)
        # Add route/species for each toxicity for display
        for t in out["toxicities"]:
            route, species = _classify_route_and_species(t)
            t["route"] = route
            t["species"] = species
    return out
