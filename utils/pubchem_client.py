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


def _extract_toxicities(data: dict) -> list[dict[str, Any]]:
    """Extract toxicity entries (LD50, LC50, etc.) from PUG View for endpoints of interest."""
    tox_entries: list[dict[str, Any]] = []
    tox_keywords = ["tox", "safety", "hazard", "health", "exposure", "carcinogen"]

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
            tox_entries.append({"type": name or "Toxicity", "value": text[:300]})
        for sub in section.get("Section", []) or []:
            process_section(sub, heading)

    record = data.get("Record", {}) or {}
    for section in record.get("Section", []) or []:
        process_section(section)
    return tox_entries


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
        "toxicities": [],  # list of {type, value} for LD50/LC50 etc.
        "ld50": [],  # subset of toxicities containing LD50
        "lc50": [],  # subset of toxicities containing LC50
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
    return out
