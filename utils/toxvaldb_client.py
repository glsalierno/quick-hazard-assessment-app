"""
Optional client for EPA CompTox ToxValDB API.
Fetches quantitative toxicity data when an API key is provided.
Not required for the app; used only when COMPTOX_API_KEY is set (e.g. in Streamlit secrets).
"""

from __future__ import annotations

from typing import Any, Optional

import requests

# Cache is applied at call site (app) to avoid importing streamlit here if unused
TOXVAL_BASE = "https://api-ccte.epa.gov/chemical/search/by-dtxsid/dtxsid/"


def fetch_toxval_data(dtxsid: str, api_key: Optional[str] = None) -> Optional[dict[str, list[dict]]]:
    """
    Fetch toxicity data from EPA CompTox ToxValDB for a given DTXSID.

    Args:
        dtxsid: DTXSID identifier (e.g. DTXSID7020182).
        api_key: EPA CompTox API key (optional). If missing, returns None.

    Returns:
        Dictionary of category -> list of records (value, units, species, route, etc.),
        or None if key missing, request fails, or no data.
    """
    if not api_key or not dtxsid or not str(dtxsid).strip().upper().startswith("DTXSID"):
        return None

    url = TOXVAL_BASE + str(dtxsid).strip()
    headers = {"x-api-key": api_key}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        return _process_toxval_response(data)
    except Exception:
        return None


def _process_toxval_response(raw: Any) -> dict[str, list[dict]]:
    """Turn API response into category -> list of records for formatter."""
    out: dict[str, list[dict]] = {
        "acute_toxicity": [],
        "carcinogenicity": [],
        "genotoxicity": [],
        "repeated_dose": [],
        "developmental": [],
        "reproductive": [],
        "neurotoxicity": [],
        "ecotoxicity": [],
        "other": [],
    }

    if not raw or not isinstance(raw, dict):
        return out

    # Handle different possible response shapes (list of results or nested 'data')
    records = raw.get("data") or raw.get("results") or []
    if isinstance(raw, list):
        records = raw

    for rec in records:
        if not isinstance(rec, dict):
            continue
        study_type = (rec.get("studyType") or rec.get("study_type") or "").lower()
        category = _categorize_study(study_type)
        val = rec.get("toxValNumeric") or rec.get("toxval_numeric") or rec.get("value")
        units = rec.get("toxValUnits") or rec.get("toxval_units") or rec.get("units", "")
        entry = {
            "study_type": rec.get("studyType") or rec.get("study_type") or "Unknown",
            "value": val,
            "units": units,
            "species": rec.get("species") or rec.get("speciesName", ""),
            "route": rec.get("exposureRoute") or rec.get("exposure_route", ""),
            "reference": rec.get("reference") or rec.get("citation", ""),
            "toxval_numeric": val,
            "toxval_units": units,
        }
        if category not in out:
            out["other"].append(entry)
        else:
            out[category].append(entry)

    return out


def _categorize_study(study_type: str) -> str:
    if any(x in study_type for x in ["ld50", "lc50", "acute", "lethal"]):
        return "acute_toxicity"
    if any(x in study_type for x in ["carcinogen", "cancer"]):
        return "carcinogenicity"
    if any(x in study_type for x in ["genotox", "mutagen", "ames"]):
        return "genotoxicity"
    if "repeat" in study_type and "dose" in study_type:
        return "repeated_dose"
    if "develop" in study_type:
        return "developmental"
    if "reproduc" in study_type:
        return "reproductive"
    if "neuro" in study_type:
        return "neurotoxicity"
    if any(x in study_type for x in ["ecotox", "fish", "daphnia", "algae"]):
        return "ecotoxicity"
    return "other"
