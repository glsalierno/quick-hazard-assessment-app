"""
Data formatting utilities for toxicity display and download.
Prioritizes quantitative toxicity data and supports comprehensive export without truncation.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

import pandas as pd


def _has_numeric_value(val: Any) -> bool:
    """True if value looks like a number (with optional unit suffix)."""
    if val is None:
        return False
    s = str(val).strip()
    # Match leading number (e.g. "123", "1.5", ">100", "~50 mg/kg")
    return bool(re.match(r"^[<>~]?\s*\d+(?:[.,]\d+)?", s))


def prioritize_toxicity_data(
    pubchem_data: dict[str, Any],
    toxval_data: Optional[dict[str, list[dict]]] = None,
) -> dict[str, list[dict]]:
    """
    Split toxicity data into quantitative (value + unit) first, then categorical.
    Uses pubchem_data['toxicities'] and optional toxval_data from ToxValDB.
    """
    prioritized: dict[str, list[dict]] = {
        "quantitative": [],
        "categorical": [],
    }

    # From PubChem toxicities
    for t in pubchem_data.get("toxicities") or []:
        value = t.get("value") or ""
        unit = t.get("unit")
        endpoint = (t.get("type") or "Toxicity").strip()
        route = t.get("route") or "—"
        species = t.get("species") or "—"
        source_section = t.get("source_section") or ""

        item = {
            "source": "PubChem",
            "endpoint": endpoint,
            "value": value,
            "units": unit or "",
            "species": species,
            "route": route,
            "details": source_section[:80] + "..." if len(source_section or "") > 80 else (source_section or ""),
        }

        if unit and _has_numeric_value(value):
            prioritized["quantitative"].append(item)
        else:
            prioritized["categorical"].append(item)

    # From ToxValDB if provided
    if toxval_data:
        for category, records in toxval_data.items():
            if not isinstance(records, list):
                continue
            for rec in records:
                val = rec.get("value") or rec.get("toxval_numeric", "")
                units = rec.get("units") or rec.get("toxval_units", "")
                item = {
                    "source": "ToxValDB",
                    "category": category,
                    "endpoint": rec.get("study_type", rec.get("endpoint", category)),
                    "value": str(val),
                    "units": str(units),
                    "species": rec.get("species", ""),
                    "route": rec.get("route", rec.get("exposure_route", "")),
                    "details": rec.get("reference", "")[:80] or "",
                }
                if units and _has_numeric_value(val):
                    prioritized["quantitative"].append(item)
                else:
                    prioritized["categorical"].append(item)

    return prioritized


def build_toxicity_display_df(prioritized: dict[str, list[dict]]) -> pd.DataFrame:
    """Build a single DataFrame for display: Type, Endpoint, Value, Units, Species, Route, Source."""
    rows = []
    for item in prioritized.get("quantitative", []):
        rows.append({
            "Type": "Quantitative",
            "Endpoint": item.get("endpoint", item.get("category", "—")),
            "Value": str(item.get("value", "")),
            "Units": item.get("units", ""),
            "Species": item.get("species", ""),
            "Route": item.get("route", ""),
            "Source": item.get("source", ""),
        })
    for item in prioritized.get("categorical", []):
        rows.append({
            "Type": "Categorical",
            "Endpoint": item.get("endpoint", item.get("category", "—")),
            "Value": (str(item.get("value", "")))[:200],
            "Units": item.get("units", ""),
            "Species": item.get("species", ""),
            "Route": item.get("route", ""),
            "Source": item.get("source", ""),
        })
    return pd.DataFrame(rows)


def create_comprehensive_download_data(
    clean_cas: str,
    pubchem_data: dict[str, Any],
    dsstox_info: Optional[dict] = None,
    toxval_data: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Build full structure for download: identifiers, properties, GHS, all toxicity rows (no truncation).
    """
    ghs = pubchem_data.get("ghs") or {}
    eco = pubchem_data.get("ecotoxicity") or {}
    fp = pubchem_data.get("flash_point")
    vp = pubchem_data.get("vapor_pressure")
    fp_str = "; ".join(fp) if isinstance(fp, list) else (fp or "")
    vp_str = "; ".join(vp) if isinstance(vp, list) else (vp or "")

    return {
        "query_info": {
            "timestamp": datetime.now().isoformat(),
            "cas_number": clean_cas,
        },
        "identifiers": {
            "iupac_name": pubchem_data.get("iupac_name", ""),
            "cid": pubchem_data.get("cid"),
            "formula": pubchem_data.get("formula", ""),
            "mw": pubchem_data.get("mw", ""),
            "smiles": pubchem_data.get("smiles", ""),
            "dsstox": dsstox_info or {},
        },
        "physical_properties": {
            "flash_point": fp_str,
            "vapor_pressure": vp_str,
        },
        "ghs": {
            "h_codes": ghs.get("h_codes") or [],
            "p_codes": ghs.get("p_codes") or [],
            "signal_word": ghs.get("signal_word", ""),
        },
        "ecotoxicity": {
            "h_codes_aquatic": eco.get("h_codes_aquatic") or [],
            "aquatic_lc50_mg_l": eco.get("aquatic_lc50_mg_l"),
            "aquatic_ec50_mg_l": eco.get("aquatic_ec50_mg_l"),
            "entries": eco.get("entries") or [],
        },
        "toxicity_endpoints": [
            {
                "exposure_pathway": t.get("route", ""),
                "species": t.get("species", ""),
                "endpoint": t.get("type", ""),
                "value": t.get("value", ""),
                "unit": t.get("unit", ""),
                "source_section": t.get("source_section", ""),
            }
            for t in (pubchem_data.get("toxicities") or [])
        ],
        "toxvaldb_available": toxval_data is not None,
    }


def download_toxicity_csv(
    clean_cas: str,
    pubchem_data: dict[str, Any],
    dsstox_info: Optional[dict],
    dtxsid: Optional[str],
    preferred_name: Optional[str],
    h_codes: list[str],
    p_codes: list[str],
    eco: dict,
) -> bytes:
    """
    Build CSV with one header row (summary) and one row per toxicity endpoint so nothing is truncated.
    """
    fp = pubchem_data.get("flash_point")
    vp = pubchem_data.get("vapor_pressure")
    fp_str = "; ".join(fp) if isinstance(fp, list) else (fp or "")
    vp_str = "; ".join(vp) if isinstance(vp, list) else (vp or "")

    # Base columns (same for every row)
    base = {
        "CAS": clean_cas,
        "DTXSID": dtxsid or "",
        "Preferred_Name": preferred_name or "",
        "IUPAC_Name": pubchem_data.get("iupac_name") or "",
        "Formula": pubchem_data.get("formula") or "",
        "MW": pubchem_data.get("mw") or "",
        "Flash_Point": fp_str,
        "Vapor_Pressure": vp_str,
        "GHS_H": " | ".join(h_codes),
        "GHS_P": " | ".join(p_codes),
        "Aquatic_H": " | ".join(eco.get("h_codes_aquatic") or []),
        "Aquatic_LC50_mg_L": eco.get("aquatic_lc50_mg_l") or "",
        "Aquatic_EC50_mg_L": eco.get("aquatic_ec50_mg_l") or "",
    }

    toxicities = pubchem_data.get("toxicities") or []
    if not toxicities:
        return pd.DataFrame([base]).to_csv(index=False).encode("utf-8")

    rows = []
    for t in toxicities:
        row = dict(base)
        row["Exposure_Pathway"] = t.get("route") or ""
        row["Species"] = t.get("species") or ""
        row["Endpoint"] = (t.get("type") or "").strip()
        row["Value"] = (t.get("value") or "")
        row["Unit"] = t.get("unit") or ""
        rows.append(row)

    return pd.DataFrame(rows).to_csv(index=False).encode("utf-8")
