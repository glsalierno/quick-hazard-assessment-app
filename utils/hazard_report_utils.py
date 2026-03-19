"""
Hazard report display utilities: character encoding, data cleaning, deduplication,
and summary builders for the tabbed hazard report (see docs/HAZARD_REPORT_PROMPTS.md).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd


def clean_text(text: Any) -> Any:
    """Clean text by handling special characters and encoding issues."""
    if not isinstance(text, str):
        return text
    if not text:
        return text
    text = unicodedata.normalize("NFKD", text)
    replacements = {
        "\ufffd": "",  # Replacement character
        "\u2013": "-",  # en dash
        "\u2014": "--",  # em dash
        "\u2018": "'",  # left single quote
        "\u2019": "'",  # right single quote
        "\u201c": '"',  # left double quote
        "\u201d": '"',  # right double quote
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    # Common mojibake
    text = text.replace("\u00e2\u20ac\u201d", "--")
    text = text.replace("\u00e2\u20ac\u2122", "'")
    text = text.replace("\u00e2\u20ac\u0153", '"')
    text = text.replace("\u00e2\u20ac\u009d", '"')
    text = text.replace("\u00c2", "")  # stray Â
    # Remove control characters except newline/tab
    text = "".join(c for c in text if ord(c) >= 32 or c in "\n\t")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply clean_text to all string columns in a DataFrame. Returns a copy."""
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    out = df.copy()
    for col in out.select_dtypes(include=["object"]).columns:
        out[col] = out[col].apply(lambda x: clean_text(str(x)) if pd.notna(x) else x)
    return out


def deduplicate_hazard_data(
    df: pd.DataFrame,
    subset: list[str] | None = None,
    keep: str = "first",
) -> pd.DataFrame:
    """Remove duplicate rows. subset: columns that define uniqueness; default common hazard keys."""
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    keys = subset or ["Source", "Endpoint", "Value", "Species", "Route"]
    available = [k for k in keys if k in df.columns]
    if not available:
        return df.copy()
    return df.drop_duplicates(subset=available, keep=keep).reset_index(drop=True)


def get_source_coverage(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Build a list of {Source, Data Points, Endpoints} for the summary dashboard."""
    coverage: list[dict[str, Any]] = []
    pubchem = result.get("pubchem") or {}
    toxval = result.get("toxval_data")
    dsstox = result.get("dsstox_info")
    carc = result.get("carc_potency_data")

    n_tox = len(pubchem.get("toxicities") or [])
    n_eco = len((pubchem.get("ecotoxicity") or {}).get("entries") or [])
    n_ghs = len((pubchem.get("ghs") or {}).get("h_codes") or [])
    if n_tox or n_eco or n_ghs or pubchem.get("formula") or pubchem.get("mw"):
        coverage.append({"Source": "PubChem", "Data Points": n_tox + n_eco + (n_ghs or 0) + 2, "Endpoints": n_tox + n_eco})

    if toxval and isinstance(toxval, dict):
        total = sum(len(v) for v in toxval.values() if isinstance(v, list))
        coverage.append({"Source": "ToxValDB", "Data Points": total, "Endpoints": total})

    if dsstox and isinstance(dsstox, dict):
        coverage.append({"Source": "DSSTox", "Data Points": 1, "Endpoints": 0})

    if carc and carc.get("found") and isinstance(carc.get("experiments"), list):
        n_exp = len(carc["experiments"])
        coverage.append({"Source": "CPDB", "Data Points": n_exp, "Endpoints": n_exp})

    return coverage


def build_ghs_summary_df(result: dict[str, Any]) -> pd.DataFrame:
    """Build a short GHS summary table for the dashboard: Hazard, Category, H_Codes, Sources."""
    rows: list[dict[str, Any]] = []
    pubchem = result.get("pubchem") or {}
    ghs = pubchem.get("ghs") or {}
    h_codes = ghs.get("h_codes") or []
    for h in h_codes:
        if not (h or "").strip():
            continue
        rows.append({
            "Hazard": "",
            "Category": "",
            "H_Codes": clean_text(h),
            "Sources": "PubChem",
        })
    return clean_dataframe(deduplicate_hazard_data(pd.DataFrame(rows), subset=["H_Codes"]) if rows else pd.DataFrame())


def build_property_summary_df(result: dict[str, Any]) -> pd.DataFrame:
    """Build physical property summary for dashboard: Property, Value, Unit, Source."""
    rows: list[dict[str, Any]] = []
    pubchem = result.get("pubchem") or {}
    formula = pubchem.get("formula")
    mw = pubchem.get("mw")
    fp = pubchem.get("flash_point")
    vp = pubchem.get("vapor_pressure")
    if formula:
        rows.append({"Property": "Molecular Formula", "Value": clean_text(str(formula)), "Unit": "", "Source": "PubChem"})
    if mw is not None and str(mw).strip():
        rows.append({"Property": "Molecular Weight", "Value": clean_text(str(mw)), "Unit": "g/mol", "Source": "PubChem"})
    if fp:
        val = "; ".join(fp) if isinstance(fp, list) else str(fp)
        rows.append({"Property": "Flash Point", "Value": clean_text(val), "Unit": "°C", "Source": "PubChem"})
    if vp:
        val = "; ".join(vp) if isinstance(vp, list) else str(vp)
        rows.append({"Property": "Vapor Pressure", "Value": clean_text(val), "Unit": "mmHg", "Source": "PubChem"})
    return clean_dataframe(pd.DataFrame(rows))
