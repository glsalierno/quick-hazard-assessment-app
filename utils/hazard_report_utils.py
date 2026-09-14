"""
Hazard report display utilities: character encoding, data cleaning, deduplication,
and summary builders for the tabbed hazard report (see docs/HAZARD_REPORT_PROMPTS.md).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd

from utils import ghs_formatter


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
        n_id = sum(1 for k in ("dtxsid", "cas", "preferred_name", "smiles") if dsstox.get(k))
        coverage.append(
            {
                "Source": "DSSTox (identifiers)",
                "Data Points": max(n_id, 1),
                "Endpoints": 0,
            }
        )

    if carc and carc.get("found") and isinstance(carc.get("experiments"), list):
        n_exp = len(carc["experiments"])
        coverage.append({"Source": "CPDB", "Data Points": n_exp, "Endpoints": n_exp})

    return coverage


def _ghs_phrase_for_summary(code: str) -> str:
    """Plain-language H-phrase for the dashboard table (no duplicated code prefix)."""
    phrase = ghs_formatter.get_h_phrase(code)
    if not ghs_formatter.phrase_is_found(phrase):
        return "(phrase not found)"
    clean = phrase.strip()
    prefix = f"{code.strip()}:"
    if clean.lower().startswith(prefix.lower()):
        clean = clean.split(":", 1)[1].strip()
    return clean


def build_ghs_summary_df(result: dict[str, Any]) -> pd.DataFrame:
    """Build a short GHS summary table: H_Codes, Phrase, Sources."""
    rows: list[dict[str, Any]] = []
    pubchem = result.get("pubchem") or {}
    ghs = pubchem.get("ghs") or {}
    h_codes = ghs.get("h_codes") or []
    for h in h_codes:
        if not (h or "").strip():
            continue
        code = clean_text(h)
        rows.append({
            "H_Codes": code,
            "Phrase": _ghs_phrase_for_summary(str(code)),
            "Sources": "PubChem",
        })
    return clean_dataframe(deduplicate_hazard_data(pd.DataFrame(rows), subset=["H_Codes"]) if rows else pd.DataFrame())


def _is_quantitative_value(value: Any, unit: Any) -> bool:
    if value is None or str(value).strip() == "":
        return False
    if unit is None or str(unit).strip() in ("", "—", "-"):
        return False
    return bool(re.match(r"^[<>~]?\s*\d+(?:[.,]\d+)?", str(value).strip()))


def build_pubchem_endpoint_df(result: dict[str, Any], *, max_rows: int = 20) -> pd.DataFrame:
    """Quantitative PubChem toxicity/ecotox endpoints for the summary dashboard."""
    rows: list[dict[str, Any]] = []
    pubchem = result.get("pubchem") or {}
    for t in pubchem.get("toxicities") or []:
        val = t.get("value")
        unit = t.get("unit")
        if not _is_quantitative_value(val, unit):
            continue
        rows.append(
            {
                "Source": "PubChem",
                "Endpoint": clean_text((t.get("type") or "Toxicity").strip()),
                "Value": clean_text(str(val)),
                "Unit": clean_text(str(unit)),
                "Species": clean_text(t.get("species") or "—"),
                "Route": clean_text(t.get("route") or "—"),
            }
        )
    eco = pubchem.get("ecotoxicity") or {}
    for e in eco.get("entries") or []:
        val_num = e.get("value_num")
        unit = e.get("unit") or "mg/L"
        if val_num is None:
            continue
        rows.append(
            {
                "Source": "PubChem",
                "Endpoint": clean_text((e.get("endpoint") or "Ecotoxicity").strip()),
                "Value": val_num,
                "Unit": clean_text(str(unit)),
                "Species": clean_text(e.get("species") or "—"),
                "Route": "—",
            }
        )
    if not rows:
        return pd.DataFrame()
    return clean_dataframe(pd.DataFrame(rows).head(max_rows))


def build_toxval_endpoint_df(result: dict[str, Any], *, max_rows: int = 20) -> pd.DataFrame:
    """EPA ToxVal rows linked via DSSTox DTXSID (local SQLite or API)."""
    toxval = result.get("toxval_data")
    if not toxval or not isinstance(toxval, dict):
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _cat, recs in toxval.items():
        if not isinstance(recs, list):
            continue
        for r in recs:
            val = r.get("value")
            unit = r.get("units") or r.get("toxval_units")
            if val is None or str(val).strip() == "":
                continue
            endpoint = r.get("endpoint") or r.get("toxval_type") or r.get("study_type") or _cat
            rows.append(
                {
                    "Source": "ToxValDB (via DSSTox)",
                    "Endpoint": clean_text(str(endpoint)),
                    "Value": val,
                    "Unit": clean_text(str(unit or "—")),
                    "Species": clean_text(r.get("species") or "—"),
                    "Route": clean_text(r.get("route") or r.get("exposure_route") or "—"),
                }
            )
    if not rows:
        return pd.DataFrame()
    return clean_dataframe(pd.DataFrame(rows).head(max_rows))


def build_dsstox_identifier_df(result: dict[str, Any]) -> pd.DataFrame:
    """DSSTox local identifiers (not toxicity endpoints — those are in ToxValDB)."""
    dsstox = result.get("dsstox_info") or {}
    if not isinstance(dsstox, dict) or not dsstox:
        return pd.DataFrame()
    labels = [
        ("DTXSID", "dtxsid"),
        ("CAS", "cas"),
        ("Preferred name", "preferred_name"),
        ("Systematic name", "systematic_name"),
        ("SMILES", "smiles"),
        ("Molecular formula", "molecular_formula"),
    ]
    rows = [
        {"Field": label, "Value": clean_text(str(dsstox.get(key)))}
        for label, key in labels
        if dsstox.get(key)
    ]
    return clean_dataframe(pd.DataFrame(rows))


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
