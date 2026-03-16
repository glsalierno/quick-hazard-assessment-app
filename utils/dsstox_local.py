"""
Enhanced DSSTox local database loader.
Extracts all available fields from DSSTox mapping files (CSV/Excel) in DSS/.
Provides rich chemical information beyond just DTXSID.

Note: On Streamlit Community Cloud, Git LFS files are not pulled by default,
so .xlsx in the repo may be LFS pointers. The loader skips pointer files and
can use a non-LFS CSV (e.g. cas_dtxsid_mapping.csv) if committed without LFS.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import streamlit as st

from config import DSS_DIR, DSS_PATH, DSSTOX_MAPPING_FILENAMES


def _dss_dir_resolved() -> str:
    """Resolve DSS directory: prefer repo root (for Streamlit Cloud), then cwd, then utils parent."""
    for base in (
        DSS_PATH,
        os.path.join(os.getcwd(), DSS_DIR),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), DSS_DIR),
    ):
        candidate = os.path.abspath(base) if base else ""
        if candidate and os.path.isdir(candidate):
            return candidate
    return os.path.abspath(DSS_DIR)


def _is_lfs_pointer(path: str) -> bool:
    """Return True if the file is a Git LFS pointer (not real content)."""
    try:
        with open(path, "rb") as f:
            head = f.read(200).decode("utf-8", errors="ignore")
        return "git-lfs" in head or "oid sha256" in head
    except Exception:
        return False


def _find_mapping_files() -> list[str]:
    """Return paths to all DSSTox mapping files in DSS/ (CSV and Excel), sorted by name."""
    dss = _dss_dir_resolved()
    if not os.path.isdir(dss):
        return []
    paths: list[str] = []
    # Prefer configured filenames first (e.g. cas_dtxsid_mapping.csv – often committed without LFS for Cloud)
    for name in DSSTOX_MAPPING_FILENAMES:
        path = os.path.join(dss, name)
        if os.path.isfile(path):
            paths.append(path)
    # Then any .csv or .xlsx in DSS/ (sorted so order is deterministic)
    for name in sorted(os.listdir(dss)):
        if name.lower().endswith(".csv") or name.lower().endswith(".xlsx"):
            path = os.path.join(dss, name)
            if path not in paths:
                paths.append(path)
    return sorted(paths)


# Column mappings: logical field -> possible column names (case-insensitive)
COLUMN_MAPPINGS = {
    "cas": ["CASRN", "CAS", "CAS Number", "CAS_NO", "cas", "casrn"],
    "dtxsid": ["DTXSID", "DTXCID", "Substance Key", "dtxsid", "DSSTOX_SUBSTANCE_ID"],
    "preferred_name": ["PREFERRED_NAME", "Preferred Name", "chemical_name", "name", "SUBSTANCE_NAME"],
    "systematic_name": ["SYSTEMATIC_NAME", "Systematic Name", "iupac_name"],
    "molecular_formula": ["MOLECULAR_FORMULA", "Molecular Formula", "formula", "MF"],
    "average_mass": ["AVERAGE_MASS", "Average Mass", "avg_mass"],
    "monoisotopic_mass": ["MONOISOTOPIC_MASS", "Monoisotopic Mass", "exact_mass"],
    "inchi": ["INCHI", "InChI", "inchi_string"],
    "inchikey": ["INCHIKEY", "InChI Key", "inchikey"],
    "smiles": ["SMILES", "Canonical SMILES", "isomeric_smiles"],
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip().upper() for col in df.columns]
    return df


def _detect_columns(df: pd.DataFrame) -> dict[str, str]:
    """Return mapping field -> actual column name in df."""
    actual: dict[str, str] = {}
    cols = set(df.columns)
    for field, names in COLUMN_MAPPINGS.items():
        for name in names:
            up = name.upper()
            if up in cols:
                actual[field] = up
                break
    return actual


@st.cache_data(ttl=86400)
def load_dsstox_enhanced() -> Optional[dict[str, dict]]:
    """
    Load DSSTox mapping from all CSV/Excel files in DSS/ with caching.
    Returns dict mapping CAS (str) -> dict of all available DSSTox fields,
    or None if no valid file is found.
    """
    paths = _find_mapping_files()
    if not paths:
        return None

    merged: dict[str, dict] = {}

    for path in paths:
        try:
            if _is_lfs_pointer(path):
                continue
            if path.lower().endswith(".csv"):
                df = pd.read_csv(path, dtype=str, keep_default_na=False)
            else:
                df = pd.read_excel(path, dtype=str)  # pragma: no cover
            df = _normalize_columns(df)
            colmap = _detect_columns(df)
            if "cas" not in colmap:
                continue

            cas_col = colmap["cas"]

            for _, row in df.iterrows():
                cas = str(row[cas_col]).strip()
                if not cas or cas.lower() in ("nan", "none"):
                    continue

                record: dict[str, object] = {}

                def _get(field: str) -> Optional[str]:
                    col = colmap.get(field)
                    if not col:
                        return None
                    val = str(row[col]).strip()
                    return val if val and val.lower() not in ("nan", "none") else None

                dtxsid = _get("dtxsid")
                if dtxsid:
                    record["dtxsid"] = dtxsid

                pref = _get("preferred_name")
                if pref:
                    record["preferred_name"] = pref

                sysn = _get("systematic_name")
                if sysn:
                    record["systematic_name"] = sysn

                mf = _get("molecular_formula")
                if mf:
                    record["molecular_formula"] = mf

                avg = _get("average_mass")
                if avg:
                    try:
                        record["average_mass"] = float(avg)
                    except ValueError:
                        record["average_mass"] = avg

                mono = _get("monoisotopic_mass")
                if mono:
                    try:
                        record["monoisotopic_mass"] = float(mono)
                    except ValueError:
                        record["monoisotopic_mass"] = mono

                inchi = _get("inchi")
                if inchi:
                    record["inchi"] = inchi

                inchikey = _get("inchikey")
                if inchikey:
                    record["inchikey"] = inchikey

                smiles = _get("smiles")
                if smiles:
                    record["smiles"] = smiles

                if not record:
                    continue

                if cas not in merged:
                    merged[cas] = record
                else:
                    # Merge without overwriting existing non-empty fields
                    for k, v in record.items():
                        if k not in merged[cas] or not merged[cas][k]:
                            merged[cas][k] = v
        except Exception:
            continue

    return merged if merged else None


def get_dsstox_info(cas_number: str, dsstox_data: Optional[dict[str, dict]]) -> Optional[dict]:
    """
    Retrieve all available DSSTox information for a CAS number.
    Returns dict of fields or None if not found.
    """
    if not dsstox_data or not cas_number:
        return None
    cas_clean = str(cas_number).strip().replace(" ", "")
    if cas_clean in dsstox_data:
        return dsstox_data[cas_clean]

    compact = cas_clean.replace("-", "")
    for k, v in dsstox_data.items():
        if k.replace("-", "") == compact:
            return v
    return None


def format_dsstox_display(dsstox_info: Optional[dict]) -> dict:
    """
    Format DSSTox information for display in the app.
    Sections: DTXSID, Names, Molecular, Structure.
    """
    if not dsstox_info:
        return {}

    display: dict[str, object] = {}

    if "dtxsid" in dsstox_info:
        display["DTXSID"] = dsstox_info["dtxsid"]

    names = []
    if "preferred_name" in dsstox_info:
        names.append(("Preferred name", dsstox_info["preferred_name"]))
    if "systematic_name" in dsstox_info:
        names.append(("Systematic name", dsstox_info["systematic_name"]))
    if names:
        display["Names"] = names

    mol_info = []
    if "molecular_formula" in dsstox_info:
        mol_info.append(("Formula", dsstox_info["molecular_formula"]))
    if "average_mass" in dsstox_info:
        mol_info.append(("Average mass", f"{dsstox_info['average_mass']}"))
    if "monoisotopic_mass" in dsstox_info:
        mol_info.append(("Monoisotopic mass", f"{dsstox_info['monoisotopic_mass']}"))
    if mol_info:
        display["Molecular"] = mol_info

    struct_info = []
    if "inchi" in dsstox_info:
        inchi = str(dsstox_info["inchi"])
        if len(inchi) > 80:
            inchi = inchi[:77] + "..."
        struct_info.append(("InChI", inchi))
    if "inchikey" in dsstox_info:
        struct_info.append(("InChI Key", dsstox_info["inchikey"]))
    if "smiles" in dsstox_info:
        struct_info.append(("SMILES", dsstox_info["smiles"]))
    if struct_info:
        display["Structure"] = struct_info

    return display


def get_dsstox_summary_stats(dsstox_data: Optional[dict[str, dict]]) -> dict:
    """Return simple summary statistics for sidebar display."""
    if not dsstox_data:
        return {}
    stats = {
        "total_compounds": len(dsstox_data),
        "with_dtxsid": 0,
        "with_preferred_name": 0,
        "with_formula": 0,
        "with_inchi": 0,
        "with_smiles": 0,
    }
    for rec in dsstox_data.values():
        if rec.get("dtxsid"):
            stats["with_dtxsid"] += 1
        if rec.get("preferred_name"):
            stats["with_preferred_name"] += 1
        if rec.get("molecular_formula"):
            stats["with_formula"] += 1
        if rec.get("inchi"):
            stats["with_inchi"] += 1
        if rec.get("smiles"):
            stats["with_smiles"] += 1
    return stats


# Backwards-compatible thin wrappers used elsewhere in the app
@st.cache_data
def load_dsstox_mapping() -> Optional[dict[str, dict]]:
    """Legacy API: return CAS -> {dtxsid, preferred_name} mapping."""
    data = load_dsstox_enhanced()
    if not data:
        return None
    simple: dict[str, dict] = {}
    for cas, rec in data.items():
        simple[cas] = {
            "dtxsid": rec.get("dtxsid"),
            "preferred_name": rec.get("preferred_name"),
        }
    return simple


def _get_record(cas_number: str, mapping_dict: Optional[dict]) -> Optional[dict]:
    """Legacy helper for get_dtxsid/get_preferred_name."""
    if not mapping_dict or not cas_number:
        return None
    key = str(cas_number).strip()
    if key in mapping_dict:
        v = mapping_dict[key]
        return v if isinstance(v, dict) else {"dtxsid": v, "preferred_name": None}
    key_compact = key.replace("-", "")
    for k, v in mapping_dict.items():
        if k.replace("-", "") == key_compact:
            return v if isinstance(v, dict) else {"dtxsid": v, "preferred_name": None}
    return None


def get_dtxsid(cas_number: str, mapping_dict: Optional[dict]) -> Optional[str]:
    """Legacy API: look up DTXSID from simple mapping."""
    rec = _get_record(cas_number, mapping_dict)
    return rec.get("dtxsid") if isinstance(rec, dict) else None


def get_preferred_name(cas_number: str, mapping_dict: Optional[dict]) -> Optional[str]:
    """Legacy API: look up preferred name from simple mapping."""
    rec = _get_record(cas_number, mapping_dict)
    return rec.get("preferred_name") if isinstance(rec, dict) else None

