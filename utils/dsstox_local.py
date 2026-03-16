"""
DSSTox local mapping loader with Streamlit caching.
Loads CAS → DTXSID from the DSS/ folder (CSV or Excel). No API key required.
Returns None if no file is found (PubChem-only mode).

Note: On Streamlit Community Cloud, Git LFS files are not pulled by default,
so .xlsx in the repo may be LFS pointers. The loader skips pointer files and
can use a non-LFS CSV (e.g. cas_dtxsid_mapping.csv) if committed without LFS.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import streamlit as st

from config import DSS_DIR, DSSTOX_MAPPING_FILENAMES


def _dss_dir_resolved() -> str:
    """Resolve DSS directory: try cwd first, then relative to this file (for Streamlit Cloud)."""
    for base in (os.getcwd(), os.path.dirname(os.path.dirname(os.path.abspath(__file__)))):
        candidate = os.path.join(base, DSS_DIR) if base else DSS_DIR
        if os.path.isdir(candidate):
            return os.path.abspath(candidate)
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
    paths = []
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


def _load_one_mapping(path: str) -> dict[str, str] | None:
    """Load a single CSV/Excel file into CAS -> DTXSID dict. Returns None on skip/error."""
    try:
        # Skip Git LFS pointer files (Streamlit Cloud doesn't run git lfs pull)
        if _is_lfs_pointer(path):
            return None
        if path.lower().endswith(".xlsx"):
            df = pd.read_excel(path)
        else:
            df = pd.read_csv(path)
        cols_lower = {c.strip().lower(): c for c in df.columns}
        cas_col = cols_lower.get("casrn") or cols_lower.get("cas")
        dtxsid_col = cols_lower.get("dtxsid") or cols_lower.get("dsstox_substance_id")
        if cas_col is None or dtxsid_col is None:
            return None
        cas_series = df[cas_col].astype(str).str.strip()
        dtxsid_series = df[dtxsid_col].astype(str).str.strip()
        mask = (cas_series.str.len() > 0) & (~cas_series.str.lower().isin(("nan", "none", "")))
        cas_series = cas_series[mask]
        dtxsid_series = dtxsid_series[mask]
        return dict(zip(cas_series, dtxsid_series))
    except Exception:
        return None


@st.cache_data
def load_dsstox_mapping():
    """
    Load DSSTox mapping from all CSV/Excel files in DSS/ with caching.
    Merges all files so every CAS in any dump is found.
    Returns dict mapping CAS (str) -> DTXSID, or None if no valid file.
    """
    paths = _find_mapping_files()
    if not paths:
        return None
    merged = {}
    for mapping_path in paths:
        one = _load_one_mapping(mapping_path)
        if one:
            merged.update(one)
    return merged if merged else None


def get_dtxsid(cas_number: str, mapping_dict: Optional[dict]) -> Optional[str]:
    """
    Look up DTXSID from local mapping.
    cas_number: CAS string (with or without dashes).
    mapping_dict: result of load_dsstox_mapping() (may be None).
    Returns DTXSID string or None if not found.
    """
    if not mapping_dict or not cas_number:
        return None
    # Normalize: strip and try with and without dashes if needed
    key = str(cas_number).strip()
    if key in mapping_dict:
        return mapping_dict[key]
    # Try normalized CAS (e.g. 67641 vs 67-64-1)
    key_compact = key.replace("-", "")
    for k, v in mapping_dict.items():
        if k.replace("-", "") == key_compact:
            return v
    return None
