"""
DSSTox local mapping loader with Streamlit caching.
Loads CAS → DTXSID from the DSS/ folder (CSV or Excel). No API key required.
Returns None if no file is found (PubChem-only mode).
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import streamlit as st

from config import DSS_DIR, DSSTOX_MAPPING_FILENAMES


def _find_mapping_file() -> Optional[str]:
    """Return path to first existing DSSTox mapping file in DSS/ (CSV or Excel)."""
    if not os.path.isdir(DSS_DIR):
        return None
    # Prefer configured filenames
    for name in DSSTOX_MAPPING_FILENAMES:
        path = os.path.join(DSS_DIR, name)
        if os.path.isfile(path):
            return path
    # Then any .csv or .xlsx in DSS/
    for name in sorted(os.listdir(DSS_DIR)):
        if name.lower().endswith(".csv") or name.lower().endswith(".xlsx"):
            return os.path.join(DSS_DIR, name)
    return None


@st.cache_data
def load_dsstox_mapping():
    """
    Load DSSTox mapping from DSS/ with caching.
    Returns dict mapping CAS (str) -> DTXSID, or None if file missing or invalid.
    """
    mapping_path = _find_mapping_file()
    if not mapping_path:
        return None
    try:
        if mapping_path.lower().endswith(".xlsx"):
            df = pd.read_excel(mapping_path)
        else:
            df = pd.read_csv(mapping_path)
        # Support common column name variants (EPA uses CASRN, DTXSID)
        cas_col = "CASRN" if "CASRN" in df.columns else ("CAS" if "CAS" in df.columns else None)
        dtxsid_col = "DTXSID" if "DTXSID" in df.columns else ("DSSTox_Substance_Id" if "DSSTox_Substance_Id" in df.columns else None)
        if cas_col is None or dtxsid_col is None:
            return None
        # Normalize to string for consistent lookup
        cas_to_dtxsid = dict(
            zip(
                df[cas_col].astype(str).str.strip(),
                df[dtxsid_col].astype(str).str.strip(),
            )
        )
        return cas_to_dtxsid
    except Exception:
        return None


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
