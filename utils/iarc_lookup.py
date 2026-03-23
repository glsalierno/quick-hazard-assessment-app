"""
Load IARC classifications from the iarc folder (fastP2OASys/iarc).
Expects CSV or Excel with columns 'CAS No.' (or 'CAS No') and 'Group' (1, 2A, 2B, 3, 4).
Used by fast-p2oasys for Chronic Human Effects when iarc folder is available.
"""

from __future__ import annotations

from pathlib import Path


def _normalize_cas(cas: str) -> str:
    """Normalize CAS to digits-only for lookup."""
    if not cas or not isinstance(cas, str):
        return ""
    s = str(cas).strip()
    if s in ("", "nan", "NaN"):
        return ""
    return "".join(c for c in s if c.isdigit())


def load_iarc_from_iarc_folder(iarc_dir: Path | str) -> dict[str, str]:
    """
    Load IARC Group by CAS from the iarc folder.
    Looks for CSV or Excel with columns 'CAS No.' (or 'CAS No') and 'Group'.
    Group values normalized to 1, 2A, 2B, 3, 4. Returns dict normalized_cas -> group.
    """
    iarc_dir = Path(iarc_dir)
    if not iarc_dir.is_dir():
        return {}

    try:
        import pandas as pd
    except ImportError:
        return {}

    # Prefer CSV (e.g. "List of Classifications - IARC Monographs ... .csv")
    csv_files = list(iarc_dir.glob("*.csv"))
    for path in csv_files:
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
            except Exception:
                continue
        cas_col = None
        group_col = None
        for c in df.columns:
            cnorm = str(c).strip().lower()
            if "cas" in cnorm and "no" in cnorm:
                cas_col = c
            if cnorm == "group":
                group_col = c
        if not cas_col or not group_col:
            continue
        out: dict[str, str] = {}
        for _, row in df.iterrows():
            cas_raw = row.get(cas_col)
            if pd.isna(cas_raw):
                continue
            cas_norm = _normalize_cas(str(cas_raw))
            if not cas_norm:
                continue
            gr = row.get(group_col)
            if pd.isna(gr):
                continue
            gr_str = str(gr).strip().upper()
            if gr_str in ("1", "2A", "2B", "3", "4"):
                out[cas_norm] = gr_str
        if out:
            return out
        break

    # Fallback: Excel (first sheet, same column logic)
    xlsx_files = list(iarc_dir.glob("*.xlsx"))
    for path in xlsx_files:
        try:
            xl = pd.ExcelFile(path, engine="openpyxl")
            df = pd.read_excel(xl, sheet_name=0)
        except Exception:
            continue
        cas_col = None
        group_col = None
        for c in df.columns:
            cnorm = str(c).strip().lower()
            if "cas" in cnorm and "no" in cnorm:
                cas_col = c
            if cnorm == "group":
                group_col = c
        if not cas_col or not group_col:
            continue
        out = {}
        for _, row in df.iterrows():
            cas_raw = row.get(cas_col)
            if pd.isna(cas_raw):
                continue
            cas_norm = _normalize_cas(str(cas_raw))
            if not cas_norm:
                continue
            gr = row.get(group_col)
            if pd.isna(gr):
                continue
            gr_str = str(gr).strip().upper()
            if gr_str in ("1", "2A", "2B", "3", "4"):
                out[cas_norm] = gr_str
        if out:
            return out
        break

    return {}
