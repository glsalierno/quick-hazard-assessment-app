"""
Load IPCC GWP 100-year values from the atmo folder (parquet from Federal LCA Commons).
Used by fast-p2oasys for Atmospheric Hazard when atmo/IPCC parquet is available.
Prefer AR6-100; fallback AR5-100 then AR4-100.
"""

from __future__ import annotations

from pathlib import Path

def _normalize_cas(cas: str) -> str:
    """Normalize CAS to digits-only for lookup."""
    if not cas or not isinstance(cas, str):
        return ""
    s = str(cas).strip()
    if s in ("(no data)", "", "nan"):
        return ""
    # Keep digits only (strip dashes/spaces)
    return "".join(c for c in s if c.isdigit())


def load_ipcc_gwp_100_from_atmo(atmo_dir: Path | str) -> dict[str, float]:
    """
    Load GWP 100-year (kg CO2e/kg) by CAS from atmo folder.
    Looks for IPCC_v*.parquet; uses Indicator AR6-100, then AR5-100, then AR4-100.
    Returns dict normalized_cas -> GWP (float).
    """
    atmo_dir = Path(atmo_dir)
    if not atmo_dir.is_dir():
        return {}

    parquet_files = sorted(atmo_dir.glob("IPCC_*.parquet"), reverse=True)
    if not parquet_files:
        return {}

    try:
        import pandas as pd
    except ImportError:
        return {}

    try:
        df = pd.read_parquet(parquet_files[0], columns=["Indicator", "CAS No", "Characterization Factor"])
    except Exception:
        return {}

    # Prefer AR6-100, then AR5-100, then AR4-100
    order = ["AR6-100", "AR5-100", "AR4-100"]
    out: dict[str, float] = {}
    for ind in order:
        sub = df[df["Indicator"] == ind]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            cas_raw = row.get("CAS No")
            cas_norm = _normalize_cas(str(cas_raw) if pd.notna(cas_raw) else "")
            if not cas_norm:
                continue
            try:
                gwp = float(row["Characterization Factor"])
            except (TypeError, ValueError):
                continue
            if cas_norm not in out:
                out[cas_norm] = gwp
    return out
