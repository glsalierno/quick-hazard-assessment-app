"""
Optional lookup tables (IARC by CAS, ODP/GWP by CAS) for P2OASys.
See docs/P2OASYS_LOOKUP_SOURCES.md for where to source the data.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def normalize_cas_for_lookup(cas: str | None) -> str:
    """Normalize CAS to digits-only for lookup."""
    if not cas or not isinstance(cas, str):
        return ""
    return "".join(c for c in cas.strip() if c.isdigit())


# Backward-compatible alias
_normalize_cas_for_lookup = normalize_cas_for_lookup


def load_iarc_csv(path: Path | str) -> dict[str, str]:
    """
    Load CSV with columns cas, iarc. Returns dict normalized_cas -> iarc (e.g. '2B', '1').
    """
    out: dict[str, str] = {}
    path = Path(path)
    if not path.is_file():
        return out
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cas = _normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                iarc = (row.get("iarc") or row.get("IARC") or "").strip().upper()
                if cas and iarc:
                    out[cas] = iarc
    except Exception:
        pass
    return out


def load_odp_gwp_csv(path: Path | str) -> dict[str, tuple[float | None, float | None]]:
    """
    Load CSV with columns cas, odp, and GWP in one of: gwp100_ar6, gwp, gwp100_fallback
    (see ``GHhaz5/databases/DATABASE_CATALOG.md``). Returns dict normalized_cas -> (odp, gwp).
    Missing or non-numeric values are None.
    """
    out: dict[str, tuple[float | None, float | None]] = {}
    path = Path(path)
    if not path.is_file():
        return out
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cas = _normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                if not cas:
                    continue
                try:
                    odp = float(str(row.get("odp") or row.get("ODP") or "").strip())
                except (ValueError, TypeError):
                    odp = None
                gwp = None
                for k in ("gwp100_ar6", "GWP100_AR6", "gwp", "GWP", "gwp100_fallback", "GWP100_FALLBACK"):
                    try:
                        v = row.get(k)
                        if v is None or (isinstance(v, str) and not v.strip()):
                            continue
                        gwp = float(str(v).strip())
                        break
                    except (ValueError, TypeError):
                        continue
                if odp is not None or gwp is not None:
                    out[cas] = (odp, gwp)
    except Exception:
        pass
    return out


def get_lookup_extra_sources(
    cas: str,
    iarc_by_cas: dict[str, str] | None = None,
    odp_gwp_by_cas: dict[str, tuple[float | None, float | None]] | None = None,
    ipcc_gwp_by_cas: dict[str, float] | None = None,
) -> dict[str, Any]:
    """
    Build extra_sources for hazard_data from optional IARC, ODP/GWP CSV, and IPCC GWP 100-yr (atmo).
    GWP: use IPCC 100-year when available (ipcc_gwp_by_cas), else from ODP/GWP CSV.

    Emits both phrase strings (other_designations) and numeric keys
    (hazard_metrics.gwp100 / hazard_metrics.odp) so the scorer can map
    Atmospheric numeric units (GWP Relative to CO2, ODP Units).

    EPA GHG/ODS list URLs (documentation only; prefer local parquet/CSV at runtime):
    https://www.epa.gov/ghgemissions/understanding-global-warming-potentials
    https://www.epa.gov/ozone-layer-protection/ozone-depleting-substances
    """
    extra: dict[str, Any] = {"toxicities": [], "hazard_metrics": {}}
    cas_norm = _normalize_cas_for_lookup(cas)
    if not cas_norm:
        return extra

    if iarc_by_cas:
        iarc = iarc_by_cas.get(cas_norm)
        if iarc:
            extra["toxicities"].append({
                "value": f"IARC Group {iarc}",
                "unit": None,
                "species_route": None,
            })

    designations: list[str] = []
    odp_val: float | None = None
    gwp_val: float | None = None
    gwp_source: str | None = None
    if odp_gwp_by_cas:
        val = odp_gwp_by_cas.get(cas_norm)
        if val:
            odp, gwp_csv = val
            if odp is not None:
                odp_val = float(odp)
                designations.append(f"ODP {odp_val}")
            if gwp_csv is not None and not (ipcc_gwp_by_cas and cas_norm in ipcc_gwp_by_cas):
                gwp_val = float(gwp_csv)
                gwp_source = "odp_gwp_by_cas.csv"
                designations.append(f"GWP {gwp_val}")
    if ipcc_gwp_by_cas:
        gwp = ipcc_gwp_by_cas.get(cas_norm)
        if gwp is not None:
            designations = [d for d in designations if not str(d).startswith("GWP ")]
            gwp_val = float(gwp)
            gwp_source = "ipcc_atmo_parquet"
            designations.append(f"GWP {gwp_val}")
    if designations:
        extra["hazard_metrics"]["other_designations"] = designations
    if gwp_val is not None:
        extra["hazard_metrics"]["gwp100"] = [gwp_val]
        extra["gwp_meta"] = {
            "gwp100": gwp_val,
            "source": gwp_source or "lookup_table",
            "tier": "experimental_or_lookup",
        }
    if odp_val is not None:
        extra["hazard_metrics"]["odp"] = [odp_val]

    return extra
