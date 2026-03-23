"""
Optional client for OECD QSAR Toolbox (with VEGA/OPERA) via PyQSARToolbox.
Requires: QSAR Toolbox installed and WebSuite running locally (Windows).
No API key; data comes from the local Toolbox. https://github.com/glsalierno/PyQSARToolbox
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_QSARToolbox = None

def _import_toolbox():
    global _QSARToolbox
    if _QSARToolbox is not None:
        return _QSARToolbox
    try:
        from pyqsartoolbox import QSARToolbox
        _QSARToolbox = QSARToolbox
        return QSARToolbox
    except ImportError:
        return None


def is_available(port: int | None) -> bool:
    """Return True if PyQSARToolbox is installed and QSAR Toolbox WebSuite is reachable at port."""
    if port is None:
        return False
    cls = _import_toolbox()
    if cls is None:
        return False
    try:
        qs = cls(port=port, timeout=5)
        qs.toolbox_version(timeout=5)
        return True
    except Exception:
        return False


def _cas_to_int(cas: str) -> int:
    """CAS with or without dashes -> int for Toolbox API."""
    s = (cas or "").strip().replace("-", "")
    return int(s) if s.isdigit() else 0


def _flatten_endpoint_data(obj: Any, path: str = "", out: list[tuple[str, Any, str | None]] | None = None) -> list[tuple[str, Any, str | None]]:
    """Recursively flatten nested endpoint JSON into (endpoint_name, value, unit) list."""
    if out is None:
        out = []
    if obj is None:
        return out
    if isinstance(obj, dict):
        # Sibling Value + Unit: emit one row
        val = obj.get("Value") if obj.get("Value") is not None else obj.get("Result") or obj.get("Prediction")
        unit = obj.get("Unit") or obj.get("Units")
        if val is not None and not isinstance(val, (dict, list)):
            out.append((path or "endpoint", val, unit))
            return out
        for k, v in obj.items():
            key = (k or "").strip()
            if not key or key in ("Value", "Unit", "Units", "Result", "Prediction"):
                continue
            p = f"{path}#{key}" if path else key
            if isinstance(v, (dict, list)):
                _flatten_endpoint_data(v, p, out)
            else:
                if isinstance(v, str) and v.strip():
                    out.append((p, v.strip(), None))
                elif v is not None and not isinstance(v, (dict, list)):
                    out.append((p, v, None))
        return out
    if isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                _flatten_endpoint_data(item, path, out)
            elif item is not None:
                out.append((f"{path}[{i}]", item, None))
        return out
    return out


def _normalize_endpoint_name(position_endpoint: str) -> str:
    """Map Toolbox position#endpoint to a short key for P2OASys/hazard_data."""
    p = (position_endpoint or "").lower()
    if "vapour pressure" in p or "vapor pressure" in p:
        return "vapor_pressure"
    if "flash point" in p or "flash point" in p:
        return "flash_point"
    if "ld50" in p or "oral" in p and "toxicity" in p:
        return "ld50_oral"
    if "lc50" in p and "aquatic" in p:
        return "lc50_aquatic"
    if "lc50" in p and "inh" in p:
        return "lc50_inhalation"
    if "skin sensit" in p:
        return "skin_sensitization"
    if "mutagen" in p or "ames" in p:
        return "mutagenicity"
    if "carcinogen" in p:
        return "carcinogenicity"
    if "biodeg" in p or "persistence" in p:
        return "biodegradation"
    if "bcf" in p or "bioaccum" in p:
        return "bioaccumulation"
    return (position_endpoint or "unknown").replace(" ", "_").replace("#", "_")[:80]


def fetch_by_cas(
    cas: str,
    port: int,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    """
    Search Toolbox by CAS, then retrieve all endpoint data for the first hit.
    Returns list of dicts with keys: endpoint, value, unit, source, chemical_name, cas, smiles.
    Suitable for conversion to extra_sources or HazardDataPoint.
    """
    cls = _import_toolbox()
    if cls is None:
        logger.warning("PyQSARToolbox not installed; pip install git+https://github.com/glsalierno/PyQSARToolbox.git")
        return []
    cas_int = _cas_to_int(cas)
    if not cas_int:
        return []
    try:
        qs = cls(port=port, timeout=timeout)
        hits = qs.search_CAS(cas_int)
        if not hits or not isinstance(hits, list):
            return []
        hit = hits[0]
        chem_id = hit.get("ChemId")
        if not chem_id:
            return []
        chemical_name = (hit.get("Names") or [None])[0] if hit.get("Names") else None
        cas_str = str(hit.get("Cas") or cas) if hit.get("Cas") else cas
        smiles = hit.get("Smiles")

        all_data = qs.get_all_endpoint_data(chem_id, includeMetadata=False)
        flat = _flatten_endpoint_data(all_data)
        results = []
        for position_endpoint, value, unit in flat:
            if value is None:
                continue
            endpoint = _normalize_endpoint_name(position_endpoint)
            results.append({
                "endpoint": endpoint,
                "value": value,
                "unit": unit,
                "source": "QSAR_Toolbox",
                "chemical_name": chemical_name or "Unknown",
                "cas": cas_str,
                "smiles": smiles,
                "position_endpoint": position_endpoint,
            })
        return results
    except Exception as e:
        logger.exception("QSAR Toolbox fetch failed for CAS %s: %s", cas, e)
        return []


def fetch_by_smiles(
    smiles: str,
    port: int,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    """Same as fetch_by_cas but search by SMILES; returns same shape."""
    cls = _import_toolbox()
    if cls is None:
        return []
    if not (smiles or "").strip():
        return []
    try:
        qs = cls(port=port, timeout=timeout)
        hits = qs.search_smiles(smiles.strip())
        if not hits or not isinstance(hits, list):
            return []
        hit = hits[0]
        chem_id = hit.get("ChemId")
        if not chem_id:
            return []
        chemical_name = (hit.get("Names") or [None])[0] if hit.get("Names") else None
        cas_str = str(hit.get("Cas") or "") if hit.get("Cas") else None
        smiles_out = hit.get("Smiles") or smiles

        all_data = qs.get_all_endpoint_data(chem_id, includeMetadata=False)
        flat = _flatten_endpoint_data(all_data)
        results = []
        for position_endpoint, value, unit in flat:
            if value is None:
                continue
            endpoint = _normalize_endpoint_name(position_endpoint)
            results.append({
                "endpoint": endpoint,
                "value": value,
                "unit": unit,
                "source": "QSAR_Toolbox",
                "chemical_name": chemical_name or "Unknown",
                "cas": cas_str,
                "smiles": smiles_out,
                "position_endpoint": position_endpoint,
            })
        return results
    except Exception as e:
        logger.exception("QSAR Toolbox fetch failed for SMILES: %s", e)
        return []


def toolbox_results_to_extra_sources(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Convert fetch_by_cas/fetch_by_smiles output to extra_sources for build_hazard_data."""
    extra: dict[str, Any] = {"toxicities": [], "ghs": {"h_codes": [], "p_codes": []}, "hazard_metrics": {}}
    fp_list: list[str] = []
    vp_list: list[str] = []
    for r in rows:
        val = r.get("value")
        unit = r.get("unit")
        endpoint = (r.get("endpoint") or "").lower()
        if endpoint == "flash_point" and val is not None:
            fp_list.append(f"{val} °C")
        elif endpoint == "vapor_pressure" and val is not None:
            vp_list.append(f"{val} {unit or 'mmHg'}")
        else:
            value_str = str(val)
            if unit:
                value_str += f" {unit}"
            extra["toxicities"].append({
                "value": value_str,
                "unit": unit,
                "species_route": [r.get("source", "QSAR_Toolbox")],
            })
    if fp_list:
        extra["hazard_metrics"]["flash_point"] = fp_list
    if vp_list:
        extra["hazard_metrics"].setdefault("other_designations", []).extend(vp_list)
    return extra
