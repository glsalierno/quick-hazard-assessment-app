"""
Build hazard_data dict for P2OASys scorer from app's PubChem compound data,
optionally merged with ToxValDB, CPDB, and other sources.

The P2OASys scorer expects:
  - ghs: { h_codes: list, p_codes: list, ... }
  - toxicities: list of { value, unit, species_route }
  - hazard_metrics: { flash_point: list, nfpa: list, other_designations: list }
"""

from __future__ import annotations

from typing import Any


def _toxval_to_toxicities(toxval_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Convert ToxValDB category->records into toxicities list for P2OASys scorer."""
    if not toxval_data or not isinstance(toxval_data, dict):
        return []
    out: list[dict[str, Any]] = []
    for category, records in toxval_data.items():
        if not isinstance(records, list):
            continue
        for rec in records:
            val = rec.get("value") or rec.get("toxval_numeric")
            units = (rec.get("units") or rec.get("toxval_units") or "").strip() or "—"
            study_type = (rec.get("study_type") or "").lower()
            route = (rec.get("route") or "").lower()
            species = (rec.get("species") or "").strip()
            if val is None and rec.get("toxval_numeric") is not None:
                val = rec["toxval_numeric"]
            if val is None:
                continue
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            species_route: list[str] = []
            if route:
                species_route.append(route)
            if species:
                species_route.append(species)
            if "ld50" in study_type or "oral" in route or "dermal" in route:
                value_str = f"LD50 {v} {units}"
            elif "lc50" in study_type and ("inhalation" in study_type or "ppm" in str(units).lower() or "inh" in route):
                value_str = f"LC50 {v} ppm" if "ppm" in str(units).lower() else f"LC50 {v} {units}"
            elif "lc50" in study_type or "ec50" in study_type or "ecotox" in category or "fish" in study_type:
                value_str = f"LC50 {v} mg/L" if "mg" in str(units).lower() and "l" in str(units).lower() else f"LC50 {v} {units}"
            elif "carcinogen" in category or "cancer" in study_type:
                value_str = f"Carcinogenicity {v} {units}"
            elif "iarc" in study_type:
                value_str = f"IARC {val}"
            else:
                value_str = f"{study_type or 'Study'} {v} {units}"
            out.append({
                "value": value_str,
                "unit": units,
                "species_route": species_route or None,
            })
    return out


def _carc_potency_to_toxicities(carc_potency_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Convert CPDB experiments (TD50, etc.) into toxicities for chronic/scoring."""
    if not carc_potency_data or not carc_potency_data.get("found") or not carc_potency_data.get("experiments"):
        return []
    out: list[dict[str, Any]] = []
    for e in carc_potency_data.get("experiments", []):
        td50 = e.get("td50")
        if td50 is not None:
            try:
                v = float(td50)
            except (TypeError, ValueError):
                continue
            route = e.get("route_name") or e.get("route") or "oral"
            species = e.get("species_name") or e.get("species") or "rat"
            out.append({
                "value": f"TD50 {v} mg/kg/day",
                "unit": "mg/kg/day",
                "species_route": [route, species] if isinstance(route, str) else [str(route)],
            })
    return out


def pubchem_to_hazard_data(pubchem_data: dict[str, Any]) -> dict[str, Any]:
    """
    Convert Quick Hazard Assessment pubchem_data to the format expected by
    p2oasys_scorer.compute_p2oasys_scores().

    pubchem_data comes from utils.pubchem_client.get_compound_data().
    """
    if not pubchem_data:
        return _empty_hazard_data()

    ghs = pubchem_data.get("ghs") or {}
    toxicities = list(pubchem_data.get("toxicities") or [])

    # hazard_metrics: scorer expects flash_point and nfpa as lists; vapor pressure in other_designations
    flash_point = pubchem_data.get("flash_point")
    if isinstance(flash_point, list):
        fp_list = list(flash_point)
    elif flash_point:
        fp_list = [str(flash_point)]
    else:
        fp_list = []

    nfpa = pubchem_data.get("nfpa")
    if isinstance(nfpa, list):
        nfpa_list = list(nfpa)
    elif nfpa:
        nfpa_list = [s.strip() for s in str(nfpa).split(";") if s.strip()]
    else:
        nfpa_list = []

    vapor_pressure = pubchem_data.get("vapor_pressure")
    if isinstance(vapor_pressure, list):
        other_designations = list(vapor_pressure)
    elif vapor_pressure:
        other_designations = [str(vapor_pressure)]
    else:
        other_designations = []

    hazard_metrics = {
        "flash_point": fp_list,
        "nfpa": nfpa_list,
        "other_designations": other_designations,
    }

    return {
        "cid": pubchem_data.get("cid"),
        "ghs": ghs,
        "toxicities": toxicities,
        "hazard_metrics": hazard_metrics,
    }


def build_hazard_data(
    pubchem_data: dict[str, Any],
    toxval_data: dict[str, Any] | None = None,
    carc_potency_data: dict[str, Any] | None = None,
    extra_sources: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build hazard_data for P2OASys by merging PubChem with ToxValDB, CPDB, and optional extra (e.g. SDS, lookup).
    PubChem is base; ToxVal/CPDB/extra toxicities are appended. GHS and hazard_metrics from extra_sources
    can supplement when PubChem is missing (e.g. SDS-extracted).
    """
    base = pubchem_to_hazard_data(pubchem_data) if pubchem_data else _empty_hazard_data()
    toxicities = list(base.get("toxicities") or [])
    ghs = dict(base.get("ghs") or {})
    hazard_metrics = dict(base.get("hazard_metrics") or {})

    for t in _toxval_to_toxicities(toxval_data):
        toxicities.append(t)
    for t in _carc_potency_to_toxicities(carc_potency_data):
        toxicities.append(t)

    if extra_sources:
        extra_tox = extra_sources.get("toxicities") or []
        if isinstance(extra_tox, list):
            toxicities.extend(extra_tox)
        extra_ghs = extra_sources.get("ghs")
        if isinstance(extra_ghs, dict):
            if extra_ghs.get("h_codes") and not ghs.get("h_codes"):
                ghs["h_codes"] = list(extra_ghs["h_codes"])
            elif extra_ghs.get("h_codes"):
                existing = set(ghs.get("h_codes") or [])
                ghs["h_codes"] = list(existing) + [c for c in extra_ghs["h_codes"] if c not in existing]
            if extra_ghs.get("p_codes") and not ghs.get("p_codes"):
                ghs["p_codes"] = list(extra_ghs["p_codes"])
            elif extra_ghs.get("p_codes"):
                existing = set(ghs.get("p_codes") or [])
                ghs["p_codes"] = list(existing) + [c for c in extra_ghs["p_codes"] if c not in existing]
            if extra_ghs.get("signal_word") and not ghs.get("signal_word"):
                ghs["signal_word"] = extra_ghs["signal_word"]
        extra_hm = extra_sources.get("hazard_metrics")
        if isinstance(extra_hm, dict):
            for k in ("flash_point", "nfpa", "other_designations"):
                arr = extra_hm.get(k)
                if isinstance(arr, list) and arr:
                    hazard_metrics.setdefault(k, []).extend(arr)
                elif arr and not hazard_metrics.get(k):
                    hazard_metrics[k] = [arr] if not isinstance(arr, list) else arr

    return {
        "cid": base.get("cid"),
        "ghs": ghs,
        "toxicities": toxicities,
        "hazard_metrics": hazard_metrics,
    }


def merge_extra_sources(base: dict[str, Any] | None, additional: dict[str, Any]) -> dict[str, Any]:
    """Merge a second extra_sources into the first (toxicities extend; ghs/hazard_metrics merged)."""
    if not additional:
        return base or {}
    out = dict(base) if base else {}
    for t in additional.get("toxicities") or []:
        out.setdefault("toxicities", []).append(t)
    g = additional.get("ghs")
    if isinstance(g, dict):
        out.setdefault("ghs", {})
        for key in ("h_codes", "p_codes"):
            for c in (g.get(key) or []):
                if c not in (out["ghs"].get(key) or []):
                    out["ghs"].setdefault(key, []).append(c)
        if g.get("signal_word") and not out["ghs"].get("signal_word"):
            out["ghs"]["signal_word"] = g["signal_word"]
    hm = additional.get("hazard_metrics")
    if isinstance(hm, dict):
        out.setdefault("hazard_metrics", {})
        for k in ("flash_point", "nfpa", "other_designations"):
            for v in (hm.get(k) or []):
                out["hazard_metrics"].setdefault(k, []).append(v)
    return out


def _empty_hazard_data() -> dict[str, Any]:
    return {
        "cid": None,
        "ghs": {"h_codes": [], "p_codes": [], "signal_word": "", "pictograms": []},
        "toxicities": [],
        "hazard_metrics": {"flash_point": [], "nfpa": [], "other_designations": []},
    }
