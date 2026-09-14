"""
Build hazard_data dict for P2OASys scorer from app's PubChem compound data,
optionally merged with ToxValDB, CPDB, and other sources.

The P2OASys scorer expects:
  - ghs: { h_codes: list, p_codes: list, ... }
  - toxicities: list of { value, unit, species_route }
  - hazard_metrics: { flash_point: list, nfpa: list, other_designations: list }
"""

from __future__ import annotations

from typing import Any, Optional


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
            # ToxValDB stores the *endpoint* in ``toxval_type`` (LD50, LC50, NOAEL, …)
            # and the *study* in ``study_type`` (acute, chronic, …). Use the endpoint
            # type to decide labeling; never infer LD50/LC50 from route alone.
            toxval_type = (rec.get("toxval_type") or "").strip()
            study_type = (rec.get("study_type") or "").lower()
            endpoint = toxval_type or (rec.get("study_type") or "")
            ep = endpoint.lower()
            route = (rec.get("route") or "").lower()
            species = (rec.get("species") or "").strip()
            units_l = str(units).lower()
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
            # Only label a record LD50/LC50/EC50 when the endpoint type explicitly says so.
            # Otherwise keep the real endpoint (e.g. "NOAEL 500 mg/kg") so it is retained
            # as evidence but is NOT scored as acute lethality.
            if "ld50" in ep:
                value_str = f"LD50 {v} {units}"
            elif "lc50" in ep and ("inhalation" in ep or "inhalation" in study_type or "ppm" in units_l or "inh" in route):
                value_str = f"LC50 {v} ppm" if "ppm" in units_l else f"LC50 {v} {units}"
            elif "lc50" in ep or "ec50" in ep:
                label = "EC50" if "ec50" in ep else "LC50"
                value_str = f"{label} {v} mg/L" if ("mg" in units_l and "l" in units_l) else f"{label} {v} {units}"
            elif "iarc" in ep:
                value_str = f"IARC {val}"
            elif "carcinogen" in category or "cancer" in ep:
                value_str = f"Carcinogenicity {v} {units}"
            else:
                value_str = f"{endpoint.strip() or 'Study'} {v} {units}"
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
        "molecular_weight": pubchem_data.get("mw"),
        "smiles": pubchem_data.get("smiles") or pubchem_data.get("canonical_smiles"),
        "formula": pubchem_data.get("formula") or pubchem_data.get("molecular_formula"),
        "cas": pubchem_data.get("cas") or pubchem_data.get("CAS"),
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
            for k in ("flash_point", "nfpa", "other_designations", "gwp100", "odp", "log_kow", "bcf_l_kg", "vapor_pressure_mmhg", "opera_pka"):
                arr = extra_hm.get(k)
                if isinstance(arr, list) and arr:
                    hazard_metrics.setdefault(k, []).extend(arr)
                elif arr and not hazard_metrics.get(k):
                    hazard_metrics[k] = [arr] if not isinstance(arr, list) else arr

    out = {
        "cid": base.get("cid"),
        "molecular_weight": base.get("molecular_weight") or (extra_sources or {}).get("molecular_weight"),
        "smiles": base.get("smiles") or (extra_sources or {}).get("smiles"),
        "formula": base.get("formula") or (extra_sources or {}).get("formula"),
        "cas": base.get("cas") or (extra_sources or {}).get("cas"),
        "ghs": ghs,
        "toxicities": toxicities,
        "hazard_metrics": hazard_metrics,
    }
    if extra_sources and extra_sources.get("opera_row") and not out.get("opera_row"):
        out["opera_row"] = extra_sources["opera_row"]
    if extra_sources and extra_sources.get("opera_pka") and not out.get("opera_pka"):
        out["opera_pka"] = extra_sources["opera_pka"]
    if extra_sources and extra_sources.get("gwp_meta"):
        out["gwp_meta"] = extra_sources["gwp_meta"]
    if extra_sources and extra_sources.get("odp_meta"):
        out["odp_meta"] = extra_sources["odp_meta"]
    if extra_sources and extra_sources.get("vp_meta"):
        out["vp_meta"] = extra_sources["vp_meta"]
    if extra_sources and extra_sources.get("acid_rain_meta"):
        out["acid_rain_meta"] = extra_sources["acid_rain_meta"]
    if extra_sources and extra_sources.get("flash_meta"):
        out["flash_meta"] = extra_sources["flash_meta"]
    if extra_sources and extra_sources.get("physical_state"):
        out["physical_state"] = extra_sources["physical_state"]
    if extra_sources:
        for fate_key in ("log_kow", "bcf_l_kg", "biodeg_half_life_days"):
            if extra_sources.get(fate_key) is not None and out.get(fate_key) is None:
                out[fate_key] = extra_sources[fate_key]
        for aq_key in ("lc50_aquatic_mg_l", "aquatic_toxicity", "aquatic_chv_mg_l"):
            if extra_sources.get(aq_key) is not None and out.get(aq_key) is None:
                out[aq_key] = extra_sources[aq_key]
        if extra_sources.get("ecosar_meta") and not out.get("ecosar_meta"):
            out["ecosar_meta"] = extra_sources["ecosar_meta"]
    return out


def merge_cameo_extra(cas: str, extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Append local CAMEO NFPA 704 strings into extra_sources (no-op if sqlite missing)."""
    try:
        from utils.cameo_lookup import cameo_extra_sources
    except ImportError:
        return extra or {}
    return merge_extra_sources(extra, cameo_extra_sources(cas))


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
        for k in ("flash_point", "nfpa", "other_designations", "gwp100", "odp", "log_kow", "bcf_l_kg", "vapor_pressure_mmhg", "opera_pka"):
            for v in (hm.get(k) or []):
                out["hazard_metrics"].setdefault(k, []).append(v)
    if additional.get("opera_row") and not out.get("opera_row"):
        out["opera_row"] = additional["opera_row"]
    if additional.get("gwp_meta"):
        out["gwp_meta"] = additional["gwp_meta"]
    if additional.get("odp_meta"):
        out["odp_meta"] = additional["odp_meta"]
    if additional.get("vp_meta"):
        out["vp_meta"] = additional["vp_meta"]
    if additional.get("flash_meta"):
        out["flash_meta"] = additional["flash_meta"]
    if additional.get("acid_rain_meta"):
        out["acid_rain_meta"] = additional["acid_rain_meta"]
    if additional.get("physical_state") and not out.get("physical_state"):
        out["physical_state"] = additional["physical_state"]
    for fate_key in ("log_kow", "bcf_l_kg", "biodeg_half_life_days"):
        if additional.get(fate_key) is not None and out.get(fate_key) is None:
            out[fate_key] = additional[fate_key]
    for aq_key in ("lc50_aquatic_mg_l", "aquatic_toxicity", "aquatic_chv_mg_l"):
        if additional.get(aq_key) is not None and out.get(aq_key) is None:
            out[aq_key] = additional[aq_key]
    if additional.get("ecosar_meta") and not out.get("ecosar_meta"):
        out["ecosar_meta"] = additional["ecosar_meta"]
    return out


def _empty_hazard_data() -> dict[str, Any]:
    return {
        "cid": None,
        "molecular_weight": None,
        "ghs": {"h_codes": [], "p_codes": [], "signal_word": "", "pictograms": []},
        "toxicities": [],
        "hazard_metrics": {"flash_point": [], "nfpa": [], "other_designations": []},
    }
