"""
Build hazard_data dict for P2OASys scorer from app's PubChem compound data.

The P2OASys scorer expects:
  - ghs: { h_codes: list, p_codes: list, ... }
  - toxicities: list of { value, unit, species_route }
  - hazard_metrics: { flash_point: list, nfpa: list, other_designations: list }
"""

from __future__ import annotations

from typing import Any


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


def _empty_hazard_data() -> dict[str, Any]:
    return {
        "cid": None,
        "ghs": {"h_codes": [], "p_codes": [], "signal_word": "", "pictograms": []},
        "toxicities": [],
        "hazard_metrics": {"flash_point": [], "nfpa": [], "other_designations": []},
    }
