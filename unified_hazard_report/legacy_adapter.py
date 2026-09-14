"""Call the existing v1.4 assessment pipeline (PubChem, DSSTox, ToxVal, CPDB) without Streamlit entrypoints."""

from __future__ import annotations

from typing import Any

from services.chemical_assessment import ChemicalAssessmentService


def get_legacy_hazards(cas: str) -> dict[str, Any]:
    """
    Run the same stack as the Streamlit app for a single CAS.

    Returns a dict shaped like ``app.py`` session ``result_data``:
    ``pubchem``, ``dsstox_info``, ``toxval_data``, ``carc_potency_data``, ``clean_cas``, ``fetch_error``, etc.
    """
    cas = (cas or "").strip()
    svc = ChemicalAssessmentService()
    result = svc.assess(cas)
    return svc.to_result_data(result)
