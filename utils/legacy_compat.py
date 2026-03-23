"""
Optional helpers mapping the unified assessment service to legacy dict shapes.
"""

from __future__ import annotations

from typing import Any, List, Optional

from services.chemical_assessment import AssessmentResult, get_assessment_service


def legacy_cas_lookup(cas: str) -> dict[str, Any]:
    """Return the same ``result_data``-like dict used throughout ``app.py``."""
    svc = get_assessment_service()
    ar = svc.assess(cas)
    if isinstance(ar, list):
        ar = ar[0]
    if isinstance(ar, AssessmentResult) and ar.has_multiple_components and ar.all_components:
        ar = ar.all_components[0]
    assert isinstance(ar, AssessmentResult)
    return svc.to_result_data(ar)


def legacy_sds_extract_cas_list(pdf_file: Any) -> List[str]:
    """CAS list from an uploaded SDS (no full assessment)."""
    svc = get_assessment_service()
    return [x.cas for x in svc.identify_from_sds(pdf_file)]
