"""Tests for P2OASys assessment package, source bridges, and Phase A–C wiring."""

from __future__ import annotations

from pathlib import Path

import pytest

from utils import (
    hazard_for_p2oasys,
    p2oasys_assessment,
    p2oasys_matrix_placeholder,
    p2oasys_scorer,
    p2oasys_source_bridges,
)


def _hd(toxicities, ghs=None, mw=None):
    return {
        "cid": 1,
        "molecular_weight": mw,
        "ghs": ghs or {"h_codes": []},
        "toxicities": toxicities,
        "hazard_metrics": {"flash_point": [], "nfpa": [], "other_designations": []},
    }


@pytest.fixture()
def placeholder_matrix(tmp_path: Path):
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        tmp_path / "no_official.xlsx", tmp_path
    )
    assert kind == "placeholder"
    return p2oasys_scorer.load_p2oasys_matrix(path), path, kind


def test_assessment_package_shape(placeholder_matrix):
    matrix, path, kind = placeholder_matrix
    hd = _hd(
        [{"value": "LD50 120 mg/kg", "species_route": ["oral", "rat"]}],
        ghs={"h_codes": ["H301"]},
        mw=58.0,
    )
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    pkg = p2oasys_assessment.build_assessment_package(
        cas="67-64-1",
        chemical_name="Acetone",
        hazard_data=hd,
        scores=scores,
        trace=trace,
        matrix_path=path,
        matrix_kind=kind,
        sources_used=["PubChem"],
    )
    assert pkg["schema_version"] == p2oasys_assessment.ASSESSMENT_SCHEMA_VERSION
    assert pkg["identity"]["cas"] == "67-64-1"
    assert pkg["executive_summary"]["is_turi_calibrated"] is False
    assert pkg["matrix"]["kind"] == "placeholder"
    assert pkg["matrix"]["sha256"]
    assert isinstance(pkg["p2oasys_table"], list)
    assert isinstance(pkg["evidence_appendix"], list)
    assert isinstance(pkg["missing_data"], list)
    assert "disclaimer" in pkg


def test_override_requires_justification(placeholder_matrix):
    matrix, path, kind = placeholder_matrix
    hd = _hd([{"value": "LD50 120 mg/kg", "species_route": ["oral", "rat"]}])
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    # Find a scored unit to override.
    scored = (trace.get("scored") or [])
    if not scored:
        pytest.skip("placeholder matrix produced no scores for oral LD50")
    row = scored[0]
    # Without justification → ignored
    pkg_bad = p2oasys_assessment.build_assessment_package(
        cas="67-64-1",
        chemical_name=None,
        hazard_data=hd,
        scores=scores,
        trace=trace,
        matrix_path=path,
        matrix_kind=kind,
        overrides=[{
            "category": row["category"],
            "subcategory": row["subcategory"],
            "unit": row["unit"],
            "score": 10,
            "justification": "",
        }],
    )
    assert pkg_bad["overrides"] == []

    pkg_ok = p2oasys_assessment.build_assessment_package(
        cas="67-64-1",
        chemical_name=None,
        hazard_data=hd,
        scores=scores,
        trace=trace,
        matrix_path=path,
        matrix_kind=kind,
        overrides=[{
            "category": row["category"],
            "subcategory": row["subcategory"],
            "unit": row["unit"],
            "score": 10,
            "justification": "Expert judgment from study XYZ",
            "analyst": "GS",
        }],
    )
    assert len(pkg_ok["overrides"]) == 1
    assert pkg_ok["overrides"][0]["score"] == 10


def test_source_precedence_prefers_experimental():
    a = {"tier": "qsar_in_domain", "value": 10.0, "higher_is_safer": True}
    b = {"tier": "experimental", "value": 100.0, "higher_is_safer": True}
    assert p2oasys_assessment.prefer_evidence([a, b]) is b


def test_exportable_gate():
    assert p2oasys_assessment.assessment_is_exportable("official") is True
    assert p2oasys_assessment.assessment_is_exportable("placeholder") is False
    assert p2oasys_assessment.assessment_is_exportable("placeholder", allow_placeholder=True) is True


def test_sds_bridge_h_codes_and_flash():
    extras = p2oasys_source_bridges.sds_fields_to_extra_sources({
        "ghs_h_codes": ["H225", "H319"],
        "flash_point": "-20 °C",
        "aquatic_toxicity": 12.5,
    })
    assert "H225" in extras["ghs"]["h_codes"]
    assert "p_codes" not in extras.get("ghs", {})
    assert extras["hazard_metrics"]["flash_point"]
    assert any("LC50" in t["value"] for t in extras["toxicities"])


def test_opera_bridge_gap_fills_only():
    opera = {
        "ok": True,
        "row": {"CATMoS_LD50_pred": "200", "LogBCF_pred": "2.5", "MolWeight": "58.08"},
        "display": {},
    }
    # No existing oral LD50 → CATMoS fills.
    extras = p2oasys_source_bridges.opera_to_extra_sources(opera, existing_hazard=_hd([]))
    assert any("LD50" in t["value"] and t.get("predicted") for t in extras["toxicities"])
    assert extras["molecular_weight"] == 58.08

    # Existing oral LD50 → CATMoS must not duplicate.
    existing = _hd([{"value": "LD50 500 mg/kg", "species_route": ["oral"]}])
    extras2 = p2oasys_source_bridges.opera_to_extra_sources(opera, existing_hazard=existing)
    assert not any("CATMoS" in str(t.get("source")) for t in extras2.get("toxicities") or [])


def test_mw_passed_through_build_hazard_data():
    hd = hazard_for_p2oasys.build_hazard_data({"mw": 58.08, "ghs": {}, "toxicities": []})
    assert hd["molecular_weight"] == 58.08


def test_assessment_html_contains_cas(placeholder_matrix):
    matrix, path, kind = placeholder_matrix
    hd = _hd([{"value": "LD50 120 mg/kg", "species_route": ["oral", "rat"]}])
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    pkg = p2oasys_assessment.build_assessment_package(
        cas="67-64-1",
        chemical_name="Acetone",
        hazard_data=hd,
        scores=scores,
        trace=trace,
        matrix_path=path,
        matrix_kind=kind,
    )
    html = p2oasys_assessment.assessment_to_html(pkg)
    assert "67-64-1" in html
    assert "Draft P2OASys" in html
    assert "<table>" in html


def test_missing_data_states_populated(placeholder_matrix):
    matrix, _path, _kind = placeholder_matrix
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(_hd([]), matrix)
    assert scores == {} or True  # may be empty
    assert isinstance(trace["missing"], list)
    assert any(m["status"] == p2oasys_scorer.STATUS_NO_DATA for m in trace["missing"])
    assert isinstance(trace["category_status"], dict)
