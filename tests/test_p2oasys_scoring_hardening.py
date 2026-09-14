"""
Regression tests for v6 P2OASys scoring hardening.

Each test pins a specific correctness fix identified in the code review so the bug
cannot silently return:

- Thousands/decimal comma parsing (``3,900`` -> 3900, ``3,9`` -> 3.9).
- Oral LD50 requires an oral or oral-equivalent route (intraperitoneal = oral); intravenous / dermal / inhalation are rejected.
- Dermal LD50 has no fallback to oral evidence.
- POD endpoints (NOAEL/LOAEL/…) are never scored as acute LD50.
- Inhalation LC50 is scored in ppm only; mg/m³ is not silently treated as ppm.
- IARC extraction is negation- and subject-aware (no bare-digit substring match).
- ToxVal records are not relabeled LD50/LC50 unless the endpoint type says so.
- The public score shape is unchanged and a decision trace is available.
- The matrix fingerprint pins the exact workbook used.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from utils import hazard_for_p2oasys, p2oasys_matrix_placeholder, p2oasys_scorer


# --------------------------------------------------------------------------- #
# Number parsing
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("3,900", 3900.0),      # thousands separator, NOT 3.9
        ("3,9", 3.9),           # decimal comma
        ("1,234,567.8", 1234567.8),
        ("5000", 5000.0),
        ("0.05", 0.05),
        (2000, 2000.0),
        ("", None),
        ("nan", None),
    ],
)
def test_num_parses_commas_correctly(raw, expected):
    assert p2oasys_scorer._num(raw) == expected


def test_numeric_threshold_thousands_separator():
    # ">1,000" must parse as 1000, not 1.
    assert p2oasys_scorer._parse_numeric_threshold(">1,000") == 1000.0


# --------------------------------------------------------------------------- #
# Oral / dermal LD50 route requirements
# --------------------------------------------------------------------------- #

def _hd(toxicities, ghs=None):
    return {
        "ghs": ghs or {"h_codes": []},
        "toxicities": toxicities,
        "hazard_metrics": {"flash_point": [], "nfpa": [], "other_designations": []},
    }


def test_oral_ld50_requires_oral_route():
    hd = _hd([
        {"value": "LD50 500 mg/kg", "species_route": ["oral", "rat"]},
    ])
    got = p2oasys_scorer._extract_ld50_oral(hd)
    assert got is not None
    assert got["value"] == 500.0
    assert got["route"] == "oral"


def test_oral_ld50_accepts_intraperitoneal_rejects_iv():
    rej: list = []
    hd = _hd([
        {"value": "LD50 5 mg/kg", "species_route": ["ip", "mouse"]},
        {"value": "LD50 8 mg/kg", "species_route": ["iv", "rat"]},
    ])
    # Intraperitoneal is oral-equivalent; IV is rejected.
    got = p2oasys_scorer._extract_ld50_oral(hd, rej)
    assert got is not None
    assert got["value"] == 5.0
    assert got["route"] == "oral"
    assert any("not oral" in str(r.get("reason", "")).lower() for r in rej)


def test_oral_ld50_accepts_oral_even_if_dermal_also_mentioned():
    # True oral rows must not be rejected just because "dermal" appears nearby.
    hd = _hd([
        {"value": "LD50 oral/dermal 500 mg/kg", "species_route": ["rat"]},
    ])
    got = p2oasys_scorer._extract_ld50_oral(hd)
    assert got is not None
    assert got["value"] == 500.0


def test_oral_ld50_accepts_gavage_and_po():
    for route in (["gavage", "rat"], ["p.o.", "rat"], ["po", "mouse"]):
        hd = _hd([{"value": "LD50 250 mg/kg", "species_route": route}])
        got = p2oasys_scorer._extract_ld50_oral(hd)
        assert got is not None, route
        assert got["value"] == 250.0


def test_oral_ld50_ignores_dermal_only_record():
    hd = _hd([
        {"value": "LD50 200 mg/kg", "species_route": ["dermal", "rabbit"]},
    ])
    assert p2oasys_scorer._extract_ld50_oral(hd) is None


def test_dermal_ld50_no_oral_fallback():
    # Only an oral record exists; dermal extraction must return None (no fallback).
    hd = _hd([
        {"value": "LD50 500 mg/kg", "species_route": ["oral", "rat"]},
    ])
    assert p2oasys_scorer._extract_ld50_dermal(hd) is None


def test_dermal_ld50_selects_dermal_record():
    hd = _hd([
        {"value": "LD50 300 mg/kg", "species_route": ["dermal", "rabbit"]},
    ])
    got = p2oasys_scorer._extract_ld50_dermal(hd)
    assert got is not None
    assert got["value"] == 300.0
    assert got["route"] == "dermal"


def test_oral_ld50_most_conservative_value():
    hd = _hd([
        {"value": "LD50 900 mg/kg", "species_route": ["oral", "rat"]},
        {"value": "LD50 120 mg/kg", "species_route": ["oral", "mouse"]},
    ])
    got = p2oasys_scorer._extract_ld50_oral(hd)
    assert got is not None
    assert got["value"] == 120.0


# --------------------------------------------------------------------------- #
# POD endpoints must not become LD50
# --------------------------------------------------------------------------- #

def test_noael_not_treated_as_ld50():
    rej: list = []
    hd = _hd([
        {"value": "NOAEL 50 mg/kg", "species_route": ["oral", "rat"]},
        {"value": "LOAEL 150 mg/kg", "species_route": ["oral", "rat"]},
    ])
    # These records don't contain "ld50" so they are simply not oral-LD50 candidates.
    assert p2oasys_scorer._extract_ld50_oral(hd, rej) is None


def test_toxrefdb_pod_not_used_as_surrogate_ld50():
    # Even if legacy comptox POD summary is present, it must not create an oral LD50.
    hd = _hd([])
    hd["comptox"] = {"toxrefdb": {"toxrefdb": {"summary": {"min_LOAEL": 12.0, "min_NOAEL": 5.0}}}}
    _scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, {})
    assert trace["evidence"]["oral_ld50"] is None


# --------------------------------------------------------------------------- #
# Inhalation LC50 units
# --------------------------------------------------------------------------- #

def test_inhalation_lc50_ppm_thousands_separator():
    hd = _hd([
        {"value": "LC50 3,900 ppm inhalation rat", "species_route": ["inhalation", "rat"]},
    ])
    # Must be 3900 ppm, not 3.9 ppm.
    got = p2oasys_scorer._extract_lc50_inhalation(hd)
    assert got is not None
    assert got["value"] == 3900.0


def test_inhalation_mgm3_not_scored_as_ppm():
    rej: list = []
    hd = _hd([
        {"value": "LC50 500 mg/m3 inhalation rat", "species_route": ["inhalation", "rat"]},
    ])
    assert p2oasys_scorer._extract_lc50_inhalation(hd, rej) is None
    assert any("mg/m" in r["reason"] or "molecular weight" in r["reason"] for r in rej)


def test_inhalation_mgm3_converts_with_mw():
    # Acetone MW ≈ 58.08; 1000 mg/m³ → ppm = 1000 * 24.45 / 58.08 ≈ 420.9
    hd = _hd([
        {"value": "LC50 1000 mg/m3 inhalation rat", "species_route": ["inhalation", "rat"]},
    ])
    hd["molecular_weight"] = 58.08
    got = p2oasys_scorer._extract_lc50_inhalation(hd)
    assert got is not None
    assert abs(got["value"] - (1000 * 24.45 / 58.08)) < 0.1
    assert got["source_unit"] == "mg/m3"


def test_qualifier_less_than_preserved():
    parsed = p2oasys_scorer.parse_measured_value("<50", higher_is_safer=True)
    assert parsed == {"value": 50.0, "qualifier": "<", "raw": "<50"}


def test_range_uses_hazardous_end_for_ld50():
    parsed = p2oasys_scorer.parse_measured_value("100-200", higher_is_safer=True)
    assert parsed is not None
    assert parsed["value"] == 100.0
    assert parsed["qualifier"] == "range"


# --------------------------------------------------------------------------- #
# IARC negation / subject awareness
# --------------------------------------------------------------------------- #

def test_iarc_not_listed_returns_none():
    # Exact text that previously produced a false IARC "1" (score 10) for CAS 151-21-3.
    text = (
        "Not listed by IARC. Certain nitrosamines are classified by IARC as either "
        "probably or possibly carcinogenic to humans (Groups 2A and 2B, respectively). (L135)"
    )
    hd = _hd([{"value": text}])
    assert p2oasys_scorer._extract_iarc(hd) is None


def test_iarc_real_classification_extracted():
    hd = _hd([{"value": "This substance is classified by IARC in Group 1 (carcinogenic to humans)."}])
    assert p2oasys_scorer._extract_iarc(hd) == "1"


def test_iarc_no_bare_digit_match():
    # A digit elsewhere in the text (reference tag) must not be read as Group 1.
    hd = _hd([{"value": "Evaluated by IARC; no group assigned. (L1)"}])
    assert p2oasys_scorer._extract_iarc(hd) is None


# --------------------------------------------------------------------------- #
# ToxVal relabeling
# --------------------------------------------------------------------------- #

def test_toxval_noael_not_relabeled_ld50():
    toxval = {"repeated_dose": [
        {"toxval_type": "NOAEL", "study_type": "chronic", "route": "oral",
         "species": "rat", "toxval_numeric": 50, "toxval_units": "mg/kg-day"},
    ]}
    out = hazard_for_p2oasys._toxval_to_toxicities(toxval)
    assert len(out) == 1
    assert "LD50" not in out[0]["value"].upper()
    assert "NOAEL" in out[0]["value"].upper()


def test_toxval_ld50_is_labeled_ld50():
    toxval = {"acute": [
        {"toxval_type": "LD50", "study_type": "acute", "route": "oral",
         "species": "rat", "toxval_numeric": 500, "toxval_units": "mg/kg"},
    ]}
    out = hazard_for_p2oasys._toxval_to_toxicities(toxval)
    assert out and out[0]["value"].upper().startswith("LD50")


def test_toxval_oral_acute_without_ld50_type_not_scored_as_ld50():
    # Previously any oral/dermal route was force-labeled LD50; now it is not.
    toxval = {"acute": [
        {"toxval_type": "POD", "study_type": "acute", "route": "oral",
         "species": "rat", "toxval_numeric": 30, "toxval_units": "mg/kg"},
    ]}
    out = hazard_for_p2oasys._toxval_to_toxicities(toxval)
    assert out and "LD50" not in out[0]["value"].upper()


# --------------------------------------------------------------------------- #
# Public shape + trace + fingerprint
# --------------------------------------------------------------------------- #

@pytest.fixture()
def placeholder_matrix(tmp_path: Path):
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        tmp_path / "no_official.xlsx", tmp_path
    )
    assert kind == "placeholder"
    return p2oasys_scorer.load_p2oasys_matrix(path), path


def test_public_shape_unchanged_and_trace_present(placeholder_matrix):
    matrix, _path = placeholder_matrix
    hd = _hd(
        [{"value": "LD50 120 mg/kg", "species_route": ["oral", "rat"]}],
        ghs={"h_codes": ["H301"]},
    )
    scores_only = p2oasys_scorer.compute_p2oasys_scores(hd, matrix)
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    assert scores_only == scores  # public shape identical
    assert trace["scorer_version"] == p2oasys_scorer.SCORER_VERSION
    assert "evidence" in trace and "rejected" in trace and "scored" in trace
    assert "missing" in trace and "category_status" in trace
    assert trace["evidence"]["oral_ld50"]["value"] == 120.0
    assert trace["evidence"]["oral_ld50"]["route"] == "oral"
    # A scored entry carries the matrix rule and the input value.
    assert any(s["score"] is not None and s["input_value"] == 120.0 for s in trace["scored"])


def test_iarc_false_positive_not_scored_end_to_end(placeholder_matrix):
    matrix, _path = placeholder_matrix
    text = (
        "Not listed by IARC. Certain nitrosamines are classified by IARC as either "
        "probably or possibly carcinogenic to humans (Groups 2A and 2B, respectively). (L135)"
    )
    hd = _hd([{"value": text}])
    scores = p2oasys_scorer.compute_p2oasys_scores(hd, matrix)
    chronic = scores.get("Chronic Human Effects", {})
    # Must NOT contain an IARC score (previously scored 10).
    for subcat, bundle in chronic.items():
        if isinstance(bundle, dict):
            assert "IARC Category" not in bundle


def test_matrix_fingerprint(placeholder_matrix):
    _matrix, path = placeholder_matrix
    fp = p2oasys_scorer.matrix_fingerprint(path)
    assert fp["exists"] is True
    assert fp["sha256"] and len(fp["sha256"]) == 64
    assert fp["size_bytes"] and fp["size_bytes"] > 0
