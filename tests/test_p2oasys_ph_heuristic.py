"""Unit tests for P2OASys pH cascade (no network)."""

from __future__ import annotations

from pathlib import Path

import config
from utils import hazard_for_p2oasys, p2oasys_ph as ph
from utils import p2oasys_scorer


def test_score_ph_bands_boundaries():
    assert ph.score_ph_units(7.0)[0] == 2
    assert ph.score_ph_units(6.5)[0] == 2
    assert ph.score_ph_units(11.5)[0] == 6
    assert ph.score_ph_units(2.0)[0] == 8
    assert ph.score_ph_units(1.5)[0] == 10


def test_acetic_acid_pka_to_1pct_band_8():
    out = ph.estimate_ph_fg_smarts("CC(=O)O")
    assert out is not None
    assert 2.4 <= out["ph_est"] <= 3.2
    assert out["score"] == 8


def test_phenol_band_4():
    out = ph.estimate_ph_fg_smarts("Oc1ccccc1")
    assert out is not None and out["score"] == 4


def test_butylamine_band_8():
    out = ph.estimate_ph_fg_smarts("CCCCN")
    assert out is not None and out["score"] == 8


def test_acetone_neutral_smarts():
    assert ph.estimate_ph_fg_smarts("CC(=O)C")["score"] == 2


def test_ester_and_amide_not_ionized():
    assert ph.estimate_ph_fg_smarts("CC(=O)OC")["score"] == 2
    assert ph.estimate_ph_fg_smarts("CC(=O)N")["score"] == 2


def test_priority1_experimental_1pct_beats_opera_and_smarts():
    hd = {
        "smiles": "CC(=O)O",
        "molecular_weight": 60.05,
        "opera_row": {"pKa_a_pred": 4.76, "pKa_b_pred": "NaN"},
        "hazard_metrics": {
            "other_designations": [
                "pH of a 1% aqueous solution = 2.4 (HSDB)"
            ]
        },
    }
    out = ph.estimate_ph_for_hazard(hd)
    assert out is not None
    assert out["source"] == "pubchem_experimental_1pct_ph"
    assert out["score"] == 8
    assert abs(out["ph"] - 2.4) < 0.05
    assert hd.get("ph_estimate") is out


def test_priority2_opera_when_no_experimental_ph():
    hd = {
        "smiles": "CC(=O)O",
        "molecular_weight": 60.05,
        "opera_row": {"pKa_a_pred": 4.76, "pKa_b_pred": "NaN"},
    }
    out = ph.estimate_ph_for_hazard(hd)
    assert out is not None
    assert out["source"] == "opera_pka_1pct"
    assert out["score"] == 8


def test_priority2_experimental_pka_before_opera():
    hd = {
        "smiles": "CC(=O)O",
        "molecular_weight": 60.05,
        "opera_row": {"pKa_a_pred": 3.0, "pKa_b_pred": "NaN"},  # would also score, but exp wins
        "hazard_metrics": {"other_designations": ["pKa 4.76 (experimental)"]},
    }
    out = ph.estimate_ph_for_hazard(hd)
    assert out is not None
    assert out["source"] == "experimental_pka_1pct"
    assert out["score"] == 8


def test_priority3_smarts_last_resort():
    hd = {"smiles": "CC(=O)O", "molecular_weight": 60.05}
    out = ph.estimate_ph_for_hazard(hd)
    assert out is not None
    assert out["source"] == "fg_pka_1pct"
    assert out["score"] == 8


def test_scorer_fills_ph_subcategory():
    hd = hazard_for_p2oasys.build_hazard_data({})
    hd["smiles"] = "CC(=O)O"
    hd["molecular_weight"] = 60.05
    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    ph_block = (scores.get("Physical Properties") or {}).get("pH") or {}
    assert ph_block.get("_max") == 8
    found = [r for r in (trace.get("scored") or []) if str(r.get("subcategory") or "").lower() == "ph"]
    assert found and found[0]["score"] == 8
