"""Tests: Antoine@298 curated VP + S/N acid-rain atmospheric heuristic."""

from __future__ import annotations

from pathlib import Path

import config
from utils import atmo_gwp, hazard_for_p2oasys, hspip_vapor_pressure as hvp, p2oasys_scorer


def test_antoine_form_hspip_log10_mmhg_c():
    assert "log10" in hvp.ANTOINE_FORM_HSPIP["equation"]
    assert hvp.ANTOINE_FORM_HSPIP["t_unit"] == "C"
    assert hvp.ANTOINE_FORM_HSPIP["p_unit"] == "mmHg"


def test_curated_methanol_antoine_near_literature():
    """cas_hspip_antoine.csv methanol override ~127 mmHg (not Y-MBSX ~373)."""
    cur = hvp.get_curated_antoine("67-56-1")
    assert cur is not None
    p = hvp.vp_mmhg_antoine(
        cur["AntA"], cur["AntB"], cur["AntC"],
        log_base=cur["log_base"], t_unit=cur["t_unit"], p_unit=cur["p_unit"],
    )
    assert p is not None
    assert 110.0 < p < 140.0


def test_ymb_antoine_methanol_overpredicts():
    p = hvp.vp_mmhg_antoine(7.444, 1127.0, 206.3)
    assert p is not None and p > 300.0


def test_precedence_curated_antoine_over_clausius():
    ymb = {"BPt": 73.0, "dHv_bpt": 36.69, "AntA": 7.444, "AntB": 1127.0, "AntC": 206.3}
    info = hvp.vapor_pressure_predicted(cas="67-56-1", ymb_row=ymb)
    assert info is not None
    assert info["method"] == "cas_hspip_antoine_298k"
    assert 110.0 < float(info["vp_mmhg_25c"]) < 140.0
    assert info["tier"] == "predicted"


def test_precedence_clausius_over_ymb_antoine_without_curated(tmp_path):
    empty = tmp_path / "empty_antoine.csv"
    empty.write_text(
        "cas,AntA,AntB,AntC,log_base,t_unit,p_unit,source,notes\n",
        encoding="utf-8",
    )
    ymb = {"BPt": 73.0, "dHv_bpt": 36.69, "AntA": 7.444, "AntB": 1127.0, "AntC": 206.3}
    info = hvp.vapor_pressure_predicted(cas="999-99-9", ymb_row=ymb, curated_path=empty)
    assert info is not None
    assert info["method"] == "clausius_clapeyron_bpt_dhv"
    assert 50.0 < float(info["vp_mmhg_25c"]) < 250.0
    # YMB Antoine kept as alternate meta
    assert info["vp_mmhg_25c_antoine_ymb"] is not None
    assert float(info["vp_mmhg_25c_antoine_ymb"]) > 300.0


def test_ymb_cached_curated_does_not_outrank_clausius(tmp_path):
    csv_path = tmp_path / "ymb_cached.csv"
    csv_path.write_text(
        "cas,AntA,AntB,AntC,log_base,t_unit,p_unit,source,notes\n"
        "67-56-1,7.444,1127.0,206.3,log10,C,mmHg,hspip_ymb_sx_cached,ymb\n",
        encoding="utf-8",
    )
    ymb = {"BPt": 73.0, "dHv_bpt": 36.69, "AntA": 7.444, "AntB": 1127.0, "AntC": 206.3}
    info = hvp.vapor_pressure_predicted(cas="67-56-1", ymb_row=ymb, curated_path=csv_path)
    assert info["method"] == "clausius_clapeyron_bpt_dhv"


def test_vp_extra_tags_predicted_and_cites_source():
    info = hvp.vapor_pressure_predicted(cas="67-56-1", ymb_row={})
    xs = hvp.vp_to_extra_sources(info)
    assert xs is not None
    des = xs["hazard_metrics"]["other_designations"][0]
    assert "mmHg" in des
    assert "predicted" in des.lower() or "Antoine" in des
    assert xs["vp_meta"]["tier"] == "predicted"


def test_acid_rain_no_sn_methanol():
    info = atmo_gwp.acid_rain_phrase_for_structure(smiles="CO", formula="CH4O")
    assert info["has_s"] is False and info["has_n"] is False
    assert info["phrase"] == "Does not contain S or N"
    assert "heuristic" in info["honesty"].lower()


def test_acid_rain_has_n_from_smiles():
    info = atmo_gwp.acid_rain_phrase_for_structure(smiles="CCN", formula="C2H7N")
    assert info["has_n"] is True
    assert "may form SOx or NOx" in info["phrase"]


def test_acid_rain_has_s_from_hspip_counts():
    info = atmo_gwp.acid_rain_phrase_for_structure(
        hspip_row={"S#": 1.0, "N#": 0.0, "Formula": "CH4S"}
    )
    assert info["has_s"] is True
    assert "may form" in info["phrase"]


def test_acid_rain_formula_not_ni_si():
    for f in ("SiH4", "NiO", "NaCl", "SnCl2"):
        info = atmo_gwp.acid_rain_phrase_for_structure(formula=f)
        assert info["has_s"] is False and info["has_n"] is False, f
        assert info["phrase"] == "Does not contain S or N"


def test_acid_rain_formula_atom_counts():
    info = atmo_gwp.acid_rain_phrase_for_structure(formula="C2H3N")
    assert info["has_n"] is True and info["has_s"] is False
    assert any(e.startswith("formula_atoms:N=") for e in info["evidence"])
    # With formula present, SMILES/RDKit must not be required
    info2 = atmo_gwp.acid_rain_phrase_for_structure(formula="C2H6OS", smiles="CS(C)=O")
    assert info2["has_s"] is True
    assert not any("rdkit" in e for e in info2["evidence"])


def test_acid_rain_ignores_sofx_boolean_s():
    """HSPiP sofx 'S' is a boolean flag, not sulfur count — must not trigger."""
    info = atmo_gwp.acid_rain_phrase_for_structure(
        formula="C3H6O",
        hspip_row={"S": "FALSE", "N": "FALSE"},  # sofx-shaped; ignored without S#/N#
    )
    assert info["has_s"] is False and info["has_n"] is False
    assert info["phrase"] == "Does not contain S or N"


def test_acid_rain_pubchem_formula_primary():
    extra = atmo_gwp.apply_acid_rain_combustion_heuristic(
        {},
        pubchem={"formula": "C2H7N", "smiles": "CCN"},
    )
    assert "may form SOx or NOx" in extra["acid_rain_meta"]["phrase"]
    assert "pubchem_formula" in extra["acid_rain_meta"]["structure_sources"]


def test_acid_rain_wires_into_extras_and_scores():
    extra = atmo_gwp.apply_acid_rain_combustion_heuristic(
        {}, smiles="CO", formula="CH4O"
    )
    assert any(
        "Does not contain S or N" in str(d)
        for d in extra["hazard_metrics"]["other_designations"]
    )
    assert extra["acid_rain_meta"]["source"] == "structural_combustion_sox_nox_heuristic"

    extra_n = atmo_gwp.apply_acid_rain_combustion_heuristic(
        {}, smiles="c1ccncc1", formula="C5H5N"
    )
    hazard = hazard_for_p2oasys.build_hazard_data({}, extra_sources=extra_n)
    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hazard, matrix)
    atmo = scores.get("Atmospheric Hazard") or {}
    # Acid Rain Formation should score via key phrase (bin 6 for may-form)
    assert atmo.get("_category_max") is not None or atmo
    found = [
        row for row in (trace.get("scored") or [])
        if "Acid Rain" in str(row.get("subcategory") or "")
    ]
    assert hazard.get("acid_rain_meta")
    assert found, "Acid Rain Formation should appear in scored trace"
    assert found[0].get("score") == 6
