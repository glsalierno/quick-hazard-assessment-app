"""Tests: ODP non-gas heuristic + flash/VP gap-fill helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import config
from utils import atmo_gwp, hazard_for_p2oasys, lookup_tables, p2oasys_scorer
from utils import hspip_vapor_pressure as hvp
from utils import pubchem_flashpoint_gapfill as fpg


def test_non_gas_sets_odp_zero_and_scores_atmospheric():
    extra = lookup_tables.get_lookup_extra_sources("67-56-1")  # methanol
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        extra, physical_state="non_gas", state_source="sds_section_9"
    )
    assert extra["hazard_metrics"]["odp"] == [0.0]
    assert extra["odp_meta"]["source"] == "heuristic_non_gas_odp0"
    assert any(str(d).startswith("ODP 0") for d in extra["hazard_metrics"]["other_designations"])

    hazard = hazard_for_p2oasys.build_hazard_data({}, extra_sources=extra)
    hazard = atmo_gwp.merge_gwp_into_hazard_data(hazard, extra)
    assert p2oasys_scorer._extract_odp(hazard) == 0.0
    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    scores, _ = p2oasys_scorer.compute_p2oasys_scores_with_trace(hazard, matrix)
    atmo = scores.get("Atmospheric Hazard") or {}
    assert atmo.get("_category_max") is not None


def test_gas_missing_odp_stays_missing():
    extra = lookup_tables.get_lookup_extra_sources("7440-59-7")  # helium-like
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        extra or {}, physical_state="gas", state_source="pubchem"
    )
    hm = (extra or {}).get("hazard_metrics") or {}
    assert not hm.get("odp")
    assert (extra.get("odp_meta") or {}).get("source") == "missing"


def test_clausius_clapeyron_vp_positive():
    # Methanol-like Y-MBSX: BPt~73 C, dHv~36.7 kJ/mol → ~100 mmHg order
    p = hvp.vp_mmhg_clausius_clapeyron(73.0, 36.69)
    assert p is not None
    assert 50.0 < p < 250.0


def test_ymb_row_to_extra_sources_tags_predicted():
    info = hvp.vapor_pressure_from_ymb_row({"BPt": 73.0, "dHv_bpt": 36.69, "AntA": 7.4, "AntB": 1127.0, "AntC": 206.3})
    assert info is not None
    assert info["tier"] == "predicted"
    xs = hvp.vp_to_extra_sources(info)
    assert xs is not None
    assert xs["vp_meta"]["source"] == "hspip_ymb_sx"
    assert "mmHg" in xs["hazard_metrics"]["other_designations"][0]
    hazard = hazard_for_p2oasys.build_hazard_data({}, extra_sources=xs)
    hazard["vp_meta"] = xs["vp_meta"]
    # merge vapor_pressure_mmhg via hazard_metrics
    hazard.setdefault("hazard_metrics", {}).update(xs["hazard_metrics"])
    vp = p2oasys_scorer._extract_vapor_pressure_mmhg(hazard)
    assert vp is not None and vp > 0


def test_flash_gapfill_skipped_when_present():
    assert fpg.gapfill_flash_point_extra("67-56-1", existing_flash=["12 °C"]) is None


def test_flash_gapfill_uses_tool(monkeypatch):
    monkeypatch.setattr(fpg, "fetch_flash_points_for_cas", lambda cas: ["11 °C", "12 °C c.c."])
    xs = fpg.gapfill_flash_point_extra("67-56-1", existing_flash=None)
    assert xs is not None
    assert xs["flash_meta"]["tier"] == "experimental"
    assert xs["hazard_metrics"]["flash_point"][0].startswith("11")
