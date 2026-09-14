"""Tests for CAA §112(b) HAP → NESHAP scoring."""

from __future__ import annotations

from pathlib import Path

import config
from utils import neshap_hap, p2oasys_scorer


def test_hap_csv_loads_and_contains_benzene():
    path = Path(config.P2OASYS_HAP_CSV_PATH)
    assert path.is_file(), f"missing HAP CSV: {path}"
    hap = neshap_hap.load_hap_cas_set(path)
    assert len(hap) >= 150
    from utils.lookup_tables import normalize_cas_for_lookup

    assert normalize_cas_for_lookup("71-43-2") in hap


def test_neshap_listed_vs_not():
    hap = neshap_hap.load_hap_cas_set(config.P2OASYS_HAP_CSV_PATH)
    listed = neshap_hap.neshap_membership("71-43-2", hap_cas=hap)  # benzene
    assert listed["listed"] is True
    assert listed["score"] == 6
    assert "Listed" in listed["phrase"]

    free = neshap_hap.neshap_membership("7732-18-5", hap_cas=hap)  # water
    assert free["listed"] is False
    assert free["score"] == 2


def test_neshap_phrase_scores_in_matrix():
    hap = neshap_hap.load_hap_cas_set(config.P2OASYS_HAP_CSV_PATH)
    extra = neshap_hap.apply_neshap_to_extra_sources({}, "71-43-2", hap_cas=hap)
    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    hazard = {"hazard_metrics": extra.get("hazard_metrics") or {}}
    # phrase corpus path used by scorer
    from utils.hazard_for_p2oasys import build_hazard_data

    hd = build_hazard_data({}, extra_sources=extra)
    scores = p2oasys_scorer.compute_p2oasys_scores(hd, matrix)
    atmo = scores.get("Atmospheric Hazard") or {}
    nesh = atmo.get("NESHAP") or {}
    assert nesh.get("_max") == 6 or nesh.get("Key Phrases") == 6
