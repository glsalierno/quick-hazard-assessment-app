"""Fate wiring: OPERA bridge structured fields + scorer Log Kow / BCF extractors."""

from __future__ import annotations

from pathlib import Path

import pytest

from utils import p2oasys_matrix_placeholder, p2oasys_scorer, p2oasys_source_bridges
import config


@pytest.fixture()
def placeholder_matrix(tmp_path: Path):
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        tmp_path / "no_official.xlsx", tmp_path
    )
    assert kind == "placeholder"
    return p2oasys_scorer.load_p2oasys_matrix(path), path, kind


@pytest.fixture()
def official_matrix():
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH), Path(config.DATA_DIR)
    )
    if kind != "official":
        pytest.skip("official P2OASys matrix not available")
    return p2oasys_scorer.load_p2oasys_matrix(path), path, kind


def test_opera_bridge_emits_logp_bcf_readybiodeg():
    opera = {
        "ok": True,
        "row": {
            "LogP_pred": "-0.77",
            "LogBCF_pred": "0.5",
            "ReadyBiodeg_pred": "1",
            "BioDeg_LogHalfLife_pred": "0.3",
            "MolWeight": "32.03",
        },
        "display": {},
    }
    xs = p2oasys_source_bridges.opera_to_extra_sources(opera, existing_hazard={})
    assert xs["log_kow"]["value"] == -0.77
    assert xs["log_kow"]["predicted"] is True
    assert abs(xs["bcf_l_kg"]["value"] - (10 ** 0.5)) < 1e-9
    assert xs["bcf_l_kg"]["predicted"] is True
    assert xs["biodeg_half_life_days"]["value"] == 10 ** 0.3
    tox = " ".join(t["value"] for t in xs["toxicities"])
    assert "Readily degradable" in tox
    assert "LogP" in tox or "log Kow" in tox.lower()
    assert "Biowin" not in tox


def test_opera_bridge_readybiodeg_not_ready_phrase():
    opera = {"ok": True, "row": {"ReadyBiodeg_pred": "0"}, "display": {}}
    xs = p2oasys_source_bridges.opera_to_extra_sources(opera)
    tox = " ".join(t["value"] for t in xs["toxicities"])
    assert "Not likely to biodegrade" in tox


def test_extract_log_kow_and_bcf_from_structured():
    hd = {
        "toxicities": [],
        "hazard_metrics": {},
        "log_kow": {"value": 3.5, "predicted": True, "source": "OPERA"},
        "bcf_l_kg": {"value": 250.0, "predicted": True, "source": "OPERA", "log_bcf": 2.4},
    }
    lk = p2oasys_scorer._extract_log_kow(hd)
    assert lk and lk["value"] == 3.5 and lk["predicted"] is True
    bcf = p2oasys_scorer._extract_bcf_l_kg(hd)
    assert bcf and bcf["value"] == 250.0 and bcf["predicted"] is True


def test_fate_numeric_scores_with_opera_fields(official_matrix):
    matrix, _path, _kind = official_matrix
    hd = {
        "ghs": {"h_codes": []},
        "toxicities": [
            {
                "value": "OPERA ready biodegradation 1; Readily degradable; Biodegradable",
                "predicted": True,
                "source": "OPERA",
            },
        ],
        "hazard_metrics": {},
        "log_kow": {"value": -0.77, "predicted": True, "source": "OPERA"},
        "bcf_l_kg": {"value": 3.16, "predicted": True, "source": "OPERA", "log_bcf": 0.5},
    }
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    assert trace["evidence"]["log_kow"]["value"] == -0.77
    assert trace["evidence"]["bcf_l_kg"]["value"] == 3.16
    fate_scored = [s for s in trace["scored"] if s["category"] == "Environmental Fate & Transport"]
    assert fate_scored, "expected Fate units to score with OPERA evidence"
    assert any("Log Kow" in s["unit"] for s in fate_scored)
    assert any("BAF/BCF" in s["unit"] for s in fate_scored)
    assert any(s.get("predicted") for s in fate_scored)
    fate = scores.get("Environmental Fate & Transport") or {}
    assert fate.get("_category_max") is not None


def test_existing_opera_bridge_catmos_still_works():
    opera = {
        "ok": True,
        "row": {"CATMoS_LD50_pred": "200", "LogBCF_pred": "2.5", "MolWeight": "58.08"},
        "display": {},
    }
    extras = p2oasys_source_bridges.opera_to_extra_sources(opera, existing_hazard={"toxicities": []})
    assert any("LD50" in t["value"] and t.get("predicted") for t in extras["toxicities"])
    assert extras["molecular_weight"] == 58.08
    assert extras.get("bcf_l_kg") is not None
