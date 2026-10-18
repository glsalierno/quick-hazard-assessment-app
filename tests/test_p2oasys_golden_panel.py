"""Offline tests for golden-panel loaders and SDS field flattening (no PubChem)."""

from __future__ import annotations

from pathlib import Path

import pytest

from utils import p2oasys_matrix_placeholder, p2oasys_scorer, p2oasys_source_bridges
import config


def test_official_matrix_is_installed():
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH), Path(config.DATA_DIR)
    )
    assert kind == "official", f"expected official matrix, got {kind} at {path}"
    fp = p2oasys_scorer.matrix_fingerprint(path)
    assert fp["exists"] and fp["sha256"]
    matrix = p2oasys_scorer.load_p2oasys_matrix(path)
    assert "Acute Human Effects" in matrix
    assert "Inhalation Toxicity" in matrix["Acute Human Effects"]


def test_ground_truth_db_loadable():
    # Import script helpers without running main.
    import importlib.util

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_p2oasys_golden_panel.py"
    spec = importlib.util.spec_from_file_location("golden_panel", script)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    db = mod._default_gt_db()
    assert db.is_file(), f"missing GT DB at {db}"
    gt = mod.load_ground_truth_by_cas(db)
    assert "71-43-2" in gt  # benzene
    assert "Acute Human Effects" in gt["71-43-2"]["scores"]
    assert gt["71-43-2"]["scores"]["Acute Human Effects"] == 10.0


def test_sds_parsed_result_flattening():
    parsed = {
        "ghs": {"h_codes": ["H225", "H319"], "p_codes": ["P210"]},
        "quantitative": {
            "flash_point": [{"value_c": -20.0, "unit": "C"}],
            "vapor_pressure": [{"value": 180, "unit": "mmHg"}],
            "aquatic_toxicity": [{"value": 12.5, "unit": "mg/L"}],
        },
    }
    flat = p2oasys_source_bridges.sds_parsed_result_to_extra_fields(parsed)
    assert flat["ghs_h_codes"] == ["H225", "H319"]
    assert "flash_point" in flat
    extras = p2oasys_source_bridges.sds_fields_to_extra_sources(flat)
    assert "H225" in extras["ghs"]["h_codes"]
    assert "p_codes" not in extras.get("ghs", {})
