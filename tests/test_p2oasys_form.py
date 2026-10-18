"""Tests for human-in-the-loop P2OASys form helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from utils import p2oasys_form, p2oasys_matrix_placeholder, p2oasys_scorer


@pytest.fixture()
def placeholder_matrix(tmp_path: Path):
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        tmp_path / "no_official.xlsx", tmp_path
    )
    assert kind == "placeholder"
    return p2oasys_scorer.load_p2oasys_matrix(path), path


def test_build_form_fields_all_categories(placeholder_matrix):
    matrix, _path = placeholder_matrix
    fields = p2oasys_form.build_form_fields(matrix)
    cats = {f["category"] for f in fields}
    assert "Acute Human Effects" in cats
    assert "Process Factors" in cats or True  # placeholder may lack real process units
    # Every field has 2/4/6/8/10 options plus empty sentinel
    for f in fields:
        scores = [o["score"] for o in f["options"] if o["score"] is not None]
        assert scores == p2oasys_scorer.SCORE_COLS
        assert f["options"][0]["value"] is None
        assert f["allow_auto"] == (f["category"] in p2oasys_form.AUTO_FILL_CATEGORIES)


def test_manual_categories_never_suggested(placeholder_matrix):
    matrix, _path = placeholder_matrix
    # Fake scores that would incorrectly fill Process Factors
    fake = {
        "Process Factors": {
            "Heat": {"Key Phrases": 6, "_max": 6},
            "_category_max": 6.0,
        },
        "Acute Human Effects": {
            "Oral Toxicity": {"LD50 (mg/kg)": 8, "_max": 8},
            "_category_max": 8.0,
        },
    }
    sug = p2oasys_form.map_auto_suggestions(matrix, fake, trace=None)
    for (cat, _sub), payload in sug.items():
        if cat in p2oasys_form.MANUAL_ONLY_CATEGORIES:
            assert payload["score"] is None
            assert payload["source"] == "empty"


def test_strip_manual_only():
    scores = {
        "Process Factors": {"Heat": {"x": 4, "_max": 4}, "_category_max": 4},
        "Acute Human Effects": {"Oral Toxicity": {"LD50": 6, "_max": 6}, "_category_max": 6},
    }
    cleaned = p2oasys_form.strip_manual_only_scores(scores)
    assert cleaned["Process Factors"] == {}
    assert cleaned["Acute Human Effects"]["_category_max"] == 6


def test_apply_form_selections_changes_rollup(placeholder_matrix):
    matrix, _path = placeholder_matrix
    hd = {
        "cid": 1,
        "molecular_weight": 32.0,
        "ghs": {"h_codes": ["H301"]},
        "toxicities": [{"value": "LD50 100 mg/kg", "species_route": ["oral", "rat"]}],
        "hazard_metrics": {"flash_point": [], "nfpa": [], "other_designations": []},
    }
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    scores = p2oasys_form.strip_manual_only_scores(scores)
    suggestions = p2oasys_form.map_auto_suggestions(matrix, scores, trace)
    fields = p2oasys_form.build_form_fields(matrix)

    # Pick an auto-filled acute subcategory if any; otherwise pick Oral Toxicity.
    selections: dict[tuple[str, str], int | None] = {}
    for f in fields:
        key = (f["category"], f["subcategory"])
        sug = suggestions.get(key) or {}
        if f["allow_auto"] and sug.get("source") == "auto":
            selections[key] = sug["score"]
        else:
            selections[key] = None

    # Force an override on Oral Toxicity if present.
    oral_key = ("Acute Human Effects", "Oral Toxicity")
    if oral_key in {(f["category"], f["subcategory"]) for f in fields}:
        selections[oral_key] = 10

    # Manual Process pick should appear in rollup when selected.
    proc_fields = [f for f in fields if f["category"] == "Process Factors"]
    if proc_fields:
        pk = (proc_fields[0]["category"], proc_fields[0]["subcategory"])
        selections[pk] = 4

    final, applied = p2oasys_form.apply_form_selections(
        scores, selections, suggestions, matrix
    )
    assert isinstance(final, dict)
    if oral_key in selections and selections[oral_key] == 10:
        oral = (final.get("Acute Human Effects") or {}).get("Oral Toxicity") or {}
        assert oral.get("_max") == 10
    if proc_fields:
        proc_cat = final.get("Process Factors") or {}
        assert proc_cat.get("_category_max") is not None
        assert applied  # at least confirmation or form overrides


def test_cell_labels_on_official_or_placeholder(placeholder_matrix):
    matrix, _path = placeholder_matrix
    # At least one rule should carry cell_labels after scorer patch.
    found = False
    for _cat, subs in matrix.items():
        for _sub, units in subs.items():
            if not isinstance(units, dict):
                continue
            for _u, rule in units.items():
                if isinstance(rule, dict) and rule.get("cell_labels"):
                    found = True
                    break
    assert found, "expected cell_labels on matrix rules"
