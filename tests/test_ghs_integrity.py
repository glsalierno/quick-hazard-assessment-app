"""GHS completeness / no silent drops (GHhaz6)."""

from __future__ import annotations

from utils import ghs_formatter, qa_checks


def test_account_ghs_no_silent_drop_381_style():
    h_codes = ["H314", "H227", "H318", "H402", "H410"]
    acc = ghs_formatter.account_ghs_codes(h_codes, [])
    retrieved = set(acc["retrieved_h_codes"])
    accounted = (
        set(acc["displayed_h_codes"])
        | set(acc["unmapped_h_codes"])
        | {x["code"] for x in acc["suppressed_or_subsumed_h_codes"]}
    )
    assert retrieved == accounted
    assert acc["h_complete"] is True
    # Lexicon has H227, H402, H410, H314
    assert "H227" in acc["displayed_h_codes"]
    assert "H402" in acc["displayed_h_codes"]
    assert "H410" in acc["displayed_h_codes"]
    assert "H314" in acc["displayed_h_codes"]
    # H318 subsumed by H314 when both present
    subsumed_codes = {x["code"] for x in acc["suppressed_or_subsumed_h_codes"]}
    assert "H318" in subsumed_codes
    assert "H318" not in acc["unmapped_h_codes"]


def test_h318_displayed_when_no_h314():
    acc = ghs_formatter.account_ghs_codes(["H318"], [])
    assert "H318" in acc["displayed_h_codes"]
    assert not acc["suppressed_or_subsumed_h_codes"]


def test_unknown_code_goes_to_unmapped():
    acc = ghs_formatter.account_ghs_codes(["H999"], [])
    assert "H999" in acc["unmapped_h_codes"]
    assert "H999" not in acc["displayed_h_codes"]
    assert acc["h_complete"] is True


def test_qa_status_build():
    result = {
        "clean_cas": "381-73-7",
        "pubchem": {
            "ghs": {"h_codes": ["H314", "H227", "H318", "H402", "H410"], "p_codes": []},
            "ecotoxicity": {"entries": []},
            "parser_version": "pubchem_client_v6",
        },
    }
    status = qa_checks.build_qa_status(result)
    assert status["retrieved_h_count"] == 5
    assert status["accounting"]["h_complete"] is True
    assert status["ok"] is True


def test_v5_v6_regression_381_73_7_no_silent_ghs_drop():
    """Exact observed mismatch class: table had H314/H227/H318/H402/H410; phrases missed some."""
    input_h = ["H314", "H227", "H318", "H402", "H410"]
    acc = ghs_formatter.account_ghs_codes(input_h, [])
    retrieved = set(acc["retrieved_h_codes"])
    accounted = (
        set(acc["displayed_h_codes"])
        | set(acc["unmapped_h_codes"])
        | {x["code"] for x in acc["suppressed_or_subsumed_h_codes"]}
    )
    assert retrieved == set(input_h)
    assert retrieved == accounted
    assert not (retrieved - accounted), "no H-code may disappear"

    for code in ("H314", "H227", "H402", "H410"):
        assert code in acc["displayed_h_codes"]
        assert code in acc["displayed_h_phrases"]
        assert ghs_formatter.phrase_is_found(acc["displayed_h_phrases"][code])

    h318_displayed = "H318" in acc["displayed_h_codes"]
    h318_subsumed = any(
        x.get("code") == "H318" for x in acc["suppressed_or_subsumed_h_codes"]
    )
    assert h318_displayed or h318_subsumed
    if h318_subsumed:
        item = next(x for x in acc["suppressed_or_subsumed_h_codes"] if x["code"] == "H318")
        assert item.get("primary") == "H314"
        assert "H314" in (item.get("reason") or "")

    result = {
        "clean_cas": "381-73-7",
        "preferred_name": "Difluoroacetic acid",
        "pubchem": {
            "cid": 9777,
            "ghs": {"h_codes": input_h, "p_codes": []},
            "ecotoxicity": {"entries": [], "aquatic_ghs_codes": ["H402", "H410"]},
            "parser_version": "pubchem_client_v6",
        },
    }
    status = qa_checks.build_qa_status(result)
    assert status["accounting"]["h_complete"] is True
    assert status.get("retrieved_h_count") == 5
    assert not any(
        i.get("code") in ("ghs_h_incomplete", "ghs_p_incomplete")
        for i in (status.get("issues") or [])
    )
