"""Ecotoxicity parser guardrails (GHhaz6)."""

from __future__ import annotations

from utils.pubchem_client import _extract_ecotoxicity, _is_quantitative_ecotox_candidate


def test_contaminated_classification_text_rejected():
    raw = (
        "Flammable liquids - Category 4 Skin corrosion/irritation - Category 1B "
        "Serious eye damage/eye irritation - Category 1"
    )
    assert _is_quantitative_ecotox_candidate(raw, "GHS Classification") is False


def test_real_aquatic_lc50_accepted():
    raw = "LC50; Species: Pimephales promelas (Fathead Minnow); Concentration: 10 mg/L for 96 h"
    assert _is_quantitative_ecotox_candidate(raw, "Ecotoxicology") is True


def test_extract_keeps_aquatic_ghs_separate():
    ghs = {"h_codes": ["H314", "H402", "H410"]}
    toxicities = [
        {
            "value": (
                "Flammable liquids - Category 4 Skin corrosion/irritation - Category 1B "
                "Serious eye damage - Category 1"
            ),
            "source_section": "GHS Classification",
        },
        {
            "value": "LC50 rainbow trout 5.2 mg/L 96 h",
            "source_section": "Ecotoxicological Information",
        },
    ]
    out = _extract_ecotoxicity(ghs, toxicities)
    assert "H402" in out["h_codes_aquatic"] or "H410" in out["h_codes_aquatic"]
    for e in out["entries"]:
        blob = (e.get("value") or "").lower()
        assert "flammable liquids" not in blob
        assert "skin corrosion" not in blob
    assert any("lc50" in (e.get("value") or "").lower() for e in out["entries"])


def test_aquatic_ghs_only_flag():
    ghs = {"h_codes": ["H402"]}
    out = _extract_ecotoxicity(ghs, [])
    assert out["aquatic_ghs_classification_only"] is True
    assert out["entries"] == []
