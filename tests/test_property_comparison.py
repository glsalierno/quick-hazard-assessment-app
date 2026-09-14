"""CAS/PubChem/OPERA physical-property comparison tests."""

from __future__ import annotations

from utils.property_comparison import build_property_comparison


def _cas_data() -> dict:
    return {
        "molecular_mass": "96.03",
        "experimental_properties": [
            {
                "name": "Melting Point",
                "raw_value": "-1 °C",
                "value": -1.0,
                "unit": "°C",
            },
            {
                "name": "Boiling Point",
                "raw_value": "134 °C",
                "value": 134.0,
                "unit": "°C",
            },
            {
                "name": "Density",
                "raw_value": "1.528 g/cm3 @ Temp: 20 °C",
                "value": 1.528,
                "unit": "g/cm3",
            },
        ],
    }


def test_builds_source_labeled_comparison_rows() -> None:
    result = build_property_comparison(
        _cas_data(),
        {"mw": "96.03", "xlogp": 0.6},
        {
            "row": {
                "MolWeight": "96.03",
                "LogP_pred": "3.98",
                "MP_pred": "15.0",
                "BP_pred": "130.0",
            },
            "warnings": [],
        },
    )
    by_name = {row["Property"]: row for row in result["rows"]}
    assert by_name["Molecular weight"]["CAS Common Chemistry (experimental)"] == "96.03"
    assert by_name["Molecular weight"]["PubChem"] == "96.03"
    assert by_name["Melting point"]["CAS Common Chemistry (experimental)"] == "-1 °C"
    assert by_name["Melting point"]["OPERA (predicted)"] == "15.0"
    assert by_name["Density"]["QA status"] == "Single-source evidence"


def test_logp_disagreement_is_visible_warning() -> None:
    result = build_property_comparison(
        {},
        {"xlogp": 0.6},
        {"row": {"LogP_pred": "3.98"}, "warnings": []},
    )
    assert any("LogP disagreement" in warning for warning in result["warnings"])
    logp = next(row for row in result["rows"] if row["Property"] == "LogP")
    assert logp["QA status"] == "Review disagreement"


def test_large_cas_opera_boiling_point_difference_warns() -> None:
    result = build_property_comparison(
        _cas_data(),
        {},
        {"row": {"BP_pred": "250.0"}, "warnings": []},
    )
    assert any("Boiling point disagreement" in warning for warning in result["warnings"])


def test_cas_range_uses_midpoint_for_comparison_but_displays_raw() -> None:
    cas = {
        "experimental_properties": [
            {
                "name": "Melting Point",
                "raw_value": "175-177 °C",
                "value_min": 175.0,
                "value_max": 177.0,
                "unit": "°C",
            }
        ]
    }
    result = build_property_comparison(
        cas,
        {},
        {"row": {"MP_pred": "180"}, "warnings": []},
    )
    row = result["rows"][0]
    assert row["CAS Common Chemistry (experimental)"] == "175-177 °C"
    assert row["QA status"].startswith("Agreement check passed")


def test_empty_sources_return_no_rows() -> None:
    result = build_property_comparison(None, None, None)
    assert result["rows"] == []
    assert result["warnings"] == []
