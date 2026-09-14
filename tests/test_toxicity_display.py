"""Prioritized toxicity view excludes blank rows (GHhaz6)."""

from __future__ import annotations

from utils import data_formatter


def test_blank_endpoint_and_value_excluded_from_prioritized():
    pubchem = {
        "toxicities": [
            {"type": "", "value": "", "unit": None, "route": "—", "species": "—"},
            {
                "type": "LD50",
                "value": "100 mg/kg",
                "unit": "mg/kg",
                "route": "Oral",
                "species": "rat",
                "source_section": "Toxicity",
            },
        ]
    }
    pri = data_formatter.prioritize_toxicity_data(pubchem, None)
    assert pri["incomplete"]
    assert any(x.get("flag") == "parser_incomplete" for x in pri["incomplete"])
    df = data_formatter.build_toxicity_display_df(pri)
    assert len(df) == 1
    assert "100" in str(df.iloc[0]["Value"])


def test_preferred_name_fallback():
    from services.chemical_assessment import ChemicalAssessmentService, ChemicalIdentity, InputSource

    ident = ChemicalIdentity(cas="381-73-7", source=InputSource.TYPED_CAS)
    name = ChemicalAssessmentService._resolve_preferred_name(
        ident,
        {"preferred_name": ""},
        {"iupac_name": "2,2-difluoroacetic acid", "title": None},
    )
    assert name == "2,2-difluoroacetic acid"
