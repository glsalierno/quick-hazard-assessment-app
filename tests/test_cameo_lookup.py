"""CAMEO Chemicals NFPA 704 local sqlite lookup."""

from pathlib import Path

from utils.cameo_lookup import cameo_hazard_metrics, lookup_cameo
from utils.hazard_for_p2oasys import build_hazard_data, merge_cameo_extra
from utils.p2oasys_scorer import _extract_nfpa_fire, _extract_nfpa_health
from v7.evidence import prefer, evidence
from v7.knowledge import evidence_from_cameo, merge_layers


BUNDLE = Path(__file__).resolve().parents[1] / "data" / "cameo_nfpa.sqlite"


def test_cameo_prefers_pure_dcm_not_mixture():
    hit = lookup_cameo("75-09-2", sqlite_path=BUNDLE)
    assert hit is not None
    assert hit["name"] == "DICHLOROMETHANE"
    assert hit["nfpa_flame"] == 1
    assert hit["nfpa_health"] == 2


def test_cameo_chloroform_and_acetone():
    ch = lookup_cameo("67-66-3", sqlite_path=BUNDLE)
    ac = lookup_cameo("67-64-1", sqlite_path=BUNDLE)
    assert ch["nfpa_health"] == 2 and ch["nfpa_flame"] == 0
    assert ac["nfpa_health"] == 1 and ac["nfpa_flame"] == 3


def test_cameo_co2_has_no_false_mixture_diamond():
    hit = lookup_cameo("124-38-9", sqlite_path=BUNDLE)
    assert hit is None  # bundled extract only stores preferred rows that have NFPA


def test_cameo_feeds_p2oasys_scorer():
    extra = merge_cameo_extra("67-66-3", {})
    hd = build_hazard_data({}, extra_sources=extra)
    assert _extract_nfpa_health(hd) == 2
    assert _extract_nfpa_fire(hd) == 0
    assert any("CAMEO" in x for x in hd["hazard_metrics"]["nfpa"])


def test_cameo_precautionary_max_over_pubchem():
    extra = {"hazard_metrics": {"nfpa": ["1 - Materials that can cause significant irritation"]}}
    extra = merge_cameo_extra("67-66-3", extra)  # CAMEO health 2
    hd = build_hazard_data({}, extra_sources=extra)
    assert _extract_nfpa_health(hd) == 2


def test_cameo_knowledge_loses_to_doss():
    layers = merge_layers(
        evidence_from_cameo("67-64-1"),
        {"nfpa_health": evidence(3, source="DoSS", provider="doss", confidence="curated", citation="d")},
    )
    assert layers["nfpa_health"].provider == "doss"
    assert layers["nfpa_health"].value == 3


def test_cameo_beats_pubchem_in_knowledge():
    pub = evidence(1, source="PubChem", provider="pubchem", confidence="experimental", citation="p")
    cameo = evidence(2, source="CAMEO", provider="cameo", confidence="experimental", citation="c")
    assert prefer(pub, cameo).provider == "cameo"
    assert prefer(cameo, pub).provider == "cameo"


def test_cameo_hazard_metrics_strings():
    rows = cameo_hazard_metrics("67-64-1")
    assert any(s.startswith("1 - Health") for s in rows)
    assert any("Fire" in s for s in rows)


def test_nfpa_property_rows_show_cameo_digits():
    from utils.cameo_lookup import nfpa_property_rows

    rows = nfpa_property_rows("67-66-3", pubchem_nfpa="Health 1")
    by_prop = {r["Property"]: r["Value"] for r in rows}
    assert by_prop["NFPA Health"] == "2"
    assert by_prop["NFPA Fire"] == "0"
    assert "NFPA (PubChem)" in by_prop
