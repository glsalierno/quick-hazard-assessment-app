"""CAMEO Chemicals NFPA 704 local sqlite lookup (no network)."""

from pathlib import Path

from utils.cameo_lookup import lookup_cameo, nfpa_property_rows

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
    assert hit is None


def test_nfpa_property_rows_show_cameo_digits():
    rows = nfpa_property_rows("67-66-3", pubchem_nfpa="Health 1")
    by_prop = {r["Property"]: r["Value"] for r in rows}
    assert by_prop["NFPA Health"] == "2"
    assert by_prop["NFPA Fire"] == "0"
    assert "NFPA (PubChem)" in by_prop
