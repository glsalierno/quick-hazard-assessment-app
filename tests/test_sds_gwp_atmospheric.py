"""Unit tests: Atmospheric GWP rule + SDS §12 eco mining → P2OASys extras."""

from __future__ import annotations

from pathlib import Path

import config
from utils import atmo_gwp, hazard_for_p2oasys, lookup_tables, p2oasys_scorer, p2oasys_source_bridges
from v7.sds_structured import parse_structured_sds


def test_classify_physical_state_gas_and_nongas():
    assert atmo_gwp.classify_physical_state("Colorless liquid") == "non_gas"
    assert atmo_gwp.classify_physical_state("White crystalline solid") == "non_gas"
    assert atmo_gwp.classify_physical_state("Compressed gas") == "gas"
    assert atmo_gwp.classify_physical_state("Liquefied gas") == "gas"
    assert atmo_gwp.classify_physical_state("aqueous solution") == "non_gas"
    assert atmo_gwp.classify_physical_state("") == "unknown"


def test_non_gas_gwp_rule_sets_zero_and_scores_atmospheric():
    extra = lookup_tables.get_lookup_extra_sources("67-56-1")  # methanol — no GWP in tables
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        extra, physical_state="non_gas", state_source="sds_section_9"
    )
    assert extra["hazard_metrics"]["gwp100"] == [0.0]
    assert extra["gwp_meta"]["source"] == "heuristic_non_gas_gwp0"
    assert any(str(d).startswith("GWP 0") for d in extra["hazard_metrics"]["other_designations"])

    hazard = hazard_for_p2oasys.build_hazard_data({}, extra_sources=extra)
    hazard = atmo_gwp.merge_gwp_into_hazard_data(hazard, extra)
    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hazard, matrix)
    atmo = scores.get("Atmospheric Hazard") or {}
    assert atmo.get("_category_max") is not None
    # GWP Relative to CO2 should score low (bin 2) for GWP=0
    gh = None
    for sub, bundle in atmo.items():
        if str(sub).startswith("_") or not isinstance(bundle, dict):
            continue
        if "GWP Relative to CO2" in bundle:
            gh = bundle["GWP Relative to CO2"]
    assert gh == 2
    assert trace["evidence"].get("gwp100", {}).get("value") == 0.0


def test_gas_with_atmo_hit_gets_numeric_gwp():
    atmo_dir = atmo_gwp.resolve_atmo_dir(
        config.ATMO_DIR, repo_root=config.REPO_ROOT, fastp2oasys_dir=config.FASTP2OASYS_DIR
    )
    assert atmo_dir is not None
    ipcc = atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir)
    # HFC-134a
    cas = "811-97-2"
    cas_norm = lookup_tables.normalize_cas_for_lookup(cas)
    assert cas_norm in ipcc
    extra = lookup_tables.get_lookup_extra_sources(cas, ipcc_gwp_by_cas=ipcc)
    assert extra["hazard_metrics"]["gwp100"][0] == ipcc[cas_norm]
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        extra, physical_state="gas", state_source="sds_section_9"
    )
    # Must NOT zero out for gas
    assert extra["hazard_metrics"]["gwp100"][0] == ipcc[cas_norm]
    hazard = hazard_for_p2oasys.build_hazard_data({}, extra_sources=extra)
    hazard = atmo_gwp.merge_gwp_into_hazard_data(hazard, extra)
    assert p2oasys_scorer._extract_gwp100(hazard) == ipcc[cas_norm]
    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    scores, _ = p2oasys_scorer.compute_p2oasys_scores_with_trace(hazard, matrix)
    atmo = scores.get("Atmospheric Hazard") or {}
    assert atmo.get("_category_max") is not None


def test_gas_missing_from_tables_defaults_to_zero():
    # Helium-like: gas, not in IPCC/CSV → default GWP/ODP 0 (not on authoritative lists)
    extra = lookup_tables.get_lookup_extra_sources("7440-59-7")
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        extra or {}, physical_state="gas", state_source="pubchem"
    )
    hm = (extra or {}).get("hazard_metrics") or {}
    assert hm.get("gwp100") == [0.0]
    assert hm.get("odp") == [0.0]
    assert (extra.get("gwp_meta") or {}).get("source") == "default_not_on_authoritative_list"
    assert (extra.get("odp_meta") or {}).get("source") == "default_not_on_authoritative_list"


def test_gas_missing_default_zero_can_be_disabled():
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        {}, physical_state="gas", state_source="pubchem", default_zero_if_unlisted=False
    )
    hm = (extra or {}).get("hazard_metrics") or {}
    assert not hm.get("gwp100")
    assert (extra.get("gwp_meta") or {}).get("source") == "missing"


def test_sds_section12_aquatic_to_eco_extra():
    sections = {
        9: "Physical state: liquid\nFlash point: 12 °C\n",
        11: "Acute toxicity\nLD50 Oral rat 5628 mg/kg\n",
        12: (
            "Ecotoxicity\nToxicity to fish: LC50 15400 mg/L\n"
            "Persistence and degradability: Readily biodegradable\n"
            "Bioaccumulative potential: Will not bioaccumulate. BCF 1.0\n"
        ),
    }
    structured = parse_structured_sds(sections)
    assert structured["physical_state"] and "liquid" in structured["physical_state"].lower()
    assert structured["section_12"]["aquatic_toxicity"]
    assert structured["section_12"]["bcf"] == 1.0
    assert any("Readily" in c or "biodegrad" in c.lower() for c in structured["section_12"]["phrase_cues"])

    fields = p2oasys_source_bridges.structured_sds_to_extra_fields(structured)
    assert fields.get("physical_state")
    assert fields.get("aquatic_toxicity") == 15400.0
    xs = p2oasys_source_bridges.sds_fields_to_extra_sources(fields)
    tox_blob = " ".join(str(t.get("value")) for t in xs.get("toxicities") or [])
    assert "LC50 15400" in tox_blob or "15400" in tox_blob
    assert "SDS_section_12" in str(xs)


def test_resolve_atmo_dir_finds_fastp2oasys():
    d = atmo_gwp.resolve_atmo_dir(
        None, repo_root=config.REPO_ROOT, fastp2oasys_dir=config.FASTP2OASYS_DIR
    )
    assert d is not None
    assert d.is_dir()
    assert list(d.glob("IPCC_*.parquet"))
