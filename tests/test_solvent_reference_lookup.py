"""Lookup-only P2OASys / CHEM21 / Hansen tables (no predicted scores)."""

from __future__ import annotations

from pathlib import Path

from utils.solvent_reference_lookup import (
    CHEM21_NOT_IN_GUIDE,
    P2OASYS_ASSESSMENT_REQUIRED,
    load_chem21_guide_by_cas,
    load_hsp_csv_by_cas,
    load_p2oasys_expert_by_cas,
    lookup_chem21,
    lookup_hsp,
    lookup_p2oasys_expert,
    resolve_lookup_tables,
)


def test_p2oasys_found_and_missing(tmp_path: Path):
    csv_path = tmp_path / "p2oasys_single_cas_clean.csv"
    csv_path.write_text(
        "name,CAS,p2oasys_score,date_created\n"
        "Acetone,67-64-1,5.2,2024-01-01\n",
        encoding="utf-8",
    )
    table = load_p2oasys_expert_by_cas(csv_path)
    hit = lookup_p2oasys_expert("67-64-1", table)
    assert hit["status"] == "found"
    assert hit["score"] == 5.2
    assert hit["name"] == "Acetone"

    miss = lookup_p2oasys_expert("71-43-2", table)
    assert miss["status"] == "not_found"
    assert miss["message"] == P2OASYS_ASSESSMENT_REQUIRED
    assert miss["score"] is None


def test_p2oasys_source_unavailable():
    miss = lookup_p2oasys_expert("67-64-1", None)
    assert miss["status"] == "source_unavailable"
    assert miss["score"] is None


def test_chem21_found_and_missing(tmp_path: Path):
    csv_path = tmp_path / "CHEM21_full.csv"
    csv_path.write_text(
        "CAS,Family,Solvent,Safety,Health,Env,Ranking Default,Ranking Discussion,graph\n"
        "67-64-1,Ketones,Acetone,5,3,5,Problematic,Recommended,{}\n",
        encoding="utf-8",
    )
    table = load_chem21_guide_by_cas(csv_path)
    hit = lookup_chem21("67-64-1", table)
    assert hit["status"] == "found"
    assert hit["ranking_default"] == "Problematic"
    assert hit["safety"] == 5.0
    miss = lookup_chem21("50-00-0", table)
    assert miss["status"] == "not_found"
    assert miss["message"] == CHEM21_NOT_IN_GUIDE


def test_hsp_prefers_hspip_cache(tmp_path: Path):
    cache = tmp_path / "hsp.csv"
    cache.write_text("cas,dD,dP,dH\n67-64-1,15.5,10.4,7.0\n", encoding="utf-8")
    hspip = load_hsp_csv_by_cas(cache)
    doss = {"67641": {"delta_d": 1.0, "delta_p": 2.0, "delta_h": 3.0, "source": "DoSS", "source_path": "x"}}
    hit = lookup_hsp("67-64-1", doss_table=doss, hspip_table=hspip)
    assert hit["status"] == "found"
    assert hit["delta_d"] == 15.5
    assert hit["source"] == "hspip_cache"


def test_resolve_missing_files(tmp_path: Path):
    tables = resolve_lookup_tables(
        p2oasys_csv=tmp_path / "nope.csv",
        chem21_csv=tmp_path / "nope2.csv",
    )
    assert tables["p2oasys"] is None
    assert tables["chem21"] is None
