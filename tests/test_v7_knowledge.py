"""v7 knowledge, SDS cache, providers, and structured SDS parser."""

from __future__ import annotations

from pathlib import Path

from v7.evidence import evidence, prefer
from v7.knowledge import compare_p2oasys, merge_layers
from v7.providers.base import SDSHit
from v7.providers.manual import ManualProvider
from v7.providers.sigma import SigmaPublicResolver
from v7.sds_cache import SDSCache, sha256_bytes
from v7.sds_structured import parse_structured_sds
from v7.providers.base import SDSDownload


def test_prefer_never_overwrites_curated_with_pubchem():
    curated = evidence(2, source="DoSS", provider="doss", confidence="curated", citation="doss")
    live = evidence(4, source="PubChem", provider="pubchem", confidence="experimental", citation="pc")
    assert prefer(curated, live).value == 2
    assert prefer(live, curated).value == 2


def test_merge_priority_manual_wins():
    layers = merge_layers(
        {"nfpa_health": evidence(1, source="PubChem", provider="pubchem", confidence="experimental", citation="a")},
        {"nfpa_health": evidence(3, source="DoSS", provider="doss", confidence="curated", citation="b")},
        {"nfpa_health": evidence(0, source="override", provider="manual", confidence="curated", citation="c")},
    )
    assert layers["nfpa_health"].value == 0
    assert layers["nfpa_health"].provider == "manual"


def test_p2oasys_difference_reason():
    cmp_ = compare_p2oasys(doss_score=5.0, computed_score=7.0)
    assert cmp_.difference == 2.0
    assert "more hazardous" in cmp_.reason
    missing = compare_p2oasys(doss_score=None, computed_score=None)
    assert missing.reason == "P2OASys assessment required"


def test_sds_cache_reuses_identical_pdf(tmp_path: Path):
    cache = SDSCache(tmp_path / "SDS")
    hit = SDSHit(
        cas="67-64-1",
        provider="manual",
        manufacturer="TestCo",
        product_id="T1",
        title="Acetone",
        sds_url=None,
        revision="2024-01",
    )
    pdf = b"%PDF-1.4 test-sds-bytes"
    d1 = SDSDownload(hit=hit, pdf_bytes=pdf, sha256=sha256_bytes(pdf))
    a = cache.store(d1, parsed={"ok": True})
    d2 = SDSDownload(hit=hit, pdf_bytes=pdf, sha256=sha256_bytes(pdf))
    b = cache.store(d2)
    assert a.sha256 == b.sha256
    assert b.reused_pdf is True
    assert b.parsed == {"ok": True}


def test_manual_provider(tmp_path: Path):
    pdf = tmp_path / "sds.pdf"
    pdf.write_bytes(b"%PDF-1.4 x")
    cat = tmp_path / "manual.csv"
    cat.write_text(
        f"cas,pdf_path,manufacturer,revision\n67-64-1,{pdf.as_posix()},Acme,2020\n",
        encoding="utf-8",
    )
    prov = ManualProvider(cat)
    hits = prov.find("67-64-1")
    assert len(hits) == 1
    dl = prov.download(hits[0])
    assert dl.pdf_bytes.startswith(b"%PDF")


def test_sigma_resolver_parses_doss_msds_url():
    r = SigmaPublicResolver(None)
    url = "http://www.sigmaaldrich.com/MSDS/MSDS/DisplayMSDSPage.do?productNumber=484229&brand=ALDRICH"
    assert r.catalog_from_doss_url(url) == "484229"


def test_structured_sds_sections():
    sections = {
        2: "Signal Word: Danger\nH225 H319\nP210\nFlame pictogram",
        8: "Gloves: nitrile\nOSHA PEL 1000 ppm\nTLV 250 ppm\nIDLH 2500 ppm",
        9: "Flash Point: -20 C\nBoiling point: 56 C",
        14: "UN 1090 Packing Group II",
        15: "TSCA listed",
    }
    data = parse_structured_sds(sections, full_text="\n".join(sections.values()))
    assert data["section_2"]["signal_word"] == "Danger"
    assert "H225" in data["section_2"]["h_statements"]
    assert data["section_8"]["osha_pel"]["value"] == "1000"
    assert data["section_9"]["flash_point"]
    assert data["section_14"]["un_number"] == "1090"
