"""Unit tests for compile_from_tci (provider + assessment mocked; no live network)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from v7.compile_service import (
    TCIAmbiguousProductError,
    compile_from_tci,
    compile_from_tci_hit,
    find_tci_hits,
)
from v7.providers.base import SDSDocument, SDSHit
from v7.providers.tci import TCIDownloadError, TCINoSDSError, TCINotFoundError, TCIProvider


@pytest.fixture()
def catalog_csv(tmp_path: Path) -> Path:
    path = tmp_path / "tci.csv"
    path.write_text(
        "cas,product_number,name,notes\n"
        "67-56-1,M0097,Methanol [for Spectrophotometry],\n"
        "67-56-1,M0628,Methanol [for HPLC Solvent],\n"
        "141-78-6,A0030,Ethyl Acetate,\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture()
def tci_provider(catalog_csv: Path, tmp_path: Path) -> TCIProvider:
    from v7.sds_cache import SDSCache

    return TCIProvider(
        product_map_csv=catalog_csv,
        cache=SDSCache(tmp_path / "SDS"),
        enable_live_search=False,
        enable_document_search_fallback=False,
        min_interval_s=0,
    )


@dataclass
class _FakeIdentity:
    cas: str
    chemical_name: str | None = None
    warnings: list[str] = field(default_factory=list)
    confidence: float = 1.0


@dataclass
class _FakeResult:
    identity: _FakeIdentity
    pubchem_data: dict[str, Any] = field(default_factory=dict)
    dsstox_info: dict[str, Any] | None = None
    toxval_data: dict[str, Any] | None = None
    carc_potency_data: dict[str, Any] | None = None
    fetch_error: str | None = None
    cas_commonchem_data: dict[str, Any] | None = None
    cas_commonchem_error: str | None = None


class _FakeAssessmentService:
    def identify_from_sds(self, pdf_file: Any) -> list[_FakeIdentity]:
        return [_FakeIdentity(cas="67-56-1", chemical_name="Methanol")]

    def assess_identity(self, identity: _FakeIdentity) -> _FakeResult:
        return _FakeResult(identity=identity, pubchem_data={"ghs": {"signal_word": "Danger"}})

    def assess(self, query: str) -> _FakeResult:
        return _FakeResult(identity=_FakeIdentity(cas=str(query), chemical_name="Methanol"))

    def to_result_data(self, result: _FakeResult) -> dict[str, Any]:
        return {
            "clean_cas": result.identity.cas,
            "preferred_name": result.identity.chemical_name,
            "pubchem": result.pubchem_data,
            "dtxsid": None,
            "fetch_error": result.fetch_error,
        }


def _fake_lookups(cas: str) -> Any:
    from v7.compile_service import CompileLookups

    return CompileLookups(
        p2oasys={
            "status": "found",
            "message": "",
            "cas": cas,
            "name": "Methanol",
            "score": 6.6,
            "date_created": None,
            "source_path": "fake.csv",
            "single_substance": True,
        },
        hsp={
            "status": "not_found",
            "message": "missing",
            "cas": cas,
            "delta_d": None,
            "delta_p": None,
            "delta_h": None,
            "source": None,
            "source_path": None,
        },
        chem21={
            "status": "not_found",
            "message": "missing",
            "cas": cas,
            "solvent": None,
            "family": None,
            "safety": None,
            "health": None,
            "env": None,
            "ranking_default": None,
            "ranking_discussion": None,
            "source_path": None,
        },
        tables_meta={},
    )


def test_find_tci_hits_multiple(tci_provider: TCIProvider):
    hits = find_tci_hits(cas="67-56-1", provider=tci_provider)
    assert {h.product_id for h in hits} == {"M0097", "M0628"}


def test_compile_from_tci_ambiguous_without_product(tci_provider: TCIProvider):
    with pytest.raises(TCIAmbiguousProductError) as ei:
        compile_from_tci(cas="67-56-1", provider=tci_provider, assessment_service=_FakeAssessmentService())
    assert len(ei.value.hits) == 2


def test_compile_from_tci_with_product_mocked_download(tci_provider: TCIProvider):
    pdf = b"%PDF-1.4 methanol-compile-test"
    doc = SDSDocument(
        manufacturer="TCI",
        product_number="M0097",
        revision="2024/01/01",
        pdf_bytes=pdf,
        source_url="https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf",
        cas="67-56-1",
        sha256="abc123",
        reused=False,
        provider="tci",
        title="Methanol",
    )
    tci_provider.download_sds = MagicMock(return_value=doc)

    with patch("v7.compile_service.lookup_databases", side_effect=_fake_lookups):
        bundle = compile_from_tci(
            cas="67-56-1",
            product_number="M0097",
            provider=tci_provider,
            assessment_service=_FakeAssessmentService(),
        )

    assert bundle.cas == "67-56-1"
    assert bundle.source == "tci_sds"
    assert bundle.sds_meta["product_number"] == "M0097"
    assert bundle.sds_meta["revision"] == "2024/01/01"
    assert bundle.sds_meta["source_url"].endswith("M0097_US_EN.pdf")
    assert bundle.sds_meta["sha256"] == "abc123"
    assert bundle.sds_meta["reused_cache"] is False
    assert bundle.lookups.p2oasys["status"] == "found"
    tci_provider.download_sds.assert_called_once()


def test_compile_from_tci_hit_cache_reuse_flag(tci_provider: TCIProvider):
    hit = SDSHit(
        cas="67-56-1",
        provider="tci",
        manufacturer="TCI",
        product_id="M0097",
        title="Methanol",
        sds_url="https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf",
        revision=None,
    )
    doc = SDSDocument(
        manufacturer="TCI",
        product_number="M0097",
        revision=None,
        pdf_bytes=b"%PDF-1.4 cached",
        source_url=hit.sds_url,
        cas="67-56-1",
        sha256="deadbeef",
        reused=True,
        provider="tci",
        cached_path=Path("cache/SDS/67-56-1/TCI/unknown-rev/deadbeef/original.pdf"),
    )
    tci_provider.download_sds = MagicMock(return_value=doc)

    with patch("v7.compile_service.lookup_databases", side_effect=_fake_lookups):
        bundle = compile_from_tci_hit(
            hit, provider=tci_provider, assessment_service=_FakeAssessmentService()
        )

    assert bundle.sds_meta["reused_cache"] is True
    assert bundle.sds_meta["sha256"] == "deadbeef"
    assert "cache" in str(bundle.sds_meta.get("cached_path") or "")


def test_compile_from_tci_missing_map_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("v7.compile_service.config.TCI_PRODUCT_MAP_CSV", str(tmp_path / "missing.csv"))
    monkeypatch.setattr("v7.compile_service.config.TCI_ENABLE_LIVE_SEARCH", False)
    monkeypatch.setattr("v7.tci_catalog.config.TCI_PRODUCT_MAP_CSV", str(tmp_path / "missing.csv"))
    monkeypatch.setattr("v7.tci_catalog.config.TCI_LEARNED_CSV", str(tmp_path / "learned.csv"))
    monkeypatch.setattr("v7.tci_catalog.config.TCI_RUNTIME_CSV", str(tmp_path / "runtime.csv"))
    monkeypatch.setattr("v7.tci_catalog.config.DATA_DIR", str(tmp_path))
    provider = TCIProvider(
        product_map_csv=None,
        enable_live_search=False,
        min_interval_s=0,
    )
    with pytest.raises(TCINotFoundError) as ei:
        find_tci_hits(cas="67-56-1", provider=provider, enable_live_search=False)
    assert "mapped" in str(ei.value).lower() or "local map" in str(ei.value).lower() or "product" in str(ei.value).lower()


def test_remember_tci_mapping_grows_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from v7.tci_catalog import ensure_runtime_catalog, remember_tci_mapping

    monkeypatch.setattr("v7.tci_catalog.config.TCI_PRODUCT_MAP_CSV", str(tmp_path / "seed.csv"))
    monkeypatch.setattr("v7.tci_catalog.config.TCI_LEARNED_CSV", str(tmp_path / "learned.csv"))
    monkeypatch.setattr("v7.tci_catalog.config.TCI_RUNTIME_CSV", str(tmp_path / "runtime.csv"))
    monkeypatch.setattr("v7.tci_catalog.config.DATA_DIR", str(tmp_path))
    (tmp_path / "seed.csv").write_text(
        "cas,product_number,name,notes\n67-56-1,M0097,Methanol,\n", encoding="utf-8"
    )
    assert remember_tci_mapping("78-33-1", "X1234", name="test") is True
    assert remember_tci_mapping("78-33-1", "X1234") is False  # already present
    runtime = ensure_runtime_catalog()
    text = runtime.read_text(encoding="utf-8")
    assert "78-33-1" in text and "X1234" in text
    assert "M0097" in text


def test_compile_from_tci_try_next_product_on_no_sds(tci_provider: TCIProvider):
    pdf = b"%PDF-1.4 methanol-second-grade"
    ok_doc = SDSDocument(
        manufacturer="TCI",
        product_number="M0628",
        revision=None,
        pdf_bytes=pdf,
        source_url="https://www.tcichemicals.com/US/en/sds/M0628_US_EN.pdf",
        cas="67-56-1",
        sha256="second",
        reused=False,
        provider="tci",
        title="Methanol HPLC",
    )

    def _dl(product: str, **kwargs: Any) -> SDSDocument:
        if product == "M0097":
            raise TCINoSDSError("No SDS for M0097")
        return ok_doc

    tci_provider.download_sds = MagicMock(side_effect=_dl)
    with patch("v7.compile_service.lookup_databases", side_effect=_fake_lookups):
        bundle = compile_from_tci(
            cas="67-56-1",
            provider=tci_provider,
            assessment_service=_FakeAssessmentService(),
            allow_first_hit=True,
            try_next_on_sds_fail=True,
        )
    assert bundle.sds_meta["product_number"] == "M0628"
    assert any("M0097" in w for w in bundle.warnings)


def test_compile_from_tci_propagates_no_sds(tci_provider: TCIProvider):
    tci_provider.download_sds = MagicMock(side_effect=TCINoSDSError("No SDS PDF at url"))
    with pytest.raises(TCINoSDSError):
        compile_from_tci(
            product_number="M0097",
            cas="67-56-1",
            provider=tci_provider,
            assessment_service=_FakeAssessmentService(),
        )


def test_compile_from_tci_propagates_403(tci_provider: TCIProvider):
    tci_provider.download_sds = MagicMock(
        side_effect=TCIDownloadError("TCI blocked the request (HTTP 403) for url")
    )
    with pytest.raises(TCIDownloadError) as ei:
        compile_from_tci(
            product_number="M0097",
            provider=tci_provider,
            assessment_service=_FakeAssessmentService(),
        )
    assert "403" in str(ei.value)


def test_user_facing_sds_error_mentions_akamai():
    from v7.sds_acquisition import user_facing_sds_error

    msg = user_facing_sds_error(TCIDownloadError("TCI blocked the request (HTTP 403)"))
    assert "Akamai" in msg or "403" in msg
