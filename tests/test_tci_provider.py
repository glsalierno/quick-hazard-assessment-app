"""Unit tests for TCIProvider (HTTP mocked; no live network required)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from v7.providers.tci import (
    TCIBlockedError,
    TCIDownloadError,
    TCINoSDSError,
    TCINotFoundError,
    TCIProvider,
    TCIPublicResolver,
    _parse_search_hits,
    get_tci_cooldown_remaining,
    reset_tci_cooldown,
    set_tci_cooldown,
)
from v7.sds_cache import SDSCache


@pytest.fixture()
def catalog_csv(tmp_path: Path) -> Path:
    path = tmp_path / "tci.csv"
    path.write_text(
        "cas,product_number,name,notes\n"
        "67-56-1,M0097,Methanol [for Spectrophotometry],\n"
        "67-56-1,M0628,Methanol [for HPLC Solvent],\n"
        "141-78-6,A0030,Ethyl Acetate,\n"
        "67-68-5,D0798,Dimethyl Sulfoxide,\n",
        encoding="utf-8",
    )
    return path


def test_resolver_sds_url_and_product_validation(catalog_csv: Path):
    r = TCIPublicResolver(catalog_csv, region="US/en", country="US", language="EN")
    assert r.sds_url_for_product("m0097") == "https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf"
    with pytest.raises(ValueError):
        r.normalize_product("BAD")


def test_find_by_cas_multiple_products(catalog_csv: Path):
    p = TCIProvider(product_map_csv=catalog_csv, enable_live_search=False, min_interval_s=0)
    hits = p.find_by_cas("67-56-1")
    assert len(hits) == 2
    assert {h.product_id for h in hits} == {"M0097", "M0628"}
    assert all(h.manufacturer == "TCI" for h in hits)
    assert all(h.sds_url and h.sds_url.endswith("_US_EN.pdf") for h in hits)


def test_find_by_cas_missing(catalog_csv: Path):
    p = TCIProvider(product_map_csv=catalog_csv, enable_live_search=False, min_interval_s=0)
    assert p.find_by_cas("00-00-0") == []
    with pytest.raises(TCINotFoundError):
        p.find_by_cas("")


def test_find_by_name(catalog_csv: Path):
    p = TCIProvider(product_map_csv=catalog_csv, enable_live_search=False, min_interval_s=0)
    hits = p.find_by_name("methanol")
    assert len(hits) == 2
    assert p.find_by_name("no-such-solvent") == []


def test_download_pdf_success_and_html_rejection(catalog_csv: Path):
    p = TCIProvider(
        product_map_csv=catalog_csv,
        enable_live_search=False,
        enable_document_search_fallback=False,
        min_interval_s=0,
    )
    pdf = b"%PDF-1.4 fake-tci-sds"
    p.http.request = MagicMock(return_value=(200, "https://example/sds.pdf", {"Content-Type": "application/pdf"}, pdf))
    data, url = p.download_pdf("https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf")
    assert data.startswith(b"%PDF")
    assert url.endswith("sds.pdf")

    p.http.request = MagicMock(
        return_value=(200, "https://example/sds.pdf", {"Content-Type": "text/html"}, b"<!DOCTYPE html><html>")
    )
    with pytest.raises(TCINoSDSError):
        p.download_pdf("https://www.tcichemicals.com/US/en/sds/A0054_US_EN.pdf")


def test_download_pdf_temporary_failure(catalog_csv: Path):
    p = TCIProvider(product_map_csv=catalog_csv, min_interval_s=0, enable_document_search_fallback=False)
    p.http.request = MagicMock(return_value=(503, "https://example", {}, b""))
    with pytest.raises(TCIDownloadError):
        p.download_pdf("https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf")


def test_download_sds_uses_cache(tmp_path: Path, catalog_csv: Path):
    cache = SDSCache(tmp_path / "SDS")
    p = TCIProvider(
        product_map_csv=catalog_csv,
        cache=cache,
        min_interval_s=0,
        enable_document_search_fallback=False,
    )
    pdf = b"%PDF-1.4 methanol-sds-bytes"
    p.http.request = MagicMock(
        return_value=(
            200,
            "https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf",
            {"Content-Type": "application/pdf"},
            pdf,
        )
    )
    doc1 = p.download_sds("M0097", cas="67-56-1")
    assert doc1.manufacturer == "TCI"
    assert doc1.product_number == "M0097"
    assert doc1.pdf_bytes == pdf
    assert doc1.sha256
    assert doc1.cached_path is not None
    assert doc1.reused is False

    # Second call must not hit the network
    p.http.request = MagicMock(side_effect=AssertionError("network should not be called"))
    doc2 = p.download_sds("M0097", cas="67-56-1")
    assert doc2.reused is True
    assert doc2.pdf_bytes == pdf


def test_parse_search_hits_filters_cas():
    html = """
    <div class="prductlist" data-id="M0097" data-casno="67-56-1"></div>
    <div class="prductlist" data-id="A0030" data-casno="141-78-6"></div>
    <a href="/US/en/p/D0798">DMSO</a>
    """
    hits = _parse_search_hits(html, query_cas="67-56-1")
    assert [h["product_number"] for h in hits] == ["M0097"]


def test_live_search_blocked_raises(catalog_csv: Path):
    p = TCIProvider(product_map_csv=catalog_csv, enable_live_search=True, min_interval_s=0)
    p.http.request = MagicMock(return_value=(403, "https://tci/search", {}, b"Access Denied"))
    with pytest.raises(TCIDownloadError):
        p._live_search("67-56-1")


def test_document_search_fallback_disabled_does_not_chain(catalog_csv: Path):
    """HTML/no-PDF at predictable URL must not POST documentSearch when fallback is off."""
    reset_tci_cooldown()
    p = TCIProvider(
        product_map_csv=catalog_csv,
        enable_live_search=False,
        enable_document_search_fallback=False,
        min_interval_s=0,
        allow_one_403_retry=False,
    )
    p.http.request = MagicMock(
        return_value=(200, "https://example/sds.pdf", {"Content-Type": "text/html"}, b"<!DOCTYPE html><html>")
    )
    with pytest.raises(TCINoSDSError):
        p.download_sds("A0030", cas="141-78-6", store_cache=False)
    # Only the predictable SDS GET — no documentSearch / CSRF chain.
    assert p.http.request.call_count == 1


def test_403_sets_cooldown_and_skips_further_http(catalog_csv: Path):
    reset_tci_cooldown()
    p = TCIProvider(
        product_map_csv=catalog_csv,
        enable_live_search=False,
        enable_document_search_fallback=False,
        min_interval_s=0,
        cooldown_s=600,
        allow_one_403_retry=False,
    )
    p.http.request = MagicMock(return_value=(403, "https://tci/sds", {}, b"Access Denied"))
    with pytest.raises(TCIBlockedError):
        p.download_pdf("https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf")
    assert get_tci_cooldown_remaining() > 0

    # Fresh client still sees process-wide cooldown and must not hit the network.
    p2 = TCIProvider(
        product_map_csv=catalog_csv,
        enable_document_search_fallback=False,
        min_interval_s=0,
        cooldown_s=600,
        allow_one_403_retry=False,
    )
    with pytest.raises(TCIBlockedError) as ei:
        p2.download_pdf("https://www.tcichemicals.com/US/en/sds/M0628_US_EN.pdf")
    assert "cooling down" in str(ei.value).lower() or "cooldown" in str(ei.value).lower()
    reset_tci_cooldown()


def test_403_does_not_chain_document_search_even_if_fallback_enabled(catalog_csv: Path):
    reset_tci_cooldown()
    p = TCIProvider(
        product_map_csv=catalog_csv,
        enable_document_search_fallback=True,
        min_interval_s=0,
        cooldown_s=600,
        allow_one_403_retry=False,
    )
    calls = []

    def fake_request(url, **kwargs):
        calls.append(url)
        return (403, url, {}, b"Denied")

    p.http.request = MagicMock(side_effect=fake_request)
    with pytest.raises(TCIBlockedError):
        p.download_sds("M0097", cas="67-56-1", store_cache=False)
    assert len(calls) == 1
    assert "documentSearch" not in calls[0]
    assert get_tci_cooldown_remaining() > 0
    reset_tci_cooldown()


def test_cache_used_even_during_cooldown(tmp_path: Path, catalog_csv: Path):
    reset_tci_cooldown()
    cache = SDSCache(tmp_path / "SDS")
    p = TCIProvider(
        product_map_csv=catalog_csv,
        cache=cache,
        min_interval_s=0,
        enable_document_search_fallback=False,
        allow_one_403_retry=False,
    )
    pdf = b"%PDF-1.4 cached-during-cooldown"
    p.http.request = MagicMock(
        return_value=(
            200,
            "https://www.tcichemicals.com/US/en/sds/M0097_US_EN.pdf",
            {"Content-Type": "application/pdf"},
            pdf,
        )
    )
    doc1 = p.download_sds("M0097", cas="67-56-1")
    assert doc1.reused is False

    set_tci_cooldown(600)
    p.http.request = MagicMock(side_effect=AssertionError("must use cache, not network"))
    doc2 = p.download_sds("M0097", cas="67-56-1")
    assert doc2.reused is True
    assert doc2.pdf_bytes == pdf
    reset_tci_cooldown()


def test_user_facing_sds_error_mentions_paste_and_no_spam():
    from v7.sds_acquisition import user_facing_sds_error

    msg = user_facing_sds_error(
        TCIBlockedError("TCI blocked the request (HTTP 403). Session cooldown started.")
    )
    low = msg.lower()
    assert "do not keep retrying" in low or "do not retry" in low
    assert "letter" in low and "4 digits" in low
    assert "paste" in low

