"""
Vendor SDS acquisition helpers for Curated Knowledge / Compile.

Providers return ``SDSDocument`` / ``SDSDownload`` only. Parsers never import
Sigma or TCI modules — call ``structured_from_pdf`` with raw bytes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import config
from v7.providers.base import SDSDocument, SDSDownload, SDSHit
from v7.providers.sigma import SigmaProvider
from v7.providers.tci import TCIBlockedError, TCIError, TCINotFoundError, TCIProvider
from v7.sds_cache import SDSCache
from v7.tci_catalog import ensure_runtime_catalog

SDSVendor = Literal["tci", "sigma"]


def sds_cache() -> SDSCache:
    return SDSCache(getattr(config, "V7_SDS_CACHE_DIR", Path("cache") / "SDS"))


def build_tci_provider(
    *,
    cache: SDSCache | None = None,
    enable_live_search: bool | None = None,
) -> TCIProvider:
    """Build TCIProvider with seed∪learned runtime catalog."""
    map_path = ensure_runtime_catalog()
    live = (
        bool(enable_live_search)
        if enable_live_search is not None
        else bool(getattr(config, "TCI_ENABLE_LIVE_SEARCH", False))
    )
    return TCIProvider(
        product_map_csv=map_path,
        region=getattr(config, "TCI_SDS_REGION", "US/en"),
        country=getattr(config, "TCI_SDS_COUNTRY", "US"),
        language=getattr(config, "TCI_SDS_LANGUAGE", "EN"),
        timeout_s=float(getattr(config, "TCI_HTTP_TIMEOUT_S", 30)),
        min_interval_s=float(getattr(config, "TCI_MIN_INTERVAL_S", 5.0)),
        enable_live_search=live,
        enable_document_search_fallback=bool(
            getattr(config, "TCI_ENABLE_DOCUMENT_SEARCH_FALLBACK", False)
        ),
        cache=cache if cache is not None else sds_cache(),
        cooldown_s=float(getattr(config, "TCI_COOLDOWN_S", 720)),
        retry_sleep_s=float(getattr(config, "TCI_403_RETRY_SLEEP_S", 8)),
    )


def build_sigma_provider(*, cache: SDSCache | None = None) -> SigmaProvider:
    return SigmaProvider(
        api_base=getattr(config, "SIGMA_API_BASE", "https://api.sigmaaldrich.com/v1"),
        api_key=getattr(config, "SIGMA_API_KEY", None),
        product_map_csv=getattr(config, "SIGMA_PRODUCT_MAP_CSV", None),
        region=getattr(config, "SIGMA_SDS_REGION", "US/en"),
        timeout_s=float(getattr(config, "SIGMA_HTTP_TIMEOUT_S", 30)),
    )


def find_vendor_hits(
    cas: str,
    vendor: SDSVendor,
    *,
    enable_live_search: bool | None = None,
) -> list[SDSHit]:
    cas = (cas or "").strip()
    if not cas:
        return []
    if vendor == "tci":
        return build_tci_provider(enable_live_search=enable_live_search).find(cas)
    if vendor == "sigma":
        return build_sigma_provider().find(cas)
    raise ValueError(f"Unknown SDS vendor: {vendor!r}")


def download_vendor_hit(hit: SDSHit) -> SDSDocument:
    """Download (or cache-reuse) a hit; always return vendor-agnostic SDSDocument."""
    if hit.provider == "tci":
        provider = build_tci_provider(enable_live_search=False)
        return provider.download_sds(hit.product_id or "", cas=hit.cas, hit=hit)
    if hit.provider == "sigma":
        provider = build_sigma_provider()
        dl = provider.download(hit)
        cache = sds_cache()
        stored = cache.store(dl)
        return SDSDocument.from_download(
            SDSDownload(
                hit=dl.hit,
                pdf_bytes=dl.pdf_bytes,
                sha256=dl.sha256,
                cached_path=stored.pdf_path,
                reused=stored.reused_pdf,
            )
        )
    raise ValueError(f"Unsupported SDS provider on hit: {hit.provider!r}")


def structured_from_pdf(pdf_bytes: bytes) -> dict[str, Any] | None:
    """Parse PDF bytes with the existing SDS engine; return v7 structured dict."""
    if not pdf_bytes:
        return None
    try:
        from utils.sds_parser import get_sds_parser
    except Exception:
        return None
    parser = get_sds_parser()
    result = parser.parse_pdf(pdf_bytes)
    if result is None:
        return None
    legacy = getattr(result, "legacy", None) or {}
    structured = legacy.get("v7_structured")
    return structured if isinstance(structured, dict) else None


def provenance_from_document(doc: SDSDocument) -> dict[str, Any]:
    return {
        "provider": doc.provider or doc.manufacturer,
        "manufacturer": doc.manufacturer,
        "product_number": doc.product_number,
        "title": doc.title,
        "revision": doc.revision,
        "source_url": doc.source_url,
        "sha256": doc.sha256,
        "reused_cache": doc.reused,
        "cached_path": str(doc.cached_path) if doc.cached_path else None,
        "cas": doc.cas,
        "extra": dict(doc.extra or {}),
    }


def user_facing_sds_error(exc: BaseException) -> str:
    """Map provider exceptions to short Streamlit-friendly messages."""
    if isinstance(exc, TCINotFoundError):
        return str(exc)
    if isinstance(exc, (TCIBlockedError, TCIError)):
        msg = str(exc)
        blocked = isinstance(exc, TCIBlockedError) or (
            "403" in msg
            or "401" in msg
            or "blocked" in msg.lower()
            or "cooling down" in msg.lower()
            or "cooldown" in msg.lower()
        )
        if blocked:
            return (
                f"{msg}\n\n"
                "**Do not keep retrying** — repeated requests make Akamai stricter and "
                "start a session cooldown (further TCI HTTP is skipped).\n\n"
                "What works now:\n"
                "1. Use a cached SDS if you already fetched this product.\n"
                "2. Open TCI in your browser, copy the product code "
                "(**letter + 4 digits**, e.g. `A0054`), and paste it once here — "
                "the map will remember it for that CAS.\n"
                "3. Wait for the cooldown to expire before any new TCI download."
            )
        return msg
    return str(exc)
