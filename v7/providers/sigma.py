"""
Sigma-Aldrich / MilliporeSigma SDS retrieval.

Assumptions (see docs/SIGMA_API_ASSUMPTIONS.md):

* There is **no verified public developer SDS API** from MilliporeSigma for
  unauthenticated research use. Community OpenAPI specs claim
  ``https://api.sigmaaldrich.com/v1`` with ``x-api-key``; that host is only
  attempted when ``SIGMA_API_KEY`` is set.
* The replaceable resolver maps CAS → catalog number (CSV or DoSS SDS URL)
  then uses official public SDS URL templates on sigmaaldrich.com.
* Parsers must not import this module.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from utils.lookup_tables import normalize_cas_for_lookup
from v7.providers.base import SDSDownload, SDSHit, SDSProvider
from v7.sds_cache import sha256_bytes

# Official public SDS page pattern (region configurable, default US/en).
_OFFICIAL_SDS_TMPL = "https://www.sigmaaldrich.com/{region}/sds/{product}"
_USER_AGENT = "GHaz7-SigmaProvider/1.0 (academic research; +https://www.sigmaaldrich.com/US/en/support/sds-finder)"


class SigmaApiClient:
    """Optional REST client. Isolated so it can be swapped without touching parsers."""

    def __init__(self, base: str, api_key: str, timeout_s: float = 30.0) -> None:
        self.base = base.rstrip("/")
        self.api_key = api_key
        self.timeout_s = timeout_s

    def _get_json(self, path: str) -> dict | None:
        url = f"{self.base}{path}"
        req = Request(url, headers={"User-Agent": _USER_AGENT, "x-api-key": self.api_key, "Accept": "application/json"})
        try:
            with urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read()
        except (HTTPError, URLError, TimeoutError, ValueError):
            return None
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def products_by_cas(self, cas: str) -> list[dict]:
        payload = self._get_json(f"/products/cas/{quote(cas)}")
        if not payload:
            return []
        items = payload.get("products") or payload.get("items") or payload.get("data") or []
        return items if isinstance(items, list) else []

    def sds_for_catalog(self, catalog_number: str, language: str = "en-US") -> dict | None:
        return self._get_json(
            f"/products/{quote(catalog_number)}/sds?language={quote(language)}&format=json"
        )


class SigmaPublicResolver:
    """CAS → catalog number → official SDS URL. No HTML scraping of search results."""

    def __init__(self, product_map_csv: Path | str | None, region: str = "US/en") -> None:
        self.region = region.strip("/") or "US/en"
        self.cas_to_product: dict[str, str] = {}
        path = Path(product_map_csv) if product_map_csv else None
        if path and path.is_file():
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    cas = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                    prod = (row.get("catalog_number") or row.get("product") or row.get("productNumber") or "").strip()
                    if cas and prod:
                        self.cas_to_product[cas] = prod

    def catalog_for_cas(self, cas: str) -> str | None:
        return self.cas_to_product.get(normalize_cas_for_lookup(cas))

    def sds_url_for_product(self, product: str) -> str:
        return _OFFICIAL_SDS_TMPL.format(region=self.region, product=quote(product, safe=""))

    def catalog_from_doss_url(self, url: str) -> str | None:
        """Parse catalog numbers from historical Sigma MSDS query URLs stored in DoSS."""
        if not url:
            return None
        m = re.search(r"productNumber=([^&]+)", url, re.I)
        if m:
            return m.group(1).strip()
        m = re.search(r"/sds/([^/?#]+)", url, re.I)
        if m:
            return m.group(1).strip()
        return None


class SigmaProvider(SDSProvider):
    name = "sigma"

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str | None,
        product_map_csv: Path | str | None = None,
        region: str = "US/en",
        timeout_s: float = 30.0,
        doss_sds_urls: dict[str, str] | None = None,
    ) -> None:
        self.timeout_s = timeout_s
        self.api = SigmaApiClient(api_base, api_key, timeout_s) if api_key else None
        self.resolver = SigmaPublicResolver(product_map_csv, region=region)
        self.doss_sds_urls = doss_sds_urls or {}

    def find(self, cas: str) -> list[SDSHit]:
        hits: list[SDSHit] = []
        if self.api:
            for prod in self.api.products_by_cas(cas):
                catalog = str(prod.get("catalogNumber") or prod.get("productNumber") or "").strip()
                if not catalog:
                    continue
                sds_meta = self.api.sds_for_catalog(catalog) or {}
                url = sds_meta.get("pdfUrl") or self.resolver.sds_url_for_product(catalog)
                hits.append(
                    SDSHit(
                        cas=cas,
                        provider=self.name,
                        manufacturer="MilliporeSigma",
                        product_id=catalog,
                        title=str(prod.get("name") or catalog),
                        sds_url=str(url) if url else None,
                        revision=str(sds_meta.get("revisionDate") or sds_meta.get("version") or "") or None,
                        extra={"via": "sigma_api", "sds_meta": sds_meta},
                    )
                )
            if hits:
                return hits

        catalog = self.resolver.catalog_for_cas(cas)
        if not catalog:
            doss_url = self.doss_sds_urls.get(normalize_cas_for_lookup(cas))
            catalog = self.resolver.catalog_from_doss_url(doss_url or "") if doss_url else None
        if not catalog:
            return []
        return [
            SDSHit(
                cas=cas,
                provider=self.name,
                manufacturer="MilliporeSigma",
                product_id=catalog,
                title=catalog,
                sds_url=self.resolver.sds_url_for_product(catalog),
                revision=None,
                extra={"via": "sigma_public_resolver"},
            )
        ]

    def download(self, hit: SDSHit) -> SDSDownload:
        if not hit.sds_url:
            raise ValueError("Sigma hit has no SDS URL")
        req = Request(hit.sds_url, headers={"User-Agent": _USER_AGENT, "Accept": "application/pdf,*/*"})
        try:
            with urlopen(req, timeout=self.timeout_s) as resp:
                data = resp.read()
                ctype = (resp.headers.get("Content-Type") or "").lower()
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"Sigma SDS download failed: {exc}") from exc
        if not data:
            raise RuntimeError("Sigma SDS download was empty")
        if "html" in ctype and not data[:8].startswith(b"%PDF"):
            raise RuntimeError(
                "Sigma returned HTML instead of a PDF. Set SIGMA_API_KEY for the "
                "documented REST client, or add SIGMA_PRODUCT_MAP_CSV with a catalog number."
            )
        return SDSDownload(hit=hit, pdf_bytes=data, sha256=sha256_bytes(data))
