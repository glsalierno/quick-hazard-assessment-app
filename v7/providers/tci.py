"""
TCI America / Tokyo Chemical Industry SDS retrieval (HTTP only).

See ``GHaz7/docs/TCI_HTTP_WORKFLOW.md`` for the investigated endpoints,
robots/ToS posture, and limitations (Akamai, no public SDS API).

Workflow::

    CAS or name
        → curated product map (and optional live catalog search)
        → TCI product number (letter + 4 digits)
        → official SDS PDF URL ``/{region}/sds/{PRODUCT}_{COUNTRY}_{LANG}.pdf``
        → optional documentSearch POST fallback (off by default; angers Akamai)
        → SDSDocument / SDSDownload (parser stays vendor-agnostic)

Parsers must not import this module.

Gentleness toward Akamai (defaults):
- Shared min-interval throttle across all TCI HTTP (``TCI_MIN_INTERVAL_S``, default 5s).
- Session cooldown after HTTP 401/403 (``TCI_COOLDOWN_S``, default 12 min) — further
  TCI HTTP is skipped; use SDS cache or paste a product code.
- Do not chain alternate endpoints after a 403/401 in the same request.
- documentSearch fallback off unless explicitly enabled.
- Mainstream browser User-Agent + Accept headers; live search still off by default.
"""

from __future__ import annotations

import csv
import io
import re
import threading
import time
from dataclasses import dataclass
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

from utils.lookup_tables import normalize_cas_for_lookup
from v7.providers.base import SDSDocument, SDSDownload, SDSHit, SDSProvider
from v7.sds_cache import SDSCache, sha256_bytes

# Mainstream browser UA — custom research UAs are often denied by Akamai.
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_ACCEPT_HTML = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
_ACCEPT_PDF = "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

_PRODUCT_RE = re.compile(r"^[A-Za-z][0-9]{4}$")
_CAS_IN_TEXT_RE = re.compile(r"\b(\d{2,7}-\d{2}-\d)\b")
_REVISION_RE = re.compile(r"Revision Date:\s*([0-9/]+)", re.I)
_VERSION_RE = re.compile(r"Version\s+([0-9.]+)", re.I)
_PRODUCT_NAME_RE = re.compile(r"Product name\s*:\s*(.+)", re.I)

# Process-wide polite throttle + Akamai cooldown (shared by every TCIHttpClient).
_throttle_lock = threading.Lock()
_shared_last_request_at = 0.0
_cooldown_until = 0.0  # monotonic deadline
_cooldown_seconds_default = 720.0  # 12 minutes


class TCIError(RuntimeError):
    """Base error for TCI SDS acquisition."""


class TCINotFoundError(TCIError):
    """No TCI product mapping for the requested CAS/name."""


class TCINoSDSError(TCIError):
    """Product exists (or is mapped) but no SDS PDF is available."""


class TCIDownloadError(TCIError):
    """Transient or protocol failure while downloading."""


class TCIBlockedError(TCIDownloadError):
    """HTTP 401/403 or active cooldown — use cache / paste product code; do not retry spam."""


def get_tci_cooldown_remaining() -> float:
    """Seconds remaining on the process-wide TCI HTTP cooldown (0 if clear)."""
    with _throttle_lock:
        return max(0.0, _cooldown_until - time.monotonic())


def reset_tci_cooldown() -> None:
    """Clear cooldown (tests / explicit reset)."""
    global _cooldown_until
    with _throttle_lock:
        _cooldown_until = 0.0


def set_tci_cooldown(seconds: float) -> None:
    """Start or extend the process-wide cooldown from now."""
    global _cooldown_until
    seconds = max(0.0, float(seconds))
    with _throttle_lock:
        _cooldown_until = max(_cooldown_until, time.monotonic() + seconds)


def _raise_if_cooldown() -> None:
    rem = get_tci_cooldown_remaining()
    if rem <= 0:
        return
    mins = max(1, int((rem + 59) // 60))
    raise TCIBlockedError(
        f"TCI HTTP cooling down ~{mins} more min after HTTP 403/401 (Akamai). "
        "Do not retry spam. Prefer SDS cache, or paste a TCI product code "
        "(letter + 4 digits) once — the map will remember it."
    )


@dataclass(frozen=True)
class TCICatalogEntry:
    cas: str
    product_number: str
    name: str | None = None
    notes: str | None = None


class TCIPublicResolver:
    """CAS/name → TCI product number → official SDS URL. No browser automation."""

    def __init__(
        self,
        product_map_csv: Path | str | None = None,
        *,
        region: str = "US/en",
        country: str = "US",
        language: str = "EN",
    ) -> None:
        self.region = region.strip("/") or "US/en"
        self.country = (country or "US").upper()
        self.language = (language or "EN").upper()
        self.by_cas: dict[str, list[TCICatalogEntry]] = {}
        self.by_name: dict[str, list[TCICatalogEntry]] = {}
        self.by_product: dict[str, TCICatalogEntry] = {}
        path = Path(product_map_csv) if product_map_csv else None
        if path and path.is_file():
            self._load_csv(path)

    def _load_csv(self, path: Path) -> None:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                cas = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                prod = (
                    row.get("product_number")
                    or row.get("product")
                    or row.get("catalog_number")
                    or row.get("productNumber")
                    or ""
                ).strip().upper()
                if not prod or not _PRODUCT_RE.match(prod):
                    continue
                name = (row.get("name") or row.get("title") or row.get("product_name") or "").strip() or None
                notes = (row.get("notes") or "").strip() or None
                entry = TCICatalogEntry(cas=cas or "", product_number=prod, name=name, notes=notes)
                self.by_product[prod] = entry
                if cas:
                    self.by_cas.setdefault(cas, []).append(entry)
                if name:
                    self.by_name.setdefault(name.casefold(), []).append(entry)

    def normalize_product(self, product: str) -> str:
        prod = (product or "").strip().upper()
        if not _PRODUCT_RE.match(prod):
            raise ValueError(
                f"Invalid TCI product number {product!r}; expected letter + 4 digits (e.g. M0097)."
            )
        return prod

    def entries_for_cas(self, cas: str) -> list[TCICatalogEntry]:
        return list(self.by_cas.get(normalize_cas_for_lookup(cas), ()))

    def entries_for_name(self, name: str) -> list[TCICatalogEntry]:
        key = (name or "").strip().casefold()
        if not key:
            return []
        exact = list(self.by_name.get(key, ()))
        if exact:
            return exact
        # substring match for convenience (e.g. "methanol")
        out: list[TCICatalogEntry] = []
        seen: set[str] = set()
        for nkey, entries in self.by_name.items():
            if key in nkey or nkey in key:
                for e in entries:
                    if e.product_number not in seen:
                        seen.add(e.product_number)
                        out.append(e)
        return out

    def sds_url_for_product(self, product: str) -> str:
        prod = self.normalize_product(product)
        filename = f"{prod}_{self.country}_{self.language}.pdf"
        return f"https://www.tcichemicals.com/{self.region}/sds/{quote(filename, safe='._-')}"

    def product_page_url(self, product: str) -> str:
        prod = self.normalize_product(product)
        return f"https://www.tcichemicals.com/{self.region}/p/{quote(prod, safe='')}"

    def document_search_url(self) -> str:
        return f"https://www.tcichemicals.com/{self.region}/documentSearch"

    def product_sds_search_url(self) -> str:
        return f"https://www.tcichemicals.com/{self.region}/documentSearch/productSDSSearchDoc"

    def catalog_search_url(self, query: str) -> str:
        return (
            f"https://www.tcichemicals.com/{self.region}/search/"
            f"?text={quote(query)}&resulttype=product"
        )


class TCIHttpClient:
    """Session-aware HTTP client with process-wide rate limiting + cooldown."""

    def __init__(
        self,
        *,
        timeout_s: float = 30.0,
        min_interval_s: float = 5.0,
        user_agent: str = _USER_AGENT,
        cooldown_s: float = _cooldown_seconds_default,
    ) -> None:
        self.timeout_s = timeout_s
        self.min_interval_s = max(0.0, min_interval_s)
        self.user_agent = user_agent
        self.cooldown_s = max(0.0, float(cooldown_s))
        self._jar = CookieJar()
        self._opener = build_opener(HTTPCookieProcessor(self._jar))

    def _throttle(self) -> None:
        global _shared_last_request_at
        if self.min_interval_s <= 0:
            with _throttle_lock:
                _shared_last_request_at = time.monotonic()
            return
        with _throttle_lock:
            elapsed = time.monotonic() - _shared_last_request_at
            wait = self.min_interval_s - elapsed
        if wait > 0:
            time.sleep(wait)
        with _throttle_lock:
            _shared_last_request_at = time.monotonic()

    def mark_blocked(self) -> None:
        """Record Akamai-style block and start session cooldown."""
        if self.cooldown_s > 0:
            set_tci_cooldown(self.cooldown_s)

    def request(
        self,
        url: str,
        *,
        data: bytes | None = None,
        accept: str = "*/*",
        referer: str | None = None,
        extra_headers: dict[str, str] | None = None,
        skip_cooldown_check: bool = False,
    ) -> tuple[int, str, dict[str, str], bytes]:
        if not skip_cooldown_check:
            _raise_if_cooldown()
        self._throttle()
        headers = {
            "User-Agent": self.user_agent,
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9",
        }
        if referer:
            headers["Referer"] = referer
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            headers["Origin"] = "https://www.tcichemicals.com"
        if extra_headers:
            headers.update(extra_headers)
        req = Request(url, data=data, headers=headers)
        try:
            with self._opener.open(req, timeout=self.timeout_s) as resp:
                body = resp.read()
                hdrs = {k: v for k, v in resp.headers.items()}
                status = getattr(resp, "status", 200) or 200
                return status, resp.geturl(), hdrs, body
        except HTTPError as exc:
            body = exc.read() if exc.fp else b""
            hdrs = {k: v for k, v in (exc.headers.items() if exc.headers else [])}
            return int(exc.code), url, hdrs, body
        except (URLError, TimeoutError) as exc:
            raise TCIDownloadError(f"TCI request failed for {url}: {exc}") from exc


def _header(headers: dict[str, str], name: str) -> str:
    want = name.lower()
    for k, v in headers.items():
        if k.lower() == want:
            return v or ""
    return ""


def _pdf_meta(pdf_bytes: bytes) -> dict[str, Any]:
    """Best-effort revision / name / CAS from the first pages (optional pypdf)."""
    out: dict[str, Any] = {}
    try:
        from pypdf import PdfReader
    except ImportError:
        return out
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = "\n".join((page.extract_text() or "") for page in reader.pages[:2])
    except Exception:
        return out
    cas_hits = _CAS_IN_TEXT_RE.findall(text)
    if cas_hits:
        out["cas_in_pdf"] = cas_hits[0]
        out["cas_all"] = cas_hits
    m = _REVISION_RE.search(text)
    if m:
        out["revision"] = m.group(1)
    m = _VERSION_RE.search(text)
    if m:
        out["version"] = m.group(1).rstrip(".")
    m = _PRODUCT_NAME_RE.search(text)
    if m:
        out["product_name"] = m.group(1).strip()
    return out


def _parse_search_hits(html: str, *, query_cas: str | None = None) -> list[dict[str, str]]:
    """Extract product cards from TCI Hybris search HTML (when reachable)."""
    hits: list[dict[str, str]] = []
    # Primary: data-id / data-casno on product list cards (either attribute order)
    pattern = (
        r'data-id=["\']([A-Za-z][0-9]{4})["\'][^>]*data-casno=["\']([^"\']*)["\']'
        r'|data-casno=["\']([^"\']*)["\'][^>]*data-id=["\']([A-Za-z][0-9]{4})["\']'
    )
    for m in re.finditer(pattern, html, re.I):
        if m.group(1):
            prod, cas = m.group(1).upper(), (m.group(2) or "").strip()
        else:
            prod, cas = m.group(4).upper(), (m.group(3) or "").strip()
        hits.append({"product_number": prod, "cas": cas})

    if not hits:
        for prod in re.findall(r"/p/([A-Za-z][0-9]{4})\b", html):
            hits.append({"product_number": prod.upper(), "cas": ""})

    # Deduplicate preserving order
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for h in hits:
        if h["product_number"] in seen:
            continue
        seen.add(h["product_number"])
        if query_cas and h.get("cas") and normalize_cas_for_lookup(h["cas"]) != normalize_cas_for_lookup(
            query_cas
        ):
            continue
        out.append(h)
    return out


class TCIProvider(SDSProvider):
    """HTTP-only TCI SDS provider implementing the shared ``SDSProvider`` interface."""

    name = "tci"

    def __init__(
        self,
        *,
        product_map_csv: Path | str | None = None,
        region: str = "US/en",
        country: str = "US",
        language: str = "EN",
        timeout_s: float = 30.0,
        min_interval_s: float = 5.0,
        enable_live_search: bool = False,
        enable_document_search_fallback: bool = False,
        cache: SDSCache | None = None,
        user_agent: str = _USER_AGENT,
        cooldown_s: float = _cooldown_seconds_default,
        retry_sleep_s: float = 8.0,
        allow_one_403_retry: bool = True,
    ) -> None:
        self.resolver = TCIPublicResolver(
            product_map_csv, region=region, country=country, language=language
        )
        self.http = TCIHttpClient(
            timeout_s=timeout_s,
            min_interval_s=min_interval_s,
            user_agent=user_agent,
            cooldown_s=cooldown_s,
        )
        self.enable_live_search = enable_live_search
        self.enable_document_search_fallback = enable_document_search_fallback
        self.cache = cache
        self.retry_sleep_s = max(0.0, float(retry_sleep_s))
        self.allow_one_403_retry = bool(allow_one_403_retry)
        self._csrf_token: str | None = None
        self._encoded_context_path: str | None = None

    # --- SDSProvider ABC -------------------------------------------------

    def find(self, cas: str) -> list[SDSHit]:
        return self.find_by_cas(cas)

    def download(self, hit: SDSHit) -> SDSDownload:
        doc = self.download_sds(hit.product_id or "", cas=hit.cas, hit=hit)
        return doc.to_download()

    # --- Convenience API -------------------------------------------------

    def find_by_cas(self, cas: str) -> list[SDSHit]:
        cas_key = normalize_cas_for_lookup(cas)
        if not cas_key:
            raise TCINotFoundError("Missing or invalid CAS number.")

        entries = self.resolver.entries_for_cas(cas_key)
        via = "product_map"
        if not entries and self.enable_live_search:
            if get_tci_cooldown_remaining() > 0:
                # Map miss + cooldown: skip live HTTP rather than thrashing Akamai.
                return []
            live = self._live_search(cas.strip())
            entries = [
                TCICatalogEntry(cas=cas_key, product_number=h["product_number"], name=None)
                for h in live
            ]
            via = "live_search"

        if not entries:
            return []

        display_cas = cas.strip() or cas_key
        return [self._hit_from_entry(entry, cas=display_cas, via=via) for entry in entries]

    def find_by_name(self, name: str) -> list[SDSHit]:
        query = (name or "").strip()
        if not query:
            raise TCINotFoundError("Missing chemical name.")

        entries = self.resolver.entries_for_name(query)
        via = "product_map"
        if not entries and self.enable_live_search:
            if get_tci_cooldown_remaining() > 0:
                return []
            live = self._live_search(query)
            entries = [
                TCICatalogEntry(
                    cas=normalize_cas_for_lookup(h.get("cas") or ""),
                    product_number=h["product_number"],
                    name=query,
                )
                for h in live
            ]
            via = "live_search"

        if not entries:
            return []

        return [self._hit_from_entry(e, cas=e.cas or "", via=via) for e in entries]

    def download_sds(
        self,
        product: str,
        *,
        cas: str = "",
        hit: SDSHit | None = None,
        store_cache: bool = True,
    ) -> SDSDocument:
        prod = self.resolver.normalize_product(product)

        # Prefer SDS cache aggressively before any network call.
        if self.cache is not None:
            cached = self.cache.locate_by_product("TCI", prod)
            if cached is not None:
                pdf = cached.pdf_path.read_bytes()
                rev = None if cached.revision in {"", "unknown-rev"} else cached.revision
                if hit is None:
                    base_hit = SDSHit(
                        cas=cas or cached.cas,
                        provider=self.name,
                        manufacturer="TCI",
                        product_id=prod,
                        title=None,
                        sds_url=self.resolver.sds_url_for_product(prod),
                        revision=rev,
                        extra={"via": "sds_cache"},
                    )
                else:
                    base_hit = SDSHit(
                        cas=hit.cas or cached.cas,
                        provider=hit.provider,
                        manufacturer=hit.manufacturer,
                        product_id=hit.product_id or prod,
                        title=hit.title,
                        sds_url=hit.sds_url or self.resolver.sds_url_for_product(prod),
                        revision=hit.revision or rev,
                        language=hit.language,
                        extra={**dict(hit.extra), "via": "sds_cache"},
                    )
                download = SDSDownload(
                    hit=base_hit,
                    pdf_bytes=pdf,
                    sha256=cached.sha256 or sha256_bytes(pdf),
                    cached_path=cached.pdf_path,
                    reused=True,
                )
                return SDSDocument.from_download(download)

        url = self.resolver.sds_url_for_product(prod)
        try:
            pdf_bytes, final_url = self.download_pdf(url, allow_one_retry=True)
        except TCIBlockedError:
            # 401/403: do NOT chain documentSearch / alternate endpoints.
            raise
        except TCINoSDSError:
            if not self.enable_document_search_fallback:
                raise
            if get_tci_cooldown_remaining() > 0:
                raise
            pdf_bytes, final_url = self._download_via_document_search(prod)

        meta = _pdf_meta(pdf_bytes)
        revision = meta.get("revision") or (hit.revision if hit else None)
        title = meta.get("product_name") or (hit.title if hit else None)
        pdf_cas = meta.get("cas_in_pdf") or ""
        if cas and pdf_cas and normalize_cas_for_lookup(cas) != normalize_cas_for_lookup(pdf_cas):
            # Informative mismatch — still return the PDF for the requested product.
            mismatch = {
                "cas_requested": cas,
                "cas_in_pdf": pdf_cas,
                "warning": "CAS in SDS PDF does not match requested CAS; verify product number.",
            }
        else:
            mismatch = {}

        base_hit = hit or SDSHit(
            cas=cas or pdf_cas or "",
            provider=self.name,
            manufacturer="TCI",
            product_id=prod,
            title=title,
            sds_url=final_url,
            revision=revision,
            extra={"via": "tci_sds_url", **mismatch, **{k: v for k, v in meta.items() if k != "cas_all"}},
        )
        if hit is not None and (revision or title or mismatch):
            extra = dict(hit.extra)
            extra.update(mismatch)
            extra.update({k: v for k, v in meta.items() if k != "cas_all"})
            base_hit = SDSHit(
                cas=hit.cas,
                provider=hit.provider,
                manufacturer=hit.manufacturer,
                product_id=hit.product_id or prod,
                title=title or hit.title,
                sds_url=final_url or hit.sds_url,
                revision=revision or hit.revision,
                language=hit.language,
                extra=extra,
            )

        digest = sha256_bytes(pdf_bytes)
        download = SDSDownload(hit=base_hit, pdf_bytes=pdf_bytes, sha256=digest)
        if store_cache and self.cache is not None:
            stored = self.cache.store(download)
            download = SDSDownload(
                hit=base_hit,
                pdf_bytes=pdf_bytes,
                sha256=digest,
                cached_path=stored.pdf_path,
                reused=stored.reused_pdf,
            )
        return SDSDocument.from_download(download)

    def download_pdf(self, url: str, *, allow_one_retry: bool = False) -> tuple[bytes, str]:
        if not url or not url.lower().startswith("http"):
            raise TCIDownloadError(f"Invalid SDS URL: {url!r}")
        status, final_url, headers, body = self.http.request(url, accept=_ACCEPT_PDF)
        if status in {429, 500, 502, 503, 504}:
            raise TCIDownloadError(
                f"Temporary TCI failure HTTP {status} for {url}. Retry later."
            )
        if status in {401, 403}:
            # Optional: one longer sleep + single retry on predictable SDS URL only.
            if (
                allow_one_retry
                and self.allow_one_403_retry
                and self.retry_sleep_s > 0
                and get_tci_cooldown_remaining() <= 0
            ):
                time.sleep(self.retry_sleep_s)
                status, final_url, headers, body = self.http.request(
                    url, accept=_ACCEPT_PDF
                )
            if status in {401, 403}:
                self.http.mark_blocked()
                raise TCIBlockedError(
                    f"TCI blocked the request (HTTP {status}) for {url}. "
                    "Session cooldown started. Prefer SDS cache, or paste a TCI "
                    "product code (letter + 4 digits); do not retry spam."
                )
            # Non-block response after optional retry — fall through to PDF checks.
        if status == 404 or not body:
            raise TCINoSDSError(f"No SDS PDF at {url} (HTTP {status}).")
        if status >= 400:
            raise TCIDownloadError(f"TCI SDS download failed HTTP {status} for {url}.")

        ctype = _header(headers, "Content-Type").lower()
        if body.startswith(b"%PDF"):
            return body, final_url
        if "pdf" in ctype and b"%PDF" in body[:1024]:
            idx = body.find(b"%PDF")
            return body[idx:], final_url
        if "html" in ctype or body.lstrip().startswith(b"<!DOCTYPE") or body.lstrip().startswith(b"<html"):
            raise TCINoSDSError(
                f"TCI returned HTML instead of a PDF for {url}. "
                "The product may lack a published SDS at the predictable URL; "
                "confirm the product number on "
                "https://www.tcichemicals.com/US/en/documentSearch "
                "(documentSearch HTTP fallback is off by default to avoid Akamai)."
            )
        raise TCIDownloadError(
            f"Unexpected content type {ctype!r} from {url} ({len(body)} bytes)."
        )

    # --- Internals -------------------------------------------------------

    def _hit_from_entry(self, entry: TCICatalogEntry, *, cas: str, via: str) -> SDSHit:
        return SDSHit(
            cas=cas or entry.cas,
            provider=self.name,
            manufacturer="TCI",
            product_id=entry.product_number,
            title=entry.name,
            sds_url=self.resolver.sds_url_for_product(entry.product_number),
            revision=None,
            extra={
                "via": via,
                "product_page": self.resolver.product_page_url(entry.product_number),
                "notes": entry.notes,
            },
        )

    def _live_search(self, query: str) -> list[dict[str, str]]:
        url = self.resolver.catalog_search_url(query)
        status, final_url, headers, body = self.http.request(url, accept=_ACCEPT_HTML)
        if status in {401, 403}:
            self.http.mark_blocked()
            raise TCIBlockedError(
                f"TCI catalog search blocked (HTTP {status}) for {query!r}. "
                "Cooldown started. Use the product map / paste product code; "
                "do not enable live search spam."
            )
        if status >= 400:
            raise TCIDownloadError(f"TCI catalog search failed HTTP {status} for {query!r}.")
        html = body.decode("utf-8", "replace")
        self._capture_session_tokens(html)
        cas_query = query if re.match(r"^\d{2,7}-\d{2}-\d$", query.strip()) else None
        return _parse_search_hits(html, query_cas=cas_query)

    def _capture_session_tokens(self, html: str) -> None:
        csrf = re.search(
            r'name=["\']CSRFToken["\'][^>]*value=["\']([^"\']+)', html
        ) or re.search(r'value=["\']([^"\']+)["\'][^>]*name=["\']CSRFToken', html)
        if csrf:
            self._csrf_token = csrf.group(1)
        ctx = re.search(r"encodedContextPath[^;]+?'(/[^']+)'", html)
        if ctx:
            self._encoded_context_path = ctx.group(1).replace("\\", "")

    def _ensure_csrf(self) -> str:
        if self._csrf_token:
            return self._csrf_token
        status, _, _, body = self.http.request(
            self.resolver.document_search_url(), accept=_ACCEPT_HTML
        )
        if status in {401, 403}:
            self.http.mark_blocked()
            raise TCIBlockedError(
                f"Cannot open TCI documentSearch (HTTP {status}); cooldown started. "
                "Do not chain alternate TCI endpoints after a block."
            )
        if status >= 400:
            raise TCIDownloadError(
                f"Cannot open TCI documentSearch (HTTP {status}); "
                "CSRF token unavailable for SDS POST fallback."
            )
        html = body.decode("utf-8", "replace")
        self._capture_session_tokens(html)
        if not self._csrf_token:
            raise TCIDownloadError(
                "TCI documentSearch HTML did not include a CSRFToken "
                "(page may be JS-rendered or blocked)."
            )
        return self._csrf_token

    def _download_via_document_search(self, product: str) -> tuple[bytes, str]:
        """POST productSDSSearchDoc then GET ``/{ctx}/sds/{filename}``.

        Only used when ``enable_document_search_fallback`` is explicitly True.
        Never call this after a 401/403 on the predictable SDS URL.
        """
        prod = self.resolver.normalize_product(product)
        csrf = self._ensure_csrf()
        form = urlencode(
            {
                "productCode": prod,
                "langSelector": "en",
                "selectedCountry": self.resolver.country,
                "CSRFToken": csrf,
            }
        ).encode()
        status, _, headers, body = self.http.request(
            self.resolver.product_sds_search_url(),
            data=form,
            accept=_ACCEPT_PDF,
            referer=self.resolver.document_search_url(),
        )
        if status in {429, 500, 502, 503, 504}:
            raise TCIDownloadError(
                f"Temporary TCI documentSearch failure HTTP {status} for {prod}."
            )
        if status in {401, 403}:
            self.http.mark_blocked()
            raise TCIBlockedError(
                f"TCI documentSearch blocked (HTTP {status}) for {prod}. "
                "Cooldown started; prefer cache / paste product code."
            )
        if body.startswith(b"%PDF"):
            return body, self.resolver.product_sds_search_url()

        cdisp = _header(headers, "Content-Disposition")
        m = re.search(r'filename[*]?=(?:UTF-8\'\')?["\']?([^";]+)', cdisp, re.I)
        if not m:
            raise TCINoSDSError(
                f"No SDS available via documentSearch for product {prod} "
                f"(HTTP {status}, no Content-Disposition filename)."
            )
        filename = m.group(1).strip().strip('"')
        ctx = self._encoded_context_path or f"/{self.resolver.region}"
        pdf_url = f"https://www.tcichemicals.com{ctx}/sds/{filename}"
        return self.download_pdf(pdf_url, allow_one_retry=False)
