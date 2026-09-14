"""IUCLID 6 Public REST API ingestion (ECHA) — replaces deprecated static ECHA CSV URLs.

Discovery notes (IUCLID 6 Public REST API documentation, e.g. v6.2 / v9.x PDFs on
https://iuclid6.echa.europa.eu/public-api):

- The **documented** public API base path is typically::

    /iuclid6-ext/api/ext/v1/

  **not** ``/rest/v1/``. The latter is kept here as an *optional* ``IUCLID_API_STYLE=rest``
  mode because some prompts/docs refer to it; if ECHA exposes that surface, the same
  client code can target it via ``IUCLID_API_BASE_URL``.

- **Unsecured inventory listing** (no API key) is documented as::

    GET /inventory

  with pagination parameters ``l`` (limit), ``o`` (offset), optional ``code`` (inventory
  code, e.g. ``EC``), ``cas``, ``number``, ``name``, etc.

- **Paged search** for documents (including substances with embedded representations) uses::

    GET /query/iuclid6/{query-name}

  e.g. ``byType`` with ``doc.type=SUBSTANCE`` and ``formatter=iuclid6.DocumentSecuredRepresentation``,
  or ``byGhs`` for GHS-related hits.

- **Raw substance document** (may require authentication on some IUCLID deployments; the
  PDF examples often show ``Authorization: Token ...`` for ``/raw/SUBSTANCE``)::

    GET /raw/SUBSTANCE/{substance-uuid}?formatter=iuclid6.DocumentSecuredRepresentation

**Disclaimer / terms:** Some ECHA hosts return HTML interstitials (cookie / legal notice)
before JSON API access. This module performs a lightweight ``GET`` on the API root,
follows redirects, and optionally POSTs user-configured acceptance URLs via environment
variables (see ``init_iuclid_client``). Inspect your browser's Network tab once and set
``IUCLID_ACCEPT_POST_URL`` / ``IUCLID_ACCEPT_POST_BODY`` if needed.

**Azure WAF / JS challenge:** ECHA may serve an HTML interstitial (e.g. Azure WAF JavaScript
challenge) to plain ``requests`` clients. When ``IUCLID_USE_CLOUDSCRAPER`` is true (default),
``create_session()`` uses ``cloudscraper`` (TLS + browser fingerprint) which often succeeds
where bare ``requests`` gets ``403`` + HTML. Set ``IUCLID_USE_CLOUDSCRAPER=false`` to force
standard ``requests`` (e.g. behind a trusted allowlisted egress).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

# --- Environment-driven configuration (see .env.example) ---

IUCLID_API_BASE_URL = os.getenv(
    "IUCLID_API_BASE_URL",
    "https://iuclid6.echa.europa.eu/iuclid6-ext/api/ext/v1/",
).rstrip("/") + "/"

IUCLID_API_STYLE = os.getenv("IUCLID_API_STYLE", "ext").strip().lower()

IUCLID_PAGE_SIZE = int(os.getenv("IUCLID_PAGE_SIZE", "100"))
IUCLID_CACHE_DIR = os.getenv("IUCLID_CACHE_DIR", "data/iuclid_cache")
IUCLID_RAW_DIR = os.getenv("IUCLID_RAW_DIR", "data/iuclid_raw")
IUCLID_MAX_RETRIES = int(os.getenv("IUCLID_MAX_RETRIES", "3"))
IUCLID_REQUEST_DELAY_MS = int(os.getenv("IUCLID_REQUEST_DELAY_MS", "150"))
IUCLID_INVENTORY_CODE = os.getenv("IUCLID_INVENTORY_CODE", "EC")
IUCLID_CLP_STRATEGY = os.getenv("IUCLID_CLP_STRATEGY", "by_ghs").strip().lower()
IUCLID_HTTP_TIMEOUT_S = int(os.getenv("IUCLID_HTTP_TIMEOUT_S", "120"))

IUCLID_ACCEPT_POST_URL = os.getenv("IUCLID_ACCEPT_POST_URL", "").strip()
IUCLID_ACCEPT_POST_BODY = os.getenv("IUCLID_ACCEPT_POST_BODY", "").strip()

IUCLID_USE_CLOUDSCRAPER = os.getenv("IUCLID_USE_CLOUDSCRAPER", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _sleep_rate_limit() -> None:
    if IUCLID_REQUEST_DELAY_MS > 0:
        time.sleep(IUCLID_REQUEST_DELAY_MS / 1000.0)


def _cache_file(cache_dir: str, key: str, suffix: str = ".json") -> "Any":
    from pathlib import Path

    base = Path(cache_dir) / "http"
    base.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return base / f"{digest}{suffix}"


def _raw_dump_path(raw_dir: str, key: str) -> "Any":
    from pathlib import Path

    base = Path(raw_dir)
    base.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return base / f"{digest}.json"


def create_session() -> requests.Session:
    """Create an HTTP client that honors ``HTTP_PROXY`` / ``HTTPS_PROXY``.

    Prefer ``cloudscraper`` when ``IUCLID_USE_CLOUDSCRAPER`` is true (default) so ECHA's
    Azure WAF JS challenge is less likely to block programmatic access. Falls back to
    ``requests.Session`` if the package is not installed or cloud mode is disabled.
    """
    session: requests.Session
    used_cloudscraper = False
    if IUCLID_USE_CLOUDSCRAPER:
        try:
            import cloudscraper

            session = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False},
            )
            used_cloudscraper = True
        except ImportError:
            logger.warning(
                "IUCLID_USE_CLOUDSCRAPER is true but cloudscraper is not installed; "
                "install with: pip install cloudscraper. Falling back to requests."
            )
            session = requests.Session()
    else:
        session = requests.Session()

    session.trust_env = True
    session.headers.setdefault("Accept", "application/json, text/plain, */*")
    session.headers.setdefault("Accept-Language", "en")
    if not used_cloudscraper:
        session.headers.setdefault(
            "User-Agent",
            "ChemDB-IUCLID-ingest/1.5 (+https://echa.europa.eu/)",
        )
    return session


def init_iuclid_client(session: requests.Session, base_url: str) -> None:
    """Prime cookies / accept disclaimer if the host serves an interstitial.

    ECHA/IUCLID behaviour changes over time. If the root GET returns HTML that looks like a
    legal notice, set ``IUCLID_ACCEPT_POST_URL`` to the form action you see in DevTools and
    ``IUCLID_ACCEPT_POST_BODY`` to the encoded body (or raw JSON) required to accept.
    """
    root = urljoin(base_url, ".")
    resp = session.get(root, timeout=IUCLID_HTTP_TIMEOUT_S, allow_redirects=True)
    ctype = (resp.headers.get("Content-Type") or "").lower()
    snippet = (resp.text or "")[:4000].lower()
    if "text/html" in ctype and ("disclaimer" in snippet or "cookie" in snippet or "terms" in snippet):
        logger.warning(
            "IUCLID root returned HTML (possible disclaimer). "
            "Set IUCLID_ACCEPT_POST_URL / IUCLID_ACCEPT_POST_BODY after inspecting browser Network tab."
        )
        if IUCLID_ACCEPT_POST_URL:
            body: str | dict[str, Any]
            if IUCLID_ACCEPT_POST_BODY.startswith("{") or IUCLID_ACCEPT_POST_BODY.startswith("["):
                body = json.loads(IUCLID_ACCEPT_POST_BODY) if IUCLID_ACCEPT_POST_BODY else {}
                post_resp = session.post(
                    IUCLID_ACCEPT_POST_URL,
                    json=body if isinstance(body, dict) else body,
                    timeout=IUCLID_HTTP_TIMEOUT_S,
                )
            else:
                post_resp = session.post(
                    IUCLID_ACCEPT_POST_URL,
                    data=IUCLID_ACCEPT_POST_BODY,
                    timeout=IUCLID_HTTP_TIMEOUT_S,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
            post_resp.raise_for_status()
            logger.info("Posted IUCLID disclaimer acceptance to %s", IUCLID_ACCEPT_POST_URL)


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    cache_dir: str,
    raw_dir: str,
    use_cache: bool = True,
) -> Any:
    """GET/POST JSON with retries, optional disk cache, and raw JSON dumps."""
    cache_key = f"{method} {url} {json.dumps(params or {}, sort_keys=True)}"
    cache_path = _cache_file(cache_dir, cache_key)
    raw_path = _raw_dump_path(raw_dir, cache_key)

    if use_cache and cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))

    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(IUCLID_MAX_RETRIES):
        try:
            _sleep_rate_limit()
            resp = session.request(method, url, params=params, timeout=IUCLID_HTTP_TIMEOUT_S)
            if resp.status_code in (500, 502, 503, 504):
                raise requests.HTTPError(f"Transient HTTP {resp.status_code}: {url}", response=resp)
            resp.raise_for_status()
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" in ctype:
                raise RuntimeError(
                    f"Unexpected HTML from {url}. Likely disclaimer/login interstitial. "
                    f"First bytes: {resp.text[:200]!r}"
                )
            data = resp.json()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            raw_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            return data
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_exc = exc
            logger.warning("IUCLID request failed (%s) attempt %s/%s: %s", url, attempt + 1, IUCLID_MAX_RETRIES, exc)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)

    raise RuntimeError(f"IUCLID request failed after {IUCLID_MAX_RETRIES} attempts: {url}") from last_exc


def _extract_uuid_from_substance_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    # Example: iuclid6:/0/SUBSTANCE/<uuid>/SUBSTANCE/<uuid>
    m = re.search(r"/SUBSTANCE/([0-9a-fA-F-]{36})/SUBSTANCE/", uri)
    if m:
        return m.group(1).lower()
    m2 = re.search(r"/SUBSTANCE/([0-9a-fA-F-]{36})$", uri)
    if m2:
        return m2.group(1).lower()
    return None


def _walk_collect_hazard_statements(obj: Any, out: list[dict[str, Any]]) -> None:
    """Heuristic extraction of GHS hazard statement-ish dicts from nested IUCLID JSON."""
    if isinstance(obj, dict):
        keys = {k.lower() for k in obj.keys()}
        if "hazardstatement" in keys or "hazard_statement" in keys or "hazardstatements" in keys:
            hs = obj.get("HazardStatement") or obj.get("hazard_statement") or obj.get("HazardStatements")
            if isinstance(hs, list):
                for item in hs:
                    if isinstance(item, dict):
                        out.append(item)
            elif isinstance(hs, dict):
                out.append(hs)
        for v in obj.values():
            _walk_collect_hazard_statements(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _walk_collect_hazard_statements(v, out)


def _guess_cas_ec_from_blob(rep: dict[str, Any]) -> tuple[str | None, str | None]:
    """Best-effort CAS / EC extraction from a nested IUCLID representation (string scan)."""
    blob = json.dumps(rep, ensure_ascii=False)
    cas_m = re.search(r"\b(\d{2,7}-\d{2}-\d)\b", blob)
    ec_m = re.search(r"\b(\d{3}-\d{3}-\d)\b", blob)
    return (cas_m.group(1) if cas_m else None, ec_m.group(1) if ec_m else None)


def _representation_to_cl_rows(substance_uuid: str, representation: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    stmts: list[dict[str, Any]] = []
    _walk_collect_hazard_statements(representation, stmts)
    cas_guess, ec_guess = _guess_cas_ec_from_blob(representation)
    for st in stmts:
        code = (
            st.get("code")
            or st.get("Code")
            or st.get("value")
            or st.get("Value")
            or st.get("selectedValue")
            or st.get("phraseCode")
        )
        text = st.get("text") or st.get("Text") or st.get("valueText") or st.get("description")
        hazard_class = None
        hazard_category = None
        parent = st
        # Best-effort: keep any obvious class/category siblings if present
        if isinstance(st, dict):
            hazard_class = st.get("hazardClass") or st.get("HazardClass")
            hazard_category = st.get("hazardCategory") or st.get("HazardCategory")
        rows.append(
            {
                "substance_uuid": substance_uuid,
                "ec_number": ec_guess,
                "cas_number": cas_guess,
                "hazard_class": hazard_class,
                "hazard_category": hazard_category,
                "h_statement_code": str(code).strip() if code else None,
                "h_statement_text": str(text).strip() if text else None,
                "supplemental_info": json.dumps(st, ensure_ascii=False)[:2000],
            }
        )
    if not rows:
        blob = json.dumps(representation, ensure_ascii=False)
        for m in re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", blob):
            rows.append(
                {
                    "substance_uuid": substance_uuid,
                    "ec_number": ec_guess,
                    "cas_number": cas_guess,
                    "hazard_class": None,
                    "hazard_category": None,
                    "h_statement_code": m,
                    "h_statement_text": None,
                    "supplemental_info": None,
                }
            )
    return rows


def get_substance_clp(session: requests.Session, base_url: str, substance_uuid: str, cache_dir: str) -> dict[str, Any]:
    """Fetch CLP/GHS-related payload for a single substance UUID.

    **ext** style uses ``GET /raw/SUBSTANCE/{uuid}?formatter=...`` (documented in IUCLID PDF).

    **rest** style (experimental) uses ``GET /substances/{uuid}/clp`` relative to a ``/rest/v1/`` base.
    """
    init_iuclid_client(session, base_url)
    if IUCLID_API_STYLE == "rest":
        url = urljoin(base_url, f"substances/{substance_uuid}/clp")
        data = _request_json(session, "GET", url, cache_dir=cache_dir, raw_dir=IUCLID_RAW_DIR)
        return data if isinstance(data, dict) else {"payload": data}

    url = urljoin(base_url, f"raw/SUBSTANCE/{substance_uuid}")
    params = {"formatter": "iuclid6.DocumentSecuredRepresentation"}
    return _request_json(session, "GET", url, params=params, cache_dir=cache_dir, raw_dir=IUCLID_RAW_DIR)


def get_all_substances(session: requests.Session, base_url: str, page_size: int, cache_dir: str) -> pd.DataFrame:
    """Fetch a substance identifier table suitable to replace the old ECHA registered-substances CSV.

    **ext** mode (default): paginates ``GET /inventory`` (documented as unsecured) for
    ``IUCLID_INVENTORY_CODE`` (default ``EC``), which yields EC number, CAS, name, formula.

    **rest** mode (experimental): ``GET /substances`` with ``limit``/``offset`` query synonyms.
    """
    init_iuclid_client(session, base_url)
    rows: list[dict[str, Any]] = []

    if IUCLID_API_STYLE == "rest":
        offset = 0
        total: int | None = None
        with tqdm(desc="IUCLID REST substances", unit="page") as pbar:
            while True:
                url = urljoin(base_url, "substances")
                data: dict[str, Any] | None = None
                last_err: Exception | None = None
                for params in (
                    {"limit": page_size, "offset": offset},
                    {"l": page_size, "o": offset},
                    {"pageSize": page_size, "page": offset // max(page_size, 1)},
                ):
                    try:
                        data = _request_json(session, "GET", url, params=params, cache_dir=cache_dir, raw_dir=IUCLID_RAW_DIR)
                        if isinstance(data, dict):
                            break
                    except Exception as exc:
                        last_err = exc
                        data = None
                if data is None or not isinstance(data, dict):
                    raise RuntimeError(
                        f"REST /substances request failed or returned non-object; inspect IUCLID host or switch IUCLID_API_STYLE=ext. Last error: {last_err}"
                    ) from last_err
                items = data.get("items") or data.get("results") or data.get("content") or data.get("data")
                if items is None:
                    raise RuntimeError(f"Could not parse REST substances payload keys={list(data.keys())}")
                if total is None:
                    total = int(data.get("total") or data.get("totalCount") or data.get("totalElements") or len(items))
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    uid = it.get("uuid") or it.get("id") or it.get("substanceUuid")
                    rows.append(
                        {
                            "uuid": str(uid) if uid else None,
                            "ec_number": it.get("ecNumber") or it.get("ec_number") or it.get("ec"),
                            "cas_number": it.get("casNumber") or it.get("cas_number") or it.get("cas"),
                            "substance_name": it.get("name") or it.get("substanceName") or it.get("publicName"),
                            "registration_date": it.get("registrationDate") or it.get("createdOn"),
                            "regulatory_pool": it.get("regulatoryPool") or it.get("regulatory_pool") or it.get("status"),
                            "infocard_url": it.get("infocardUrl") or it.get("infocard_url"),
                        }
                    )
                offset += len(items)
                pbar.update(1)
                if not items or (total is not None and offset >= total) or len(items) == 0:
                    break
        return pd.DataFrame(rows)

    # --- ext: inventory pagination ---
    offset = 0
    total_count: int | None = None
    with tqdm(desc="IUCLID inventory", unit="page") as pbar:
        while True:
            url = urljoin(base_url, "inventory")
            params: dict[str, Any] = {
                "code": IUCLID_INVENTORY_CODE,
                "l": page_size,
                "o": offset,
                "count": "true",
            }
            data = _request_json(session, "GET", url, params=params, cache_dir=cache_dir, raw_dir=IUCLID_RAW_DIR)
            total_count = int(data.get("totalCount", total_count or 0))
            batch = data.get("results") or []
            for item in batch:
                uri = item.get("uri") if isinstance(item, dict) else None
                rep = item.get("representation") if isinstance(item, dict) else None
                if not isinstance(rep, dict):
                    continue
                number = rep.get("number")
                cas_number = rep.get("casNumber")
                name = rep.get("name")
                rows.append(
                    {
                        "uuid": uri,
                        "ec_number": str(number).strip() if number is not None else None,
                        "cas_number": str(cas_number).strip() if cas_number is not None else None,
                        "substance_name": str(name).strip() if name is not None else None,
                        "registration_date": rep.get("createdOn") or rep.get("modifiedOn"),
                        "regulatory_pool": rep.get("status"),
                        "infocard_url": uri,
                    }
                )
            offset += len(batch)
            pbar.update(1)
            if not batch or offset >= total_count:
                break

    return pd.DataFrame(rows)


def build_full_cl_inventory(
    substances_df: pd.DataFrame,
    session: requests.Session,
    base_url: str,
    cache_dir: str,
    max_workers: int = 5,
) -> pd.DataFrame:
    """Build a flattened C&L / GHS-style table.

    **Strategy ``by_ghs`` (default):** paginates ``GET /query/iuclid6/byGhs`` with
    ``doc.type=SUBSTANCE`` and ``formatter=iuclid6.DocumentSecuredRepresentation`` so each page
    may embed GHS sections directly (avoids N+1 raw fetches when the host allows anonymous search).

    **Strategy ``per_substance``:** uses ``ThreadPoolExecutor`` to call ``get_substance_clp`` for
    each UUID in ``substances_df['uuid']`` (must be real IUCLID SUBSTANCE UUIDs, not inventory URIs).
    """
    init_iuclid_client(session, base_url)

    if IUCLID_CLP_STRATEGY == "per_substance":
        uuids = []
        for u in substances_df.get("uuid", []):
            if u is None or (isinstance(u, float) and pd.isna(u)):
                continue
            su = str(u).strip()
            extracted = _extract_uuid_from_substance_uri(su)
            if extracted:
                uuids.append(extracted)
            elif re.fullmatch(r"[0-9a-fA-F-]{36}", su):
                uuids.append(su.lower())

        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(get_substance_clp, session, base_url, uid, cache_dir): uid for uid in uuids}
            for fut in tqdm(as_completed(futures), total=len(futures), desc="IUCLID per-substance CLP"):
                uid = futures[fut]
                try:
                    payload = fut.result()
                except Exception as exc:
                    logger.warning("CLP fetch failed for %s: %s", uid, exc)
                    continue
                if IUCLID_API_STYLE == "rest":
                    rep = payload if isinstance(payload, dict) else {}
                    rows.extend(_representation_to_cl_rows(uid, rep))
                else:
                    rep = payload if isinstance(payload, dict) else {}
                    inner = rep.get("representation") if isinstance(rep.get("representation"), dict) else rep
                    rows.extend(_representation_to_cl_rows(uid, inner))
        return pd.DataFrame(rows)

    # --- by_ghs bulk pagination ---
    out_rows: list[dict[str, Any]] = []
    offset = 0
    total_count: int | None = None
    with tqdm(desc="IUCLID byGhs", unit="page") as pbar:
        while True:
            url = urljoin(base_url, "query/iuclid6/byGhs")
            params = {
                "doc.type": "SUBSTANCE",
                "formatter": "iuclid6.DocumentSecuredRepresentation",
                "l": IUCLID_PAGE_SIZE,
                "o": offset,
                "count": "true",
            }
            try:
                data = _request_json(session, "GET", url, params=params, cache_dir=cache_dir, raw_dir=IUCLID_RAW_DIR)
            except Exception as exc:
                logger.error(
                    "byGhs query failed (%s). If your IUCLID host requires authentication for search, "
                    "switch IUCLID_CLP_STRATEGY=per_substance with substance UUIDs, or use cached JSON in %s. Error: %s",
                    url,
                    IUCLID_RAW_DIR,
                    exc,
                )
                break
            if total_count is None:
                total_count = int(data.get("totalCount", 0))
            batch = data.get("results") or []
            for item in batch:
                if not isinstance(item, dict):
                    continue
                uri = item.get("uri")
                uid = _extract_uuid_from_substance_uri(uri) or ""
                rep = item.get("representation")
                if isinstance(rep, dict):
                    out_rows.extend(_representation_to_cl_rows(uid or "unknown", rep))
            offset += len(batch)
            pbar.update(1)
            if not batch or (total_count is not None and offset >= total_count):
                break

    return pd.DataFrame(out_rows)


def registered_and_cl_dataframes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """High-level helper: authenticated session + both tables for ``echa_loader``."""
    session = create_session()
    base = IUCLID_API_BASE_URL
    reg = get_all_substances(session, base, IUCLID_PAGE_SIZE, IUCLID_CACHE_DIR)
    cl = build_full_cl_inventory(reg, session, base, IUCLID_CACHE_DIR)
    return reg, cl


def to_echa_loader_frames(reg: pd.DataFrame, cl: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map IUCLID frames to the column names expected by ``ingest/echa_loader.py``."""
    reg2 = reg.copy()
    if "cas_number" in reg2.columns:
        reg2["cas_rn"] = reg2["cas_number"]
    elif "cas_rn" not in reg2.columns:
        reg2["cas_rn"] = None

    cl2 = cl.copy()
    if "cas_number" in cl2.columns:
        cl2["cas_rn"] = cl2["cas_number"]
    elif "cas_rn" not in cl2.columns:
        cl2["cas_rn"] = None
    if "h_statement_code" in cl2.columns:
        cl2 = cl2.rename(columns={"h_statement_code": "hazard_code"})
    if "h_statement_text" in cl2.columns:
        cl2 = cl2.rename(columns={"h_statement_text": "classification"})
    if "classification" not in cl2.columns:
        cl2["classification"] = None
    if "hazard_code" not in cl2.columns:
        cl2["hazard_code"] = None
    if "source_reference" not in cl2.columns:
        cl2["source_reference"] = "IUCLID Public REST API (byGhs/raw)"
    return reg2, cl2
