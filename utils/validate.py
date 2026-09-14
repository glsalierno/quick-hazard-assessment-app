"""
Cross-validate OPERA predictions against PubChem, EPA ToxVal (API + local SQLite), and ECHA web data.

Uses exponential backoff for HTTP, optional disk cache under ``data/validation_cross_cache/``,
and ``urllib.robotparser`` before scraping ECHA.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
import urllib.robotparser
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin, urlparse

from utils import opera_batch, opera_client

_LOG = logging.getLogger(__name__)

PUBCHEM_REST = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
PUBCHEM_VIEW = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound"
ECHA_ORIGIN = "https://echa.europa.eu"
DEFAULT_UA = "Mozilla/5.0 (compatible; HazqueryCrossValidate/1.0; +https://github.com/glsalierno/quick-hazard-assessment-app) research/QSAR"

H_CODE_RE = re.compile(r"\bH\d{3}(?:\s*\+\s*H\d{3})?\b", re.I)


def _default_data_dir() -> Path:
    try:
        import config as _cfg

        return Path(getattr(_cfg, "DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
    except Exception:
        return Path(__file__).resolve().parents[1] / "data"


def _cache_dir() -> Path:
    d = _default_data_dir() / "validation_cross_cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _disk_cache_path(key: str) -> Path:
    return _cache_dir() / f"{key}.json"


def _disk_cache_get(key: str, max_age_seconds: float = 21_600.0) -> Any | None:
    p = _disk_cache_path(key)
    if not p.is_file():
        return None
    try:
        if time.time() - p.stat().st_mtime > max_age_seconds:
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _disk_cache_set(key: str, payload: Any) -> None:
    try:
        _disk_cache_path(key).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        _LOG.debug("validation cache write failed: %s", e)


def _request_with_backoff(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    data: dict[str, str] | None = None,
    timeout: float = 45.0,
    max_retries: int = 5,
    session: requests.Session | None = None,
) -> requests.Response:
    """GET/POST with exponential backoff on 429/5xx and transient errors."""
    sess = session or requests.Session()
    hdrs = {"User-Agent": _user_agent(), **(headers or {})}
    delay = 0.6
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            if method.upper() == "GET":
                r = sess.get(url, headers=hdrs, params=params, timeout=timeout)
            else:
                r = sess.post(url, headers=hdrs, params=params, data=data, timeout=timeout)
            if r.status_code == 429 or (500 <= r.status_code < 600):
                time.sleep(delay + random.uniform(0, 0.3))
                delay = min(delay * 2, 16.0)
                continue
            return r
        except (requests.RequestException, OSError) as e:
            last_err = e
            time.sleep(delay + random.uniform(0, 0.3))
            delay = min(delay * 2, 16.0)
    assert last_err is not None
    raise last_err


def _user_agent() -> str:
    try:
        import os

        return os.environ.get("HAZQUERY_VALIDATION_UA", DEFAULT_UA).strip() or DEFAULT_UA
    except Exception:
        return DEFAULT_UA


def _robots_allowed(url: str, ua: str | None = None) -> bool:
    """Return True if ``url`` may be fetched per host ``robots.txt`` (fail-open on parse errors)."""
    try:
        parts = urlparse(url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            return True
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(ua or _user_agent(), url)
    except Exception as e:
        _LOG.debug("robots.txt check failed for %s: %s", url, e)
        return True


def _cas_to_cid(cas: str) -> Optional[int]:
    cas = (cas or "").strip()
    if not cas:
        return None
    key = hashlib.sha256(f"cid|{cas}".encode()).hexdigest()
    hit = _disk_cache_get(f"pubchem_cid_{key}", max_age_seconds=86400.0 * 30)
    if isinstance(hit, int):
        return hit
    url = f"{PUBCHEM_REST}/compound/xref/RN/{quote(cas, safe='')}/cids/TXT"
    try:
        r = _request_with_backoff("GET", url)
        if not r.ok:
            _disk_cache_set(f"pubchem_cid_{key}", None)
            return None
        for line in (r.text or "").splitlines():
            line = line.strip()
            if line.isdigit():
                cid = int(line)
                _disk_cache_set(f"pubchem_cid_{key}", cid)
                return cid
    except Exception as e:
        _LOG.debug("CID resolve failed for %s: %s", cas, e)
    _disk_cache_set(f"pubchem_cid_{key}", None)
    return None


def _pug_view_heading(cid: int, heading: str) -> Optional[dict]:
    """Fetch PUG View JSON for one ``heading`` (e.g. ``Melting Point``)."""
    key = hashlib.sha256(f"pugview|{cid}|{heading}".encode()).hexdigest()
    hit = _disk_cache_get(f"pc_{key}", max_age_seconds=86400.0 * 7)
    if isinstance(hit, dict):
        return hit
    url = f"{PUBCHEM_VIEW}/{cid}/JSON?heading={quote(heading)}"
    try:
        r = _request_with_backoff("GET", url)
        if r.status_code != 200:
            return None
        data = r.json()
        _disk_cache_set(f"pc_{key}", data)
        return data
    except Exception as e:
        _LOG.debug("PUG View %s / %s: %s", cid, heading, e)
    return None


def _first_information_string(blob: dict | None) -> tuple[str, str]:
    """Extract (value_text, reference_snippet) from PUG View Record."""
    if not blob or not isinstance(blob, dict):
        return "", ""
    ref = ""
    rec = blob.get("Record") or blob
    sections = rec.get("RecordSection") or rec.get("Section") or []
    if isinstance(sections, dict):
        sections = [sections]

    def walk(nodes: list, depth: int = 0) -> str:
        if depth > 40:
            return ""
        for node in nodes:
            if not isinstance(node, dict):
                continue
            info = node.get("Information")
            if isinstance(info, list):
                for block in info:
                    if not isinstance(block, dict):
                        continue
                    vals = block.get("Value") or {}
                    if isinstance(vals, dict):
                        swm = vals.get("StringWithMarkup")
                        if isinstance(swm, list) and swm:
                            s0 = swm[0]
                            if isinstance(s0, dict) and s0.get("String"):
                                return str(s0["String"]).strip()
                        if vals.get("String"):
                            return str(vals["String"]).strip()
            subs = node.get("Section")
            if isinstance(subs, list):
                got = walk(subs, depth + 1)
                if got:
                    return got
        return ""

    val = walk(sections)
    return val, ref


def get_pubchem_experimental(cas: str, endpoint: str) -> dict[str, Any]:
    """
    Return experimental (or curated display) values from PubChem for ``cas`` relevant to ``endpoint``.

    ``endpoint`` is typically an OPERA column name (e.g. ``LogP_pred``, ``MP_pred``).

    Returns keys: ``source``, ``value``, ``unit``, ``reference`` (strings; may be empty).
    """
    out: dict[str, Any] = {"source": "PubChem", "value": "", "unit": "", "reference": ""}
    cas = (cas or "").strip()
    ep = (endpoint or "").strip().lower()
    if not cas:
        return out

    cache_key = hashlib.sha256(f"pc_exp|{cas}|{endpoint}".encode()).hexdigest()
    hit = _disk_cache_get(cache_key)
    if isinstance(hit, dict) and hit.get("source") == "PubChem":
        return hit

    cid = _cas_to_cid(cas)
    if cid is None:
        _disk_cache_set(cache_key, out)
        return out

    try:
        if "logp" in ep or "log_p" in ep or "logd" in ep:
            r = _request_with_backoff(
                "GET",
                f"{PUBCHEM_REST}/compound/cid/{cid}/property/XLogP,IsomericSMILES/JSON",
            )
            if r.ok:
                j = r.json()
                props = (j.get("PropertyTable") or {}).get("Properties") or [{}]
                p0 = props[0] if props else {}
                xlogp = p0.get("XLogP")
                if xlogp is not None:
                    out["value"] = str(xlogp)
                    out["unit"] = "dimensionless (XLogP)"
                    out["reference"] = "PubChem PUG REST property XLogP (estimated display value)"
        elif "mp_pred" in ep or ep.startswith("mp") or "melting" in ep:
            blob = _pug_view_heading(cid, "Melting Point")
            val, ref = _first_information_string(blob)
            if val:
                out["value"] = val
                out["unit"] = "°C (as reported in PubChem)"
                out["reference"] = ref or "PubChem PUG View: Melting Point"
        elif "bp_pred" in ep or ep.startswith("bp") or "boiling" in ep:
            blob = _pug_view_heading(cid, "Boiling Point")
            val, ref = _first_information_string(blob)
            if val:
                out["value"] = val
                out["unit"] = "°C (as reported in PubChem)"
                out["reference"] = ref or "PubChem PUG View: Boiling Point"
        elif "ld50" in ep or "catmos" in ep:
            blob = _pug_view_heading(cid, "Toxicity Data")
            val, ref = _first_information_string(blob)
            if val:
                out["value"] = val[:500]
                out["unit"] = ""
                out["reference"] = ref or "PubChem PUG View: Toxicity Data (excerpt)"
        else:
            blob = _pug_view_heading(cid, "Experimental Properties")
            val, ref = _first_information_string(blob)
            if val:
                out["value"] = val[:800]
                out["unit"] = ""
                out["reference"] = ref or "PubChem PUG View: Experimental Properties (excerpt)"
    except Exception as e:
        _LOG.debug("get_pubchem_experimental %s: %s", cas, e)

    _disk_cache_set(cache_key, out)
    return out


def _get_epa_api_key() -> str | None:
    import os

    from utils.streamlit_secrets import get_secret

    k = get_secret("EPA_API_KEY") or get_secret("COMPTOX_API_KEY")
    if k:
        return k
    try:
        import config as _cfg

        return (getattr(_cfg, "EPA_API_KEY", None) or "").strip() or None
    except Exception:
        return None


def get_epa_toxval(cas: str, endpoint: str) -> dict[str, Any]:
    """
    Return a representative ToxVal-style toxicity row for ``cas`` filtered by ``endpoint``.

    Priority:

    1. **Local SQLite** ``chemical_db`` (built from COMPTOX Excel) when available.
    2. **Remote CCTE API** using ``EPA_API_KEY`` / ``COMPTOX_API_KEY`` (``x-api-key`` header),
       reusing :func:`utils.toxvaldb_client.fetch_toxval_data` after DTXSID resolution from DSSTox.

    Returns keys: ``source``, ``value``, ``unit``, ``reference``, ``toxval_type``.
    """
    out: dict[str, Any] = {
        "source": "",
        "value": "",
        "unit": "",
        "reference": "",
        "toxval_type": "",
    }
    cas = (cas or "").strip()
    ep = (endpoint or "").strip().lower()
    if not cas:
        return out

    cache_key = hashlib.sha256(f"epa_tv|{cas}|{endpoint}".encode()).hexdigest()
    hit = _disk_cache_get(cache_key)
    if isinstance(hit, dict) and "value" in hit:
        return hit

    want_ld50 = any(x in ep for x in ("ld50", "catmos", "acute"))

    # --- Local SQLite ToxValDB ---
    try:
        from utils import chemical_db

        loc = chemical_db.get_toxicity_by_cas(cas, numeric_only=True)
        best: dict[str, Any] | None = None
        for r in loc:
            st = str(r.get("study_type") or "").lower()
            val = r.get("toxval_numeric")
            if val is None:
                continue
            if want_ld50 and not any(k in st for k in ("ld50", "oral", "acute", "lethal")):
                continue
            if best is None:
                best = r
            else:
                try:
                    if float(val) < float(best.get("toxval_numeric") or 1e99):
                        best = r
                except (TypeError, ValueError):
                    pass
        if best:
            out["source"] = "ToxValDB (local SQLite)"
            out["value"] = str(best.get("toxval_numeric", ""))
            out["unit"] = str(best.get("toxval_units") or best.get("units") or "")
            out["reference"] = str(best.get("reference") or best.get("study_type") or "")
            out["toxval_type"] = str(best.get("study_type") or "")
            _disk_cache_set(cache_key, out)
            return out
    except Exception as e:
        _LOG.debug("local ToxValDB skip: %s", e)

    # --- Remote API ---
    api_key = _get_epa_api_key()
    if not api_key:
        _disk_cache_set(cache_key, out)
        return out

    try:
        from utils import chemical_db
        from utils import toxvaldb_client

        ds = chemical_db.get_dsstox_by_cas(cas)
        dtxsid = (ds or {}).get("dtxsid") if ds else None
        if not dtxsid:
            _disk_cache_set(cache_key, out)
            return out
        data = toxvaldb_client.fetch_toxval_data(str(dtxsid), api_key=api_key)
        if not data:
            _disk_cache_set(cache_key, out)
            return out
        flat: list[dict[str, Any]] = []
        for _cat, recs in data.items():
            for rec in recs or []:
                flat.append(rec)
        cand = flat
        if want_ld50:
            cand = [
                r
                for r in flat
                if any(
                    k in str(r.get("study_type", "")).lower()
                    for k in ("ld50", "oral", "acute", "lethal")
                )
            ] or flat
        if not cand:
            _disk_cache_set(cache_key, out)
            return out
        pick = min(
            cand,
            key=lambda r: float(r.get("toxval_numeric") or r.get("value") or 1e99),
        )
        out["source"] = "EPA CCTE / ToxVal (API)"
        out["value"] = str(pick.get("toxval_numeric") or pick.get("value") or "")
        out["unit"] = str(pick.get("toxval_units") or pick.get("units") or "")
        out["reference"] = str(pick.get("reference") or "")
        out["toxval_type"] = str(pick.get("study_type") or "")
    except Exception as e:
        _LOG.debug("EPA ToxVal API: %s", e)

    _disk_cache_set(cache_key, out)
    return out


def get_echa_classification(cas: str, endpoint: str) -> dict[str, Any]:
    """
    Lightweight ECHA HTML search for harmonised hazard codes.

    Respects ``robots.txt`` via :func:`_robots_allowed`. Uses ``BeautifulSoup`` on the
    public search-results page; structure may change without notice.

    Returns: ``classification``, ``h_codes`` (comma-separated), ``source_url``, ``note``.
    """
    out: dict[str, Any] = {
        "classification": "",
        "h_codes": "",
        "source_url": "",
        "note": "",
    }
    cas = (cas or "").strip()
    if not cas:
        return out

    cache_key = hashlib.sha256(f"echa|{cas}|{endpoint}".encode()).hexdigest()
    hit = _disk_cache_get(cache_key, max_age_seconds=86400.0 * 3)
    if isinstance(hit, dict) and "h_codes" in hit:
        return hit

    search_url = (
        f"{ECHA_ORIGIN}/search-for-chemicals/-/dislist/search-results"
        f"?keywords={quote(cas)}"
    )
    if not _robots_allowed(search_url):
        out["note"] = "Blocked by robots.txt for this URL; skipping ECHA fetch."
        _disk_cache_set(cache_key, out)
        return out

    try:
        r = _request_with_backoff("GET", search_url)
        if r.status_code != 200:
            out["note"] = f"ECHA search HTTP {r.status_code}"
            _disk_cache_set(cache_key, out)
            return out
        soup = BeautifulSoup(r.text, "html.parser")
        link = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/substance-information/" in href or "/information-on-chemicals/" in href:
                link = urljoin(ECHA_ORIGIN, href)
                break
        if not link:
            out["note"] = "No substance link found on ECHA search results (markup may have changed)."
            _disk_cache_set(cache_key, out)
            return out
        if not _robots_allowed(link):
            out["note"] = "Substance URL disallowed by robots.txt."
            _disk_cache_set(cache_key, out)
            return out
        r2 = _request_with_backoff("GET", link)
        if r2.status_code != 200:
            out["note"] = f"ECHA substance HTTP {r2.status_code}"
            _disk_cache_set(cache_key, out)
            return out
        soup2 = BeautifulSoup(r2.text, "html.parser")
        text = soup2.get_text(" ", strip=True)
        codes = sorted(set(H_CODE_RE.findall(text)))
        out["h_codes"] = ", ".join(codes)
        out["source_url"] = link
        if "catmos" in (endpoint or "").lower() or "ld50" in (endpoint or "").lower():
            acute = [c for c in codes if c.upper().startswith("H3")]
            out["classification"] = "Acute toxicity hazard phrases: " + ", ".join(acute) if acute else ""
        else:
            out["classification"] = "Hazard statements (subset): " + ", ".join(codes[:12])
    except Exception as e:
        out["note"] = str(e)[:500]
        _LOG.debug("ECHA scrape: %s", e)

    _disk_cache_set(cache_key, out)
    return out


def cross_validate_cas_list(
    cas_list: list[str],
    endpoint: str,
    *,
    use_disk_cache: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
    sleep_between_cas: float = 0.35,
) -> pd.DataFrame:
    """
    For each CAS: OPERA prediction (via ``predict_opera``), PubChem, EPA ToxVal, ECHA.

    ``endpoint`` should be an OPERA column name (e.g. ``LogP_pred``) understood by
    :func:`opera_client.predict_opera`.

    Rate limiting: small sleep between rows; PubChem/EPA helpers use backoff + disk cache.
    """
    clean = sorted({(c or "").strip() for c in cas_list if (c or "").strip()})
    if not clean:
        return pd.DataFrame()

    if use_disk_cache:
        bulk_key = hashlib.sha256((endpoint + "|" + "\n".join(clean)).encode()).hexdigest()
        cached = _disk_cache_get(f"cross|{bulk_key}", max_age_seconds=3600.0)
        if isinstance(cached, list):
            return pd.DataFrame(cached)

    sm_map = opera_batch.smiles_from_cas_batch(clean)
    smiles_run = [sm_map[c] for c in clean if sm_map.get(c)]
    pred_by_smiles: dict[str, dict[str, Any]] = {}
    if smiles_run:
        try:
            raw_pred = opera_client.predict_opera(smiles_run, endpoint)  # type: ignore[arg-type]
            pred_by_smiles = raw_pred if isinstance(raw_pred, dict) else {}
            if "__error__" in pred_by_smiles:
                _LOG.warning("OPERA endpoint error: %s", pred_by_smiles.get("__error__"))
                pred_by_smiles = {}
        except Exception as e:
            _LOG.warning("OPERA batch predict failed: %s", e)
            pred_by_smiles = {}

    rows: list[dict[str, Any]] = []
    n = len(clean)
    for i, cas in enumerate(clean, start=1):
        if progress_callback:
            progress_callback(i, n)
        sm = sm_map.get(cas) or ""
        opera_val = ""
        if sm and isinstance(pred_by_smiles, dict):
            pr = pred_by_smiles.get(sm, {})
            if isinstance(pr, dict):
                opera_val = str(pr.get("value", "") or "")
        elif sm:
            try:
                one = opera_client.predict_opera(sm, endpoint)
                if isinstance(one, dict):
                    opera_val = str(one.get("value", "") or "")
            except Exception:
                pass

        pc = get_pubchem_experimental(cas, endpoint)
        epa = get_epa_toxval(cas, endpoint)
        echa = get_echa_classification(cas, endpoint)
        rows.append(
            {
                "CAS": cas,
                "SMILES": sm or None,
                "OPERA_value": opera_val or None,
                "OPERA_endpoint": endpoint,
                "PubChem_value": pc.get("value") or None,
                "PubChem_unit": pc.get("unit") or None,
                "PubChem_reference": pc.get("reference") or None,
                "ToxVal_value": epa.get("value") or None,
                "ToxVal_unit": epa.get("unit") or None,
                "ToxVal_source": epa.get("source") or None,
                "ToxVal_type": epa.get("toxval_type") or None,
                "ECHA_classification": echa.get("classification") or None,
                "ECHA_h_codes": echa.get("h_codes") or None,
                "ECHA_source_url": echa.get("source_url") or None,
                "ECHA_note": echa.get("note") or None,
            }
        )
        time.sleep(sleep_between_cas)

    df = pd.DataFrame(rows)
    if use_disk_cache:
        bulk_key = hashlib.sha256((endpoint + "|" + "\n".join(clean)).encode()).hexdigest()
        _disk_cache_set(f"cross|{bulk_key}", df.to_dict(orient="records"))
    return df


def load_cas_list_from_p2oasys_csv(path: str | Path, *, cas_column: str = "CAS", limit: int = 500) -> list[str]:
    """
    Extract checksum-valid CAS tokens from a P2OASys-style CSV (cells may list multiple CAS).

    Uses :func:`utils.cas_text_extract.find_checksum_valid_cas_in_text` per cell.
    """
    from utils.cas_text_extract import find_checksum_valid_cas_in_text

    p = Path(path)
    if not p.is_file():
        return []
    df = pd.read_csv(p, nrows=limit + 50)
    if cas_column not in df.columns:
        return []
    found: list[str] = []
    for raw in df[cas_column].astype(str):
        for c in find_checksum_valid_cas_in_text(raw):
            found.append(c)
        if len(found) >= limit * 3:
            break
    return sorted(set(found))[:limit]
