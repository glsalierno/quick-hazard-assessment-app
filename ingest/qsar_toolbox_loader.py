"""Load REACH-style substance identifiers and hazard-like data from the **local** QSAR Toolbox WebAPI.

The OECD QSAR Toolbox WebAPI (Kestrel) is documented at a high level on
https://qsartoolbox.org/developers/webapi/ . It is started from **Toolbox WebSuite** and/or the
**Toolbox Server** status app; the window shows **Listening on localhost:** followed by a port
(for example **8804** on Toolbox 4.7.x; the port is not fixed). Set ``QSAR_TOOLBOX_PORT`` to that value.

Concrete HTTP paths match **PyQSARToolbox** (``pip install pyqsartoolbox``). Toolbox **4.7+** often
serves the WebAPI over **HTTPS** on the listening port; plain **HTTP** can fail with ``BadStatusLine``
and binary garbage (TLS/HTTP2 bytes). This module probes **https** then **http** when ``QSAR_SCHEME=auto``
(default). Set ``QSAR_SCHEME=https`` or ``http`` to force one.

**Not the same as** https://repository.qsartoolbox.org/ — that site is the **Toolbox Repository**
(catalog of downloadable profilers, databases, QSARs). It is **not** the local ``/api/v6`` host;
``repository.qsartoolbox.org/api/`` may 404 in a browser because the public site is not the WebAPI.

Examples: ``GET .../about/toolbox/version``, ``GET .../search/cas/{cas}/{ignoreStereo}``,
``GET .../data/all/{chem_id}?includeMetadata=false``.

**Important:** The WebAPI does **not** expose a documented "list all REACH substances" cursor
like ``GET /substances?page=``. This module therefore:

1. Optionally probes a few speculative REST paths (if your Toolbox build adds them).
2. Otherwise reads a **CAS seed list** (text or CSV) and resolves each CAS via ``search/cas``,
   in pages of ``QSAR_PAGE_SIZE``.

Set ``QSAR_SUBSTANCE_SEED_PATH`` to a file of CAS numbers (one per line), or rely on the default
lookup for ``DSS/cas_dtxsid_mapping.csv`` in the current working directory when present.

**PyQSARToolbox (default):** ``connect_to_websuite``, ``build_dataframes``, and ``load_echa_from_qsar_toolbox``
use the library when ``QSAR_USE_PYQSARTOOLBOX`` is true, with ``requests.get`` patched for ``QSAR_VERIFY_SSL``.
Set ``QSAR_USE_PYQSARTOOLBOX=false`` to force the older direct ``requests`` session path only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from tqdm import tqdm

from ingest.crosswalk import normalize_cas

logger = logging.getLogger(__name__)

# Default only for backward compatibility; set QSAR_TOOLBOX_PORT to match "Listening on localhost:####"
# in the Toolbox Server / WebSuite window (e.g. 8804 on many 4.7 installs).
QSAR_TOOLBOX_PORT = int(os.getenv("QSAR_TOOLBOX_PORT", "51946"))
# Hostname only or full URL; scheme for API is chosen by QSAR_SCHEME / discovery (not always this URL's scheme).
QSAR_HOST = os.getenv("QSAR_HOST", "127.0.0.1").strip().rstrip("/")
QSAR_API_VERSION = os.getenv("QSAR_API_VERSION", "v6").strip().strip("/")
QSAR_SCHEME = os.getenv("QSAR_SCHEME", "auto").strip().lower()
QSAR_VERIFY_SSL = os.getenv("QSAR_VERIFY_SSL", "false").strip().lower() in ("1", "true", "yes", "on")
QSAR_CACHE_DIR = os.getenv("QSAR_CACHE_DIR", "data/qsar_cache")
QSAR_PAGE_SIZE = int(os.getenv("QSAR_PAGE_SIZE", "100"))
QSAR_REQUEST_DELAY_MS = int(os.getenv("QSAR_REQUEST_DELAY_MS", "50"))
QSAR_MAX_RETRIES = int(os.getenv("QSAR_MAX_RETRIES", "3"))
QSAR_HTTP_TIMEOUT_S = int(os.getenv("QSAR_HTTP_TIMEOUT_S", "120"))
QSAR_SUBSTANCE_SEED_PATH = os.getenv("QSAR_SUBSTANCE_SEED_PATH", "").strip()
QSAR_SEED_CAS_COLUMN = os.getenv("QSAR_SEED_CAS_COLUMN", "").strip()
QSAR_CLASSIFICATION_MAX_WORKERS = int(os.getenv("QSAR_CLASSIFICATION_MAX_WORKERS", "5"))
# Prefer PyQSARToolbox (matches local TLS scheme); set to false to force raw ``requests`` loader only.
QSAR_USE_PYQSARTOOLBOX = os.getenv("QSAR_USE_PYQSARTOOLBOX", "true").strip().lower() in ("1", "true", "yes", "on")
QSAR_AUTO_INSTALL_PYQSARTOOLBOX = os.getenv("QSAR_AUTO_INSTALL_PYQSARTOOLBOX", "false").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
QSAR_PYQSARTOOLBOX_GIT = os.getenv(
    "QSAR_PYQSARTOOLBOX_GIT",
    "git+https://github.com/USEtox/PyQSARToolbox.git",
).strip()


def _sleep_rate_limit() -> None:
    if QSAR_REQUEST_DELAY_MS > 0:
        time.sleep(QSAR_REQUEST_DELAY_MS / 1000.0)


def _hostname_from_qsar_host() -> str:
    raw = (QSAR_HOST or "127.0.0.1").strip()
    if "://" in raw:
        u = urlparse(raw)
        return (u.hostname or "127.0.0.1").strip()
    return raw.split("/")[0].strip() or "127.0.0.1"


def discover_qsar_api_base(session: requests.Session, port: int | None = None) -> str:
    """Pick working ``{scheme}://host:port/api/v6/`` (HTTPS first on *auto* for Toolbox 4.7+)."""
    p = int(port if port is not None else QSAR_TOOLBOX_PORT)
    hostname = _hostname_from_qsar_host()
    if QSAR_SCHEME == "https":
        schemes: list[str] = ["https"]
    elif QSAR_SCHEME == "http":
        schemes = ["http"]
    else:
        schemes = ["https", "http"]

    if not QSAR_VERIFY_SSL:
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass

    last_err: Exception | None = None
    for scheme in schemes:
        base = f"{scheme}://{hostname}:{p}/api/{QSAR_API_VERSION}/"
        url = urljoin(base, "about/toolbox/version")
        try:
            r = session.get(url, timeout=8)
            if r.status_code == 200:
                logger.info("QSAR Toolbox WebAPI base: %s", base)
                return base
        except Exception as exc:
            last_err = exc
            logger.debug("QSAR probe failed %s: %s", url, exc)
            continue
    raise ConnectionError(
        f"Could not reach QSAR Toolbox WebAPI on port {p} with schemes {schemes} (QSAR_SCHEME={QSAR_SCHEME!r}). "
        f"If you saw BadStatusLine/binary errors over HTTP, try HTTPS (default auto probes https first). "
        f"Last error: {last_err}"
    ) from last_err


def _cache_path(cache_dir: str, key: str) -> Path:
    base = Path(cache_dir) / "http"
    base.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return base / f"{digest}.json"


def create_qsar_session(port: int | None = None) -> requests.Session:
    """Return a ``requests.Session`` with ``qsar_api_base_url`` set after http/https discovery."""
    p = int(port if port is not None else QSAR_TOOLBOX_PORT)
    session = requests.Session()
    session.trust_env = True
    session.headers.update(
        {
            "accept": "application/json, text/plain, */*",
            "User-Agent": "ChemDB-QSAR-Toolbox-ingest/1.5",
        }
    )
    # Local Kestrel often uses HTTPS with a self-signed cert; False = skip verify unless QSAR_VERIFY_SSL=true.
    session.verify = QSAR_VERIFY_SSL
    base = discover_qsar_api_base(session, p)
    session.qsar_api_base_url = base  # type: ignore[attr-defined]
    return session


def _session_base_url(session: requests.Session, override: str | None) -> str:
    if override is not None:
        return override if override.endswith("/") else override + "/"
    base = getattr(session, "qsar_api_base_url", None)
    if not base:
        raise ValueError("Session is missing qsar_api_base_url; create the session with create_qsar_session().")
    return str(base)


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    cache_dir: str,
    use_cache: bool = True,
) -> Any:
    cache_key = f"{method} {url} {json.dumps(params or {}, sort_keys=True)}"
    path = _cache_path(cache_dir, cache_key)
    if use_cache and path.exists():
        return json.loads(path.read_text(encoding="utf-8"))

    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(QSAR_MAX_RETRIES):
        try:
            _sleep_rate_limit()
            resp = session.request(method, url, params=params, timeout=QSAR_HTTP_TIMEOUT_S)
            if resp.status_code in (500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype and not (resp.text or "").strip().startswith(("{", "[")):
                raise RuntimeError(f"Non-JSON from {url}: {ctype} {resp.text[:200]!r}")
            data = resp.json()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            return data
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_err = exc
            logger.warning("QSAR request failed %s (attempt %s/%s): %s", url, attempt + 1, QSAR_MAX_RETRIES, exc)
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError(f"QSAR Toolbox request failed after {QSAR_MAX_RETRIES} attempts: {url}") from last_err


def assert_websuite_alive(session: requests.Session, base_url: str) -> str:
    """Verify WebSuite responds; return toolbox version string if available."""
    url = urljoin(base_url, "about/toolbox/version")
    try:
        _sleep_rate_limit()
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            raise ConnectionError(f"HTTP {r.status_code} from {url}: {r.text[:200]!r}")
        return (r.text or "").strip() or "unknown"
    except requests.RequestException as exc:
        port = _tcp_port_from_api_base(base_url)
        raise ConnectionError(
            f"QSAR Toolbox WebAPI not reachable at {base_url!r} (port {port}). "
            f"Start **Toolbox Server / WebSuite**, confirm **Listening on localhost**, and set QSAR_TOOLBOX_PORT. "
            f"If the error looked like BadStatusLine with binary data, use HTTPS (set QSAR_SCHEME=https or keep auto). "
            f"Original error: {exc}"
        ) from exc


def _tcp_port_from_api_base(base_url: str) -> int:
    """Extract TCP port from API base ``http://host:port/api/v6/``."""
    u = urlparse(base_url.rstrip("/"))
    return int(u.port) if u.port else QSAR_TOOLBOX_PORT


def _address_and_port_from_api_base(base_url: str) -> tuple[str, int]:
    """Build ``QSARToolbox`` ``address`` (``scheme://host``) and TCP port from a discovered API base URL."""
    u = urlparse((base_url or "").strip())
    scheme = (u.scheme or "http").strip()
    host = (u.hostname or "127.0.0.1").strip()
    port = int(u.port) if u.port else QSAR_TOOLBOX_PORT
    return f"{scheme}://{host}", port


@contextmanager
def _patched_requests_get_verify(verify_ssl: bool):
    """PyQSARToolbox calls bare ``requests.get``; ensure local HTTPS uses ``verify=...`` consistently."""

    orig = requests.get

    def _wrapped(url: str, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("verify", verify_ssl)
        return orig(url, **kwargs)

    requests.get = _wrapped  # type: ignore[method-assign]
    try:
        yield
    finally:
        requests.get = orig  # type: ignore[method-assign]


def install_dependencies() -> None:
    """Ensure PyQSARToolbox is importable; optionally ``pip install`` when ``QSAR_AUTO_INSTALL_PYQSARTOOLBOX=true``."""
    try:
        import pyqsartoolbox  # noqa: F401
    except ImportError:
        if not QSAR_AUTO_INSTALL_PYQSARTOOLBOX:
            logger.error(
                "PyQSARToolbox is not installed. Run: pip install %s "
                "(or set QSAR_AUTO_INSTALL_PYQSARTOOLBOX=true to auto-install).",
                QSAR_PYQSARTOOLBOX_GIT,
            )
            raise
        logger.info("Installing PyQSARToolbox from %s", QSAR_PYQSARTOOLBOX_GIT)
        subprocess.check_call([sys.executable, "-m", "pip", "install", QSAR_PYQSARTOOLBOX_GIT])


def _import_qsar_toolbox_class() -> Any:
    """Return ``QSARToolbox`` class, or a legacy misspelled export if present."""
    try:
        from pyqsartoolbox import QSARToolbox

        return QSARToolbox
    except ImportError:
        pass
    try:
        from pyqsartoolbox import QSARTooolbox  # type: ignore[attr-defined]

        logger.warning("Using legacy class name QSARTooolbox from pyqsartoolbox; prefer upstream QSARToolbox.")
        return QSARTooolbox
    except ImportError:
        return None


def connect_to_websuite(port: int | None = None) -> Any:
    """Connect via PyQSARToolbox, probing **https/http** like ``create_qsar_session`` so TLS-only ports work.

    Verifies ``webapi_version()`` and ``toolbox_version()`` after construction (``__init__`` already pings the server).
    """
    if QSAR_AUTO_INSTALL_PYQSARTOOLBOX:
        install_dependencies()
    cls = _import_qsar_toolbox_class()
    if cls is None:
        raise ImportError(
            "PyQSARToolbox is not installed. Run install_dependencies() or: pip install "
            + QSAR_PYQSARTOOLBOX_GIT
        )
    p = int(port if port is not None else QSAR_TOOLBOX_PORT)
    try:
        session = create_qsar_session(port=p)
        base_url = _session_base_url(session, None)
        address, p2 = _address_and_port_from_api_base(base_url)
        with _patched_requests_get_verify(QSAR_VERIFY_SSL):
            qs = cls(port=p2, address=address, timeout=QSAR_HTTP_TIMEOUT_S)
            qs.webapi_version(timeout=min(30, QSAR_HTTP_TIMEOUT_S))
            qs.toolbox_version(timeout=min(30, QSAR_HTTP_TIMEOUT_S))
    except (OSError, requests.exceptions.RequestException, ValueError) as exc:
        raise ConnectionError(
            f"Error: Could not connect to QSAR Toolbox WebSuite. Ensure it is running on port {p}."
        ) from exc
    return qs


def _hit_to_cas_string(hit: dict[str, Any], fallback_cas: str) -> str:
    cas_val = hit.get("Cas")
    if isinstance(cas_val, int):
        cas_str = str(cas_val)
        if len(cas_str) >= 3:
            return f"{cas_str[:-3]}-{cas_str[-3:-1]}-{cas_str[-1]}"
        return cas_str
    if cas_val is not None and str(cas_val).strip():
        return str(cas_val).strip()
    return fallback_cas


def get_toolbox_substances_dataframe(qs: Any, identifiers: list[str]) -> pd.DataFrame:
    """Resolve substances with ``search_CAS`` for each seed identifier (spec: ``get_all_substances(qs, identifiers)``)."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for cas_raw in tqdm(identifiers, desc="QSAR Toolbox search_CAS"):
        dashed = normalize_cas(cas_raw) or str(cas_raw).strip()
        n = _cas_to_int(dashed)
        if n is None:
            logger.debug("Skip non-numeric CAS seed: %r", cas_raw)
            _sleep_rate_limit()
            time.sleep(0.05)
            continue
        hits: Any = None
        try:
            hits = qs.search_CAS(n)
        except Exception as exc:
            logger.warning("search_CAS failed for %s: %s", dashed, exc)
        if not hits or not isinstance(hits, list):
            _sleep_rate_limit()
            time.sleep(0.1)
            continue
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            cid = hit.get("ChemId")
            if not cid or str(cid) in seen:
                continue
            seen.add(str(cid))
            names = hit.get("Names") or []
            name0 = names[0] if isinstance(names, list) and names else None
            rows.append(
                {
                    "uuid": cid,
                    "ec_number": hit.get("ECNumber"),
                    "cas_number": _hit_to_cas_string(hit, dashed),
                    "substance_name": name0 or dashed,
                    "registration_date": None,
                    "regulatory_pool": hit.get("SubstanceType"),
                    "infocard_url": None,
                }
            )
        _sleep_rate_limit()
        time.sleep(0.1)
    return pd.DataFrame(rows)


def _norm_leaf_str(val: Any) -> str | None:
    if val is None or isinstance(val, (dict, list)):
        return None
    s = str(val).strip()
    return s or None


def _ghs_structured_rows_from_tree(
    chem_id: str,
    ec_number: str | None,
    cas_number: str | None,
    data: Any,
) -> list[dict[str, Any]]:
    """Recursively collect dict leaves that look like IUCLID/GHS hazard fields."""

    rows: list[dict[str, Any]] = []

    def pick(d: dict[str, Any], *needles: str) -> str | None:
        for k, v in d.items():
            compact = str(k).lower().replace(" ", "").replace("_", "")
            if not any(n in compact for n in needles):
                continue
            s = _norm_leaf_str(v)
            if s:
                return s
        return None

    def visit(obj: Any) -> None:
        if isinstance(obj, dict):
            h_code = pick(obj, "hazardstatementcode", "hstatementcode", "hcode")
            h_text = pick(obj, "hazardstatementtext", "hstatementtext", "statementtext")
            h_class = pick(obj, "hazardclass", "ghshazardclass", "signalword")
            h_cat = pick(obj, "hazardcategory", "hazardcat", "categorycode")
            if h_code or h_text or h_class or h_cat:
                rows.append(
                    {
                        "substance_uuid": chem_id,
                        "ec_number": ec_number,
                        "cas_number": cas_number,
                        "hazard_class": h_class,
                        "hazard_category": h_cat,
                        "h_statement_code": h_code,
                        "h_statement_text": h_text,
                        "supplemental_info": "structured_ghs_walk",
                    }
                )
            for v in obj.values():
                visit(v)
        elif isinstance(obj, list):
            for it in obj:
                visit(it)

    visit(data)
    return rows


def get_classifications(qs: Any, chem_id: str, ec_number: str | None = None, cas_number: str | None = None) -> list[dict[str, Any]]:
    """``get_all_endpoint_data`` + structured GHS walk + regex flattening (same spirit as ``/data/all``)."""
    try:
        data = qs.get_all_endpoint_data(str(chem_id).strip(), includeMetadata=False)
    except Exception as exc:
        logger.warning("get_all_endpoint_data failed chem_id=%s: %s", chem_id, exc)
        return []
    structured = _ghs_structured_rows_from_tree(str(chem_id), ec_number, cas_number, data)
    legacy = _endpoint_data_to_classification_rows(str(chem_id), ec_number, cas_number, data)
    return structured + legacy


def _dedupe_classification_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    df = pd.DataFrame(rows)
    keys = [c for c in df.columns if c in ("substance_uuid", "h_statement_code", "h_statement_text", "hazard_class", "hazard_category")]
    if keys:
        df = df.drop_duplicates(subset=keys)
    else:
        df = df.drop_duplicates()
    return df.to_dict(orient="records")


def build_dataframes(qs: Any, identifiers: list[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build ``(substances_df, cl_hazards_df)`` in the same shapes as ``get_all_substances`` + ``get_all_classifications``."""
    if identifiers is None:
        seed_path = _default_seed_path()
        if not seed_path:
            raise FileNotFoundError(
                "No CAS seed file. Pass ``identifiers=[...]`` or set QSAR_SUBSTANCE_SEED_PATH / add DSS/cas_dtxsid_mapping.csv."
            )
        identifiers = _load_cas_seed(seed_path)
        logger.info("QSAR Toolbox (PyQSAR): %s identifiers from %s", len(identifiers), seed_path.resolve())

    reg = get_toolbox_substances_dataframe(qs, identifiers)
    records: list[dict[str, Any]] = []
    for row in tqdm(reg.to_dict(orient="records"), total=len(reg), desc="QSAR Toolbox get_all_endpoint_data"):
        cid = row.get("uuid")
        if not cid:
            continue
        try:
            ec = str(row.get("ec_number") or "").strip() or None
            cas = str(row.get("cas_number") or "").strip() or None
            records.extend(get_classifications(qs, str(cid), ec_number=ec, cas_number=cas))
        except Exception as exc:
            logger.warning("Classification fetch failed for %s: %s", cid, exc)
    cl = pd.DataFrame(_dedupe_classification_rows(records))
    return reg, cl


def _qsar_disk_snapshot_paths() -> tuple[Path, Path]:
    base = Path(QSAR_CACHE_DIR)
    return base / "qsar_toolbox_substances_cache.csv", base / "qsar_toolbox_cl_hazards_cache.csv"


def _load_disk_snapshots() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    p_sub, p_cl = _qsar_disk_snapshot_paths()
    if not (p_sub.is_file() and p_cl.is_file()):
        return None
    return pd.read_csv(p_sub), pd.read_csv(p_cl)


def _write_disk_snapshots(reg: pd.DataFrame, cl: pd.DataFrame) -> None:
    p_sub, p_cl = _qsar_disk_snapshot_paths()
    p_sub.parent.mkdir(parents=True, exist_ok=True)
    reg.to_csv(p_sub, index=False)
    cl.to_csv(p_cl, index=False)
    logger.info("Wrote QSAR disk snapshots: %s, %s", p_sub, p_cl)


def _load_via_pyqsartoolbox(
    *,
    port: int | None,
    identifiers: list[str] | None,
    use_cache: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    qs = connect_to_websuite(port=port)
    with _patched_requests_get_verify(QSAR_VERIFY_SSL):
        reg, cl = build_dataframes(qs, identifiers=identifiers)
    if use_cache:
        _write_disk_snapshots(reg, cl)
    return reg, cl


def _load_via_requests_session(use_cache: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    session = create_qsar_session()
    base_url = _session_base_url(session, None)
    reg = get_all_substances(session, QSAR_PAGE_SIZE, QSAR_CACHE_DIR, base_url=base_url)
    cl = get_all_classifications(session, reg, QSAR_PAGE_SIZE, QSAR_CACHE_DIR, base_url=base_url)
    if not use_cache:
        logger.info("use_cache=False ignored for QSAR HTTP loader (per-request cache still avoids duplicate URLs).")
    return reg, cl


def _try_bulk_substances(session: requests.Session, base_url: str, page: int, size: int, cache_dir: str) -> list[dict[str, Any]] | None:
    """Try undocumented bulk list endpoints; return rows or None if not supported."""
    _ = cache_dir  # reserved for future HTTP response caching on these probes
    candidates = [
        ("GET", urljoin(base_url, "substances"), {"page": page, "size": size}),
        ("GET", urljoin(base_url, "substances"), {"page": page + 1, "pageSize": size}),
        ("GET", urljoin(base_url, "chemicals"), {"page": page, "size": size}),
        ("GET", urljoin(base_url, "search/substances"), {"offset": page * size, "limit": size}),
    ]
    for method, url, params in candidates:
        try:
            if method == "GET":
                _sleep_rate_limit()
                r = session.get(url, params=params, timeout=15)
                if r.status_code != 200:
                    continue
                data = r.json()
            else:
                continue
            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("items") or data.get("results") or data.get("data") or data.get("content")
            if isinstance(items, list) and items:
                logger.info("Using bulk substance endpoint: %s params=%s", url, params)
                return [x for x in items if isinstance(x, dict)]
        except Exception:
            continue
    return None


def _default_seed_path() -> Path | None:
    if QSAR_SUBSTANCE_SEED_PATH:
        p = Path(QSAR_SUBSTANCE_SEED_PATH)
        return p if p.is_file() else None
    for candidate in (Path("DSS") / "cas_dtxsid_mapping.csv", Path("cas_list.txt")):
        if candidate.is_file():
            return candidate
    return None


def _load_cas_seed(path: Path) -> list[str]:
    """Load CAS strings from a text file (one per line) or CSV."""
    if path.suffix.lower() in (".csv", ".tsv"):
        df = pd.read_csv(path, nrows=None)
        col = QSAR_SEED_CAS_COLUMN
        if col and col in df.columns:
            series = df[col]
        else:
            # First column that looks like CAS
            for c in df.columns:
                if "cas" in str(c).lower():
                    series = df[c]
                    break
            else:
                series = df.iloc[:, 0]
        out = [str(x).strip() for x in series.tolist() if str(x).strip() and str(x).lower() != "nan"]
        return out
    text = path.read_text(encoding="utf-8", errors="replace")
    return [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]


def _cas_to_int(cas: str) -> int | None:
    s = (cas or "").strip().replace("-", "")
    return int(s) if s.isdigit() else None


def _search_cas_row(session: requests.Session, base_url: str, cas: str, cache_dir: str) -> dict[str, Any] | None:
    n = _cas_to_int(cas)
    if n is None:
        return None
    url = urljoin(base_url, f"search/cas/{n}/true")
    try:
        data = _request_json(session, "GET", url, cache_dir=cache_dir)
    except Exception:
        return None
    if isinstance(data, list) and data:
        hit = data[0]
        return hit if isinstance(hit, dict) else None
    return None


def get_all_substances(
    session: requests.Session,
    page_size: int,
    cache_dir: str,
    *,
    base_url: str | None = None,
) -> pd.DataFrame:
    """Resolve substances via WebAPI ``search/cas`` over a seed list (paged), or bulk if available."""
    if base_url is None:
        base_url = _session_base_url(session, None)
    assert_websuite_alive(session, base_url)

    rows: list[dict[str, Any]] = []
    page = 0
    bulk = _try_bulk_substances(session, base_url, page=0, size=min(page_size, 10), cache_dir=cache_dir)
    if bulk is not None:
        max_pages = int(os.getenv("QSAR_BULK_MAX_PAGES", "10000"))
        with tqdm(desc="QSAR bulk substances", unit="page") as pbar:
            while page < max_pages:
                chunk = _try_bulk_substances(session, base_url, page=page, size=page_size, cache_dir=cache_dir)
                if not chunk:
                    break
                for it in chunk:
                    rows.append(
                        {
                            "uuid": it.get("ChemId") or it.get("chemId") or it.get("id"),
                            "ec_number": it.get("ECNumber") or it.get("ec_number") or it.get("EC"),
                            "cas_number": it.get("Cas") or it.get("cas") or it.get("CAS"),
                            "substance_name": (it.get("Names") or [None])[0] if isinstance(it.get("Names"), list) else it.get("Name"),
                            "registration_date": it.get("RegistrationDate") or it.get("createdOn"),
                            "regulatory_pool": it.get("SubstanceType") or it.get("substanceType"),
                            "infocard_url": None,
                        }
                    )
                page += 1
                pbar.update(1)
                if len(chunk) < page_size:
                    break
        return pd.DataFrame(rows)

    seed_path = _default_seed_path()
    if not seed_path:
        raise FileNotFoundError(
            "No bulk /substances API found and no CAS seed file. Set QSAR_SUBSTANCE_SEED_PATH to a "
            "text file (one CAS per line) or CSV with CAS column, or add DSS/cas_dtxsid_mapping.csv "
            "under the working directory."
        )

    cas_list = _load_cas_seed(seed_path)
    logger.info("QSAR substance load: %s CAS entries from %s", len(cas_list), seed_path.resolve())

    for i in tqdm(range(0, len(cas_list), page_size), desc="QSAR search/cas pages", unit="page"):
        chunk = cas_list[i : i + page_size]
        for cas_raw in chunk:
            dashed = normalize_cas(cas_raw) or cas_raw.strip()
            hit = _search_cas_row(session, base_url, dashed, cache_dir)
            if not hit:
                continue
            names = hit.get("Names") or []
            name0 = names[0] if isinstance(names, list) and names else None
            cas_val = hit.get("Cas")
            if isinstance(cas_val, int):
                cas_str = str(cas_val)
                if len(cas_str) >= 3:
                    cas_str = f"{cas_str[:-3]}-{cas_str[-3:-1]}-{cas_str[-1]}"
            else:
                cas_str = str(cas_val) if cas_val is not None else dashed
            rows.append(
                {
                    "uuid": hit.get("ChemId"),
                    "ec_number": hit.get("ECNumber"),
                    "cas_number": cas_str,
                    "substance_name": name0 or dashed,
                    "registration_date": None,
                    "regulatory_pool": hit.get("SubstanceType"),
                    "infocard_url": None,
                }
            )
    return pd.DataFrame(rows)


def _flatten_for_ghs(obj: Any, path: str = "", out: list[tuple[str, Any]] | None = None) -> list[tuple[str, Any]]:
    if out is None:
        out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}#{k}" if path else str(k)
            if isinstance(v, (dict, list)):
                _flatten_for_ghs(v, p, out)
            else:
                out.append((p, v))
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                _flatten_for_ghs(item, path, out)
            else:
                out.append((path, item))
    return out


def _endpoint_data_to_classification_rows(
    chem_id: str,
    ec_number: str | None,
    cas_number: str | None,
    data: Any,
) -> list[dict[str, Any]]:
    """Turn ``/data/all/{chem_id}`` JSON into flat hazard-ish rows for ``echa_loader``."""
    rows: list[dict[str, Any]] = []
    flat = _flatten_for_ghs(data)
    h_codes: set[str] = set()
    for path, val in flat:
        if not isinstance(val, str):
            continue
        path_l = (path or "").lower()
        if "ghs" in path_l or "hazard" in path_l or "classification" in path_l or "h_statement" in path_l:
            for m in re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", val):
                h_codes.add(m)
            if val.strip():
                rows.append(
                    {
                        "substance_uuid": chem_id,
                        "ec_number": ec_number,
                        "cas_number": cas_number,
                        "hazard_class": None,
                        "hazard_category": None,
                        "h_statement_code": None,
                        "h_statement_text": val.strip()[:4000],
                        "supplemental_info": path[:500],
                    }
                )
    for code in sorted(h_codes):
        rows.append(
            {
                "substance_uuid": chem_id,
                "ec_number": ec_number,
                "cas_number": cas_number,
                "hazard_class": None,
                "hazard_category": None,
                "h_statement_code": code,
                "h_statement_text": None,
                "supplemental_info": "regex_from_data_all",
            }
        )
    if not rows and isinstance(data, (dict, list)):
        blob = json.dumps(data, ensure_ascii=False)
        for m in re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", blob):
            rows.append(
                {
                    "substance_uuid": chem_id,
                    "ec_number": ec_number,
                    "cas_number": cas_number,
                    "hazard_class": None,
                    "hazard_category": None,
                    "h_statement_code": m,
                    "h_statement_text": None,
                    "supplemental_info": "regex_blob_data_all",
                }
            )
    return rows


def _fetch_classifications_for_row(
    session: requests.Session,
    base_url: str,
    cache_dir: str,
    row: dict[str, Any],
) -> list[dict[str, Any]]:
    chem_id = row.get("uuid")
    if not chem_id:
        return []
    url = urljoin(base_url, f"data/all/{str(chem_id).strip()}")
    params = {"includeMetadata": "false"}
    try:
        data = _request_json(session, "GET", url, params=params, cache_dir=cache_dir)
    except Exception as exc:
        logger.debug("data/all failed for %s: %s", chem_id, exc)
        return []
    return _endpoint_data_to_classification_rows(
        str(chem_id),
        str(row.get("ec_number") or "") or None,
        str(row.get("cas_number") or "") or None,
        data,
    )


def get_all_classifications(
    session: requests.Session,
    substances_df: pd.DataFrame,
    page_size: int,
    cache_dir: str,
    *,
    base_url: str | None = None,
) -> pd.DataFrame:
    """Pull ``GET /data/all/{chem_id}`` per substance (parallel) and flatten GHS-like signals.

    ``page_size`` is reserved for future batching; workers are capped by ``QSAR_CLASSIFICATION_MAX_WORKERS``.
    """
    _ = page_size
    if base_url is None:
        base_url = _session_base_url(session, None)
    assert_websuite_alive(session, base_url)

    records: list[dict[str, Any]] = []
    rows_dicts = substances_df.to_dict(orient="records")
    workers = max(1, QSAR_CLASSIFICATION_MAX_WORKERS)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_fetch_classifications_for_row, session, base_url, cache_dir, r): r.get("uuid")
            for r in rows_dicts
            if r.get("uuid")
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc="QSAR data/all (CLP/GHS)"):
            try:
                records.extend(fut.result())
            except Exception as exc:
                logger.warning("classification worker failed: %s", exc)

    return pd.DataFrame(records)


def to_echa_loader_frames(reg: pd.DataFrame, cl: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Align column names with ``ingest/echa_loader._upsert_substances`` / ``_load_cl_hazards``."""
    reg2 = reg.copy()
    if "cas_number" in reg2.columns:
        reg2["cas_rn"] = reg2["cas_number"].map(lambda x: normalize_cas(str(x)) if x is not None and str(x) != "nan" else None)
    elif "cas_rn" not in reg2.columns:
        reg2["cas_rn"] = None
    if "substance_name" not in reg2.columns and "name" in reg2.columns:
        reg2["substance_name"] = reg2["name"]

    cl2 = cl.copy()
    if "cas_number" in cl2.columns:
        cl2["cas_rn"] = cl2["cas_number"].map(lambda x: normalize_cas(str(x)) if x is not None and str(x) != "nan" else None)
    elif "cas_rn" not in cl2.columns:
        cl2["cas_rn"] = None
    if "h_statement_code" in cl2.columns:
        cl2 = cl2.rename(columns={"h_statement_code": "hazard_code"})
    if "h_statement_text" in cl2.columns:
        cl2 = cl2.rename(columns={"h_statement_text": "classification"})
    if "hazard_code" not in cl2.columns:
        cl2["hazard_code"] = None
    if "classification" not in cl2.columns:
        cl2["classification"] = None
    if "source_reference" not in cl2.columns:
        cl2["source_reference"] = "QSAR Toolbox WebSuite (data/all)"
    return reg2, cl2


def load_echa_from_qsar_toolbox(
    use_cache: bool = True,
    *,
    port: int | None = None,
    identifiers: list[str] | None = None,
    reuse_snapshot: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Orchestrate substance + classification loads; returns two DataFrames (registered + C&L-style).

    Uses **PyQSARToolbox** when available (``QSAR_USE_PYQSARTOOLBOX``), including correct **https/http**
    discovery and TLS ``verify`` handling for Toolbox 4.7+. Falls back to raw ``requests`` if the
    library is missing or connection fails.

    ``use_cache``: when true, writes CSV snapshots under ``QSAR_CACHE_DIR`` after a successful fetch.
    ``reuse_snapshot``: when true, loads those CSV snapshots if both exist (skips the network).
    """
    if reuse_snapshot:
        snap = _load_disk_snapshots()
        if snap is not None:
            logger.info("Loaded QSAR disk snapshots from %s", QSAR_CACHE_DIR)
            return snap

    if QSAR_USE_PYQSARTOOLBOX:
        try:
            return _load_via_pyqsartoolbox(port=port, identifiers=identifiers, use_cache=use_cache)
        except ImportError as exc:
            logger.warning("PyQSARToolbox path unavailable (%s); falling back to HTTP session loader.", exc)
        except ConnectionError as exc:
            logger.warning("PyQSARToolbox connection failed (%s); falling back to HTTP session loader.", exc)
        except Exception as exc:  # pragma: no cover
            logger.warning("PyQSARToolbox loader error (%s); falling back to HTTP session loader.", exc)

    reg, cl = _load_via_requests_session(use_cache=use_cache)
    if use_cache:
        _write_disk_snapshots(reg, cl)
    return reg, cl
