"""Offline ECHA / IUCLID ingestion: REACH bulk archives (``.zip`` / ``.7z`` → ``.i6z``) and optional CHEM HTML.

**Academic use:** download and process locally; do **not** redistribute raw ECHA/IUCLID files.

**Local file workflow (recommended):** After you **manually download** REACH Study Results from IUCLID
(terms accepted in the browser), set ``OFFLINE_LOCAL_ARCHIVE`` to the path of that ``.zip`` / ``.7z``, or
to a folder that already contains extracted ``.i6z`` files (or a single dossier folder for quick tests).
The loader **does not re-download** that path. Extraction goes under ``OFFLINE_DATA_DIR/extracted/…`` and is
**reused** when a sidecar marker matches the archive file mtime. GHS / CLP-like rows are read from
``Document.i6d`` XML when present; set ``OFFLINE_SCRAPE_CL=true`` only if you still need ECHA CHEM scraping
for substances **without** in-dossier classification (slow; CHEM is often SPA-only).

**REACH Study Results companion XLSX:** ECHA publishes a ``reach_study_results-dossier_info_*.xlsx`` workbook
alongside the dossier ZIP. Study-result ``.i6z`` files often omit CAS/EC/name in ``Document.i6d``; the loader
auto-merges that spreadsheet when it sits in the **same folder** as ``OFFLINE_LOCAL_ARCHIVE``, or when you set
``OFFLINE_DOSSIER_INFO_XLSX`` (optional ``OFFLINE_DOSSIER_INFO_SHEET``).

**Remote download (optional):** set ``REACH_STUDY_RESULTS_URL`` or rely on index scraping; disclaimer POST
env vars match ``iuclid_api_loader`` if an interstitial appears.

**Dependencies:** ``.7z`` needs **7-Zip** on PATH or ``pip install py7zr``. ``.zip`` uses stdlib ``zipfile``.
Optional: ``pip install lxml`` for CHEM HTML tables (often empty; see below).

**i6z parse parallelism:** ``OFFLINE_I6Z_USE_MP`` (default true), ``OFFLINE_I6Z_MAX_WORKERS`` (empty = up to
``min(cpu_count(), 32)``; ``1`` = sequential), ``OFFLINE_I6Z_MIN_FILES_FOR_MP`` (default 4; below this, sequential).

**Network behaviour:** ``create_offline_session()`` defaults to **cloudscraper**. **ECHA CHEM** is an
Angular SPA; ``fetch_cl_from_echa_chem`` usually returns no rows—prefer **IUCLID XML** GHS extraction.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import pandas as pd
import requests
from tqdm import tqdm

from ingest.crosswalk import normalize_cas

logger = logging.getLogger(__name__)

# Public ECHA bulk download (same as ``ingest.echa_loader``); inlined so offline loader does not
# depend on the full online ingestion module tree on minimal deployments (e.g. Streamlit Cloud).
ECHA_REGISTERED_URL = "https://echa.europa.eu/documents/10162/13634/registered_substances.csv"

OFFLINE_DATA_DIR = Path(os.getenv("OFFLINE_DATA_DIR", "data/offline"))
OFFLINE_CACHE_DIR = Path(os.getenv("OFFLINE_CACHE_DIR", "data/offline_cache"))
REACH_STUDY_RESULTS_URL = os.getenv("REACH_STUDY_RESULTS_URL", "").strip()
REACH_STUDY_INDEX_URL = os.getenv(
    "REACH_STUDY_INDEX_URL",
    "https://iuclid6.echa.europa.eu/reach-study-results",
).strip()
DISCLAIMER_URL = os.getenv("DISCLAIMER_URL", "").strip() or os.getenv("IUCLID_ACCEPT_POST_URL", "").strip()
DISCLAIMER_POST_DATA_RAW = os.getenv("DISCLAIMER_POST_DATA", "").strip() or os.getenv("IUCLID_ACCEPT_POST_BODY", "").strip()
OFFLINE_CHEM_DELAY_S = float(os.getenv("OFFLINE_CHEM_DELAY_S", "1.5"))
OFFLINE_CHEM_MAX_WORKERS = int(os.getenv("OFFLINE_CHEM_MAX_WORKERS", "5"))
OFFLINE_HTTP_TIMEOUT_S = int(os.getenv("OFFLINE_HTTP_TIMEOUT_S", "120"))
CHEM_BASE = os.getenv("ECHA_CHEM_BASE", "https://chem.echa.europa.eu").rstrip("/")
OFFLINE_USE_CLOUDSCRAPER = os.getenv("OFFLINE_USE_CLOUDSCRAPER", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
OFFLINE_LOCAL_ARCHIVE = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
OFFLINE_SCRAPE_CL = os.getenv("OFFLINE_SCRAPE_CL", "false").strip().lower() in ("1", "true", "yes", "on")
OFFLINE_LARGE_I6Z_WARN = int(os.getenv("OFFLINE_LARGE_I6Z_WARN", "10000"))

_SESSION_HEADERS = {
    "User-Agent": "ChemDB-offline-echa-research/1.0 (academic; contact local lab)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
}

_CHEM_SPA_WARNED = False


def create_offline_session() -> requests.Session:
    """Session for ECHA/IUCLID/CHEM fetches; prefers **cloudscraper** (do not override its browser headers)."""
    session: requests.Session
    used_cloud = False
    if OFFLINE_USE_CLOUDSCRAPER:
        try:
            import cloudscraper

            session = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False},
            )
            used_cloud = True
        except ImportError:
            logger.warning("OFFLINE_USE_CLOUDSCRAPER=true but cloudscraper missing; pip install cloudscraper")
            session = requests.Session()
    else:
        session = requests.Session()
    session.trust_env = True
    session.headers.setdefault("Accept-Language", "en")
    if not used_cloud:
        session.headers.setdefault("Accept", _SESSION_HEADERS["Accept"])
        session.headers.setdefault("User-Agent", _SESSION_HEADERS["User-Agent"])
    return session


def _registered_csv_urls() -> list[str]:
    raw = os.getenv("OFFLINE_REGISTERED_CSV_URLS", "").strip()
    urls: list[str] = [ECHA_REGISTERED_URL]
    if raw:
        urls.extend(u.strip() for u in raw.split(",") if u.strip())
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def download_registered_substances_csv(session: requests.Session, destination: Path) -> bool:
    """Try several known CSV endpoints (ECHA occasionally moves paths)."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    for url in _registered_csv_urls():
        try:
            r = session.get(url, timeout=OFFLINE_HTTP_TIMEOUT_S)
            if r.status_code != 200 or len(r.content) < 500:
                continue
            head = r.content[:8000]
            if b"EC" not in head and b"ec" not in head.lower() and b"CAS" not in head:
                continue
            destination.write_bytes(r.content)
            logger.info("Registered substances CSV saved from %s -> %s", url, destination)
            return True
        except requests.RequestException as exc:
            logger.debug("Registered CSV %s: %s", url, exc)
    logger.warning("Registered substances CSV not downloaded from any URL (set OFFLINE_REGISTERED_CSV_URLS).")
    return False


def _tag_local(tag: str | None) -> str:
    if not tag:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _norm_xml_text(el: Any) -> str:
    if el is None or el.text is None:
        return ""
    return str(el.text).strip()


def _ghs_rows_from_i6d_root(root: Any, *, substance_uuid: str, ec_number: str | None, cas_number: str | None) -> list[dict[str, Any]]:
    """Collect GHS / CLP-like rows from IUCLID XML (namespace-agnostic, heuristic)."""
    import xml.etree.ElementTree as ET

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None, str | None, str | None]] = set()

    def add_row(
        h_class: str | None,
        h_cat: str | None,
        h_code: str | None,
        h_text: str | None,
        sup: str,
    ) -> None:
        key = (h_class, h_cat, h_code, h_text)
        if key in seen:
            return
        seen.add(key)
        if not (h_class or h_cat or h_code or h_text):
            return
        rows.append(
            {
                "substance_uuid": substance_uuid,
                "ec_number": ec_number,
                "cas_number": cas_number,
                "hazard_class": h_class,
                "hazard_category": h_cat,
                "h_statement_code": h_code,
                "h_statement_text": (h_text or "")[:4000] or None,
                "supplemental_info": sup,
            }
        )

    for el in root.iter():
        loc_raw = _tag_local(el.tag)
        loc = loc_raw.lower().replace(" ", "").replace("_", "") if loc_raw else ""
        if not loc:
            continue
        if any(
            k in loc
            for k in (
                "ghsclassification",
                "classificationandlabelling",
                "clpclassification",
                "hazardclassification",
            )
        ):
            try:
                blob = ET.tostring(el, encoding="unicode", method="xml")
            except Exception:
                blob = ""
            for code in sorted(set(re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", blob))):
                add_row(None, None, code, None, "i6d_ghs_classification_blob")
        if "hazardclass" in loc and "category" not in loc:
            t = _norm_xml_text(el)
            if t:
                add_row(t, None, None, None, "i6d_hazard_class")
        if "hazardcategory" in loc or ("hazard" in loc and "category" in loc):
            t = _norm_xml_text(el)
            if t and len(t) < 200:
                add_row(None, t, None, None, "i6d_hazard_category")
        if "hazardstatementcode" in loc or loc == "hcode":
            t = _norm_xml_text(el)
            if t:
                add_row(None, None, t, None, "i6d_hazard_statement_code")
        if "hazardstatementtext" in loc or ("hazardstatement" in loc and "text" in loc and "code" not in loc):
            t = _norm_xml_text(el)
            if t:
                add_row(None, None, None, t, "i6d_hazard_statement_text")

    try:
        whole = ET.tostring(root, encoding="unicode", method="xml")
    except Exception:
        whole = ""
    for code in sorted(set(re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", whole))):
        add_row(None, None, code, None, "i6d_xml_regex_h")

    return rows


def _parse_i6d_xml(xml_bytes: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse ``Document.i6d``: substance fields + optional GHS rows (from XML, not CHEM)."""
    import xml.etree.ElementTree as ET

    out: dict[str, Any] = {
        "substance_name": None,
        "ec_number": None,
        "cas_number": None,
        "registration_date": None,
        "regulatory_pool": None,
    }
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        logger.debug("XML parse error: %s", exc)
        return out, []

    name_candidates = ("SubstanceName", "Name", "ChemicalName", "PreferredName", "InternationalChemicalName")
    ec_names = ("ECNumber", "ECListingNumber", "EuropeanCommunityNumber")
    cas_names = ("CASNumber", "CASRN", "CASNo", "Number")
    reg_names = ("RegistrationDate", "LastModified", "SubmissionDate")
    pool_names = ("RegulatoryProgramme", "RegulatoryPool", "SubstanceType")

    for el in root.iter():
        loc = _tag_local(el.tag)
        text = (el.text or "").strip() if el.text else ""
        if not text and not list(el):
            continue
        if loc in name_candidates and text and not out["substance_name"]:
            out["substance_name"] = text
        elif loc in ec_names and text and not out["ec_number"]:
            out["ec_number"] = text
        elif loc in cas_names and text and re.search(r"\d", text) and not out["cas_number"]:
            if "EC" not in text.upper() or re.search(r"\d{2,3}-\d{2,3}-\d", text):
                out["cas_number"] = text
        elif loc in reg_names and text and not out["registration_date"]:
            out["registration_date"] = text
        elif loc in pool_names and text and not out["regulatory_pool"]:
            out["regulatory_pool"] = text

    ghs = _ghs_rows_from_i6d_root(
        root,
        substance_uuid="",
        ec_number=str(out.get("ec_number") or "").strip() or None,
        cas_number=str(out.get("cas_number") or "").strip() or None,
    )
    return out, ghs


def _robots_crawl_delay(session: requests.Session, base: str) -> float:
    try:
        r = session.get(urljoin(base, "/robots.txt"), timeout=15)
        if r.status_code != 200:
            return OFFLINE_CHEM_DELAY_S
        for ln in (r.text or "").splitlines():
            if ln.lower().startswith("crawl-delay:"):
                try:
                    return max(float(ln.split(":", 1)[1].strip()), OFFLINE_CHEM_DELAY_S)
                except ValueError:
                    break
    except Exception as exc:
        logger.debug("robots.txt fetch skipped: %s", exc)
    return OFFLINE_CHEM_DELAY_S


def download_with_disclaimer(
    session: requests.Session,
    url: str,
    destination: Path,
    *,
    disclaimer_url: str | None = None,
    post_data: dict[str, Any] | None = None,
) -> bool:
    """GET optional disclaimer page, POST acceptance if ``post_data`` / parsed CSRF, then stream-download ``url``."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    merged_post: dict[str, Any] = dict(post_data or {})
    if DISCLAIMER_POST_DATA_RAW and not merged_post:
        try:
            merged_post = json.loads(DISCLAIMER_POST_DATA_RAW)
        except json.JSONDecodeError:
            logger.warning("DISCLAIMER_POST_DATA is not valid JSON; ignoring.")

    du = (disclaimer_url or DISCLAIMER_URL or "").strip()
    if du:
        try:
            g = session.get(du, timeout=OFFLINE_HTTP_TIMEOUT_S)
            logger.info("Disclaimer page GET %s -> HTTP %s", du, g.status_code)
            html = g.text or ""
            for pat in (
                r'name=["\']_csrf["\']\s+value=["\']([^"\']+)["\']',
                r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']',
                r'name=["\']csrf_token["\']\s+value=["\']([^"\']+)["\']',
            ):
                m = re.search(pat, html, re.I)
                if m and "csrf" not in {k.lower() for k in merged_post}:
                    merged_post["_csrf"] = m.group(1)
                    merged_post["csrf_token"] = m.group(1)
                    break
            if merged_post:
                session.post(du, data=merged_post, timeout=OFFLINE_HTTP_TIMEOUT_S)
        except requests.RequestException as exc:
            logger.warning("Disclaimer flow failed (%s); continuing with direct download.", exc)

    try:
        resp = session.get(url, stream=True, timeout=OFFLINE_HTTP_TIMEOUT_S)
        if resp.status_code != 200:
            logger.error("Download failed HTTP %s for %s", resp.status_code, url)
            return False
        total = int(resp.headers.get("Content-Length") or 0)
        nread = 0
        with destination.open("wb") as fh, tqdm(
            desc=destination.name,
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    fh.write(chunk)
                    nread += len(chunk)
                    if total:
                        bar.update(len(chunk))
                    else:
                        bar.update(len(chunk))
        logger.info("Saved %s (%s bytes)", destination, nread)
        return True
    except requests.RequestException as exc:
        logger.error("Download error for %s: %s", url, exc)
        return False


def _reach_index_urls(primary: str | None) -> list[str]:
    raw = os.getenv("OFFLINE_REACH_INDEX_URLS", "").strip()
    urls: list[str] = []
    if raw:
        urls.extend(u.strip() for u in raw.split(",") if u.strip())
    p = (primary or REACH_STUDY_INDEX_URL).strip()
    if p:
        urls.append(p)
    for extra in (
        "https://iuclid6.echa.europa.eu/rsr-dossiers",
        "https://iuclid6.echa.europa.eu/download",
    ):
        if extra not in urls:
            urls.append(extra)
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _pick_archive_link(text: str, page_url: str) -> str | None:
    hrefs = re.findall(r'href=["\']([^"\']+\.(?:7z|zip))["\']', text, re.I)
    reach = [
        h
        for h in hrefs
        if "reach" in h.lower() or "study" in h.lower() or "result" in h.lower() or "rsr" in h.lower() or "dossier" in h.lower()
    ]
    pool = reach or hrefs
    if not pool:
        raw_urls = re.findall(r"https?://[^\s\"'<>]+\.(?:7z|zip)", text, re.I)
        pool = [
            u
            for u in raw_urls
            if any(k in u.lower() for k in ("reach", "study", "result", "rsr", "iuclid", "dossier"))
        ] or raw_urls
    if not pool:
        return None
    link = pool[0]
    if link.startswith("//"):
        link = "https:" + link
    elif link.startswith("/"):
        u = urlparse(page_url)
        link = f"{u.scheme}://{u.netloc}{link}"
    return link


def get_latest_reach_study_url(session: requests.Session, index_url: str | None = None) -> str | None:
    """Scrape IUCLID pages for a ``*.7z`` / ``*.zip`` bulk link (may be empty if downloads are gated)."""
    for idx in _reach_index_urls(index_url):
        try:
            r = session.get(idx, timeout=OFFLINE_HTTP_TIMEOUT_S)
            if r.status_code != 200:
                logger.info("REACH index skip %s -> HTTP %s", idx, r.status_code)
                continue
            text = r.text or ""
        except requests.RequestException as exc:
            logger.info("REACH index skip %s: %s", idx, exc)
            continue
        link = _pick_archive_link(text, idx)
        if link:
            logger.info("Picked archive link from %s -> %s", idx, link[:120])
            return link
    logger.warning(
        "No .7z/.zip link found on IUCLID index pages (downloads may require login). "
        "Set REACH_STUDY_RESULTS_URL to the file URL from a browser session."
    )
    return None


def _extract_7z(archive: Path, out_dir: Path) -> bool:
    out_dir.mkdir(parents=True, exist_ok=True)
    import shutil

    exe = shutil.which("7z") or shutil.which("7z.exe")
    if exe:
        try:
            subprocess.run([exe, "x", str(archive), f"-o{out_dir}", "-y"], check=True, timeout=3600)
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as exc:
            logger.warning("7z CLI failed: %s", exc)
    try:
        import py7zr  # type: ignore[import-untyped]

        with py7zr.SevenZipFile(archive, mode="r") as z:
            z.extractall(path=out_dir)
        return True
    except ImportError:
        logger.error("Install 7-Zip (7z on PATH) or: pip install py7zr")
    except Exception as exc:
        logger.error("py7zr extract failed: %s", exc)
    return False


def _parse_one_i6z(i6z_path: Path) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    stem = i6z_path.stem
    row: dict[str, Any] = {
        "uuid": stem,
        "ec_number": None,
        "cas_number": None,
        "substance_name": None,
        "registration_date": None,
        "regulatory_pool": None,
        "infocard_url": f"https://echa.europa.eu/substance-information/-/substanceinfo/{stem}",
    }
    try:
        with zipfile.ZipFile(i6z_path, "r") as zf:
            names = zf.namelist()
            doc = next((n for n in names if n.lower().endswith("document.i6d")), None)
            if not doc:
                doc = next((n for n in names if n.lower().endswith(".i6d")), None)
            if not doc:
                logger.debug("No .i6d in %s", i6z_path)
                return row, []
            xml_bytes = zf.read(doc)
    except (zipfile.BadZipFile, OSError, KeyError) as exc:
        logger.warning("i6z read failed %s: %s", i6z_path, exc)
        return None, []

    meta, ghs = _parse_i6d_xml(xml_bytes)
    row.update({k: meta.get(k) or row.get(k) for k in meta})
    if row.get("cas_number"):
        row["cas_number"] = normalize_cas(str(row["cas_number"])) or row["cas_number"]
    ec = str(row.get("ec_number") or "").strip() or None
    cas = str(row.get("cas_number") or "").strip() or None
    for d in ghs:
        d["substance_uuid"] = stem
        d["ec_number"] = d.get("ec_number") or ec
        d["cas_number"] = d.get("cas_number") or cas
    return row, ghs


def _parse_one_i6z_mp_safe(path_str: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Pickle-friendly entry point for :class:`ProcessPoolExecutor` (Windows spawn)."""
    try:
        return _parse_one_i6z(Path(path_str))
    except Exception as exc:
        logger.warning("i6z worker failed %s: %s", path_str, exc)
        return None, []


def _collect_parsed_i6z_rows(i6z_files: list[Path], desc: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse many ``.i6z`` files; uses processes when configured and safe, else sequential."""
    rows: list[dict[str, Any]] = []
    cl_accum: list[dict[str, Any]] = []
    if not i6z_files:
        return rows, cl_accum

    use_mp = os.getenv("OFFLINE_I6Z_USE_MP", "true").strip().lower() in ("1", "true", "yes", "on")
    try:
        min_files_mp = int(os.getenv("OFFLINE_I6Z_MIN_FILES_FOR_MP", "4"))
    except ValueError:
        min_files_mp = 4

    def _sequential() -> None:
        for p in tqdm(i6z_files, desc=desc):
            one, ghs = _parse_one_i6z(p)
            if one:
                rows.append(one)
            cl_accum.extend(ghs)

    if not use_mp or len(i6z_files) < min_files_mp:
        _sequential()
        return rows, cl_accum

    raw_workers = os.getenv("OFFLINE_I6Z_MAX_WORKERS", "").strip()
    if raw_workers:
        try:
            configured = int(raw_workers)
        except ValueError:
            configured = os.cpu_count() or 4
    else:
        configured = os.cpu_count() or 4
    if configured <= 1:
        _sequential()
        return rows, cl_accum

    paths_str = [str(p.resolve()) for p in i6z_files]
    max_workers = max(1, min(configured, len(paths_str), 32))
    chunksize = max(1, len(paths_str) // (max_workers * 8) or 1)

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(
                tqdm(
                    executor.map(_parse_one_i6z_mp_safe, paths_str, chunksize=chunksize),
                    total=len(paths_str),
                    desc=desc,
                )
            )
        for one, ghs in results:
            if one:
                rows.append(one)
            cl_accum.extend(ghs)
    except Exception as exc:
        logger.warning("i6z multiprocessing failed (%s); falling back to sequential parse.", exc)
        rows.clear()
        cl_accum.clear()
        _sequential()
    return rows, cl_accum


def _extract_marker_path(extract_root: Path) -> Path:
    return extract_root / ".offline_extract_meta.json"


def _extract_reuse_ok(extract_root: Path, archive: Path) -> bool:
    if not extract_root.is_dir():
        return False
    if not _unique_i6z_paths(extract_root):
        return False
    mp = _extract_marker_path(extract_root)
    if not mp.is_file():
        return False
    try:
        m = json.loads(mp.read_text(encoding="utf-8"))
        return Path(m["archive"]).resolve() == archive.resolve() and abs(float(m["mtime"]) - archive.stat().st_mtime) < 0.01
    except Exception:
        return False


def _write_extract_marker(extract_root: Path, archive: Path) -> None:
    _extract_marker_path(extract_root).write_text(
        json.dumps({"archive": str(archive.resolve()), "mtime": archive.stat().st_mtime}),
        encoding="utf-8",
    )


def _extract_dir_for_archive(archive_file: Path, data_dir: Path) -> Path:
    key = hashlib.sha256(str(archive_file.resolve()).encode("utf-8")).hexdigest()[:16]
    return data_dir / "extracted" / f"{archive_file.stem}_{key}"


def _unique_i6z_paths(root: Path) -> list[Path]:
    """``.i6z`` / ``.I6Z`` listing without duplicates (Windows paths are case-insensitive)."""
    return sorted({*root.rglob("*.i6z"), *root.rglob("*.I6Z")}, key=lambda p: str(p.resolve()).lower())


def extract_i6z_metadata(
    archive_or_dir: Path,
    data_dir: Path,
    *,
    force_extract: bool = False,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Extract ``.7z`` / ``.zip`` (or scan a folder), parse each ``.i6z`` / ``Document.i6d``.

    Returns ``(substances_df, cl_rows_from_i6d)``. Reuses ``data_dir/extracted/<stem>_<hash>/`` when the
    sidecar marker matches the archive mtime (unless ``force_extract``).
    """
    rows: list[dict[str, Any]] = []
    cl_accum: list[dict[str, Any]] = []
    ap = Path(archive_or_dir)

    if ap.is_dir():
        i6z_files = _unique_i6z_paths(ap)
        if len(i6z_files) > OFFLINE_LARGE_I6Z_WARN:
            logger.warning(
                "Found %s .i6z files under %s; parsing may use significant memory. Consider a subset folder.",
                len(i6z_files),
                ap,
            )
        rdir, cdir = _collect_parsed_i6z_rows(i6z_files, "Parse i6z (directory)")
        rows.extend(rdir)
        cl_accum.extend(cdir)
        return pd.DataFrame(rows), cl_accum

    if not ap.is_file():
        logger.error("Archive missing: %s", ap)
        return pd.DataFrame(), []

    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    extract_root = _extract_dir_for_archive(ap, data_dir)
    extract_root.parent.mkdir(parents=True, exist_ok=True)
    reuse = (not force_extract) and _extract_reuse_ok(extract_root, ap)

    if not reuse:
        if extract_root.exists():
            shutil.rmtree(extract_root, ignore_errors=True)
        extract_root.mkdir(parents=True, exist_ok=True)
        if ap.suffix.lower() == ".7z":
            if not _extract_7z(ap, extract_root):
                return pd.DataFrame(), []
        elif ap.suffix.lower() == ".zip":
            try:
                with zipfile.ZipFile(ap, "r") as zf:
                    zf.extractall(extract_root)
            except zipfile.BadZipFile as exc:
                logger.error("Zip extract failed: %s", exc)
                return pd.DataFrame(), []
        else:
            logger.error("Unsupported archive type: %s", ap.suffix)
            return pd.DataFrame(), []
        _write_extract_marker(extract_root, ap)
    else:
        logger.info("Reusing extracted dossiers under %s (same archive mtime).", extract_root)

    i6z_files = _unique_i6z_paths(extract_root)
    if len(i6z_files) > OFFLINE_LARGE_I6Z_WARN:
        logger.warning(
            "Found %s .i6z files after extraction; parsing may use significant memory.",
            len(i6z_files),
        )
    rarc, carc = _collect_parsed_i6z_rows(i6z_files, "Parse i6z")
    rows.extend(rarc)
    cl_accum.extend(carc)
    return pd.DataFrame(rows), cl_accum


def _cache_key(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]
    return h


def fetch_cl_from_echa_chem(
    session: requests.Session,
    ec_number: str | None,
    cas_number: str | None,
    substance_name: str | None,
    cache_dir: Path,
    *,
    min_delay_s: float | None = None,
) -> list[dict[str, Any]]:
    """Scrape ECHA CHEM search + substance page for GHS-style rows (best-effort HTML).

    **Limitation:** CHEM serves an Angular app; search results and classifications load in the browser, so
    this function often returns **[]** unless ECHA exposes stable server-rendered links again.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    query = (ec_number or "").strip() or (cas_number or "").strip() or (substance_name or "").strip()
    if not query:
        return []

    delay = min_delay_s if min_delay_s is not None else OFFLINE_CHEM_DELAY_S
    time.sleep(delay)

    ck = _cache_key("chem", query)
    cache_html = cache_dir / f"chem_{ck}.html"
    if cache_html.is_file():
        html_text = cache_html.read_text(encoding="utf-8", errors="replace")
    else:
        search_url = f"{CHEM_BASE}/search?query={quote(query)}"
        try:
            r = session.get(search_url, timeout=OFFLINE_HTTP_TIMEOUT_S)
            html_text = r.text or ""
            cache_html.write_text(html_text, encoding="utf-8", errors="replace")
        except requests.RequestException as exc:
            logger.warning("CHEM search failed %s: %s", query, exc)
            return []

    out: list[dict[str, Any]] = []
    # Follow first plausible substance / information link
    links = re.findall(r'href=["\']([^"\']+)["\']', html_text, re.I)
    follow: str | None = None
    for raw in links:
        if "/substance/" in raw or "/substanceinfo/" in raw or "/information-on-chemicals/" in raw.lower():
            follow = raw
            break
    if follow and not follow.startswith("http"):
        follow = urljoin(CHEM_BASE + "/", follow)

    if follow:
        time.sleep(delay)
        sub_cache = cache_dir / f"chem_sub_{_cache_key(follow)}.html"
        if sub_cache.is_file():
            page = sub_cache.read_text(encoding="utf-8", errors="replace")
        else:
            try:
                r2 = session.get(follow, timeout=OFFLINE_HTTP_TIMEOUT_S)
                page = r2.text or ""
                sub_cache.write_text(page, encoding="utf-8", errors="replace")
            except requests.RequestException as exc:
                logger.warning("CHEM substance page failed %s: %s", follow, exc)
                page = html_text
    else:
        page = html_text

    # lxml path for tables
    try:
        from lxml import html as lhtml  # type: ignore[import-untyped]

        doc = lhtml.fromstring(page)
        for table in doc.xpath("//table[contains(@class,'classification') or contains(@class,'hazard')]"):
            for tr in table.xpath(".//tr"):
                cells = [re.sub(r"\s+", " ", (c.text_content() or "").strip()) for c in tr.xpath(".//th|.//td")]
                if len(cells) < 2:
                    continue
                joined = " | ".join(cells)
                codes = re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", joined)
                h_code = codes[0] if codes else None
                out.append(
                    {
                        "substance_uuid": None,
                        "ec_number": ec_number,
                        "cas_number": cas_number,
                        "hazard_class": cells[0] if cells else None,
                        "hazard_category": cells[1] if len(cells) > 1 else None,
                        "h_statement_code": h_code,
                        "h_statement_text": joined[:4000] if not h_code else None,
                        "supplemental_info": "echa_chem_table",
                    }
                )
    except ImportError:
        pass

    if not out:
        for m in re.finditer(
            r"(Flam\.|Ox\.|Toxic|Corrosive|Environmental|Hazard)\s*[^\n<]{0,120}",
            page,
            re.I,
        ):
            out.append(
                {
                    "substance_uuid": None,
                    "ec_number": ec_number,
                    "cas_number": cas_number,
                    "hazard_class": None,
                    "hazard_category": None,
                    "h_statement_code": None,
                    "h_statement_text": m.group(0).strip()[:2000],
                    "supplemental_info": "echa_chem_regex",
                }
            )
        for hc in sorted(set(re.findall(r"\bH\d{3}(?:\([^\]]+\))?\b", page))):
            out.append(
                {
                    "substance_uuid": None,
                    "ec_number": ec_number,
                    "cas_number": cas_number,
                    "hazard_class": None,
                    "hazard_category": None,
                    "h_statement_code": hc,
                    "h_statement_text": None,
                    "supplemental_info": "echa_chem_h_regex",
                }
            )

    global _CHEM_SPA_WARNED
    if not out and len(html_text) > 5000 and not _CHEM_SPA_WARNED:
        _CHEM_SPA_WARNED = True
        logger.warning(
            "ECHA CHEM returned HTML but no classifiable rows (SPA / no static links). "
            "C&L via this path will stay empty until an API or headless flow is wired; see module docstring."
        )

    return out


def _merge_registered_csv(substances: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    if not csv_path.is_file():
        return substances
    try:
        reg = pd.read_csv(csv_path, low_memory=False)
    except Exception as exc:
        logger.warning("Could not read registered CSV %s: %s", csv_path, exc)
        return substances
    if substances.empty:
        return substances

    # Heuristic column names (ECHA CSV varies)
    cas_col = next((c for c in reg.columns if "cas" in str(c).lower()), None)
    ec_col = next((c for c in reg.columns if str(c).lower() in ("ec number", "ec_number", "ecnumber")), None)
    name_col = next((c for c in reg.columns if "name" in str(c).lower()), None)
    if not cas_col and not ec_col:
        return substances

    def pick_cas(row):
        v = row.get("cas_number")
        if v is not None and str(v).strip() and str(v).lower() != "nan":
            return normalize_cas(str(v)) or str(v).strip()
        return None

    out = substances.copy()
    for i, row in out.iterrows():
        cas = pick_cas(row)
        ec = str(row.get("ec_number") or "").strip() or None
        if cas and not ec and cas_col and ec_col:
            hit = reg[reg[cas_col].astype(str).str.replace("-", "", regex=False) == cas.replace("-", "")]
            if hit.empty and cas_col:
                hit = reg[reg[cas_col].astype(str) == cas]
            if not hit.empty and ec_col:
                out.at[i, "ec_number"] = str(hit.iloc[0][ec_col]).strip()
        if (not cas or str(cas).lower() == "nan") and ec and cas_col and ec_col:
            hit = reg[reg[ec_col].astype(str) == ec]
            if not hit.empty:
                out.at[i, "cas_number"] = normalize_cas(str(hit.iloc[0][cas_col])) or hit.iloc[0][cas_col]
        if name_col and (not row.get("substance_name") or str(row.get("substance_name")).lower() == "nan"):
            if cas and cas_col:
                hit = reg[reg[cas_col].astype(str).str.contains(re.escape(cas[:6]), na=False)]
                if not hit.empty:
                    out.at[i, "substance_name"] = str(hit.iloc[0][name_col]).strip()
    return out


def _find_sibling_dossier_info_xlsx(archive_path: Path) -> Path | None:
    """ECHA often ships ``reach_study_results*dossier_info*.xlsx`` next to the dossier ``.zip``."""
    base = archive_path.parent if archive_path.is_file() else archive_path
    for pattern in ("reach_study_results*dossier_info*.xlsx", "*dossier_info*.xlsx"):
        hits = sorted(base.glob(pattern))
        if hits:
            return hits[0]
    return None


def _pick_dossier_info_sheet(xl: pd.ExcelFile, archive_path: Path) -> str:
    names = xl.sheet_names
    pref = os.getenv("OFFLINE_DOSSIER_INFO_SHEET", "").strip()
    if pref:
        if pref in names:
            return pref
        logger.warning(
            "OFFLINE_DOSSIER_INFO_SHEET=%r not in workbook (have %s); picking automatically.",
            pref,
            names[:8],
        )
    stem = archive_path.stem.lower()
    date_m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", stem)
    short_date: str | None = None
    long_date: str | None = None
    if date_m:
        a, b, y = date_m.groups()
        short_date = f"{a}-{b}-{y[2:]}"
        long_date = f"{a}-{b}-{y}"
    reach_sheets = [
        n
        for n in names
        if "reach" in n.lower() and "study" in n.lower() and "note" not in n.lower()
    ]
    if short_date:
        for n in reach_sheets:
            compact = n.replace(" ", "")
            if short_date in compact or (long_date and long_date in compact):
                return n
    if reach_sheets:
        return reach_sheets[0]
    return names[0]


def _merge_dossier_info_xlsx(
    substances: pd.DataFrame,
    xlsx_path: Path,
    *,
    archive_path: Path,
) -> pd.DataFrame:
    """Fill CAS / EC / name from ECHA's ``dossier_info`` Excel (UUID join)."""
    if substances.empty or not xlsx_path.is_file():
        return substances
    try:
        xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as exc:
        logger.warning("Could not open dossier info workbook %s: %s", xlsx_path, exc)
        return substances
    sheet = _pick_dossier_info_sheet(xl, archive_path)
    try:
        raw = pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl")
    except Exception as exc:
        logger.warning("Could not read sheet %r in %s: %s", sheet, xlsx_path, exc)
        return substances

    uuid_col = next(
        (str(c) for c in raw.columns if "dossier" in str(c).lower() and "uuid" in str(c).lower()),
        None,
    )
    if not uuid_col:
        logger.warning("Dossier info %s: no DOSSIER UUID column; columns=%s", xlsx_path.name, list(raw.columns)[:15])
        return substances
    cas_col = next((str(c) for c in raw.columns if "cas_number" in str(c).lower()), None)
    inv_col = next((str(c) for c in raw.columns if "inventory" in str(c).lower()), None)
    name_col = next(
        (
            str(c)
            for c in raw.columns
            if "name" in str(c).lower() and "substance" in str(c).lower() and "iupac" not in str(c).lower()
        ),
        None,
    )
    iupac_col = next((str(c) for c in raw.columns if "iupac" in str(c).lower()), None)

    slim = pd.DataFrame(
        {
            "_uuid_join": raw[uuid_col].astype(str).str.strip().str.lower(),
        }
    )
    if cas_col is not None:
        slim["_dossier_cas"] = raw[cas_col].map(
            lambda x: normalize_cas(str(x)) if x is not None and str(x).strip() and str(x).lower() != "nan" else pd.NA
        )
    if inv_col is not None:
        slim["_dossier_ec"] = raw[inv_col].map(
            lambda x: str(x).strip() if x is not None and str(x).strip() and str(x).lower() != "nan" else pd.NA
        )
    name_primary = raw[name_col] if name_col else None
    name_iupac = raw[iupac_col] if iupac_col else None
    if name_primary is not None or name_iupac is not None:
        p = (
            name_primary.map(lambda x: str(x).strip() if x is not None and str(x).strip() and str(x).lower() != "nan" else pd.NA)
            if name_primary is not None
            else pd.Series(pd.NA, index=raw.index)
        )
        q = (
            name_iupac.map(lambda x: str(x).strip() if x is not None and str(x).strip() and str(x).lower() != "nan" else pd.NA)
            if name_iupac is not None
            else pd.Series(pd.NA, index=raw.index)
        )
        slim["_dossier_name"] = p.combine_first(q)

    slim = slim.dropna(subset=["_uuid_join"])
    slim = slim[slim["_uuid_join"].str.len() > 5]
    slim = slim.drop_duplicates(subset=["_uuid_join"], keep="first")

    out = substances.copy()
    out["_uuid_join"] = out["uuid"].astype(str).str.strip().str.lower()
    merged = out.merge(slim, on="_uuid_join", how="left")
    drop_cols = ["_uuid_join"]
    for orig, doss in ("cas_number", "_dossier_cas"), ("ec_number", "_dossier_ec"), ("substance_name", "_dossier_name"):
        if doss not in merged.columns:
            continue
        left = merged[orig].replace("", pd.NA)
        left = left.mask(left.astype(str).str.lower().isin(("", "nan", "none")))
        merged[orig] = left.combine_first(merged[doss])
        drop_cols.append(doss)
    merged = merged.drop(columns=drop_cols, errors="ignore")
    n_cas = int(merged["cas_number"].notna().sum()) if "cas_number" in merged.columns else 0
    logger.info(
        "Merged dossier info %s (sheet %r): %s lookup rows -> substances with CAS: %s / %s",
        xlsx_path.name,
        sheet,
        len(slim),
        n_cas,
        len(merged),
    )
    return merged


def _snapshot_paths() -> tuple[Path, Path]:
    OFFLINE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return OFFLINE_CACHE_DIR / "offline_substances.csv", OFFLINE_CACHE_DIR / "offline_cl_hazards.csv"


OFFLINE_SUBSTANCE_DF_COLUMNS = [
    "uuid",
    "ec_number",
    "cas_number",
    "substance_name",
    "registration_date",
    "regulatory_pool",
    "infocard_url",
]
OFFLINE_CL_DF_COLUMNS = [
    "substance_uuid",
    "ec_number",
    "cas_number",
    "hazard_class",
    "hazard_category",
    "h_statement_code",
    "h_statement_text",
    "supplemental_info",
]


def normalize_offline_dataframe_columns(substances: pd.DataFrame, cl: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ensure empty frames have stable columns for downstream scripts (e.g. ``test_offline_10``)."""
    if substances.empty and substances.columns.size == 0:
        substances = pd.DataFrame(columns=OFFLINE_SUBSTANCE_DF_COLUMNS)
    if cl.empty and cl.columns.size == 0:
        cl = pd.DataFrame(columns=OFFLINE_CL_DF_COLUMNS)
    return substances, cl


def empty_offline_pair() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Typed empty ``(substances_df, cl_hazards_df)`` for early exits."""
    return normalize_offline_dataframe_columns(pd.DataFrame(), pd.DataFrame())


def _cl_row_has_signal(d: dict[str, Any]) -> bool:
    return bool(d.get("h_statement_code") or d.get("h_statement_text") or d.get("hazard_class"))


def build_offline_dataframes(
    force_download: bool = False,
    use_cache: bool = True,
    *,
    max_substances_for_cl: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build substances + C&L frames: local ``OFFLINE_LOCAL_ARCHIVE`` or downloaded REACH bulk + optional CHEM."""
    OFFLINE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = create_offline_session()
    local_raw = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
    scrape_cl = os.getenv("OFFLINE_SCRAPE_CL", "false").strip().lower() in ("1", "true", "yes", "on")

    crawl = _robots_crawl_delay(session, CHEM_BASE)
    post_data: dict[str, Any] | None = None
    if DISCLAIMER_POST_DATA_RAW:
        try:
            post_data = json.loads(DISCLAIMER_POST_DATA_RAW)
        except json.JSONDecodeError:
            pass

    archive_path: Path | None = None
    if local_raw:
        archive_path = Path(local_raw).expanduser().resolve()
        if not archive_path.exists():
            logger.error("OFFLINE_LOCAL_ARCHIVE does not exist: %s", archive_path)
            return empty_offline_pair()
        logger.info("Using OFFLINE_LOCAL_ARCHIVE (no remote REACH download): %s", archive_path)
    else:
        archive_path = OFFLINE_DATA_DIR / "REACH_Study_Results.7z"
        url = REACH_STUDY_RESULTS_URL or get_latest_reach_study_url(session)
        if not url:
            logger.error("Set OFFLINE_LOCAL_ARCHIVE, or REACH_STUDY_RESULTS_URL / index scraping for download mode.")
            return empty_offline_pair()

        if not archive_path.name.lower().endswith((".7z", ".zip")):
            tail = Path(urlparse(url).path).name
            if tail:
                archive_path = OFFLINE_DATA_DIR / tail

        if force_download or not archive_path.is_file():
            ok = download_with_disclaimer(
                session,
                url,
                archive_path,
                disclaimer_url=DISCLAIMER_URL or None,
                post_data=post_data,
            )
            if not ok:
                logger.error("REACH bulk download failed.")
                return empty_offline_pair()

    assert archive_path is not None
    substances, cl_i6z = extract_i6z_metadata(archive_path, OFFLINE_DATA_DIR, force_extract=force_download)

    dossier_env = os.getenv("OFFLINE_DOSSIER_INFO_XLSX", "").strip()
    dossier_path = Path(os.path.expandvars(dossier_env)).expanduser().resolve() if dossier_env else None
    if dossier_path and not dossier_path.is_file():
        logger.warning("OFFLINE_DOSSIER_INFO_XLSX does not exist: %s", dossier_path)
        dossier_path = None
    if dossier_path is None:
        found = _find_sibling_dossier_info_xlsx(archive_path)
        if found is not None:
            dossier_path = found
            logger.info("Using dossier info workbook beside archive: %s", dossier_path.name)
    if dossier_path is not None and dossier_path.is_file():
        substances = _merge_dossier_info_xlsx(substances, dossier_path, archive_path=archive_path)

    reg_csv = OFFLINE_DATA_DIR / "registered_substances.csv"
    if not reg_csv.is_file():
        download_registered_substances_csv(session, reg_csv)
    substances = _merge_registered_csv(substances, reg_csv)

    cl_rows: list[dict[str, Any]] = list(cl_i6z)
    uids_with_i6d_cl = {str(d.get("substance_uuid")) for d in cl_i6z if _cl_row_has_signal(d)}

    to_scrape = substances[~substances["uuid"].astype(str).isin(uids_with_i6d_cl)].copy()
    if not scrape_cl:
        to_scrape = substances.iloc[0:0].copy()
    else:
        logger.info(
            "OFFLINE_SCRAPE_CL=true: scraping ECHA CHEM for %s substances without i6d classification signal.",
            len(to_scrape),
        )
        if len(to_scrape) > 500 and max_substances_for_cl is None:
            logger.warning(
                "Many substances (%s) lack i6d CL signals; CHEM fallback is slow/unreliable. "
                "Set --offline-max-cl or OFFLINE_MAX_CL to cap requests.",
                len(to_scrape),
            )
    if max_substances_for_cl is not None and not to_scrape.empty:
        to_scrape = to_scrape.head(max_substances_for_cl)

    def worker(idx_row: tuple[int, Any]) -> tuple[int, list[dict[str, Any]]]:
        _, r = idx_row
        ws = create_offline_session()
        ec = str(r.get("ec_number") or "").strip() or None
        cas = str(r.get("cas_number") or "").strip() or None
        name = str(r.get("substance_name") or "").strip() or None
        uid = str(r.get("uuid") or "")
        try:
            part = fetch_cl_from_echa_chem(
                ws,
                ec,
                cas,
                name,
                OFFLINE_CACHE_DIR / "chem_pages",
                min_delay_s=crawl,
            )
            for d in part:
                d["substance_uuid"] = uid or d.get("substance_uuid")
                si = d.get("supplemental_info")
                d["supplemental_info"] = f"{si}_chem_fallback" if si else "chem_fallback"
            return idx_row[0], part
        except Exception as exc:
            logger.warning("CHEM scrape skip %s: %s", cas or ec or name, exc)
            return idx_row[0], []

    pairs = list(to_scrape.iterrows())
    if pairs:
        with ThreadPoolExecutor(max_workers=OFFLINE_CHEM_MAX_WORKERS) as ex:
            futs = [ex.submit(worker, p) for p in pairs]
            for fut in tqdm(as_completed(futs), total=len(futs), desc="ECHA CHEM C&L (fallback)"):
                try:
                    _, chunk = fut.result()
                    cl_rows.extend(chunk)
                except Exception as exc:
                    logger.warning("Worker error: %s", exc)
    elif substances.empty:
        logger.warning("No .i6z rows parsed; check archive path, extraction, and contents.")

    cl = pd.DataFrame(cl_rows)
    substances, cl = normalize_offline_dataframe_columns(substances, cl)
    if use_cache:
        p_sub, p_cl = _snapshot_paths()
        substances.to_csv(p_sub, index=False)
        cl.to_csv(p_cl, index=False)
        logger.info("Wrote offline snapshots: %s, %s", p_sub, p_cl)

    return substances, cl


def load_echa_from_offline(
    use_cache: bool = True,
    *,
    force_rebuild: bool = False,
    force_download: bool = False,
    max_substances_for_cl: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load CSV snapshots when present; otherwise build. Set ``force_rebuild`` to ignore snapshots."""
    if not use_cache or force_rebuild or force_download or max_substances_for_cl is not None:
        return build_offline_dataframes(
            force_download=force_download,
            use_cache=True,
            max_substances_for_cl=max_substances_for_cl,
        )
    p_sub, p_cl = _snapshot_paths()
    if p_sub.is_file() and p_cl.is_file():
        logger.info("Loading offline snapshots from %s", OFFLINE_CACHE_DIR)
        reg = pd.read_csv(p_sub)
        cl = pd.read_csv(p_cl)
        reg, cl = normalize_offline_dataframe_columns(reg, cl)
        # Legacy builds only had UUID from .i6z; merge ECHA dossier-info XLSX without a full archive re-parse.
        if (
            not reg.empty
            and "cas_number" in reg.columns
            and reg["cas_number"].notna().sum() == 0
        ):
            la = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
            arch = Path(os.path.expandvars(la)).expanduser().resolve() if la else None
            dx: Path | None = None
            if arch and arch.is_file():
                denv = os.getenv("OFFLINE_DOSSIER_INFO_XLSX", "").strip()
                if denv:
                    cand = Path(os.path.expandvars(denv)).expanduser().resolve()
                    dx = cand if cand.is_file() else None
                if dx is None:
                    dx = _find_sibling_dossier_info_xlsx(arch)
            if dx is not None and dx.is_file() and arch is not None and arch.is_file():
                logger.info(
                    "Snapshot has no CAS values; merging dossier info from %s and refreshing %s.",
                    dx.name,
                    p_sub.name,
                )
                reg = _merge_dossier_info_xlsx(reg, dx, archive_path=arch)
                reg.to_csv(p_sub, index=False)
        return reg, cl
    reg, cl = build_offline_dataframes(force_download=False, use_cache=True)
    return normalize_offline_dataframe_columns(reg, cl)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    la = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
    if not la:
        print(
            "Set OFFLINE_LOCAL_ARCHIVE to your REACH .zip/.7z or a folder of .i6z files, then re-run:\n"
            "  OFFLINE_LOCAL_ARCHIVE=C:\\\\path\\\\to\\\\REACH_Study_Results.zip OFFLINE_SCRAPE_CL=false python -m ingest.offline_echa_loader",
            file=sys.stderr,
        )
        sys.exit(2)
    reg, cl = build_offline_dataframes(force_download=False, use_cache=True)
    print("substances_df:", reg.shape)
    print("cl_hazards_df:", cl.shape)
    if not reg.empty:
        print(reg.head(3).to_string())
    if not cl.empty:
        print(cl.head(5).to_string())
