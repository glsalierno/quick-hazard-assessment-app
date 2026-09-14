"""
Production-parity merge of ``extra_sources`` for P2OASys
(IARC, ODP/GWP, IPCC GWP, NESHAP, OPERA precompute, IUCLID, ECOSAR, CAMEO).

Mirrors the Streamlit P2OASys tab: ``lookup_tables.get_lookup_extra_sources`` plus
OPERA gap-fill from ``opera_precompute.sqlite``,
``build_extra_sources_from_iuclid_unified``, and ECOSAR aquatic predictions
(EPI Suite API) merged via ``hazard_for_p2oasys.merge_extra_sources``.

IUCLID requires ``OFFLINE_LOCAL_ARCHIVE`` pointing at a local REACH archive / context
understood by ``unified_lookup``. ECOSAR runs after IUCLID and only when acute
aquatic LC50/EC50 is still missing (disable with ``HAZQUERY_SKIP_ECOSAR=1``).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import config
from unified_hazard_report.unified_lookup import unified_lookup
from utils import (
    atmo_gwp,
    hazard_for_p2oasys,
    iarc_lookup,
    lookup_tables,
    neshap_hap,
    opera_precompute_cache,
    p2oasys_source_bridges,
)
from utils.iuclid_p2oasys_bridge import build_extra_sources_from_iuclid_unified

logger = logging.getLogger(__name__)

_CTX_CACHE: dict[str, Any] = {}
_LOOKUP_CACHE: dict[str, Any] | None = None

# Prefer hazquery/IUCLID (junctions), then GHhaz4 canonical tree.
_HAZQUERY_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_REACH_CANDIDATES = [
    _HAZQUERY_ROOT / "IUCLID" / "reach_study_results_dossiers_23-05-2023",
    _HAZQUERY_ROOT / "IUCLID" / "reach_study_results_dossiers_23-05-2023.zip",
    _HAZQUERY_ROOT
    / "GHhaz4"
    / "ECHA IUCLID database"
    / "reach_study_results_dossiers_23-05-2023.zip",
]
_DEFAULT_DOSSIER_XLSX_CANDIDATES = [
    _HAZQUERY_ROOT / "IUCLID" / "reach_study_results-dossier_info_23-05-2023.xlsx",
    _HAZQUERY_ROOT
    / "GHhaz4"
    / "ECHA IUCLID database"
    / "reach_study_results-dossier_info_23-05-2023.xlsx",
]


def ensure_offline_env_defaults() -> None:
    """Point OFFLINE_* at local REACH assets when env is empty and files exist."""
    if not (os.environ.get("OFFLINE_LOCAL_ARCHIVE") or "").strip():
        for cand in _DEFAULT_REACH_CANDIDATES:
            if cand.exists():
                os.environ["OFFLINE_LOCAL_ARCHIVE"] = str(cand)
                break
    if not (os.environ.get("OFFLINE_DOSSIER_INFO_XLSX") or "").strip():
        for cand in _DEFAULT_DOSSIER_XLSX_CANDIDATES:
            if cand.is_file():
                os.environ["OFFLINE_DOSSIER_INFO_XLSX"] = str(cand)
                break
    cache_dir = Path(getattr(config, "DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "offline_cache"
    if not (os.environ.get("OFFLINE_CACHE_DIR") or "").strip() and cache_dir.is_dir():
        os.environ["OFFLINE_CACHE_DIR"] = str(cache_dir)


def _offline_ctx_singleton() -> Any:
    ensure_offline_env_defaults()
    fp = (os.environ.get("OFFLINE_LOCAL_ARCHIVE") or "").strip()
    if not fp:
        return None
    try:
        from unified_hazard_report.data_context import OfflineDataContext

        return OfflineDataContext()
    except Exception as exc:
        logger.warning("Offline REACH context unavailable: %s", exc)
        return None


def get_offline_ctx() -> Any:
    """Shared ``OfflineDataContext`` for IUCLID (keyed by ``OFFLINE_LOCAL_ARCHIVE``)."""
    ensure_offline_env_defaults()
    key = (os.environ.get("OFFLINE_LOCAL_ARCHIVE") or "").strip()
    if not key:
        return None
    if key not in _CTX_CACHE:
        _CTX_CACHE[key] = _offline_ctx_singleton()
    return _CTX_CACHE[key]


def opera_extra_from_precompute(clean_cas: str) -> dict[str, Any] | None:
    """Load OPERA row from precompute SQLite and bridge to extra_sources."""
    db = opera_precompute_cache.default_precompute_db_path()
    row = opera_precompute_cache.get_cas_row(db, clean_cas)
    if not row:
        digits = lookup_tables.normalize_cas_for_lookup(clean_cas)
        if digits:
            row = opera_precompute_cache.get_cas_row(db, digits)
            if not row:
                try:
                    from utils.p2oasys_score_lookup import format_cas_display

                    row = opera_precompute_cache.get_cas_row(db, format_cas_display(digits))
                except Exception:
                    row = None
    if not row:
        return None
    return p2oasys_source_bridges.opera_to_extra_sources(
        {"ok": True, "row": row, "display": {}, "source": "opera_precompute"},
        existing_hazard={},
    )


def _lookup_cache_load() -> dict[str, Any]:
    cache: dict[str, Any] = {}
    iarc_dir = getattr(config, "IARC_DIR", None)
    if iarc_dir and os.path.isdir(iarc_dir):
        cache["iarc_by_cas"] = iarc_lookup.load_iarc_from_iarc_folder(iarc_dir)
    else:
        cache["iarc_by_cas"] = None
    if not cache["iarc_by_cas"] and getattr(config, "P2OASYS_IARC_CSV_PATH", None):
        p = config.P2OASYS_IARC_CSV_PATH
        if p and os.path.isfile(p):
            cache["iarc_by_cas"] = lookup_tables.load_iarc_csv(p)
    cache["odp_gwp_by_cas"] = None
    if getattr(config, "P2OASYS_ODP_GWP_CSV_PATH", None):
        odp_path = config.P2OASYS_ODP_GWP_CSV_PATH
        if odp_path and os.path.isfile(odp_path):
            cache["odp_gwp_by_cas"] = lookup_tables.load_odp_gwp_csv(odp_path)
    cache["ipcc_gwp_by_cas"] = None
    atmo_dir = atmo_gwp.resolve_atmo_dir(
        getattr(config, "ATMO_DIR", None),
        repo_root=getattr(config, "REPO_ROOT", None),
        fastp2oasys_dir=getattr(config, "FASTP2OASYS_DIR", None),
    )
    if atmo_dir is not None:
        cache["ipcc_gwp_by_cas"] = atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir)
    hap_path = getattr(config, "P2OASYS_HAP_CSV_PATH", None)
    cache["hap_cas"] = neshap_hap.load_hap_cas_set(hap_path) if hap_path else neshap_hap.load_hap_cas_set()
    return cache


def merge_extra_sources_for_cas(clean_cas: str) -> tuple[dict[str, Any] | None, str]:
    """
    Build ``extra_sources`` like the P2OASys tab (lookups + OPERA + IUCLID + CAMEO).

    Returns ``(extra_dict_or_none, short_note)`` e.g. ``"lookups+OPERA+IUCLID"``.
    """
    global _LOOKUP_CACHE
    notes: list[str] = []
    extra: dict[str, Any] | None = None
    if _LOOKUP_CACHE is None:
        _LOOKUP_CACHE = _lookup_cache_load()
    iarc = _LOOKUP_CACHE.get("iarc_by_cas")
    odp = _LOOKUP_CACHE.get("odp_gwp_by_cas")
    ipcc = _LOOKUP_CACHE.get("ipcc_gwp_by_cas")
    if iarc or odp or ipcc:
        extra = lookup_tables.get_lookup_extra_sources(
            clean_cas,
            iarc_by_cas=iarc,
            odp_gwp_by_cas=odp,
            ipcc_gwp_by_cas=ipcc,
        )
        notes.append("lookups")
    hap = _LOOKUP_CACHE.get("hap_cas")
    if hap:
        extra = neshap_hap.apply_neshap_to_extra_sources(extra, clean_cas, hap_cas=hap)
        notes.append("NESHAP")

    try:
        opera_xs = opera_extra_from_precompute(clean_cas)
        if opera_xs:
            extra = hazard_for_p2oasys.merge_extra_sources(extra, opera_xs)
            notes.append("OPERA")
    except Exception as exc:
        logger.debug("OPERA precompute merge skipped for %s: %s", clean_cas, exc)

    ctx = get_offline_ctx()
    if ctx is not None:
        try:
            ul = unified_lookup(clean_cas, ctx)
            iu = build_extra_sources_from_iuclid_unified(ul)
            if iu:
                extra = hazard_for_p2oasys.merge_extra_sources(extra, iu)
                notes.append("IUCLID")
        except Exception as exc:
            logger.debug("IUCLID merge skipped for %s: %s", clean_cas, exc)

    try:
        from utils import ecosar_client

        ecosar_xs = ecosar_client.fetch_ecosar_extra_sources(
            clean_cas,
            existing_hazard=extra or {},
        )
        if ecosar_xs:
            extra = hazard_for_p2oasys.merge_extra_sources(extra, ecosar_xs)
            notes.append("ECOSAR")
    except Exception as exc:
        logger.debug("ECOSAR merge skipped for %s: %s", clean_cas, exc)

    before_nfpa = len((extra or {}).get("hazard_metrics", {}).get("nfpa") or [])
    extra = hazard_for_p2oasys.merge_cameo_extra(clean_cas, extra)
    after_nfpa = len((extra or {}).get("hazard_metrics", {}).get("nfpa") or [])
    if after_nfpa > before_nfpa:
        notes.append("CAMEO")
    return extra, "+".join(notes) if notes else "PubChem-only"
