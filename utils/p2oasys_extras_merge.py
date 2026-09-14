"""
Production-parity merge of ``extra_sources`` for P2OASys (IARC, ODP/GWP, IPCC GWP, IUCLID).

Mirrors the Streamlit P2OASys tab: ``lookup_tables.get_lookup_extra_sources`` plus
``build_extra_sources_from_iuclid_unified`` merged via ``hazard_for_p2oasys.merge_extra_sources``.

IUCLID requires ``OFFLINE_LOCAL_ARCHIVE`` pointing at a local REACH archive / context
understood by ``unified_lookup``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import config
from unified_hazard_report.unified_lookup import unified_lookup
from utils import atmo_gwp, hazard_for_p2oasys, iarc_lookup, lookup_tables
from utils.iuclid_p2oasys_bridge import build_extra_sources_from_iuclid_unified

logger = logging.getLogger(__name__)

_CTX_CACHE: dict[str, Any] = {}
_LOOKUP_CACHE: dict[str, Any] | None = None


def _offline_ctx_singleton() -> Any:
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
    key = (os.environ.get("OFFLINE_LOCAL_ARCHIVE") or "").strip()
    if not key:
        return None
    if key not in _CTX_CACHE:
        _CTX_CACHE[key] = _offline_ctx_singleton()
    return _CTX_CACHE[key]


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
    return cache


def merge_extra_sources_for_cas(clean_cas: str) -> tuple[dict[str, Any] | None, str]:
    """
    Build ``extra_sources`` like the P2OASys tab (lookups + IUCLID).

    Returns ``(extra_dict_or_none, short_note)`` e.g. ``"lookups+IUCLID"`` or ``"PubChem-only"``.
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
    return extra, "+".join(notes) if notes else "PubChem-only"
