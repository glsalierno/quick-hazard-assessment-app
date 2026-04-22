"""
REACH / IUCLID offline hooks for the main Streamlit app.

Uses ``OfflineDataContext`` + ``unified_lookup`` (same stack as the unified hazard report CLI).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
import streamlit as st

from ingest.crosswalk import normalize_cas
from utils import hazard_report_utils

logger = logging.getLogger(__name__)

_OFFLINE_SYNC_KEYS = (
    "OFFLINE_LOCAL_ARCHIVE",
    "OFFLINE_DOSSIER_INFO_XLSX",
    "OFFLINE_DOSSIER_INFO_SHEET",
    "OFFLINE_DATA_DIR",
    "OFFLINE_CACHE_DIR",
    "OFFLINE_SCRAPE_CL",
    "OFFLINE_I6Z_USE_MP",
    "OFFLINE_I6Z_MAX_WORKERS",
    "OFFLINE_I6Z_MIN_FILES_FOR_MP",
)


def sync_offline_secrets_from_st_secrets() -> None:
    """Copy selected ``st.secrets`` keys into ``os.environ`` when unset (``offline_echa_loader`` uses ``os.getenv``)."""
    try:
        sec: Any = st.secrets
    except (FileNotFoundError, RuntimeError, AttributeError):
        return
    for key in _OFFLINE_SYNC_KEYS:
        if (os.environ.get(key) or "").strip():
            continue
        try:
            if key in sec:
                val = sec[key]
                os.environ[key] = str(val).strip() if val is not None else ""
        except Exception:
            continue


def offline_archive_fingerprint() -> str:
    return (os.getenv("OFFLINE_LOCAL_ARCHIVE") or "").strip()


@st.cache_resource(show_spinner="Loading offline REACH index…")
def get_offline_context(archive_fingerprint: str) -> Any:
    """
    Build :class:`unified_hazard_report.data_context.OfflineDataContext` once per archive path.

    ``archive_fingerprint`` must be the resolved ``OFFLINE_LOCAL_ARCHIVE`` string (or empty to skip).
    """
    if not archive_fingerprint:
        return None
    try:
        from unified_hazard_report.data_context import OfflineDataContext

        return OfflineDataContext()
    except Exception as exc:
        logger.warning("Offline REACH context unavailable: %s", exc)
        return None


def render_reach_iuclid_panel(clean_cas: str) -> None:
    """
    Option A: dossier index (name, EC, UUID, infocard URL).
    Option B: IUCLID ``Document.i6d`` endpoint snippets + offline C&L rows when present.
    """
    sync_offline_secrets_from_st_secrets()
    cas_norm = normalize_cas((clean_cas or "").strip()) or (clean_cas or "").strip()
    if not cas_norm:
        return

    fp = offline_archive_fingerprint()
    with st.expander("REACH / IUCLID (offline dossier)", expanded=False):
        if not fp:
            st.info(
                "No offline REACH archive configured. Set **OFFLINE_LOCAL_ARCHIVE** to your ``.zip`` / ``.7z`` "
                "or a folder of ``.i6z`` files (PowerShell or ``.streamlit/secrets.toml``), then reload the app."
            )
            return

        ctx = get_offline_context(fp)
        if ctx is None:
            st.warning("Could not load the offline REACH context. Check **OFFLINE_LOCAL_ARCHIVE** and snapshot paths.")
            return

        try:
            from unified_hazard_report.unified_lookup import unified_lookup

            data = unified_lookup(cas_norm, ctx)
        except Exception as exc:
            st.error(f"REACH / IUCLID lookup failed: {exc}")
            logger.exception("unified_lookup failed for CAS %s", cas_norm)
            return

        uuids = data.get("iuclid_uuids") or []
        if not uuids:
            st.info(f"No REACH dossier index row matches **{cas_norm}** in the offline substances snapshot.")
            st.caption(
                "If you expect a hit, confirm CAS formatting, merge **OFFLINE_DOSSIER_INFO_XLSX**, or rebuild snapshots."
            )
            return

        st.caption(f"{len(uuids)} matching dossier UUID(s) in the offline index.")

        cols = [c for c in ("uuid", "substance_name", "ec_number", "cas_number", "infocard_url") if c in ctx.substances_df.columns]
        if cols:
            uid_set = {str(u) for u in uuids}
            dossier_df = ctx.substances_df[ctx.substances_df["uuid"].astype(str).isin(uid_set)][cols].drop_duplicates()
            st.markdown("**Dossier index (ECHA / IUCLID)**")
            st.dataframe(
                hazard_report_utils.clean_dataframe(dossier_df),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.dataframe(pd.DataFrame(data.get("iuclid_substances") or []), use_container_width=True, hide_index=True)

        cl_rows = data.get("iuclid_cl_rows") or []
        if cl_rows:
            st.markdown("**Classification / GHS-style rows (from ``Document.i6d``)**")
            cdf = pd.DataFrame(cl_rows)
            st.dataframe(
                hazard_report_utils.clean_dataframe(cdf),
                use_container_width=True,
                hide_index=True,
                height=min(400, 35 * (len(cl_rows) + 2)),
            )
        else:
            st.caption("No C&L / GHS-style rows in the offline hazard snapshot for this substance (common for Study Results–only builds).")

        eps = data.get("iuclid_endpoints") or []
        if eps:
            st.markdown("**Study endpoint snippets (heuristic scan of ``Document.i6d``)**")
            edf = pd.DataFrame(eps)
            if "uuid" in edf.columns:
                edf = edf[["uuid", "endpoint_name", "result", "units"]] if all(
                    c in edf.columns for c in ("endpoint_name", "result")
                ) else edf
            st.dataframe(
                hazard_report_utils.clean_dataframe(edf),
                use_container_width=True,
                hide_index=True,
                height=min(420, 28 * (min(len(eps), 40) + 2)),
            )
        else:
            st.caption(
                "No endpoint snippets extracted from local ``.i6z`` (missing file under the extract path, "
                "or XML did not match heuristics)."
            )
