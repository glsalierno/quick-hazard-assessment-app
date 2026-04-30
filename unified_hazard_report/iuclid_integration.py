"""
REACH / IUCLID offline hooks for the main Streamlit app.

Uses ``OfflineDataContext`` + ``unified_lookup`` (same stack as the unified hazard report CLI).

**Secrets / env:** ``ingest.offline_echa_loader`` only reads ``os.environ``. Values may come from the shell,
from ``.streamlit/secrets.toml`` (via Streamlit), or from a **direct TOML read** of that file when Streamlit's
``st.secrets`` does not expose keys the way we expect (e.g. ``key in st.secrets`` false for valid keys).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from ingest.crosswalk import normalize_cas
from utils import ghs_formatter, hazard_report_utils
from utils.iuclid_phrase_mapper import has_phrase_mapping
from unified_hazard_report.iuclid_cache import cache_db_path, rebuild_iuclid_cache

logger = logging.getLogger(__name__)

# App root = parent of ``unified_hazard_report/`` (works when cwd differs from the package directory).
_APP_ROOT = Path(__file__).resolve().parents[1]

OFFLINE_ENV_KEYS = (
    "OFFLINE_LOCAL_ARCHIVE",
    "OFFLINE_DOSSIER_INFO_XLSX",
    "OFFLINE_DOSSIER_INFO_SHEET",
    "OFFLINE_DATA_DIR",
    "OFFLINE_CACHE_DIR",
    "OFFLINE_SCRAPE_CL",
    "OFFLINE_I6Z_USE_MP",
    "OFFLINE_I6Z_MAX_WORKERS",
    "OFFLINE_I6Z_MIN_FILES_FOR_MP",
    "HAZQUERY_DISABLE_DOCLING",
    "IUCLID_FORMAT_DIR",
)

# Back-compat for internal references
_OFFLINE_SYNC_KEYS = OFFLINE_ENV_KEYS

_last_offline_context_error: str | None = None


def _candidate_secrets_toml_paths() -> list[Path]:
    """Prefer cwd (``streamlit run`` root), then package app root."""
    seen: set[Path] = set()
    out: list[Path] = []
    for base in (Path.cwd(), _APP_ROOT):
        p = (base / ".streamlit" / "secrets.toml").resolve()
        if p.is_file() and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _sync_offline_from_toml_files() -> None:
    """Load flat string keys from ``.streamlit/secrets.toml`` into ``os.environ`` when unset or empty."""
    try:
        import tomllib
    except ImportError:
        return  # pragma: no cover
    def _extract_value(data: dict[str, Any], key: str) -> Any:
        # Primary requirement: exact top-level key.
        if key in data:
            return data.get(key)
        # Fallback: common nested sections users create in secrets.toml.
        for section_name in ("offline", "offline_loader", "reach", "iuclid"):
            section = data.get(section_name)
            if isinstance(section, dict) and key in section:
                return section.get(key)
        return None

    for path in _candidate_secrets_toml_paths():
        try:
            raw = path.read_text(encoding="utf-8")
            data = tomllib.loads(raw)
        except Exception as exc:
            logger.warning("Could not parse %s: %s", path, exc)
            continue
        if not isinstance(data, dict):
            continue
        for key in _OFFLINE_SYNC_KEYS:
            if (os.environ.get(key) or "").strip():
                continue
            val = _extract_value(data, key)
            if val is None:
                continue
            if isinstance(val, (dict, list)):
                continue
            s = str(val).strip()
            if not s:
                if key == "OFFLINE_LOCAL_ARCHIVE" and _extract_value(data, key) is not None:
                    logger.warning(
                        "%s is set in %s but empty; offline REACH will not load until you set a non-empty path "
                        "(or set the variable in PowerShell before `streamlit run`).",
                        key,
                        path,
                    )
                continue
            os.environ[key] = s
            logger.debug("Set %s from %s (toml)", key, path.name)


def _sync_offline_from_st_secrets_object() -> None:
    """Copy selected keys from ``st.secrets`` into ``os.environ`` when unset or empty."""
    try:
        sec: Any = st.secrets
    except (FileNotFoundError, RuntimeError, AttributeError):
        return
    for key in _OFFLINE_SYNC_KEYS:
        if (os.environ.get(key) or "").strip():
            continue
        val: Any = None
        try:
            val = sec[key]
        except Exception:
            try:
                val = getattr(sec, key, None)
            except Exception:
                val = None
        if val is None:
            continue
        try:
            s = str(val).strip()
        except Exception:
            continue
        if s:
            os.environ[key] = s
            logger.debug("Set %s from st.secrets", key)


def sync_offline_secrets_from_st_secrets() -> None:
    """
    Ensure ``os.environ`` has offline keys for ``ingest.offline_echa_loader``.

    Order: **TOML on disk** (reliable), then **st.secrets** (covers hosted / alternate layouts).
    Only fills keys that are missing or whitespace-empty in the environment.
    """
    _sync_offline_from_toml_files()
    _sync_offline_from_st_secrets_object()
    if os.getenv("HAZQUERY_DEBUG_OFFLINE_SYNC", "").strip().lower() in ("1", "true", "yes", "on"):
        la = (os.environ.get("OFFLINE_LOCAL_ARCHIVE") or "").strip()
        logger.info(
            "HAZQUERY_DEBUG_OFFLINE_SYNC: OFFLINE_LOCAL_ARCHIVE len=%s toml_files=%s",
            len(la),
            len(_candidate_secrets_toml_paths()),
        )
    apply_repo_iuclid_defaults_for_streamlit_cloud()


def apply_repo_iuclid_defaults_for_streamlit_cloud() -> None:
    """
    On Streamlit Cloud, if ``OFFLINE_LOCAL_ARCHIVE`` / ``IUCLID_FORMAT_DIR`` are still unset,
    default to committed demo paths under the repo when those files exist.

    User-set Secrets / env always win. Set ``HAZQUERY_DISABLE_REPO_IUCLID_DEFAULTS=1`` to skip.
    """
    if os.getenv("HAZQUERY_DISABLE_REPO_IUCLID_DEFAULTS", "").strip().lower() in ("1", "true", "yes", "on"):
        return
    try:
        from services.config import ServiceConfig

        on_cloud = ServiceConfig.is_streamlit_cloud()
    except Exception:
        on_cloud = bool(
            os.getenv("STREAMLIT_CLOUD") == "1"
            or os.getenv("IS_STREAMLIT_CLOUD") == "1"
            or str(os.getenv("HOSTNAME", "")).endswith(".streamlit.app")
        )
    if not on_cloud:
        return

    root = _APP_ROOT
    demo_zip = root / "data" / "reach_demo" / "reach_subset.zip"
    format_dir = root / "data" / "iuclid_format" / "IUCLID_6_9_0_0_format"

    if not (os.getenv("OFFLINE_LOCAL_ARCHIVE") or "").strip() and demo_zip.is_file():
        os.environ["OFFLINE_LOCAL_ARCHIVE"] = str(demo_zip.resolve())
        logger.info(
            "Streamlit Cloud: default OFFLINE_LOCAL_ARCHIVE=%s (committed demo subset; not full REACH)",
            os.environ["OFFLINE_LOCAL_ARCHIVE"],
        )
    if not (os.getenv("IUCLID_FORMAT_DIR") or "").strip() and format_dir.is_dir():
        os.environ["IUCLID_FORMAT_DIR"] = str(format_dir.resolve())
        logger.info(
            "Streamlit Cloud: default IUCLID_FORMAT_DIR=%s (committed format bundle)",
            os.environ["IUCLID_FORMAT_DIR"],
        )


def offline_archive_fingerprint() -> str:
    return (os.getenv("OFFLINE_LOCAL_ARCHIVE") or "").strip()


def committed_reach_demo_zip_path() -> Path:
    """Absolute path to the small committed demo archive (may or may not exist on disk)."""
    return (_APP_ROOT / "data" / "reach_demo" / "reach_subset.zip").resolve()


def using_committed_reach_demo_archive() -> bool:
    """
    True when ``OFFLINE_LOCAL_ARCHIVE`` points at the repo's committed demo zip.

    Used to show that REACH coverage is intentionally partial and parsed fields may be incomplete.
    """
    fp = (os.getenv("OFFLINE_LOCAL_ARCHIVE") or "").strip()
    if not fp:
        return False
    try:
        return Path(fp).expanduser().resolve() == committed_reach_demo_zip_path()
    except OSError:
        return False


def offline_reach_archive_status() -> tuple[bool, str]:
    """
    Whether ``OFFLINE_LOCAL_ARCHIVE`` can be used on this machine.

    Returns
    -------
    ok, code
        ``(True, "ok")`` if the env var is set and ``Path(...).exists()``.
        Otherwise ``(False, "unset" | "missing" | "badpath")``.
    """
    sync_offline_secrets_from_st_secrets()
    fp = offline_archive_fingerprint()
    if not fp:
        return False, "unset"
    try:
        if not Path(fp).expanduser().exists():
            return False, "missing"
    except OSError:
        return False, "badpath"
    return True, "ok"


@st.cache_resource(show_spinner="Loading offline REACH index…")
def get_offline_context(archive_fingerprint: str) -> Any:
    """
    Build :class:`unified_hazard_report.data_context.OfflineDataContext` once per archive path.

    ``archive_fingerprint`` must be the resolved ``OFFLINE_LOCAL_ARCHIVE`` string (or empty to skip).
    """
    global _last_offline_context_error
    _last_offline_context_error = None
    if not archive_fingerprint:
        return None
    try:
        from unified_hazard_report.data_context import OfflineDataContext

        return OfflineDataContext()
    except Exception as exc:
        _last_offline_context_error = f"{type(exc).__name__}: {exc}"
        logger.exception("Offline REACH context unavailable (archive fingerprint len=%s)", len(archive_fingerprint))
        return None


def render_reach_iuclid_panel_unconfigured(code: str) -> None:
    """
    Show a single REACH / IUCLID expander when no usable offline archive is configured.

    Used on Streamlit Cloud (no multi-GB ECHA bundle in the repo) and local runs without
    ``OFFLINE_LOCAL_ARCHIVE``. Avoids calling :func:`render_reach_iuclid_panel`, which would
    duplicate messaging and may touch loader paths unnecessarily.
    """
    with st.expander("REACH / IUCLID (offline dossier)", expanded=False):
        try:
            from services.config import ServiceConfig

            on_cloud = ServiceConfig.is_streamlit_cloud()
        except Exception:
            on_cloud = False

        cloud_note = (
            "**Streamlit Cloud:** the full ECHA REACH export is too large for GitHub. "
            "This app may use a **small committed demo** (`data/reach_demo/reach_subset.zip`) — "
            "most substances are **not** included, dossier content can be **missing or partial**, and parsing is **heuristic**. "
            "For full coverage, run **locally** with ``OFFLINE_LOCAL_ARCHIVE`` pointing at your official dossier "
            "``.zip`` / ``.7z`` or a folder of ``.i6z`` files.\n\n"
        )
        if code == "unset":
            st.info(
                (cloud_note if on_cloud else "")
                + "🔒 **IUCLID offline lookup is not configured.** "
                "Set **`OFFLINE_LOCAL_ARCHIVE`** in Streamlit **Secrets** (cloud) or your shell / ``.env`` (local) "
                "to your REACH study-results archive or a folder of extracted ``.i6z`` dossiers. "
                "See README → **Offline REACH / IUCLID (optional)**."
            )
            if on_cloud:
                st.markdown(
                    "**Streamlit Cloud:** open the app → **⋮ Manage app** → **Secrets**, and add a **top-level** "
                    "TOML key (name must match exactly):"
                )
                st.code(
                    "# Optional — IUCLID phrase/picklist format tree (~100 MB); copy from your "
                    "# \"IUCLID 6 9.0.0_format\" folder into the repo (no spaces in path recommended)\n"
                    'IUCLID_FORMAT_DIR = "/mount/src/quick-hazard-assessment-app/data/iuclid_format/IUCLID_6_9_0_0_format"\n'
                    "\n"
                    "# Required for dossier / .i6z lookup — must be a path INSIDE the Cloud clone.\n"
                    "# Full REACH bulk (~10+ GB) cannot live on GitHub; use a small committed demo zip/folder only.\n"
                    'OFFLINE_LOCAL_ARCHIVE = "/mount/src/quick-hazard-assessment-app/data/reach_demo/reach_subset.zip"\n',
                    language="toml",
                )
                st.caption(
                    "Save Secrets, then **Reboot**. See **`data/echa_cloud/README.txt`** in the repo for what to copy "
                    "from your local `ECHA IUCLID database` folder vs what must stay local or be shrunk for demos."
                )
        elif code == "badpath":
            st.warning(
                "**IUCLID (offline REACH):** ``OFFLINE_LOCAL_ARCHIVE`` is set but could not be read as a path "
                "on this host. Fix the value in Secrets or the environment."
            )
        else:
            st.warning(
                (cloud_note if on_cloud else "")
                + "**IUCLID (offline REACH):** ``OFFLINE_LOCAL_ARCHIVE`` is set, but that path **does not exist** "
                "or is unreadable on this server. Cloud paths differ from a laptop — use a path inside the deployed "
                "repo, mount external storage, or run locally with an absolute path to your archive."
            )
        st.caption(
            "Secrets: top-level TOML key ``OFFLINE_LOCAL_ARCHIVE`` (same spelling as the environment variable). "
            "Optional: ``OFFLINE_DOSSIER_INFO_XLSX``, ``IUCLID_FORMAT_DIR``."
        )
        try:
            st.page_link("pages/02_Offline_Loader_Test.py", label="Open **Offline ECHA loader** test page", icon="🧪")
        except Exception:
            st.caption("Sidebar → **Offline ECHA loader** to verify paths and snapshots.")


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
    toml_hits = _candidate_secrets_toml_paths()
    with st.expander("REACH / IUCLID (offline dossier)", expanded=False):
        if not fp:
            st.info(
                "No **OFFLINE_LOCAL_ARCHIVE** value is visible in ``os.environ`` after syncing from "
                "``.streamlit/secrets.toml`` and ``st.secrets``. Use a **top-level** TOML key exactly named "
                "``OFFLINE_LOCAL_ARCHIVE`` (same spelling as the environment variable), or set the variable in "
                "PowerShell **before** ``streamlit run app.py``."
            )
            st.caption(
                f"Checked TOML: {len(toml_hits)} file(s) found — "
                f"{', '.join(p.as_posix() for p in toml_hits) if toml_hits else 'none at cwd or app root `.streamlit/secrets.toml`'}"
            )
            try:
                st.page_link("pages/02_Offline_Loader_Test.py", label="Open **Offline ECHA loader** test page", icon="🧪")
            except Exception:
                st.caption("Use the sidebar multipage entry **Offline ECHA loader** to verify paths and load snapshots.")
            return

        try:
            archive_path = Path(fp).expanduser()
        except OSError as exc:
            st.warning(f"**OFFLINE_LOCAL_ARCHIVE** could not be interpreted as a path: `{fp}` ({exc}).")
            return

        if not archive_path.exists():
            st.warning(
                f"**OFFLINE_LOCAL_ARCHIVE** is set to `{fp}`, but that path **does not exist** on this server "
                "(wrong path, not deployed with the repo, or a laptop-only absolute path). "
                "On **Streamlit Cloud**, use a path under the repo (e.g. a small committed subset) or host the archive "
                "externally and download it at startup. See README → **Offline REACH / IUCLID (optional)**."
            )
            st.caption(
                f"Checked TOML: {len(toml_hits)} file(s) found — "
                f"{', '.join(p.as_posix() for p in toml_hits) if toml_hits else 'none at cwd or app root `.streamlit/secrets.toml`'}"
            )
            try:
                st.page_link("pages/02_Offline_Loader_Test.py", label="Open **Offline ECHA loader** test page", icon="🧪")
            except Exception:
                st.caption("Use the sidebar multipage entry **Offline ECHA loader** to verify paths and load snapshots.")
            return

        if using_committed_reach_demo_archive():
            st.info(
                "**Demo IUCLID / REACH database:** you are using the committed archive "
                "`data/reach_demo/reach_subset.zip`. It is only a **small subset** of REACH dossiers for demos "
                "and Cloud file limits — **not** the official full export. Most CAS numbers will have **no** dossier; "
                "study snippets and classifications may be **missing, truncated, or incomplete**. "
                "Parsing uses **heuristics**, not a full IUCLID engine — **not** for regulatory, registration, "
                "or completeness claims. Use ECHA’s downloads and a local full archive when you need authoritative data."
            )

        c1, c2 = st.columns([1, 2])
        ctx = get_offline_context(fp)
        cas_target_uuids = ctx.uuids_for_cas(cas_norm) if ctx is not None else []
        force_refresh_cache = st.checkbox(
            "Force refresh cached UUIDs",
            value=False,
            key=f"force_refresh_iuclid_cache_{cas_norm}",
            help="When enabled, re-parse and overwrite existing cache rows for matched dossier UUIDs.",
        )
        if c1.button(
            "Rebuild IUCLID snippet cache",
            key=f"rebuild_iuclid_cache_{cas_norm}",
            help="Re-parse only dossiers matched for this CAS and store snippets in SQLite cache.",
        ):
            with st.spinner("Rebuilding IUCLID snippet cache from .i6z files..."):
                try:
                    stats = rebuild_iuclid_cache(
                        force_extract=False,
                        verbose_debug=False,
                        target_uuids=cas_target_uuids,
                        skip_existing_cache=not force_refresh_cache,
                    )
                    st.success(
                        "IUCLID cache rebuilt: "
                        f"{stats.get('parsed', 0)}/{stats.get('i6z_total', 0)} dossiers parsed; "
                        f"skipped cached={stats.get('skipped_cached', 0)}; "
                        f"CL rows={stats.get('cl_rows', 0)}, endpoint rows={stats.get('endpoint_rows', 0)}"
                    )
                except Exception as exc:
                    logger.exception("Failed to rebuild IUCLID snippet cache")
                    st.error(f"Cache rebuild failed: {type(exc).__name__}: {exc}")
        c2.caption(
            f"Snippet cache DB: `{cache_db_path()}` | CAS-target UUIDs: `{len(cas_target_uuids)}` | "
            "SKIP_EXISTING_CACHE=true by default"
        )

        if ctx is None:
            st.error(
                "**Offline archive path is set**, but the REACH index could not be loaded. "
                "Confirm the path exists, is readable, and snapshots under ``OFFLINE_CACHE_DIR`` can be built."
            )
            if _last_offline_context_error:
                with st.expander("Error detail", expanded=False):
                    st.code(_last_offline_context_error)
            try:
                st.page_link("pages/02_Offline_Loader_Test.py", label="Open **Offline ECHA loader** test page", icon="🧪")
            except Exception:
                st.caption("Sidebar → **Offline ECHA loader** for a standalone loader test.")
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
            cap = (
                "If you expect a hit, confirm CAS formatting, merge **OFFLINE_DOSSIER_INFO_XLSX**, or rebuild snapshots."
            )
            if using_committed_reach_demo_archive():
                cap += (
                    " With the **repo demo zip**, absent dossiers are **expected** for almost all substances — "
                    "expand the subset or point **OFFLINE_LOCAL_ARCHIVE** at a full local archive."
                )
            st.caption(cap)
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
            if "h_statement_code" in cdf.columns:
                cdf["h_statement_phrase"] = cdf["h_statement_code"].map(
                    lambda x: ghs_formatter.get_h_phrase(str(x).strip())
                    if str(x).strip().upper().startswith("H")
                    else ""
                )
            if "h_statement_code_label" in cdf.columns:
                cdf["h_statement_code_display"] = cdf.apply(
                    lambda r: str(r.get("h_statement_code_label") or "").strip()
                    if str(r.get("h_statement_code_label") or "").strip()
                    and str(r.get("h_statement_code_label") or "").strip() != str(r.get("h_statement_code") or "").strip()
                    else f"{str(r.get('h_statement_code') or '').strip()} (unmapped)",
                    axis=1,
                )
            st.dataframe(
                hazard_report_utils.clean_dataframe(cdf),
                use_container_width=True,
                hide_index=True,
                height=min(400, 35 * (len(cl_rows) + 2)),
            )
        else:
            st.caption("No C&L / GHS-style rows in the offline hazard snapshot for this substance (common for Study Results–only builds).")

        eps_norm = data.get("iuclid_endpoints_normalized") or []
        eps = data.get("iuclid_endpoints") or []
        if eps_norm:
            st.markdown("**Study endpoint snippets (structured normalized fields)**")
            ndf_full = pd.DataFrame(eps_norm)

            def _has_text(series: pd.Series) -> pd.Series:
                return series.fillna("").astype(str).str.strip().ne("")

            elv = ndf_full.get("effect_level_value", pd.Series(index=ndf_full.index, dtype=float))
            sp = ndf_full.get("species_label", pd.Series("", index=ndf_full.index))
            dv = ndf_full.get("duration_value", pd.Series(index=ndf_full.index, dtype=float))
            epl = ndf_full.get("effect_endpoint_label", pd.Series("", index=ndf_full.index))

            has_structured_hazard = (
                elv.notna()
                | _has_text(sp)
                | dv.notna()
                | _has_text(epl)
            )
            with_data_rows = ndf_full[has_structured_hazard].copy()
            missing_rows = ndf_full[~has_structured_hazard].copy()

            endpoint_name_col = "endpoint_name_label" if "endpoint_name_label" in ndf_full.columns else "endpoint_name"
            if endpoint_name_col in missing_rows.columns:
                missing_agg = (
                    missing_rows.groupby(endpoint_name_col, dropna=False)
                    .size()
                    .reset_index(name="rows_missing_structured_data")
                    .sort_values("rows_missing_structured_data", ascending=False)
                    .rename(columns={endpoint_name_col: "endpoint_name_label"})
                )
            else:
                missing_agg = pd.DataFrame(columns=["endpoint_name_label", "rows_missing_structured_data"])

            # Phrase mapping diagnostics (full normalized set, no filtering).
            code_label_pairs = [
                ("study_result_type_code", "study_result_type_label"),
                ("purpose_flag_code", "purpose_flag_label"),
                ("reliability_code", "reliability_label"),
                ("species_code", "species_label"),
                ("strain_code", "strain_label"),
                ("sex_code", "sex_label"),
                ("administration_exposure_code", "administration_exposure_label"),
                ("effect_endpoint_code", "effect_endpoint_label"),
                ("based_on_code", "based_on_label"),
            ]
            gap_rows: list[dict[str, Any]] = []
            for code_col, label_col in code_label_pairs:
                if code_col not in ndf_full.columns:
                    continue
                tmp = ndf_full[
                    [code_col]
                    + ([label_col] if label_col in ndf_full.columns else [])
                    + ([endpoint_name_col] if endpoint_name_col in ndf_full.columns else [])
                ].copy()
                tmp[code_col] = tmp[code_col].fillna("").astype(str).str.strip()
                if label_col in tmp.columns:
                    tmp[label_col] = tmp[label_col].fillna("").astype(str).str.strip()
                unmapped = tmp[tmp[code_col].astype(str).str.fullmatch(r"\d+")].copy()
                if unmapped.empty:
                    continue
                if label_col in tmp.columns:
                    unresolved_label = (unmapped[label_col] == "") | (unmapped[label_col] == unmapped[code_col])
                else:
                    unresolved_label = pd.Series(True, index=unmapped.index)
                unresolved_lookup = ~unmapped[code_col].map(has_phrase_mapping)
                bad = unmapped[unresolved_label | unresolved_lookup]
                if bad.empty:
                    continue
                bad["field"] = code_col.replace("_code", "")
                bad["code"] = bad[code_col]
                bad["endpoint"] = bad[endpoint_name_col] if endpoint_name_col in bad.columns else ""
                gap_rows.extend(bad[["field", "code", "endpoint"]].drop_duplicates().to_dict(orient="records"))
            if gap_rows:
                gdf = pd.DataFrame(gap_rows)
                gdf = (
                    gdf.groupby(["field", "code"], dropna=False)
                    .agg(
                        occurrences=("code", "count"),
                        endpoint_examples=(
                            "endpoint",
                            lambda s: "; ".join(sorted({str(v) for v in s if str(v).strip()})[:3]),
                        ),
                    )
                    .reset_index()
                    .sort_values(["occurrences", "field", "code"], ascending=[False, True, True])
                    .head(20)
                )
                with st.expander("Potential phrase-mapping gaps", expanded=False):
                    st.caption(
                        "Numeric picklist codes that still look unmapped in the phrase package "
                        "(blank label, label equals code, or missing from the mapper index). Sample up to 20."
                    )
                    st.dataframe(
                        hazard_report_utils.clean_dataframe(gdf),
                        use_container_width=True,
                        hide_index=True,
                        height=min(320, 28 * (len(gdf) + 2)),
                    )

            def _duration_display(row: pd.Series) -> str:
                du = str(row.get("duration_unit") or "").strip()
                dv2 = row.get("duration_value")
                dr = str(row.get("duration_raw") or "").strip()
                if dv2 is not None and pd.notna(dv2):
                    try:
                        fv = float(dv2)
                        if fv == int(fv):
                            base = str(int(fv))
                        else:
                            base = str(fv).rstrip("0").rstrip(".")
                    except (TypeError, ValueError):
                        base = str(dv2).strip()
                    if du:
                        return f"{base} {du}".strip()
                    return base
                return dr

            def _key_badge(v: Any) -> str:
                try:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return "—"
                    return "✓" if int(float(v)) == 1 else "—"
                except (TypeError, ValueError):
                    return "—"

            display_cols = [
                "endpoint_name_label",
                "effect_level_value",
                "effect_level_unit",
                "species_label",
                "study_result_type_label",
                "duration_display",
                "reliability_label",
                "key_result",
            ]
            wdf = with_data_rows.copy()
            if not wdf.empty:
                wdf["duration_display"] = wdf.apply(_duration_display, axis=1)
                wdf["key_result"] = wdf.get("key_result", pd.Series(0, index=wdf.index)).map(_key_badge)
                for c in ("endpoint_name_label", "effect_level_value", "effect_level_unit", "species_label", "study_result_type_label", "reliability_label"):
                    if c not in wdf.columns:
                        wdf[c] = ""
                wdf = wdf[[c for c in display_cols if c in wdf.columns]]

            st.markdown("**Endpoint templates with extracted data**")
            if wdf.empty:
                st.caption("No rows with structured hazard fields yet (effect level, species, duration value, or effect endpoint).")
            else:
                st.dataframe(
                    hazard_report_utils.clean_dataframe(wdf),
                    use_container_width=True,
                    hide_index=True,
                    height=min(460, 28 * (min(len(wdf), 60) + 2)),
                )
                st.download_button(
                    "Download rows with structured hazard data (CSV)",
                    data=wdf.to_csv(index=False).encode("utf-8"),
                    file_name=f"iuclid_structured_with_data_{cas_norm}.csv",
                    mime="text/csv",
                    key=f"dl_structured_with_data_{cas_norm}",
                )

            st.markdown("**Endpoint templates missing structured data**")
            if missing_agg.empty:
                st.caption("Every normalized row has at least one structured hazard field.")
            else:
                st.dataframe(
                    hazard_report_utils.clean_dataframe(missing_agg),
                    use_container_width=True,
                    hide_index=True,
                    height=min(300, 28 * (min(len(missing_agg), 40) + 2)),
                )
                st.download_button(
                    "Download missing-data summary (CSV)",
                    data=missing_agg.to_csv(index=False).encode("utf-8"),
                    file_name=f"iuclid_structured_missing_summary_{cas_norm}.csv",
                    mime="text/csv",
                    key=f"dl_structured_missing_{cas_norm}",
                )
        elif eps:
            st.markdown("**Study endpoint snippets (heuristic scan of ``Document.i6d``)**")
            edf = pd.DataFrame(eps)
            if "endpoint_name" in edf.columns:
                if "endpoint_name_label" in edf.columns:
                    edf["endpoint_name_display"] = edf.apply(
                        lambda r: str(r.get("endpoint_name_label") or "").strip()
                        if str(r.get("endpoint_name_label") or "").strip()
                        and str(r.get("endpoint_name_label") or "").strip() != str(r.get("endpoint_name") or "").strip()
                        else f"{str(r.get('endpoint_name') or '').strip()} (unmapped)",
                        axis=1,
                    )
                else:
                    edf["endpoint_name_display"] = edf["endpoint_name"].astype(str) + " (unmapped)"
            if "uuid" in edf.columns:
                edf = edf[["uuid", "endpoint_name_display", "result", "units", "species"]] if all(
                    c in edf.columns for c in ("endpoint_name_display", "result")
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
