"""
Quick Hazard Assessment — Streamlit app (v2.0).

Multi-source hazard assessment (PubChem, DSSTox, ToxValDB, CPDB) with optional OPERA QSAR
from a pre-computed cache.
"""

from __future__ import annotations

import io
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

import config
from utils import cas_validator, chemical_db, data_formatter, dsstox_local, ghs_formatter, pubchem_client, smiles_drawer
from utils import atmo_gwp, hazard_for_p2oasys, hazard_report_utils, iarc_lookup, lookup_tables, p2oasys_aggregate, p2oasys_scorer
from utils import summary_utils
from utils.input_handler import get_input_handler
from utils.markitdown_check import is_markitdown_available
from utils.sds_integration import apply_assessment_query

try:
    from utils import sds_pdf_utils, sds_regex_extractor
except ImportError:
    sds_pdf_utils = sds_regex_extractor = None  # optional: SDS PDF flow (v1.4)

try:
    from utils import carcinogenic_potency_client
except ImportError:
    carcinogenic_potency_client = None  # optional: not present in some deployments

try:
    from utils import qsar_toolbox_client
except ImportError:
    qsar_toolbox_client = None  # optional: OECD QSAR Toolbox + VEGA (Windows, PyQSARToolbox)


def _is_assessment_result(obj: Any) -> bool:
    """
    Duck-type check — avoids Streamlit reload where ``isinstance(x, AssessmentResult)``
    fails because the service was constructed with a different class object than
    ``from services.chemical_assessment import AssessmentResult`` in this run.
    """
    return obj is not None and hasattr(obj, "identity") and hasattr(obj, "pubchem_data")


def _reach_iuclid_panel_unconfigured_fallback(code: str) -> None:
    """
    Same UX as ``render_reach_iuclid_panel_unconfigured`` when that helper is missing from an
    older ``iuclid_integration`` (avoids ``ImportError: cannot import name ...`` on partial deploys).
    """
    with st.expander("REACH / IUCLID (offline dossier)", expanded=False):
        try:
            from services.config import ServiceConfig

            on_cloud = ServiceConfig.is_streamlit_cloud()
        except Exception:
            on_cloud = False
        cloud_note = (
            "**Streamlit Cloud:** the full ECHA REACH export is too large for GitHub. "
            "A **committed demo** (`data/reach_demo/reach_subset.zip`) covers only a **tiny subset** — "
            "most CAS have no dossier; data may be **missing or partial**; parsing is **heuristic**. "
            "For full coverage, run **locally** with ``OFFLINE_LOCAL_ARCHIVE`` to your official archive.\n\n"
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
                    "# Optional — phrase/picklist format tree; copy \"IUCLID 6 9.0.0_format\" into repo (no spaces)\n"
                    'IUCLID_FORMAT_DIR = "/mount/src/quick-hazard-assessment-app/data/iuclid_format/IUCLID_6_9_0_0_format"\n'
                    "\n"
                    "# Dossier lookup — demo zip only (NOT full REACH; incomplete by design)\n"
                    'OFFLINE_LOCAL_ARCHIVE = "/mount/src/quick-hazard-assessment-app/data/reach_demo/reach_subset.zip"\n',
                    language="toml",
                )
                st.caption(
                    "Save Secrets, then **Reboot**. Read **`data/echa_cloud/README.txt`** for what to upload vs keep local."
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


# Page config
st.set_page_config(page_title=config.APP_TITLE, layout="centered", initial_sidebar_state="collapsed")

MARKITDOWN_OK, _MARKITDOWN_ERR = is_markitdown_available()

# Offline REACH / IUCLID: mirror secrets into os.environ before any ingest reads env (optional package).
_IUCLID_SYNC_FAILED: str | None = None
try:
    from unified_hazard_report.iuclid_integration import sync_offline_secrets_from_st_secrets

    sync_offline_secrets_from_st_secrets()
except ModuleNotFoundError as exc:
    _IUCLID_SYNC_FAILED = str(exc)
    logging.getLogger(__name__).warning("IUCLID integration module not found: %s", exc)
except Exception as exc:
    _IUCLID_SYNC_FAILED = f"{type(exc).__name__}: {exc}"
    logging.getLogger(__name__).warning("IUCLID secrets sync skipped: %s", exc)

# Session state: persist query and result to avoid re-fetching on every rerun
if "query" not in st.session_state:
    st.session_state["query"] = None
if "result_for" not in st.session_state:
    st.session_state["result_for"] = None
if "result_data" not in st.session_state:
    st.session_state["result_data"] = None  # { "pubchem": ..., "dsstox_info": ..., "clean_cas": ... }

if _IUCLID_SYNC_FAILED and not st.session_state.get("_iuclid_sync_banner_shown"):
    st.session_state["_iuclid_sync_banner_shown"] = True
    st.info(
        "**IUCLID offline hooks did not load** (optional). The rest of the app runs; REACH / IUCLID expander may be limited. "
        f"Reason: `{_IUCLID_SYNC_FAILED}`"
    )

# GHS display preferences (persist during session)
if "show_h_phrases" not in st.session_state:
    st.session_state["show_h_phrases"] = True
if "show_p_phrases" not in st.session_state:
    st.session_state["show_p_phrases"] = True
if "show_signal_word" not in st.session_state:
    st.session_state["show_signal_word"] = True
if "ghs_layout" not in st.session_state:
    st.session_state["ghs_layout"] = "two_columns"
if "sds_staged_chemical_input" not in st.session_state:
    st.session_state["sds_staged_chemical_input"] = None
if "_last_sds_upload_name" not in st.session_state:
    st.session_state["_last_sds_upload_name"] = None
# Shared SDS PDF for CAS extraction
if "shared_sds_pdf_bytes" not in st.session_state:
    st.session_state["shared_sds_pdf_bytes"] = None
if "shared_sds_pdf_name" not in st.session_state:
    st.session_state["shared_sds_pdf_name"] = None
if "sds_extraction_pipeline" not in st.session_state:
    try:
        from utils.alternative_extraction import normalize_sds_pipeline_mode

        _raw = (
            os.environ.get("HAZQUERY_EXTRACTION_PIPELINE", "").strip()
            or os.environ.get("HAZQUERY_DEFAULT_SDS_PIPELINE", "").strip()
            or getattr(config, "DEFAULT_SDS_EXTRACTION_PIPELINE", "hybrid_md_ocr")
        )
        st.session_state["sds_extraction_pipeline"] = normalize_sds_pipeline_mode(str(_raw))
    except Exception:
        st.session_state["sds_extraction_pipeline"] = "hybrid_md_ocr"
else:
    try:
        from utils.alternative_extraction import SUPPORTED_SDS_PIPELINES, normalize_sds_pipeline_mode

        _pip = st.session_state.get("sds_extraction_pipeline")
        if _pip not in SUPPORTED_SDS_PIPELINES:
            st.session_state["sds_extraction_pipeline"] = normalize_sds_pipeline_mode(str(_pip or ""))
    except Exception:
        pass
if "pdf_cache_behavior" not in st.session_state:
    st.session_state["pdf_cache_behavior"] = "use"
if "sds_ocr_engine" not in st.session_state:
    st.session_state["sds_ocr_engine"] = "tesseract"
if "sds_tesseract_psm" not in st.session_state:
    st.session_state["sds_tesseract_psm"] = 6

# Prefer SQLite chemical DB when present (fast). CSV DSSTox is loaded lazily (cached) in the
# sidebar so the page title renders before a large mapping file is read (helps Streamlit Cloud).
db_stats = chemical_db.get_db_stats()
use_sqlite_dsstox = db_stats.get("dsstox", {}).get("exists", False)
use_sqlite_toxval = db_stats.get("toxvaldb", {}).get("exists", False)

_LOG = logging.getLogger(__name__)


@st.cache_resource(show_spinner="Loading DSSTox (CSV)…")
def _cached_dsstox_csv_data() -> Optional[dict[str, Any]]:
    """Load DSS/ CSV mapping once per worker. Returns None on failure or empty."""
    try:
        return dsstox_local.load_dsstox_enhanced()
    except Exception as e:
        _LOG.warning("DSSTox CSV load failed: %s", e)
        return None


dsstox_data: Optional[dict[str, Any]] = None

# Title and description
st.title(f"🧪 {config.APP_TITLE}")
if not MARKITDOWN_OK:
    st.error(_MARKITDOWN_ERR or "MarkItDown is required for SDS PDF parsing.")
    st.caption("Typed CAS/name still works below. SDS upload is disabled until MarkItDown is installed.")

# Sidebar: database stats (SQLite or CSV)
with st.sidebar:
    if not use_sqlite_dsstox:
        dsstox_data = _cached_dsstox_csv_data()
    st.header("📊 Local database")
    if use_sqlite_dsstox:
        dsstox_records = int(db_stats.get("dsstox", {}).get("records") or 0)
        st.success(f"✅ DSSTox (SQLite): {dsstox_records:,} compounds")
    elif dsstox_data:
        stats = dsstox_local.get_dsstox_summary_stats(dsstox_data)
        st.success(f"✅ DSSTox (CSV): {stats.get('total_compounds', 0)} compounds")
        st.caption(
            f"{stats.get('with_dtxsid', 0)} with DTXSID, "
            f"{stats.get('with_preferred_name', 0)} with names"
        )
    else:
        st.warning("DSSTox not loaded (PubChem-only mode).")
    if use_sqlite_toxval:
        tox_records = int(db_stats.get("toxvaldb", {}).get("records") or 0)
        tox_chems = int(db_stats.get("toxvaldb", {}).get("chemicals") or 0)
        st.success(f"✅ ToxValDB (SQLite): {tox_records:,} records")
        st.caption(f"{tox_chems:,} chemicals")
    else:
        st.error("ToxValDB (SQLite) not found. Build it locally with `scripts/setup_chemical_db.py`.")
    if carcinogenic_potency_client and carcinogenic_potency_client.is_available():
        st.success(f"✅ {carcinogenic_potency_client.DISPLAY_NAME} (SQLite)")
    elif carcinogenic_potency_client:
        st.caption(f"{carcinogenic_potency_client.DISPLAY_NAME} not loaded.")
    with st.expander("Assessment service (runtime)", expanded=False):
        try:
            from services.config import ServiceConfig

            st.markdown(ServiceConfig.get_capability_message())
        except Exception as e:
            st.caption(str(e))
    if config.USE_PUBCHEM_CAS_VALIDATION:
        try:
            from utils.pubchem_validator import get_pubchem_validator

            pubchem_stats = get_pubchem_validator().get_stats()
            if pubchem_stats.get("total_checked", 0) > 0:
                with st.expander("🔍 PubChem CAS validation", expanded=False):
                    st.metric("CAS checked", pubchem_stats["total_checked"])
                    st.metric("Found in PubChem", pubchem_stats["found_in_pubchem"])
                    st.metric("Not found", pubchem_stats["not_found"])
                    st.caption(
                        "Only checksum-valid CAS shown; no invalid or made-up CAS. "
                        + ("PubChem-verified only. Set SHOW_ONLY_PUBCHEM_VERIFIED=0 to include unverified." if config.SHOW_ONLY_PUBCHEM_VERIFIED else "Unverified included.")
                    )
        except Exception:
            pass
    try:
        from utils.sds_debug import render_sds_debug_sidebar_controls

        render_sds_debug_sidebar_controls()
    except ImportError:
        pass
    try:
        from utils.sds_strategy import PRESETS, get as strategy_get

        with st.expander("🧪 SDS extraction strategy (test combos)", expanded=False):
            st.caption("Override extraction settings. Re-upload SDS to test. Resets on page reload.")
            preset = st.selectbox(
                "Preset",
                options=["(config default)", "docling_pubchem", *(k for k in PRESETS.keys() if k != "docling_pubchem")],
                key="sds_strategy_preset",
                format_func=lambda k: PRESETS.get(k, {}).get("label", k) if k != "(config default)" else k,
            )
            if preset != "(config default)":
                st.session_state["sds_strategy_override"] = {k: v for k, v in PRESETS[preset].items() if k != "label"}
            else:
                st.session_state["sds_strategy_override"] = {}
            if st.session_state.get("sds_strategy_override"):
                st.json({k: v for k, v in st.session_state["sds_strategy_override"].items()})
            with st.expander("📋 Strategy env overrides (legacy unified parser)", expanded=False):
                st.markdown("""
**SDS PDF CAS upload** uses **v1.4 only** (MarkItDown + regex or Hybrid). See
[docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md).

Presets below tweak **USE_DOCLING**, **USE_OCR**, etc. for any code paths that still
read ``utils.sds_strategy`` (not the primary SDS upload extractor).

| Option | Effect |
|--------|--------|
| USE_DOCLING | IBM Docling (where still used) |
| USE_OCR | Tesseract (where still used) |
| SHOW_ONLY_PUBCHEM_VERIFIED | Hide unverified CAS |
""")
    except ImportError:
        pass
    try:
        from utils.alternative_extraction import PIPELINE_LABELS, PIPELINE_SIDEBAR_ORDER

        _v14_expanded = bool(sds_pdf_utils and sds_regex_extractor)
        with st.expander("📄 SDS CAS extraction (v1.4 — two pipelines only)", expanded=_v14_expanded):
            st.markdown(
                "Only **Hybrid** and **MarkItDown + regex** are supported. "
                "See [docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md) for why other parsers were removed.\n\n"
                "**Hybrid:** MarkItDown first; **OCR** (under *Advanced OCR options*) runs only if no CAS is found. "
                "Requires `pip install 'markitdown[pdf]'`; OCR fallback needs **Poppler** + **Tesseract** or **EasyOCR** on PATH "
                "or `HAZQUERY_POPPLER_PATH`."
            )
            if not MARKITDOWN_OK:
                st.warning(
                    "MarkItDown not installed — v1.4 SDS pipelines cannot run. "
                    "Run: `pip install 'markitdown[pdf]'`"
                )
            opts = [k for k in PIPELINE_SIDEBAR_ORDER if k in PIPELINE_LABELS]
            st.selectbox(
                "Extraction strategy",
                options=opts,
                key="sds_extraction_pipeline",
                format_func=lambda k: PIPELINE_LABELS.get(k, k),
            )
            st.selectbox(
                "Cache behavior",
                options=["use", "force", "clear_once"],
                key="pdf_cache_behavior",
                format_func=lambda x: {
                    "use": "Use cache if available",
                    "force": "Force reprocess (ignore cache)",
                    "clear_once": "Clear cache once, then use cache",
                }.get(x, x),
            )
            with st.expander("Advanced OCR options", expanded=False):
                st.caption("Used only when **Hybrid** runs OCR because MarkItDown found no CAS.")
                st.selectbox(
                    "OCR engine (hybrid fallback)",
                    options=["tesseract", "easyocr", "llm_vision"],
                    key="sds_ocr_engine",
                    format_func=lambda x: {
                        "tesseract": "Tesseract (default)",
                        "easyocr": "EasyOCR (slower)",
                        "llm_vision": "LLM vision (falls back to Tesseract)",
                    }.get(x, x),
                    disabled=False,
                )
                st.number_input(
                    "Tesseract PSM (6=block, 11=sparse)", min_value=0, max_value=13, key="sds_tesseract_psm"
                )
            st.caption(
                "Env: `HAZQUERY_EXTRACTION_PIPELINE`, `HAZQUERY_DEFAULT_SDS_PIPELINE`, "
                "`HAZQUERY_EXTRACTION_CACHE`, `HAZQUERY_POPPLER_PATH`."
            )
    except ImportError:
        pass

st.info(
    "**Quick Hazard Assessment v2.0** — Enter a CAS, name, or SMILES — or upload an SDS PDF.\n\n"
    "• Real-time lookup from PubChem, DSSTox, ToxValDB, CPDB\n"
    "• **OPERA QSAR** predictions (batch pre-computed, cached)\n\n"
    "Core pipeline: **ChemicalAssessmentService** (multi-source evidence)."
)

# Banner when CAS was chosen from SDS upload or unified parser (results render below this message)
if st.session_state.pop("show_assessment_from_unified", False):
    q = st.session_state.get("query") or ""
    st.success(
        f"**Database assessment started for:** `{q}`. "
        "Scroll down (or stay on this page) to view **Hazard assessment** and **P2OASys scoring** tabs below."
    )
    extra = st.session_state.pop("unified_assess_note", None)
    if extra:
        st.info(extra)

# --- Unified chemical input: CAS first, SDS upload below (1A) ---
st.markdown("### Chemical input")

# Widget key `cas_query_input` must only be updated *before* st.text_input runs (Streamlit rule).
_pending_cas = st.session_state.pop("_pending_cas_query_input", None)
if _pending_cas is not None:
    st.session_state["cas_query_input"] = _pending_cas

with st.form("cas_input", clear_on_submit=False):
    default = st.session_state.get("query") or st.session_state.get("cas_query_input") or ""
    if "cas_query_input" not in st.session_state:
        st.session_state["cas_query_input"] = default
    cas = st.text_input(
        "CAS, chemical name, or SMILES",
        key="cas_query_input",
        placeholder="e.g., 67-64-1, acetone, or CC(=O)C",
        help="Type directly or upload SDS below to auto-fill CAS.",
    )
    submitted = st.form_submit_button("Assess")

# --- SDS PDF path (input_handler + alternative_extraction; does not run through assess() until user picks a CAS) ---
# SDS upload below CAS input (requires MarkItDown for v1.4 extraction)
uf_shared = None
if sds_pdf_utils and sds_regex_extractor and MARKITDOWN_OK:
    uf_shared = st.file_uploader(
        "Or upload SDS (PDF) to extract CAS",
        type=["pdf"],
        key="shared_sds_pdf_upload",
        help="Parsing runs automatically. Single CAS fills the field above; multiple CAS: pick one below.",
    )
    if uf_shared is not None:
        st.session_state["shared_sds_pdf_bytes"] = uf_shared.getvalue()
        st.session_state["shared_sds_pdf_name"] = uf_shared.name
        if st.session_state.get("_last_sds_upload_name") != uf_shared.name:
            st.session_state["_last_sds_upload_name"] = uf_shared.name
            st.session_state["sds_staged_chemical_input"] = None
            with st.spinner("Extracting CAS from SDS…"):
                staged = get_input_handler().process_sds_pdf(uf_shared)
            st.session_state["sds_staged_chemical_input"] = staged
            st.session_state["_last_sds_upload_name"] = uf_shared.name
            if staged and staged.cas_numbers and len(staged.cas_numbers) == 1:
                st.session_state["_pending_cas_query_input"] = staged.cas_numbers[0]
                st.rerun()
    else:
        st.session_state["shared_sds_pdf_bytes"] = None
        st.session_state["shared_sds_pdf_name"] = None
        st.session_state["_last_sds_upload_name"] = None
        st.session_state["sds_staged_chemical_input"] = None

staged_ci = st.session_state.get("sds_staged_chemical_input")
if staged_ci is not None and staged_ci.cas_numbers:
    if len(staged_ci.cas_numbers) > 1:
        st.caption(f"**{len(staged_ci.cas_numbers)} CAS** from SDS. Choose one and assess:")
        if staged_ci.extraction_rows:
            _df_sds = pd.DataFrame(staged_ci.extraction_rows)
            _show_cols = [c for c in ("cas", "chemical_name", "concentration", "confidence", "pubchem_verified", "name_validated", "method", "source") if c in _df_sds.columns]
            _disp = _df_sds[_show_cols].copy() if _show_cols else _df_sds
            if "confidence" in _disp.columns:
                _disp["confidence"] = _disp["confidence"].apply(lambda x: f"{float(x):.0%}" if x is not None else "—")
            _col_config = {
                "cas": st.column_config.TextColumn("CAS", width="small"),
                "chemical_name": st.column_config.TextColumn("Chemical name", width="large"),
                "concentration": st.column_config.TextColumn("Concentration", width="medium"),
                "confidence": st.column_config.TextColumn("Confidence", width="small", help="Graduated score; high = validated"),
                "method": st.column_config.TextColumn("Method", width="small"),
                "source": st.column_config.TextColumn("Source", width="small"),
            }
            if "pubchem_verified" in _disp.columns:
                _col_config["pubchem_verified"] = st.column_config.CheckboxColumn("In PubChem", disabled=True, help="CAS verified in PubChem")
            if "name_validated" in _disp.columns:
                _col_config["name_validated"] = st.column_config.CheckboxColumn("Name match", disabled=True)
            st.dataframe(_disp, use_container_width=True, hide_index=True, column_config={k: v for k, v in _col_config.items() if k in _disp.columns})
            st.caption(
                "**Confidence:** High (80–100%) = multiple validations; Medium (50–80%) = some checks. "
                "Use manual correction below to override or add CAS."
            )
        pick = st.selectbox("CAS for assessment", options=staged_ci.cas_numbers, key="top_sds_cas_pick")
        if st.button("Assess selected CAS", type="primary", key="top_sds_run_assess_btn"):
            apply_assessment_query(pick, show_banner=True, banner_note=f"Assessing **{pick}** from SDS.")
            st.rerun()

        with st.expander("✏️ Manual correction"):
            st.markdown("If the extracted CAS is incorrect, you can:")
            col1, col2 = st.columns(2)
            with col1:
                correct_cas = st.text_input("Correct CAS number:", key="manual_cas_override", placeholder="e.g., 67-64-1")
                if st.button("Use this CAS", key="manual_cas_use_btn"):
                    if correct_cas and cas_validator.is_valid_cas_format(str(correct_cas).strip()):
                        st.session_state["query"] = cas_validator.normalize_cas_input(correct_cas) or correct_cas.strip()
                        st.session_state["result_for"] = None
                        st.session_state["_pending_cas_query_input"] = correct_cas.strip()
                        st.rerun()
                    elif correct_cas:
                        st.warning("Enter a valid CAS format (e.g., 67-64-1).")
            with col2:
                if st.button("Report issue", key="manual_cas_report_btn"):
                    st.info("Thank you! This helps improve the extractor.")
    elif len(staged_ci.cas_numbers) == 1 and not cas:
        st.caption("CAS extracted — click **Assess** above.")
        with st.expander("✏️ Manual correction"):
            correct_cas = st.text_input("Correct CAS if wrong:", key="manual_cas_single", placeholder="e.g., 67-64-1")
            if st.button("Use this CAS", key="manual_cas_single_btn"):
                if correct_cas and cas_validator.is_valid_cas_format(str(correct_cas).strip()):
                    st.session_state["query"] = cas_validator.normalize_cas_input(correct_cas) or correct_cas.strip()
                    st.session_state["result_for"] = None
                    st.session_state["_pending_cas_query_input"] = correct_cas.strip()
                    st.rerun()
                elif correct_cas:
                    st.warning("Enter a valid CAS format (e.g., 67-64-1).")
elif staged_ci is not None and not staged_ci.cas_numbers:
    _ex_err = getattr(staged_ci, "extraction_error", None)
    if _ex_err:
        st.error(f"**SDS extraction issue:** {_ex_err}")
    st.warning("No CAS extracted from SDS. Type a CAS or name above.")

# Example shortcuts: hide once user has SDS, typed input, or an active assessment query
_typed = (st.session_state.get("cas_query_input") or "").strip()
_hide_examples = (
    uf_shared is not None
    or st.session_state.get("sds_staged_chemical_input") is not None
    or bool(_typed)
    or bool(st.session_state.get("query"))
)
if not _hide_examples:
    st.markdown("**Examples:**")
    example_cols = st.columns(4)
    for i, (example_cas, label) in enumerate(config.EXAMPLE_CHEMICALS):
        if example_cols[i].button(label, key=f"ex_{i}"):
            st.session_state["query"] = example_cas
            st.session_state["_pending_cas_query_input"] = example_cas
            st.session_state["result_for"] = None  # force re-fetch
            st.rerun()
    st.caption(
        "**Assessment scope:** one compound per run — each full lookup runs **PubChem + DSSTox + ToxValDB + CPDB** "
        "(plus **OPERA** cache when configured) for **a single resolved CAS** at a time. "
        "Multi-ingredient SDS: extract CAS list, choose one, assess, then switch CAS and run again."
    )

# When form is submitted, set query to what the user typed
if submitted and cas:
    clean_cas = cas_validator.normalize_cas_input(cas)
    if clean_cas:
        st.session_state["query"] = clean_cas
        st.session_state["_pending_cas_query_input"] = clean_cas
        st.session_state["result_for"] = None
    st.rerun()

# --- Typed CAS / name assessment (ChemicalAssessmentService only; independent of SDS PDF pipeline) ---
# Run assessment when we have a query and either no cached result or result is for a different query
current_query = st.session_state.get("query")
if current_query:
    clean_cas = cas_validator.normalize_cas_input(current_query)
    need_fetch = st.session_state.get("result_for") != clean_cas
    if need_fetch:
        with st.spinner("Fetching data and generating structure..."):
            try:
                from services.chemical_assessment import get_assessment_service

                _svc = get_assessment_service()
                _ar = _svc.assess(current_query)
                if _ar is None:
                    st.error(f"Assessment returned no result for **{current_query}**.")
                    st.session_state["result_for"] = clean_cas
                    st.session_state["result_data"] = None
                else:
                    if isinstance(_ar, list):
                        _ar = _ar[0] if _ar else None
                    if (
                        _ar
                        and _is_assessment_result(_ar)
                        and getattr(_ar, "has_multiple_components", False)
                        and getattr(_ar, "all_components", None)
                    ):
                        _comps = _ar.all_components
                        _ar = _comps[0] if _comps else _ar
                    if not _is_assessment_result(_ar):
                        st.error(f"Unexpected assessment result for **{current_query}** (type: {type(_ar).__name__}). Try a different CAS.")
                        st.session_state["result_for"] = clean_cas
                        st.session_state["result_data"] = None
                    else:
                        st.session_state["result_for"] = clean_cas
                        st.session_state["result_data"] = _svc.to_result_data(_ar)
            except (ValueError, TypeError) as e:
                st.error(f"Could not assess **{current_query}**: {e}")
                st.session_state["result_for"] = clean_cas
                st.session_state["result_data"] = None

    result = st.session_state.get("result_data")
    tab_haz, tab_p2o = st.tabs(["Hazard assessment", "P2OASys scoring"])
    with tab_haz:
        if not result or not result.get("pubchem"):
            msg = "No hazard data yet. Enter a CAS or name above and click **Assess**, or upload an SDS and select a CAS."
            if result and result.get("fetch_error"):
                msg = f"**Could not fetch hazard data:** {result['fetch_error']}. Check network, PubChem connectivity, or try a different CAS."
            st.info(msg)
        elif result and result.get("pubchem"):
            pubchem_data = result["pubchem"]
            dsstox_info = result.get("dsstox_info")
            dtxsid = result.get("dtxsid")
            preferred_name = result.get("preferred_name")
            clean_cas = result["clean_cas"]
            toxval_data = result.get("toxval_data")
            carc_potency_data = result.get("carc_potency_data")

            # --- Summary dashboard (source tabs concept; clean display) ---
            st.subheader("📊 Summary dashboard")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("CAS", hazard_report_utils.clean_text(clean_cas))
            with col2:
                pn = hazard_report_utils.clean_text(preferred_name or "—")
                st.metric("Preferred name", (pn[:18] + "…") if len(pn) > 18 else pn)
            with col3:
                formula = (pubchem_data.get("formula") or "—")
                st.metric("Formula", hazard_report_utils.clean_text(str(formula)))
            with col4:
                mw = pubchem_data.get("mw") or "—"
                st.metric("MW", f"{hazard_report_utils.clean_text(str(mw))} g/mol" if mw != "—" else "—")
            with col5:
                st.metric("DTXSID", hazard_report_utils.clean_text(dtxsid or "—"))
            ghs_summary = hazard_report_utils.build_ghs_summary_df(result)
            if not ghs_summary.empty:
                st.markdown("**GHS classifications**")
                st.dataframe(hazard_report_utils.clean_dataframe(ghs_summary), use_container_width=True, hide_index=True)
            prop_summary = hazard_report_utils.build_property_summary_df(result)
            if not prop_summary.empty:
                st.markdown("**Physical properties**")
                st.dataframe(hazard_report_utils.clean_dataframe(prop_summary), use_container_width=True, hide_index=True)
            coverage = hazard_report_utils.get_source_coverage(result)
            if coverage:
                st.markdown("**Data coverage by source**")
                st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(coverage)), use_container_width=True, hide_index=True)
            st.markdown("---")

            try:
                import unified_hazard_report.iuclid_integration as _iu_mod

                offline_reach_archive_status = _iu_mod.offline_reach_archive_status
                render_reach_iuclid_panel = _iu_mod.render_reach_iuclid_panel
                _iu_render_unconfigured = getattr(_iu_mod, "render_reach_iuclid_panel_unconfigured", None)

                _iu_ok, _iu_code = offline_reach_archive_status()
                if not _iu_ok:
                    if _iu_render_unconfigured is not None:
                        _iu_render_unconfigured(_iu_code)
                    else:
                        _reach_iuclid_panel_unconfigured_fallback(_iu_code)
                else:
                    render_reach_iuclid_panel(str(clean_cas))
            except (ImportError, ModuleNotFoundError) as exc:
                logging.getLogger(__name__).warning("REACH / IUCLID integration import failed: %s", exc)
                with st.expander("REACH / IUCLID (offline dossier)", expanded=False):
                    st.info(
                        "🔒 **IUCLID offline integration is not available** in this deployment "
                        f"({type(exc).__name__}: `{exc}`). "
                        "Run from a full repo checkout or install missing packages; REACH dossiers still require "
                        "``OFFLINE_LOCAL_ARCHIVE`` locally."
                    )
            except Exception as exc:
                logging.getLogger(__name__).exception("REACH / IUCLID panel failed")
                st.error(f"REACH / IUCLID panel error: **{type(exc).__name__}:** {exc}")

            # --- Molecular structure at top ---
            if pubchem_data.get("smiles"):
                st.subheader("Molecular Structure")
                if "mol_draw_style" not in st.session_state:
                    st.session_state["mol_draw_style"] = "acs_1996"
                if "mol_draw_show_h" not in st.session_state:
                    st.session_state["mol_draw_show_h"] = False
                with st.expander("Drawing options", expanded=False):
                    c1, c2 = st.columns(2)
                    with c1:
                        style = st.selectbox(
                            "Style",
                            ["acs_1996", "acs_2006", "nature", "simple"],
                            format_func=lambda x: {
                                "acs_1996": "ACS 1996 (Classic)",
                                "acs_2006": "ACS 2006 (Modern)",
                                "nature": "Nature/Science",
                                "simple": "Simple (Minimal)",
                            }.get(x, x),
                            key="mol_style_select",
                        )
                        st.session_state["mol_draw_style"] = style
                    with c2:
                        show_h = st.checkbox("Show explicit hydrogens", value=False, key="mol_show_h")
                        st.session_state["mol_draw_show_h"] = show_h
                mol_img = smiles_drawer.draw_smiles(
                    pubchem_data["smiles"],
                    width=600,
                    height=350,
                    style=st.session_state["mol_draw_style"],
                    explicit_hydrogens=st.session_state["mol_draw_show_h"],
                )
                if mol_img is not None:
                    st.image(mol_img, width="stretch")
                # If mol_img is None, draw_smiles already rendered the JS fallback
    
            # --- Identifiers and properties in columns ---
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Identifiers")
                st.write(f"**CAS:** {clean_cas}")
                st.write(f"**IUPAC Name:** {hazard_report_utils.clean_text(pubchem_data.get('iupac_name') or 'N/A')}")
                smiles_val = pubchem_data.get("smiles")
                if smiles_val:
                    st.write("**SMILES:**")
                    st.code(smiles_val, language="text")
                else:
                    st.write("**SMILES:** N/A")
                if preferred_name:
                    st.write(f"**Preferred name (DSSTox):** {preferred_name}")
    
                # Enhanced DSSTox display
                display_data = dsstox_local.format_dsstox_display(dsstox_info) if dsstox_info else {}
                if display_data.get("DTXSID"):
                    st.success(f"**DTXSID:** {display_data['DTXSID']} *(from DSSTox local)*")
                else:
                    if dsstox_data is None:
                        st.write("**DTXSID:** DSSTox local database not loaded.")
                    else:
                        st.write("**DTXSID:** This CAS was not found in the local DSSTox file.")
    
                if display_data.get("Names"):
                    with st.expander("📋 DSSTox names", expanded=False):
                        for label, value in display_data["Names"]:
                            st.write(f"**{label}:** {value}")
                if display_data.get("Molecular"):
                    with st.expander("🧪 DSSTox molecular data", expanded=False):
                        for label, value in display_data["Molecular"]:
                            st.write(f"**{label}:** {value}")
                if display_data.get("Structure"):
                    with st.expander("🔬 DSSTox structure identifiers", expanded=False):
                        for label, value in display_data["Structure"]:
                            st.write(f"**{label}:** {value}")
            with col2:
                st.subheader("Key Properties")
                # Build table: Property, Value, Unit, Observations (like toxicity endpoints)
                fp = pubchem_data.get("flash_point")
                vp = pubchem_data.get("vapor_pressure")
                fp_list = [str(x).strip() for x in (fp if isinstance(fp, list) else [fp] if fp else []) if x]
                if not fp_list and fp and not isinstance(fp, list):
                    fp_list = [x.strip() for x in str(fp).split(";") if x.strip()]
                vp_list = [str(x).strip() for x in (vp if isinstance(vp, list) else [vp] if vp else []) if x]
                if not vp_list and vp and not isinstance(vp, list):
                    vp_list = [x.strip() for x in str(vp).split(";") if x.strip()]
                prop_rows = [
                    {"Property": "Molecular Formula", "Value": hazard_report_utils.clean_text(pubchem_data.get("formula") or "—"), "Unit": "—", "Observations": ""},
                    {"Property": "Molecular Weight", "Value": hazard_report_utils.clean_text(str(pubchem_data.get("mw") or "—")), "Unit": "g/mol", "Observations": ""},
                    {"Property": "Flash Point", "Value": hazard_report_utils.clean_text(" | ".join(fp_list) if fp_list else "—"), "Unit": "°C (typical)", "Observations": "Multiple values" if len(fp_list) > 1 else ""},
                    {"Property": "Vapor Pressure", "Value": hazard_report_utils.clean_text(" | ".join(vp_list) if vp_list else "—"), "Unit": "mmHg (typical)", "Observations": "Multiple values" if len(vp_list) > 1 else ""},
                ]
                st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(prop_rows)), width="stretch", hide_index=True)
    
            # --- Toxic doses & toxicity endpoints (no truncation; prioritized + full table + raw) ---
            toxicities = pubchem_data.get("toxicities") or []
            prioritized = data_formatter.prioritize_toxicity_data(pubchem_data, toxval_data)
    
            st.markdown("---")
            st.subheader("📌 Toxic doses & toxicity endpoints")
            tab_prioritized, tab_complete, tab_raw = st.tabs(["📊 Prioritized view", "📋 Complete table", "🔬 Raw data"])
    
            with tab_prioritized:
                st.caption("Quantitative values (with units) first, then categorical. All data shown.")
                if prioritized["quantitative"] or prioritized["categorical"]:
                    df_pri = data_formatter.build_toxicity_display_df(prioritized)
                    st.dataframe(hazard_report_utils.clean_dataframe(df_pri), width="stretch", hide_index=True, height=400)
                else:
                    st.info("No toxicity endpoints found in current data sources.")
    
            with tab_complete:
                st.caption("All endpoints from PubChem and ToxValDB (if available). No truncation.")
                rows = []
                for t in toxicities:
                    rows.append({
                        "Source": "PubChem",
                        "Exposure pathway": t.get("route") or "—",
                        "Species": t.get("species") or "—",
                        "Endpoint": (t.get("type") or "Toxicity").strip(),
                        "Value": t.get("value") or "",
                        "Unit": t.get("unit") or "—",
                    })
                if toxval_data:
                    for _cat, recs in toxval_data.items():
                        for r in recs:
                            rows.append({
                                "Source": "ToxValDB",
                                "Exposure pathway": r.get("route", "—"),
                                "Species": r.get("species", ""),
                                "Endpoint": r.get("study_type", _cat),
                                "Value": str(r.get("value", "")),
                                "Unit": r.get("units", ""),
                            })
                if rows:
                    st.dataframe(hazard_report_utils.clean_dataframe(hazard_report_utils.deduplicate_hazard_data(pd.DataFrame(rows), subset=["Source", "Endpoint", "Value", "Species", "Exposure pathway"])), width="stretch", hide_index=True, height=400)
                else:
                    st.info("No toxicity data available for this compound.")
    
            with tab_raw:
                st.caption("Unmodified data from APIs (for advanced use).")
                raw_sub = st.tabs(["PubChem", "DSSTox", "ToxValDB"])
                with raw_sub[0]:
                    st.json(pubchem_data)
                with raw_sub[1]:
                    if dsstox_info:
                        st.json(dsstox_info)
                    else:
                        st.write("No DSSTox record for this compound.")
                with raw_sub[2]:
                    if toxval_data:
                        st.json(toxval_data)
                    else:
                        st.write("No ToxValDB data (optional: set COMPTOX_API_KEY for EPA ToxValDB).")
    
            # --- Ecotoxicity (aquatic LC50/EC50, species, H4xx) ---
            eco = pubchem_data.get("ecotoxicity") or {}
            eco_entries = eco.get("entries") or []
            h_aquatic = eco.get("h_codes_aquatic") or []
            if eco_entries or h_aquatic:
                st.subheader("🐟 Ecotoxicity")
                if h_aquatic:
                    st.markdown("**Aquatic hazard (GHS):** " + ", ".join(h_aquatic))
                lc = eco.get("aquatic_lc50_mg_l")
                ec = eco.get("aquatic_ec50_mg_l")
                if lc is not None:
                    st.write(f"**LC50 (mg/L):** {lc}")
                if ec is not None:
                    st.write(f"**EC50 (mg/L):** {ec}")
                if eco_entries:
                    # Split into quantitative vs text-only entries
                    quant_rows = []
                    text_rows = []
                    for e in eco_entries:
                        sp = (e.get("species") or "—").strip()
                        endpoint = (e.get("endpoint") or "").upper() or "Toxicity"
                        duration = e.get("duration") or ""
                        raw = e.get("value") or ""
                        unit = e.get("unit") or "mg/L"
                        val_num = e.get("value_num")
                        ci_low = e.get("ci_low")
                        ci_high = e.get("ci_high")
                        ci_str = ""
                        if ci_low is not None and ci_high is not None:
                            ci_str = f"{ci_low}–{ci_high}"
                        if val_num is not None:
                            row = {
                                "Species": sp,
                                "Endpoint": endpoint,
                                "Duration": duration,
                                "Value": val_num,
                                "Unit": unit,
                                "CI (low–high)": ci_str,
                                "Conditions / notes": e.get("conditions") or "",
                            }
                            quant_rows.append(row)
                        else:
                            row = {
                                "Species": sp,
                                "Endpoint": endpoint,
                                "Duration": duration,
                                "Value / excerpt": raw,
                                "Unit": unit,
                                "CI (low–high)": ci_str,
                                "Conditions / notes": e.get("conditions") or "",
                            }
                            text_rows.append(row)
    
                    if quant_rows:
                        st.markdown("**Aquatic toxicity – quantitative endpoints**")
                        st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(quant_rows)), width="stretch", hide_index=True)
                    if text_rows:
                        st.markdown("**Aquatic toxicity – text-only PubChem excerpts**")
                        st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(text_rows)), width="stretch", hide_index=True)
                        # Prefer local Ollama (no API key); fallback to OpenAI if key is set
                        ollama_ok = summary_utils.is_ollama_available(
                            getattr(config, "OLLAMA_HOST", "http://localhost:11434")
                        )
                        api_key = (st.secrets.get("OPENAI_API_KEY") or "").strip() if hasattr(st, "secrets") else ""
                        if ollama_ok or api_key:
                            if st.button("Summarize excerpts with AI", key="summarize_eco_text"):
                                combined = " ".join((r.get("Value / excerpt") or "") for r in text_rows)[:3000]
                                with st.spinner("Summarizing…"):
                                    if ollama_ok:
                                        summary = summary_utils.summarize_text_with_ollama(
                                            combined,
                                            host=getattr(config, "OLLAMA_HOST", "http://localhost:11434"),
                                            model=getattr(config, "OLLAMA_MODEL", "qwen2:0.5b"),
                                        )
                                    else:
                                        summary = summary_utils.summarize_text_with_llm(combined, api_key)
                                if summary:
                                    st.caption("**AI summary:** " + summary)
                                else:
                                    st.caption("Summary unavailable (try again or check Ollama/API key).")
                        else:
                            st.caption("**Summarize excerpts with AI** uses a local LLM (no API key): run [Ollama](https://ollama.ai) and pull a small model (e.g. `ollama pull qwen2:0.5b`). Or add `OPENAI_API_KEY` in app secrets for cloud option.")
    
            # --- Carcinogenic Potency Database ---
            _carc_name = carcinogenic_potency_client.DISPLAY_NAME if carcinogenic_potency_client else "Carcinogenic Potency Database"
            if carcinogenic_potency_client and carcinogenic_potency_client.is_available() and carc_potency_data and carc_potency_data.get("found"):
                st.subheader(f"📊 {_carc_name}")
                experiments = carc_potency_data.get("experiments") or []
                doses = carc_potency_data.get("doses") or []
                if experiments:
                    # Rule-based summary (no LLM)
                    cpdb_summary = summary_utils.summarize_cpdb_experiments(experiments)
                    summary_paragraph = summary_utils.format_cpdb_summary(cpdb_summary)
                    st.info("**Summary:** " + summary_paragraph)
                    ollama_ok_cpdb = summary_utils.is_ollama_available(
                        getattr(config, "OLLAMA_HOST", "http://localhost:11434")
                    )
                    api_key_cpdb = (st.secrets.get("OPENAI_API_KEY") or "").strip() if hasattr(st, "secrets") else ""
                    if (ollama_ok_cpdb or api_key_cpdb) and st.button("One-sentence AI summary", key="summarize_cpdb_ai"):
                        with st.spinner("Summarizing…"):
                            if ollama_ok_cpdb:
                                one_liner = summary_utils.summarize_cpdb_with_ollama(
                                    summary_paragraph,
                                    host=getattr(config, "OLLAMA_HOST", "http://localhost:11434"),
                                    model=getattr(config, "OLLAMA_MODEL", "qwen2:0.5b"),
                                )
                            else:
                                one_liner = summary_utils.summarize_cpdb_with_llm(summary_paragraph, api_key_cpdb)
                        if one_liner:
                            st.caption("**AI:** " + one_liner)
                    elif not (ollama_ok_cpdb or api_key_cpdb):
                        st.caption("Run [Ollama](https://ollama.ai) locally (e.g. `ollama pull qwen2:0.5b`) or set `OPENAI_API_KEY` to enable one-sentence AI summary.")
                    # Experiments: use decoded labels (species_name, route_name, etc.) and opinion_label
                    exp_rows = []
                    for e in experiments[:200]:
                        exp_rows.append({
                            "Species": e.get("species_name") or e.get("species") or "—",
                            "Sex": e.get("sex") or "—",
                            "Strain": e.get("strain_name") or e.get("strain") or "—",
                            "Route": e.get("route_name") or e.get("route") or "—",
                            "Target tissue": e.get("tissue_name") or e.get("tissue") or "—",
                            "Tumor type": e.get("tumor_name") or e.get("tumor") or "—",
                            "TD50 (mg/kg/day)": e.get("td50") or "—",
                            "Lower conf.": e.get("lc") or "—",
                            "Upper conf.": e.get("uc") or "—",
                            "Author's opinion": e.get("opinion_label") or "—",
                            "Source": "NCI/NTP" if (e.get("source") or "") == "ncintp" else "Literature",
                        })
                    st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(exp_rows)), width="stretch", hide_index=True, height=300)
                    st.caption("TD50 = dose rate (mg/kg/day) to induce tumors in half of test animals. Lower TD50 = more potent. Author's opinion = published author's assessment of carcinogenicity at this site.")
                    # Dose–response: sorted low to high (client already returns sorted), show with clear headers
                    if doses:
                        st.markdown("**Dose–response data** (doses ordered low → high)")
                        dose_rows = []
                        for d in doses[:500]:
                            dose_rows.append({
                                "Experiment ID": d.get("idnum") or "—",
                                "Dose (mg/kg/day)": d.get("dose") or "—",
                                "Dose group": d.get("dose_order") or "—",
                                "Tumors": d.get("tumors") or "—",
                                "Total animals": d.get("total") or "—",
                            })
                        st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(dose_rows)), width="stretch", hide_index=True, height=250)
                        st.caption(f"{len(doses)} dose–response row(s). Each row = one dose group within an experiment (Experiment ID links to the table above).")
                else:
                    st.info(f"No experiments found in the {_carc_name} for this chemical.")
            elif carcinogenic_potency_client and carcinogenic_potency_client.is_available():
                st.subheader(f"📊 {_carc_name}")
                st.info(f"No data for this chemical in the {_carc_name}.")
    
            # --- GHS Classification (filtered, user-controlled) ---
            st.subheader("⚠️ GHS Classification")
            ghs = pubchem_data.get("ghs") or {}
            h_codes = ghs.get("h_codes") or []
            p_codes = ghs.get("p_codes") or []
            signal_word = (ghs.get("signal_word") or "").strip()
    
            # Build code -> phrase only for phrases that exist (filter out "(phrase not found)")
            def _filter_found_phrases(codes: list[str], expand_fn) -> dict[str, str]:
                out = {}
                for code in (c for c in codes if (c or "").strip()):
                    phrase = (expand_fn(code) or "").strip()
                    if phrase and "(phrase not found)" not in phrase.lower():
                        out[code.strip()] = phrase
                return out
    
            h_phrases_dict = _filter_found_phrases(h_codes, ghs_formatter.get_h_phrase)
            p_phrases_dict = _filter_found_phrases(p_codes, ghs_formatter.get_p_phrase)
            has_signal = signal_word and signal_word.lower() not in ("none", "n/a", "")
            has_any_ghs = bool(h_phrases_dict or p_phrases_dict or has_signal)
    
            if has_any_ghs:
                with st.expander("⚙️ GHS display options", expanded=False):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        show_h = st.checkbox(
                            "Show Hazard (H) phrases",
                            value=st.session_state["show_h_phrases"],
                            key="ghs_show_h",
                        )
                        st.session_state["show_h_phrases"] = show_h
                    with c2:
                        show_p = st.checkbox(
                            "Show Precautionary (P) phrases",
                            value=st.session_state["show_p_phrases"],
                            key="ghs_show_p",
                        )
                        st.session_state["show_p_phrases"] = show_p
                    with c3:
                        show_signal = st.checkbox(
                            "Show signal word",
                            value=st.session_state["show_signal_word"],
                            key="ghs_show_signal",
                        )
                        st.session_state["show_signal_word"] = show_signal
                    layout_choice = st.radio(
                        "Layout:",
                        ["Two columns (H left, P right)", "Single column (H then P)"],
                        horizontal=True,
                        index=0 if st.session_state["ghs_layout"] == "two_columns" else 1,
                        key="ghs_layout_radio",
                    )
                    st.session_state["ghs_layout"] = (
                        "two_columns" if layout_choice.startswith("Two") else "single_column"
                    )
    
                if h_phrases_dict or p_phrases_dict:
                    st.caption(
                        f"📊 Found {len(h_phrases_dict)} hazard and {len(p_phrases_dict)} precautionary statements"
                    )
    
                if st.session_state["ghs_layout"] == "two_columns":
                    col_left, col_right = st.columns(2)
                    with col_left:
                        st.markdown("**Hazard Statements**")
                        if st.session_state["show_h_phrases"]:
                            if h_phrases_dict:
                                for code, phrase in h_phrases_dict.items():
                                    st.write(f"**{code}:** {hazard_report_utils.clean_text(phrase)}")
                            else:
                                st.write("*No hazard statements found*")
                        else:
                            st.write("*Hidden*")
                    with col_right:
                        st.markdown("**Precautionary Statements**")
                        if st.session_state["show_p_phrases"]:
                            if p_phrases_dict:
                                for code, phrase in p_phrases_dict.items():
                                    st.write(f"**{code}:** {hazard_report_utils.clean_text(phrase)}")
                            else:
                                st.write("*No precautionary statements found*")
                        else:
                            st.write("*Hidden*")
                else:
                    if st.session_state["show_h_phrases"]:
                        st.markdown("**Hazard Statements**")
                        if h_phrases_dict:
                            for code, phrase in h_phrases_dict.items():
                                st.write(f"**{code}:** {hazard_report_utils.clean_text(phrase)}")
                        else:
                            st.write("*No hazard statements found*")
                        st.write("")
                    if st.session_state["show_p_phrases"]:
                        st.markdown("**Precautionary Statements**")
                        if p_phrases_dict:
                            for code, phrase in p_phrases_dict.items():
                                st.write(f"**{code}:** {hazard_report_utils.clean_text(phrase)}")
                        else:
                            st.write("*No precautionary statements found*")
    
                if st.session_state["show_signal_word"] and has_signal:
                    st.write(f"**Signal word:** {signal_word}")
                else:
                    st.write("No GHS classification data available from PubChem.")
    
            # --- Citation ---
            st.markdown("---")
            st.caption(
                f"📝 **For research use:** If this tool contributes to your work, "
                f"please cite the Zenodo DOI: {config.ZENODO_DOI}"
            )
    
            # --- Download: full report (no truncation) ---
            st.markdown("---")
            st.subheader("📥 Download report")
            eco = pubchem_data.get("ecotoxicity") or {}
            h_codes = (pubchem_data.get("ghs") or {}).get("h_codes") or []
            p_codes = (pubchem_data.get("ghs") or {}).get("p_codes") or []
    
            st.caption("Full report includes all identifiers, properties, GHS, and every toxicity endpoint (no truncation).")
            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                full_csv = data_formatter.download_toxicity_csv(
                    clean_cas, pubchem_data, dsstox_info, dtxsid, preferred_name, h_codes, p_codes, eco
                )
                st.download_button(
                    "⬇️ Download full report (CSV)",
                    data=full_csv,
                    file_name=f"hazard_report_{clean_cas.replace('-', '_')}.csv",
                    mime="text/csv",
                    key="download_csv",
                )
            with col_dl2:
                download_payload = data_formatter.create_comprehensive_download_data(
                    clean_cas, pubchem_data, dsstox_info, toxval_data
                )
                json_bytes = json.dumps(download_payload, indent=2, default=str).encode("utf-8")
                st.download_button(
                    "⬇️ Download full report (JSON)",
                    data=json_bytes,
                    file_name=f"hazard_report_{clean_cas.replace('-', '_')}.json",
                    mime="application/json",
                    key="download_json",
                )
    
            # QSAR Toolbox (VEGA) — show when local WebSuite is running and we have data
            _qtb_port = getattr(config, "QSAR_TOOLBOX_PORT", None)
            if _qtb_port and qsar_toolbox_client and qsar_toolbox_client.is_available(_qtb_port):
                _qtb_cache = "qsar_toolbox_rows_" + clean_cas
                if _qtb_cache not in st.session_state:
                    st.session_state[_qtb_cache] = qsar_toolbox_client.fetch_by_cas(
                        clean_cas, _qtb_port
                    )
                _qtb_rows = st.session_state.get(_qtb_cache) or []
                if _qtb_rows:
                    with st.expander("🧪 QSAR Toolbox (VEGA)", expanded=True):
                        st.caption("Local OECD QSAR Toolbox with VEGA/OPERA add-ons (no API key). Windows only.")
                        _qtb_df = pd.DataFrame(_qtb_rows)[
                            ["endpoint", "value", "unit", "position_endpoint"]
                        ].rename(columns={"position_endpoint": "Toolbox category"})
                        st.dataframe(hazard_report_utils.clean_dataframe(_qtb_df), use_container_width=True, hide_index=True)
                else:
                    with st.expander("🧪 QSAR Toolbox (VEGA)", expanded=False):
                        st.caption("Toolbox is running but no endpoint data returned for this CAS. Try SMILES search or check Toolbox databases.")
            elif qsar_toolbox_client and _qtb_port and not qsar_toolbox_client.is_available(_qtb_port):
                with st.expander("🧪 QSAR Toolbox (VEGA)", expanded=False):
                    st.caption("Start **QSAR Toolbox WebSuite** and set `QSAR_TOOLBOX_PORT` (e.g. 51946) to use local VEGA data. Install: `pip install git+https://github.com/glsalierno/PyQSARToolbox.git`")
            elif qsar_toolbox_client and not _qtb_port:
                with st.expander("🧪 QSAR Toolbox (VEGA)", expanded=False):
                    st.caption("Set **QSAR_TOOLBOX_PORT** (e.g. 51946) in env or config and start QSAR Toolbox WebSuite to use local VEGA data (Windows).")

            with st.expander("📚 Information sources"):
                st.markdown("""
                **All these information sources are gratefully acknowledged:**

                **Identifiers & structure**
                - **PubChem**: identifiers, molecular formula, GHS, toxicity text from PUG View.
                - **DSSTox (local)**: DTXSID, preferred/systematic names, formula, InChI/SMILES (SQLite or CSV mapping).

                **Toxicity & hazard data**
                - **ToxValDB (local)**: quantitative toxicity values from COMPTOX Excel → SQLite (no API key).
                - **Carcinogenic Potency Database (CPDB, local)**: TD50 and experiment data from CPDB SQLite (built via `scripts/build_carcinogenic_potency_from_cpdb_tabs.py`).
                - **IARC**: classifications from the **iarc folder** (e.g. `fastP2OASys/iarc`) or optional CSV (`P2OASYS_IARC_CSV_PATH`).
                - **ODP/GWP**: optional CSV (`P2OASYS_ODP_GWP_CSV_PATH`) for ozone depletion and global warming potential.
                - **IPCC GWP 100-year**: from **atmo folder** (e.g. `fastP2OASys/atmo`) Federal LCA Commons parquet.

                **Predictions & toolboxes**
                - **QSAR Toolbox (VEGA/OPERA)**: local OECD Toolbox WebSuite (Windows); PyQSARToolbox; set `QSAR_TOOLBOX_PORT` when WebSuite is running.
                - **Hazard scrapers** (optional): ECHA CHEM, Danish QSAR, VEGA API, NIH NICEATM ICE — via `utils.hazard_scrapers` and CLI `scripts/run_unified_hazard_scraper.py`.

                **SDS**
                - **SDS PDF**: regex extraction (GHS, CAS, quantitative values) in the **SDS PDF comparison** section; optional Ollama/LLM extraction when available.

                **P2OASys scoring**
                - **Hazard matrix**: internal configuration used for mapping endpoints to scores (not user-facing).

                **Summarization (optional)**
                - **Ollama** (local): one-sentence summaries for ecotoxicity excerpts and CPDB (no API key).
                - **OpenAI** (optional): `OPENAI_API_KEY` for gpt-4o-mini summarization when set in app secrets.
                """)
        else:
            st.error(f"No data found for '{current_query}'. Please check the input.")

    with tab_p2o:
        if result and result.get("pubchem"):
            pubchem_data = result["pubchem"]
            clean_cas = result["clean_cas"]
            # P2OASys matrix: internal config for endpoint→score mapping (not shown to user)
            matrix_path = Path(config.P2OASYS_MATRIX_PATH)
            if not matrix_path.exists():
                st.info("P2OASys scoring is not available.")
            else:
                with st.spinner("Computing P2OASys scores…"):
                    extra_sources = None
                    iarc_by_cas = None
                    odp_gwp_by_cas = None
                    ipcc_gwp_by_cas = None
                    # IARC: prefer iarc folder (fastP2OASys/iarc), else optional CSV
                    iarc_dir = getattr(config, "IARC_DIR", None)
                    if iarc_dir and os.path.isdir(iarc_dir):
                        cache_key_iarc = "p2oasys_iarc_from_folder"
                        if cache_key_iarc not in st.session_state:
                            st.session_state[cache_key_iarc] = iarc_lookup.load_iarc_from_iarc_folder(iarc_dir)
                        iarc_by_cas = st.session_state[cache_key_iarc]
                    if not iarc_by_cas and getattr(config, "P2OASYS_IARC_CSV_PATH", None):
                        iarc_path = config.P2OASYS_IARC_CSV_PATH
                        if iarc_path and os.path.isfile(iarc_path):
                            cache_key_iarc_csv = "p2oasys_iarc_lookup"
                            if cache_key_iarc_csv not in st.session_state:
                                st.session_state[cache_key_iarc_csv] = lookup_tables.load_iarc_csv(iarc_path)
                            iarc_by_cas = st.session_state[cache_key_iarc_csv]
                    if getattr(config, "P2OASYS_ODP_GWP_CSV_PATH", None):
                        odp_path = config.P2OASYS_ODP_GWP_CSV_PATH
                        if odp_path and os.path.isfile(odp_path):
                            cache_key_odp = "p2oasys_odp_gwp_lookup"
                            if cache_key_odp not in st.session_state:
                                st.session_state[cache_key_odp] = lookup_tables.load_odp_gwp_csv(odp_path)
                            odp_gwp_by_cas = st.session_state[cache_key_odp]
                    atmo_dir = getattr(config, "ATMO_DIR", None)
                    if atmo_dir and os.path.isdir(atmo_dir):
                        cache_key_atmo = "p2oasys_ipcc_gwp_100"
                        if cache_key_atmo not in st.session_state:
                            st.session_state[cache_key_atmo] = atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir)
                        ipcc_gwp_by_cas = st.session_state[cache_key_atmo]
                    if iarc_by_cas or odp_gwp_by_cas or ipcc_gwp_by_cas:
                        extra_sources = lookup_tables.get_lookup_extra_sources(
                            result["clean_cas"],
                            iarc_by_cas=iarc_by_cas,
                            odp_gwp_by_cas=odp_gwp_by_cas,
                            ipcc_gwp_by_cas=ipcc_gwp_by_cas,
                        )
                    # QSAR Toolbox (VEGA) — local WebSuite, no API key (Windows + PyQSARToolbox)
                    port = getattr(config, "QSAR_TOOLBOX_PORT", None)
                    if port and qsar_toolbox_client and qsar_toolbox_client.is_available(port):
                        cache_key = "qsar_toolbox_rows_" + result["clean_cas"]
                        if cache_key not in st.session_state:
                            st.session_state[cache_key] = qsar_toolbox_client.fetch_by_cas(
                                result["clean_cas"], port
                            )
                        qtb_rows = st.session_state.get(cache_key) or []
                        if qtb_rows:
                            extra_sources = hazard_for_p2oasys.merge_extra_sources(
                                extra_sources,
                                qsar_toolbox_client.toolbox_results_to_extra_sources(qtb_rows),
                            )
                    hazard_data = hazard_for_p2oasys.build_hazard_data(
                        pubchem_data,
                        toxval_data=result.get("toxval_data"),
                        carc_potency_data=result.get("carc_potency_data"),
                        extra_sources=extra_sources,
                    )
                    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
                    scores = p2oasys_scorer.compute_p2oasys_scores(hazard_data, matrix)
                overall_max = p2oasys_aggregate.aggregate_category_scores(scores, "max")
                overall_mean = p2oasys_aggregate.aggregate_category_scores(scores, "mean")
                overall_weighted = p2oasys_aggregate.aggregate_category_scores(scores, "weighted_mean")
                n_cat, cat_names = p2oasys_aggregate.count_scored_categories(scores)
                st.subheader("P2OASys itemized scoring")
                st.caption(
                    "Scores 2–10 (higher = more hazardous). Data from PubChem, ToxValDB, CPDB, IARC/ODP/GWP lookups, and QSAR Toolbox (VEGA) when available."
                )
                st.metric("Categories scored", n_cat)
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Overall (max)", f"{overall_max:.1f}" if overall_max == overall_max else "—")
                with col2:
                    st.metric("Overall (mean)", f"{overall_mean:.1f}" if overall_mean == overall_mean else "—")
                with col3:
                    st.metric("Overall (weighted mean)", f"{overall_weighted:.1f}" if overall_weighted == overall_weighted else "—")
                rows = []
                for category, data in scores.items():
                    if category.startswith("_"):
                        continue
                    if not isinstance(data, dict):
                        continue
                    cat_max = data.get("_category_max")
                    for subcat, subdata in data.items():
                        if subcat.startswith("_"):
                            continue
                        if isinstance(subdata, dict):
                            submax = subdata.get("_max", "")
                            for unit_name, score in subdata.items():
                                if unit_name != "_max" and isinstance(score, (int, float)):
                                    rows.append({"Category": category, "Subcategory": subcat, "Endpoint": unit_name, "Score": score, "Category max": cat_max})
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                    st.download_button(
                        "Download P2OASys scores (CSV)",
                        data=pd.DataFrame(rows).to_csv(index=False),
                        file_name=f"p2oasys_scores_{clean_cas.replace('-', '_')}.csv",
                        mime="text/csv",
                        key="download_p2oasys_csv",
                    )
                else:
                    st.info("No itemized scores for this compound with current data and matrix.")
        else:
            st.info("Run a hazard assessment above (enter CAS or name and click Assess) to see P2OASys scores here.")

# Footer when no query yet
if not current_query:
    st.markdown("---")
