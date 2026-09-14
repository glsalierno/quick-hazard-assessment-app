"""
Quick Hazard Assessment — Streamlit app.
Chemical hazard assessment from PubChem + DSSTox local (no API key required).
"""

from __future__ import annotations

import io
import json
import logging
import re
import os
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import streamlit as st

import config
from utils import (
    cas_validator,
    chemical_db,
    data_formatter,
    dsstox_local,
    ghs_formatter,
    opera_client,
    property_comparison,
    pubchem_client,
    smiles_drawer,
    p2oasys_score_lookup,
    p2oasys_score_ribbon,
)
from utils import atmo_gwp, hazard_for_p2oasys, hazard_report_utils, iarc_lookup, lookup_tables, p2oasys_aggregate, p2oasys_assessment, p2oasys_matrix_placeholder, p2oasys_scorer, p2oasys_source_bridges, p2oasys_upload_csv, solvent_reference_lookup
from utils import qa_checks
from utils import validation_snapshot
from utils.opera_mapper import OperaEndpointMapper, load_mapping
from utils import summary_utils
from utils.streamlit_secrets import get_secret
from v7.ui_curated import render_curated_knowledge, render_sds_fetch_panel
from utils.markitdown_check import is_markitdown_available
from utils.sds_integration import apply_assessment_query

try:
    from utils import sds_pdf_utils, sds_regex_extractor
except ImportError:
    sds_pdf_utils = sds_regex_extractor = None  # optional: SDS PDF flow (v1.4)

try:
    from utils.input_handler import get_input_handler
except ImportError:
    get_input_handler = None  # type: ignore[misc, assignment]

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


def _cached_solvent_reference_tables() -> dict:
    key = "solvent_reference_tables"
    if key not in st.session_state:
        st.session_state[key] = solvent_reference_lookup.resolve_lookup_tables(
            p2oasys_csv=getattr(config, "P2OASYS_EXPERT_CSV_PATH", None),
            chem21_csv=getattr(config, "CHEM21_GUIDE_CSV_PATH", None),
            doss_xlsx=getattr(config, "DOSS_XLSX_PATH", None),
            hspip_csv=getattr(config, "HSPIP_CACHE_CSV_PATH", None),
        )
    return st.session_state[key]


def _render_solvent_reference_panel(cas: str, preferred_name: str = "", pubchem: dict | None = None, hazard_data: dict | None = None) -> None:
    """Expert P2OASys / CHEM21 / Hansen lookup. Does not predict P2OASys."""
    tables = _cached_solvent_reference_tables()
    p2 = solvent_reference_lookup.lookup_p2oasys_expert(
        cas, tables["p2oasys"], source_path=tables.get("p2oasys_path")
    )
    c21 = solvent_reference_lookup.lookup_chem21(
        cas,
        tables["chem21"],
        source_path=tables.get("chem21_path"),
        pubchem=pubchem,
        hazard_data=hazard_data,
        allow_estimate=True,
    )
    hsp = solvent_reference_lookup.lookup_hsp(
        cas, hspip_table=tables.get("hspip")
    )

    st.subheader("Reference database lookup")
    st.caption(
        "P2OASys categories come from the **expert curated GT** (score lookup DB / "
        "expert-vs-auto panel). CHEM21 rankings are **reconstructed** from PubChem "
        "GHS/physchem (Prat et al. algorithm). Hansen δD/δP/δH come from "
        "[HSPiP CLI](https://github.com/glsalierno/cas-to-HSPiP_data) only."
    )

    # Full-width expert category ribbon (8 + overall) with flask color bins.
    expert_row = p2oasys_score_ribbon.render_expert_p2oasys_panel(cas)
    if expert_row is None and p2["status"] == "found":
        st.caption(
            f"Overall-only fallback from single-CAS clean CSV: {p2['score']:.1f}"
            if p2.get("score") is not None
            else "Overall-only fallback unavailable."
        )
    elif expert_row is None and preferred_name:
        st.caption(f"Queried as: {preferred_name}")

    col_c21, col_hsp = st.columns(2)
    with col_c21:
        st.markdown("**CHEM21**")
        if c21["status"] in ("found", "estimated"):
            label = c21.get("ranking_default") or "—"
            st.metric("Ranking", label)
            st.caption(
                f"S {c21.get('safety') if c21.get('safety') is not None else '—'} · "
                f"H {c21.get('health') if c21.get('health') is not None else '—'} · "
                f"E {c21.get('env') if c21.get('env') is not None else '—'}"
            )
            if c21.get("solvent"):
                st.caption(str(c21["solvent"]))
            if c21.get("ranking_discussion") and c21["ranking_discussion"] != c21.get("ranking_default"):
                st.caption(f"Discussion ranking: {c21['ranking_discussion']}")
            if c21.get("message"):
                st.caption(c21["message"])
        elif c21["status"] == "not_found":
            st.metric("Ranking", "—")
            st.info(c21["message"])
        else:
            st.metric("Ranking", "—")
            st.info(c21["message"])

    with col_hsp:
        st.markdown("**Hansen (HSPiP from CAS)**")
        if hsp["status"] == "found":
            st.metric("δD / δP / δH", f"{hsp['delta_d']} / {hsp['delta_p']} / {hsp['delta_h']}")
            src = hsp.get("source") or ""
            if src:
                st.caption(src)
        else:
            st.metric("δD / δP / δH", "—")
            st.markdown(hsp.get("message") or "")

        run_hsp = st.button(
            "Run HSPiP for this CAS",
            key=f"hspip_run_{cas}",
            help="PubChem SMILES → HSPiP CLI → data/hsp_by_cas.csv.",
        )
        if run_hsp:
            with st.spinner("Running cas-to-HSPiP routine (SMILES → HSPiP CLI)…"):
                hsp2 = solvent_reference_lookup.ensure_hspip_for_cas(cas, force=True)
            # Refresh cached tables so next paint sees the new row.
            st.session_state.pop("solvent_reference_tables", None)
            if hsp2.get("status") == "found":
                st.success(
                    f"HSPiP OK: {hsp2.get('delta_d')} / {hsp2.get('delta_p')} / {hsp2.get('delta_h')} "
                    f"({hsp2.get('source')})"
                )
                st.rerun()
            else:
                st.error(hsp2.get("message") or "HSPiP run failed")


# Page config
st.set_page_config(page_title=config.APP_TITLE, layout="wide", initial_sidebar_state="collapsed")
st.markdown(
    """
    <style>
    /* Use the window width from first paint (not Streamlit centered/mobile column). */
    .block-container {
        max-width: 100% !important;
        padding-left: 2rem;
        padding-right: 2rem;
        padding-top: 1.25rem;
    }
    [data-testid="stAppViewContainer"] .main {
        overflow-x: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)



@st.cache_resource
def _v7_orchestrator():
    from v7.orchestrator import KnowledgeOrchestrator

    return KnowledgeOrchestrator(config)


MARKITDOWN_OK, _MARKITDOWN_ERR = is_markitdown_available()

# Offline REACH / IUCLID: mirror ``.streamlit/secrets.toml`` + ``st.secrets`` into os.environ (optional package).
try:
    from unified_hazard_report.iuclid_integration import offline_archive_fingerprint, sync_offline_secrets_from_st_secrets

    sync_offline_secrets_from_st_secrets()
    if os.getenv("HAZQUERY_DEBUG_OFFLINE_SYNC", "").strip().lower() in ("1", "true", "yes", "on"):
        _la = offline_archive_fingerprint()
        logging.getLogger(__name__).info(
            "HAZQUERY_DEBUG_OFFLINE_SYNC: OFFLINE_LOCAL_ARCHIVE length=%s (chars after sync)",
            len(_la),
        )
except Exception:
    pass

# Session state: persist query and result to avoid re-fetching on every rerun
if "query" not in st.session_state:
    st.session_state["query"] = None
if "result_for" not in st.session_state:
    st.session_state["result_for"] = None
if "result_data" not in st.session_state:
    st.session_state["result_data"] = None  # { "pubchem": ..., "dsstox_info": ..., "clean_cas": ... }

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


class _OperaTransientFailure(RuntimeError):
    """Raised so Streamlit does not cache a failed OPERA CLI result for 24h."""

    def __init__(self, payload: dict[str, Any]):
        super().__init__(str((payload.get("result") or {}).get("error") or "opera_failed"))
        self.payload = payload


@st.cache_data(show_spinner="Running local OPERA QSAR…", ttl=86400, max_entries=48)
def _cached_opera_panel(smiles_key: str, cas_key: str, xlogp_key: str = "") -> dict[str, Any]:
    """Cache successful OPERA batch output per SMILES + CAS (24h TTL).

    Failures (timeout / CDK / MCR) raise ``_OperaTransientFailure`` so they are not
    sticky-cached; use ``_opera_panel_safe`` at call sites.
    """
    if not opera_client.is_opera_available():
        return {"installed": False}
    xlogp = None
    if xlogp_key not in ("", "None"):
        try:
            xlogp = float(xlogp_key)
        except (TypeError, ValueError):
            xlogp = None
    r = opera_client.get_opera_predictions(smiles_key, cas_key or None, pubchem_xlogp=xlogp)
    payload = {"installed": True, "result": r or {}}
    if r is not None and r.get("ok") is False:
        raise _OperaTransientFailure(payload)
    return payload


def _opera_panel_safe(smiles_key: str, cas_key: str, xlogp_key: str = "") -> dict[str, Any]:
    """Return cached success, or an uncached failure payload."""
    try:
        return _cached_opera_panel(smiles_key, cas_key, xlogp_key)
    except _OperaTransientFailure as exc:
        return exc.payload



dsstox_data: Optional[dict[str, Any]] = None

# Title
st.title(f"🧪 {config.APP_TITLE}")
if not MARKITDOWN_OK:
    st.error(_MARKITDOWN_ERR or "MarkItDown is required for SDS PDF parsing.")
    st.caption("Typed CAS still works below. SDS upload is disabled until MarkItDown is installed.")

# Sidebar: database stats (SQLite or CSV)
with st.sidebar:
    if not use_sqlite_dsstox:
        dsstox_data = _cached_dsstox_csv_data()
    st.header("📊 Local database")
    if use_sqlite_dsstox:
        dsstox_records = db_stats.get("dsstox", {}).get("records")
        if dsstox_records is None:
            st.success("✅ DSSTox (SQLite): loaded")
        else:
            st.success(f"✅ DSSTox (SQLite): {int(dsstox_records):,} compounds")
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
        tox_records = db_stats.get("toxvaldb", {}).get("records")
        tox_chems = db_stats.get("toxvaldb", {}).get("chemicals")
        if tox_records is None:
            st.success("✅ ToxValDB (SQLite): loaded")
        else:
            st.success(f"✅ ToxValDB (SQLite): {int(tox_records):,} records")
            if tox_chems is not None:
                st.caption(f"{int(tox_chems):,} chemicals")
    else:
        st.error("ToxValDB (SQLite) not found. Build it locally with `scripts/setup_chemical_db.py`.")
    if carcinogenic_potency_client and carcinogenic_potency_client.is_available():
        st.success(f"✅ {carcinogenic_potency_client.DISPLAY_NAME} (SQLite)")
    elif carcinogenic_potency_client:
        st.caption(f"{carcinogenic_potency_client.DISPLAY_NAME} not loaded.")
    try:
        from utils import opera_client as _opera_side

        if _opera_side.is_opera_available():
            st.success("✅ OPERA (local CLI)")
            st.caption(f"`{_opera_side.find_opera_executable()}`")
        else:
            st.caption("OPERA (local QSAR): not detected — set HAZQUERY_OPERA_EXE or install under Program Files.")
    except Exception:
        st.caption("OPERA (local QSAR): status unknown.")
    try:
        from utils import ecosar_client as _ecosar_side

        if not _ecosar_side.is_ecosar_enabled():
            st.caption("ECOSAR: disabled (`HAZQUERY_SKIP_ECOSAR`).")
        elif _ecosar_side.is_ecosar_api_available():
            st.success("✅ ECOSAR (EPI Suite API)")
            st.caption(f"`{_ecosar_side.api_base_url()}` · aquatic LC50/EC50/ChV gap-fill")
        else:
            st.caption(
                "ECOSAR (EPI Suite API): unreachable — check network or "
                "`HAZQUERY_EPISUITE_API_BASE` (default https://episuite.dev/api)."
            )
    except Exception:
        st.caption("ECOSAR: status unknown.")
    try:
        from utils.cameo_lookup import cameo_status as _cameo_status

        _cs = _cameo_status()
        if _cs.get("available"):
            st.success(f"✅ CAMEO Chemicals NFPA ({_cs.get('schema')})")
            st.caption(f"{_cs.get('n_nfpa', 0)} diamonds · {_cs.get('version') or 'local'}")
        else:
            st.caption("CAMEO NFPA: sqlite not found (optional `data/cameo_nfpa.sqlite` or CAMEO Chemicals 3.1.0).")
    except Exception:
        pass
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

# Banner when CAS was chosen from SDS upload or unified parser (results render below this message)
if st.session_state.pop("show_assessment_from_unified", False):
    q = st.session_state.get("query") or ""
    st.success(f"Using CAS `{q}` from SDS.")
    extra = st.session_state.pop("unified_assess_note", None)
    if extra:
        st.info(extra)

# --- Chemical input: CAS typed OR SDS upload (side by side) ---
st.markdown("### Chemical input")

_MAIN_PANELS = (
    "Hazard info retrieval",
    "SDS auto search",
    "P2OASys scoring",
    "OPERA vs ToxVal",
)
_PANEL_BLURBS = {
    "Hazard info retrieval": "PubChem, DSSTox, ToxVal, GHS for this CAS",
    "SDS auto search": "Auto-fetch TCI SDS and merge fields",
    "P2OASys scoring": "Expert P2OASys + CHEM21 + Hansen lookup",
    "OPERA vs ToxVal": "OPERA QSAR vs experimental ToxVal",
}
if "main_panel" not in st.session_state:
    st.session_state["main_panel"] = _MAIN_PANELS[0]

# Widget key `cas_query_input` must only be updated *before* st.text_input runs (Streamlit rule).
_pending_cas = st.session_state.pop("_pending_cas_query_input", None)
if _pending_cas is not None:
    st.session_state["cas_query_input"] = _pending_cas

_in_cas, _in_sds = st.columns(2)
with _in_cas:
    default = st.session_state.get("query") or st.session_state.get("cas_query_input") or ""
    if "cas_query_input" not in st.session_state:
        st.session_state["cas_query_input"] = default
    cas = st.text_input(
        "CAS number",
        key="cas_query_input",
        placeholder="e.g., 67-56-1",
    )
with _in_sds:
    uf_shared = None
    if sds_pdf_utils and sds_regex_extractor and MARKITDOWN_OK:
        uf_shared = st.file_uploader(
            "Upload SDS (PDF)",
            type=["pdf"],
            key="shared_sds_pdf_upload",
        )
    elif not MARKITDOWN_OK:
        st.caption("SDS upload unavailable (MarkItDown not installed).")

# --- SDS PDF extraction staging ---
if sds_pdf_utils and sds_regex_extractor and MARKITDOWN_OK:
    if uf_shared is not None:
        st.session_state["shared_sds_pdf_bytes"] = uf_shared.getvalue()
        st.session_state["shared_sds_pdf_name"] = uf_shared.name
        if st.session_state.get("_last_sds_upload_name") != uf_shared.name:
            st.session_state["_last_sds_upload_name"] = uf_shared.name
            st.session_state["sds_staged_chemical_input"] = None
            with st.spinner("Extracting CAS from SDS…"):
                if get_input_handler is None:
                    st.error("SDS input handler is not available (utils.input_handler import failed).")
                    staged = None
                else:
                    staged = get_input_handler().process_sds_pdf(uf_shared)
            st.session_state["sds_staged_chemical_input"] = staged
            st.session_state["sds_hazard_fields"] = getattr(staged, "hazard_fields", None) or {}
            st.session_state["_last_sds_upload_name"] = uf_shared.name
            if staged and staged.cas_numbers and len(staged.cas_numbers) == 1:
                st.session_state["_pending_cas_query_input"] = staged.cas_numbers[0]
                st.rerun()
    else:
        st.session_state["shared_sds_pdf_bytes"] = None
        st.session_state["shared_sds_pdf_name"] = None
        st.session_state["_last_sds_upload_name"] = None
        st.session_state["sds_staged_chemical_input"] = None
        st.session_state["sds_hazard_fields"] = {}

staged_ci = st.session_state.get("sds_staged_chemical_input")
if staged_ci is not None and staged_ci.cas_numbers:
    if len(staged_ci.cas_numbers) > 1:
        st.caption(f"**{len(staged_ci.cas_numbers)} CAS** from SDS. Choose one:")
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
        pick = st.selectbox("CAS to use", options=staged_ci.cas_numbers, key="top_sds_cas_pick")
        if st.button("Use selected CAS", type="primary", key="top_sds_run_assess_btn"):
            apply_assessment_query(pick, show_banner=True, banner_note=f"Using **{pick}** from SDS.")
            st.session_state["main_panel"] = "Hazard info retrieval"
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
                        st.session_state["main_panel"] = "Hazard info retrieval"
                        st.rerun()
                    elif correct_cas:
                        st.warning("Enter a valid CAS format (e.g., 67-64-1).")
            with col2:
                if st.button("Report issue", key="manual_cas_report_btn"):
                    st.info("Thank you! This helps improve the extractor.")
    elif len(staged_ci.cas_numbers) == 1 and not (cas or "").strip():
        st.caption("CAS extracted into the field above — click **Hazard info retrieval** to continue.")
        with st.expander("✏️ Manual correction"):
            correct_cas = st.text_input("Correct CAS if wrong:", key="manual_cas_single", placeholder="e.g., 67-64-1")
            if st.button("Use this CAS", key="manual_cas_single_btn"):
                if correct_cas and cas_validator.is_valid_cas_format(str(correct_cas).strip()):
                    st.session_state["query"] = cas_validator.normalize_cas_input(correct_cas) or correct_cas.strip()
                    st.session_state["result_for"] = None
                    st.session_state["_pending_cas_query_input"] = correct_cas.strip()
                    st.session_state["main_panel"] = "Hazard info retrieval"
                    st.rerun()
                elif correct_cas:
                    st.warning("Enter a valid CAS format (e.g., 67-64-1).")
elif staged_ci is not None and not staged_ci.cas_numbers:
    _ex_err = getattr(staged_ci, "extraction_error", None)
    if _ex_err:
        st.error(f"**SDS extraction issue:** {_ex_err}")
    st.warning("No CAS extracted from SDS. Enter a CAS number at left.")

st.markdown("### Actions")
_act_cols = st.columns(len(_MAIN_PANELS), gap="small")
for _label, _acol in zip(_MAIN_PANELS, _act_cols):
    with _acol:
        _active = st.session_state["main_panel"] == _label
        if st.button(
            _label,
            key=f"main_panel_nav_{_label}",
            type="primary" if _active else "secondary",
            use_container_width=True,
            help=_PANEL_BLURBS.get(_label, ""),
        ):
            _cas_raw = (st.session_state.get("cas_query_input") or cas or "").strip()
            _clean = cas_validator.normalize_cas_input(_cas_raw) if _cas_raw else ""
            if not _clean and not st.session_state.get("query"):
                st.warning("Enter a CAS number (or upload an SDS) first.")
            else:
                if _clean:
                    st.session_state["query"] = _clean
                    st.session_state["_pending_cas_query_input"] = _clean
                    # Always refresh base identity when switching actions for a new/changed CAS
                    if st.session_state.get("result_for") != _clean:
                        st.session_state["result_for"] = None
                    if _label == "Hazard info retrieval":
                        st.session_state["result_for"] = None
                    if _label == "SDS auto search":
                        st.session_state[f"curated_sds_auto_done_{_clean}"] = False
                        st.session_state["auto_run_sds_tci"] = True
                    if _label == "OPERA vs ToxVal":
                        st.session_state["auto_run_opera_toxval"] = True
                st.session_state["main_panel"] = _label
                st.rerun()
        st.caption(_PANEL_BLURBS.get(_label, ""))

# --- Typed CAS / name assessment (ChemicalAssessmentService only; independent of SDS PDF pipeline) ---
# Run assessment when we have a query and either no cached result or result is for a different query
current_query = st.session_state.get("query")
if current_query:
    clean_cas = cas_validator.normalize_cas_input(current_query)
    _prev = st.session_state.get("result_data")
    need_fetch = (
        st.session_state.get("result_for") != clean_cas
        or not _prev
        or _prev.get("result_schema_version") != "v6_cas_1"
        or not _prev.get("pubchem")
        or bool(_prev.get("fetch_error"))
    )
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
    _panel = st.session_state.get("main_panel") or _MAIN_PANELS[0]
    st.markdown(f"### {_panel}")
    if _panel == "Hazard info retrieval":
        if not result or not result.get("pubchem"):
            msg = "No hazard data yet. Enter a CAS above (or upload an SDS), then click **Hazard info retrieval**."
            if result and result.get("fetch_error"):
                msg = (
                    f"**Could not fetch hazard data:** {result['fetch_error']}. "
                    "Check network or click **Hazard info retrieval** again (failed lookups are retried automatically)."
                )
            st.info(msg)
        elif result and result.get("pubchem"):
            pubchem_data = result["pubchem"]
            dsstox_info = result.get("dsstox_info")
            dtxsid = result.get("dtxsid")
            preferred_name = result.get("preferred_name")
            clean_cas = result["clean_cas"]
            toxval_data = result.get("toxval_data")
            carc_potency_data = result.get("carc_potency_data")
            cas_commonchem_data = result.get("cas_commonchem") or {}
            cas_commonchem_error = result.get("cas_commonchem_error")

            # Run OPERA automatically whenever PubChem supplies a usable SMILES.
            # The 24-hour cache prevents repeated execution on Streamlit reruns.
            _op: dict[str, Any] = {"installed": False}
            _opera_auto_error: Optional[str] = None
            _auto_smiles = str(pubchem_data.get("smiles") or "").strip()
            if _auto_smiles:
                try:
                    with st.spinner("Running OPERA predictions automatically..."):
                        _xlogp = pubchem_data.get("xlogp")
                        _op = _opera_panel_safe(
                            _auto_smiles,
                            str(clean_cas or ""),
                            str(_xlogp) if _xlogp is not None else "",
                        )
                except Exception as exc:
                    _opera_auto_error = str(exc)
            _opera_result = _op.get("result") or {}
            _property_comparison = property_comparison.build_property_comparison(
                cas_commonchem_data,
                pubchem_data,
                _opera_result,
            )

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
            comparison_rows = _property_comparison.get("rows") or []
            if comparison_rows:
                st.markdown("**CAS / PubChem / OPERA property comparison**")
                st.caption(
                    "CAS Common Chemistry values are experimental/curated; PubChem values "
                    "are retrieved records; OPERA values are predictions. Blank cells mean "
                    "that source did not provide the property."
                )
                st.dataframe(
                    hazard_report_utils.clean_dataframe(pd.DataFrame(comparison_rows)),
                    width="stretch",
                    hide_index=True,
                )
                for _comparison_warning in _property_comparison.get("warnings") or []:
                    st.warning(str(_comparison_warning))
            elif cas_commonchem_error:
                st.caption(f"CAS Common Chemistry unavailable for this record: {cas_commonchem_error}")

            if cas_commonchem_data:
                _cas_properties = cas_commonchem_data.get("experimental_properties") or []
                with st.expander(
                    f"CAS Common Chemistry evidence ({len(_cas_properties)} experimental properties)",
                    expanded=False,
                ):
                    st.caption(
                        f"CAS RN `{cas_commonchem_data.get('cas_rn') or clean_cas}` · "
                        f"retrieved `{cas_commonchem_data.get('retrieved_at') or 'unknown'}` · "
                        f"parser `{cas_commonchem_data.get('parser_version') or 'unknown'}`"
                    )
                    if cas_commonchem_data.get("replaced_rns"):
                        st.info(
                            "Deleted/replaced CAS RNs linked by CAS Common Chemistry: "
                            + ", ".join(str(item) for item in cas_commonchem_data["replaced_rns"])
                        )
                    if _cas_properties:
                        _cas_rows = []
                        for _cas_prop in _cas_properties:
                            _citation = _cas_prop.get("citation") or {}
                            _cas_rows.append(
                                {
                                    "Property": _cas_prop.get("name"),
                                    "Value as reported": _cas_prop.get("raw_value"),
                                    "Parsed value": _cas_prop.get("value")
                                    if _cas_prop.get("value") is not None
                                    else (
                                        f"{_cas_prop.get('value_min')}–{_cas_prop.get('value_max')}"
                                        if _cas_prop.get("value_min") is not None
                                        else ""
                                    ),
                                    "Unit": _cas_prop.get("unit"),
                                    "Conditions": _cas_prop.get("conditions"),
                                    "Citation": _citation.get("source"),
                                }
                            )
                        st.dataframe(
                            hazard_report_utils.clean_dataframe(pd.DataFrame(_cas_rows)),
                            width="stretch",
                            hide_index=True,
                        )
                    else:
                        st.info("CAS Common Chemistry returned identity data but no experimental properties.")
            coverage = hazard_report_utils.get_source_coverage(result)
            if coverage:
                st.markdown("**Data coverage by source**")
                st.dataframe(hazard_report_utils.clean_dataframe(pd.DataFrame(coverage)), use_container_width=True, hide_index=True)

            # --- QA / evidence integrity (v6) ---
            _json_payload = data_formatter.create_comprehensive_download_data(
                clean_cas, pubchem_data, dsstox_info, toxval_data
            )
            _csv_payload = data_formatter.download_toxicity_csv(
                clean_cas,
                pubchem_data,
                dsstox_info,
                dtxsid,
                preferred_name,
                (pubchem_data.get("ghs") or {}).get("h_codes") or [],
                (pubchem_data.get("ghs") or {}).get("p_codes") or [],
                pubchem_data.get("ecotoxicity") or {},
            )
            qa_status = qa_checks.build_qa_status(
                result, csv_payload=_csv_payload, json_payload=_json_payload
            )
            acc = qa_status.get("accounting") or {}
            with st.expander("🔍 QA / evidence integrity", expanded=bool(qa_status.get("warnings"))):
                st.caption(
                    f"Lexicon `{acc.get('lexicon_version', 'ghs_formatter_v6')}` · "
                    f"parser `{(pubchem_data.get('parser_version') or 'pubchem_client_v6')}`"
                )
                st.write(
                    f"**H-codes:** retrieved {qa_status.get('retrieved_h_count', 0)} · "
                    f"displayed {qa_status.get('displayed_h_count', 0)} · "
                    f"unmapped {len(qa_status.get('unmapped_h_codes') or [])} · "
                    f"subsumed {len(qa_status.get('suppressed_or_subsumed_h_codes') or [])}"
                )
                st.write(
                    f"**P-codes:** retrieved {qa_status.get('retrieved_p_count', 0)} · "
                    f"displayed {qa_status.get('displayed_p_count', 0)}"
                )
                if not acc.get("h_complete") or not acc.get("p_complete"):
                    st.error("GHS accounting incomplete — some codes are not in displayed/unmapped/subsumed buckets.")
                if qa_status.get("unmapped_h_codes"):
                    st.warning(
                        "Retrieved H-codes with no lexicon phrase (still accounted for): "
                        + ", ".join(qa_status["unmapped_h_codes"])
                    )
                for item in qa_status.get("suppressed_or_subsumed_h_codes") or []:
                    st.info(
                        f"**{item.get('code')}** subsumed by **{item.get('primary')}**: {item.get('reason')}"
                        + (f" Phrase: {item.get('phrase')}" if item.get("phrase") else "")
                    )
                for iss in qa_status.get("issues") or []:
                    sev = iss.get("severity")
                    msg = iss.get("message") or ""
                    if sev == "error":
                        st.error(msg)
                    elif sev == "warning" and iss.get("code") not in (
                        "ghs_h_unmapped",
                        "ghs_p_unmapped",
                    ):
                        st.warning(msg)

            st.info(
                "This is a screening and evidence-retrieval tool, not a regulatory classification, "
                "SDS replacement, or final alternatives-assessment determination. Verify against "
                "supplier SDS, authoritative regulatory sources, and expert review."
            )

            pub_ep = hazard_report_utils.build_pubchem_endpoint_df(result, max_rows=20)
            tox_ep = hazard_report_utils.build_toxval_endpoint_df(result, max_rows=20)
            dsstox_id = hazard_report_utils.build_dsstox_identifier_df(result)
            if not pub_ep.empty or not tox_ep.empty or not dsstox_id.empty:
                st.markdown("**Endpoint values by source**")
                st.caption(
                    "DSSTox supplies **identifiers** (CAS ↔ DTXSID). "
                    "Quantitative toxicity endpoints for EPA CompTox come from **ToxValDB** (linked by DTXSID). "
                    "Full tables are in **Toxic doses & toxicity endpoints** below."
                )
                ep_tab_pub, ep_tab_tox, ep_tab_ds = st.tabs(
                    ["PubChem endpoints", "ToxVal (EPA, via DSSTox)", "DSSTox identifiers"]
                )
                with ep_tab_pub:
                    if not pub_ep.empty:
                        st.dataframe(pub_ep, use_container_width=True, hide_index=True)
                    else:
                        st.info("No quantitative PubChem toxicity endpoints for this compound (categorical GHS/text may still appear below).")
                with ep_tab_tox:
                    if not tox_ep.empty:
                        st.dataframe(tox_ep, use_container_width=True, hide_index=True)
                    elif result.get("dtxsid"):
                        st.info(
                            f"DTXSID **{result.get('dtxsid')}** resolved, but no ToxVal rows were loaded. "
                            "Check sidebar ToxValDB status and `CHEMICAL_DB_PATH`."
                        )
                    else:
                        st.info("No DTXSID — ToxVal requires DSSTox identifier lookup first.")
                with ep_tab_ds:
                    if not dsstox_id.empty:
                        st.dataframe(dsstox_id, use_container_width=True, hide_index=True)
                    else:
                        st.info("No local DSSTox record for this CAS.")
            st.markdown("---")

            try:
                from unified_hazard_report.iuclid_integration import render_reach_iuclid_panel

                render_reach_iuclid_panel(str(clean_cas))
            except Exception as exc:
                logging.getLogger(__name__).debug("REACH / IUCLID panel skipped: %s", exc, exc_info=True)

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

                with st.expander("OPERA predictions (local QSAR)", expanded=False):
                    st.caption(
                        "OPERA runs automatically for each assessed chemical with a valid SMILES. "
                        "NIEHS [OPERA 2.9](https://github.com/kmansouri/OPERA) command-line batch "
                        "(`-s` SMILES file, `-o` CSV). Prefer `OPERA.exe`; parallel `OPERA_P.exe` can fail the CDK step on some structures. "
                        "Override path with `HAZQUERY_OPERA_EXE` or `config.HAZQUERY_OPERA_EXE`."
                    )
                    if _opera_auto_error:
                        st.warning(f"OPERA integration error: {_opera_auto_error}")
                    else:
                        if not _op.get("installed"):
                            st.info(
                                "OPERA is not installed or not found. Install the OPERA 2.9 CL build from "
                                "GitHub releases and ensure `OPERA.exe` exists under "
                                "`C:\\Program Files\\OPERA\\application\\`, or set environment variable "
                                "`HAZQUERY_OPERA_EXE` to the full path."
                            )
                        else:
                            res = _op.get("result") or {}
                            for w in res.get("warnings") or []:
                                st.warning(str(w))
                            if not res.get("ok"):
                                st.warning(res.get("error") or "OPERA did not return predictions for this structure.")
                                if res.get("stderr"):
                                    with st.expander("OPERA stderr (debug)", expanded=False):
                                        st.code(str(res.get("stderr"))[:8000], language="text")
                            else:
                                disp = res.get("display") or {}
                                if disp:
                                    st.dataframe(
                                        hazard_report_utils.clean_dataframe(
                                            pd.DataFrame([{"Property": k, "Predicted value": v} for k, v in disp.items()])
                                        ),
                                        width="stretch",
                                        hide_index=True,
                                    )
                                else:
                                    st.caption("OPERA finished but none of the selected endpoints had values.")
                                if res.get("exe"):
                                    st.caption(f"Executable: `{res['exe']}` · parser `{res.get('parser_version') or 'opera_client_v6'}`")
    
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
                try:
                    from utils.cameo_lookup import nfpa_property_rows

                    prop_rows.extend(nfpa_property_rows(str(clean_cas or ""), pubchem_data.get("nfpa")))
                except Exception:
                    logging.getLogger(__name__).debug("CAMEO NFPA property rows skipped", exc_info=True)
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
                _inc = prioritized.get("incomplete") or []
                if _inc:
                    st.caption(
                        f"{len(_inc)} incomplete toxicity row(s) excluded from prioritized view "
                        "(blank endpoint/value — see Raw data tab)."
                    )
    
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
                                "Endpoint": r.get("endpoint") or r.get("toxval_type") or r.get("study_type", _cat),
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
                        api_key = get_secret("OPENAI_API_KEY")
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
                    api_key_cpdb = get_secret("OPENAI_API_KEY")
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
    
            # --- GHS Classification (v6: no silent drops; unmapped/subsumed listed) ---
            st.subheader("⚠️ GHS Classification")
            ghs = pubchem_data.get("ghs") or {}
            h_codes = ghs.get("h_codes") or []
            p_codes = ghs.get("p_codes") or []
            signal_word = (ghs.get("signal_word") or "").strip()
            ghs_acc = ghs_formatter.account_ghs_codes(h_codes, p_codes)
            h_phrases_dict = ghs_acc.get("displayed_h_phrases") or {}
            p_phrases_dict = ghs_acc.get("displayed_p_phrases") or {}
            has_signal = signal_word and signal_word.lower() not in ("none", "n/a", "")
            has_any_ghs = bool(
                h_codes
                or p_codes
                or h_phrases_dict
                or p_phrases_dict
                or has_signal
            )
            if ghs_acc.get("unmapped_h_codes") or ghs_acc.get("unmapped_p_codes"):
                st.warning(
                    "Some retrieved GHS codes lack lexicon phrases and are listed under "
                    "**Unmapped codes** below (not silently dropped)."
                )
            if ghs_acc.get("suppressed_or_subsumed_h_codes"):
                with st.expander("Subsumed / co-listed H-codes", expanded=True):
                    for item in ghs_acc["suppressed_or_subsumed_h_codes"]:
                        st.write(
                            f"**{item.get('code')}** (subsumed by **{item.get('primary')}**): "
                            f"{item.get('reason')}"
                        )
                        if item.get("phrase"):
                            st.caption(f"Phrase: {item['phrase']}")
    
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

                _unmap_h = ghs_acc.get("unmapped_h_codes") or []
                _unmap_p = ghs_acc.get("unmapped_p_codes") or []
                if _unmap_h or _unmap_p:
                    st.markdown("**Unmapped codes** (retrieved but no lexicon phrase)")
                    if _unmap_h:
                        st.write("H: " + ", ".join(_unmap_h))
                    if _unmap_p:
                        st.write("P: " + ", ".join(_unmap_p))
                st.caption(
                    f"Accounting: retrieved H={len(ghs_acc.get('retrieved_h_codes') or [])} "
                    f"= displayed {len(ghs_acc.get('displayed_h_codes') or [])} "
                    f"+ unmapped {len(_unmap_h)} "
                    f"+ subsumed {len(ghs_acc.get('suppressed_or_subsumed_h_codes') or [])}"
                )
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

            with st.expander("Developer: QA validation snapshot", expanded=False):
                st.caption(
                    "Writes a compact local JSON under `validation_snapshots/` "
                    "(identifiers, GHS accounting, ecotox, prioritized tox, OPERA/QA warnings)."
                )
                if st.button("Write validation snapshot JSON", key="write_validation_snapshot_btn"):
                    try:
                        _opera_snap: dict[str, Any] = {}
                        _smiles_snap = (
                            pubchem_data.get("smiles")
                            or pubchem_data.get("isomeric_smiles")
                            or ""
                        ).strip()
                        if _smiles_snap:
                            try:
                                _panel = _opera_panel_safe(
                                    _smiles_snap,
                                    clean_cas or "",
                                    str(
                                        pubchem_data.get("xlogp")
                                        if pubchem_data.get("xlogp") is not None
                                        else ""
                                    ),
                                ) or {}
                                _res = _panel.get("result") if isinstance(_panel, dict) else {}
                                if isinstance(_res, dict):
                                    _opera_snap = {
                                        **_res,
                                        "available": bool(_panel.get("installed")),
                                        "warnings": list(_res.get("warnings") or []),
                                    }
                                else:
                                    _opera_snap = {
                                        "available": bool(_panel.get("installed")),
                                        "warnings": [],
                                    }
                            except Exception:
                                _opera_snap = {
                                    "warnings": ["OPERA panel unavailable for snapshot"]
                                }
                        _snap_path = validation_snapshot.write_validation_snapshot(
                            result,
                            opera_panel=_opera_snap,
                            csv_payload=full_csv,
                            json_payload=download_payload,
                        )
                        st.success(f"Wrote `{_snap_path.name}` under validation_snapshots/")
                        st.code(str(_snap_path.name))
                    except Exception as _snap_exc:
                        st.error(f"Snapshot write failed: {_snap_exc}")
    
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
                - **CAS Common Chemistry**: curated CAS identity, synonyms, structures, replaced RNs, and cited experimental physical properties (CC BY-NC 4.0).
                - **PubChem**: identifiers, molecular formula, GHS, toxicity text from PUG View.
                - **DSSTox (local)**: DTXSID, preferred/systematic names, formula, InChI/SMILES (SQLite or CSV mapping).

                **Toxicity & hazard data**
                - **CAMEO Chemicals (local)**: NFPA 704 Health/Fire/Instability from desktop `cameo.sqlite` or bundled `data/cameo_nfpa.sqlite` (not a website scrape).
                - **ToxValDB (local)**: quantitative toxicity values from COMPTOX Excel → SQLite (no API key).
                - **Carcinogenic Potency Database (CPDB, local)**: TD50 and experiment data from CPDB SQLite (built via `scripts/build_carcinogenic_potency_from_cpdb_tabs.py`).
                - **ECHA / IUCLID (offline)**: dossier index (`uuid`, identifiers, infocard links) and parsed `Document.i6d` snippets from local `.zip`/`.7z`/`.i6z` snapshots.
                - **IARC**: classifications from the **iarc folder** (e.g. `fastP2OASys/iarc`) or optional CSV (`P2OASYS_IARC_CSV_PATH`).
                - **ODP/GWP**: optional CSV (`P2OASYS_ODP_GWP_CSV_PATH`) for ozone depletion and global warming potential.
                - **IPCC GWP 100-year**: from **atmo folder** (e.g. `fastP2OASys/atmo`) Federal LCA Commons parquet.

                **Predictions & toolboxes**
                - **QSAR Toolbox (VEGA/OPERA)**: local OECD Toolbox WebSuite (Windows); PyQSARToolbox; set `QSAR_TOOLBOX_PORT` when WebSuite is running.
                - **Hazard scrapers** (optional): ECHA CHEM, Danish QSAR, VEGA API, NIH NICEATM ICE — via `utils.hazard_scrapers` and CLI `scripts/run_unified_hazard_scraper.py`.

                **SDS**
                - **SDS PDF**: regex extraction (GHS, CAS, quantitative values) in the **SDS PDF comparison** section.

                **P2OASys scoring**
                - **Hazard matrix**: internal configuration used for mapping endpoints to scores (not user-facing).

                **Summarization (optional)**
                - **Rule-based/local app summaries**: endpoint rollups and one-line synthesis shown in the report views.
                """)
        else:
            st.error(f"No data found for '{current_query}'. Please check the input.")

    if False:  # CAS Common Chemistry tab hidden for now (restore as its own tab when needed)
        st.subheader("CAS Common Chemistry record")
        st.caption(
            "Curated identity, structure, synonym, replaced-RN, and experimental "
            "physical-property evidence from CAS Common Chemistry (CC BY-NC 4.0). "
            "The API does not expose structured toxicity or GHS endpoints."
        )
        if not result:
            st.info("Run an assessment to retrieve the CAS Common Chemistry record.")
        else:
            _cas_record = result.get("cas_commonchem") or {}
            _cas_error = result.get("cas_commonchem_error")
            if not _cas_record:
                if _cas_error:
                    st.warning(f"CAS Common Chemistry record unavailable: {_cas_error}")
                else:
                    st.info(
                        "No CAS Common Chemistry record was retrieved. Confirm that "
                        "`CAS_COMMONCHEM_API_KEY` is configured and that this CAS RN is "
                        "included in Common Chemistry."
                    )
            else:
                _cas_props = _cas_record.get("experimental_properties") or []
                _cas_synonyms = _cas_record.get("synonyms") or []
                _cas_replaced = _cas_record.get("replaced_rns") or []

                _cas_col1, _cas_col2, _cas_col3, _cas_col4 = st.columns(4)
                with _cas_col1:
                    st.metric("CAS RN", _cas_record.get("cas_rn") or result.get("clean_cas") or "—")
                with _cas_col2:
                    _cas_name = str(_cas_record.get("name") or "—")
                    st.metric(
                        "CAS name",
                        (_cas_name[:24] + "…") if len(_cas_name) > 24 else _cas_name,
                    )
                with _cas_col3:
                    st.metric("Experimental properties", len(_cas_props))
                with _cas_col4:
                    st.metric("Synonyms", len(_cas_synonyms))

                st.markdown("### Identity and structure")
                _cas_identity_rows = [
                    {"Field": "CAS Registry Number", "Value": _cas_record.get("cas_rn")},
                    {"Field": "CAS name", "Value": _cas_record.get("name")},
                    {"Field": "Molecular formula", "Value": _cas_record.get("molecular_formula")},
                    {"Field": "Molecular mass", "Value": _cas_record.get("molecular_mass")},
                    {"Field": "Canonical SMILES", "Value": _cas_record.get("canonical_smiles")},
                    {"Field": "SMILES", "Value": _cas_record.get("smiles")},
                    {"Field": "InChI", "Value": _cas_record.get("inchi")},
                    {"Field": "InChIKey", "Value": _cas_record.get("inchi_key")},
                    {"Field": "CAS substance URI", "Value": _cas_record.get("uri")},
                    {
                        "Field": "Molfile available",
                        "Value": "Yes" if _cas_record.get("has_molfile") else "No",
                    },
                ]
                _cas_identity_df = pd.DataFrame(
                    [row for row in _cas_identity_rows if row.get("Value") not in (None, "")]
                )
                st.dataframe(
                    hazard_report_utils.clean_dataframe(_cas_identity_df),
                    width="stretch",
                    hide_index=True,
                )

                st.markdown("### All experimental properties returned by CAS")
                st.caption(
                    "This table is generic: any additional property names returned by "
                    "the API will appear automatically; it is not limited to melting "
                    "point, boiling point, or density."
                )
                if _cas_props:
                    _all_cas_property_rows = []
                    for _prop in _cas_props:
                        _prop_citation = _prop.get("citation") or {}
                        _parsed_value = _prop.get("value")
                        if _parsed_value is None and _prop.get("value_min") is not None:
                            _parsed_value = (
                                f"{_prop.get('value_min')}–{_prop.get('value_max')}"
                            )
                        _all_cas_property_rows.append(
                            {
                                "Property": _prop.get("name"),
                                "Value as reported": _prop.get("raw_value"),
                                "Parsed value/range": _parsed_value,
                                "Unit": _prop.get("unit"),
                                "Conditions": _prop.get("conditions"),
                                "Source number": _prop.get("source_number"),
                                "Citation": _prop_citation.get("source"),
                                "Document URI": _prop_citation.get("document_uri"),
                            }
                        )
                    st.dataframe(
                        hazard_report_utils.clean_dataframe(
                            pd.DataFrame(_all_cas_property_rows)
                        ),
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.info("CAS returned no experimental property rows for this substance.")

                st.markdown("### Property citations")
                _cas_citations = _cas_record.get("property_citations") or []
                if _cas_citations:
                    st.dataframe(
                        hazard_report_utils.clean_dataframe(
                            pd.DataFrame(
                                [
                                    {
                                        "Source number": citation.get("source_number"),
                                        "Citation": citation.get("source"),
                                        "Document URI": citation.get("document_uri"),
                                    }
                                    for citation in _cas_citations
                                ]
                            )
                        ),
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.info("No property citations were returned for this record.")

                _cas_meta_left, _cas_meta_right = st.columns(2)
                with _cas_meta_left:
                    st.markdown("### Synonyms")
                    if _cas_synonyms:
                        st.dataframe(
                            pd.DataFrame(
                                {
                                    "Synonym": [
                                        hazard_report_utils.clean_text(str(item))
                                        for item in _cas_synonyms
                                    ]
                                }
                            ),
                            width="stretch",
                            hide_index=True,
                            height=320,
                        )
                    else:
                        st.info("No synonyms returned.")
                with _cas_meta_right:
                    st.markdown("### Deleted or replaced CAS RNs")
                    if _cas_replaced:
                        st.dataframe(
                            pd.DataFrame({"CAS RN": _cas_replaced}),
                            width="stretch",
                            hide_index=True,
                        )
                    else:
                        st.info("No deleted or replaced CAS RNs returned.")

                    st.markdown("### Provenance")
                    st.write(f"**Retrieved:** {_cas_record.get('retrieved_at') or '—'}")
                    st.write(f"**Parser:** `{_cas_record.get('parser_version') or '—'}`")
                    st.write("**License:** CAS Common Chemistry CC BY-NC 4.0")

                with st.expander("CAS response field coverage", expanded=False):
                    _field_names = (
                        (_cas_record.get("capability_observation") or {}).get(
                            "top_level_fields"
                        )
                        or []
                    )
                    st.write(
                        ", ".join(f"`{field}`" for field in _field_names)
                        if _field_names
                        else "No field inventory recorded."
                    )
                    st.caption(
                        "The normalized record intentionally excludes structure SVGs and "
                        "does not claim toxicity fields that the API did not return."
                    )

                _cas_json = json.dumps(
                    _cas_record,
                    indent=2,
                    default=str,
                ).encode("utf-8")
                st.download_button(
                    "Download normalized CAS record (JSON)",
                    data=_cas_json,
                    file_name=(
                        f"cas_commonchem_{str(result.get('clean_cas') or 'record').replace('-', '_')}.json"
                    ),
                    mime="application/json",
                    key="download_cas_commonchem_json",
                )

    elif _panel == "P2OASys scoring":
        if not result or not result.get("clean_cas"):
            st.info("Enter a CAS above, then click **P2OASys scoring** to look up P2OASys, CHEM21, and Hansen parameters.")
        else:
            clean_cas = result["clean_cas"]
            pubchem_data = result.get("pubchem") or {}
            preferred_name_p2o = result.get("preferred_name") or pubchem_data.get("iupac_name") or ""
            hazard_blob = result.get("hazard_data") or result.get("hazard") or {}
            _render_solvent_reference_panel(
                clean_cas,
                str(preferred_name_p2o or ""),
                pubchem=pubchem_data if isinstance(pubchem_data, dict) else None,
                hazard_data=hazard_blob if isinstance(hazard_blob, dict) else None,
            )

            st.divider()
            run_draft = st.checkbox(
                "Compute Automatic P2OASys Assessment from gathered evidence (screening draft for review)",
                value=False,
                key="p2o_experimental_draft",
                help="Automatic scoring is paused. Use expert-database lookup above. "
                "Enable only for pipeline testing.",
            )
            if not run_draft:
                st.caption(
                    "Automatic P2OASys prediction is off. If this CAS is not in the expert "
                    "database, a **P2OASys assessment is required** (see TURI P2OASys)."
                )
            elif not result.get("pubchem"):
                st.info("Draft scoring needs a successful PubChem retrieval.")
            else:
                st.subheader("Automatic P2OASys Assessment")
                # Ribbon is rendered AFTER evidence gathering + scoring so greys
                # always reflect the just-finished draft (not a stale one-shot session).
                if st.session_state.get("p2o_main_auto_cas") != clean_cas:
                    st.session_state.pop("p2o_main_auto_score_fp", None)
                    st.session_state.pop("p2o_main_ribbon_fresh", None)
                st.caption(
                    "Auto ribbon updates when evidence gathering finishes below."
                )
                st.markdown(
                    '''<div id="p2o-compact-layout"></div>
                    <style>
                    /* Tighter P2OASys draft stack */
                    .block-container { padding-top: 1rem; max-width: 100% !important; }
                    div[data-testid="stExpander"] { margin-bottom: 0.25rem; }
                    div[data-testid="metric-container"] { padding: 0.25rem 0.5rem; }
                    </style>''',
                    unsafe_allow_html=True,
                )
                st.info(
                    "For a full human-in-the-loop subcategory form (matrix dropdowns, Process/Life Cycle "
                    "left blank), open the **P2OASys Assessment** page in the sidebar "
                    "(pages/05_P2OASys_Assessment.py)."
                )
                st.caption(
                    "Guided workflow: **identify → gather → coverage → draft scores → review/override → export**. "
                    "Scores 2–10 (higher = more hazardous). This is a screening draft for expert review."
                )

                # --- Step 1: Identity ---
                with st.expander("1 · Chemical identity", expanded=False):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.metric("CAS", clean_cas or "—")
                    with c2:
                        st.metric("Name", (preferred_name_p2o[:40] + "…") if len(str(preferred_name_p2o)) > 40 else (preferred_name_p2o or "—"))
                    with c3:
                        st.metric("MW", pubchem_data.get("mw") or "—")

                # P2OASys matrix: official TURI xlsx, or auto-generated dev placeholder.
                matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
                    Path(config.P2OASYS_MATRIX_PATH),
                    Path(config.DATA_DIR),
                )
                if matrix_kind == "missing" or not matrix_path.is_file():
                    st.info(
                        "P2OASys scoring is not available. Add the TURI hazard matrix workbook to `data/` "
                        "as `Hazard Matrix Group Review 9-19-23.xlsx`, or set `P2OASYS_MATRIX_PATH` to that file. "
                        "If you turned off the dev placeholder, unset `P2OASYS_DISABLE_AUTO_PLACEHOLDER`."
                    )
                else:
                    if matrix_kind == "placeholder":
                        st.warning(
                            "**Dev placeholder matrix** — Official TURI workbook not found. "
                            "Scores are for pipeline testing only and are **not** TURI-calibrated. "
                            "Install from https://p2oasys.turi.org/chemical/hazard-score-matrix "
                            "or set `P2OASYS_MATRIX_PATH`."
                        )
                    with st.spinner("Gathering evidence and computing P2OASys scores…"):
                        pipeline_notes: list[str] = []
                        sources_used: list[str] = ["PubChem"]
                        extra_sources = None
                        iarc_by_cas = None
                        odp_gwp_by_cas = None
                        ipcc_gwp_by_cas = None
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
                        atmo_dir = atmo_gwp.resolve_atmo_dir(
                            getattr(config, "ATMO_DIR", None),
                            repo_root=getattr(config, "REPO_ROOT", None),
                            fastp2oasys_dir=getattr(config, "FASTP2OASYS_DIR", None),
                        )
                        if atmo_dir is not None:
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
                            sources_used.append("IARC/ODP/GWP lookups")

                        # SDS gap-fill when session holds extracted SDS fields.
                        _sds_fields = st.session_state.get("sds_hazard_fields") or st.session_state.get("sds_extracted_fields")
                        if _sds_fields:
                            _sds_xs = p2oasys_source_bridges.sds_fields_to_extra_sources(_sds_fields)
                            if _sds_xs:
                                extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, _sds_xs)
                                sources_used.append("SDS")
                                pipeline_notes.extend(p2oasys_source_bridges.merge_pipeline_notes(_sds_xs))

                        _state, _state_src = atmo_gwp.infer_physical_state_from_sources(
                            sds_fields=_sds_fields if _sds_fields else None,
                            pubchem=result.get("pubchem"),
                        )
                        extra_sources = atmo_gwp.apply_atmospheric_gwp_rule(
                            extra_sources, physical_state=_state, state_source=_state_src
                        )

                        # NESHAP: CAA §112(b) HAP list membership
                        try:
                            from utils import neshap_hap

                            hap_path = getattr(config, "P2OASYS_HAP_CSV_PATH", None)
                            if "p2oasys_hap_cas" not in st.session_state:
                                st.session_state["p2oasys_hap_cas"] = neshap_hap.load_hap_cas_set(hap_path)
                            _hap = st.session_state["p2oasys_hap_cas"]
                            if _hap:
                                extra_sources = neshap_hap.apply_neshap_to_extra_sources(
                                    extra_sources, result["clean_cas"], hap_cas=_hap
                                )
                                sources_used.append("NESHAP/HAP list")
                        except Exception:
                            logging.getLogger(__name__).debug("NESHAP HAP lookup skipped", exc_info=True)

                        # Acid Rain Formation: PubChem formula/SMILES primary; HSPiP Y-MBSX optional.
                        try:
                            extra_sources = atmo_gwp.apply_acid_rain_combustion_heuristic(
                                extra_sources,
                                pubchem=pubchem_data or result.get("pubchem"),
                            )
                            if (extra_sources or {}).get("acid_rain_meta"):
                                pipeline_notes.extend(
                                    [n for n in (extra_sources.get("_pipeline_notes") or []) if "Acid Rain" in str(n)]
                                )
                        except Exception:
                            logging.getLogger(__name__).debug("Acid rain heuristic skipped", exc_info=True)

                        if _state != "unknown":
                            pipeline_notes.append(
                                f"Physical state={_state} ({_state_src}); atmospheric GWP/ODP rule applied"
                            )

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
                                sources_used.append("QSAR Toolbox")

                        # PubChem flash-point gap-fill (pubchem-flashpoint-retriever) when LCSS/SDS empty.
                        try:
                            from utils.pubchem_flashpoint_gapfill import gapfill_flash_point_extra

                            _hm0 = (extra_sources or {}).get("hazard_metrics") or {}
                            _existing_fp = list(_hm0.get("flash_point") or [])
                            if not _existing_fp and pubchem_data:
                                _existing_fp = list(pubchem_data.get("flash_point") or [])
                            _fp_xs = gapfill_flash_point_extra(str(result["clean_cas"]), existing_flash=_existing_fp or None)
                            if _fp_xs:
                                extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, _fp_xs)
                                sources_used.append("PubChem-flashpoint-retriever")
                                pipeline_notes.extend(_fp_xs.get("_pipeline_notes") or [])
                        except Exception:
                            logging.getLogger(__name__).debug("Flash point gap-fill skipped", exc_info=True)

                        # HSPiP predicted vapor pressure gap-fill for Physical scoring.
                        try:
                            from v7.hspip_expand import hspip_vp_extra_for_cas

                            _hm1 = (extra_sources or {}).get("hazard_metrics") or {}
                            _has_vp = False
                            for _d in _hm1.get("other_designations") or []:
                                if re.search(r"mm\s*Hg|mmHg", str(_d), re.I):
                                    _has_vp = True
                                    break
                            if not _has_vp and pubchem_data:
                                for _d in pubchem_data.get("vapor_pressure") or []:
                                    if re.search(r"mm\s*Hg|mmHg|Pa|kPa|torr", str(_d), re.I):
                                        _has_vp = True
                                        break
                            if not _has_vp:
                                _smi = str((pubchem_data or {}).get("smiles") or "").strip() or None
                                _vp_xs = hspip_vp_extra_for_cas(str(result["clean_cas"]), smiles=_smi)
                                if _vp_xs:
                                    extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, _vp_xs)
                                    sources_used.append("HSPiP-VP")
                                    pipeline_notes.extend(_vp_xs.get("_pipeline_notes") or [])
                        except Exception:
                            logging.getLogger(__name__).debug("HSPiP VP gap-fill skipped", exc_info=True)

                        try:
                            from unified_hazard_report.iuclid_integration import (
                                get_offline_context,
                                offline_archive_fingerprint,
                            )
                            from unified_hazard_report.unified_lookup import unified_lookup
                            from utils.iuclid_p2oasys_bridge import build_extra_sources_from_iuclid_unified

                            _fp_iu = offline_archive_fingerprint()
                            if _fp_iu:
                                _ctx_iu = get_offline_context(_fp_iu)
                                if _ctx_iu is not None:
                                    _iu_ul = unified_lookup(str(result["clean_cas"]), _ctx_iu)
                                    _iu_xs = build_extra_sources_from_iuclid_unified(_iu_ul)
                                    if _iu_xs:
                                        extra_sources = hazard_for_p2oasys.merge_extra_sources(
                                            extra_sources, _iu_xs
                                        )
                                        sources_used.append("IUCLID")
                        except Exception:
                            logging.getLogger(__name__).debug(
                                "IUCLID → P2OASys merge skipped", exc_info=True
                            )

                        try:
                            from utils import ecosar_client as _ecosar

                            _smi_eco = str((pubchem_data or {}).get("smiles") or "").strip() or None
                            _ecosar_xs = _ecosar.fetch_ecosar_extra_sources(
                                str(result.get("clean_cas") or clean_cas or "") or None,
                                smiles=_smi_eco,
                                existing_hazard=extra_sources or {},
                            )
                            if _ecosar_xs:
                                extra_sources = hazard_for_p2oasys.merge_extra_sources(
                                    extra_sources, _ecosar_xs
                                )
                                sources_used.append("ECOSAR (predicted)")
                                pipeline_notes.extend(
                                    p2oasys_source_bridges.merge_pipeline_notes(_ecosar_xs)
                                )
                        except Exception:
                            logging.getLogger(__name__).debug(
                                "ECOSAR → P2OASys gap-fill skipped", exc_info=True
                            )

                        extra_sources = hazard_for_p2oasys.merge_cameo_extra(
                            str(clean_cas or ""), extra_sources
                        )
                        try:
                            from utils.cameo_lookup import lookup_cameo as _lookup_cameo

                            if _lookup_cameo(str(clean_cas or "")):
                                sources_used.append("CAMEO Chemicals")
                        except Exception:
                            pass

                        # Build experimental base first, then OPERA gap-fill only.
                        hazard_data = hazard_for_p2oasys.build_hazard_data(
                            pubchem_data,
                            toxval_data=result.get("toxval_data"),
                            carc_potency_data=result.get("carc_potency_data"),
                            extra_sources=extra_sources,
                        )
                        hazard_data = atmo_gwp.merge_gwp_into_hazard_data(hazard_data, extra_sources)
                        hazard_data = atmo_gwp.merge_acid_rain_into_hazard_data(hazard_data, extra_sources)
                        if result.get("toxval_data"):
                            sources_used.append("ToxValDB")
                        if result.get("carc_potency_data"):
                            sources_used.append("CPDB")

                        _auto_smiles = str(pubchem_data.get("smiles") or "").strip()
                        if _auto_smiles and opera_client.is_opera_available():
                            try:
                                _xlogp = pubchem_data.get("xlogp")
                                _op = _opera_panel_safe(
                                    _auto_smiles,
                                    str(clean_cas or ""),
                                    str(_xlogp) if _xlogp is not None else "",
                                )
                                _opera_xs = p2oasys_source_bridges.opera_to_extra_sources(
                                    (_op.get("result") or {}),
                                    existing_hazard=hazard_data,
                                )
                                if _opera_xs:
                                    # Merge MW if PubChem lacked it.
                                    if not hazard_data.get("molecular_weight") and _opera_xs.get("molecular_weight"):
                                        hazard_data["molecular_weight"] = _opera_xs["molecular_weight"]
                                    if _opera_xs.get("opera_row"):
                                        hazard_data["opera_row"] = _opera_xs["opera_row"]
                                    if _auto_smiles and not hazard_data.get("smiles"):
                                        hazard_data["smiles"] = _auto_smiles
                                    for t in _opera_xs.get("toxicities") or []:
                                        hazard_data.setdefault("toxicities", []).append(t)
                                    for k, arr in (_opera_xs.get("hazard_metrics") or {}).items():
                                        hazard_data.setdefault("hazard_metrics", {}).setdefault(k, []).extend(arr)
                                    sources_used.append("OPERA (gap-fill)")
                                    pipeline_notes.extend(p2oasys_source_bridges.merge_pipeline_notes(_opera_xs))
                            except Exception:
                                logging.getLogger(__name__).debug(
                                    "OPERA → P2OASys gap-fill skipped", exc_info=True
                                )

                        matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
                        scores, score_trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(
                            hazard_data, matrix
                        )
                        try:
                            from utils import p2oasys_form as _p2o_form

                            scores = _p2o_form.strip_manual_only_scores(scores)
                        except Exception:
                            pass
                        st.session_state["p2o_main_auto_scores"] = scores
                        st.session_state["p2o_main_auto_cas"] = clean_cas
                        try:
                            _src = locals().get("sources_used")
                            if _src is None:
                                _src = list(st.session_state.get("p2o_main_sources") or [])
                            st.session_state["p2o_main_sources"] = list(_src)
                            p2oasys_score_lookup.upsert_auto_from_draft(
                                {
                                    "ok": True,
                                    "cas": clean_cas,
                                    "chemical_name": preferred_name_p2o,
                                    "scores": scores,
                                    "sources_used": _src,
                                }
                            )
                        except Exception:
                            pass
                        # Paint ribbon from the scores that just finished (same run).
                        p2oasys_score_ribbon.render_score_ribbon(clean_cas, auto_scores=scores)
                        matrix_fp = p2oasys_scorer.matrix_fingerprint(matrix_path)

                    # --- Step 2: Gather status ---
                    with st.expander("2 · Evidence gathering", expanded=False):
                        st.write("Sources used: " + ", ".join(dict.fromkeys(sources_used)))
                        if pipeline_notes:
                            for note in pipeline_notes:
                                st.caption(f"• {note}")
                        n_tox = len(hazard_data.get("toxicities") or [])
                        n_h = len((hazard_data.get("ghs") or {}).get("h_codes") or [])
                        st.caption(f"Toxicity evidence rows: {n_tox} · GHS H-codes: {n_h}")

                    # --- Step 3: Coverage / conflicts ---
                    cat_status = score_trace.get("category_status") or {}
                    rejected = score_trace.get("rejected") or []
                    missing = score_trace.get("missing") or []
                    with st.expander("3 · Coverage and conflicts", expanded=False):
                        status_rows = [
                            {"Category": c, "Status": s}
                            for c, s in cat_status.items()
                        ]
                        if status_rows:
                            st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)
                        st.caption(
                            f"Rejected evidence: {len(rejected)} · Missing matrix units: {len(missing)}"
                        )
                        if rejected:
                            with st.expander(f"Rejected evidence ({len(rejected)})"):
                                st.dataframe(pd.DataFrame(rejected), use_container_width=True, hide_index=True)
                        if missing:
                            with st.expander(f"Missing / not assessed units ({len(missing)})"):
                                st.dataframe(pd.DataFrame(missing), use_container_width=True, hide_index=True)

                    # --- Step 4: Draft scores ---
                    overall_max = p2oasys_aggregate.aggregate_category_scores(scores, "max")
                    overall_mean = p2oasys_aggregate.aggregate_category_scores(scores, "mean")
                    overall_weighted = p2oasys_aggregate.aggregate_category_scores(scores, "weighted_mean")
                    n_cat, _ = p2oasys_aggregate.count_scored_categories(scores)
                    with st.expander("4 · Automatic P2OASys scores", expanded=False):
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
                            if category.startswith("_") or not isinstance(data, dict):
                                continue
                            cat_max = data.get("_category_max")
                            for subcat, subdata in data.items():
                                if subcat.startswith("_") or not isinstance(subdata, dict):
                                    continue
                                for unit_name, score in subdata.items():
                                    if unit_name != "_max" and isinstance(score, (int, float)):
                                        match = next(
                                            (
                                                s for s in (score_trace.get("scored") or [])
                                                if s.get("category") == category
                                                and s.get("subcategory") == subcat
                                                and s.get("unit") == unit_name
                                            ),
                                            {},
                                        )
                                        rows.append({
                                            "Category": category,
                                            "Subcategory": subcat,
                                            "Endpoint": unit_name,
                                            "Score": score,
                                            "Status": match.get("status") or "Scored",
                                            "Input": match.get("input_value"),
                                            "Qualifier": match.get("qualifier"),
                                            "Category max": cat_max,
                                        })
                        if rows:
                            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                        else:
                            st.info("No itemized scores for this compound with current data and matrix.")

                    # --- Step 5: Review / override ---
                    with st.expander("5 · Analyst review / override", expanded=False):
                        st.caption("Overrides require a justification and are recorded in the assessment package.")
                        ov_cat = st.text_input("Override category", key="p2o_ov_cat")
                        ov_sub = st.text_input("Override subcategory", key="p2o_ov_sub")
                        ov_unit = st.text_input("Override endpoint / unit", key="p2o_ov_unit")
                        ov_score = st.selectbox("Override score", [2, 4, 6, 8, 10], key="p2o_ov_score")
                        ov_just = st.text_area("Justification (required)", key="p2o_ov_just")
                        ov_analyst = st.text_input("Analyst initials", value="", key="p2o_ov_analyst")
                        if "p2oasys_overrides" not in st.session_state:
                            st.session_state["p2oasys_overrides"] = []
                        if st.button("Apply override", key="p2o_ov_apply"):
                            if not (ov_cat and ov_sub and ov_unit and ov_just.strip()):
                                st.error("Category, subcategory, unit, and justification are required.")
                            else:
                                st.session_state["p2oasys_overrides"].append({
                                    "category": ov_cat.strip(),
                                    "subcategory": ov_sub.strip(),
                                    "unit": ov_unit.strip(),
                                    "score": int(ov_score),
                                    "justification": ov_just.strip(),
                                    "analyst": ov_analyst.strip() or "analyst",
                                })
                                st.success("Override staged for this session.")
                        if st.session_state.get("p2oasys_overrides"):
                            st.dataframe(
                                pd.DataFrame(st.session_state["p2oasys_overrides"]),
                                use_container_width=True,
                                hide_index=True,
                            )
                            if st.button("Clear overrides", key="p2o_ov_clear"):
                                st.session_state["p2oasys_overrides"] = []

                    overrides = list(st.session_state.get("p2oasys_overrides") or [])
                    assessment = p2oasys_assessment.build_assessment_package(
                        cas=clean_cas,
                        chemical_name=preferred_name_p2o or None,
                        hazard_data=hazard_data,
                        scores=scores,
                        trace=score_trace,
                        matrix_path=matrix_path,
                        matrix_kind=matrix_kind,
                        overrides=overrides,
                        pipeline_notes=pipeline_notes,
                        sources_used=sources_used,
                    )

                    # --- Step 6: Export ---
                    with st.expander("6 · Export assessment", expanded=True):
                        _fp_hash = (matrix_fp.get("sha256") or "")[:12]
                        st.caption(
                            f"Matrix: `{matrix_fp.get('filename')}` ({matrix_kind}) · "
                            f"sha256 `{_fp_hash}…` · scorer `{score_trace.get('scorer_version')}` · "
                            f"schema `{assessment.get('schema_version')}`"
                        )
                        require_official = getattr(config, "P2OASYS_REQUIRE_OFFICIAL_MATRIX", True)
                        allow_placeholder = st.checkbox(
                            "Allow export of placeholder-matrix draft (not TURI-calibrated)",
                            value=not require_official,
                            key="p2o_allow_placeholder_export",
                        )
                        can_export = p2oasys_assessment.assessment_is_exportable(
                            matrix_kind, allow_placeholder=allow_placeholder
                        )
                        if rows:
                            st.download_button(
                                "Download P2OASys scores (CSV)",
                                data=pd.DataFrame(rows).to_csv(index=False),
                                file_name=f"p2oasys_scores_{clean_cas.replace('-', '_')}.csv",
                                mime="text/csv",
                                key="download_p2oasys_csv",
                                disabled=not can_export,
                            )
                        try:
                            _sds_src = ""
                            _sds_url = ""
                            _sds_year = ""
                            _sds_fields = st.session_state.get("sds_hazard_fields") or st.session_state.get("sds_extracted_fields") or {}
                            if isinstance(_sds_fields, dict):
                                _sds_src = str(_sds_fields.get("sds_source") or _sds_fields.get("source") or "")
                                _sds_url = str(_sds_fields.get("sds_url") or _sds_fields.get("url") or "")
                                _sds_year = str(_sds_fields.get("sds_year") or _sds_fields.get("year") or "")
                            _upload_csv = p2oasys_upload_csv.build_single_cas_upload_csv(
                                cas=clean_cas,
                                name=str(preferred_name_p2o or ""),
                                scores=scores,
                                score_trace=score_trace,
                                sds_source=_sds_src,
                                sds_url=_sds_url,
                                sds_year=_sds_year,
                            )
                            _ustats = p2oasys_upload_csv.upload_csv_stats()
                            st.download_button(
                                "Download P2OASys website upload (CSV)",
                                data=_upload_csv,
                                file_name=f"p2oasys_upload_{clean_cas.replace('-', '_')}.csv",
                                mime="text/csv",
                                key="download_p2oasys_website_upload",
                                disabled=not can_export,
                                help="Single-CAS file in the P2OASys 'Upload from file' layout. Process/Life Cycle blank.",
                            )
                            st.caption(
                                f"Website upload CSV: {_ustats.get('filled_units', 0)} units filled; "
                                "Process / Life Cycle left blank for human curation."
                            )
                        except Exception:
                            logging.getLogger(__name__).exception("P2OASys website-upload CSV failed")
                            st.caption("Website-upload CSV could not be built for this run.")
                        st.download_button(
                            "Download assessment package (JSON)",
                            data=json.dumps(assessment, indent=2, default=str),
                            file_name=f"p2oasys_assessment_{clean_cas.replace('-', '_')}.json",
                            mime="application/json",
                            key="download_p2oasys_assessment",
                            disabled=not can_export,
                        )
                        st.download_button(
                            "Download assessment summary (HTML)",
                            data=p2oasys_assessment.assessment_to_html(assessment),
                            file_name=f"p2oasys_assessment_{clean_cas.replace('-', '_')}.html",
                            mime="text/html",
                            key="download_p2oasys_html",
                            disabled=not can_export,
                        )
                        st.download_button(
                            "Download scoring trace (JSON)",
                            data=json.dumps(
                                {"cas": clean_cas, "matrix": matrix_fp, "matrix_kind": matrix_kind, **score_trace},
                                indent=2,
                                default=str,
                            ),
                            file_name=f"p2oasys_trace_{clean_cas.replace('-', '_')}.json",
                            mime="application/json",
                            key="download_p2oasys_trace",
                            disabled=not can_export,
                        )
                        if not can_export:
                            st.info(
                                "Export blocked: official TURI matrix required "
                                "(`P2OASYS_REQUIRE_OFFICIAL_MATRIX=1`). Check the box above to export a "
                                "placeholder draft for development only."
                            )
                        st.caption(assessment.get("disclaimer") or "")

    elif _panel == "SDS auto search":
        if not result or not result.get("clean_cas"):
            render_curated_knowledge(None)
        else:
            try:
                orch = _v7_orchestrator()
                clean = str(result["clean_cas"])
                auto = bool(st.session_state.pop("auto_run_sds_tci", False)) or not st.session_state.get(
                    f"curated_sds_auto_done_{clean}"
                )
                sds_rec, sds_meta = render_sds_fetch_panel(
                    clean,
                    orch,
                    pubchem=result.get("pubchem"),
                    auto_run=auto,
                )
                rec = sds_rec or orch.assemble(
                    clean,
                    pubchem=result.get("pubchem"),
                )
                # If auto-fetch returned a record earlier in the same call, prefer it
                if sds_rec is not None:
                    rec = sds_rec
                render_curated_knowledge(rec, sds_meta=sds_meta)
            except Exception as exc:
                st.error(f"SDS auto search failed: {exc}")

    elif _panel == "OPERA vs ToxVal":
        data_dir = Path(config.DATA_DIR)
        endpoints_json = data_dir / "opera_endpoints.json"
        mapping_path = data_dir / "opera_to_toxval_mapping.json"
        db_path = Path(config.CHEMICAL_DB_PATH)

        mapping = load_mapping(mapping_path).get("mapping", {})
        available_eps = [k for k, v in mapping.items() if (v.get("toxval_types") or [])]
        if not available_eps:
            if st.button("Build OPERA→ToxVal mapping", key="unc_build_map"):
                mapper = OperaEndpointMapper(
                    endpoints_json_path=endpoints_json,
                    toxval_sqlite_path=db_path if db_path.is_file() else None,
                    mapping_output_path=mapping_path,
                )
                out = mapper.build_and_save()
                st.success(f"Mapping ready: {out}")
                st.rerun()
            else:
                st.info("No OPERA→ToxVal mapping yet. Click once to build it from local data.")
        else:
            endpoint = st.selectbox(
                "OPERA endpoint",
                options=sorted(available_eps),
                key="unc_endpoint",
            )
            mapped_types = mapping.get(endpoint, {}).get("toxval_types", [])
            cas_default = (result or {}).get("clean_cas") if isinstance(result, dict) else ""
            cas_list = [str(cas_default).strip()] if cas_default else []
            auto_unc = bool(st.session_state.pop("auto_run_opera_toxval", False))
            run_unc = auto_unc or st.button("Re-run comparison", key="unc_run")
            if run_unc:
                if not cas_list:
                    st.warning("Enter a CAS first.")
                else:
                    with st.spinner("Running OPERA vs ToxVal…"):
                        smiles_map = opera_batch.smiles_from_cas_batch(cas_list)
                        smiles = [smiles_map[c] for c in cas_list if smiles_map.get(c)]
                        pred_by_smiles = (
                            opera_batch.predict_opera_batch(smiles, endpoint) if smiles else {}
                        )
                        rows: list[dict[str, Any]] = []
                        for cas_u in cas_list:
                            smi = smiles_map.get(cas_u) or ""
                            pred = pred_by_smiles.get(smi, {}) if smi else {}
                            op_val = pd.to_numeric(pred.get("value"), errors="coerce")
                            tox_rows = chemical_db.get_toxicity_by_cas(cas_u, numeric_only=True)
                            vals: list[float] = []
                            for r in tox_rows:
                                stype = str(r.get("study_type") or "").strip().lower()
                                if stype not in {t.lower() for t in mapped_types}:
                                    continue
                                vv = pd.to_numeric(r.get("toxval_numeric"), errors="coerce")
                                if pd.notna(vv):
                                    vals.append(float(vv))
                            med = float(np.median(vals)) if vals else float("nan")
                            vmin = float(np.min(vals)) if vals else float("nan")
                            vmax = float(np.max(vals)) if vals else float("nan")
                            badge = "No data"
                            if pd.notna(op_val) and pd.notna(med) and med > 0 and op_val > 0:
                                ratio = max(op_val / med, med / op_val)
                                if ratio <= 2:
                                    badge = "Pass"
                                elif ratio <= 5:
                                    badge = "Warning"
                                else:
                                    badge = "Fail"
                            rows.append(
                                {
                                    "CAS": cas_u,
                                    "endpoint": endpoint,
                                    "opera_prediction": float(op_val) if pd.notna(op_val) else np.nan,
                                    "toxval_n": len(vals),
                                    "toxval_median": med,
                                    "toxval_min": vmin,
                                    "toxval_max": vmax,
                                    "agreement_badge": badge,
                                }
                            )
                        out_df = pd.DataFrame(rows)
                        st.session_state["unc_last_df"] = out_df
            out_df = st.session_state.get("unc_last_df")
            if isinstance(out_df, pd.DataFrame) and not out_df.empty:
                st.dataframe(out_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download CSV",
                    data=out_df.to_csv(index=False),
                    file_name=f"opera_toxval_{endpoint}.csv",
                    mime="text/csv",
                    key="unc_download_csv",
                )

# Footer when no query yet
if not current_query:
    st.markdown("---")
