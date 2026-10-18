"""
P2OASys assessment (human-in-the-loop) — GHaz7.

Matrix-driven subcategory dropdowns (scores 2/4/6/8/10 + cell labels). Auto-draft
prefills Acute / Chronic / Ecological / Fate & Transport / Atmospheric / Physical;
Process Factors and Life Cycle Factors start unselected.

Run from the app root::

    streamlit run app.py
    # then open sidebar page "P2OASys Assessment"

Or::

    streamlit run pages/05_P2OASys_Assessment.py

Optional env (Windows): dot-source ``GHhaz5\\env\\local_paths.ps1`` before starting.
"""

from __future__ import annotations

import re

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="P2OASys Assessment",
    layout="wide",
    initial_sidebar_state="expanded",
)

import config
from services.chemical_assessment import ChemicalAssessmentService
from utils import (
    atmo_gwp,
    hazard_for_p2oasys,
    iarc_lookup,
    lookup_tables,
    opera_client,
    p2oasys_aggregate,
    p2oasys_assessment,
    p2oasys_form,
    p2oasys_matrix_placeholder,
    p2oasys_scorer,
    p2oasys_source_bridges,
)
from utils.markitdown_check import is_markitdown_available
from utils import p2oasys_score_lookup, p2oasys_score_ribbon

logger = logging.getLogger(__name__)

try:
    from utils import sds_pdf_utils, sds_regex_extractor
    from utils.input_handler import get_input_handler
except ImportError:
    sds_pdf_utils = sds_regex_extractor = None  # type: ignore[misc, assignment]
    get_input_handler = None  # type: ignore[misc, assignment]

try:
    from utils import qsar_toolbox_client
except ImportError:
    qsar_toolbox_client = None  # type: ignore[misc, assignment]

MARKITDOWN_OK = is_markitdown_available()

SESSION_DRAFT = "p2o_hitl_draft"
SESSION_SELECTIONS = "p2o_hitl_selections"
SESSION_SDS_FIELDS = "p2o_hitl_sds_fields"


def _cas_key(cas: str) -> str:
    return (cas or "").strip()


@st.cache_data(ttl=86400, show_spinner=False, max_entries=64)
def _cached_matrix() -> tuple[dict[str, Any], str, str]:
    """Return (matrix, matrix_kind, path_str)."""
    path, kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH),
        Path(config.DATA_DIR),
    )
    if kind == "missing" or not path.is_file():
        return {}, kind, str(path)
    matrix = p2oasys_scorer.load_p2oasys_matrix(path)
    return matrix, kind, str(path)


def _try_opera_cached_only(smiles: str, cas: str, xlogp: Any) -> Optional[dict[str, Any]]:
    """
    OPERA gap-fill without hanging the UI on a cold CLI run.

    Prefer precompute SQLite (`get_cas_row`). Never use a short CLI timeout here:
    cold OPERA 2.9 typically needs ~90s for one structure, so a 20s timeout always
    fails. On cache miss, skip CLI (main app / precompute scripts populate the DB).
    """
    smiles = (smiles or "").strip()
    if not smiles:
        return None
    try:
        from utils import opera_precompute_cache

        db = opera_precompute_cache.default_precompute_db_path()
        row = opera_precompute_cache.get_cas_row(db, cas, smiles)
        if not row:
            # Fall back to CAS-only cache (batch precompute may store a different SMILES form)
            row = opera_precompute_cache.get_cas_row(db, cas)
        if not row:
            logger.info("OPERA precompute miss for %s — skipping CLI on HITL page", cas)
            return None
        warnings = list(opera_client.check_opera_pubchem_logp(row, xlogp))
        display: dict[str, str] = {}
        for label, col in getattr(opera_client, "DISPLAY_COLUMNS", {}).items():
            v = row.get(col, "")
            if v != "" and v != "NA" and str(v).lower() != "nan":
                display[label] = str(v).strip()
        return {
            "ok": True,
            "error": None,
            "display": display,
            "row": row,
            "warnings": warnings,
            "from_precompute": True,
        }
    except Exception:
        logger.debug("OPERA precompute read failed", exc_info=True)
        return None




def _gather_draft(cas: str, sds_fields: dict[str, Any] | None) -> dict[str, Any]:
    """Mirror app.py draft evidence path; return package of intermediate artifacts."""
    svc = ChemicalAssessmentService()
    assessment = svc.assess(cas)
    result = svc.to_result_data(assessment)
    clean_cas = result.get("clean_cas") or cas
    pubchem = result.get("pubchem") or {}
    preferred_name = result.get("preferred_name") or ""

    matrix, matrix_kind, matrix_path_str = _cached_matrix()
    if not matrix:
        return {
            "ok": False,
            "error": (
                "P2OASys matrix not available. Add "
                "`Hazard Matrix Group Review 9-19-23.xlsx` under data/ or set "
                "P2OASYS_MATRIX_PATH."
            ),
            "result": result,
        }
    if not pubchem:
        return {
            "ok": False,
            "error": result.get("fetch_error")
            or "Draft scoring needs a successful PubChem retrieval for this CAS.",
            "result": result,
            "matrix_kind": matrix_kind,
            "matrix_path": matrix_path_str,
        }

    pipeline_notes: list[str] = []
    sources_used: list[str] = ["PubChem"]
    extra_sources = None

    iarc_by_cas = None
    odp_gwp_by_cas = None
    ipcc_gwp_by_cas = None
    iarc_dir = getattr(config, "IARC_DIR", None)
    if iarc_dir and os.path.isdir(iarc_dir):
        iarc_by_cas = iarc_lookup.load_iarc_from_iarc_folder(iarc_dir)
    if not iarc_by_cas and getattr(config, "P2OASYS_IARC_CSV_PATH", None):
        iarc_path = config.P2OASYS_IARC_CSV_PATH
        if iarc_path and os.path.isfile(iarc_path):
            iarc_by_cas = lookup_tables.load_iarc_csv(iarc_path)
    if getattr(config, "P2OASYS_ODP_GWP_CSV_PATH", None):
        odp_path = config.P2OASYS_ODP_GWP_CSV_PATH
        if odp_path and os.path.isfile(odp_path):
            odp_gwp_by_cas = lookup_tables.load_odp_gwp_csv(odp_path)
    atmo_dir = atmo_gwp.resolve_atmo_dir(
        getattr(config, "ATMO_DIR", None),
        repo_root=getattr(config, "REPO_ROOT", None),
        fastp2oasys_dir=getattr(config, "FASTP2OASYS_DIR", None),
    )
    if atmo_dir is not None:
        ipcc_gwp_by_cas = atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir)
    if iarc_by_cas or odp_gwp_by_cas or ipcc_gwp_by_cas:
        extra_sources = lookup_tables.get_lookup_extra_sources(
            clean_cas,
            iarc_by_cas=iarc_by_cas,
            odp_gwp_by_cas=odp_gwp_by_cas,
            ipcc_gwp_by_cas=ipcc_gwp_by_cas,
        )
        sources_used.append("IARC/ODP/GWP lookups")

    if sds_fields:
        sds_xs = p2oasys_source_bridges.sds_fields_to_extra_sources(sds_fields)
        if sds_xs:
            extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, sds_xs)
            sources_used.append("SDS")
            pipeline_notes.extend(p2oasys_source_bridges.merge_pipeline_notes(sds_xs))

    # Atmospheric GWP/ODP rule: non-gas → 0; unlisted gas/unknown → 0 (authoritative lists).
    _state, _state_src = atmo_gwp.infer_physical_state_from_sources(
        sds_fields=sds_fields, pubchem=pubchem
    )
    extra_sources = atmo_gwp.apply_atmospheric_gwp_rule(
        extra_sources, physical_state=_state, state_source=_state_src
    )

    # NESHAP: CAA §112(b) HAP list membership
    try:
        from utils import neshap_hap

        hap_path = getattr(config, "P2OASYS_HAP_CSV_PATH", None)
        hap_cas = neshap_hap.load_hap_cas_set(hap_path)
        if hap_cas:
            extra_sources = neshap_hap.apply_neshap_to_extra_sources(
                extra_sources, clean_cas, hap_cas=hap_cas
            )
            sources_used.append("NESHAP/HAP list")
    except Exception:
        pass

    # Acid Rain Formation: PubChem formula/SMILES primary; HSPiP Y-MBSX N#/S# optional.
    try:
        extra_sources = atmo_gwp.apply_acid_rain_combustion_heuristic(
            extra_sources,
            pubchem=pubchem,
        )
        if (extra_sources or {}).get("acid_rain_meta"):
            pipeline_notes.extend(
                [n for n in (extra_sources.get("_pipeline_notes") or []) if "Acid Rain" in str(n)]
            )
    except Exception:
        logger.debug("Acid rain heuristic skipped", exc_info=True)

    if _state != "unknown":
        pipeline_notes.append(
            f"Physical state={_state} ({_state_src}); atmospheric GWP/ODP rule applied"
        )

    port = getattr(config, "QSAR_TOOLBOX_PORT", None)
    if qsar_toolbox_client and port and qsar_toolbox_client.is_available(port):
        try:
            qtb_rows = qsar_toolbox_client.fetch_by_cas(clean_cas, port) or []
            if qtb_rows:
                extra_sources = hazard_for_p2oasys.merge_extra_sources(
                    extra_sources,
                    qsar_toolbox_client.toolbox_results_to_extra_sources(qtb_rows),
                )
                sources_used.append("QSAR Toolbox")
        except Exception:
            logger.debug("QSAR Toolbox merge skipped", exc_info=True)


    # PubChem flash-point gap-fill (pubchem-flashpoint-retriever) when LCSS/SDS empty.
    try:
        from utils.pubchem_flashpoint_gapfill import gapfill_flash_point_extra

        _hm0 = (extra_sources or {}).get("hazard_metrics") or {}
        _existing_fp = list(_hm0.get("flash_point") or [])
        if not _existing_fp and pubchem:
            _existing_fp = list(pubchem.get("flash_point") or [])
        _fp_xs = gapfill_flash_point_extra(str(clean_cas), existing_flash=_existing_fp or None)
        if _fp_xs:
            extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, _fp_xs)
            sources_used.append("PubChem-flashpoint-retriever")
            pipeline_notes.extend(_fp_xs.get("_pipeline_notes") or [])
    except Exception:
        logger.debug("Flash point gap-fill skipped", exc_info=True)

    # HSPiP predicted vapor pressure gap-fill for Physical scoring.
    try:
        from v7.hspip_expand import hspip_vp_extra_for_cas

        _hm1 = (extra_sources or {}).get("hazard_metrics") or {}
        _has_vp = False
        for _d in _hm1.get("other_designations") or []:
            if re.search(r"mm\s*Hg|mmHg", str(_d), re.I):
                _has_vp = True
                break
        if not _has_vp and pubchem:
            for _d in pubchem.get("vapor_pressure") or []:
                if re.search(r"mm\s*Hg|mmHg|Pa|kPa|torr", str(_d), re.I):
                    _has_vp = True
                    break
        if not _has_vp:
            _smi = str((pubchem or {}).get("smiles") or "").strip() or None
            _vp_xs = hspip_vp_extra_for_cas(str(clean_cas), smiles=_smi)
            if _vp_xs:
                extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, _vp_xs)
                sources_used.append("HSPiP-VP")
                pipeline_notes.extend(_vp_xs.get("_pipeline_notes") or [])
    except Exception:
        logger.debug("HSPiP VP gap-fill skipped", exc_info=True)

    try:
        from unified_hazard_report.iuclid_integration import (
            get_offline_context,
            offline_archive_fingerprint,
        )
        from unified_hazard_report.unified_lookup import unified_lookup
        from utils.iuclid_p2oasys_bridge import build_extra_sources_from_iuclid_unified

        fp_iu = offline_archive_fingerprint()
        if fp_iu:
            ctx_iu = get_offline_context(fp_iu)
            if ctx_iu is not None:
                iu_ul = unified_lookup(str(clean_cas), ctx_iu)
                iu_xs = build_extra_sources_from_iuclid_unified(iu_ul)
                if iu_xs:
                    extra_sources = hazard_for_p2oasys.merge_extra_sources(
                        extra_sources, iu_xs
                    )
                    sources_used.append("IUCLID")
    except Exception:
        logger.debug("IUCLID → P2OASys merge skipped", exc_info=True)

    try:
        from utils import ecosar_client

        ecosar_xs = ecosar_client.fetch_ecosar_extra_sources(
            str(clean_cas or "") or None,
            smiles=str(pubchem.get("smiles") or "").strip() or None,
            existing_hazard=extra_sources or {},
        )
        if ecosar_xs:
            extra_sources = hazard_for_p2oasys.merge_extra_sources(extra_sources, ecosar_xs)
            sources_used.append("ECOSAR (predicted)")
            pipeline_notes.extend(p2oasys_source_bridges.merge_pipeline_notes(ecosar_xs))
    except Exception:
        logger.debug("ECOSAR → P2OASys gap-fill skipped", exc_info=True)

    extra_sources = hazard_for_p2oasys.merge_cameo_extra(str(clean_cas or ""), extra_sources)

    hazard_data = hazard_for_p2oasys.build_hazard_data(
        pubchem,
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

    smiles = str(pubchem.get("smiles") or "").strip()
    if smiles and not hazard_data.get("smiles"):
        hazard_data["smiles"] = smiles
    if clean_cas and not hazard_data.get("cas"):
        hazard_data["cas"] = str(clean_cas)
    if smiles:
        opera_panel = _try_opera_cached_only(smiles, str(clean_cas or ""), pubchem.get("xlogp"))
        if opera_panel:
            try:
                opera_xs = p2oasys_source_bridges.opera_to_extra_sources(
                    opera_panel,
                    existing_hazard=hazard_data,
                )
                if opera_xs:
                    if not hazard_data.get("molecular_weight") and opera_xs.get("molecular_weight"):
                        hazard_data["molecular_weight"] = opera_xs["molecular_weight"]
                    if opera_xs.get("opera_row"):
                        hazard_data["opera_row"] = opera_xs["opera_row"]
                    for t in opera_xs.get("toxicities") or []:
                        hazard_data.setdefault("toxicities", []).append(t)
                    for k, arr in (opera_xs.get("hazard_metrics") or {}).items():
                        hazard_data.setdefault("hazard_metrics", {}).setdefault(k, []).extend(arr)
                    for fate_key in ("log_kow", "bcf_l_kg", "biodeg_half_life_days"):
                        if opera_xs.get(fate_key) is not None and hazard_data.get(fate_key) is None:
                            hazard_data[fate_key] = opera_xs[fate_key]
                    sources_used.append("OPERA (cached)")
                    pipeline_notes.extend(p2oasys_source_bridges.merge_pipeline_notes(opera_xs))
            except Exception:
                logger.debug("OPERA → P2OASys gap-fill skipped", exc_info=True)
        else:
            pipeline_notes.append(
                "OPERA skipped on this page (no precompute cache hit / not installed)."
            )

    scores, score_trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(
        hazard_data, matrix
    )
    # Never present auto scores for Process / Life Cycle.
    scores = p2oasys_form.strip_manual_only_scores(scores)

    fields = p2oasys_form.build_form_fields(matrix)
    suggestions = p2oasys_form.map_auto_suggestions(matrix, scores, score_trace)

    return {
        "ok": True,
        "cas": clean_cas,
        "chemical_name": preferred_name,
        "result": result,
        "hazard_data": hazard_data,
        "scores": scores,
        "trace": score_trace,
        "matrix": matrix,
        "matrix_kind": matrix_kind,
        "matrix_path": matrix_path_str,
        "fields": fields,
        "suggestions": {
            f"{c}||{s}": v for (c, s), v in suggestions.items()
        },
        "sources_used": sources_used,
        "pipeline_notes": pipeline_notes,
    }


def _suggestions_from_draft(draft: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    raw = draft.get("suggestions") or {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for k, v in raw.items():
        if "||" in k:
            c, s = k.split("||", 1)
            out[(c, s)] = v
    return out


def _default_selections(draft: dict[str, Any]) -> dict[str, Optional[int]]:
    """Session-serializable selections keyed by 'cat||sub'."""
    sug = _suggestions_from_draft(draft)
    sel: dict[str, Optional[int]] = {}
    for field in draft.get("fields") or []:
        key = f"{field['category']}||{field['subcategory']}"
        suggestion = sug.get((field["category"], field["subcategory"])) or {}
        if field.get("allow_auto") and suggestion.get("source") == "auto":
            sel[key] = suggestion.get("score")
        else:
            sel[key] = None
    return sel


def _render_sidebar_summary(
    scores: dict[str, Any],
    matrix: dict[str, Any],
    *,
    cas: str,
    chemical_name: str,
    matrix_kind: str,
) -> None:
    rows = p2oasys_form.category_score_rows(scores, matrix)
    st.sidebar.header("Category scores")
    st.sidebar.caption(f"CAS {cas}" + (f" — {chemical_name}" if chemical_name else ""))
    if matrix_kind == "placeholder":
        st.sidebar.warning("Dev placeholder matrix — not TURI-calibrated.")
    for row in rows:
        score = row["score"]
        label = row["category"]
        if row["manual_only"]:
            label = f"{label} *"
        if score is None:
            st.sidebar.write(f"**{label}**: —")
        else:
            st.sidebar.metric(label, f"{score:g}" if isinstance(score, float) else score)
    overall = p2oasys_aggregate.aggregate_category_scores(scores, "max")
    n_cat, _ = p2oasys_aggregate.count_scored_categories(scores)
    st.sidebar.markdown("---")
    st.sidebar.metric("Overall (max of categories)", "—" if overall != overall else f"{overall:g}")
    st.sidebar.caption(f"{n_cat} categories scored. * = Process/Life Cycle (manual).")


# -----------------------------------------------------------------------------
# Page body
# -----------------------------------------------------------------------------

st.title("P2OASys assessment")
st.caption(
    "Human-in-the-loop draft: auto-score six hazard categories from evidence, then "
    "confirm or override every subcategory using official matrix bins (2 / 4 / 6 / 8 / 10). "
    "**Process Factors** and **Life Cycle Factors** are never auto-filled."
)

matrix, matrix_kind, matrix_path_str = _cached_matrix()
if not matrix:
    st.error(
        "P2OASys scoring matrix not available. Place "
        "`Hazard Matrix Group Review 9-19-23.xlsx` in `data/` or set `P2OASYS_MATRIX_PATH`."
    )
    st.stop()

if matrix_kind == "placeholder":
    st.warning(
        "Using **dev placeholder matrix**. Install the official TURI workbook for "
        "calibrated scores: https://p2oasys.turi.org/chemical/hazard-score-matrix"
    )

col_cas, col_sds = st.columns([2, 2])
with col_cas:
    cas_in = st.text_input("CAS number", value="67-56-1", placeholder="e.g. 67-56-1", key="p2o_hitl_cas")
with col_sds:
    sds_upload = None
    if sds_pdf_utils and sds_regex_extractor and MARKITDOWN_OK and get_input_handler:
        sds_upload = st.file_uploader("Optional SDS (PDF)", type=["pdf"], key="p2o_hitl_sds")
    else:
        st.caption("SDS upload unavailable (MarkItDown / SDS parsers not loaded).")

sds_fields: dict[str, Any] = st.session_state.get(SESSION_SDS_FIELDS) or {}
if sds_upload is not None and get_input_handler is not None:
    name = sds_upload.name
    if st.session_state.get("_p2o_hitl_sds_name") != name:
        st.session_state["_p2o_hitl_sds_name"] = name
        with st.spinner("Extracting CAS / hazard fields from SDS…"):
            staged = get_input_handler().process_sds_pdf(sds_upload)
        sds_fields = getattr(staged, "hazard_fields", None) or {}
        st.session_state[SESSION_SDS_FIELDS] = sds_fields
        if staged and staged.cas_numbers:
            if len(staged.cas_numbers) == 1:
                st.info(f"SDS CAS detected: **{staged.cas_numbers[0]}** — paste into the CAS box if needed.")
            else:
                st.info("Multiple CAS in SDS: " + ", ".join(staged.cas_numbers[:8]))


# Cached expert/auto ribbon (before Generate draft)
_cas_for_ribbon = _cas_key(cas_in)
_lookup_row = None
if _cas_for_ribbon:
    _lookup_row = p2oasys_score_ribbon.render_score_ribbon(_cas_for_ribbon)
else:
    st.caption("Enter a CAS to check the score lookup ribbon.")

fetch_tci_anyway = st.checkbox(
    "Fetch TCI SDS anyway",
    value=False,
    key="p2o_hitl_fetch_tci",
    help=(
        "When cached expert/auto scores exist, automatic TCI HTTP is skipped. "
        "Check this to force a TCI SDS fetch (map/cache; live search stays off)."
    ),
)

gen = st.button("Generate draft", type="primary", key="p2o_hitl_generate")

if gen:
    cas_clean = _cas_key(cas_in)
    if not cas_clean:
        st.error("Enter a CAS number.")
    else:
        # TCI: skip when lookup has scores unless user opted in.
        effective_sds = dict(sds_fields or {})
        tci_note = None
        if fetch_tci_anyway:
            with st.spinner("Fetching TCI SDS (opt-in)…"):
                tci_fields = p2oasys_score_ribbon.try_tci_sds_fields(cas_clean)
            if tci_fields:
                effective_sds = {**effective_sds, **tci_fields}
                tci_note = "TCI SDS fetched on explicit user request."
            else:
                tci_note = "TCI SDS opt-in attempted but no fields returned."
        elif p2oasys_score_lookup.should_skip_tci(cas_clean):
            tci_note = (
                "TCI auto-fetch skipped — cached expert/auto P2OASys scores in lookup DB."
            )
        with st.spinner("Gathering evidence and computing draft P2OASys scores…"):
            draft = _gather_draft(cas_clean, effective_sds or None)
        if draft.get("ok") and tci_note:
            notes = list(draft.get("pipeline_notes") or [])
            notes.insert(0, tci_note)
            draft["pipeline_notes"] = notes
        if not draft.get("ok"):
            st.error(draft.get("error") or "Draft failed.")
            st.session_state.pop(SESSION_DRAFT, None)
        else:
            st.session_state[SESSION_DRAFT] = draft
            st.session_state[SESSION_SELECTIONS] = _default_selections(draft)
            st.session_state["p2o_hitl_gen"] = int(st.session_state.get("p2o_hitl_gen") or 0) + 1
            upsert_row = None
            try:
                upsert_row = p2oasys_score_lookup.upsert_auto_from_draft(draft)
            except Exception:
                logger.debug("Score-lookup upsert skipped", exc_info=True)
            st.success(
                f"Draft ready for {draft.get('cas')} "
                f"({draft.get('chemical_name') or 'name unknown'}). "
                f"Sources: {', '.join(draft.get('sources_used') or [])}"
            )
            if upsert_row and upsert_row.get("has_auto"):
                st.caption(
                    "Auto scores upserted into the lookup DB — ribbon will skip TCI next time."
                )

draft = st.session_state.get(SESSION_DRAFT)
if not draft or not draft.get("ok"):
    st.info("Enter a CAS and click **Generate draft** to load matrix dropdowns with auto prefills.")
    # Still show empty matrix form skeleton for dry-run visibility.
    with st.expander("Matrix form preview (no scores)", expanded=False):
        fields = p2oasys_form.build_form_fields(matrix)
        by_cat: dict[str, list] = {}
        for f in fields:
            by_cat.setdefault(f["category"], []).append(f)
        for cat, flist in by_cat.items():
            st.markdown(f"**{cat}** — {len(flist)} subcategories")
            if cat in p2oasys_form.MANUAL_ONLY_CATEGORIES:
                st.caption("Manual only — never auto-filled.")
    st.stop()

fields: list[dict[str, Any]] = draft["fields"]
suggestions = _suggestions_from_draft(draft)
selections_store: dict[str, Optional[int]] = dict(
    st.session_state.get(SESSION_SELECTIONS) or _default_selections(draft)
)

st.subheader(f"Draft for {draft.get('cas')} — {draft.get('chemical_name') or ''}")
if draft.get("pipeline_notes"):
    with st.expander("Pipeline notes"):
        for note in draft["pipeline_notes"]:
            st.caption(f"• {note}")

# Group fields by category (matrix order).
by_category: dict[str, list[dict[str, Any]]] = {}
for f in fields:
    by_category.setdefault(f["category"], []).append(f)

for category, cat_fields in by_category.items():
    manual = category in p2oasys_form.MANUAL_ONLY_CATEGORIES
    title = category + (" (manual — not auto-filled)" if manual else "")
    with st.expander(title, expanded=not manual):
        for field in cat_fields:
            sub = field["subcategory"]
            key = f"{category}||{sub}"
            sug = suggestions.get((category, sub)) or {}
            # Refresh options from the suggestion's preferred options_unit when available.
            options = field["options"]
            opt_unit = sug.get("options_unit") or field["primary_unit"]
            rules = field.get("rules") or {}
            if opt_unit in rules:
                options = p2oasys_form.build_score_options(rules[opt_unit], include_empty=True)

            current = selections_store.get(key)
            if key not in selections_store:
                current = sug.get("score") if (field.get("allow_auto") and sug.get("source") == "auto") else None
                selections_store[key] = current

            labels = [o["label"] for o in options]
            idx = p2oasys_form.option_index_for_score(options, current)
            gen = int(st.session_state.get("p2o_hitl_gen") or 0)
            chosen_label = st.selectbox(
                sub,
                options=labels,
                index=min(idx, len(labels) - 1),
                key=f"p2o_sel_{gen}_{category}_{sub}",
                help=f"Matrix unit for labels: {opt_unit}",
            )
            # Map label back to score value.
            chosen_opt = next((o for o in options if o["label"] == chosen_label), options[0])
            selections_store[key] = chosen_opt.get("score")

            if sug.get("source") == "auto" and field.get("allow_auto"):
                st.caption(sug.get("evidence") or f"Auto: {sug.get('score')}")
            else:
                st.caption("Not auto-filled — please select")

st.session_state[SESSION_SELECTIONS] = selections_store

# Convert store → typed selections for apply.
typed_selections: dict[tuple[str, str], Optional[int]] = {}
for k, v in selections_store.items():
    if "||" not in k:
        continue
    c, s = k.split("||", 1)
    typed_selections[(c, s)] = v

final_scores, applied_overrides = p2oasys_form.apply_form_selections(
    draft["scores"],
    typed_selections,
    suggestions,
    draft["matrix"],
)

_render_sidebar_summary(
    final_scores,
    draft["matrix"],
    cas=str(draft.get("cas") or ""),
    chemical_name=str(draft.get("chemical_name") or ""),
    matrix_kind=str(draft.get("matrix_kind") or ""),
)

st.markdown("### Rollup after overrides")
rollup_rows = p2oasys_form.category_score_rows(final_scores, draft["matrix"])
st.dataframe(
    pd.DataFrame([
        {
            "Category": r["category"],
            "Score": r["score"],
            "Status": r["status"],
            "Manual-only": r["manual_only"],
        }
        for r in rollup_rows
    ]),
    use_container_width=True,
    hide_index=True,
)

pkg = p2oasys_assessment.build_assessment_package(
    cas=str(draft.get("cas") or ""),
    chemical_name=draft.get("chemical_name"),
    hazard_data=draft.get("hazard_data") or {},
    scores=draft["scores"],
    trace=draft.get("trace") or {},
    matrix_path=Path(draft["matrix_path"]),
    matrix_kind=str(draft.get("matrix_kind") or "official"),
    overrides=[
        {
            "category": o["category"],
            "subcategory": o["subcategory"],
            "unit": o["unit"],
            "score": o["score"],
            "justification": o.get("justification") or p2oasys_form.JUSTIFICATION_FORM,
            "analyst": o.get("analyst") or "analyst",
        }
        for o in (
            p2oasys_form.build_overrides_from_selections(
                typed_selections, suggestions, draft["matrix"]
            )
        )
    ],
    pipeline_notes=draft.get("pipeline_notes"),
    sources_used=draft.get("sources_used"),
)

# Prefer package scores (apply_overrides inside build_assessment_package) for export;
# keep displayed rollup from apply_form_selections which collapses multi-unit subs.
st.caption(
    f"Overrides applied: {len(pkg.get('overrides') or [])}. "
    f"Matrix: {Path(draft['matrix_path']).name} ({draft.get('matrix_kind')})."
)

require_official = getattr(config, "P2OASYS_REQUIRE_OFFICIAL_MATRIX", True)
allow_ph = False
if draft.get("matrix_kind") != "official":
    allow_ph = st.checkbox(
        "Allow export with placeholder matrix (dev only)",
        value=False,
        key="p2o_hitl_allow_ph",
    )
can_export = p2oasys_assessment.assessment_is_exportable(
    str(draft.get("matrix_kind")),
    allow_placeholder=allow_ph if draft.get("matrix_kind") != "official" else True,
)
# When official required and kind official, always exportable.
if draft.get("matrix_kind") == "official":
    can_export = True
elif require_official and not allow_ph:
    can_export = False

c1, c2, c3 = st.columns(3)
cas_slug = str(draft.get("cas") or "cas").replace("-", "_")
with c1:
    st.download_button(
        "Download assessment JSON",
        data=json.dumps(pkg, indent=2, default=str),
        file_name=f"p2oasys_assessment_{cas_slug}.json",
        mime="application/json",
        disabled=not can_export,
        key="p2o_hitl_dl_json",
    )
with c2:
    st.download_button(
        "Download assessment HTML",
        data=p2oasys_assessment.assessment_to_html(pkg),
        file_name=f"p2oasys_assessment_{cas_slug}.html",
        mime="text/html",
        disabled=not can_export,
        key="p2o_hitl_dl_html",
    )
with c3:
    # Category CSV from displayed final_scores
    csv_df = pd.DataFrame([
        {"category": r["category"], "score": r["score"], "status": r["status"]}
        for r in rollup_rows
    ])
    st.download_button(
        "Download category scores CSV",
        data=csv_df.to_csv(index=False),
        file_name=f"p2oasys_scores_{cas_slug}.csv",
        mime="text/csv",
        disabled=not can_export,
        key="p2o_hitl_dl_csv",
    )

if applied_overrides:
    with st.expander(f"Override audit ({len(applied_overrides)})"):
        st.dataframe(pd.DataFrame(applied_overrides), use_container_width=True, hide_index=True)
