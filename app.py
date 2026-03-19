"""
Quick Hazard Assessment — Streamlit app.
Chemical hazard assessment from PubChem + DSSTox local (no API key required).
"""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd
import streamlit as st

import config
from utils import cas_validator, chemical_db, data_formatter, dsstox_local, ghs_formatter, pubchem_client, smiles_drawer
from utils import hazard_for_p2oasys, p2oasys_aggregate, p2oasys_scorer
from utils import sds_compare, sds_pdf_utils, sds_regex_extractor
from utils import summary_utils, toxvaldb_client

try:
    from utils import carcinogenic_potency_client
except ImportError:
    carcinogenic_potency_client = None  # optional: not present in some deployments

# Page config
st.set_page_config(page_title=config.APP_TITLE, layout="centered", initial_sidebar_state="collapsed")

# Session state: persist query and result to avoid re-fetching on every rerun
if "query" not in st.session_state:
    st.session_state["query"] = None
if "result_for" not in st.session_state:
    st.session_state["result_for"] = None
if "result_data" not in st.session_state:
    st.session_state["result_data"] = None  # { "pubchem": ..., "dsstox_info": ..., "clean_cas": ... }

# SDS comparison (Phase 1 regex extraction)
if "sds_result" not in st.session_state:
    st.session_state["sds_result"] = None
if "sds_compare_cas" not in st.session_state:
    st.session_state["sds_compare_cas"] = None
if "sds_comparison" not in st.session_state:
    st.session_state["sds_comparison"] = None
# GHS display preferences (persist during session)
if "show_h_phrases" not in st.session_state:
    st.session_state["show_h_phrases"] = True
if "show_p_phrases" not in st.session_state:
    st.session_state["show_p_phrases"] = True
if "show_signal_word" not in st.session_state:
    st.session_state["show_signal_word"] = True
if "ghs_layout" not in st.session_state:
    st.session_state["ghs_layout"] = "two_columns"

# Prefer SQLite chemical DB when present (fast); else fall back to CSV-based DSSTox
db_stats = chemical_db.get_db_stats()
use_sqlite_dsstox = db_stats.get("dsstox", {}).get("exists", False)
use_sqlite_toxval = db_stats.get("toxvaldb", {}).get("exists", False)
dsstox_data = None if use_sqlite_dsstox else dsstox_local.load_dsstox_enhanced()

# Title and description
st.title(f"🧪 {config.APP_TITLE}")
st.markdown(
    "Chemical hazard assessment from **PubChem** + **DSSTox local** (no API key required)."
)

# Sidebar: database stats (SQLite or CSV)
with st.sidebar:
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

# Input form
with st.form("cas_input"):
    cas_label = "Enter CAS number or chemical name:"
    # Prefill from session state if we have a previous query
    default = st.session_state.get("query") or ""
    cas = st.text_input(cas_label, value=default, placeholder="e.g., 67-64-1 or acetone")
    col1, col2 = st.columns([1, 5])
    with col1:
        submitted = st.form_submit_button("Assess")

# Example buttons (outside form — use session state to set query and rerun)
st.markdown("**Examples:**")
example_cols = st.columns(4)
for i, (example_cas, label) in enumerate(config.EXAMPLE_CHEMICALS):
    if example_cols[i].button(label, key=f"ex_{i}"):
        st.session_state["query"] = example_cas
        st.session_state["result_for"] = None  # force re-fetch
        st.rerun()

# When form is submitted, set query to what the user typed
if submitted and cas:
    clean_cas = cas_validator.normalize_cas_input(cas)
    if clean_cas:
        st.session_state["query"] = clean_cas
        st.session_state["result_for"] = None
    st.rerun()

# Run assessment when we have a query and either no cached result or result is for a different query
current_query = st.session_state.get("query")
if current_query:
    clean_cas = cas_validator.normalize_cas_input(current_query)
    need_fetch = st.session_state.get("result_for") != clean_cas
    if need_fetch:
        with st.spinner("Fetching data and generating structure..."):
            # DSSTox: SQLite (fast) or CSV
            if use_sqlite_dsstox:
                dsstox_info = chemical_db.get_dsstox_by_cas(clean_cas)
            else:
                dsstox_info = dsstox_local.get_dsstox_info(clean_cas, dsstox_data) if dsstox_data else None
            dtxsid = (dsstox_info or {}).get("dtxsid")
            preferred_name = (dsstox_info or {}).get("preferred_name")

            # PubChem
            if cas_validator.is_valid_cas_format(clean_cas):
                input_type = "cas"
            else:
                input_type = "name"
            pubchem_data = pubchem_client.get_compound_data(clean_cas, input_type=input_type)

            # ToxValDB: SQLite (local) or API
            toxval_data = None
            if dtxsid and use_sqlite_toxval:
                recs = chemical_db.get_toxicity_by_dtxsid(dtxsid, numeric_only=False)
                toxval_data = {}
                for rec in recs:
                    cat = (rec.get("study_type") or "other").strip() or "other"
                    toxval_data.setdefault(cat, []).append({
                        "value": rec.get("toxval_numeric"),
                        "units": rec.get("toxval_units", ""),
                        "species": rec.get("species", ""),
                        "route": rec.get("exposure_route", ""),
                        "study_type": rec.get("study_type", ""),
                    })
            elif dtxsid:
                try:
                    api_key = st.secrets.get("COMPTOX_API_KEY") if hasattr(st, "secrets") else None
                    if not api_key:
                        import os
                        api_key = os.environ.get("COMPTOX_API_KEY")
                    if api_key:
                        toxval_data = toxvaldb_client.fetch_toxval_data(dtxsid, api_key)
                except Exception:
                    toxval_data = None

            carc_potency_data = carcinogenic_potency_client.get_data_by_cas(clean_cas) if (carcinogenic_potency_client and carcinogenic_potency_client.is_available()) else None

            st.session_state["result_for"] = clean_cas
            st.session_state["result_data"] = {
                "pubchem": pubchem_data,
                "dsstox_info": dsstox_info,
                "dtxsid": dtxsid,
                "preferred_name": preferred_name,
                "clean_cas": clean_cas,
                "toxval_data": toxval_data,
                "carc_potency_data": carc_potency_data,
            }

    result = st.session_state.get("result_data")
    tab_haz, tab_p2o = st.tabs(["Hazard assessment", "P2OASys scoring"])
    with tab_haz:
        if result and result.get("pubchem"):
            pubchem_data = result["pubchem"]
            dsstox_info = result.get("dsstox_info")
            dtxsid = result.get("dtxsid")
            preferred_name = result.get("preferred_name")
            clean_cas = result["clean_cas"]
            toxval_data = result.get("toxval_data")
            carc_potency_data = result.get("carc_potency_data")
    
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
                st.write(f"**IUPAC Name:** {pubchem_data.get('iupac_name') or 'N/A'}")
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
                    {"Property": "Molecular Formula", "Value": pubchem_data.get("formula") or "—", "Unit": "—", "Observations": ""},
                    {"Property": "Molecular Weight", "Value": pubchem_data.get("mw") or "—", "Unit": "g/mol", "Observations": ""},
                    {"Property": "Flash Point", "Value": " | ".join(fp_list) if fp_list else "—", "Unit": "°C (typical)", "Observations": "Multiple values" if len(fp_list) > 1 else ""},
                    {"Property": "Vapor Pressure", "Value": " | ".join(vp_list) if vp_list else "—", "Unit": "mmHg (typical)", "Observations": "Multiple values" if len(vp_list) > 1 else ""},
                ]
                st.dataframe(pd.DataFrame(prop_rows), width="stretch", hide_index=True)
    
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
                    st.dataframe(df_pri, width="stretch", hide_index=True, height=400)
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
                    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, height=400)
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
                        st.dataframe(pd.DataFrame(quant_rows), width="stretch", hide_index=True)
                    if text_rows:
                        st.markdown("**Aquatic toxicity – text-only PubChem excerpts**")
                        st.dataframe(pd.DataFrame(text_rows), width="stretch", hide_index=True)
                        # Optional: summarize excerpts with mini-LLM if API key is set
                        api_key = (st.secrets.get("OPENAI_API_KEY") or "").strip() if hasattr(st, "secrets") else ""
                        if api_key:
                            if st.button("Summarize excerpts with AI", key="summarize_eco_text"):
                                combined = " ".join((r.get("Value / excerpt") or "") for r in text_rows)[:3000]
                                with st.spinner("Summarizing…"):
                                    summary = summary_utils.summarize_text_with_llm(combined, api_key)
                                if summary:
                                    st.caption("**AI summary:** " + summary)
                                else:
                                    st.caption("Summary unavailable (check API key or try again).")
                        else:
                            st.caption("Add `OPENAI_API_KEY` in app secrets (Manage app → Secrets) to enable **Summarize excerpts with AI** (gpt-4o-mini).")
    
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
                    api_key = (st.secrets.get("OPENAI_API_KEY") or "").strip() if hasattr(st, "secrets") else ""
                    if api_key and st.button("One-sentence AI summary", key="summarize_cpdb_ai"):
                        with st.spinner("Summarizing…"):
                            one_liner = summary_utils.summarize_cpdb_with_llm(summary_paragraph, api_key)
                        if one_liner:
                            st.caption("**AI:** " + one_liner)
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
                    st.dataframe(pd.DataFrame(exp_rows), width="stretch", hide_index=True, height=300)
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
                        st.dataframe(pd.DataFrame(dose_rows), width="stretch", hide_index=True, height=250)
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
                                    st.write(f"**{code}:** {phrase}")
                            else:
                                st.write("*No hazard statements found*")
                        else:
                            st.write("*Hidden*")
                    with col_right:
                        st.markdown("**Precautionary Statements**")
                        if st.session_state["show_p_phrases"]:
                            if p_phrases_dict:
                                for code, phrase in p_phrases_dict.items():
                                    st.write(f"**{code}:** {phrase}")
                            else:
                                st.write("*No precautionary statements found*")
                        else:
                            st.write("*Hidden*")
                else:
                    if st.session_state["show_h_phrases"]:
                        st.markdown("**Hazard Statements**")
                        if h_phrases_dict:
                            for code, phrase in h_phrases_dict.items():
                                st.write(f"**{code}:** {phrase}")
                        else:
                            st.write("*No hazard statements found*")
                        st.write("")
                    if st.session_state["show_p_phrases"]:
                        st.markdown("**Precautionary Statements**")
                        if p_phrases_dict:
                            for code, phrase in p_phrases_dict.items():
                                st.write(f"**{code}:** {phrase}")
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
    
            with st.expander("📚 Data sources"):
                st.markdown("""
                **Data sources**
                - **PubChem**: identifiers, properties, GHS, toxicity text from PUG View.
                - **DSSTox (local)**: DTXSID, preferred/systematic names, formula, InChI/SMILES when present in your mapping file.
                - **ToxValDB (local)**: quantitative toxicity values loaded from the local COMPTOX Excel files into the SQLite database (no API key required).
                - **Carcinogenic Potency Database (local)**: TD50 and experiment data from the CPDB SQLite (built from CPDB tab files via `scripts/build_carcinogenic_potency_from_cpdb_tabs.py`).
                """)
        else:
            st.error(f"No data found for '{current_query}'. Please check the input.")

    with tab_p2o:
        if result and result.get("pubchem"):
            pubchem_data = result["pubchem"]
            clean_cas = result["clean_cas"]
            matrix_path = p2oasys_scorer.DEFAULT_MATRIX_PATH
            if not matrix_path.exists():
                st.info(
                    "P2OASys matrix file not found. Place **"
                    + config.P2OASYS_MATRIX_FILENAME
                    + "** in the `data/` folder (see [TURI P2OASys](https://p2oasys.turi.org/chemical/hazard-score-matrix))."
                )
                st.caption("Path checked: `" + str(matrix_path) + "`")
            else:
                with st.spinner("Computing P2OASys scores…"):
                    hazard_data = hazard_for_p2oasys.pubchem_to_hazard_data(pubchem_data)
                    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
                    scores = p2oasys_scorer.compute_p2oasys_scores(hazard_data, matrix)
                overall_max = p2oasys_aggregate.aggregate_category_scores(scores, "max")
                overall_mean = p2oasys_aggregate.aggregate_category_scores(scores, "mean")
                overall_weighted = p2oasys_aggregate.aggregate_category_scores(scores, "weighted_mean")
                n_cat, cat_names = p2oasys_aggregate.count_scored_categories(scores)
                st.subheader("P2OASys itemized scoring")
                st.caption(
                    "Scores 2–10 (higher = more hazardous). From TURI P2OASys matrix; data from PubChem."
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

# --- SDS PDF comparison (Phase 1) ---
st.markdown("---")
st.subheader("📄 SDS PDF comparison (Phase 1)")
with st.expander("Upload SDS PDF and compare extracted fields to v1.3", expanded=False):
    uploaded = st.file_uploader("Upload Safety Data Sheet (PDF)", type=["pdf"])

    if uploaded is not None:
        if st.button("Extract from SDS (regex) + compare", key="sds_extract_compare_btn"):
            with st.spinner("Extracting text from PDF…"):
                pdf_bytes = uploaded.getvalue()
                raw_text = sds_pdf_utils.extract_text_from_pdf_bytes(pdf_bytes)
                raw_text = sds_pdf_utils.normalize_whitespace(raw_text)

            if not raw_text.strip():
                st.warning(
                    "No readable text was found. For scanned PDFs, install Tesseract and Poppler and ensure "
                    "`pdf2image` and `pytesseract` are installed — see [OCR setup](docs/OCR_SETUP.md)."
                )
                st.session_state["sds_result"] = None
                st.session_state["sds_compare_cas"] = None
                st.session_state["sds_comparison"] = None
            else:
                with st.spinner("Extracting structured SDS fields…"):
                    sds_result = sds_regex_extractor.extract_sds_fields_from_text(raw_text)
                st.session_state["sds_result"] = sds_result
                st.session_state["sds_compare_cas"] = None
                st.session_state["sds_comparison"] = None

    sds_result = st.session_state.get("sds_result")
    if sds_result:
        st.markdown("### Extracted from SDS (meaningful-only)")
        st.json(sds_result)

        cas_numbers = sds_result.get("cas_numbers") or []
        if cas_numbers:
            cas_options = [c for c in cas_numbers if cas_validator.is_valid_cas_format(c)] or list(cas_numbers)

            selected_cas = st.selectbox(
                "CAS used for PubChem comparison",
                options=cas_options,
                index=0,
                key="sds_compare_cas_select",
            )

            if st.session_state.get("sds_compare_cas") != selected_cas:
                with st.spinner("Fetching PubChem data for comparison…"):
                    pubchem_data = pubchem_client.get_compound_data(selected_cas, input_type="cas")

                if not pubchem_data:
                    st.error("PubChem lookup failed for the selected CAS.")
                    st.session_state["sds_compare_cas"] = selected_cas
                    st.session_state["sds_comparison"] = None
                else:
                    with st.spinner("Comparing SDS vs v1.3 (GHS + quantitative)…"):
                        comparison = sds_compare.compare_sds_to_pubchem(sds_result, pubchem_data)
                    st.session_state["sds_compare_cas"] = selected_cas
                    st.session_state["sds_comparison"] = comparison

        if st.session_state.get("sds_comparison"):
            st.markdown("### SDS vs v1.3 comparison (GHS + quantitative)")
            st.json(st.session_state["sds_comparison"])
        else:
            st.info("Upload an SDS PDF, extract fields, and pick a CAS to show the comparison.")
    else:
        st.caption("Upload a PDF to extract GHS H/P codes and quantitative fields from Section text (Phase 1).")

# Footer when no query yet
if not current_query:
    st.markdown("---")
