"""
Quick Hazard Assessment — Streamlit app.
Chemical hazard assessment from PubChem + DSSTox local (no API key required).
"""

from __future__ import annotations

import io
from typing import Any

import pandas as pd
import streamlit as st

import config
from utils import cas_validator, dsstox_local, ghs_formatter, pubchem_client, smiles_drawer

# Page config
st.set_page_config(page_title=config.APP_TITLE, layout="centered", initial_sidebar_state="collapsed")

# Session state: persist query and result to avoid re-fetching on every rerun
if "query" not in st.session_state:
    st.session_state["query"] = None
if "result_for" not in st.session_state:
    st.session_state["result_for"] = None
if "result_data" not in st.session_state:
    st.session_state["result_data"] = None  # { "pubchem": ..., "dtxsid": ..., "clean_cas": ... }
if "dsstox_loaded" not in st.session_state:
    st.session_state["dsstox_loaded"] = None  # True/False/None before first load
# GHS display preferences (persist during session)
if "show_h_phrases" not in st.session_state:
    st.session_state["show_h_phrases"] = True
if "show_p_phrases" not in st.session_state:
    st.session_state["show_p_phrases"] = True
if "show_signal_word" not in st.session_state:
    st.session_state["show_signal_word"] = True
if "ghs_layout" not in st.session_state:
    st.session_state["ghs_layout"] = "two_columns"

# Load DSSTox mapping once (cached)
dtxsid_map = dsstox_local.load_dsstox_mapping()
if st.session_state["dsstox_loaded"] is None:
    st.session_state["dsstox_loaded"] = dtxsid_map is not None

if dtxsid_map is None:
    st.info("DSSTox local database not found. Running in **PubChem-only** mode. DTXSID will not be shown.")
else:
    st.session_state["dsstox_loaded"] = True

# Title and description
st.title(f"🧪 {config.APP_TITLE}")
st.markdown(
    "Chemical hazard assessment from **PubChem** + **DSSTox local** (no API key required)."
)

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
            dtxsid = dsstox_local.get_dtxsid(clean_cas, dtxsid_map)
            # Resolve input type: CAS format vs name
            if cas_validator.is_valid_cas_format(clean_cas):
                input_type = "cas"
            else:
                input_type = "name"
            pubchem_data = pubchem_client.get_compound_data(clean_cas, input_type=input_type)
            st.session_state["result_for"] = clean_cas
            preferred_name = dsstox_local.get_preferred_name(clean_cas, dtxsid_map)
            st.session_state["result_data"] = {
                "pubchem": pubchem_data,
                "dtxsid": dtxsid,
                "preferred_name": preferred_name,
                "clean_cas": clean_cas,
            }

    result = st.session_state.get("result_data")
    if result and result.get("pubchem"):
        pubchem_data = result["pubchem"]
        dtxsid = result.get("dtxsid")
        preferred_name = result.get("preferred_name")
        clean_cas = result["clean_cas"]

        # --- Molecular structure at top ---
        if pubchem_data.get("smiles"):
            st.subheader("Molecular Structure")
            smiles_drawer.draw_smiles(pubchem_data["smiles"])

        # --- Identifiers and properties in columns ---
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Identifiers")
            st.write(f"**CAS:** {clean_cas}")
            st.write(f"**IUPAC Name:** {pubchem_data.get('iupac_name') or 'N/A'}")
            if preferred_name:
                st.write(f"**Preferred name (DSSTox):** {preferred_name}")
            if dtxsid:
                st.success(f"**DTXSID:** {dtxsid} *(from DSSTox local)*")
            else:
                if dtxsid_map is None:
                    st.warning(
                        "**DTXSID:** No local database loaded. "
                        "Place a CAS→DTXSID mapping file in the **DSS/** folder (see [DSS/README.md](https://github.com/glsalierno/quick-hazard-assessment-app/blob/main/DSS/README.md))."
                    )
                else:
                    st.info(
                        "**DTXSID:** This CAS was not found in the local DSSTox file. "
                        "Try an updated mapping from EPA Figshare or use PubChem-only data."
                    )
        with col2:
            st.subheader("Key Properties")
            st.write(f"**Molecular Formula:** {pubchem_data.get('formula') or 'N/A'}")
            st.write(f"**Molecular Weight:** {pubchem_data.get('mw') or 'N/A'}")
            # Flash point: one value per line (list or split by ";")
            fp = pubchem_data.get("flash_point")
            if isinstance(fp, list):
                fp_list = [str(x).strip() for x in fp if x]
            else:
                fp_list = [x.strip() for x in (str(fp or "").split(";")) if x.strip()]
            if fp_list:
                st.markdown("**Flash Point:**")
                for p in fp_list:
                    st.write(f"- {p}")
            else:
                st.write("**Flash Point:** N/A")
            # Vapor pressure: one value per line
            vp = pubchem_data.get("vapor_pressure")
            if isinstance(vp, list):
                vp_list = [str(x).strip() for x in vp if x]
            else:
                vp_list = [x.strip() for x in (str(vp or "").split(";")) if x.strip()]
            if vp_list:
                st.markdown("**Vapor Pressure:**")
                for p in vp_list:
                    st.write(f"- {p}")
            else:
                st.write("**Vapor Pressure:** N/A")

        # --- Endpoints of interest (LD50, LC50 from PubChem) ---
        ld50_list = pubchem_data.get("ld50") or []
        lc50_list = pubchem_data.get("lc50") or []
        if ld50_list or lc50_list:
            st.subheader("📌 Endpoints of interest")
            if ld50_list:
                st.markdown("**LD50 (PubChem):**")
                for v in ld50_list[:10]:
                    st.write(f"- {v}")
                if len(ld50_list) > 10:
                    st.caption(f"*… and {len(ld50_list) - 10} more*")
            if lc50_list:
                st.markdown("**LC50 (PubChem):**")
                for v in lc50_list[:10]:
                    st.write(f"- {v}")
                if len(lc50_list) > 10:
                    st.caption(f"*… and {len(lc50_list) - 10} more*")

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
        st.caption(f"*{config.OPERA_NOTE}*")

        # --- Download report as CSV ---
        def _report_row() -> dict[str, Any]:
            fp = pubchem_data.get("flash_point")
            vp = pubchem_data.get("vapor_pressure")
            fp_str = "; ".join(fp) if isinstance(fp, list) else (fp or "")
            vp_str = "; ".join(vp) if isinstance(vp, list) else (vp or "")
            return {
                "CAS": clean_cas,
                "DTXSID": dtxsid or "",
                "Preferred Name": preferred_name or "",
                "IUPAC Name": pubchem_data.get("iupac_name") or "",
                "Molecular Formula": pubchem_data.get("formula") or "",
                "Molecular Weight": pubchem_data.get("mw") or "",
                "Flash Point": fp_str,
                "Vapor Pressure": vp_str,
                "GHS H-codes": " | ".join(h_codes),
                "GHS P-codes": " | ".join(p_codes),
            }

        df_report = pd.DataFrame([_report_row()])
        buf = io.BytesIO()
        df_report.to_csv(buf, index=False)
        buf.seek(0)
        st.download_button(
            "Download report as CSV",
            data=buf,
            file_name=f"hazard_report_{clean_cas.replace('-', '_')}.csv",
            mime="text/csv",
            key="download_csv",
        )

        # --- Expandable raw data ---
        with st.expander("View raw PubChem data"):
            st.json(pubchem_data)
    else:
        st.error(f"No data found for '{current_query}'. Please check the input.")

# Footer when no query yet
if not current_query:
    st.markdown("---")
    st.caption(f"*{config.OPERA_NOTE}*")
