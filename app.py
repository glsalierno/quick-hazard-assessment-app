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
            st.session_state["result_data"] = {
                "pubchem": pubchem_data,
                "dtxsid": dtxsid,
                "clean_cas": clean_cas,
            }

    result = st.session_state.get("result_data")
    if result and result.get("pubchem"):
        pubchem_data = result["pubchem"]
        dtxsid = result.get("dtxsid")
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
            if dtxsid:
                st.success(f"**DTXSID:** {dtxsid} *(from DSSTox local)*")
            else:
                st.info("DTXSID not found in local database.")
        with col2:
            st.subheader("Key Properties")
            st.write(f"**Molecular Formula:** {pubchem_data.get('formula') or 'N/A'}")
            st.write(f"**Molecular Weight:** {pubchem_data.get('mw') or 'N/A'}")
            st.write(f"**Flash Point:** {pubchem_data.get('flash_point') or 'N/A'}")
            st.write(f"**Vapor Pressure:** {pubchem_data.get('vapor_pressure') or 'N/A'}")

        # --- GHS Classification ---
        st.subheader("⚠️ GHS Classification")
        ghs = pubchem_data.get("ghs") or {}
        h_codes = ghs.get("h_codes") or []
        p_codes = ghs.get("p_codes") or []
        if h_codes or p_codes:
            h_phrases = ghs_formatter.expand_h_codes_with_phrases(h_codes)
            p_phrases = ghs_formatter.expand_p_codes_with_phrases(p_codes)
            for line in h_phrases:
                st.write(f"**{line.split(':')[0]}:** {line.split(':', 1)[-1].strip()}")
            for line in p_phrases:
                st.write(f"**{line.split(':')[0]}:** {line.split(':', 1)[-1].strip()}")
            if ghs.get("signal_word"):
                st.caption(f"Signal word: {ghs['signal_word']}")
        else:
            st.write("No GHS data available from PubChem.")

        # --- Citation ---
        st.markdown("---")
        st.caption(
            f"📝 **For research use:** If this tool contributes to your work, "
            f"please cite the Zenodo DOI: {config.ZENODO_DOI}"
        )
        st.caption(f"*{config.OPERA_NOTE}*")

        # --- Download report as CSV ---
        def _report_row() -> dict[str, Any]:
            row = {
                "CAS": clean_cas,
                "DTXSID": dtxsid or "",
                "IUPAC Name": pubchem_data.get("iupac_name") or "",
                "Molecular Formula": pubchem_data.get("formula") or "",
                "Molecular Weight": pubchem_data.get("mw") or "",
                "Flash Point": pubchem_data.get("flash_point") or "",
                "Vapor Pressure": pubchem_data.get("vapor_pressure") or "",
                "GHS H-codes": " | ".join(h_codes),
                "GHS P-codes": " | ".join(p_codes),
            }
            return row

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
