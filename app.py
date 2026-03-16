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

# Load enhanced DSSTox data once (cached inside utils)
dsstox_data = dsstox_local.load_dsstox_enhanced()

# Title and description
st.title(f"🧪 {config.APP_TITLE}")
st.markdown(
    "Chemical hazard assessment from **PubChem** + **DSSTox local** (no API key required)."
)

# Sidebar: DSSTox database stats (if loaded)
with st.sidebar:
    st.header("📊 DSSTox database")
    if dsstox_data:
        stats = dsstox_local.get_dsstox_summary_stats(dsstox_data)
        st.success(f"Loaded {stats.get('total_compounds', 0)} compounds")
        st.caption(
            f"{stats.get('with_dtxsid', 0)} with DTXSID, "
            f"{stats.get('with_preferred_name', 0)} with names, "
            f"{stats.get('with_formula', 0)} with formulas"
        )
    else:
        st.warning("DSSTox local database not loaded (PubChem-only mode).")

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
            # DSSTox local (enhanced)
            dsstox_info = dsstox_local.get_dsstox_info(clean_cas, dsstox_data) if dsstox_data else None
            dtxsid = dsstox_info.get("dtxsid") if dsstox_info else None
            preferred_name = dsstox_info.get("preferred_name") if dsstox_info else None

            # Resolve input type: CAS format vs name
            if cas_validator.is_valid_cas_format(clean_cas):
                input_type = "cas"
            else:
                input_type = "name"
            pubchem_data = pubchem_client.get_compound_data(clean_cas, input_type=input_type)
            st.session_state["result_for"] = clean_cas
            st.session_state["result_data"] = {
                "pubchem": pubchem_data,
                "dsstox_info": dsstox_info,
                "dtxsid": dtxsid,
                "preferred_name": preferred_name,
                "clean_cas": clean_cas,
            }

    result = st.session_state.get("result_data")
    if result and result.get("pubchem"):
        pubchem_data = result["pubchem"]
        dsstox_info = result.get("dsstox_info")
        dtxsid = result.get("dtxsid")
        preferred_name = result.get("preferred_name")
        clean_cas = result["clean_cas"]

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

        # --- Toxic doses (route, species, value, unit) ---
        toxicities = pubchem_data.get("toxicities") or []
        if toxicities:
            st.subheader("📌 Toxic doses & toxicity endpoints")
            st.caption("Exposure pathway and species are inferred from PubChem text where available.")
            rows = []
            for t in toxicities[:30]:
                route = t.get("route") or "—"
                species = t.get("species") or "—"
                endpoint = (t.get("type") or "Toxicity").strip()
                value = (t.get("value") or "")[:180]
                unit = t.get("unit") or "—"
                rows.append({"Exposure pathway": route, "Species": species, "Endpoint": endpoint, "Value": value, "Unit": unit})
            if rows:
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            if len(toxicities) > 30:
                st.caption(f"*Showing 30 of {len(toxicities)} entries. Full list in raw data.*")

        # --- Ecotoxicity (aquatic LC50/EC50, species, H4xx) ---
        eco = pubchem_data.get("ecotoxicity") or {}
        eco_entries = eco.get("entries") or []
        h_aquatic = eco.get("h_codes_aquatic") or []
        if eco_entries or h_aquatic:
            st.subheader("🐟 Ecotoxicity")
            if h_aquatic:
                st.markdown("**Aquatic hazard (GHS):** " + ", ".join(h_aquatic))
            if eco_entries:
                st.markdown("**Aquatic toxicity (PubChem):**")
                # Group entries by species so each species is on one line
                by_species: dict[str, list[dict]] = {}
                for e in eco_entries:
                    sp = (e.get("species") or "—").strip()
                    by_species.setdefault(sp, []).append(e)
                for sp, entries in by_species.items():
                    parts = []
                    for e in entries:
                        val = e.get("value") or ""
                        u = e.get("unit") or ""
                        parts.append(val + (f" ({u})" if u else ""))
                    joined = " | ".join(parts)
                    st.write(f"- **Species:** {sp} — {joined}")
            lc = eco.get("aquatic_lc50_mg_l")
            ec = eco.get("aquatic_ec50_mg_l")
            if lc is not None:
                st.write(f"**LC50 (mg/L):** {lc}")
            if ec is not None:
                st.write(f"**EC50 (mg/L):** {ec}")

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
            eco = pubchem_data.get("ecotoxicity") or {}
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
                "Aquatic H-codes": " | ".join(eco.get("h_codes_aquatic") or []),
                "Aquatic LC50 (mg/L)": eco.get("aquatic_lc50_mg_l") or "",
                "Aquatic EC50 (mg/L)": eco.get("aquatic_ec50_mg_l") or "",
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
