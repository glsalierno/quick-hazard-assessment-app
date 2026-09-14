"""
CAS Database Builder — expand HSPiP / SDS caches from a CAS list.

Workflow (automatic expansion):
  CAS list
    → PubChem SMILES → HSPiP CLI → ``data/hsp_by_cas.csv``
    → TCI map / live search → SDS cache (+ learn product map)
    → optional Sigma SDS fallback
    → report P2OASys / CHEM21 / DoSS coverage (lookup only)

HSPiP path is required for Hansen expansion:
https://github.com/glsalierno/cas-to-HSPiP_data

Run::

    streamlit run pages/04_CAS_Database_Builder.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="CAS Database Builder", layout="wide")

from v7.cas_db_builder import build_from_cas_list
from v7.hspip_expand import (
    bootstrap_hsp_from_doss,
    ensure_writable_hspip_dir,
    resolve_hspip_dir,
)
from v7.settings_store import get_hspip_install_dir, set_hspip_install_dir

st.title("CAS Database Builder")
st.markdown(
    "Build / expand local databases **from a list of CAS numbers**. "
    "Hansen parameters use the [cas-to-HSPiP_data](https://github.com/glsalierno/cas-to-HSPiP_data) "
    "workflow (PubChem SMILES → licensed HSPiP CLI). "
    "TCI SDS is attempted automatically from CAS (map → live search); "
    "TCI often blocks catalog search (Akamai) — product SDS by number still works, "
    "and successful hits grow the learned map."
)

st.subheader("1 · HSPiP location")
st.caption(
    "Point to the folder that contains **HSPiP.exe** (CLI license required). "
    "On this PC the install is typically "
    "`C:\\Program Files\\Hansen-Solubility-6\\HSPiP`. "
    "Program Files is read-only — use **Prepare writable workdir** so CLI can write Out.dat. "
    "Until HSPiP CLI works, Hansen can be bootstrapped from DoSS."
)
current = get_hspip_install_dir() or ""
hspip_in = st.text_input(
    "HSPiP install directory",
    value=current,
    placeholder=r"C:\Program Files\Hansen-Solubility-6\HSPiP",
    key="builder_hspip_dir",
)
c_save, c_check, c_work, c_boot = st.columns(4)
with c_save:
    if st.button("Save HSPiP path", key="builder_save_hspip"):
        try:
            saved = set_hspip_install_dir(hspip_in)
            st.success(f"Saved: {saved}")
        except ValueError as exc:
            st.error(str(exc))
with c_check:
    if st.button("Validate HSPiP path", key="builder_check_hspip"):
        try:
            path = resolve_hspip_dir(hspip_in or None)
            st.success(f"Found HSPiP.exe in {path}")
        except FileNotFoundError as exc:
            st.error(str(exc))
with c_work:
    if st.button("Prepare writable workdir", key="builder_hspip_workdir"):
        try:
            work = ensure_writable_hspip_dir(hspip_in or None)
            set_hspip_install_dir(str(work))
            st.success(f"CLI will use writable copy: {work}")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
with c_boot:
    if st.button("Bootstrap Hansen from DoSS", key="builder_boot_doss"):
        try:
            n = bootstrap_hsp_from_doss()
            st.success(f"Hansen cache now has {n} rows (DoSS bootstrap).")
        except Exception as exc:
            st.error(f"DoSS bootstrap failed: {exc}")

st.subheader("2 · CAS list")
starter = Path("data/starter_cas_list.txt")
c_load, _ = st.columns([1, 3])
with c_load:
    if starter.is_file() and st.button("Load starter CAS list", key="builder_load_starter"):
        st.session_state["builder_cas_text"] = starter.read_text(encoding="utf-8")
cas_text = st.text_area(
    "One CAS per line (commas allowed)",
    height=160,
    placeholder="67-56-1\n78-33-1\n141-78-6",
    key="builder_cas_text",
)
uploaded = st.file_uploader("Or upload a text/CSV with CAS column / one CAS per line", type=["txt", "csv"])
if uploaded is not None:
    raw = uploaded.getvalue().decode("utf-8", "replace")
    if uploaded.name.lower().endswith(".csv") and "cas" in raw.lower().split("\n", 1)[0].lower():
        try:
            df_u = pd.read_csv(uploaded)
            col = next((c for c in df_u.columns if str(c).strip().lower() in {"cas", "casrn", "cas_number"}), None)
            if col:
                cas_text = "\n".join(str(x) for x in df_u[col].dropna().astype(str))
                st.info(f"Loaded {df_u[col].notna().sum()} CAS from column `{col}`.")
        except Exception:
            cas_text = raw
    else:
        cas_text = raw

st.subheader("3 · What to expand")
do_hsp = st.checkbox("Expand HSPiP Hansen cache (D/P/H)", value=True)
skip_hsp = st.checkbox("Skip CAS already in HSPiP cache", value=True)
sds_mode = st.selectbox(
    "SDS acquisition",
    options=[
        ("tci_then_sigma", "TCI then Sigma fallback"),
        ("tci", "TCI only"),
        ("sigma", "Sigma only"),
        ("none", "Skip SDS"),
    ],
    format_func=lambda x: x[1],
    index=0,
)
tci_live = st.checkbox(
    "Enable TCI live CAS→product search",
    value=True,
    help="Often HTTP 403 from campus/server networks. Map + learned rows still apply.",
)

if st.button("Build / expand databases", type="primary", key="builder_go"):
    if not (cas_text or "").strip():
        st.error("Enter at least one CAS.")
    else:
        status = st.empty()
        prefer = sds_mode[0]

        def _prog(msg: str) -> None:
            status.caption(msg)

        with st.spinner("Expanding databases…"):
            summary = build_from_cas_list(
                cas_text,
                expand_hsp=do_hsp,
                expand_sds=prefer,  # type: ignore[arg-type]
                hspip_dir=hspip_in or None,
                skip_existing_hsp=skip_hsp,
                enable_tci_live_search=tci_live,
                progress=_prog,
            )

        for note in summary.notes:
            st.info(note)

        rows_out = []
        for r in summary.rows:
            h = r.hsp
            rows_out.append(
                {
                    "CAS": r.cas,
                    "HSPiP": h.status if h else ("skipped" if not do_hsp else "—"),
                    "δD": h.delta_d if h else None,
                    "δP": h.delta_p if h else None,
                    "δH": h.delta_h if h else None,
                    "HSPiP msg": h.message if h else "",
                    "SDS": r.sds_status,
                    "SDS vendor": r.sds_vendor,
                    "SDS product": r.sds_product,
                    "SDS msg": r.sds_message[:200],
                    "P2OASys": r.p2oasys_status,
                    "P2 score": r.p2oasys_score,
                    "CHEM21": r.chem21_status,
                    "HSP ref": r.doss_hsp_status,
                }
            )
        df = pd.DataFrame(rows_out)
        st.dataframe(df, width="stretch", hide_index=True)
        st.download_button(
            "Download build report (CSV)",
            data=df.to_csv(index=False),
            file_name="cas_db_build_report.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download build report (JSON)",
            data=json.dumps(
                {
                    "notes": summary.notes,
                    "hsp_cache_path": summary.hsp_cache_path,
                    "rows": rows_out,
                },
                indent=2,
                default=str,
            ),
            file_name="cas_db_build_report.json",
            mime="application/json",
        )
        if summary.hsp_cache_path:
            st.caption(f"HSPiP cache file: `{summary.hsp_cache_path}`")
            p = Path(summary.hsp_cache_path)
            if p.is_file():
                st.caption(f"Cache size: {p.stat().st_size} bytes")

st.divider()
st.markdown(
    "**Notes**\n"
    "- Expert P2OASys scores are **looked up**, never predicted.\n"
    "- TCI has **no public CAS API**; live HTML search is frequently blocked. "
    "When a product is resolved once, it is stored in `tci_catalog_learned.csv`.\n"
    "- HSPiP requires a commercial CLI license "
    "([cas-to-HSPiP_data](https://github.com/glsalierno/cas-to-HSPiP_data))."
)
