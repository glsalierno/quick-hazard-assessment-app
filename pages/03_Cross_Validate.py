"""
Multipage Streamlit: cross-validate OPERA predictions vs PubChem, ToxVal (local/API), and ECHA HTML.

Run from repo root::

    streamlit run app.py

Set ``EPA_API_KEY`` or ``COMPTOX_API_KEY`` (or add to ``.streamlit/secrets.toml``) for remote EPA data.
Local ``data/chemical_db.sqlite`` ToxValDB is used automatically when present.
"""

from __future__ import annotations

import io
import os
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="OPERA cross-validation", layout="wide")


def _sync_secrets_to_environ() -> None:
    from utils.streamlit_secrets import get_secret

    for key in ("EPA_API_KEY", "COMPTOX_API_KEY"):
        v = get_secret(key)
        if v and not (os.environ.get(key) or "").strip():
            os.environ[key] = v


_sync_secrets_to_environ()

import config
from utils import opera_batch
from utils.validate import cross_validate_cas_list, load_cas_list_from_p2oasys_csv

if "cv_cas_text" not in st.session_state:
    st.session_state["cv_cas_text"] = "67-64-1\n50-78-2\n71-43-2"

st.title("OPERA cross-validation")
st.caption(
    "Compare one OPERA endpoint per row against PubChem (PUG REST / PUG View), "
    "EPA ToxVal (local SQLite and/or API), and a lightweight ECHA search scrape. "
    "ECHA markup changes frequently — treat scraped rows as indicative only."
)

epa_ok = bool(
    (os.environ.get("EPA_API_KEY") or os.environ.get("COMPTOX_API_KEY") or "").strip()
    or getattr(config, "EPA_API_KEY", None)
)
if not epa_ok:
    st.warning(
        "No **EPA_API_KEY** / **COMPTOX_API_KEY** in environment or Streamlit secrets — "
        "remote EPA ToxVal API calls will be skipped (local SQLite ToxValDB still works if built)."
    )

endpoints = opera_batch.get_all_opera_endpoints()
if not endpoints:
    st.error(
        "No ``data/opera_endpoints.json`` found. Run: "
        "`python scripts/discover_opera_endpoints.py --offline --out data/opera_endpoints.json`"
    )
    st.stop()

pred_cols = [e for e in endpoints if "_pred" in e.get("name", "") or e.get("name", "").startswith("CATMoS")]
choices = pred_cols if pred_cols else endpoints
labels = [f"{c['name']} — {c['label']}"[:120] for c in choices]
name_by_label = dict(zip(labels, [c["name"] for c in choices]))

st.text_area("CAS numbers (one per line)", key="cv_cas_text", height=160)

csv_path = getattr(config, "P2OASYS_COMPOUND_LIST_CSV", None) or ""
if csv_path:
    if st.button("Load CAS from P2OASYS_COMPOUND_LIST_CSV"):
        loaded = load_cas_list_from_p2oasys_csv(csv_path, limit=200)
        if loaded:
            st.session_state["cv_cas_text"] = "\n".join(loaded)
            st.rerun()
        else:
            st.warning(f"No CAS extracted from `{csv_path}`.")

endpoint_label = st.selectbox("OPERA endpoint", options=labels, index=0)
endpoint = name_by_label[endpoint_label]

use_streamlit_cache = st.checkbox("Cache identical runs in Streamlit for 1 hour", value=False)

if st.button("Run cross-validation", type="primary"):
    cas_list = [ln.strip() for ln in str(st.session_state.get("cv_cas_text", "")).splitlines() if ln.strip()]
    if not cas_list:
        st.warning("Enter at least one CAS.")
    else:
        prog = st.progress(0, text="Starting…")
        try:

            def _cb(cur: int, total: int) -> None:
                if total <= 0:
                    return
                prog.progress(min(cur / total, 1.0), text=f"Processing {cur}/{total}…")

            if use_streamlit_cache:

                @st.cache_data(ttl=3600, show_spinner=False)
                def _run(cas_t: tuple[str, ...], ep: str) -> list[dict[str, Any]]:
                    df = cross_validate_cas_list(list(cas_t), ep, use_disk_cache=True, progress_callback=None)
                    return df.to_dict(orient="records")

                rows = _run(tuple(sorted(set(cas_list))), endpoint)
                df = pd.DataFrame(rows)
            else:
                df = cross_validate_cas_list(
                    cas_list,
                    endpoint,
                    use_disk_cache=True,
                    progress_callback=_cb,
                )
            prog.progress(1.0, text="Done.")
            st.session_state["cv_result_df"] = df
        except Exception as e:
            prog.progress(1.0, text="Failed.")
            st.error(str(e))

if "cv_result_df" in st.session_state:
    res = st.session_state["cv_result_df"]
    if isinstance(res, pd.DataFrame) and not res.empty:
        st.subheader("Results")
        st.dataframe(res, use_container_width=True, hide_index=True)
        buf = io.StringIO()
        res.to_csv(buf, index=False)
        st.download_button(
            "Download CSV",
            data=buf.getvalue().encode("utf-8"),
            file_name="opera_cross_validation.csv",
            mime="text/csv",
        )
    elif isinstance(res, pd.DataFrame):
        st.info("Empty result.")

st.markdown("---")
st.markdown(
    "**Setup:** regenerate OPERA columns with "
    "`python scripts/discover_opera_endpoints.py --offline --out data/opera_endpoints.json` "
    "(or run without `--offline` if OPERA is installed). "
    "Set **P2OASYS_COMPOUND_LIST_CSV** in ``config`` / environment to a P2OASys export CSV to enable the load button."
)
