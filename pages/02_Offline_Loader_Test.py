"""
Multipage Streamlit: offline REACH / IUCLID loader (i6z + optional dossier XLSX).

Run from the app root::

    streamlit run app.py

Set paths via **environment variables** in PowerShell before launch, **or** add the same keys to
``.streamlit/secrets.toml`` (this page copies them into ``os.environ`` when unset so
``ingest.offline_echa_loader`` sees them).
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import streamlit as st

st.set_page_config(page_title="Offline ECHA loader", layout="wide")

from unified_hazard_report.iuclid_integration import (
    OFFLINE_ENV_KEYS,
    sync_offline_secrets_from_st_secrets,
    using_committed_reach_demo_archive,
)


def _cfg(key: str, default: str = "") -> str:
    return (os.environ.get(key) or "").strip() or default


sync_offline_secrets_from_st_secrets()

if using_committed_reach_demo_archive():
    st.warning(
        "**Demo REACH archive:** `OFFLINE_LOCAL_ARCHIVE` points at the committed "
        "`data/reach_demo/reach_subset.zip`. This is a **non-exhaustive** subset for demos — "
        "most substances are absent and parsed data may be incomplete. Not for regulatory use."
    )


def _nonempty_col(df: Any, col: str) -> int:
    if df is None or col not in df.columns:
        return 0
    s = df[col]
    if s.dtype == object:
        t = s.astype(str).str.strip()
        return int(((t != "") & (t.lower() != "nan") & (t != "None")).sum())
    return int(s.notna().sum())


st.title("Offline ECHA / IUCLID loader")
st.info(
    "**Where this lives:** use the **sidebar** (pages menu) to open this page. "
    "The main Quick Hazard Assessment flow in ``app.py`` does **not** show REACH/IUCLID dossiers yet — "
    "only this page and the **unified hazard report** CLI (``unified_hazard_report/``) consume these snapshots."
)
st.caption(
    "Uses ``load_echa_from_offline()`` from ``ingest.offline_echa_loader`` (parallel i6z parse when enabled). "
    "First full build can take many minutes; snapshots under ``OFFLINE_CACHE_DIR`` make later loads fast."
)

st.subheader("Effective configuration")
st.json({k: _cfg(k) or "(not set)" for k in OFFLINE_ENV_KEYS})

la = _cfg("OFFLINE_LOCAL_ARCHIVE")
if not la:
    st.warning(
        "Set **OFFLINE_LOCAL_ARCHIVE** to your REACH ``.zip`` / ``.7z`` or a folder of ``.i6z`` files "
        "(PowerShell: ``$env:OFFLINE_LOCAL_ARCHIVE = '...'``), or add it to ``.streamlit/secrets.toml``."
    )
else:
    p = Path(os.path.expandvars(la)).expanduser()
    st.info(f"Archive / folder path resolves to: `{p}` (exists: {p.exists()})")

force_rebuild = st.checkbox(
    "Force rebuild (ignore CSV snapshots; re-parse i6z)",
    value=False,
    help="Same as load_echa_from_offline(force_rebuild=True).",
)
max_cl = st.number_input(
    "Cap CHEM fallback rows (optional; 0 = no cap)",
    min_value=0,
    value=0,
    help="Passed as max_substances_for_cl when > 0. Leave 0 unless OFFLINE_SCRAPE_CL=true.",
)
if _cfg("OFFLINE_SCRAPE_CL").lower() in ("1", "true", "yes", "on"):
    st.warning(
        "**OFFLINE_SCRAPE_CL** is on — ECHA CHEM scraping is slow and often empty. Prefer i6d classification only."
    )

if st.button("Load offline data", type="primary"):
    if not la:
        st.error("OFFLINE_LOCAL_ARCHIVE is not set.")
    else:
        try:
            from ingest.offline_echa_loader import load_echa_from_offline
        except ImportError as e:
            st.error(f"Cannot import offline loader: {e}. Run Streamlit from the **quick-hazard-assessment-app** root.")
            st.stop()

        kw: dict[str, Any] = {
            "use_cache": True,
            "force_rebuild": force_rebuild,
            "force_download": False,
        }
        if max_cl and max_cl > 0:
            kw["max_substances_for_cl"] = int(max_cl)

        with st.spinner("Loading / building offline tables (first run can take a long time)…"):
            t0 = time.perf_counter()
            try:
                substances_df, hazards_df = load_echa_from_offline(**kw)
            except Exception as exc:
                st.exception(exc)
                st.stop()
            elapsed = time.perf_counter() - t0

        st.session_state["offline_substances"] = substances_df
        st.session_state["offline_hazards"] = hazards_df
        st.session_state["offline_elapsed_s"] = elapsed
        st.success(f"Finished in **{elapsed:.2f} s**")

if "offline_substances" in st.session_state:
    substances_df = st.session_state["offline_substances"]
    hazards_df = st.session_state["offline_hazards"]
    elapsed = float(st.session_state.get("offline_elapsed_s", 0.0))

    c1, c2, c3 = st.columns(3)
    c1.metric("Dossier index rows (uuid)", len(substances_df))
    c2.metric("C&L / GHS rows (from i6d)", len(hazards_df))
    c3.metric("Last run (s)", f"{elapsed:.2f}")

    n_cas = _nonempty_col(substances_df, "cas_number")
    n_ec = _nonempty_col(substances_df, "ec_number")
    n_name = _nonempty_col(substances_df, "substance_name")

    st.subheader("What is ECHA / IUCLID here?")
    st.markdown(
        """
| Table | Meaning |
|------|--------|
| **Dossier index** (substances) | One row per **IUCLID dossier UUID** (`uuid`). `infocard_url` is the ECHA substance information link. CAS/EC/name usually come from **`Document.i6d`** and/or the **dossier-info XLSX** (`OFFLINE_DOSSIER_INFO_XLSX`). |
| **C&L / hazards** | GHS / classification-like rows parsed from **`Document.i6d`** XML (heuristic). REACH **Study Results** dossiers often include **little or no** classifiable GHS in that XML, so this table can be **empty** even when the index has tens of thousands of UUIDs. |

**If “Hazard / CL rows” is 0:** that is common for Study Results–only builds with `OFFLINE_SCRAPE_CL=false`. Your IUCLID/ECHA identifiers still appear in the **dossier index** (`uuid`, `infocard_url`, CAS when merged). For more hazard lines you can try **Force rebuild** after updating archives, point **`OFFLINE_DOSSIER_INFO_XLSX`** at ECHA’s companion workbook, or enable **`OFFLINE_SCRAPE_CL=true`** (slow; CHEM HTML is often sparse).
        """
    )
    c4, c5, c6 = st.columns(3)
    c4.metric("Rows with CAS", n_cas)
    c5.metric("Rows with EC", n_ec)
    c6.metric("Rows with name", n_name)

    if len(hazards_df) == 0:
        st.warning(
            "The **C&L / GHS** snapshot is empty. Scroll down — **IUCLID dossier identifiers and ECHA links** "
            "are in the **dossier index** table below (`uuid`, `infocard_url`, etc.), not in the hazards table."
        )

    try:
        import psutil

        vm = psutil.virtual_memory()
        st.caption(f"Memory: {vm.percent}% used ({vm.used / (1024**3):.1f} / {vm.total / (1024**3):.1f} GiB)")
    except Exception:
        pass

    preview_cols = [c for c in ("uuid", "cas_number", "ec_number", "substance_name", "infocard_url") if c in substances_df.columns]
    preview = substances_df[preview_cols] if preview_cols else substances_df

    st.subheader("Dossier index preview (IUCLID UUID + ECHA identifiers)")
    st.dataframe(preview.head(25), use_container_width=True, hide_index=True)

    with st.expander("Full substances table (first 200 columns as-is)", expanded=False):
        st.dataframe(substances_df.head(200), use_container_width=True)
    with st.expander("C&L / GHS hazard rows (full preview)", expanded=len(hazards_df) > 0):
        if len(hazards_df) == 0:
            st.caption("No rows to show.")
        else:
            st.dataframe(hazards_df.head(200), use_container_width=True)

st.divider()
st.markdown(
    """
**PowerShell (before `streamlit run app.py`):**

```powershell
cd path\\to\\quick-hazard-assessment-app
$env:OFFLINE_LOCAL_ARCHIVE = "C:\\path\\to\\REACH_Study_Results.zip"
$env:OFFLINE_DOSSIER_INFO_XLSX = "C:\\path\\to\\reach_study_results-dossier_info_*.xlsx"   # optional
$env:OFFLINE_SCRAPE_CL = "false"
$env:OFFLINE_I6Z_USE_MP = "true"
$env:OFFLINE_I6Z_MAX_WORKERS = "8"
streamlit run app.py
```

**``secrets.toml``:** use the same key names; this page copies them into the environment when unset.
"""
)
