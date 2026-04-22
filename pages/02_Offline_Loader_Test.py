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

_SYNC_KEYS = (
    "OFFLINE_LOCAL_ARCHIVE",
    "OFFLINE_DOSSIER_INFO_XLSX",
    "OFFLINE_DOSSIER_INFO_SHEET",
    "OFFLINE_DATA_DIR",
    "OFFLINE_CACHE_DIR",
    "OFFLINE_SCRAPE_CL",
    "OFFLINE_I6Z_USE_MP",
    "OFFLINE_I6Z_MAX_WORKERS",
    "OFFLINE_I6Z_MIN_FILES_FOR_MP",
    "HAZQUERY_DISABLE_DOCLING",
)


def _sync_secrets_to_environ() -> None:
    """Mirror ``st.secrets`` into ``os.environ`` when the variable is empty (loader uses ``os.getenv``)."""
    try:
        sec: Any = st.secrets
    except (FileNotFoundError, RuntimeError, AttributeError):
        return
    for key in _SYNC_KEYS:
        if (os.environ.get(key) or "").strip():
            continue
        try:
            if key in sec:
                val = sec[key]
                os.environ[key] = str(val).strip() if val is not None else ""
        except Exception:
            continue


def _cfg(key: str, default: str = "") -> str:
    return (os.environ.get(key) or "").strip() or default


_sync_secrets_to_environ()

st.title("Offline ECHA / IUCLID loader")
st.caption(
    "Uses ``load_echa_from_offline()`` from ``ingest.offline_echa_loader`` (parallel i6z parse when enabled). "
    "First full build can take many minutes; snapshots under ``OFFLINE_CACHE_DIR`` make later loads fast."
)

st.subheader("Effective configuration")
st.json({k: _cfg(k) or "(not set)" for k in _SYNC_KEYS})

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
    c1.metric("Substances rows", len(substances_df))
    c2.metric("Hazard / CL rows", len(hazards_df))
    c3.metric("Last run (s)", f"{elapsed:.2f}")

    try:
        import psutil

        vm = psutil.virtual_memory()
        st.caption(f"Memory: {vm.percent}% used ({vm.used / (1024**3):.1f} / {vm.total / (1024**3):.1f} GiB)")
    except Exception:
        pass

    with st.expander("Preview substances", expanded=False):
        st.dataframe(substances_df.head(50), use_container_width=True)
    with st.expander("Preview hazards", expanded=False):
        st.dataframe(hazards_df.head(50), use_container_width=True)

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
