"""
SDS parser debugging toolkit — session logs, raw previews, and failure heuristics.

Enable via sidebar **SDS parser debug** or environment ``SDS_DEBUG=1``.
"""

from __future__ import annotations

import os
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

# Max log entries kept in Streamlit session (avoid memory blowup)
_MAX_LOGS = 250
_PREVIEW_CHARS = 4000
_SECTION_SNIPPET = 2800


def is_sds_debug_enabled() -> bool:
    """True if logging should run (Streamlit session or env)."""
    if os.getenv("SDS_DEBUG", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    try:
        import streamlit as st

        return bool(st.session_state.get("sds_debug_enabled", False))
    except Exception:
        return False


def make_json_safe(obj: Any, max_depth: int = 8, _depth: int = 0) -> Any:
    """Convert objects to structures safe for ``st.json``."""
    if _depth > max_depth:
        return "<max depth>"
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        if len(obj) > _PREVIEW_CHARS:
            return obj[:_PREVIEW_CHARS] + f"\n… [{len(obj) - _PREVIEW_CHARS} more chars]"
        return obj
    if isinstance(obj, bytes):
        return f"<bytes len={len(obj)}>"
    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            return {k: make_json_safe(v, max_depth, _depth + 1) for k, v in asdict(obj).items()}
        except Exception:
            return str(obj)[:500]
    if isinstance(obj, dict):
        out = {}
        for i, (k, v) in enumerate(obj.items()):
            if i >= 80:
                out["__truncated__"] = f"{len(obj) - 80} more keys"
                break
            out[str(k)] = make_json_safe(v, max_depth, _depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        return [make_json_safe(x, max_depth, _depth + 1) for x in obj[:120]] + (
            [f"… +{len(obj) - 120} items"] if len(obj) > 120 else []
        )
    if hasattr(obj, "to_dict"):
        try:
            return make_json_safe(obj.to_dict(), max_depth, _depth + 1)
        except Exception:
            pass
    if hasattr(obj, "head") and hasattr(obj, "columns"):  # pandas DataFrame
        try:
            df = obj
            rec = df.head(15).fillna("").astype(str).to_dict(orient="records")
            return {
                "type": "DataFrame",
                "shape": [int(df.shape[0]), int(df.shape[1])],
                "columns": list(df.columns.astype(str)),
                "head_15": rec,
            }
        except Exception:
            return str(obj)[:800]
    return str(obj)[:800]


def cas_rows_brief(rows: Any) -> List[Dict[str, Any]]:
    """Compact CAS extraction rows for tables."""
    out: List[Dict[str, Any]] = []
    if not rows:
        return out
    for r in rows[:40]:
        if hasattr(r, "cas"):
            out.append(
                {
                    "cas": getattr(r, "cas", None),
                    "chemical_name": (getattr(r, "chemical_name", None) or "")[:60] or None,
                    "concentration": (getattr(r, "concentration", None) or "")[:40] or None,
                    "method": getattr(r, "method", None),
                    "section": getattr(r, "section", None),
                    "validated": getattr(r, "validated", None),
                }
            )
        elif isinstance(r, dict):
            out.append({k: r.get(k) for k in ("cas", "chemical_name", "concentration", "method", "section")})
    if len(rows) > 40:
        out.append({"note": f"+ {len(rows) - 40} more rows"})
    return out


def diagnose_extraction_gaps(rows: Any) -> List[str]:
    """Human-readable hints when names/concentrations are missing."""
    hints: List[str] = []
    if not rows:
        hints.append("No CAS rows — check PDF text layer, OCR, or Section 3 headers.")
        return hints
    lst = list(rows)
    n = len(lst)
    with_name = sum(1 for r in lst if (getattr(r, "chemical_name", None) or "").strip())
    with_conc = sum(1 for r in lst if (getattr(r, "concentration", None) or "").strip())
    methods = {getattr(r, "method", "") for r in lst}

    if with_name == 0:
        hints.append(
            "**Names missing:** `focused_regex` often supplies CAS only. "
            "Whitespace/table parsers need pipe/tab tables or aligned columns in **flattened** text — "
            "pypdf usually destroys column layout."
        )
    if with_conc == 0 and n > 0:
        hints.append(
            "**Concentrations missing:** Same as names — need structured table cells or Docling `export_to_dataframe`."
        )
    if not any(
        x
        for m in methods
        for x in (
            "line_composition" in (m or ""),
            "html_table" in (m or ""),
            "pipe_table" in (m or ""),
            "delimiter_table" in (m or ""),
            "docling" in (m or ""),
        )
    ):
        hints.append(
            "No **table/composition** methods in rows (only regex-style) → flattened PDF text likely **broke column alignment**."
        )
    if not any("docling" in (m or "") for m in methods):
        hints.append("**Docling** did not label any row — install `docling` or unset `HAZQUERY_DISABLE_DOCLING`.")
    if with_name < n and with_name > 0:
        hints.append(f"Partial names ({with_name}/{n}) — merge may have filled some CAS from regex only.")
    return hints


def sds_debug_log(stage: str, data: Any = None, metadata: Optional[Dict[str, Any]] = None) -> None:
    """Append one log entry when debug is on (Streamlit session)."""
    if not is_sds_debug_enabled():
        return
    try:
        import streamlit as st

        entry = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "stage": stage,
            "data": make_json_safe(data) if data is not None else None,
            "metadata": make_json_safe(metadata or {}),
        }
        logs: List[Dict[str, Any]] = st.session_state.setdefault("sds_debug_logs", [])
        logs.append(entry)
        if len(logs) > _MAX_LOGS:
            del logs[: len(logs) - _MAX_LOGS]
    except Exception:
        pass


class SDSDebugger:
    """Optional OO wrapper; prefer ``sds_debug_log()`` from parser code."""

    def enable_debug(self) -> None:
        try:
            import streamlit as st

            st.session_state["sds_debug_enabled"] = True
            st.session_state.setdefault("sds_debug_logs", [])
            st.session_state["sds_debug_counter"] = st.session_state.get("sds_debug_counter", 0) + 1
        except Exception:
            pass

    def disable_debug(self) -> None:
        try:
            import streamlit as st

            st.session_state["sds_debug_enabled"] = False
        except Exception:
            pass

    def clear_logs(self) -> None:
        try:
            import streamlit as st

            st.session_state["sds_debug_logs"] = []
            st.session_state["sds_debug_counter"] = st.session_state.get("sds_debug_counter", 0) + 1
        except Exception:
            pass

    def get_logs(self) -> List[Dict[str, Any]]:
        try:
            import streamlit as st

            return list(st.session_state.get("sds_debug_logs", []))
        except Exception:
            return []

    def render_debug_ui(self) -> None:
        """Streamlit expander: metrics, stage filter, log viewer, heuristics."""
        try:
            import streamlit as st
        except Exception:
            return

        if not st.session_state.get("sds_debug_enabled", False) and not os.getenv("SDS_DEBUG"):
            return

        with st.expander("🔍 SDS parser debug console", expanded=False):
            logs = self.get_logs()
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Log entries", len(logs))
            with c2:
                st.metric("Session counter", st.session_state.get("sds_debug_counter", 0))
            with c3:
                if st.button("🧹 Clear debug logs", key="sds_dbg_clear"):
                    self.clear_logs()
                    st.rerun()

            # Heuristics from last parse.cas_final if present
            last_rows = None
            for e in reversed(logs):
                if e.get("stage") == "parse.cas_final" and isinstance(e.get("data"), dict):
                    last_rows = e["data"].get("rows")
                    break
            if last_rows and isinstance(last_rows, list):

                class _Row:
                    __slots__ = ("cas", "chemical_name", "concentration", "method")

                    def __init__(self, d: Dict[str, Any]):
                        self.cas = d.get("cas")
                        self.chemical_name = d.get("chemical_name")
                        self.concentration = d.get("concentration")
                        self.method = d.get("method")

                fakes = [_Row(r) for r in last_rows if isinstance(r, dict) and r.get("cas")]
                hints = diagnose_extraction_gaps(fakes)
                if hints:
                    st.markdown("**Diagnostics**")
                    for h in hints:
                        st.markdown(f"- {h}")

            if not logs:
                st.info("No debug logs yet. Upload an SDS and click **Extract CAS from SDS** (or enable before parsing).")
                return

            stages = sorted({e["stage"] for e in logs})
            pick = st.selectbox("Filter by stage", ["(all)"] + stages, key="sds_dbg_stage_filter")
            filtered = logs if pick == "(all)" else [e for e in logs if e["stage"] == pick]

            st.caption(f"Showing {len(filtered)} / {len(logs)} entries")

            for i, log in enumerate(reversed(filtered[-50:])):  # newest first, cap UI
                title = f"`{log['stage']}` · {log.get('timestamp', '')}"
                with st.expander(title, expanded=(i == 0)):
                    if log.get("data") is not None:
                        st.json(log["data"])
                    if log.get("metadata"):
                        st.caption("Metadata")
                        st.json(log["metadata"])


def render_sds_debug_sidebar_controls() -> None:
    """Checkbox + env hint for sidebar."""
    try:
        import streamlit as st
    except Exception:
        return

    env_on = os.getenv("SDS_DEBUG", "").strip().lower() in ("1", "true", "yes", "on")
    st.checkbox(
        "SDS parser debug logging",
        key="sds_debug_enabled",
        help="Log parsing stages to the debug console below. Or set env SDS_DEBUG=1.",
    )
    if env_on:
        st.caption("Env **SDS_DEBUG** is on — logging active.")
    dbg = get_sds_debugger()
    dbg.render_debug_ui()


def _sds_debugger_factory() -> SDSDebugger:
    return SDSDebugger()


try:
    import streamlit as st

    get_sds_debugger = st.cache_resource(_sds_debugger_factory)
except Exception:

    def get_sds_debugger() -> SDSDebugger:  # type: ignore[misc]
        return _sds_debugger_factory()
