"""
Bridge SDS / unified input → existing session-based assessment pipeline (v1.3 behavior).
"""

from __future__ import annotations

from typing import Optional

import streamlit as st


def apply_assessment_query(
    identifier: str,
    *,
    show_banner: bool = False,
    banner_note: Optional[str] = None,
) -> None:
    """
    Set session state so the main `if current_query:` block runs full fetch
    (PubChem + DSSTox + ToxValDB + CPDB + …) exactly like typing in the form.

    Uses ``_pending_cas_query_input`` so the CAS text field can be synced on the
    next run *before* ``st.text_input(..., key="cas_query_input")`` is created
    (required on Streamlit Cloud / recent Streamlit versions).
    """
    ident = (identifier or "").strip()
    if not ident:
        return
    st.session_state["query"] = ident
    # Cannot assign cas_query_input after the text_input widget exists (StreamlitAPIException on Cloud).
    # Apply on next run via app.py before the widget is created.
    st.session_state["_pending_cas_query_input"] = ident
    st.session_state["result_for"] = None
    if show_banner:
        st.session_state["show_assessment_from_unified"] = True
        if banner_note:
            st.session_state["unified_assess_note"] = banner_note
        else:
            st.session_state.pop("unified_assess_note", None)
