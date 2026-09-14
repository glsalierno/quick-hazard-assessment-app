"""Safe Streamlit secrets access (no crash when secrets.toml is absent)."""

from __future__ import annotations

import os
from typing import Any


def get_secret(key: str, default: str = "") -> str:
    """
    Return a secret string without requiring ``.streamlit/secrets.toml``.

    Newer Streamlit raises ``StreamlitSecretNotFoundError`` on ``st.secrets.get``
    when no secrets file exists. Env vars are checked first.
    """
    env = (os.environ.get(key) or "").strip()
    if env:
        return env
    try:
        import streamlit as st

        if not hasattr(st, "secrets"):
            return default
        try:
            raw: Any = st.secrets[key]  # may raise if file missing or key absent
        except Exception:
            return default
        return str(raw or "").strip() or default
    except Exception:
        return default
