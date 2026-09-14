"""
Runtime / deployment hints for services (Streamlit Cloud vs local).
"""

from __future__ import annotations

import os


class ServiceConfig:
    """Lightweight config for capability messaging (no Streamlit dependency)."""

    @staticmethod
    def is_streamlit_cloud() -> bool:
        return bool(
            os.getenv("STREAMLIT_CLOUD") == "1"
            or os.getenv("IS_STREAMLIT_CLOUD") == "1"
            or os.getenv("HOSTNAME", "").endswith(".streamlit.app")
        )

    @staticmethod
    def get_capability_message() -> str:
        if ServiceConfig.is_streamlit_cloud():
            return (
                "☁️ **Streamlit Cloud**\n"
                "- ✅ PubChem + local SQLite lookups (when databases are bundled)\n"
                "- ✅ SDS parsing (rule-based; optional local AI only on your machine)\n"
                "- 💡 Run the app locally for Ollama-enhanced SDS parsing"
            )
        return (
            "🖥️ **Local run**\n"
            "- ✅ Full database + SDS parsing\n"
            "- ✅ Optional Ollama enhancement when configured (see SDS parser capabilities)"
        )
