"""
Detect runtime environment and capabilities for SDS parsing.
"""

from __future__ import annotations

import os
from typing import Any

import psutil

try:
    import torch
except Exception:
    torch = None  # type: ignore[assignment]


class EnvironmentDetector:
    @staticmethod
    def _is_streamlit_cloud() -> bool:
        # Streamlit Cloud commonly exposes this path/env; keep checks permissive.
        return bool(
            os.getenv("STREAMLIT_CLOUD") == "1"
            or os.getenv("IS_STREAMLIT_CLOUD") == "1"
            or os.getenv("HOSTNAME", "").endswith(".streamlit.app")
        )

    @staticmethod
    def _check_ollama() -> bool:
        try:
            import requests

            r = requests.get("http://localhost:11434/api/tags", timeout=1.5)
            return r.status_code == 200
        except Exception:
            return False

    @staticmethod
    def detect() -> dict[str, Any]:
        total_mem_gb = psutil.virtual_memory().total / (1024**3)
        has_gpu = bool(torch is not None and torch.cuda.is_available())
        is_cloud = EnvironmentDetector._is_streamlit_cloud()
        has_ollama = EnvironmentDetector._check_ollama()

        env: dict[str, Any] = {
            "is_streamlit_cloud": is_cloud,
            "is_local": not is_cloud,
            "memory_gb": total_mem_gb,
            "cpu_count": psutil.cpu_count(),
            "has_gpu": has_gpu,
            "has_ollama": has_ollama,
        }
        if is_cloud:
            env["capability"] = "cloud_basic"
            env["max_methods"] = ["regex", "table_parsing", "pattern_matching"]
            env["can_use_ai"] = False
        elif total_mem_gb >= 8 and has_ollama:
            env["capability"] = "local_high"
            env["max_methods"] = ["regex", "table_parsing", "pattern_matching", "ollama_ai"]
            env["can_use_ai"] = True
        elif total_mem_gb >= 4:
            env["capability"] = "local_medium"
            env["max_methods"] = ["regex", "table_parsing", "pattern_matching"]
            env["can_use_ai"] = False
        else:
            env["capability"] = "local_low"
            env["max_methods"] = ["regex", "pattern_matching"]
            env["can_use_ai"] = False
        return env

    @staticmethod
    def get_capability_message() -> str:
        env = EnvironmentDetector.detect()
        if env["is_streamlit_cloud"]:
            return (
                "🖥️ **Running on Streamlit Cloud**\n"
                "- ✅ Rule-based extraction enabled\n"
                "- ✅ CAS extraction from Section 1/3/15 + focused patterns\n"
                "- ✅ GHS/physical/ecotox parsing\n"
                "- 🔒 AI model loading disabled in cloud mode"
            )
        if env["can_use_ai"]:
            return (
                "🚀 **Running locally with AI enhancement**\n"
                "- ✅ Rule-based extraction\n"
                "- ✅ Optional Ollama AI enhancement (`phi3:mini`)\n"
                "- ✅ Highest accuracy parsing path"
            )
        llm_wanted = False
        try:
            from config import USE_LLM_CAS_EXTRACTION
            llm_wanted = USE_LLM_CAS_EXTRACTION
        except ImportError:
            pass
        if llm_wanted and not env.get("has_ollama"):
            return (
                "💻 **Running locally (rule-based mode)**\n"
                "- ✅ Rule-based extraction enabled\n"
                "- ⚠️ `USE_LLM_CAS_EXTRACTION` is on, but Ollama is not running. Start Ollama and pull `qwen2:0.5b` for LLM CAS fallback."
            )
        return (
            "💻 **Running locally (rule-based mode)**\n"
            "- ✅ Rule-based extraction enabled\n"
            "- ℹ️ For AI enhancement install/start Ollama and pull `qwen2:0.5b` or `phi3:mini`. Set `USE_LLM_CAS_EXTRACTION=1` to enable."
        )
