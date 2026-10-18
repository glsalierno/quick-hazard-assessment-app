"""
Gap-fill flash point via glsalierno/pubchem-flashpoint-retriever.

Prefer importing vendor/get_fp2.py (or hazquery clone); fall back to subprocess.
Used only when PubChem LCSS/SDS flash point is already missing.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_REPO_CANDIDATES = [
    Path(__file__).resolve().parents[1] / "vendor" / "pubchem_flashpoint_retriever",
    Path(__file__).resolve().parents[4] / "pubchem-flashpoint-retriever",  # hazquery/
    Path(__file__).resolve().parents[3] / "pubchem-flashpoint-retriever",
]


def _resolve_tool_dir() -> Optional[Path]:
    for c in _REPO_CANDIDATES:
        if (c / "get_fp2.py").is_file():
            return c
    return None


def _load_get_fp2_module():
    tool = _resolve_tool_dir()
    if tool is None:
        return None
    path = tool / "get_fp2.py"
    spec = importlib.util.spec_from_file_location("pubchem_fp_get_fp2", path)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch_flash_points_for_cas(cas: str) -> list[str]:
    """Return flash-point strings for one CAS (may be empty)."""
    cas = (cas or "").strip()
    if not cas:
        return []
    mod = _load_get_fp2_module()
    if mod is not None:
        try:
            cid = mod.get_compound_id_from_cas(cas)
            if not cid:
                return []
            return list(mod.get_flash_points(cid) or [])
        except Exception as exc:
            logger.debug("flashpoint import path failed: %s", exc)
    tool = _resolve_tool_dir()
    if tool is None:
        return []
    try:
        proc = subprocess.run(
            [sys.executable, str(tool / "get_fp2.py")],
            input=cas + "\n",
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if proc.returncode != 0 or not (proc.stdout or "").strip():
            return []
        data = json.loads(proc.stdout)
        return list(data.get(cas) or [])
    except Exception as exc:
        logger.debug("flashpoint subprocess failed: %s", exc)
        return []


def gapfill_flash_point_extra(
    cas: str,
    *,
    existing_flash: list[Any] | None = None,
) -> dict[str, Any] | None:
    """
    Build extra_sources fragment when flash point is missing.

    Tags values as experimental (PubChem Experimental Properties) via
    ``flash_meta.tier = experimental`` / ``source = pubchem_flashpoint_retriever``.
    """
    if existing_flash:
        return None
    vals = fetch_flash_points_for_cas(cas)
    if not vals:
        return None
    return {
        "hazard_metrics": {"flash_point": vals[:5]},
        "flash_meta": {
            "source": "pubchem_flashpoint_retriever",
            "tier": "experimental",
            "n_values": len(vals),
        },
        "_pipeline_notes": [
            f"Flash point gap-fill via pubchem-flashpoint-retriever ({len(vals)} value(s))"
        ],
    }
