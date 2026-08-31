"""Bundled example reports that can be opened without a live PubChem fetch."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import config

EXAMPLE_REPORTS_DIR = Path(config.DATA_DIR) / "example_reports"
DIOXOLANE_EXAMPLE_PATH = EXAMPLE_REPORTS_DIR / "1_3_dioxolane.json"


def load_dioxolane_example() -> dict[str, Any]:
    """Return Streamlit session payload for the bundled 1,3-dioxolane report."""
    with DIOXOLANE_EXAMPLE_PATH.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return {
        "pubchem": payload["pubchem"],
        "dsstox_info": payload.get("dsstox_info"),
        "dtxsid": payload.get("dtxsid"),
        "preferred_name": payload.get("preferred_name"),
        "clean_cas": payload["clean_cas"],
        "toxval_data": payload.get("toxval_data"),
        "is_example": True,
        "example_id": payload.get("id"),
        "snapshot_date": payload.get("snapshot_date"),
    }
