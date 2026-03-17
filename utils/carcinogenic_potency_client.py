"""
Carcinogenic Potency Database client — read-only access to pre-built SQLite.
Uses data/carcinogenic_potency.sqlite built by scripts/build_carcinogenic_potency_from_cpdb_tabs.py.
Display name for UI: always use full name "Carcinogenic Potency Database".
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

# Repo root = parent of utils/
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "carcinogenic_potency.sqlite"

DISPLAY_NAME = "Carcinogenic Potency Database"


def _normalize_cas(cas: str | None) -> str:
    if not cas or not isinstance(cas, str):
        return ""
    return cas.strip().replace("-", "").replace(" ", "")


def is_available(db_path: Path | str | None = None) -> bool:
    """Return True if the Carcinogenic Potency Database SQLite file exists."""
    path = db_path or DEFAULT_DB_PATH
    return os.path.isfile(path)


def get_data_by_cas(cas: str, db_path: Path | str | None = None) -> dict[str, Any]:
    """
    Get experiments and dose–response rows for a CAS number.
    Returns dict with keys: found, display_name, experiments, doses.
    """
    path = db_path or DEFAULT_DB_PATH
    out: dict[str, Any] = {
        "found": False,
        "display_name": DISPLAY_NAME,
        "experiments": [],
        "doses": [],
    }
    if not cas or not is_available(path):
        return out

    cas_norm = _normalize_cas(cas)
    if not cas_norm:
        return out

    try:
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row

        # Match CAS with or without dashes (DB may store 79-06-1 or 79061)
        cur = conn.execute(
            """
            SELECT * FROM cpdb_experiments
            WHERE REPLACE(REPLACE(COALESCE(cas,''), '-', ''), ' ', '') = ?
            ORDER BY idnum
            LIMIT 500
            """,
            (cas_norm,),
        )
        rows = cur.fetchall()
        experiments = [dict(zip(r.keys(), r)) for r in rows]

        if not experiments:
            conn.close()
            return out

        idnums = [str(e["idnum"]) for e in experiments]
        placeholders = ",".join("?" * len(idnums))
        cur = conn.execute(
            f"""
            SELECT * FROM cpdb_doses
            WHERE idnum IN ({placeholders})
            ORDER BY idnum, dose_order
            """,
            idnums,
        )
        dose_rows = cur.fetchall()
        doses = [dict(zip(r.keys(), r)) for r in dose_rows]

        conn.close()

        out["found"] = True
        out["experiments"] = experiments
        out["doses"] = doses
    except Exception:
        pass

    return out
