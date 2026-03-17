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


def _opinion_label(code: str | None) -> str:
    """Convert CPDB opinion code to user-friendly label (author's carcinogenicity assessment)."""
    if not code:
        return "—"
    c = str(code).strip()
    if c in ("p", "+"):
        return "Positive (author considered carcinogenic)"
    if c == "0":
        return "Negative"
    if c == "-":
        return "Equivocal"
    return c


def get_data_by_cas(cas: str, db_path: Path | str | None = None) -> dict[str, Any]:
    """
    Get experiments and dose–response rows for a CAS number.
    Experiments are joined with code tables for human-readable species, route, tissue, tumor, strain.
    Opinion is decoded to a short label. Doses are sorted by dose (low to high) with all columns preserved.
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

        # Experiments with decoded labels from code tables (LEFT JOIN so we keep rows even if code missing)
        cur = conn.execute(
            """
            SELECT
                e.idnum, e.chemcode, e.name, e.cas, e.source,
                e.species, e.strain, e.sex, e.route, e.tissue, e.tumor,
                e.opinion, e.td50, e.lc, e.uc, e.pval,
                COALESCE(s.spname, e.species) AS species_name,
                COALESCE(r.rtename, e.route) AS route_name,
                COALESCE(t.tisname, e.tissue) AS tissue_name,
                COALESCE(u.tumname, e.tumor) AS tumor_name,
                COALESCE(st.strname, e.strain) AS strain_name
            FROM cpdb_experiments e
            LEFT JOIN cpdb_species s ON e.species = s.species
            LEFT JOIN cpdb_route r ON e.route = r.route
            LEFT JOIN cpdb_tissue t ON e.tissue = t.tissue
            LEFT JOIN cpdb_tumor u ON e.tumor = u.tumor
            LEFT JOIN cpdb_strain st ON e.strain = st.strain
            WHERE REPLACE(REPLACE(COALESCE(e.cas,''), '-', ''), ' ', '') = ?
            ORDER BY e.idnum
            LIMIT 500
            """,
            (cas_norm,),
        )
        rows = cur.fetchall()
        experiments = [dict(zip(r.keys(), r)) for r in rows]
        for e in experiments:
            e["opinion_label"] = _opinion_label(e.get("opinion"))

        if not experiments:
            conn.close()
            return out

        idnums = [str(e["idnum"]) for e in experiments]
        placeholders = ",".join("?" * len(idnums))
        # Doses: sort by numeric dose ascending (low to high), keep all columns
        cur = conn.execute(
            f"""
            SELECT * FROM cpdb_doses
            WHERE idnum IN ({placeholders})
            ORDER BY idnum, CAST(COALESCE(dose,'0') AS REAL) ASC, dose_order
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
