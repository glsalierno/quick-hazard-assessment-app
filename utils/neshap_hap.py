"""
CAA §112(b) Hazardous Air Pollutant (HAP) membership for P2OASys NESHAP scoring.

Authoritative list: Clean Air Act §112(b) (govinfo HTML → data/caa112b_hap_by_cas.csv).
EPA SRS list 165 (CAA112(b) HAP) is the same inventory; interactive CDX export is
optional for refresh — prefer ``scripts/build_caa112b_hap_csv.py``.

Matrix Key Phrases (Atmospheric Hazard / NESHAP):
  - Not listed as EPA hazardous air pollutant → 2
  - Not considered to be a hazardous air pollutant → 4
  - Listed as EPA hazardous air pollutant → 6
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from utils.lookup_tables import normalize_cas_for_lookup

PHRASE_LISTED = "Listed as EPA hazardous air pollutant"
PHRASE_NOT_LISTED = "Not listed as EPA hazardous air pollutant"
NESHAP_SCORE_LISTED = 6
NESHAP_SCORE_NOT_LISTED = 2


def default_hap_csv_path(repo_root: Path | str | None = None) -> Path:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent
    return root / "data" / "caa112b_hap_by_cas.csv"


def load_hap_cas_set(path: Path | str | None = None) -> set[str]:
    """Return normalized (digits-only) CAS set for CAA §112(b) HAPs."""
    path = Path(path) if path else default_hap_csv_path()
    out: set[str] = set()
    if not path.is_file():
        return out
    try:
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cas = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                if cas:
                    out.add(cas)
    except Exception:
        return out
    return out


def neshap_membership(
    cas: str | None,
    hap_cas: set[str] | None = None,
    *,
    hap_csv: Path | str | None = None,
) -> dict[str, Any]:
    """
    Return NESHAP list-membership result for a CAS.

    Keys: listed (bool), phrase, score, source, cas_norm.
    """
    cas_norm = normalize_cas_for_lookup(cas)
    table = hap_cas if hap_cas is not None else load_hap_cas_set(hap_csv)
    listed = bool(cas_norm and cas_norm in table)
    return {
        "listed": listed,
        "phrase": PHRASE_LISTED if listed else PHRASE_NOT_LISTED,
        "score": NESHAP_SCORE_LISTED if listed else NESHAP_SCORE_NOT_LISTED,
        "source": "caa112b_hap_csv",
        "cas_norm": cas_norm,
        "n_hap": len(table),
    }


def apply_neshap_to_extra_sources(
    extra_sources: dict[str, Any] | None,
    cas: str | None,
    *,
    hap_cas: set[str] | None = None,
    hap_csv: Path | str | None = None,
) -> dict[str, Any]:
    """Attach NESHAP phrase designation + meta onto extra_sources for the scorer."""
    out: dict[str, Any] = dict(extra_sources) if extra_sources else {}
    info = neshap_membership(cas, hap_cas=hap_cas, hap_csv=hap_csv)
    if not info.get("cas_norm") or not info.get("n_hap"):
        return out
    hm = dict(out.get("hazard_metrics") or {})
    designations = list(hm.get("other_designations") or [])
    # Drop prior NESHAP / HAP phrase stubs
    designations = [
        d
        for d in designations
        if "hazardous air pollutant" not in str(d).lower()
        and "neshap" not in str(d).lower()
    ]
    designations.append(info["phrase"])
    hm["other_designations"] = designations
    out["hazard_metrics"] = hm
    out["neshap_meta"] = {
        "listed": info["listed"],
        "phrase": info["phrase"],
        "score": info["score"],
        "source": info["source"],
    }
    notes = list(out.get("_pipeline_notes") or [])
    note = f"NESHAP: {info['phrase']} ({info['source']})"
    if note not in notes:
        notes.append(note)
    out["_pipeline_notes"] = notes
    return out
