"""
Lookup-only solvent references: expert P2OASys, CHEM21 (reconstructed), Hansen (HSPiP cache).

Does not predict P2OASys. CHEM21 rankings are always reconstructed from PubChem/GHS
via the Prat-style engine when data is available. Hansen is HSPiP-only (no DoSS fallback).
Missing expert P2OASys CAS → status ``not_found`` with message ``P2OASys assessment required``.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Literal, TypedDict

from utils.lookup_tables import normalize_cas_for_lookup

LookupStatus = Literal["found", "not_found", "source_unavailable", "estimated"]

P2OASYS_ASSESSMENT_REQUIRED = "P2OASys assessment required"
HSPIP_REPO_URL = "https://github.com/glsalierno/cas-to-HSPiP_data"
CHEM21_NOT_IN_GUIDE = "CHEM21 rating not in solvent guide"

_SKIP_CHEM21_COLUMNS = frozenset({"graph", "H3_phrase", "H4_phrase"})


class P2OASysLookup(TypedDict):
    status: LookupStatus
    message: str
    cas: str
    name: str | None
    score: float | None
    date_created: str | None
    source_path: str | None
    single_substance: bool


class Chem21Lookup(TypedDict):
    status: LookupStatus
    message: str
    cas: str
    solvent: str | None
    family: str | None
    safety: float | None
    health: float | None
    env: float | None
    ranking_default: str | None
    ranking_discussion: str | None
    source_path: str | None


class HspLookup(TypedDict):
    status: LookupStatus
    message: str
    cas: str
    delta_d: float | None
    delta_p: float | None
    delta_h: float | None
    source: str | None
    source_path: str | None


def _empty_p2oasys(cas: str, status: LookupStatus, message: str, source: str | None = None) -> P2OASysLookup:
    return {
        "status": status,
        "message": message,
        "cas": cas,
        "name": None,
        "score": None,
        "date_created": None,
        "source_path": source,
        "single_substance": True,
    }


def _empty_chem21(cas: str, status: LookupStatus, message: str, source: str | None = None) -> Chem21Lookup:
    return {
        "status": status,
        "message": message,
        "cas": cas,
        "solvent": None,
        "family": None,
        "safety": None,
        "health": None,
        "env": None,
        "ranking_default": None,
        "ranking_discussion": None,
        "source_path": source,
    }


def _empty_hsp(cas: str, status: LookupStatus, message: str, source: str | None = None) -> HspLookup:
    return {
        "status": status,
        "message": message,
        "cas": cas,
        "delta_d": None,
        "delta_p": None,
        "delta_h": None,
        "source": None,
        "source_path": source,
    }


def _parse_float(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)) and raw == raw:
        return float(raw)
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_p2oasys_expert_by_cas(path: Path | str) -> dict[str, dict[str, Any]]:
    """
    Load ``p2oasys_single_cas_clean.csv`` (columns: name, CAS, p2oasys_score, date_created).
    Returns digits-only CAS → row dict. Later rows overwrite earlier duplicates.
    """
    out: dict[str, dict[str, Any]] = {}
    path = Path(path)
    if not path.is_file():
        return out
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cas = normalize_cas_for_lookup(row.get("CAS") or row.get("cas") or row.get("primary_cas"))
            if not cas:
                continue
            score = _parse_float(row.get("p2oasys_score") or row.get("evaluation") or row.get("score"))
            if score is None:
                continue
            out[cas] = {
                "name": (row.get("name") or row.get("Solvent Name") or "").strip() or None,
                "score": score,
                "date_created": (row.get("date_created") or "").strip() or None,
                "source_path": str(path),
            }
    return out


def load_chem21_guide_by_cas(path: Path | str) -> dict[str, dict[str, Any]]:
    """Load published CHEM21 solvent guide CSV (AI4Green / solvent_flashcards)."""
    out: dict[str, dict[str, Any]] = {}
    path = Path(path)
    if not path.is_file():
        return out
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cas = normalize_cas_for_lookup(row.get("CAS") or row.get("cas"))
            if not cas:
                continue
            slim = {k: v for k, v in row.items() if k and k not in _SKIP_CHEM21_COLUMNS}
            out[cas] = {
                "solvent": (slim.get("Solvent") or "").strip() or None,
                "family": (slim.get("Family") or "").strip() or None,
                "safety": _parse_float(slim.get("Safety")),
                "health": _parse_float(slim.get("Health")),
                "env": _parse_float(slim.get("Env")),
                "ranking_default": (slim.get("Ranking Default") or "").strip() or None,
                "ranking_discussion": (slim.get("Ranking Discussion") or "").strip() or None,
                "source_path": str(path),
            }
    return out


def load_hsp_csv_by_cas(path: Path | str) -> dict[str, dict[str, Any]]:
    """
    Load Hansen parameters from a CAS-keyed CSV (HSPiP CLI cache or similar).

    Accepted column names: cas/CAS; D/dD/delta_d/δd; P/dP/delta_p; H/dH/delta_h.
    """
    out: dict[str, dict[str, Any]] = {}
    path = Path(path)
    if not path.is_file():
        return out
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cas = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
            if not cas:
                continue
            d_val = _first_float(row, ("D", "dD", "delta_d", "deltaD", "δd", "d"))
            p_val = _first_float(row, ("P", "dP", "delta_p", "deltaP", "δp", "p"))
            h_val = _first_float(row, ("H", "dH", "delta_h", "deltaH", "δh", "h"))
            if d_val is None and p_val is None and h_val is None:
                continue
            src = str(row.get("source") or row.get("Source") or "hspip_cache").strip() or "hspip_cache"
            out[cas] = {
                "delta_d": d_val,
                "delta_p": p_val,
                "delta_h": h_val,
                "source": src,
                "source_path": str(path),
                "rer": _parse_float(row.get("RER") or row.get("rer")),
                "smiles": (str(row.get("smiles") or row.get("SMILES") or "").strip() or None),
            }
    return out


def _first_float(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for k in keys:
        if k in row:
            val = _parse_float(row.get(k))
            if val is not None:
                return val
    lower = {str(k).strip().lower(): v for k, v in row.items() if k}
    for k in keys:
        val = _parse_float(lower.get(k.lower()))
        if val is not None:
            return val
    return None


def load_doss_hsp_by_cas(xlsx_path: Path | str, sheet: str = "DoSS original datapoints") -> dict[str, dict[str, Any]]:
    """Hansen D/P/H from DoSS original datapoints (header row 1)."""
    path = Path(xlsx_path)
    if not path.is_file():
        return {}
    try:
        import pandas as pd
    except ImportError:
        return {}
    try:
        df = pd.read_excel(path, sheet_name=sheet, header=1)
    except Exception:
        return {}
    if "CAS" not in df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        cas = normalize_cas_for_lookup(str(row.get("CAS") or ""))
        if not cas:
            continue
        d_val = _parse_float(row.get("D"))
        p_val = _parse_float(row.get("P"))
        h_val = _parse_float(row.get("H"))
        if d_val is None and p_val is None and h_val is None:
            continue
        out[cas] = {
            "delta_d": d_val,
            "delta_p": p_val,
            "delta_h": h_val,
            "source": "DoSS",
            "source_path": str(path),
        }
    return out


def lookup_p2oasys_expert(
    cas: str | None,
    table: dict[str, dict[str, Any]] | None,
    *,
    source_path: str | None = None,
) -> P2OASysLookup:
    cas_disp = (cas or "").strip()
    cas_key = normalize_cas_for_lookup(cas)
    if table is None:
        return _empty_p2oasys(
            cas_disp,
            "source_unavailable",
            "P2OASys reference database is not configured.",
            source_path,
        )
    if not cas_key:
        return _empty_p2oasys(cas_disp, "not_found", P2OASYS_ASSESSMENT_REQUIRED, source_path)
    hit = table.get(cas_key)
    if not hit:
        return _empty_p2oasys(cas_disp, "not_found", P2OASYS_ASSESSMENT_REQUIRED, source_path)
    return {
        "status": "found",
        "message": "Expert P2OASys score from TURI database (single-CAS).",
        "cas": cas_disp,
        "name": hit.get("name"),
        "score": hit.get("score"),
        "date_created": hit.get("date_created"),
        "source_path": hit.get("source_path") or source_path,
        "single_substance": True,
    }


def lookup_chem21(
    cas: str | None,
    table: dict[str, dict[str, Any]] | None,
    *,
    source_path: str | None = None,
    pubchem: dict[str, Any] | None = None,
    hazard_data: dict[str, Any] | None = None,
    allow_estimate: bool = True,
    estimate_mode: str = "conservative",
) -> Chem21Lookup:
    """Always reconstruct CHEM21 via Prat-style engine when PubChem/hazard data exist.

    The published 53-solvent guide is no longer the primary ranking source.
    ``table`` / ``source_path`` are retained for API compatibility only.
    """
    cas_disp = (cas or "").strip()
    _ = table, source_path  # guide unused for ranking

    miss = _empty_chem21(
        cas_disp,
        "not_found",
        "CHEM21 ranking needs PubChem GHS/physchem data — run hazard retrieval first.",
        "utils/chem21_engine.py",
    )

    if not allow_estimate:
        return miss
    if not pubchem and not hazard_data:
        return miss

    try:
        from utils.chem21_from_pubchem import estimate_chem21
    except Exception:
        return miss

    try:
        est = estimate_chem21(pubchem, hazard_data=hazard_data, mode=estimate_mode)
    except Exception as exc:
        out = dict(miss)
        out["message"] = f"CHEM21 reconstruction failed: {exc}"
        return out  # type: ignore[return-value]

    ranking = est.get("ranking_default")
    has_scores = any(est.get(k) is not None for k in ("safety", "health", "env"))
    if not has_scores and not ranking:
        return miss

    return {
        "status": "estimated",
        "message": str(
            est.get("message")
            or "CHEM21 reconstructed ranking (Prat et al. algorithm via PubChem GHS/physchem)."
        ),
        "cas": cas_disp,
        "solvent": None,
        "family": None,
        "safety": est.get("safety"),
        "health": est.get("health"),
        "env": est.get("env"),
        "ranking_default": ranking,
        "ranking_discussion": est.get("ranking_discussion"),
        "source_path": "utils/chem21_engine.py",
    }


def _is_hspip_cli_source(source: str | None) -> bool:
    s = (source or "").strip().lower()
    return s in {"hspip_cli", "hspip", "computed", "cli"} or s.startswith("hspip_cli")


def _is_doss_like_source(source: str | None) -> bool:
    s = (source or "").strip().lower()
    return s in {"doss", "doss_bootstrap"} or "doss" in s


def lookup_hsp(
    cas: str | None,
    *,
    doss_table: dict[str, dict[str, Any]] | None = None,
    hspip_table: dict[str, dict[str, Any]] | None = None,
) -> HspLookup:
    """HSPiP-only Hansen lookup (cache / CLI). DoSS is ignored."""
    _ = doss_table  # retained for call-site compatibility; never used
    cas_disp = (cas or "").strip()
    cas_key = normalize_cas_for_lookup(cas)
    if not cas_key:
        return _empty_hsp(cas_disp, "not_found", "Enter a CAS to look up Hansen parameters.")

    def _found(hit: dict[str, Any], *, note: str | None = None) -> HspLookup:
        src = hit.get("source")
        return {
            "status": "found",
            "message": note or "Hansen parameters from HSPiP.",
            "cas": cas_disp,
            "delta_d": hit.get("delta_d"),
            "delta_p": hit.get("delta_p"),
            "delta_h": hit.get("delta_h"),
            "source": src,
            "source_path": hit.get("source_path"),
            "needs_hspip_override": False,
        }

    cache_hit = (hspip_table or {}).get(cas_key) if hspip_table else None
    if cache_hit and _is_hspip_cli_source(cache_hit.get("source")):
        return _found(cache_hit, note="Hansen from HSPiP CLI cache.")
    if cache_hit and not _is_doss_like_source(cache_hit.get("source")):
        # Non-DoSS cache row (e.g. prior CLI without tagged source)
        if all(cache_hit.get(k) is not None for k in ("delta_d", "delta_p", "delta_h")):
            return _found(cache_hit, note="Hansen from HSPiP cache.")

    if not hspip_table:
        return _empty_hsp(
            cas_disp,
            "source_unavailable",
            "No HSPiP cache loaded. Configure HSPIP_INSTALL_DIR / HSPIP_CACHE_CSV_PATH, then Run HSPiP.",
        )
    return _empty_hsp(
        cas_disp,
        "not_found",
        "No HSPiP Hansen row for this CAS — use **Run HSPiP for this CAS**.",
    )

def ensure_hspip_for_cas(cas: str, *, force: bool = False) -> HspLookup:
    """Mini cas-to-HSPiP_data routine for one CAS; writes ``hsp_by_cas.csv`` and returns lookup.

    If *force* is True, recompute even when a DoSS-bootstrap row exists.
    True ``hspip_cli`` rows are kept unless *force* is True.
    """
    cas_disp = (cas or "").strip()
    try:
        from v7 import hspip_expand
    except Exception as exc:
        return _empty_hsp(cas_disp, "source_unavailable", f"HSPiP expand module unavailable: {exc}")

    try:
        summary = hspip_expand.expand_hsp_for_cas_list(
            [cas_disp],
            skip_existing=not force,
            overwrite_non_cli=True,
        )
    except Exception as exc:
        return _empty_hsp(cas_disp, "source_unavailable", f"HSPiP run failed: {exc}")

    # Reload cache row
    cache_path = hspip_expand.hsp_cache_path()
    table = load_hsp_csv_by_cas(cache_path)
    hit = table.get(normalize_cas_for_lookup(cas))
    if hit and _is_hspip_cli_source(hit.get("source")):
        return {
            "status": "found",
            "message": "Hansen from HSPiP CLI (just computed / cache).",
            "cas": cas_disp,
            "delta_d": hit.get("delta_d"),
            "delta_p": hit.get("delta_p"),
            "delta_h": hit.get("delta_h"),
            "source": hit.get("source"),
            "source_path": hit.get("source_path") or str(cache_path),
            "needs_hspip_override": False,
        }
    # Surface expand error if any
    err = next((r for r in summary.results if r.status == "error"), None)
    if err:
        return _empty_hsp(cas_disp, "source_unavailable", err.message or "HSPiP expand error")
    if hit:
        return lookup_hsp(cas, hspip_table=table, doss_table=None)
    return _empty_hsp(cas_disp, "not_found", "HSPiP ran but no Hansen row was written.")


def resolve_lookup_tables(
    *,
    p2oasys_csv: Path | str | None,
    chem21_csv: Path | str | None,
    doss_xlsx: Path | str | None = None,
    hspip_csv: Path | str | None = None,
) -> dict[str, Any]:
    """Load whatever files exist. Missing paths yield empty tables (not errors)."""
    p2_path = Path(p2oasys_csv) if p2oasys_csv else None
    c21_path = Path(chem21_csv) if chem21_csv else None
    doss_path = Path(doss_xlsx) if doss_xlsx else None
    hsp_path = Path(hspip_csv) if hspip_csv else None
    return {
        "p2oasys": load_p2oasys_expert_by_cas(p2_path) if p2_path and p2_path.is_file() else None,
        "p2oasys_path": str(p2_path) if p2_path and p2_path.is_file() else None,
        "chem21": load_chem21_guide_by_cas(c21_path) if c21_path and c21_path.is_file() else None,
        "chem21_path": str(c21_path) if c21_path and c21_path.is_file() else None,
        "doss_hsp": load_doss_hsp_by_cas(doss_path) if doss_path and doss_path.is_file() else None,
        "hspip": load_hsp_csv_by_cas(hsp_path) if hsp_path and hsp_path.is_file() else None,
    }
