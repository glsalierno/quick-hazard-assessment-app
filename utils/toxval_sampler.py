"""
Sample CAS identifiers from ToxVal (SQLite ``toxvaldb``) for OPERA endpoint validation.

Uses ``opera_to_toxval_mapping.json``-style mapping: OPERA column name -> ``toxval_types``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


def _normalize_cas(c: str) -> str:
    return (c or "").strip()


def resolve_opera_endpoint_keys(endpoint_query: str, mapping: dict[str, Any]) -> list[str]:
    """
    Map a user shorthand (e.g. ``LogP``, ``LD50``) to OPERA JSON / CSV column keys present in ``mapping``.
    """
    q = (endpoint_query or "").strip()
    if not q:
        return []
    keys = [k for k in mapping if isinstance(k, str) and k]
    if q in keys:
        return [q]
    cand = f"{q}_pred" if not q.endswith("_pred") else q
    if cand in keys:
        return [cand]
    ql = q.lower()
    if ql in ("ld50", "oral_ld50", "acute_ld50"):
        for pref in ("CATMoS_LD50_pred",):
            if pref in keys:
                return [pref]
    if ql in ("logp", "log_p", "kow"):
        for pref in ("LogP_pred",):
            if pref in keys:
                return [pref]
    hits = [k for k in keys if ql in k.lower()]
    return sorted(set(hits))[:3]


def _cas_with_numeric_for_types(
    toxval_db_path: Path,
    study_types_lower: list[str],
    *,
    limit: int,
    shuffle: bool,
) -> list[str]:
    if not toxval_db_path.is_file() or not study_types_lower:
        return []
    con = sqlite3.connect(toxval_db_path)
    try:
        marks = ",".join("?" for _ in study_types_lower)
        order = "ORDER BY RANDOM()" if shuffle else "ORDER BY casrn, rowid"
        q = f"""
            SELECT DISTINCT TRIM(CAST(casrn AS TEXT)) AS cas
            FROM toxvaldb
            WHERE LOWER(TRIM(study_type)) IN ({marks})
              AND toxval_numeric IS NOT NULL
              AND CAST(toxval_numeric AS TEXT) != ''
              AND casrn IS NOT NULL
              AND LENGTH(TRIM(CAST(casrn AS TEXT))) > 0
            {order}
            LIMIT ?
        """
        df = pd.read_sql_query(q, con, params=list(study_types_lower) + [int(limit)])
    finally:
        con.close()
    out = [_normalize_cas(x) for x in df["cas"].dropna().astype(str).tolist()]
    return [c for c in out if c]


def sample_cas_for_endpoints(
    toxval_db_path: Path | str,
    mapping: dict[str, Any],
    endpoints_list: list[str],
    *,
    n_per_endpoint: int = 100,
    shuffle: bool = True,
) -> set[str]:
    """
    For each requested endpoint, collect CAS that have numeric ToxVal rows for mapped ``study_type`` values.

    ``mapping`` should be the inner ``mapping`` dict from ``opera_to_toxval_mapping.json``
    (keys = OPERA column names like ``LogP_pred``).
    """
    p = Path(toxval_db_path)
    out: set[str] = set()
    n = max(1, int(n_per_endpoint))
    for raw_ep in endpoints_list:
        keys = resolve_opera_endpoint_keys(str(raw_ep), mapping)
        ep_key = None
        for k in keys:
            if (mapping.get(k) or {}).get("toxval_types"):
                ep_key = k
                break
        if not ep_key:
            continue
        payload = mapping.get(ep_key) or {}
        types = [str(t).strip().lower() for t in (payload.get("toxval_types") or []) if str(t).strip()]
        if not types:
            continue
        cas_block = _cas_with_numeric_for_types(p, types, limit=n, shuffle=shuffle)
        for c in cas_block:
            out.add(c)
    return out


def get_common_cas_across_endpoints(
    toxval_db_path: Path | str,
    mapping: dict[str, Any],
    endpoints_list: list[str],
    *,
    pool_limit: int = 5000,
    shuffle: bool = False,
) -> set[str]:
    """
    CAS that have at least one numeric ToxVal row for **every** resolved endpoint in ``endpoints_list``.

    Uses a larger per-endpoint distinct-CAS pool (``pool_limit``) then set intersection.
    """
    p = Path(toxval_db_path)
    sets_list: list[set[str]] = []
    for raw_ep in endpoints_list:
        keys = resolve_opera_endpoint_keys(str(raw_ep), mapping)
        ep_key = None
        for k in keys:
            if (mapping.get(k) or {}).get("toxval_types"):
                ep_key = k
                break
        if not ep_key:
            continue
        payload = mapping.get(ep_key) or {}
        types = [str(t).strip().lower() for t in (payload.get("toxval_types") or []) if str(t).strip()]
        if not types:
            continue
        cas_block = _cas_with_numeric_for_types(p, types, limit=int(pool_limit), shuffle=shuffle)
        sets_list.append(set(cas_block))
    if len(sets_list) < 2:
        return set()
    out = sets_list[0].copy()
    for s in sets_list[1:]:
        out &= s
    return out


def load_mapping_dict(mapping_json_path: Path | str) -> dict[str, Any]:
    from utils.opera_mapper import load_mapping

    data = load_mapping(Path(mapping_json_path))
    return data.get("mapping") or {}
