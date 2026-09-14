"""
SQLite cache for full OPERA batch rows keyed by CAS + SMILES hash.

Used to avoid repeated MATLAB/OPERA startup during validation or large lists.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd


def default_precompute_db_path() -> Path:
    try:
        import config as _cfg

        raw = (getattr(_cfg, "OPERA_PRECOMPUTE_DB_PATH", None) or "").strip()
        if raw:
            return Path(raw)
        return Path(getattr(_cfg, "DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "opera_precompute.sqlite"
    except Exception:
        return Path(__file__).resolve().parents[1] / "data" / "opera_precompute.sqlite"


def _smiles_hash(smiles: str) -> str:
    return hashlib.sha256((smiles or "").encode("utf-8")).hexdigest()


def _connect(path: Path | str) -> sqlite3.Connection:
    """Open SQLite with WAL and a long busy timeout for concurrent writers."""
    p = Path(path)
    con = sqlite3.connect(str(p), timeout=120.0)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=60000")
    except Exception:
        pass
    return con


def init_db(path: Path | str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    con = _connect(p)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS opera_cas_result (
                cas TEXT PRIMARY KEY,
                smiles TEXT NOT NULL,
                smiles_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                updated REAL NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS opera_predictions (
                cas TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                value TEXT,
                unit TEXT,
                smiles TEXT,
                smiles_hash TEXT NOT NULL,
                updated REAL NOT NULL,
                PRIMARY KEY (cas, endpoint)
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_opera_pred_cas ON opera_predictions(cas)")
        con.commit()
    finally:
        con.close()


def is_cas_cached(path: Path | str, cas: str, smiles: str) -> bool:
    """True if we have a stored row for this CAS with the same SMILES hash."""
    cas_n = (cas or "").strip()
    sm = (smiles or "").strip()
    if not cas_n or not sm:
        return False
    sh = _smiles_hash(sm)
    p = Path(path)
    if not p.is_file():
        return False
    con = _connect(p)
    try:
        row = con.execute(
            "SELECT smiles_hash FROM opera_cas_result WHERE cas = ?",
            (cas_n,),
        ).fetchone()
    finally:
        con.close()
    if not row:
        return False
    return str(row[0]) == sh


def get_cas_row(path: Path | str, cas: str, smiles: str = "") -> dict[str, Any] | None:
    """
    Return the stored OPERA output row dict for CAS, or None.

    If ``smiles`` is provided, require matching smiles_hash (same rule as ``is_cas_cached``).
    """
    cas_n = (cas or "").strip()
    if not cas_n:
        return None
    p = Path(path)
    if not p.is_file():
        return None
    sh = _smiles_hash(smiles) if (smiles or "").strip() else None
    con = _connect(p)
    try:
        row = con.execute(
            "SELECT smiles_hash, result_json FROM opera_cas_result WHERE cas = ?",
            (cas_n,),
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None
    if sh is not None and str(row[0]) != sh:
        return None
    try:
        data = json.loads(row[1])
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def put_cas_row(path: Path | str, cas: str, smiles: str, row_dict: dict[str, Any]) -> None:
    """Store one OPERA output row (flat dict) for CAS; refresh denormalized endpoint rows."""
    cas_n = (cas or "").strip()
    sm = (smiles or "").strip()
    if not cas_n or not sm:
        return
    sh = _smiles_hash(sm)
    payload = {k: v for k, v in row_dict.items() if not str(k).startswith("_")}
    now = time.time()
    blob = json.dumps(payload, ensure_ascii=False, default=str)
    p = Path(path)
    init_db(p)
    delay = 0.05
    for attempt in range(10):
        con = _connect(p)
        try:
            con.execute(
                """
                INSERT INTO opera_cas_result (cas, smiles, smiles_hash, result_json, updated)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cas) DO UPDATE SET
                    smiles=excluded.smiles,
                    smiles_hash=excluded.smiles_hash,
                    result_json=excluded.result_json,
                    updated=excluded.updated
                """,
                (cas_n, sm, sh, blob, now),
            )
            con.execute("DELETE FROM opera_predictions WHERE cas = ?", (cas_n,))
            for k, v in payload.items():
                if k in ("input_smiles", "molecule_id", "MoleculeID"):
                    continue
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    continue
                con.execute(
                    """
                    INSERT INTO opera_predictions (cas, endpoint, value, unit, smiles, smiles_hash, updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cas, endpoint) DO UPDATE SET
                        value=excluded.value,
                        unit=excluded.unit,
                        smiles=excluded.smiles,
                        smiles_hash=excluded.smiles_hash,
                        updated=excluded.updated
                    """,
                    (cas_n, str(k), str(v), "", sm, sh, now),
                )
            con.commit()
            return
        except sqlite3.OperationalError:
            try:
                con.rollback()
            except Exception:
                pass
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
        finally:
            con.close()
    raise sqlite3.OperationalError("SQLite write retries exhausted in put_cas_row")


def put_batch_dataframe(path: Path | str, df: pd.DataFrame) -> int:
    """Persist each row that has ``_opera_cache_cas`` and ``input_smiles``. Returns rows written."""
    if df.empty or "_opera_cache_cas" not in df.columns:
        return 0
    n = 0
    for _, row in df.iterrows():
        cas = str(row.get("_opera_cache_cas") or "").strip()
        sm = str(row.get("input_smiles") or "").strip()
        if not cas or not sm:
            continue
        put_cas_row(path, cas, sm, row.to_dict())
        n += 1
    return n


def get_opera_value(path: Path | str, cas: str, endpoint: str) -> str | None:
    """Return raw string value for endpoint column, or None."""
    cas_n = (cas or "").strip()
    ep = (endpoint or "").strip()
    if not cas_n or not ep:
        return None
    p = Path(path)
    if not p.is_file():
        return None
    con = _connect(p)
    try:
        row = con.execute(
            "SELECT value FROM opera_predictions WHERE cas = ? AND endpoint = ?",
            (cas_n, ep),
        ).fetchone()
        if row and row[0] is not None and str(row[0]).strip() != "":
            return str(row[0])
        row2 = con.execute(
            "SELECT result_json FROM opera_cas_result WHERE cas = ?",
            (cas_n,),
        ).fetchone()
    finally:
        con.close()
    if not row2:
        return None
    try:
        data = json.loads(row2[0])
        v = data.get(ep)
        if v is None:
            return None
        return str(v)
    except Exception:
        return None


def clear_cache(path: Path | str) -> bool:
    """
    Remove the precompute SQLite file entirely (fresh start).

    Returns True if a file was removed. Prefer this only on a dedicated cache path
    (``OPERA_PRECOMPUTE_DB_PATH``), not on ``chemical_db.sqlite``.
    """
    p = Path(path)
    if not p.is_file():
        return False
    try:
        p.unlink()
        return True
    except OSError:
        return False


def count_cached_cas(path: Path | str) -> int:
    p = Path(path)
    if not p.is_file():
        return 0
    con = _connect(p)
    try:
        n = con.execute("SELECT COUNT(*) FROM opera_cas_result").fetchone()[0]
    finally:
        con.close()
    return int(n or 0)
