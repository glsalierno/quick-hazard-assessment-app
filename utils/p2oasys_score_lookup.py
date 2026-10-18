"""
CAS-keyed P2OASys expert + auto score lookup (SQLite).

Built by ``scripts/build_p2oasys_score_lookup.py`` from the GHhaz5 expert-vs-auto
panel and ``p2oasys_single_cas_clean.csv``. Used by the P2OASys Assessment page
to show a score ribbon and skip redundant TCI SDS fetches when scores are known.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

from utils.lookup_tables import normalize_cas_for_lookup

# Display CAS form for UI (digits-only key → hyphenated when possible).
def format_cas_display(cas: str | None) -> str:
    digits = normalize_cas_for_lookup(cas)
    if not digits:
        return (cas or "").strip()
    if len(digits) >= 5:
        return f"{digits[:-3]}-{digits[-3:-1]}-{digits[-1]}"
    return digits


def harvest_db_path() -> Path:
    try:
        import config

        raw = getattr(config, "P2OASYS_HARVEST_DB", None)
        if raw:
            return Path(raw)
    except Exception:
        pass
    env = (os.environ.get("P2OASYS_HARVEST_DB") or "").strip()
    if env:
        return Path(env)
    root = Path(__file__).resolve().parent.parent
    return root.parent.parent.parent / "fastP2OASys" / "p2oasys_harvest.sqlite"


def default_db_path() -> Path:
    try:
        import config

        raw = getattr(config, "P2OASYS_SCORE_LOOKUP_DB", None)
        if raw:
            return Path(raw)
    except Exception:
        pass
    env = (os.environ.get("P2OASYS_SCORE_LOOKUP_DB") or "").strip()
    if env:
        return Path(env)
    # utils/ → app root / data
    return Path(__file__).resolve().parent.parent / "data" / "p2oasys_score_lookup.sqlite"


_ROW_COLUMNS = (
    "cas",
    "name_expert",
    "name_auto",
    "expert_acute",
    "expert_chronic",
    "expert_ecological",
    "expert_fate",
    "expert_atmospheric",
    "expert_physical",
    "expert_process",
    "expert_life_cycle",
    "expert_overall",
    "auto_acute",
    "auto_chronic",
    "auto_ecological",
    "auto_fate",
    "auto_atmospheric",
    "auto_physical",
    "auto_overall",
    "has_expert",
    "has_auto",
    "tci_product",
    "sds_tci_ok",
    "tci_skip_reason",
    "auto_scorer_version",
    "auto_sources",
    "expert_source",
    "updated_at",
)

EXPERT_CATEGORY_KEYS = (
    ("expert_acute", "Acute"),
    ("expert_chronic", "Chronic"),
    ("expert_ecological", "Ecological"),
    ("expert_fate", "Fate & Transport"),
    ("expert_atmospheric", "Atmospheric"),
    ("expert_physical", "Physical"),
    ("expert_process", "Process"),
    ("expert_life_cycle", "Life Cycle"),
)

AUTO_CATEGORY_KEYS = (
    ("auto_acute", "Acute"),
    ("auto_chronic", "Chronic"),
    ("auto_ecological", "Ecological"),
    ("auto_fate", "Fate & Transport"),
    ("auto_atmospheric", "Atmospheric"),
    ("auto_physical", "Physical"),
)


def _connect(db_path: Path | str | None = None) -> sqlite3.Connection | None:
    path = Path(db_path) if db_path else default_db_path()
    if not path.is_file():
        return None
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def lookup(cas: str | None, *, db_path: Path | str | None = None) -> dict[str, Any] | None:
    """Return score row for CAS, or None if DB missing / CAS not present."""
    key = normalize_cas_for_lookup(cas)
    if not key:
        return None
    conn = _connect(db_path)
    if conn is None:
        return None
    try:
        # Match digits-only or hyphenated CAS stored in DB.
        disp = format_cas_display(key)
        row = conn.execute(
            "SELECT * FROM by_cas WHERE cas = ? OR REPLACE(cas, '-', '') = ? LIMIT 1",
            (disp, key),
        ).fetchone()
        if row is None:
            # Also try raw digits as stored
            row = conn.execute(
                "SELECT * FROM by_cas WHERE REPLACE(cas, '-', '') = ? LIMIT 1",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return {c: row[c] if c in row.keys() else None for c in _ROW_COLUMNS if c in row.keys()}
    finally:
        conn.close()


def has_cached_scores(cas: str | None, *, db_path: Path | str | None = None) -> bool:
    """True when expert or auto category/overall scores are present for CAS."""
    row = lookup(cas, db_path=db_path)
    if not row:
        return False
    return bool(row.get("has_expert") or row.get("has_auto"))


def should_skip_tci(cas: str | None, *, db_path: Path | str | None = None) -> bool:
    """
    True when cached expert or auto scores are enough to show the ribbon
    without a live TCI SDS fetch. Explicit user override can still fetch SDS.
    """
    return has_cached_scores(cas, db_path=db_path)



from datetime import datetime, timezone

# Live-draft matrix category names -> auto_* columns. Process/LC never stored as auto.
_AUTO_CATEGORY_TO_COL: dict[str, str] = {
    "Acute Human Effects": "auto_acute",
    "Chronic Human Effects": "auto_chronic",
    "Ecological Hazards": "auto_ecological",
    "Environmental Fate & Transport": "auto_fate",
    "Environmental Fate and Transport": "auto_fate",
    "Atmospheric Hazard": "auto_atmospheric",
    "Physical Properties": "auto_physical",
}

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS by_cas (
  cas TEXT PRIMARY KEY,
  name_expert TEXT,
  name_auto TEXT,
  expert_acute REAL,
  expert_chronic REAL,
  expert_ecological REAL,
  expert_fate REAL,
  expert_atmospheric REAL,
  expert_physical REAL,
  expert_process REAL,
  expert_life_cycle REAL,
  expert_overall REAL,
  auto_acute REAL,
  auto_chronic REAL,
  auto_ecological REAL,
  auto_fate REAL,
  auto_atmospheric REAL,
  auto_physical REAL,
  auto_overall REAL,
  has_expert INTEGER NOT NULL DEFAULT 0,
  has_auto INTEGER NOT NULL DEFAULT 0,
  tci_product TEXT,
  sds_tci_ok INTEGER,
  tci_skip_reason TEXT,
  auto_scorer_version TEXT,
  auto_sources TEXT,
  expert_source TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS subcat_scores (
  cas TEXT NOT NULL,
  source TEXT NOT NULL,
  category TEXT NOT NULL,
  subcategory TEXT NOT NULL,
  score REAL NOT NULL,
  PRIMARY KEY (cas, source, category, subcategory)
);
CREATE INDEX IF NOT EXISTS idx_subcat_cas_source ON subcat_scores(cas, source);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _connect_rw(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA_SQL)
    return conn


def category_scores_to_auto_cols(scores: dict[str, Any] | None) -> dict[str, float | None]:
    """Map scorer category bundles to auto_* columns (six only)."""
    out: dict[str, float | None] = {
        "auto_acute": None,
        "auto_chronic": None,
        "auto_ecological": None,
        "auto_fate": None,
        "auto_atmospheric": None,
        "auto_physical": None,
    }
    if not isinstance(scores, dict):
        return out
    for cat, bundle in scores.items():
        col = _AUTO_CATEGORY_TO_COL.get(str(cat).strip())
        if not col:
            continue
        val = None
        if isinstance(bundle, dict):
            raw = bundle.get("_category_max")
            if isinstance(raw, (int, float)) and raw == raw:
                val = float(raw)
        elif isinstance(bundle, (int, float)) and bundle == bundle:
            val = float(bundle)
        out[col] = val
    return out


def upsert_auto_scores(
    cas: str,
    *,
    name: str | None = None,
    auto_cols: dict[str, float | None] | None = None,
    auto_overall: float | None = None,
    sources: str | None = None,
    scorer_version: str | None = None,
    tci_product: str | None = None,
    db_path: Path | str | None = None,
) -> dict[str, Any] | None:
    """Insert or update auto six-category scores without wiping expert columns.

    Existing CAS: only auto_* / name_auto / sources / version / tci_product change.
    New CAS: inserted with has_expert=0.
    """
    key_digits = normalize_cas_for_lookup(cas)
    if not key_digits:
        return None
    auto_cols = auto_cols or {}
    has_auto = int(
        any(
            auto_cols.get(c) is not None
            for c in (
                "auto_acute",
                "auto_chronic",
                "auto_ecological",
                "auto_fate",
                "auto_atmospheric",
                "auto_physical",
            )
        )
        or (auto_overall is not None)
    )
    conn = _connect_rw(db_path)
    try:
        disp = format_cas_display(key_digits)
        existing = conn.execute(
            "SELECT cas FROM by_cas WHERE cas = ? OR REPLACE(cas, '-', '') = ? LIMIT 1",
            (disp, key_digits),
        ).fetchone()
        pk = existing["cas"] if existing else disp
        now = _now_iso()
        if existing:
            conn.execute(
                """
                UPDATE by_cas SET
                  name_auto = COALESCE(?, name_auto),
                  auto_acute = ?,
                  auto_chronic = ?,
                  auto_ecological = ?,
                  auto_fate = ?,
                  auto_atmospheric = ?,
                  auto_physical = ?,
                  auto_overall = ?,
                  has_auto = ?,
                  auto_sources = COALESCE(?, auto_sources),
                  auto_scorer_version = COALESCE(?, auto_scorer_version),
                  tci_product = COALESCE(?, tci_product),
                  tci_skip_reason = CASE
                    WHEN has_expert = 1 OR ? = 1 THEN 'cached_scores'
                    ELSE tci_skip_reason
                  END,
                  updated_at = ?
                WHERE cas = ?
                """,
                (
                    (name or None),
                    auto_cols.get("auto_acute"),
                    auto_cols.get("auto_chronic"),
                    auto_cols.get("auto_ecological"),
                    auto_cols.get("auto_fate"),
                    auto_cols.get("auto_atmospheric"),
                    auto_cols.get("auto_physical"),
                    auto_overall,
                    has_auto,
                    sources,
                    scorer_version,
                    tci_product,
                    has_auto,
                    now,
                    pk,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO by_cas (
                  cas, name_auto,
                  auto_acute, auto_chronic, auto_ecological,
                  auto_fate, auto_atmospheric, auto_physical, auto_overall,
                  has_expert, has_auto, auto_sources, auto_scorer_version,
                  tci_product, tci_skip_reason, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?)
                """,
                (
                    pk,
                    name or None,
                    auto_cols.get("auto_acute"),
                    auto_cols.get("auto_chronic"),
                    auto_cols.get("auto_ecological"),
                    auto_cols.get("auto_fate"),
                    auto_cols.get("auto_atmospheric"),
                    auto_cols.get("auto_physical"),
                    auto_overall,
                    has_auto,
                    sources,
                    scorer_version,
                    tci_product,
                    "cached_scores" if has_auto else None,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return lookup(cas, db_path=db_path)


def upsert_auto_from_draft(draft: dict[str, Any], *, db_path: Path | str | None = None) -> dict[str, Any] | None:
    """Persist a successful Generate-draft result into the lookup DB (upsert)."""
    if not isinstance(draft, dict) or not draft.get("ok"):
        return None
    cas = str(draft.get("cas") or "").strip()
    if not cas:
        return None
    scores = draft.get("scores") or {}
    auto_cols = category_scores_to_auto_cols(scores)
    overall = None
    try:
        from utils import p2oasys_aggregate

        ov = p2oasys_aggregate.aggregate_category_scores(scores, "max")
        if isinstance(ov, (int, float)) and ov == ov:
            overall = float(ov)
    except Exception:
        nums = [v for v in auto_cols.values() if isinstance(v, (int, float))]
        overall = max(nums) if nums else None
    sources = draft.get("sources_used")
    if isinstance(sources, (list, tuple)):
        sources_s = ";".join(str(s) for s in sources if s)
    else:
        sources_s = str(sources) if sources else None
    version = None
    try:
        from utils import p2oasys_scorer

        version = getattr(p2oasys_scorer, "SCORER_VERSION", None) or getattr(
            p2oasys_scorer, "__version__", None
        )
    except Exception:
        version = None
    tci_product = None
    for blob in (draft.get("result"), draft.get("hazard_data"), draft):
        if isinstance(blob, dict):
            tci_product = blob.get("_tci_product") or blob.get("tci_product") or tci_product
    row = upsert_auto_scores(
        cas,
        name=str(draft.get("chemical_name") or "") or None,
        auto_cols=auto_cols,
        auto_overall=overall,
        sources=sources_s,
        scorer_version=str(version) if version else None,
        tci_product=str(tci_product) if tci_product else None,
        db_path=db_path,
    )
    try:
        replace_subcat_scores(cas, "auto", scores, db_path=db_path)
    except Exception:
        pass
    return row


def iter_subcat_cells(scores: dict[str, Any] | None):
    """Yield (category, subcategory, score) from a live-scores-shaped bundle."""
    if not isinstance(scores, dict):
        return
    for cat, bundle in scores.items():
        if not isinstance(bundle, dict):
            continue
        cat_key = str(cat).strip()
        if cat_key == "Environmental Fate and Transport":
            cat_key = "Environmental Fate & Transport"
        for sub, cell in bundle.items():
            if str(sub).startswith("_"):
                continue
            val = None
            if isinstance(cell, dict):
                raw = cell.get("_max")
                if isinstance(raw, (int, float)) and raw == raw:
                    val = float(raw)
            elif isinstance(cell, (int, float)) and cell == cell:
                val = float(cell)
            if val is not None:
                yield cat_key, str(sub).strip(), val


def replace_subcat_scores(
    cas: str,
    source: str,
    scores: dict[str, Any] | None,
    *,
    db_path: Path | str | None = None,
) -> int:
    """Replace all subcategory rows for one CAS + source ('expert' or 'auto')."""
    key_digits = normalize_cas_for_lookup(cas)
    if not key_digits or source not in ("expert", "auto"):
        return 0
    rows = list(iter_subcat_cells(scores))
    conn = _connect_rw(db_path)
    try:
        disp = format_cas_display(key_digits)
        existing = conn.execute(
            "SELECT cas FROM subcat_scores WHERE REPLACE(cas, '-', '') = ? AND source = ? LIMIT 1",
            (key_digits, source),
        ).fetchone()
        pk = existing["cas"] if existing else disp
        conn.execute(
            "DELETE FROM subcat_scores WHERE (cas = ? OR REPLACE(cas, '-', '') = ?) AND source = ?",
            (pk, key_digits, source),
        )
        conn.executemany(
            "INSERT OR REPLACE INTO subcat_scores (cas, source, category, subcategory, score) "
            "VALUES (?, ?, ?, ?, ?)",
            [(pk, source, cat, sub, score) for cat, sub, score in rows],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def load_subcat_bundle(
    cas: str | None,
    source: str,
    *,
    db_path: Path | str | None = None,
) -> dict[str, Any] | None:
    """Rebuild a live-scores-shaped subcategory dict from sqlite, or None."""
    key = normalize_cas_for_lookup(cas)
    if not key or source not in ("expert", "auto"):
        return None
    conn = _connect(db_path)
    if conn is None:
        return None
    try:
        try:
            rows = conn.execute(
                "SELECT category, subcategory, score FROM subcat_scores "
                "WHERE REPLACE(cas, '-', '') = ? AND source = ?",
                (key, source),
            ).fetchall()
        except sqlite3.OperationalError:
            return None
    finally:
        conn.close()
    if not rows:
        return None
    out: dict[str, Any] = {}
    for row in rows:
        cat = str(row["category"])
        sub = str(row["subcategory"])
        score = float(row["score"])
        bundle = out.setdefault(cat, {})
        bundle[sub] = {"_max": score}
    return out


_EXPERT_CAT_TO_COL = {
    "Acute Human Effects": "expert_acute",
    "Chronic Human Effects": "expert_chronic",
    "Ecological Hazards": "expert_ecological",
    "Environmental Fate & Transport": "expert_fate",
    "Atmospheric Hazard": "expert_atmospheric",
    "Physical Properties": "expert_physical",
    "Process Factors": "expert_process",
    "Life Cycle Factors": "expert_life_cycle",
}

_AUTO6_EXPERT_COLS = (
    "expert_acute",
    "expert_chronic",
    "expert_ecological",
    "expert_fate",
    "expert_atmospheric",
    "expert_physical",
)


def upsert_expert_scores(
    cas: str,
    *,
    name: str | None = None,
    expert_cols: dict[str, float | None] | None = None,
    expert_overall: float | None = None,
    expert_source: str | None = None,
    db_path: Path | str | None = None,
) -> dict[str, Any] | None:
    """Insert or update expert 8-category scores without wiping auto columns."""
    key_digits = normalize_cas_for_lookup(cas)
    if not key_digits:
        return None
    expert_cols = expert_cols or {}
    has_expert = int(
        any(expert_cols.get(c) is not None for c in (
            "expert_acute", "expert_chronic", "expert_ecological",
            "expert_fate", "expert_atmospheric", "expert_physical",
            "expert_process", "expert_life_cycle",
        ))
        or expert_overall is not None
    )
    conn = _connect_rw(db_path)
    try:
        disp = format_cas_display(key_digits)
        existing = conn.execute(
            "SELECT cas FROM by_cas WHERE cas = ? OR REPLACE(cas, '-', '') = ? LIMIT 1",
            (disp, key_digits),
        ).fetchone()
        pk = existing["cas"] if existing else disp
        now = _now_iso()
        if existing:
            conn.execute(
                """
                UPDATE by_cas SET
                  name_expert = COALESCE(?, name_expert),
                  expert_acute = ?, expert_chronic = ?, expert_ecological = ?,
                  expert_fate = ?, expert_atmospheric = ?, expert_physical = ?,
                  expert_process = ?, expert_life_cycle = ?, expert_overall = ?,
                  has_expert = ?,
                  expert_source = COALESCE(?, expert_source),
                  tci_skip_reason = CASE
                    WHEN has_auto = 1 OR ? = 1 THEN 'cached_scores'
                    ELSE tci_skip_reason
                  END,
                  updated_at = ?
                WHERE cas = ?
                """,
                (
                    name or None,
                    expert_cols.get("expert_acute"),
                    expert_cols.get("expert_chronic"),
                    expert_cols.get("expert_ecological"),
                    expert_cols.get("expert_fate"),
                    expert_cols.get("expert_atmospheric"),
                    expert_cols.get("expert_physical"),
                    expert_cols.get("expert_process"),
                    expert_cols.get("expert_life_cycle"),
                    expert_overall,
                    has_expert,
                    expert_source,
                    has_expert,
                    now,
                    pk,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO by_cas (
                  cas, name_expert,
                  expert_acute, expert_chronic, expert_ecological,
                  expert_fate, expert_atmospheric, expert_physical,
                  expert_process, expert_life_cycle, expert_overall,
                  has_expert, has_auto, expert_source, tci_skip_reason, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?)
                """,
                (
                    pk, name or None,
                    expert_cols.get("expert_acute"),
                    expert_cols.get("expert_chronic"),
                    expert_cols.get("expert_ecological"),
                    expert_cols.get("expert_fate"),
                    expert_cols.get("expert_atmospheric"),
                    expert_cols.get("expert_physical"),
                    expert_cols.get("expert_process"),
                    expert_cols.get("expert_life_cycle"),
                    expert_overall,
                    has_expert,
                    expert_source,
                    "cached_scores" if has_expert else None,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return lookup(cas, db_path=db_path)


def seed_expert_from_harvest(
    *,
    harvest_db: Path | str | None = None,
    db_path: Path | str | None = None,
    force: bool = False,
) -> int:
    """Copy chosen single-CAS expert subcategory + category scores into lookup sqlite."""
    hpath = Path(harvest_db) if harvest_db else harvest_db_path()
    if not hpath.is_file():
        return 0
    lookup_path = Path(db_path) if db_path else default_db_path()
    hconn = sqlite3.connect(str(hpath))
    hconn.row_factory = sqlite3.Row
    try:
        n_chosen = hconn.execute(
            "SELECT COUNT(*) FROM chemicals WHERE chosen=1"
        ).fetchone()[0]
        if not n_chosen:
            return 0
        cells = hconn.execute(
            "SELECT cas, category, subcategory, score FROM subcat_scores "
            "WHERE source='expert' AND cas IS NOT NULL AND cas != ''"
        ).fetchall()
        chems = hconn.execute(
            "SELECT cas, name, col_id FROM chemicals WHERE chosen=1 AND cas IS NOT NULL"
        ).fetchall()
        cat_max: dict[str, dict[str, float]] = {}
        for row in cells:
            cas = str(row["cas"])
            cat = str(row["category"])
            score = float(row["score"])
            blob = cat_max.setdefault(cas, {})
            prev = blob.get(cat)
            if prev is None or score > prev:
                blob[cat] = score
    finally:
        hconn.close()

    conn = _connect_rw(lookup_path)
    try:
        if not force:
            n = conn.execute(
                "SELECT COUNT(DISTINCT cas) FROM subcat_scores WHERE source='expert'"
            ).fetchone()[0]
            if n and int(n) >= int(n_chosen) * 0.8:
                return int(
                    conn.execute(
                        "SELECT COUNT(*) FROM subcat_scores WHERE source='expert'"
                    ).fetchone()[0]
                )
        conn.execute("DELETE FROM subcat_scores WHERE source='expert'")
        conn.executemany(
            "INSERT OR REPLACE INTO subcat_scores (cas, source, category, subcategory, score) "
            "VALUES (?, 'expert', ?, ?, ?)",
            [(str(r["cas"]), str(r["category"]), str(r["subcategory"]), float(r["score"])) for r in cells],
        )
        conn.commit()
    finally:
        conn.close()

    src_label = "p2oasys.turi.org harvest pages 1-101 (clean single-CAS)"
    for chem in chems:
        cas = str(chem["cas"])
        cols = {c: None for c in (
            "expert_acute", "expert_chronic", "expert_ecological",
            "expert_fate", "expert_atmospheric", "expert_physical",
            "expert_process", "expert_life_cycle",
        )}
        for cat, val in (cat_max.get(cas) or {}).items():
            col = _EXPERT_CAT_TO_COL.get(cat)
            if col:
                cols[col] = val
        nums = [cols[c] for c in _AUTO6_EXPERT_COLS if cols[c] is not None]
        overall = max(nums) if nums else None
        upsert_expert_scores(
            cas,
            name=str(chem["name"] or "") or None,
            expert_cols=cols,
            expert_overall=overall,
            expert_source=src_label,
            db_path=lookup_path,
        )
    return len(cells)


def expert_subcat_json_candidates() -> list[Path]:
    root = Path(__file__).resolve().parent.parent / "data"
    return [
        root / "expert_subcat_by_cas_priority62.json",
        root / "expert_subcat_by_cas.json",
    ]


def ensure_expert_subcat_seeded(*, db_path: Path | str | None = None, force: bool = False) -> int:
    """Import expert subcategory scores: full harvest sqlite first, else 62-CAS JSON."""
    n_harvest = seed_expert_from_harvest(db_path=db_path, force=force)
    if n_harvest:
        return n_harvest
    conn = _connect_rw(db_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM subcat_scores WHERE source = 'expert'"
        ).fetchone()[0]
        if n and not force:
            return int(n)
    finally:
        conn.close()
    path = next((p for p in expert_subcat_json_candidates() if p.is_file()), None)
    if path is None:
        return 0
    import json

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    if not isinstance(data, dict):
        return 0
    total = 0
    for cas_key, bundle in data.items():
        if isinstance(bundle, dict):
            total += replace_subcat_scores(str(cas_key), "expert", bundle, db_path=db_path)
    return total


def db_stats(db_path: Path | str | None = None) -> dict[str, int]:
    conn = _connect(db_path)
    empty = {
        "n_cas": 0,
        "n_with_expert": 0,
        "n_with_auto": 0,
        "n_with_both": 0,
        "n_with_tci_product": 0,
        "n_expert_subcat_cas": 0,
        "n_auto_subcat_cas": 0,
        "n_expert_subcat_cells": 0,
    }
    if conn is None:
        return empty
    try:
        cur = conn.execute(
            """
            SELECT
              COUNT(*) AS n_cas,
              SUM(CASE WHEN has_expert = 1 THEN 1 ELSE 0 END) AS n_with_expert,
              SUM(CASE WHEN has_auto = 1 THEN 1 ELSE 0 END) AS n_with_auto,
              SUM(CASE WHEN has_expert = 1 AND has_auto = 1 THEN 1 ELSE 0 END) AS n_with_both,
              SUM(CASE WHEN tci_product IS NOT NULL AND TRIM(tci_product) != '' THEN 1 ELSE 0 END) AS n_with_tci_product
            FROM by_cas
            """
        )
        row = cur.fetchone()
        out = {k: int(row[k] or 0) for k in row.keys()}
        try:
            out["n_expert_subcat_cas"] = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT cas) FROM subcat_scores WHERE source='expert'"
                ).fetchone()[0]
                or 0
            )
            out["n_auto_subcat_cas"] = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT cas) FROM subcat_scores WHERE source='auto'"
                ).fetchone()[0]
                or 0
            )
            out["n_expert_subcat_cells"] = int(
                conn.execute(
                    "SELECT COUNT(*) FROM subcat_scores WHERE source='expert'"
                ).fetchone()[0]
                or 0
            )
        except sqlite3.OperationalError:
            out.update(
                n_expert_subcat_cas=0,
                n_auto_subcat_cas=0,
                n_expert_subcat_cells=0,
            )
        return out
    finally:
        conn.close()
