#!/usr/bin/env python3
"""
Build / rebuild ``data/p2oasys_score_lookup.sqlite`` from authoritative CSVs.

Primary fill: GHhaz5 ``report_expert_vs_auto_smiles_subset.csv`` (expert 8 cats +
auto 6 cats). Expert overall / name joined from ``p2oasys_single_cas_clean.csv``.

When ``p2oasys_score`` is missing, expert_overall = mean of available expert
category scores (same as typical TURI single-CAS overall; verified vs clean CSV).

Usage (from app root)::

    python scripts/build_p2oasys_score_lookup.py

Env overrides:
  P2OASYS_SCORE_LOOKUP_DB
  P2OASYS_EXPERT_VS_AUTO_CSV
  P2OASYS_EXPERT_CSV / FASTP2OASYS_DIR
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

import config  # noqa: E402
from utils.lookup_tables import normalize_cas_for_lookup  # noqa: E402
from utils.p2oasys_score_lookup import default_db_path, format_cas_display  # noqa: E402

EXPERT_COL_MAP = {
    "expert_Acute_Human_Effects": "expert_acute",
    "expert_Chronic_Human_Effects": "expert_chronic",
    "expert_Ecological_Hazards": "expert_ecological",
    "expert_Environmental_Fate_and_Transport": "expert_fate",
    "expert_Atmospheric_Hazard": "expert_atmospheric",
    "expert_Physical_Properties": "expert_physical",
    "expert_Process_Factors": "expert_process",
    "expert_Life_Cycle_Factors": "expert_life_cycle",
}

AUTO_COL_MAP = {
    "auto_Acute_Human_Effects": "auto_acute",
    "auto_Chronic_Human_Effects": "auto_chronic",
    "auto_Ecological_Hazards": "auto_ecological",
    "auto_Environmental_Fate_and_Transport": "auto_fate",
    "auto_Atmospheric_Hazard": "auto_atmospheric",
    "auto_Physical_Properties": "auto_physical",
}

EXPERT_SCORE_COLS = list(EXPERT_COL_MAP.values())
AUTO_SCORE_COLS = list(AUTO_COL_MAP.values())

SCHEMA_SQL = """
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
CREATE INDEX IF NOT EXISTS idx_by_cas_digits ON by_cas (cas);
"""


def _to_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)) and v == v:
        return float(v)
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _resolve_panel_csv(cli: str | None) -> Path:
    if cli:
        return Path(cli)
    env = (os.environ.get("P2OASYS_EXPERT_VS_AUTO_CSV") or "").strip()
    if env:
        return Path(env)
    haz = Path(getattr(config, "_HAZQUERY_ROOT", APP_ROOT.parent.parent.parent))
    return haz / "GHhaz5" / "report_expert_vs_auto_smiles_subset.csv"


def _resolve_expert_overall_csv(cli: str | None) -> Path:
    if cli:
        return Path(cli)
    path = getattr(config, "P2OASYS_EXPERT_CSV_PATH", None)
    if path:
        return Path(path)
    return Path(getattr(config, "FASTP2OASYS_DIR", "")) / "data" / "p2oasys_single_cas_clean.csv"


def _mean_expert_overall(row: dict) -> float | None:
    vals = [_to_float(row.get(c)) for c in EXPERT_SCORE_COLS]
    present = [v for v in vals if v is not None]
    if not present:
        return None
    return sum(present) / len(present)


def build(
    *,
    panel_csv: Path,
    expert_csv: Path,
    db_path: Path,
) -> dict[str, int]:
    if not panel_csv.is_file():
        raise FileNotFoundError(f"Panel CSV not found: {panel_csv}")

    panel = pd.read_csv(panel_csv)
    if "cas" not in panel.columns:
        raise ValueError(f"Panel CSV missing cas column: {panel_csv}")

    expert_by_key: dict[str, dict] = {}
    if expert_csv.is_file():
        expert_df = pd.read_csv(expert_csv)
        cas_col = "CAS" if "CAS" in expert_df.columns else "cas"
        for _, erow in expert_df.iterrows():
            key = normalize_cas_for_lookup(str(erow.get(cas_col) or ""))
            if not key:
                continue
            expert_by_key[key] = {
                "name": (str(erow.get("name") or "").strip() or None),
                "score": _to_float(erow.get("p2oasys_score")),
            }
    else:
        print(f"WARN: expert overall CSV missing ({expert_csv}); using category mean fallback only.")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Idempotent rebuild: drop and recreate for clean upsert semantics.
    if db_path.is_file():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA_SQL)
        seen: set[str] = set()
        rows_out = []
        for _, prow in panel.iterrows():
            key = normalize_cas_for_lookup(str(prow.get("cas") or ""))
            if not key or key in seen:
                continue
            seen.add(key)
            cas_disp = format_cas_display(key)

            rec: dict = {c: None for c in (
                "name_expert", "name_auto",
                *EXPERT_SCORE_COLS, "expert_overall",
                *AUTO_SCORE_COLS, "auto_overall",
                "tci_product", "sds_tci_ok", "tci_skip_reason",
                "auto_scorer_version", "auto_sources", "expert_source",
            )}
            rec["cas"] = cas_disp
            rec["updated_at"] = now

            for src, dst in EXPERT_COL_MAP.items():
                rec[dst] = _to_float(prow.get(src))
            for src, dst in AUTO_COL_MAP.items():
                rec[dst] = _to_float(prow.get(src))

            rec["auto_overall"] = _to_float(prow.get("auto_overall_max"))
            rec["name_auto"] = (str(prow.get("auto_name") or "").strip() or None)
            gt_name = (str(prow.get("gt_name") or "").strip() or None)
            rec["name_expert"] = gt_name
            rec["auto_scorer_version"] = (str(prow.get("scorer_version") or "").strip() or None)
            rec["auto_sources"] = (str(prow.get("sources") or "").strip() or None)

            tci_prod = prow.get("tci_product")
            if tci_prod is not None and not (isinstance(tci_prod, float) and pd.isna(tci_prod)):
                s = str(tci_prod).strip()
                rec["tci_product"] = s or None
            else:
                rec["tci_product"] = None

            sds_ok = prow.get("sds_tci_ok")
            if sds_ok is None or (isinstance(sds_ok, float) and pd.isna(sds_ok)):
                rec["sds_tci_ok"] = None
            else:
                rec["sds_tci_ok"] = 1 if bool(sds_ok) else 0

            tci_err = prow.get("tci_error")
            if tci_err is not None and not (isinstance(tci_err, float) and pd.isna(tci_err)):
                rec["tci_skip_reason"] = str(tci_err).strip() or None

            ex = expert_by_key.get(key)
            if ex:
                if ex.get("name"):
                    rec["name_expert"] = ex["name"]
                if ex.get("score") is not None:
                    rec["expert_overall"] = ex["score"]
                    rec["expert_source"] = "p2oasys_single_cas_clean"
            if rec["expert_overall"] is None:
                rec["expert_overall"] = _mean_expert_overall(rec)
                if rec["expert_overall"] is not None and not rec.get("expert_source"):
                    rec["expert_source"] = "mean_of_expert_categories"

            has_expert = any(rec.get(c) is not None for c in EXPERT_SCORE_COLS) or rec.get("expert_overall") is not None
            has_auto = any(rec.get(c) is not None for c in AUTO_SCORE_COLS) or rec.get("auto_overall") is not None
            rec["has_expert"] = 1 if has_expert else 0
            rec["has_auto"] = 1 if has_auto else 0
            rows_out.append(rec)

        cols = [
            "cas", "name_expert", "name_auto",
            *EXPERT_SCORE_COLS, "expert_overall",
            *AUTO_SCORE_COLS, "auto_overall",
            "has_expert", "has_auto",
            "tci_product", "sds_tci_ok", "tci_skip_reason",
            "auto_scorer_version", "auto_sources", "expert_source", "updated_at",
        ]
        sql = (
            f"INSERT INTO by_cas ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
        )
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows_out])
        conn.commit()

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
        return {
            "n_cas": int(row[0] or 0),
            "n_with_expert": int(row[1] or 0),
            "n_with_auto": int(row[2] or 0),
            "n_with_both": int(row[3] or 0),
            "n_with_tci_product": int(row[4] or 0),
        }
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build P2OASys score lookup SQLite DB")
    p.add_argument("--panel-csv", default=None, help="expert_vs_auto smiles subset CSV")
    p.add_argument("--expert-csv", default=None, help="p2oasys_single_cas_clean.csv")
    p.add_argument("--db", default=None, help="Output SQLite path")
    args = p.parse_args(argv)

    panel = _resolve_panel_csv(args.panel_csv)
    expert = _resolve_expert_overall_csv(args.expert_csv)
    db = Path(args.db) if args.db else default_db_path()

    print(f"Panel CSV : {panel}")
    print(f"Expert CSV: {expert}")
    print(f"DB out    : {db}")
    stats = build(panel_csv=panel, expert_csv=expert, db_path=db)
    print("Counts:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
