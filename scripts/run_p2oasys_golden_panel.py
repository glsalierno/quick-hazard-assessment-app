#!/usr/bin/env python3
"""
P2OASys golden-panel comparison (v6 hardening).

Compares automatic GHhaz6 category scores (official TURI matrix) to expert /
scraped reference scores from:

1. ``p2oasys_ground_truth.db`` (category-level; default: GHhaz4 SDS examples DB)
2. ``fastP2OASys/data/p2oasys_single_cas_clean.csv`` (overall expert score)

Prefers pure-looking substance names when multiple GT rows share a CAS.

Usage (from quick-hazard-assessment-app):

  python scripts/run_p2oasys_golden_panel.py --limit 20
  python scripts/run_p2oasys_golden_panel.py --cas 71-43-2 --cas 108-88-3
  python scripts/run_p2oasys_golden_panel.py --limit 50 -o data/golden_panel_results.csv

Network: PubChem lookups are required unless hazard JSON cache is provided.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_APP_ROOT = _SCRIPT_DIR.parent
_HAZQUERY = _APP_ROOT.parent.parent  # .../hazquery
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))
os.chdir(_APP_ROOT)

import config
from utils import (
    hazard_for_p2oasys,
    p2oasys_aggregate,
    p2oasys_assessment,
    p2oasys_matrix_placeholder,
    p2oasys_scorer,
    pubchem_client,
)

# Top-level categories comparable to the scraped GT DB.
COMPARE_CATEGORIES = [
    "Acute Human Effects",
    "Chronic Human Effects",
    "Ecological Hazards",
    "Environmental Fate & Transport",
    "Atmospheric Hazard",
    "Physical Properties",
    "Process Factors",
    "Life Cycle Factors",
]

# Curated pure-chemical CAS list for a small smoke panel.
DEFAULT_GOLDEN_CAS = [
    "71-43-2",   # Benzene
    "108-88-3",  # Toluene
    "50-00-0",   # Formaldehyde
    "67-64-1",   # Acetone
    "67-63-0",   # Isopropanol
    "64-17-5",   # Ethanol
    "75-09-2",   # Dichloromethane
    "79-01-6",   # Trichloroethylene
    "107-06-2",  # 1,2-Dichloroethane
    "100-41-4",  # Ethylbenzene
]

_PRODUCTISH = re.compile(
    r"\b(detergent|cleaner|remover|softener|handwash|laundry|foam|spray|"
    r"product|solution|blend|mix|formula|brand|ecolution|great value|"
    r"goo gone|firemaster)\b",
    re.I,
)


def _default_gt_db() -> Path:
    env = os.environ.get("P2OASYS_GROUND_TRUTH_DB", "").strip()
    if env:
        return Path(env)
    local = Path(config.DATA_DIR) / "p2oasys_ground_truth.db"
    if local.is_file():
        return local
    return (
        _HAZQUERY
        / "GHhaz4"
        / "sds examples"
        / "data"
        / "p2oasys_ground_truth.db"
    )


def _default_expert_csv() -> Path:
    env = os.environ.get("P2OASYS_EXPERT_CSV", "").strip()
    if env:
        return Path(env)
    return _HAZQUERY / "fastP2OASys" / "data" / "p2oasys_single_cas_clean.csv"


def _purity_rank(name: str) -> int:
    """Lower is better (prefer chemical-looking names over product blends)."""
    if not name:
        return 50
    if _PRODUCTISH.search(name):
        return 40
    if ";" in name or "CAS#" in name.upper():
        return 45
    # Short single-token or classic chemical name.
    if len(name) < 40 and " " not in name.strip():
        return 0
    if re.match(r"^[A-Za-z0-9 ,\-\(\)]+$", name) and len(name) < 60:
        return 5
    return 20


def load_ground_truth_by_cas(db_path: Path) -> dict[str, dict[str, Any]]:
    """
    Return ``{cas: {name, scores: {category: float}}}`` preferring pure names.
    """
    if not db_path.is_file():
        raise FileNotFoundError(f"Ground-truth DB not found: {db_path}")
    con = sqlite3.connect(str(db_path))
    substances = pd.read_sql(
        "SELECT substance_id, name, cas_list FROM substances",
        con,
    )
    scores = pd.read_sql(
        "SELECT substance_id, category_name, score_value FROM substance_scores",
        con,
    )
    con.close()

    scores = scores[scores["category_name"].isin(COMPARE_CATEGORIES)]
    out: dict[str, dict[str, Any]] = {}
    for _, row in substances.iterrows():
        cas = str(row["cas_list"] or "").strip()
        if not cas or "," in cas:
            continue
        sid = int(row["substance_id"])
        name = str(row["name"] or "")
        sub = scores[scores["substance_id"] == sid]
        cat_map = {
            str(r["category_name"]): float(r["score_value"])
            for _, r in sub.iterrows()
            if r["score_value"] is not None
        }
        if not cat_map:
            continue
        cand = {"name": name, "scores": cat_map, "rank": _purity_rank(name), "substance_id": sid}
        prev = out.get(cas)
        if prev is None or cand["rank"] < prev["rank"]:
            out[cas] = cand
    return out


def load_expert_overall(csv_path: Path) -> dict[str, float]:
    if not csv_path.is_file():
        return {}
    df = pd.read_csv(csv_path)
    cas_col = "CAS" if "CAS" in df.columns else "cas"
    score_col = "p2oasys_score" if "p2oasys_score" in df.columns else None
    if score_col is None:
        return {}
    out: dict[str, float] = {}
    for _, r in df.iterrows():
        cas = str(r.get(cas_col) or "").strip()
        try:
            out[cas] = float(r[score_col])
        except (TypeError, ValueError):
            continue
    return out


def score_cas(cas: str, matrix: dict[str, Any]) -> dict[str, Any]:
    """Fetch PubChem + compute P2OASys category scores for one CAS."""
    pub = pubchem_client.get_compound_data(cas)
    if not pub:
        return {"cas": cas, "error": "no PubChem data", "scores": {}, "overall_max": None}
    hd = hazard_for_p2oasys.build_hazard_data(pub)
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hd, matrix)
    cat = {
        c: (scores.get(c) or {}).get("_category_max")
        for c in COMPARE_CATEGORIES
    }
    overall = p2oasys_aggregate.aggregate_category_scores(scores, "max")
    return {
        "cas": cas,
        "error": None,
        "name": pub.get("iupac_name") or pub.get("title"),
        "scores": cat,
        "overall_max": overall if overall == overall else None,
        "n_scored_units": len(trace.get("scored") or []),
        "n_missing_units": len(trace.get("missing") or []),
        "category_status": trace.get("category_status") or {},
        "scorer_version": trace.get("scorer_version"),
    }


def compare_row(
    cas: str,
    auto: dict[str, Any],
    gt: Optional[dict[str, Any]],
    expert_overall: Optional[float],
) -> dict[str, Any]:
    gt_name = (gt or {}).get("name")
    gt_pure = _purity_rank(str(gt_name or "")) <= 5
    row: dict[str, Any] = {
        "cas": cas,
        "auto_name": auto.get("name"),
        "gt_name": gt_name,
        "gt_is_pure": gt_pure,
        "error": auto.get("error"),
        "auto_overall_max": auto.get("overall_max"),
        "expert_overall": expert_overall,
        "n_scored_units": auto.get("n_scored_units"),
        "n_missing_units": auto.get("n_missing_units"),
    }
    abs_errs: list[float] = []
    covered = 0
    for cat in COMPARE_CATEGORIES:
        a = (auto.get("scores") or {}).get(cat)
        g = ((gt or {}).get("scores") or {}).get(cat)
        row[f"auto__{cat}"] = a
        row[f"gt__{cat}"] = g
        if a is not None and g is not None:
            err = abs(float(a) - float(g))
            row[f"abs_err__{cat}"] = err
            abs_errs.append(err)
            covered += 1
            # False-green: expert >= 8 (high hazard) but auto <= 4 (looks safe)
            row[f"false_green__{cat}"] = bool(g >= 8 and a <= 4)
        else:
            row[f"abs_err__{cat}"] = None
            row[f"false_green__{cat}"] = None
    row["n_categories_compared"] = covered
    row["mae_categories"] = (sum(abs_errs) / len(abs_errs)) if abs_errs else None
    if expert_overall is not None and auto.get("overall_max") is not None:
        row["abs_err_overall"] = abs(float(auto["overall_max"]) - float(expert_overall))
    else:
        row["abs_err_overall"] = None
    return row


def summarize(df: pd.DataFrame) -> dict[str, Any]:
    mae_cols = [c for c in df.columns if c.startswith("abs_err__") and c != "abs_err_overall"]
    fg_cols = [c for c in df.columns if c.startswith("false_green__")]

    def _metrics(sub: pd.DataFrame) -> dict[str, Any]:
        maes = sub[mae_cols].stack().dropna() if mae_cols and len(sub) else pd.Series(dtype=float)
        fg = sub[fg_cols].stack().dropna() if fg_cols and len(sub) else pd.Series(dtype=float)
        return {
            "n_cas": int(len(sub)),
            "n_category_pairs": int(maes.shape[0]),
            "mae_category": float(maes.mean()) if len(maes) else None,
            "median_abs_err_category": float(maes.median()) if len(maes) else None,
            "false_green_rate": float(fg.mean()) if len(fg) else None,
        }

    all_m = _metrics(df)
    pure = df[df["gt_is_pure"] == True] if "gt_is_pure" in df.columns else df.iloc[0:0]
    pure_m = _metrics(pure)
    return {
        "n_cas": all_m["n_cas"],
        "n_with_error": int(df["error"].notna().sum()) if "error" in df else 0,
        "all_gt_matches": all_m,
        "pure_gt_matches": pure_m,
        # Primary gates use pure-GT metrics when available; else all.
        "mae_category": pure_m["mae_category"] if pure_m["n_category_pairs"] else all_m["mae_category"],
        "median_abs_err_category": (
            pure_m["median_abs_err_category"]
            if pure_m["n_category_pairs"]
            else all_m["median_abs_err_category"]
        ),
        "false_green_rate": (
            pure_m["false_green_rate"] if pure_m["n_category_pairs"] else all_m["false_green_rate"]
        ),
        "n_category_pairs": (
            pure_m["n_category_pairs"] if pure_m["n_category_pairs"] else all_m["n_category_pairs"]
        ),
        "mae_overall_vs_expert": float(df["abs_err_overall"].dropna().mean())
        if "abs_err_overall" in df and df["abs_err_overall"].notna().any()
        else None,
        "matrix_required_official": True,
        "notes": [
            "Primary MAE/false-green use pure-looking GT substance names only "
            "(product/mixture SDS scores for the same CAS are reported under all_gt_matches).",
            "Process Factors / Life Cycle / Atmospheric often under-scored until dedicated rules land.",
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run P2OASys golden-panel comparison.")
    ap.add_argument("--gt-db", type=Path, default=None, help="Path to p2oasys_ground_truth.db")
    ap.add_argument("--expert-csv", type=Path, default=None, help="Path to p2oasys_single_cas_clean.csv")
    ap.add_argument("--cas", action="append", default=[], help="CAS to include (repeatable)")
    ap.add_argument("--limit", type=int, default=0, help="Max CAS (0 = all selected)")
    ap.add_argument("--use-default-golden", action="store_true", help="Use curated DEFAULT_GOLDEN_CAS")
    ap.add_argument("-o", "--output", type=Path, default=Path("data/golden_panel_results.csv"))
    ap.add_argument("--summary-json", type=Path, default=Path("data/golden_panel_summary.json"))
    ap.add_argument("--require-official", action="store_true", default=True)
    args = ap.parse_args()

    gt_db = args.gt_db or _default_gt_db()
    expert_csv = args.expert_csv or _default_expert_csv()

    matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH), Path(config.DATA_DIR)
    )
    if args.require_official and matrix_kind != "official":
        print(
            f"ERROR: official matrix required (got kind={matrix_kind!r} path={matrix_path}). "
            "Install Hazard Matrix Group Review 9-19-23.xlsx under data/.",
            file=sys.stderr,
        )
        return 2
    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    fp = p2oasys_scorer.matrix_fingerprint(matrix_path)

    gt = load_ground_truth_by_cas(gt_db) if gt_db.is_file() else {}
    expert = load_expert_overall(expert_csv)

    if args.cas:
        cas_list = list(dict.fromkeys(args.cas))
    elif args.use_default_golden or not gt:
        cas_list = list(DEFAULT_GOLDEN_CAS)
    else:
        # Prefer GT CAS that also look pure, then fall back to default golden overlap.
        ranked = sorted(
            ((c, info["rank"], info["name"]) for c, info in gt.items()),
            key=lambda x: (x[1], x[0]),
        )
        cas_list = [c for c, r, _ in ranked if r <= 5]
        if not cas_list:
            cas_list = [c for c, _, _ in ranked]
        # Ensure curated chemicals are included first.
        cas_list = list(dict.fromkeys(DEFAULT_GOLDEN_CAS + cas_list))

    if args.limit > 0:
        cas_list = cas_list[: args.limit]

    print(f"Matrix: {matrix_kind} sha256={fp.get('sha256', '')[:12]}…")
    print(f"GT DB: {gt_db} ({len(gt)} CAS)")
    print(f"Expert CSV: {expert_csv} ({len(expert)} CAS)")
    print(f"Evaluating {len(cas_list)} CAS…")

    rows: list[dict[str, Any]] = []
    for i, cas in enumerate(cas_list, 1):
        print(f"[{i}/{len(cas_list)}] {cas}", flush=True)
        try:
            auto = score_cas(cas, matrix)
        except Exception as exc:  # noqa: BLE001
            auto = {"cas": cas, "error": f"{type(exc).__name__}: {exc}", "scores": {}, "overall_max": None}
        rows.append(compare_row(cas, auto, gt.get(cas), expert.get(cas)))

    df = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    summary = summarize(df)
    summary["matrix"] = {"kind": matrix_kind, **{k: fp.get(k) for k in ("sha256", "filename", "size_bytes")}}
    summary["scorer_version"] = p2oasys_scorer.SCORER_VERSION
    summary["assessment_schema"] = p2oasys_assessment.ASSESSMENT_SCHEMA_VERSION
    summary["output"] = str(args.output)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Wrote {args.output}")
    print(f"Wrote {args.summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
