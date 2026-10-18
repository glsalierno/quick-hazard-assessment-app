"""Compare expert GT information density vs auto P2OASys (no network)."""
from __future__ import annotations

import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from utils.p2oasys_score_ribbon import SUBCAT_DEFS, SUBCAT_NAME_ALIASES, MATRIX_ROWS

LOOKUP = APP / "data" / "p2oasys_score_lookup.sqlite"
EXPERT_JSON = APP / "data" / "expert_subcat_by_cas_priority62.json"
BASELINE = APP / "data" / "auto_vs_expert_baseline_2026-09-11.json"
PRIORITY_CSV = Path(
    r"c:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\doss-ondemand\priority_expert_p2oasys_scores.csv"
)
SMILES_CSV = Path(
    r"c:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz5\report_expert_vs_auto_smiles_subset.csv"
)
WIRING = Path(
    r"c:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz5\report_subcategory_wiring.json"
)
OUT = APP / "data" / "expert_vs_auto_density.json"

AUTO6 = [r[0] for r in MATRIX_ROWS if r[2]]
HUMAN = [r[0] for r in MATRIX_ROWS if not r[2]]


def _canon_sub(name: str) -> str:
    n = str(name).strip()
    return SUBCAT_NAME_ALIASES.get(n, n)


def main() -> None:
    con = sqlite3.connect(LOOKUP)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM by_cas WHERE has_expert=1"
    ).fetchall()
    n_expert = len(rows)
    cat_cols = [
        ("acute", "expert_acute", "auto_acute"),
        ("chronic", "expert_chronic", "auto_chronic"),
        ("ecological", "expert_ecological", "auto_ecological"),
        ("fate", "expert_fate", "auto_fate"),
        ("atmospheric", "expert_atmospheric", "auto_atmospheric"),
        ("physical", "expert_physical", "auto_physical"),
        ("process", "expert_process", None),
        ("life_cycle", "expert_life_cycle", None),
    ]
    cat_stats = []
    for label, ecol, acol in cat_cols:
        e_n = sum(1 for r in rows if r[ecol] is not None)
        if acol:
            a_n = sum(1 for r in rows if r[acol] is not None)
            both = sum(1 for r in rows if r[ecol] is not None and r[acol] is not None)
            miss = sum(1 for r in rows if r[ecol] is not None and r[acol] is None)
        else:
            a_n = both = miss = 0
        cat_stats.append({
            "category": label,
            "expert_filled": e_n,
            "expert_pct": round(100 * e_n / n_expert, 1) if n_expert else 0,
            "auto_filled": a_n,
            "auto_pct": round(100 * a_n / n_expert, 1) if n_expert else 0,
            "auto_missing_when_expert": miss,
            "gap_pp": round(100 * (e_n - a_n) / n_expert, 1) if n_expert else 0,
            "auto_scorable": acol is not None,
        })

    auto6_filled = []
    for r in rows:
        n = sum(
            1
            for _, _, acol in cat_cols
            if acol and r[acol] is not None
        )
        auto6_filled.append(n)
    expert6_filled = []
    for r in rows:
        n = sum(
            1
            for _, ecol, acol in cat_cols
            if acol and r[ecol] is not None
        )
        expert6_filled.append(n)

    expert_sub = json.loads(EXPERT_JSON.read_text(encoding="utf-8"))
    n62 = len(expert_sub)
    # per canonical subcat fill among 62
    sub_fill = []
    expert_cells_auto6 = []
    expert_cells_all = []
    for cas, bundle in expert_sub.items():
        n_all = n_a6 = 0
        for cat, auto_ok in ((k, k in AUTO6) for k in list(SUBCAT_DEFS)):
            b = bundle.get(cat) or {}
            if cat == "Environmental Fate & Transport":
                b = b or bundle.get("Environmental Fate and Transport") or {}
            for sub, _acr in SUBCAT_DEFS[cat]:
                cell = b.get(sub) if isinstance(b, dict) else None
                scored = isinstance(cell, dict) and isinstance(cell.get("_max"), (int, float))
                if scored:
                    n_all += 1
                    if auto_ok:
                        n_a6 += 1
        expert_cells_all.append(n_all)
        expert_cells_auto6.append(n_a6)

    n_auto6_slots = sum(len(SUBCAT_DEFS[c]) for c in AUTO6)
    n_all_slots = sum(len(v) for v in SUBCAT_DEFS.values())

    subcat_rows = []
    for cat, defs in SUBCAT_DEFS.items():
        auto_ok = cat in AUTO6
        for sub, acr in defs:
            filled = 0
            for bundle in expert_sub.values():
                b = bundle.get(cat) or {}
                if cat == "Environmental Fate & Transport":
                    b = b or bundle.get("Environmental Fate and Transport") or {}
                cell = b.get(sub) if isinstance(b, dict) else None
                if isinstance(cell, dict) and isinstance(cell.get("_max"), (int, float)):
                    filled += 1
            subcat_rows.append({
                "category": cat,
                "subcategory": sub,
                "acronym": acr,
                "expert_n": filled,
                "expert_pct": round(100 * filled / n62, 1),
                "auto_scorable_category": auto_ok,
            })

    wiring = {}
    if WIRING.is_file():
        wj = json.loads(WIRING.read_text(encoding="utf-8"))
        for cat, cblob in (wj.get("categories") or {}).items():
            for sub in cblob.get("subcategories") or []:
                if not isinstance(sub, dict):
                    continue
                units = sub.get("units") or []
                wired = [u for u in units if isinstance(u, dict) and u.get("wired")]
                unwired = [u for u in units if isinstance(u, dict) and not u.get("wired")]
                wiring[_canon_sub(sub.get("name") or "")] = {
                    "n_units": len(units),
                    "n_wired": len(wired),
                    "wired_units": [u.get("unit") for u in wired],
                    "unwired_notes": [
                        (u.get("unit"), u.get("note") or u.get("rule_type"))
                        for u in unwired
                    ],
                }

    for row in subcat_rows:
        w = wiring.get(row["subcategory"]) or wiring.get(_canon_sub(row["subcategory"])) or {}
        row["n_wired_units"] = w.get("n_wired", 0)
        row["n_matrix_units"] = w.get("n_units", 0)
        row["wiring_gap"] = bool(
            row["auto_scorable_category"]
            and row["expert_pct"] >= 40
            and w.get("n_wired", 0) <= 1
        )

    # 62-set join with smiles-subset auto category fill + unit counts
    smiles = {}
    if SMILES_CSV.is_file():
        with SMILES_CSV.open(encoding="utf-8", newline="") as f:
            for rec in csv.DictReader(f):
                smiles[rec.get("cas", "").strip()] = rec
    pri = []
    if PRIORITY_CSV.is_file():
        with PRIORITY_CSV.open(encoding="utf-8", newline="") as f:
            pri = list(csv.DictReader(f))

    join62 = []
    for rec in pri:
        cas = (rec.get("cas") or "").strip()
        sm = smiles.get(cas) or {}
        try:
            expert_sub_n = int(float(rec.get("total_n_sub") or 0))
        except ValueError:
            expert_sub_n = 0
        try:
            expert_cells = int(float(rec.get("n_score_cells") or 0))
        except ValueError:
            expert_cells = 0
        try:
            auto_units = float(sm.get("n_scored_units") or 0)
        except ValueError:
            auto_units = 0
        auto6_n = 0
        for col in (
            "auto_Acute_Human_Effects",
            "auto_Chronic_Human_Effects",
            "auto_Ecological_Hazards",
            "auto_Environmental_Fate_and_Transport",
            "auto_Atmospheric_Hazard",
            "auto_Physical_Properties",
        ):
            v = sm.get(col)
            if v not in (None, ""):
                auto6_n += 1
        join62.append({
            "cas": cas,
            "name": rec.get("name") or sm.get("gt_name") or "",
            "expert_n_sub": expert_sub_n,
            "expert_n_cells": expert_cells,
            "auto_n_units": auto_units,
            "auto6_cats": auto6_n,
            "expert_auto6_full": rec.get("auto6_full"),
        })

    baseline = {}
    if BASELINE.is_file():
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))

    opportunities = []
    for row in sorted(subcat_rows, key=lambda r: (-r["expert_pct"], r["category"])):
        if not row["auto_scorable_category"]:
            continue
        if row["expert_pct"] >= 50 and row["n_wired_units"] <= 1:
            opportunities.append({
                "subcategory": row["subcategory"],
                "category": row["category"],
                "expert_fill_pct_on_62": row["expert_pct"],
                "wired_units": row["n_wired_units"],
                "why": "Experts score this often; auto has few wired extractors",
            })

    # Category-level biggest coverage holes on 1108
    holes = sorted(
        [c for c in cat_stats if c["auto_scorable"]],
        key=lambda c: -c["auto_missing_when_expert"],
    )

    out = {
        "n_expert_cas_category_db": n_expert,
        "n_priority62_subcat": n62,
        "n_auto6_subcat_slots": n_auto6_slots,
        "n_all_subcat_slots": n_all_slots,
        "category_fill_1108": cat_stats,
        "auto6_cats_filled_mean": round(sum(auto6_filled) / len(auto6_filled), 2) if auto6_filled else 0,
        "expert6_cats_filled_mean": round(sum(expert6_filled) / len(expert6_filled), 2) if expert6_filled else 0,
        "priority62_expert_auto6_subcats_mean": round(
            sum(expert_cells_auto6) / len(expert_cells_auto6), 2
        ) if expert_cells_auto6 else 0,
        "priority62_expert_all_subcats_mean": round(
            sum(expert_cells_all) / len(expert_cells_all), 2
        ) if expert_cells_all else 0,
        "priority62_expert_auto6_density_pct": round(
            100 * (sum(expert_cells_auto6) / (n62 * n_auto6_slots)), 1
        ) if n62 else 0,
        "join62_n": len(join62),
        "join62_expert_n_sub_mean": round(sum(r["expert_n_sub"] for r in join62) / len(join62), 2) if join62 else 0,
        "join62_expert_n_cells_mean": round(sum(r["expert_n_cells"] for r in join62) / len(join62), 2) if join62 else 0,
        "join62_auto_n_units_mean": round(sum(r["auto_n_units"] for r in join62) / len(join62), 2) if join62 else 0,
        "join62_auto6_cats_mean": round(sum(r["auto6_cats"] for r in join62) / len(join62), 2) if join62 else 0,
        "subcat_fill_62": subcat_rows,
        "coverage_holes_1108": holes,
        "subcat_opportunities": opportunities[:12],
        "baseline_mae": [
            {
                "category": c["category"],
                "pct_auto_filled": round(c["pct_auto_filled"], 1),
                "mae": round(c["mae"], 2),
                "n_miss_auto": c["n_miss_auto"],
            }
            for c in (baseline.get("categories") or [])
        ],
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in out.items() if k != "subcat_fill_62"}, indent=2))
    print("wrote", OUT)


if __name__ == "__main__":
    main()
