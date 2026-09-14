#!/usr/bin/env python3
"""Subcategory-level expert harvest vs auto P2OASys density (no network)."""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from utils.p2oasys_score_ribbon import MATRIX_ROWS, SUBCAT_DEFS, SUBCAT_NAME_ALIASES
import config

HARVEST = Path(config.P2OASYS_HARVEST_DB)
LOOKUP = Path(config.P2OASYS_SCORE_LOOKUP_DB)
OUT = APP / "data" / "harvest_expert_vs_auto_density.json"

AUTO6 = [r[0] for r in MATRIX_ROWS if r[2]]


def _canon_sub(name: str) -> str:
    n = str(name).strip()
    return SUBCAT_NAME_ALIASES.get(n, n)


def load_pairs(con: sqlite3.Connection, source: str) -> dict[str, dict[tuple[str, str], float]]:
    """cas -> {(category, subcategory): score} for chosen harvest CAS only."""
    chosen = {
        str(r[0])
        for r in con.execute("SELECT cas FROM chemicals WHERE chosen=1 AND cas IS NOT NULL")
    }
    out: dict[str, dict[tuple[str, str], float]] = defaultdict(dict)
    for cas, cat, sub, score in con.execute(
        "SELECT cas, category, subcategory, score FROM subcat_scores WHERE source=?",
        (source,),
    ):
        if not cas or cas not in chosen:
            continue
        key = (str(cat), _canon_sub(str(sub)))
        out[str(cas)][key] = float(score)
    return out


def main() -> None:
    if not HARVEST.is_file():
        raise SystemExit(f"missing {HARVEST}")
    h = sqlite3.connect(str(HARVEST))
    expert = load_pairs(h, "expert")
    auto_h = load_pairs(h, "auto")
    n_chem = h.execute("SELECT COUNT(*) FROM chemicals WHERE chosen=1").fetchone()[0]
    splits = dict(
        h.execute("SELECT split, COUNT(*) FROM chemicals GROUP BY split").fetchall()
    )
    n_conflict = h.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0]
    h.close()

    auto = dict(auto_h)
    if LOOKUP.is_file():
        l = sqlite3.connect(str(LOOKUP))
        for cas, cat, sub, score in l.execute(
            "SELECT cas, category, subcategory, score FROM subcat_scores WHERE source='auto'"
        ):
            key = (str(cat), _canon_sub(str(sub)))
            auto.setdefault(str(cas), {})[key] = float(score)
        l.close()

    n_expert = len(expert)
    n_auto = sum(1 for cas in expert if auto.get(cas))
    slots = [(cat, sub) for cat, defs in SUBCAT_DEFS.items() for sub, _acr in defs]
    auto6_slots = [(c, s) for c, s in slots if c in AUTO6]

    sub_rows = []
    for cat, sub in slots:
        auto_ok = cat in AUTO6
        e_n = a_n = both = e_only = a_only = 0
        abs_err = []
        for cas, emap in expert.items():
            ev = emap.get((cat, sub))
            av = (auto.get(cas) or {}).get((cat, sub))
            if ev is not None:
                e_n += 1
            if av is not None:
                a_n += 1
            if ev is not None and av is not None:
                both += 1
                abs_err.append(abs(av - ev))
            elif ev is not None and av is None:
                e_only += 1
            elif ev is None and av is not None:
                a_only += 1
        sub_rows.append(
            {
                "category": cat,
                "subcategory": sub,
                "auto_scorable": auto_ok,
                "expert_n": e_n,
                "expert_pct": round(100 * e_n / n_expert, 1) if n_expert else 0,
                "auto_n": a_n,
                "auto_pct": round(100 * a_n / n_expert, 1) if n_expert else 0,
                "both_n": both,
                "expert_only": e_only,
                "auto_only": a_only,
                "mae": round(sum(abs_err) / len(abs_err), 2) if abs_err else None,
                "gap_pp": round(100 * (e_n - a_n) / n_expert, 1) if n_expert else 0,
            }
        )

    def mean_filled(pairs, cas_map, slot_set) -> float:
        if not cas_map:
            return 0.0
        vals = []
        for cas in pairs:
            n = sum(1 for sl in slot_set if sl in cas_map.get(cas, {}))
            vals.append(n)
        return round(sum(vals) / len(vals), 2) if vals else 0.0

    holes = sorted(
        [r for r in sub_rows if r["auto_scorable"]],
        key=lambda r: (-r["expert_only"], -r["expert_pct"]),
    )[:15]

    out = {
        "n_ghhaz7_cas": n_expert,
        "n_chemicals_chosen_table": n_chem,
        "n_auto_cas_with_subcats": n_auto,
        "auto_coverage_pct": round(100 * n_auto / n_expert, 1) if n_expert else 0,
        "splits": splits,
        "n_dup_cas_conflicts": n_conflict,
        "expert_auto6_subcats_mean": mean_filled(expert, expert, auto6_slots),
        "auto_auto6_subcats_mean": mean_filled(expert, auto, auto6_slots),
        "expert_all_subcats_mean": mean_filled(expert, expert, slots),
        "auto_all_subcats_mean": mean_filled(expert, auto, slots),
        "n_auto6_slots": len(auto6_slots),
        "n_all_slots": len(slots),
        "thin_auto_subcats": holes,
        "subcat_rows": sub_rows,
        "source": str(HARVEST),
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    slim = {k: v for k, v in out.items() if k != "subcat_rows"}
    print(json.dumps(slim, indent=2))
    print("wrote", OUT)


if __name__ == "__main__":
    main()
