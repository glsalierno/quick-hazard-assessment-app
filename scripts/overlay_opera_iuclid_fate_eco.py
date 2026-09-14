#!/usr/bin/env python3
"""
Overlay OPERA Fate (+ optional IUCLID aquatic) onto harvest auto P2OASys.

Uses opera_precompute.sqlite via p2oasys_extras_merge / opera_to_extra_sources.
IUCLID uses OFFLINE_LOCAL_ARCHIVE when available (local REACH zip + snippet cache).

Max-merges subcategory cells; refreshes harvest_expert_vs_auto_density.json.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))
os.chdir(APP)

import config
from utils import hazard_for_p2oasys, p2oasys_scorer
from utils.p2oasys_extras_merge import (
    ensure_offline_env_defaults,
    get_offline_ctx,
    merge_extra_sources_for_cas,
    opera_extra_from_precompute,
)
from utils.p2oasys_score_ribbon import MATRIX_ROWS, SUBCAT_DEFS, SUBCAT_NAME_ALIASES
from utils.iuclid_p2oasys_bridge import build_extra_sources_from_iuclid_unified
from unified_hazard_report.unified_lookup import unified_lookup

HARVEST = Path(config.P2OASYS_HARVEST_DB)
LOOKUP = Path(config.P2OASYS_SCORE_LOOKUP_DB)
OUT = APP / "data" / "harvest_expert_vs_auto_density.json"

AUTO6 = [r[0] for r in MATRIX_ROWS if r[2]]
FATE_CAT = "Environmental Fate & Transport"
ECO_CAT = "Ecological Hazards"
FATE_SUBS = {s for s, _ in SUBCAT_DEFS[FATE_CAT]}
ECO_SUBS = {s for s, _ in SUBCAT_DEFS[ECO_CAT]}

# Excel sometimes names the category-level block after the category itself.
SUBCAT_NAME_ALIASES_LOCAL = {
    **SUBCAT_NAME_ALIASES,
    "Environmental Fate & Transport": "Bioconcentration/ Bioaccumulation",  # avoid; prefer real sub names
    "Ecological Hazards": "Acute Aquatic Toxicity",
}


def _canon_sub(name: str) -> str:
    n = str(name).strip()
    if n in ("Environmental Fate & Transport", "Ecological Hazards"):
        return n  # category-level placeholder — skip via FATE_SUBS/ECO_SUBS filter
    return SUBCAT_NAME_ALIASES.get(n, n)


def chosen_cas(con: sqlite3.Connection) -> list[str]:
    return [str(r[0]) for r in con.execute("SELECT cas FROM chemicals WHERE chosen=1 AND cas IS NOT NULL") if r[0]]


def load_max_pairs(con: sqlite3.Connection, source: str, allow: set[str]) -> dict[str, dict[tuple[str, str], float]]:
    out: dict[str, dict[tuple[str, str], float]] = defaultdict(dict)
    for cas, cat, sub, score in con.execute(
        "SELECT cas, category, subcategory, score FROM subcat_scores WHERE source=?",
        (source,),
    ):
        if not cas or cas not in allow:
            continue
        cat_s = str(cat).replace("Environmental Fate and Transport", "Environmental Fate & Transport")
        key = (cat_s, _canon_sub(str(sub)))
        if key[1] in (FATE_CAT, ECO_CAT):
            continue
        val = float(score)
        prev = out[str(cas)].get(key)
        if prev is None or val > prev:
            out[str(cas)][key] = val
    return out


def merge_score(amap: dict, key: tuple[str, str], score: float) -> bool:
    prev = amap.get(key)
    if prev is None or float(score) > float(prev):
        amap[key] = float(score)
        return prev is None
    return False


def upsert_subcat(con: sqlite3.Connection, cas: str, cat: str, sub: str, score: float) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(subcat_scores)")}
    if "col_id" in cols:
        existing = con.execute(
            "SELECT col_id FROM subcat_scores WHERE cas=? AND source='auto' AND category=? AND subcategory=?",
            (cas, cat, sub),
        ).fetchone()
        if existing:
            con.execute(
                "UPDATE subcat_scores SET score=? WHERE cas=? AND source='auto' AND category=? AND subcategory=?",
                (float(score), cas, cat, sub),
            )
        else:
            con.execute(
                "INSERT INTO subcat_scores (col_id, cas, source, category, subcategory, score) VALUES (?,?,?,?,?,?)",
                (-1, cas, "auto", cat, sub, float(score)),
            )
    else:
        con.execute(
            "INSERT OR REPLACE INTO subcat_scores (cas, source, category, subcategory, score) VALUES (?,?,?,?,?)",
            (cas, "auto", cat, sub, float(score)),
        )


def mean_filled(pairs, cas_map, slot_set) -> float:
    vals = [sum(1 for sl in slot_set if sl in cas_map.get(cas, {})) for cas in pairs]
    return round(sum(vals) / len(vals), 2) if vals else 0.0


def cat_metrics(expert, auto, slots):
    from statistics import mean

    out = []
    for cat in sorted({c for c, _ in slots}):
        sub_slots = [(c, s) for c, s in slots if c == cat]
        both_diffs = []
        e_fill = a_fill = both_n = 0
        for cas in expert:
            emap = expert.get(cas) or {}
            amap = auto.get(cas) or {}
            ev = max((emap[sl] for sl in sub_slots if sl in emap), default=None)
            av = max((amap[sl] for sl in sub_slots if sl in amap), default=None)
            if ev is not None:
                e_fill += 1
            if av is not None:
                a_fill += 1
            if ev is not None and av is not None:
                both_n += 1
                both_diffs.append(abs(float(av) - float(ev)))
        n = len(expert) or 1
        out.append(
            {
                "category": cat,
                "expert_fill_pct": round(100.0 * e_fill / n, 1),
                "auto_fill_pct": round(100.0 * a_fill / n, 1),
                "both_n": both_n,
                "mae": round(mean(both_diffs), 2) if both_diffs else None,
                "exact_pct": round(100.0 * sum(1 for d in both_diffs if d == 0) / both_n, 1) if both_n else None,
                "within_1_pct": round(100.0 * sum(1 for d in both_diffs if d <= 1) / both_n, 1) if both_n else None,
                "within_2_pct": round(100.0 * sum(1 for d in both_diffs if d <= 2) / both_n, 1) if both_n else None,
            }
        )
    return out


def subcat_rows(expert, auto, slots):
    from statistics import mean

    rows = []
    n = len(expert) or 1
    for cat, sub in slots:
        key = (cat, sub)
        expert_n = auto_n = both_n = expert_only = auto_only = 0
        diffs = []
        for cas in expert:
            ev = (expert.get(cas) or {}).get(key)
            av = (auto.get(cas) or {}).get(key)
            if ev is not None:
                expert_n += 1
            if av is not None:
                auto_n += 1
            if ev is not None and av is not None:
                both_n += 1
                diffs.append(abs(float(av) - float(ev)))
            elif ev is not None:
                expert_only += 1
            elif av is not None:
                auto_only += 1
        rows.append(
            {
                "category": cat,
                "subcategory": sub,
                "auto_scorable": cat in AUTO6,
                "expert_n": expert_n,
                "expert_pct": round(100.0 * expert_n / n, 1),
                "auto_n": auto_n,
                "auto_pct": round(100.0 * auto_n / n, 1),
                "both_n": both_n,
                "expert_only": expert_only,
                "auto_only": auto_only,
                "mae": round(mean(diffs), 2) if diffs else None,
                "exact_pct": round(100.0 * sum(1 for d in diffs if d == 0) / both_n, 1) if both_n else None,
                "within_1_pct": round(100.0 * sum(1 for d in diffs if d <= 1) / both_n, 1) if both_n else None,
                "within_2_pct": round(100.0 * sum(1 for d in diffs if d <= 2) / both_n, 1) if both_n else None,
                "gap_pp": round(100.0 * expert_n / n - 100.0 * auto_n / n, 1),
            }
        )
    return rows


def snapshot(expert, auto, slots, auto6_slots, *, note: str):
    n_expert = len(expert)
    n_auto = sum(1 for cas in expert if auto.get(cas))
    rows = subcat_rows(expert, auto, slots)
    focus = {
        (FATE_CAT, "Persistence"),
        (FATE_CAT, "Rapid Degradability"),
        (FATE_CAT, "Bioconcentration/ Bioaccumulation"),
        (ECO_CAT, "Acute Aquatic Toxicity"),
        (ECO_CAT, "Chronic Aquatic Toxicity (fish, crustacea or algae)"),
        ("Atmospheric Hazard", "Greenhouse Gas"),
        ("Atmospheric Hazard", "NESHAP"),
        ("Physical Properties", "pH"),
    }
    thin = sorted(
        [r for r in rows if r["auto_scorable"] and (r["auto_pct"] or 0) < 15],
        key=lambda r: -(r.get("gap_pp") or 0),
    )[:12]
    return {
        "n_ghhaz7_cas": n_expert,
        "n_auto_cas_with_subcats": n_auto,
        "auto_coverage_pct": round(100.0 * n_auto / n_expert, 1) if n_expert else 0.0,
        "expert_auto6_subcats_mean": mean_filled(expert, expert, auto6_slots),
        "auto_auto6_subcats_mean": mean_filled(expert, auto, auto6_slots),
        "expert_all_subcats_mean": mean_filled(expert, expert, slots),
        "auto_all_subcats_mean": mean_filled(expert, auto, slots),
        "n_auto6_slots": len(auto6_slots),
        "n_all_slots": len(slots),
        "score_scale_note": (
            "P2OASys scores are integers 2–10. exact = identical score; "
            "within_1 = |auto−expert|≤1 (adjacent bands, not exact); within_2 = |diff|≤2."
        ),
        "category_match": cat_metrics(expert, auto, slots),
        "overlay_focus_rows": [r for r in rows if (r["category"], r["subcategory"]) in focus],
        "thin_auto_subcats": thin,
        "subcat_rows": rows,
        "note": note,
        "updated": "2026-09-14",
    }


def subcat_maxes_from_scores(scores: dict) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    for cat, bundle in (scores or {}).items():
        if not isinstance(bundle, dict):
            continue
        cat_s = str(cat).replace("Environmental Fate and Transport", FATE_CAT)
        for sub, cell in bundle.items():
            if str(sub).startswith("_"):
                continue
            sub_s = _canon_sub(str(sub))
            if sub_s in (FATE_CAT, ECO_CAT):
                continue
            val = None
            if isinstance(cell, dict) and isinstance(cell.get("_max"), (int, float)):
                val = float(cell["_max"])
            elif isinstance(cell, (int, float)):
                val = float(cell)
            if val is None:
                continue
            if cat_s == FATE_CAT and sub_s in FATE_SUBS:
                out[(FATE_CAT, sub_s)] = val
            elif cat_s == ECO_CAT and sub_s in ECO_SUBS:
                out[(ECO_CAT, sub_s)] = val
    return out


def main() -> None:
    ensure_offline_env_defaults()
    do_iuclid = (os.environ.get("OVERLAY_IUCLID") or "1").strip().lower() not in ("0", "false", "no")
    # Cap IUCLID attempts — full zip parse is slow; cache-only hits are fine
    iuclid_limit = int((os.environ.get("OVERLAY_IUCLID_MAX") or "200").strip() or "200")

    if not HARVEST.is_file():
        raise SystemExit(f"missing harvest db: {HARVEST}")

    hcon = sqlite3.connect(str(HARVEST))
    allow = set(chosen_cas(hcon))
    expert = load_max_pairs(hcon, "expert", allow)
    auto = load_max_pairs(hcon, "auto", allow)
    if LOOKUP.is_file():
        lcon = sqlite3.connect(str(LOOKUP))
        for cas, amap in load_max_pairs(lcon, "auto", allow).items():
            for k, v in amap.items():
                merge_score(auto.setdefault(cas, {}), k, v)
        lcon.close()

    slots = [(cat, sub) for cat, defs in SUBCAT_DEFS.items() for sub, _ in defs]
    auto6_slots = [(c, s) for c, s in slots if c in AUTO6]
    before = snapshot(expert, auto, slots, auto6_slots, note="before OPERA/IUCLID fate-eco overlay")

    matrix = p2oasys_scorer.load_p2oasys_matrix(Path(config.P2OASYS_MATRIX_PATH))
    ctx = get_offline_ctx() if do_iuclid else None
    print(
        f"expert={len(expert)} opera_precompute={sum(1 for c in expert if opera_extra_from_precompute(c))} "
        f"iuclid_ctx={ctx is not None} iuclid_limit={iuclid_limit}"
    )

    n_opera = n_iuclid = n_fate_cells = n_eco_cells = 0
    iuclid_tried = 0
    t0 = time.time()

    for i, cas in enumerate(sorted(expert)):
        extra = None
        opera_xs = opera_extra_from_precompute(cas)
        if opera_xs:
            extra = hazard_for_p2oasys.merge_extra_sources(extra, opera_xs)
            n_opera += 1

        if ctx is not None and iuclid_tried < iuclid_limit:
            iuclid_tried += 1
            try:
                ul = unified_lookup(cas, ctx)
                iu = build_extra_sources_from_iuclid_unified(ul)
                if iu:
                    extra = hazard_for_p2oasys.merge_extra_sources(extra, iu)
                    n_iuclid += 1
            except Exception:
                pass

        if not extra:
            continue

        hd = hazard_for_p2oasys.build_hazard_data({}, extra_sources=extra)
        scores = p2oasys_scorer.compute_p2oasys_scores(hd, matrix)
        cells = subcat_maxes_from_scores(scores)
        amap = auto.setdefault(cas, {})
        for key, sc in cells.items():
            if merge_score(amap, key, sc):
                if key[0] == FATE_CAT:
                    n_fate_cells += 1
                elif key[0] == ECO_CAT:
                    n_eco_cells += 1
            else:
                amap[key] = float(sc)

        if (i + 1) % 200 == 0:
            print(f"  … {i+1}/{len(expert)} ({time.time()-t0:.0f}s)")

    after = snapshot(
        expert,
        auto,
        slots,
        auto6_slots,
        note=(
            f"after OPERA/IUCLID fate-eco; opera_cas={n_opera}, iuclid_hit={n_iuclid}, "
            f"iuclid_tried={iuclid_tried}, fate_cells={n_fate_cells}, eco_cells={n_eco_cells}"
        ),
    )
    after["before"] = {
        k: before[k]
        for k in ("auto_auto6_subcats_mean", "auto_all_subcats_mean", "category_match", "overlay_focus_rows")
        if k in before
    }
    after["overlay_stats"] = {
        "opera_cas": n_opera,
        "iuclid_hit": n_iuclid,
        "iuclid_tried": iuclid_tried,
        "fate_cells_written": n_fate_cells,
        "eco_cells_written": n_eco_cells,
        "elapsed_s": round(time.time() - t0, 1),
    }
    OUT.write_text(json.dumps(after, indent=2), encoding="utf-8")
    print("wrote", OUT)

    for path in (HARVEST, LOOKUP):
        if not path.is_file():
            continue
        con = sqlite3.connect(str(path))
        try:
            for cas, amap in auto.items():
                if cas not in allow:
                    continue
                for (cat, sub), sc in amap.items():
                    if cat in (FATE_CAT, ECO_CAT) and sub in (FATE_SUBS | ECO_SUBS):
                        upsert_subcat(con, cas, cat, sub, sc)
                        # Expert site labels (aliases) for DoSS / harvest parity
                        if sub == "Rapid Degradability":
                            upsert_subcat(
                                con, cas, cat, "Degradability in water, soil, sediment", sc
                            )
                        if sub == "Persistence":
                            upsert_subcat(
                                con, cas, cat, "Persistence in air, water, soil/sediment", sc
                            )
            con.commit()
            print("updated", path.name)
        finally:
            con.close()

    slim = {k: v for k, v in after.items() if k not in ("subcat_rows", "before")}
    print(json.dumps(slim, indent=2)[:5500])


if __name__ == "__main__":
    main()
