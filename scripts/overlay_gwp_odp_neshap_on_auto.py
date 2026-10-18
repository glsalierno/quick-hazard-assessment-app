#!/usr/bin/env python3
"""
Overlay Greenhouse Gas (GWP), Ozone Depletor (ODP), and NESHAP onto harvest auto.

Rules:
  - Lookup IPCC/CSV GWP100 + ODP when present
  - apply_atmospheric_gwp_rule(state=unknown): missing → 0 (default_not_on_authoritative_list)
  - NESHAP: CAA §112(b) HAP list → Listed (6) / Not listed (2)

Persists subcategory cells and refreshes harvest_expert_vs_auto_density.json.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

import config
from utils import atmo_gwp, lookup_tables, neshap_hap, p2oasys_scorer
from utils.p2oasys_score_ribbon import MATRIX_ROWS, SUBCAT_DEFS, SUBCAT_NAME_ALIASES

HARVEST = Path(config.P2OASYS_HARVEST_DB)
LOOKUP = Path(config.P2OASYS_SCORE_LOOKUP_DB)
OUT = APP / "data" / "harvest_expert_vs_auto_density.json"

AUTO6 = [r[0] for r in MATRIX_ROWS if r[2]]
CAT = "Atmospheric Hazard"
SUB_GHG = "Greenhouse Gas"
SUB_ODP = "Ozone Depletor"
SUB_NESHAP = "NESHAP"

GWP_RULE = {
    "type": "numeric",
    "unit": "GWP Relative to CO2",
    "thresholds": [(30.0, 2), (30.0, 4), (200.0, 6), (300.0, 8), (1000.0, 10)],
}
ODP_RULE = {
    "type": "numeric",
    "unit": "ODP Units",
    "thresholds": [(0.0, 2), (0.001, 4), (0.01, 6), (0.1, 8), (0.1, 10)],
}


def _canon_sub(name: str) -> str:
    return SUBCAT_NAME_ALIASES.get(str(name).strip(), str(name).strip())


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
    cats = sorted({c for c, _ in slots})
    for cat in cats:
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
        row = {
            "category": cat,
            "expert_fill_pct": round(100.0 * e_fill / n, 1),
            "auto_fill_pct": round(100.0 * a_fill / n, 1),
            "both_n": both_n,
            "mae": round(mean(both_diffs), 2) if both_diffs else None,
            "exact_pct": round(100.0 * sum(1 for d in both_diffs if d == 0) / both_n, 1) if both_n else None,
            "within_1_pct": round(100.0 * sum(1 for d in both_diffs if d <= 1) / both_n, 1) if both_n else None,
            "within_2_pct": round(100.0 * sum(1 for d in both_diffs if d <= 2) / both_n, 1) if both_n else None,
        }
        out.append(row)
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
        auto_ok = cat in AUTO6
        rows.append(
            {
                "category": cat,
                "subcategory": sub,
                "auto_scorable": auto_ok,
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
    focus_keys = {
        (CAT, SUB_GHG),
        (CAT, SUB_ODP),
        (CAT, SUB_NESHAP),
        (CAT, "Acid Rain Formation"),
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
        "overlay_focus_rows": [r for r in rows if (r["category"], r["subcategory"]) in focus_keys],
        "thin_auto_subcats": thin,
        "subcat_rows": rows,
        "note": note,
        "updated": "2026-09-14",
    }


def score_from_extra(extra: dict) -> tuple[int | None, int | None, int | None]:
    hm = (extra or {}).get("hazard_metrics") or {}
    gwp = None
    odp = None
    try:
        if hm.get("gwp100"):
            gwp = p2oasys_scorer._score_numeric(GWP_RULE, float(hm["gwp100"][0]), higher_is_safer=False)
    except Exception:
        gwp = None
    try:
        if hm.get("odp"):
            odp = p2oasys_scorer._score_numeric(ODP_RULE, float(hm["odp"][0]), higher_is_safer=False)
    except Exception:
        odp = None
    nesh = (extra or {}).get("neshap_meta") or {}
    nesh_score = int(nesh["score"]) if nesh.get("score") is not None else None
    return gwp, odp, nesh_score


def main() -> None:
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
    before = snapshot(expert, auto, slots, auto6_slots, note="before GWP/ODP/NESHAP overlay")

    odp_gwp = lookup_tables.load_odp_gwp_csv(config.P2OASYS_ODP_GWP_CSV_PATH)
    atmo_dir = atmo_gwp.resolve_atmo_dir(
        getattr(config, "ATMO_DIR", None),
        repo_root=getattr(config, "REPO_ROOT", None),
        fastp2oasys_dir=getattr(config, "FASTP2OASYS_DIR", None),
    )
    ipcc = atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir) if atmo_dir else {}
    hap = neshap_hap.load_hap_cas_set(getattr(config, "P2OASYS_HAP_CSV_PATH", None))
    print(f"lookup sizes: odp_gwp={len(odp_gwp)} ipcc={len(ipcc)} hap={len(hap)} expert_cas={len(expert)}")

    n_ghg = n_odp = n_nesh = n_listed = 0
    keys = ((CAT, SUB_GHG), (CAT, SUB_ODP), (CAT, SUB_NESHAP))

    for cas in sorted(expert):
        extra = lookup_tables.get_lookup_extra_sources(
            cas, odp_gwp_by_cas=odp_gwp, ipcc_gwp_by_cas=ipcc
        )
        extra = atmo_gwp.apply_atmospheric_gwp_rule(
            extra, physical_state="unknown", state_source="harvest_overlay"
        )
        extra = neshap_hap.apply_neshap_to_extra_sources(extra, cas, hap_cas=hap)
        gwp_s, odp_s, nesh_s = score_from_extra(extra)
        amap = auto.setdefault(cas, {})
        if gwp_s is not None and merge_score(amap, keys[0], gwp_s):
            n_ghg += 1
        elif gwp_s is not None:
            amap[keys[0]] = float(gwp_s)
        if odp_s is not None and merge_score(amap, keys[1], odp_s):
            n_odp += 1
        elif odp_s is not None:
            amap[keys[1]] = float(odp_s)
        if nesh_s is not None and merge_score(amap, keys[2], nesh_s):
            n_nesh += 1
        elif nesh_s is not None:
            amap[keys[2]] = float(nesh_s)
        if (extra.get("neshap_meta") or {}).get("listed"):
            n_listed += 1

    after = snapshot(
        expert,
        auto,
        slots,
        auto6_slots,
        note=(
            f"after GWP/ODP/NESHAP overlay; ghg={n_ghg}, odp={n_odp}, neshap={n_nesh}, "
            f"hap_listed={n_listed}, hap_table={len(hap)}"
        ),
    )
    # Keep prior acid/pH before block if present on disk
    if OUT.is_file():
        try:
            prev = json.loads(OUT.read_text(encoding="utf-8"))
            after["prior_note"] = prev.get("note")
        except Exception:
            pass
    after["before"] = {
        k: before[k]
        for k in ("auto_auto6_subcats_mean", "auto_all_subcats_mean", "category_match", "overlay_focus_rows")
        if k in before
    }
    after["overlay_stats"] = {
        "ghg_cells_written": n_ghg,
        "odp_cells_written": n_odp,
        "neshap_cells_written": n_nesh,
        "hap_listed_in_expert_set": n_listed,
        "hap_table_size": len(hap),
        "ipcc_gwp_size": len(ipcc),
        "odp_gwp_csv_size": len(odp_gwp),
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
                for (cat, sub), sc in (
                    (keys[0], amap.get(keys[0])),
                    (keys[1], amap.get(keys[1])),
                    (keys[2], amap.get(keys[2])),
                ):
                    if sc is not None:
                        upsert_subcat(con, cas, cat, sub, sc)
            con.commit()
            print("updated", path.name)
        finally:
            con.close()

    slim = {k: v for k, v in after.items() if k not in ("subcat_rows", "before")}
    print(json.dumps(slim, indent=2)[:5000])


if __name__ == "__main__":
    main()
