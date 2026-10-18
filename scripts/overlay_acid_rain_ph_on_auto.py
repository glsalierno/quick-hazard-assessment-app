#!/usr/bin/env python3
"""
Overlay Acid Rain (formula/SMILES N|S) + pH cascade onto harvest auto P2OASys.

Does not re-run full PubChem/OPERA harvest. Uses:
  - OPERA precompute SMILES (+ row) when present
  - PubChem SMILES via pubchempy only for CAS missing from OPERA (best-effort)
  - utils.atmo_gwp acid-rain phrase → matrix bands
  - utils.p2oasys_ph cascade (exp 1% → pKa → SMARTS)

Writes lookup/harvest auto subcategory cells (max merge) and refreshes
data/harvest_expert_vs_auto_density.json for the canvas.

Score band note: P2OASys uses integers 2–10; ``within_1`` means |auto−expert|≤1
(adjacent scores), which is NOT the same as exact match.
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

import config
from utils import atmo_gwp, p2oasys_ph
from utils.p2oasys_score_ribbon import MATRIX_ROWS, SUBCAT_DEFS, SUBCAT_NAME_ALIASES

HARVEST = Path(config.P2OASYS_HARVEST_DB)
LOOKUP = Path(config.P2OASYS_SCORE_LOOKUP_DB)
OPERA_DB = Path(getattr(config, "OPERA_PRECOMPUTE_DB_PATH", "") or (Path(config.DATA_DIR) / "opera_precompute.sqlite"))
OUT = APP / "data" / "harvest_expert_vs_auto_density.json"

AUTO6 = [r[0] for r in MATRIX_ROWS if r[2]]
ACID_RAIN_CAT = "Atmospheric Hazard"
ACID_RAIN_SUB = "Acid Rain Formation"
PH_CAT = "Physical Properties"
PH_SUB = "pH"

# Matrix Key Phrases for Acid Rain (Physical Hazard sheet phrasing)
_ACID_RAIN_SCORE = {
    "Does not contain S or N": 2,
    "Contain S or N but does not form SOx or NOx": 4,
    "Product may form SOx or NOx upon combustion": 6,
    "Produces SOx and NOx": 8,
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


def load_opera_structs() -> dict[str, dict]:
    """cas -> {smiles, mw, row} from opera_precompute.sqlite + optional smiles cache."""
    out: dict[str, dict] = {}
    cache_path = APP / "data" / "overlay_smiles_cache.json"
    if cache_path.is_file():
        try:
            for cas, smi in json.loads(cache_path.read_text(encoding="utf-8")).items():
                if smi:
                    out[str(cas)] = {"smiles": str(smi), "mw": None, "row": {}}
        except Exception:
            pass
    if not OPERA_DB.is_file():
        return out
    con = sqlite3.connect(str(OPERA_DB))
    try:
        for cas, smiles, blob in con.execute(
            "SELECT cas, smiles, result_json FROM opera_cas_result"
        ):
            row = {}
            try:
                row = json.loads(blob) if blob else {}
            except Exception:
                row = {}
            mw = None
            try:
                mw = float(row.get("MolWeight")) if row.get("MolWeight") not in (None, "", "NA") else None
            except (TypeError, ValueError):
                mw = None
            smi = (smiles or row.get("Canonical_SMILES") or row.get("SMILES") or "").strip() or None
            prev = out.get(str(cas).strip()) or {}
            out[str(cas).strip()] = {
                "smiles": smi or prev.get("smiles"),
                "mw": mw if mw is not None else prev.get("mw"),
                "row": row or prev.get("row") or {},
            }
    finally:
        con.close()
    return out


def save_smiles_cache(structs: dict[str, dict]) -> None:
    cache_path = APP / "data" / "overlay_smiles_cache.json"
    payload = {cas: (st.get("smiles") or "") for cas, st in structs.items() if st.get("smiles")}
    cache_path.write_text(json.dumps(payload, indent=0), encoding="utf-8")


def pubchem_smiles(cas: str) -> str | None:
    try:
        import pubchempy as pcp

        hits = pcp.get_compounds(cas, "name")
        if not hits:
            hits = pcp.get_compounds(cas, "formula")  # unlikely
        if not hits:
            # CAS as identity
            hits = pcp.get_compounds(cas, "cid") if cas.isdigit() else []
        if not hits:
            from pubchempy import get_compounds

            hits = get_compounds(cas, namespace="name")
        if hits:
            c = hits[0]
            return (
                getattr(c, "connectivity_smiles", None)
                or getattr(c, "canonical_smiles", None)
                or getattr(c, "isomeric_smiles", None)
            )
    except Exception:
        return None
    return None


def acid_rain_score(smiles: str | None, formula: str | None = None) -> tuple[int, dict] | None:
    if not smiles and not formula:
        return None
    info = atmo_gwp.acid_rain_phrase_for_structure(smiles=smiles, formula=formula)
    phrase = info.get("phrase") or ""
    score = _ACID_RAIN_SCORE.get(phrase)
    if score is None:
        # fallback by support flag
        score = 6 if info.get("has_s") or info.get("has_n") else 2
    return int(score), info


def ph_score(smiles: str | None, *, mw: float | None = None, opera_row: dict | None = None) -> tuple[int, dict] | None:
    hd: dict = {}
    if smiles:
        hd["smiles"] = smiles
    if mw:
        hd["molecular_weight"] = mw
    if opera_row:
        hd["opera_row"] = opera_row
    info = p2oasys_ph.estimate_ph_for_hazard(hd)
    if not info or info.get("score") is None:
        return None
    return int(info["score"]), info


def upsert_subcat(con: sqlite3.Connection, cas: str, cat: str, sub: str, score: float) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(subcat_scores)")}
    if "col_id" in cols:
        # harvest schema: col_id NOT NULL (often -1 for auto)
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


def merge_score(amap: dict, key: tuple[str, str], score: float) -> bool:
    """Max-merge into auto map; return True if new or raised."""
    prev = amap.get(key)
    if prev is None or score > prev:
        amap[key] = float(score)
        return True
    return False


def cat_metrics(expert, auto, slots):
    rows = []
    for cat in AUTO6:
        sub_slots = [(c, s) for c, s in slots if c == cat]
        e_n = a_n = both = exact = within1 = within2 = 0
        abs_err = []
        for cas, emap in expert.items():
            ev = max((emap[sl] for sl in sub_slots if sl in emap), default=None)
            amap = auto.get(cas) or {}
            av = max((amap[sl] for sl in sub_slots if sl in amap), default=None)
            if ev is not None:
                e_n += 1
            if av is not None:
                a_n += 1
            if ev is not None and av is not None:
                both += 1
                d = abs(av - ev)
                abs_err.append(d)
                if d < 1e-9:
                    exact += 1
                if d <= 1 + 1e-9:
                    within1 += 1
                if d <= 2 + 1e-9:
                    within2 += 1
        n = len(expert) or 1
        rows.append(
            {
                "category": cat,
                "expert_fill_pct": round(100 * e_n / n, 1),
                "auto_fill_pct": round(100 * a_n / n, 1),
                "both_n": both,
                "mae": round(sum(abs_err) / len(abs_err), 2) if abs_err else None,
                "exact_pct": round(100 * exact / both, 1) if both else None,
                "within_1_pct": round(100 * within1 / both, 1) if both else None,
                "within_2_pct": round(100 * within2 / both, 1) if both else None,
            }
        )
    return rows


def subcat_rows(expert, auto, slots):
    n_expert = len(expert) or 1
    rows = []
    for cat, sub in slots:
        auto_ok = cat in AUTO6
        e_n = a_n = both = e_only = a_only = exact = within1 = within2 = 0
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
                d = abs(av - ev)
                abs_err.append(d)
                if d < 1e-9:
                    exact += 1
                if d <= 1 + 1e-9:
                    within1 += 1
                if d <= 2 + 1e-9:
                    within2 += 1
            elif ev is not None and av is None:
                e_only += 1
            elif ev is None and av is not None:
                a_only += 1
        rows.append(
            {
                "category": cat,
                "subcategory": sub,
                "auto_scorable": auto_ok,
                "expert_n": e_n,
                "expert_pct": round(100 * e_n / n_expert, 1),
                "auto_n": a_n,
                "auto_pct": round(100 * a_n / n_expert, 1),
                "both_n": both,
                "expert_only": e_only,
                "auto_only": a_only,
                "mae": round(sum(abs_err) / len(abs_err), 2) if abs_err else None,
                "exact_pct": round(100 * exact / both, 1) if both else None,
                "within_1_pct": round(100 * within1 / both, 1) if both else None,
                "within_2_pct": round(100 * within2 / both, 1) if both else None,
                "gap_pp": round(100 * (e_n - a_n) / n_expert, 1),
            }
        )
    return rows


def mean_filled(pairs, cas_map, slot_set) -> float:
    vals = [sum(1 for sl in slot_set if sl in cas_map.get(cas, {})) for cas in pairs]
    return round(sum(vals) / len(vals), 2) if vals else 0.0


def snapshot(expert, auto, slots, auto6_slots, *, note: str):
    n_expert = len(expert)
    n_auto = sum(1 for cas in expert if auto.get(cas))
    rows = subcat_rows(expert, auto, slots)
    focus = {
        (ACID_RAIN_CAT, ACID_RAIN_SUB),
        (PH_CAT, PH_SUB),
        ("Acute Human Effects", "Health"),
        ("Physical Properties", "Flammability: Liquid"),
        ("Physical Properties", "Reactivity"),
    }
    return {
        "n_ghhaz7_cas": n_expert,
        "n_auto_cas_with_subcats": n_auto,
        "auto_coverage_pct": round(100 * n_auto / n_expert, 1) if n_expert else 0,
        "expert_auto6_subcats_mean": mean_filled(expert, expert, auto6_slots),
        "auto_auto6_subcats_mean": mean_filled(expert, auto, auto6_slots),
        "expert_all_subcats_mean": mean_filled(expert, expert, slots),
        "auto_all_subcats_mean": mean_filled(expert, auto, slots),
        "n_auto6_slots": len(auto6_slots),
        "n_all_slots": len(slots),
        "score_scale_note": (
            "P2OASys scores are integers 2–10. exact = identical score; "
            "within_1 = |auto−expert|≤1 (adjacent bands, not exact); "
            "within_2 = |diff|≤2."
        ),
        "category_match": cat_metrics(expert, auto, slots),
        "overlay_focus_rows": [r for r in rows if (r["category"], r["subcategory"]) in focus],
        "thin_auto_subcats": sorted(
            [r for r in rows if r["auto_scorable"]],
            key=lambda r: (-r["expert_only"], -r["expert_pct"]),
        )[:12],
        "subcat_rows": rows,
        "note": note,
        "updated": time.strftime("%Y-%m-%d"),
    }


def main() -> None:
    if not HARVEST.is_file():
        raise SystemExit(f"missing {HARVEST}")

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
    before = snapshot(expert, auto, slots, auto6_slots, note="before acid-rain/pH overlay")

    structs = load_opera_structs()
    n_ar = n_ph = n_smi_opera = n_smi_pc = n_fail = 0
    ar_key = (ACID_RAIN_CAT, ACID_RAIN_SUB)
    ph_key = (PH_CAT, PH_SUB)

    # Prefetch pubchem for missing — limit to keep runtime reasonable
    missing = [cas for cas in sorted(expert) if not (structs.get(cas) or {}).get("smiles")]
    print(f"opera smiles hits: {sum(1 for c in expert if (structs.get(c) or {}).get('smiles'))} / {len(expert)}")
    print(f"pubchem fetch candidates: {len(missing)} (fetch disabled for speed; using OPERA SMILES only)")
    # PubChem gap-fill is optional — enable by setting OVERLAY_PUBCHEM=1
    do_pc = (os.environ.get("OVERLAY_PUBCHEM") or "").strip() in ("1", "true", "yes")
    if not do_pc:
        missing = []

    for i, cas in enumerate(sorted(expert)):
        st = structs.get(cas) or {}
        smiles = st.get("smiles")
        if smiles:
            n_smi_opera += 1
        elif cas in missing:
            # throttle network a bit
            smiles = pubchem_smiles(cas)
            if smiles:
                n_smi_pc += 1
                st = {"smiles": smiles, "mw": st.get("mw"), "row": st.get("row") or {}}
                structs[cas] = st
            if (i + 1) % 50 == 0:
                print(f"  … {i+1}/{len(expert)} CAS processed")

        if not smiles:
            n_fail += 1
            continue

        try:
            ar = acid_rain_score(smiles)
            if ar:
                sc, _meta = ar
                if merge_score(auto.setdefault(cas, {}), ar_key, sc):
                    n_ar += 1
        except Exception:
            pass

        try:
            ps = ph_score(smiles, mw=st.get("mw"), opera_row=st.get("row"))
            if ps:
                sc, _meta = ps
                if merge_score(auto.setdefault(cas, {}), ph_key, sc):
                    n_ph += 1
        except Exception:
            pass

    save_smiles_cache(structs)

    after = snapshot(
        expert,
        auto,
        slots,
        auto6_slots,
        note=(
            f"after acid-rain/pH overlay; acid_rain_cells={n_ar}, pH_cells={n_ph}, "
            f"smiles_opera={n_smi_opera}, smiles_pubchem={n_smi_pc}, no_smiles={n_fail}"
        ),
    )
    after["before"] = {
        k: before[k]
        for k in (
            "auto_auto6_subcats_mean",
            "auto_all_subcats_mean",
            "category_match",
            "overlay_focus_rows",
        )
        if k in before
    }
    after["overlay_stats"] = {
        "acid_rain_cells_written": n_ar,
        "ph_cells_written": n_ph,
        "smiles_from_opera": n_smi_opera,
        "smiles_from_pubchem": n_smi_pc,
        "no_smiles": n_fail,
    }

    OUT.write_text(json.dumps(after, indent=2), encoding="utf-8")
    print("wrote JSON", OUT)

    # Persist auto cells for acid rain + pH
    for path in (HARVEST, LOOKUP):
        if not path.is_file():
            continue
        con = sqlite3.connect(str(path))
        try:
            for cas, amap in auto.items():
                if cas not in allow:
                    continue
                if ar_key in amap:
                    upsert_subcat(con, cas, ACID_RAIN_CAT, ACID_RAIN_SUB, amap[ar_key])
                if ph_key in amap:
                    upsert_subcat(con, cas, PH_CAT, PH_SUB, amap[ph_key])
            con.commit()
            print("updated", path.name)
        except Exception as exc:
            print("DB write failed for", path, exc)
        finally:
            con.close()

    slim = {k: v for k, v in after.items() if k not in ("subcat_rows", "before")}
    print(json.dumps(slim, indent=2)[:4500])


if __name__ == "__main__":
    main()
