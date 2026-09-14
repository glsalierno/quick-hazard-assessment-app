#!/usr/bin/env python3
"""Auto P2OASys for every clean single-CAS harvest row. Resumable JSONL checkpoint.

Skips TCI SDS (too slow for 1k+ CAS). Writes auto category + subcategory scores
into GHaz7 p2oasys_score_lookup.sqlite and harvest.sqlite (source='auto').
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
import traceback
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))
os.chdir(APP)

import config
from services.chemical_assessment import ChemicalAssessmentService
from utils import (
    atmo_gwp,
    hazard_for_p2oasys,
    p2oasys_form,
    p2oasys_matrix_placeholder,
    p2oasys_scorer,
    p2oasys_score_lookup,
)
from utils.p2oasys_extras_merge import merge_extra_sources_for_cas

CHECKPOINT = Path(config.FASTP2OASYS_DIR) / "harvest_auto_assess.jsonl"
HARVEST = Path(config.P2OASYS_HARVEST_DB)


def _say(msg: str) -> None:
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("ascii", "replace").decode("ascii"), flush=True)


def load_cas_list() -> list[tuple[str, str]]:
    if not HARVEST.is_file():
        raise SystemExit(f"Harvest DB missing: {HARVEST}")
    con = sqlite3.connect(str(HARVEST))
    rows = con.execute(
        "SELECT cas, name FROM chemicals WHERE chosen=1 AND cas IS NOT NULL ORDER BY cas"
    ).fetchall()
    con.close()
    return [(str(c), str(n or "")) for c, n in rows]


def already_done() -> set[str]:
    done: set[str] = set()
    if not CHECKPOINT.is_file():
        return done
    with CHECKPOINT.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            cas = str(rec.get("cas") or "")
            if cas and not rec.get("error"):
                done.add(cas)
    return done


def score_one(svc, matrix, cas: str) -> dict:
    t0 = time.time()
    try:
        ar = svc.assess(cas)
    except Exception as exc:
        return {"cas": cas, "error": f"assess failed: {exc}", "elapsed_s": round(time.time() - t0, 3)}
    if ar.fetch_error and not ar.pubchem_data:
        return {
            "cas": cas,
            "error": ar.fetch_error or "No PubChem data",
            "elapsed_s": round(time.time() - t0, 3),
        }
    pub = ar.pubchem_data or {}
    clean = str(ar.identity.cas or cas).strip()
    sources = ["PubChem"] if pub else []
    if ar.toxval_data:
        sources.append("ToxValDB")
    extra, pipe_note = merge_extra_sources_for_cas(clean)
    if pipe_note and pipe_note != "PubChem-only":
        sources.append(pipe_note)
    pub_for_state = dict(pub)
    state, state_src = atmo_gwp.infer_physical_state_from_sources(
        sds_fields=None, structured_sds=None, pubchem=pub_for_state
    )
    extra = atmo_gwp.apply_atmospheric_gwp_rule(extra, physical_state=state, state_source=state_src)
    hazard_data = hazard_for_p2oasys.build_hazard_data(
        pub,
        toxval_data=ar.toxval_data,
        carc_potency_data=getattr(ar, "carc_potency_data", None),
        extra_sources=extra,
    )
    hazard_data = atmo_gwp.merge_gwp_into_hazard_data(hazard_data, extra)
    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hazard_data, matrix)
    scores = p2oasys_form.strip_manual_only_scores(scores)
    name = ar.identity.chemical_name or pub.get("iupac_name") or pub.get("title")
    p2oasys_score_lookup.upsert_auto_from_draft(
        {
            "ok": True,
            "cas": clean,
            "scores": scores,
            "chemical_name": name,
            "sources_used": sources,
        }
    )
    write_harvest_auto(clean, scores)
    n_scored = len((trace or {}).get("scored") or [])
    return {
        "cas": clean,
        "error": None,
        "name": name,
        "n_scored_units": n_scored,
        "scorer_version": (trace or {}).get("scorer_version"),
        "elapsed_s": round(time.time() - t0, 3),
        "sources": sources,
    }


def write_harvest_auto(cas: str, scores: dict) -> None:
    if not HARVEST.is_file():
        return
    rows = []
    for cat, sub, score in p2oasys_score_lookup.iter_subcat_cells(scores):
        rows.append((-1, cas, "auto", cat, sub, float(score)))
    if not rows:
        return
    con = sqlite3.connect(str(HARVEST))
    try:
        con.execute(
            "DELETE FROM subcat_scores WHERE source='auto' AND cas=?",
            (cas,),
        )
        con.executemany(
            "INSERT INTO subcat_scores (col_id, cas, source, category, subcategory, score) "
            "VALUES (?,?,?,?,?,?)",
            rows,
        )
        con.commit()
    finally:
        con.close()


def main() -> int:
    cas_list = load_cas_list()
    done = already_done()
    todo = [(c, n) for c, n in cas_list if c not in done]
    _say(f"harvest_cas={len(cas_list)} done={len(done)} todo={len(todo)}")
    matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH), Path(config.DATA_DIR)
    )
    _say(f"Matrix: {matrix_path} kind={matrix_kind}")
    if not matrix_path.is_file():
        _say("ERROR: matrix missing")
        return 2
    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    svc = ChemicalAssessmentService()
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    ok = err = 0
    for i, (cas, name) in enumerate(todo, 1):
        _say(f"[{i}/{len(todo)}] {cas} {name}")
        try:
            rec = score_one(svc, matrix, cas)
        except Exception as exc:
            rec = {
                "cas": cas,
                "error": str(exc),
                "trace": traceback.format_exc()[-800:],
            }
        with CHECKPOINT.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")
        if rec.get("error"):
            err += 1
            _say(f"  ERROR {rec['error']}")
        else:
            ok += 1
            _say(f"  units={rec.get('n_scored_units')} elapsed={rec.get('elapsed_s')}s")
        time.sleep(0.2)
    _say(f"Finished ok={ok} err={err} checkpoint={CHECKPOINT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
