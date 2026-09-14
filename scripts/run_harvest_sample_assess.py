#!/usr/bin/env python3
"""Auto P2OASys drafts for a few harvest single-CAS chemicals (GHaz7)."""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from pathlib import Path

APP = Path(r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz6\GHaz7\quick-hazard-assessment-app")
sys.path.insert(0, str(APP))
os.chdir(APP)

import config
from services.chemical_assessment import ChemicalAssessmentService
from utils import (
    atmo_gwp,
    hazard_for_p2oasys,
    p2oasys_aggregate,
    p2oasys_form,
    p2oasys_matrix_placeholder,
    p2oasys_scorer,
)
from utils.p2oasys_extras_merge import merge_extra_sources_for_cas

AUTO6 = [
    "Acute Human Effects",
    "Chronic Human Effects",
    "Ecological Hazards",
    "Environmental Fate & Transport",
    "Atmospheric Hazard",
    "Physical Properties",
]
HUMAN_ONLY = ["Process Factors", "Life Cycle Factors"]

CAS_LIST = [
    ("67-64-1", "Acetone"),
    ("67-56-1", "Methanol"),
    ("64-17-5", "Ethanol"),
    ("108-88-3", "Toluene"),
    ("71-43-2", "Benzene"),
]

OUT = Path(r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz5") / "harvest_sample_assess_runs.json"


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
            "name": getattr(ar.identity, "chemical_name", None),
            "elapsed_s": round(time.time() - t0, 3),
        }
    pub = ar.pubchem_data or {}
    clean = str(ar.identity.cas or cas).strip()
    sources = ["PubChem"] if pub else []
    if ar.toxval_data:
        sources.append("ToxValDB")
    if getattr(ar, "carc_potency_data", None):
        sources.append("CPDB")
    if ar.dsstox_info:
        sources.append("DSSTox")

    extra, pipe_note = merge_extra_sources_for_cas(clean)
    if pipe_note and pipe_note != "PubChem-only":
        sources.append(pipe_note)

    pub_for_state = dict(pub)
    _state, _state_src = atmo_gwp.infer_physical_state_from_sources(
        sds_fields=None, structured_sds=None, pubchem=pub_for_state
    )
    extra = atmo_gwp.apply_atmospheric_gwp_rule(
        extra, physical_state=_state, state_source=_state_src
    )
    if _state != "unknown":
        sources.append(f"GWP-rule:{_state}")

    hazard_data = hazard_for_p2oasys.build_hazard_data(
        pub,
        toxval_data=ar.toxval_data,
        carc_potency_data=getattr(ar, "carc_potency_data", None),
        extra_sources=extra,
    )
    hazard_data = atmo_gwp.merge_gwp_into_hazard_data(hazard_data, extra)

    scores, trace = p2oasys_scorer.compute_p2oasys_scores_with_trace(hazard_data, matrix)
    scores = p2oasys_form.strip_manual_only_scores(scores)

    cat = {}
    status = (trace or {}).get("category_status") or {}
    for c in AUTO6 + HUMAN_ONLY:
        if c in HUMAN_ONLY:
            cat[c] = None
        else:
            cat[c] = (scores.get(c) or {}).get("_category_max")

    overall = None
    try:
        overall = p2oasys_aggregate.aggregate_category_scores(scores, "max")
        if overall != overall:
            overall = None
    except Exception:
        overall = None

    fill = {}
    for c in AUTO6:
        st = status.get(c) if isinstance(status, dict) else None
        if isinstance(st, dict):
            st_val = st.get("status") or st.get("state") or st
        else:
            st_val = st
        fill[c] = {"score": cat.get(c), "status": st_val}

    return {
        "cas": clean,
        "error": None,
        "name": ar.identity.chemical_name or pub.get("iupac_name") or pub.get("title"),
        "scores": cat,
        "fill": fill,
        "sources": sources,
        "pipe_note": pipe_note,
        "overall_max": overall,
        "n_scored_units": len((trace or {}).get("scored") or []),
        "n_missing_units": len((trace or {}).get("missing") or []),
        "scorer_version": (trace or {}).get("scorer_version"),
        "elapsed_s": round(time.time() - t0, 3),
        "physical_state": _state,
        "physical_state_source": _state_src,
    }


def main() -> int:
    print(f"APP={APP}", flush=True)
    matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH), Path(config.DATA_DIR)
    )
    print(f"Matrix: {matrix_path} kind={matrix_kind}", flush=True)
    if not matrix_path.is_file():
        print("ERROR: matrix missing", flush=True)
        return 2
    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    svc = ChemicalAssessmentService()
    results = []
    for cas, label in CAS_LIST:
        print(f"\n=== {cas} ({label}) ===", flush=True)
        try:
            r = score_one(svc, matrix, cas)
        except Exception as exc:
            r = {
                "cas": cas,
                "label": label,
                "error": str(exc),
                "trace": traceback.format_exc()[-1200:],
            }
        results.append(r)
        if r.get("error"):
            print("ERROR", r["error"], flush=True)
            if r.get("trace"):
                print(r["trace"][-600:], flush=True)
        else:
            print(
                f"name={r.get('name')} sources={r.get('sources')} "
                f"elapsed={r.get('elapsed_s')}s units={r.get('n_scored_units')}/{r.get('n_scored_units',0)+r.get('n_missing_units',0)}",
                flush=True,
            )
            for k in AUTO6:
                print(f"  {k}: {r['scores'].get(k)}", flush=True)
            print(f"  overall_max={r.get('overall_max')} state={r.get('physical_state')}", flush=True)
        time.sleep(0.3)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"\nWrote {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
