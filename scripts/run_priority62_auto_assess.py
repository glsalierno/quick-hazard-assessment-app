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
from utils import p2oasys_score_lookup

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
    ('67-64-1', 'Acetone'),
    ('108-24-7', 'Acetic Anhydride (Ac2O)'),
    ('142-82-5', 'Heptanes'),
    ('1634-04-4', 'Tert-Butyl Methyl Ether (MTBE, Methyl Tert-Butyl Ether)'),
    ('110-54-3', 'Hexanes'),
    ('75-09-2', 'Dichloromethane (DCM)'),
    ('71-43-2', 'Benzene (Benzol, phenyl hydride)'),
    ('680-31-9', 'HMPA (Hexamethylphosphoramide)'),
    ('56-23-5', 'Carbon Tetrachloride'),
    ('123-91-1', '1,4-Dioxane (Dietheyelene diozide, diethylene ether)'),
    ('109-86-4', '2-Methoxy-ethanol (Ethylene Glycol Monomethyl Ether)'),
    ('872-50-4', '1-Methyl-2-pyrrolidinone (NMP, N Mehtylpyrrolidinone, N-methyl-2-pyrrolidone)'),
    ('64-19-7', 'Acetic Acid (Glacial Acetic Acic, ethanoic acid)'),
    ('75-05-8', 'Acetonitrile'),
    ('7664-41-7', 'Ammonia (ammonium hydroxide)'),
    ('71-36-3', '1 Butanol (Butanol, n-butanol, Butan-1-ol)'),
    ('628-63-7', 'Amyl Acetate (1-Pentyl Acetate)'),
    ('100-66-3', 'Anisole (methoxybenzene)'),
    ('100-51-6', 'Benzyl Alcohol (phenylmethanol, phenyl carbinol)'),
    ('123-86-4', 'Butyl Acetate (n-BuOAc)'),
    ('75-15-0', 'Carbon Disulfide'),
    ('108-90-7', 'Chlorobenzene'),
    ('67-66-3', 'Chloroform'),
    ('124-38-9', 'CO2 Sc (Supercritical CO2)'),
    ('110-82-7', 'Cyclohexane (Hexahydrobenzene)'),
    ('108-94-1', 'Cyclohexanone'),
    ('5989-27-5', 'D Limonene (d-p-Mentha-1,8-diene 4-Isopropenyl-1-methylcyclohexene)'),
    ('107-06-2', 'DCE (Ethylene dichloride)'),
    ('123-42-2', 'Diacetone Alcohol (DAA)'),
    ('60-29-7', 'Diethyl Ether'),
    ('108-20-3', 'Diisopropyl Ether (Isoppropyl ether)'),
    ('115-10-6', 'Dimethyl Ether'),
    ('127-19-5', 'DMAc (N,N Dimethyl Acetamide)'),
    ('110-71-4', 'DME (1,2 Dimethoxyethane, Ethylene Glycol Dimethyl Ether)'),
    ('7226-23-5', "DMPU (1,3-Dimethyl-3,4,5,6-tetrahydro-2(1H)-pyrimidinone, N,N\'-dimethylpropylene"),
    ('67-68-5', 'DMSO (Dimethyl sulfoxide)'),
    ('64-17-5', 'Ethanol (Ethyl Alcohol)'),
    ('141-78-6', 'Ethyl Acetate'),
    ('107-21-1', 'Ethylene Glycol'),
    ('64-18-6', 'Formic Acid (Methanoic acid)'),
    ('108-21-4', 'Isopropyl Acetate (iPrOAc)'),
    ('67-63-0', 'Isopropyl Alcohol (Isopropanol, 2 propanol)'),
    ('96-47-9', 'Me THF (2-Methyltetrahydrofuran)'),
    ('67-56-1', 'Methanol (Methyl Alcohol)'),
    ('79-20-9', 'Methyl Acetate'),
    ('78-93-3', 'Methyl Ethyl Ketone (MEK, 2-Butanone)'),
    ('108-10-1', 'Methyl isobutyl ketone (MIBK, 4-Methyl-2-pentanone)'),
    ('108-87-2', 'Methylcyclohexane (Me-Cyclohexane)'),
    ('75-52-5', 'NitroMethane (Nitrocarbol)'),
    ('109-66-0', 'Pentane'),
    ('110-86-1', 'Pyridine'),
    ('64742-95-6', 'Shellsol A 100'),
    ('64742-94-5', 'Shellsol A 150'),
    ('126-33-0', 'Sulfolane (Tetramethylene sulfone)'),
    ('75-65-0', 'Tert-butanol (Tert-butyl alcohol, 2 methyl-2-propanol)'),
    ('109-99-9', 'Tetrahydrofuran (THF, Oxolane)'),
    ('102-71-6', 'Triethanolamine (TEA, Trolamine)'),
    ('8052-41-3', 'Turpenoid'),
    ('7732-18-5', 'Water'),
    ('1330-20-7', 'Xylene (1,2 Dimethylbenzene)'),
    ('77-93-0', 'Triethyl Acetate'),
    ('8006-64-2', 'Turpentine'),
]

OUT = Path(r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz5") / "priority62_auto_p2oasys_runs.json"


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
    hazard_data = atmo_gwp.merge_acid_rain_into_hazard_data(hazard_data, extra)

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
        "name": name,
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
