"""
Live QA assessment for CAS 381-73-7 (GHhaz6). Writes validation snapshot + summary JSON.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.chemical_assessment import ChemicalAssessmentService
from utils import data_formatter, ghs_formatter, qa_checks, validation_snapshot

CONTAMINATION_PHRASES = (
    "Flammable liquids",
    "Skin corrosion",
    "Serious eye damage",
    "Category 1",
    "Category 4",
)


def _endpoint_text_blob(entry: dict) -> str:
    parts = []
    for k in ("endpoint", "type", "value", "unit", "species", "route", "text", "raw_text", "description"):
        v = entry.get(k)
        if v is not None:
            parts.append(str(v))
    return " ".join(parts)


def main() -> int:
    cas = "381-73-7"
    expected_h = {"H314", "H227", "H318", "H402", "H410"}
    url = "http://localhost:8502"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    svc = ChemicalAssessmentService()
    ar = svc.assess(cas)
    result = svc.to_result_data(ar)

    pubchem = result.get("pubchem") or {}
    ghs = pubchem.get("ghs") or {}
    h_codes = list(ghs.get("h_codes") or [])
    p_codes = list(ghs.get("p_codes") or [])
    accounting = ghs_formatter.account_ghs_codes(h_codes, p_codes)
    eco = pubchem.get("ecotoxicity") or {}
    entries = list(eco.get("entries") or [])
    prioritized = data_formatter.prioritize_toxicity_data(pubchem, result.get("toxval_data"))
    qa_status = qa_checks.build_qa_status(result)

    # OPERA (optional; may be slow)
    opera_panel: dict = {"available": False, "warnings": [], "skipped": True}
    smiles = (pubchem.get("smiles") or "").strip()
    if smiles and os.environ.get("HAZQUERY_SKIP_OPERA", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        try:
            from utils import opera_client

            if opera_client.is_opera_available():
                opera_panel = opera_client.get_opera_predictions(
                    smiles,
                    cas,
                    pubchem_xlogp=pubchem.get("xlogp"),
                ) or {}
                opera_panel["available"] = bool(opera_panel.get("ok"))
                opera_panel["skipped"] = False
            else:
                opera_panel = {
                    "available": False,
                    "warnings": ["OPERA not installed / not found"],
                    "skipped": False,
                }
        except Exception as exc:
            opera_panel = {
                "available": False,
                "warnings": [f"OPERA error: {exc}"],
                "skipped": False,
            }

    snap_path = validation_snapshot.write_validation_snapshot(
        result,
        opera_panel=opera_panel,
        filename=f"{cas}_v6_result_data.json",
    )

    contaminated = []
    for e in entries:
        blob = _endpoint_text_blob(e if isinstance(e, dict) else {})
        for phrase in CONTAMINATION_PHRASES:
            if phrase.lower() in blob.lower():
                contaminated.append({"phrase": phrase, "entry": e})

    blank_pri = [
        r
        for r in (prioritized.get("quantitative") or []) + (prioritized.get("categorical") or [])
        if not str(r.get("endpoint") or r.get("type") or "").strip()
        and not str(r.get("value") or "").strip()
    ]

    preferred = (result.get("preferred_name") or "").strip()
    name_ok = bool(preferred) or bool(
        (pubchem.get("title") or pubchem.get("iupac_name") or "").strip()
    )
    # Accept if preferred filled OR (titles exist but preferred blank is a FAIL)
    preferred_status = "ok" if preferred else ("fail_blank" if name_ok else "sparse_ok")

    summary = {
        "validated_at_utc": now,
        "app_url": url,
        "cas": cas,
        "preferred_name": preferred,
        "pubchem_cid": pubchem.get("cid"),
        "iupac_name": pubchem.get("iupac_name"),
        "pubchem_title": pubchem.get("title"),
        "retrieved_h_codes": accounting["retrieved_h_codes"],
        "displayed_h_codes": accounting["displayed_h_codes"],
        "unmapped_h_codes": accounting["unmapped_h_codes"],
        "suppressed_or_subsumed_h_codes": accounting["suppressed_or_subsumed_h_codes"],
        "h_complete": accounting["h_complete"],
        "expected_h_present": sorted(expected_h & set(accounting["retrieved_h_codes"])),
        "expected_h_missing_from_retrieved": sorted(expected_h - set(accounting["retrieved_h_codes"])),
        "ecotox_entry_count": len(entries),
        "ecotox_contamination_hits": contaminated,
        "aquatic_ghs_codes": eco.get("h_codes_aquatic") or [],
        "aquatic_ghs_classification_only": eco.get("aquatic_ghs_classification_only"),
        "blank_prioritized_rows": len(blank_pri),
        "incomplete_excluded": len(prioritized.get("incomplete") or []),
        "preferred_name_status": preferred_status,
        "opera": {
            "available": opera_panel.get("available"),
            "skipped": opera_panel.get("skipped"),
            "warnings": opera_panel.get("warnings") or [],
            "ok": opera_panel.get("ok"),
            "error": opera_panel.get("error"),
        },
        "qa_ok": qa_status.get("ok"),
        "qa_warnings": qa_status.get("warnings") or [],
        "snapshot_file": snap_path.name,
        "fetch_error": result.get("fetch_error"),
    }

    out_summary = APP_ROOT / "validation_snapshots" / f"{cas}_v6_validation_summary.json"
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"\nSnapshot: {snap_path}")
    print(f"Summary:  {out_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
