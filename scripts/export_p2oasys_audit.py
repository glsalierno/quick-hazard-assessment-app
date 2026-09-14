#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_APP_ROOT = _SCRIPT_DIR.parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))
os.chdir(_APP_ROOT)

import config
from services.chemical_assessment import ChemicalAssessmentService
from utils import hazard_for_p2oasys, p2oasys_matrix_placeholder, p2oasys_scorer
from utils.p2oasys_extras_merge import merge_extra_sources_for_cas


def _load_cas_list(input_csv: Path | None, limit: int) -> list[str]:
    cas_values: list[str] = []
    if input_csv is not None:
        df = pd.read_csv(input_csv, dtype=str)
        if "CAS" not in df.columns:
            raise ValueError(f"Input CSV missing required CAS column: {input_csv}")
        cas_values = [str(x).strip() for x in df["CAS"].tolist() if str(x).strip()]
    else:
        text = sys.stdin.read()
        cas_values = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]

    seen: set[str] = set()
    deduped: list[str] = []
    for cas in cas_values:
        if cas not in seen:
            seen.add(cas)
            deduped.append(cas)
    if limit > 0:
        deduped = deduped[:limit]
    return deduped


def _safe_raw_payload(result: Any) -> dict[str, Any]:
    return {
        "identity": {
            "cas": result.identity.cas,
            "chemical_name": result.identity.chemical_name,
            "dtxsid": result.identity.dtxsid,
            "source": getattr(result.identity.source, "value", str(result.identity.source)),
        },
        "pubchem_data": result.pubchem_data,
        "dsstox_info": result.dsstox_info,
        "toxval_data": result.toxval_data,
        "carc_potency_data": result.carc_potency_data,
        "fetch_error": result.fetch_error,
    }


def _write_error_line(error_log: Path, cas: str, exc: Exception) -> None:
    error_log.parent.mkdir(parents=True, exist_ok=True)
    with error_log.open("a", encoding="utf-8") as ef:
        ef.write(f"[{datetime.now().isoformat(timespec='seconds')}] CAS={cas}\n")
        ef.write(f"{type(exc).__name__}: {exc}\n")
        ef.write(traceback.format_exc())
        ef.write("\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Export P2OASys scoring audit JSONL for CAS list.")
    ap.add_argument("--input", type=Path, default=None, help="CSV file with CAS column. If omitted, reads CAS from stdin.")
    ap.add_argument("--output", type=Path, default=Path("p2oasys_audit.jsonl"), help="Output JSONL path.")
    ap.add_argument("--limit", type=int, default=0, help="Max number of CAS to process (0=all).")
    ap.add_argument("--verbose", action="store_true", help="Verbose stderr progress and errors.")
    ap.add_argument("--error-log", type=Path, default=None, help="Optional error log file.")
    args = ap.parse_args()

    cas_list = _load_cas_list(args.input, args.limit)
    if not cas_list:
        raise SystemExit("No CAS to process.")

    matrix_path, _kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH), Path(config.DATA_DIR)
    )
    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    svc = ChemicalAssessmentService()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    n = len(cas_list)
    with args.output.open("w", encoding="utf-8") as out_f:
        for i, cas in enumerate(cas_list, start=1):
            record: dict[str, Any] = {
                "cas": cas,
                "raw_data": None,
                "p2oasys_input": None,
                "final_scores": None,
                "pipeline_note": "",
                "error": None,
            }
            try:
                ar = svc.assess(cas)
                record["raw_data"] = _safe_raw_payload(ar)
                if ar.fetch_error or not ar.pubchem_data:
                    record["error"] = ar.fetch_error or "No PubChem data"
                else:
                    resolved = str(ar.identity.cas or cas).strip()
                    extra_sources, pipeline_note = merge_extra_sources_for_cas(resolved)
                    hazard_data = hazard_for_p2oasys.build_hazard_data(
                        ar.pubchem_data,
                        toxval_data=ar.toxval_data,
                        carc_potency_data=ar.carc_potency_data,
                        extra_sources=extra_sources,
                    )
                    scores = p2oasys_scorer.compute_p2oasys_scores(hazard_data, matrix)
                    record["p2oasys_input"] = hazard_data
                    record["final_scores"] = scores
                    record["pipeline_note"] = pipeline_note
            except Exception as exc:  # noqa: BLE001
                record["error"] = f"{type(exc).__name__}: {exc}"
                if args.error_log is not None:
                    _write_error_line(args.error_log, cas, exc)
                if args.verbose:
                    print(f"\n[{i}/{n}] {cas} failed: {exc}", file=sys.stderr)

            out_f.write(json.dumps(record, ensure_ascii=True, default=str) + "\n")

            if args.verbose and i % 10 == 0:
                print(".", end="", file=sys.stderr, flush=True)
            if args.verbose and i % 100 == 0:
                print(f" {i}/{n}", file=sys.stderr, flush=True)

    if args.verbose:
        print(f"\nWrote audit JSONL: {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
