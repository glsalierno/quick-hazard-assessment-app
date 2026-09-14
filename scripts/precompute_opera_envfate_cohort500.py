
#!/usr/bin/env python3
"""Batch-precompute OPERA EnvFate + logP for cohort500 into opera_precompute.sqlite.

One OPERA invocation on a single .smi (not 500 serial calls). Resume-safe via SQLite.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config
from utils import opera_batch, opera_client
from utils.opera_precompute_cache import (
    count_cached_cas,
    default_precompute_db_path,
    get_cas_row,
    init_db,
    is_cas_cached,
    put_cas_row,
)


def _load_cas_list(path: Path) -> list[str]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "cas_list" in data:
            return [str(c).strip() for c in data["cas_list"] if str(c).strip()]
        if isinstance(data, list):
            return [str(c).strip() for c in data if str(c).strip()]
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str)
        for c in ("CAS", "cas", "casrn", "CASRN"):
            if c in df.columns:
                return [str(x).strip() for x in df[c].dropna() if str(x).strip()]
    lines = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        c = line.strip().split("#", 1)[0].strip()
        if c:
            lines.append(c)
    return lines


def _resolve_smiles(cas_list: list[str], smiles_cache: Path | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if smiles_cache and smiles_cache.is_file():
        if smiles_cache.suffix.lower() == ".json":
            raw = json.loads(smiles_cache.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for k, v in raw.items():
                    if v:
                        out[str(k).strip()] = str(v).strip()
        else:
            for line in smiles_cache.read_text(encoding="utf-8", errors="replace").splitlines():
                if not line.strip() or line.startswith("#"):
                    continue
                parts = line.split("\t") if "\t" in line else line.split(",")
                if len(parts) >= 2:
                    # SMILES\tCAS or CAS\tSMILES
                    a, b = parts[0].strip(), parts[1].strip()
                    if a.count("-") >= 2 and not b.count("-") >= 2:
                        out[a] = b
                    else:
                        out[b] = a  # SMILES\tCAS
    missing = [c for c in cas_list if c not in out]
    if missing:
        print(f"Resolving {len(missing)} CAS via PubChem/local batch…")
        fetched = opera_batch.smiles_from_cas_batch(missing)
        for c, sm in fetched.items():
            if sm:
                out[c] = sm
    return out


def _run_opera_batch(
    smi_path: Path,
    out_csv: Path,
    *,
    endpoints: list[str],
    timeout: int,
    work_dir: Path,
) -> dict:
    exe = opera_client.find_opera_executable()
    if not exe:
        return {"ok": False, "error": "opera_not_found"}
    return opera_client.run_opera_on_paths(
        smi_path,
        out_csv,
        opera_exe=exe,
        timeout_seconds=timeout,
        cwd=str(opera_client._opera_bundle_root(exe)),
        endpoints=endpoints,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--cas-file",
        type=Path,
        default=Path(r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz5\report_expert_vs_auto_cohort500_caslist.json"),
    )
    ap.add_argument("--cache-db", type=Path, default=default_precompute_db_path())
    ap.add_argument("--work-dir", type=Path, default=None)
    ap.add_argument("--smiles-cache", type=Path, default=None)
    ap.add_argument("--endpoints", nargs="+", default=["EnvFate", "logP"])
    ap.add_argument("--timeout", type=int, default=7200)
    ap.add_argument("--force", action="store_true", help="Recompute even if cached")
    ap.add_argument("--copy-to", type=Path, nargs="*", default=None, help="Extra sqlite destinations")
    ap.add_argument("--max-cas", type=int, default=None)
    args = ap.parse_args()

    work = args.work_dir or (Path(config.DATA_DIR) / "opera_envfate_cohort500")
    work.mkdir(parents=True, exist_ok=True)
    log_path = work / "precompute_failures.jsonl"
    report_meta = work / "precompute_meta.json"

    cas_list = _load_cas_list(args.cas_file)
    if args.max_cas:
        cas_list = cas_list[: args.max_cas]
    print(f"CAS loaded: {len(cas_list)} from {args.cas_file}")

    smiles_map = _resolve_smiles(cas_list, args.smiles_cache)
    # Persist smiles map for resume
    (work / "cohort500_smiles.json").write_text(json.dumps(smiles_map, indent=2), encoding="utf-8")

    no_smiles = [c for c in cas_list if not smiles_map.get(c)]
    with_smiles = [(c, smiles_map[c]) for c in cas_list if smiles_map.get(c)]
    print(f"With SMILES: {len(with_smiles)} | skip no-SMILES: {len(no_smiles)}")
    for c in no_smiles:
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"cas": c, "error": "no_smiles"}) + "\n")

    init_db(args.cache_db)
    pending = []
    for cas, sm in with_smiles:
        if (not args.force) and is_cas_cached(args.cache_db, cas, sm):
            # Also accept cache hit by CAS alone if row already has EnvFate-ish columns
            row = get_cas_row(args.cache_db, cas, sm)
            if row and (row.get("LogP_pred") is not None or row.get("LogBCF_pred") is not None):
                continue
        pending.append((cas, sm))
    print(f"Pending OPERA: {len(pending)} | already cached: {len(with_smiles) - len(pending)}")

    t0 = time.time()
    written = 0
    failed: list[dict] = []

    if pending:
        smi_path = work / "cohort500_envfate.smi"
        out_csv = work / "opera_envfate_cohort500.csv"
        lines = [f"{sm}\t{cas}" for cas, sm in pending]
        smi_path.write_text("\n".join(lines) + "\n", encoding="ascii", errors="replace")
        print(f"Wrote {smi_path} ({len(lines)} mols)")
        print(f"Running OPERA endpoints={args.endpoints} timeout={args.timeout}s …")
        batch = _run_opera_batch(
            smi_path,
            out_csv,
            endpoints=list(args.endpoints),
            timeout=int(args.timeout),
            work_dir=work,
        )
        if not batch.get("ok"):
            print("BATCH FAILED:", batch.get("error"))
            # Fallback: try smaller chunks of 50
            chunk = 50
            for i in range(0, len(pending), chunk):
                sub = pending[i : i + chunk]
                sub_smi = work / f"cohort_chunk_{i}.smi"
                sub_out = work / f"opera_chunk_{i}.csv"
                sub_smi.write_text(
                    "\n".join(f"{sm}\t{cas}" for cas, sm in sub) + "\n",
                    encoding="ascii",
                    errors="replace",
                )
                print(f"Chunk {i}-{i+len(sub)} …")
                br = _run_opera_batch(
                    sub_smi,
                    sub_out,
                    endpoints=list(args.endpoints),
                    timeout=int(args.timeout),
                    work_dir=work,
                )
                if not br.get("ok"):
                    for cas, sm in sub:
                        failed.append({"cas": cas, "error": br.get("error")})
                        with log_path.open("a", encoding="utf-8") as fh:
                            fh.write(json.dumps({"cas": cas, "error": br.get("error")}) + "\n")
                    continue
                rows = br.get("rows") or []
                # Map by MoleculeID (CAS) when present
                by_id = {}
                for r in rows:
                    mid = str(r.get("MoleculeID") or r.get("molecule_id") or "").strip()
                    if mid:
                        by_id[mid] = r
                for cas, sm in sub:
                    row = by_id.get(cas)
                    if not row and len(rows) == len(sub):
                        # positional fallback
                        idx = sub.index((cas, sm))
                        row = rows[idx] if idx < len(rows) else None
                    if not row:
                        failed.append({"cas": cas, "error": "missing_row"})
                        with log_path.open("a", encoding="utf-8") as fh:
                            fh.write(json.dumps({"cas": cas, "error": "missing_row"}) + "\n")
                        continue
                    put_cas_row(args.cache_db, cas, sm, row)
                    written += 1
        else:
            rows = batch.get("rows") or []
            print(f"OPERA returned {len(rows)} rows")
            by_id = {}
            for r in rows:
                mid = str(r.get("MoleculeID") or "").strip()
                if mid:
                    by_id[mid] = r
            for cas, sm in pending:
                row = by_id.get(cas)
                if not row:
                    failed.append({"cas": cas, "error": "missing_row"})
                    with log_path.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"cas": cas, "error": "missing_row"}) + "\n")
                    continue
                put_cas_row(args.cache_db, cas, sm, row)
                written += 1

    elapsed = time.time() - t0
    n_cached = count_cached_cas(args.cache_db)
    meta = {
        "cas_total": len(cas_list),
        "with_smiles": len(with_smiles),
        "no_smiles": len(no_smiles),
        "pending": len(pending),
        "written": written,
        "failed": len(failed),
        "elapsed_sec": elapsed,
        "cache_db": str(args.cache_db),
        "cached_total": n_cached,
        "endpoints": args.endpoints,
    }
    report_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(json.dumps(meta, indent=2))

    # Copy sqlite to extra destinations
    copies = list(args.copy_to or [])
    g5 = Path(r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz5\quick-hazard-assessment-app\data\opera_precompute.sqlite")
    if g5.parent.is_dir() and str(g5) not in copies:
        copies.append(g5)
    import shutil
    for dest in copies:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.resolve() != Path(args.cache_db).resolve():
            shutil.copy2(args.cache_db, dest)
            print(f"Copied sqlite -> {dest}")

    return 0 if written or n_cached else 1


if __name__ == "__main__":
    raise SystemExit(main())
