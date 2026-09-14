"""
Pre-compute OPERA predictions for many CAS and store them in SQLite.

Reads unique CAS from ToxVal SQLite, samples CAS by endpoint mapping, or from a file.
Runs ``batch_predict`` in chunks (``--chunk-size`` / ``--batch-size``, default 20).
Optional ``--parallel`` uses process workers; SQLite uses WAL + write retries.

Examples::

    python scripts/precompute_opera_for_cas_list.py --max-cas 40 --chunk-size 10
    python scripts/precompute_opera_for_cas_list.py --cas-file path/to/cas_list.txt
    python scripts/precompute_opera_for_cas_list.py --sample-from-toxval --endpoints LogP_pred CATMoS_LD50_pred --n-per-endpoint 50
    # Linux/macOS only — parallel OPERA; on Windows omit --parallel or add --parallel-opera-unsafe
    python scripts/precompute_opera_for_cas_list.py --sample-from-toxval --endpoints LogP LD50 --parallel --max-workers 4 --n-per-endpoint 30

PowerShell: ``tqdm`` writes to stderr; ``2>&1 | Tee-Object`` may show ``NativeCommandError`` for progress lines even when the job is fine. Use ``--no-tqdm`` for quieter logs, or ``*> logs\\run.log`` to merge streams without Tee.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from utils import opera_batch
from utils.opera_precompute_cache import default_precompute_db_path, init_db, is_cas_cached, put_batch_dataframe
from utils.opera_precompute_worker import run_one_batch
from utils.toxval_sampler import load_mapping_dict, sample_cas_for_endpoints


def _unique_cas_from_toxval(chem_db: Path, max_cas: int | None) -> list[str]:
    if not chem_db.is_file():
        return []
    con = sqlite3.connect(chem_db)
    try:
        q = """
            SELECT DISTINCT TRIM(CAST(casrn AS TEXT)) AS cas
            FROM toxvaldb
            WHERE casrn IS NOT NULL AND LENGTH(TRIM(CAST(casrn AS TEXT))) > 0
            ORDER BY cas
        """
        df = pd.read_sql_query(q, con)
    finally:
        con.close()
    cas_list = [str(x).strip() for x in df["cas"].dropna().tolist() if str(x).strip()]
    if max_cas is not None:
        cas_list = cas_list[: int(max_cas)]
    return cas_list


def _unique_cas_from_file(path: Path, max_cas: int | None) -> list[str]:
    if not path.is_file():
        return []
    found: list[str] = []
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str, low_memory=False)
        col = None
        for c in ("CAS", "cas", "casrn", "CASRN"):
            if c in df.columns:
                col = c
                break
        if col is None:
            return []
        for raw in df[col].dropna().astype(str):
            c = raw.strip()
            if c and c not in found:
                found.append(c)
    else:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            c = line.strip().split("#", 1)[0].strip()
            if c and c not in found:
                found.append(c)
    if max_cas is not None:
        found = found[: int(max_cas)]
    return found


def _run_batches_sequential(
    pending: list[tuple[str, str]],
    cache_db: Path,
    batch_size: int,
    use_tqdm: bool,
) -> tuple[int, list[str]]:
    chunk = max(1, int(batch_size))
    iterator = range(0, len(pending), chunk)
    tqdm_fn = None
    if use_tqdm:
        try:
            from tqdm import tqdm

            tqdm_fn = tqdm
        except Exception:
            tqdm_fn = None
    if tqdm_fn:
        iterator = tqdm_fn(list(iterator), desc="OPERA batches", unit="batch")

    total_written = 0
    all_err: list[str] = []
    for start in iterator:
        batch = pending[start : start + chunk]
        cas_chunk = [c for c, _ in batch]
        sm_chunk = [s for _, s in batch]
        mids = [f"pc_{c.replace('-', '_')}_{int(time.time() * 1000)}" for c in cas_chunk]
        br = opera_batch.batch_predict(
            sm_chunk,
            molecule_ids=mids,
            cas_for_cache=cas_chunk,
            sqlite_cache_path=None,
            disk_cache=True,
            fallback_single_on_error=True,
        )
        if br.df.empty:
            all_err.extend(br.errors or ["empty_batch"])
            print("FAILED CAS chunk:", ", ".join(cas_chunk))
            continue
        total_written += put_batch_dataframe(cache_db, br.df)
        all_err.extend(br.errors or [])
        # Identify CAS that did not return rows (often CDK descriptor failures).
        returned = set(br.df["input_smiles"].astype(str).tolist()) if "input_smiles" in br.df.columns else set()
        missing_cas = [cas_chunk[i] for i, sm in enumerate(sm_chunk) if sm not in returned]
        if missing_cas:
            print("FAILED CAS in chunk:", ", ".join(missing_cas))
    return total_written, all_err


def _run_batches_parallel(
    pending: list[tuple[str, str]],
    cache_db: Path,
    batch_size: int,
    max_workers: int,
    use_tqdm: bool,
) -> tuple[int, list[str]]:
    chunk = max(1, int(batch_size))
    tasks: list[tuple[str, list[str], list[str], list[str], str]] = []
    for start in range(0, len(pending), chunk):
        batch = pending[start : start + chunk]
        cas_chunk = [c for c, _ in batch]
        sm_chunk = [s for _, s in batch]
        wid = f"{os.getpid()}_{start}_{int(time.time() * 1000)}"
        mids = [f"{wid}_{i}_{c.replace('-', '_')}" for i, c in enumerate(cas_chunk)]
        tasks.append((str(cache_db), cas_chunk, sm_chunk, mids, wid))

    total_written = 0
    all_err: list[str] = []
    workers = max(1, int(max_workers))

    futures = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for t in tasks:
            futures.append(ex.submit(run_one_batch, t))
        if use_tqdm:
            try:
                from tqdm import tqdm

                it = tqdm(as_completed(futures), total=len(futures), desc="Parallel batches", unit="batch")
            except Exception:
                it = as_completed(futures)
        else:
            it = as_completed(futures)
        for fut in it:
            try:
                res = fut.result()
                total_written += int(res.get("written") or 0)
                all_err.extend(res.get("errors") or [])
            except Exception as e:
                all_err.append(str(e))
    return total_written, all_err


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-compute OPERA rows for CAS list into SQLite.")
    parser.add_argument(
        "--chem-db",
        "--toxval-db",
        type=Path,
        dest="chem_db",
        default=Path(config.CHEMICAL_DB_PATH),
        help="SQLite with toxvaldb.casrn (same as local ToxVal / chemical DB)",
    )
    parser.add_argument(
        "--cas-file",
        "--cas-list-file",
        type=Path,
        dest="cas_file",
        default=None,
        help="Optional .txt (one CAS per line) or .csv with CAS column",
    )
    parser.add_argument("--cache-db", type=Path, default=default_precompute_db_path(), help="Output SQLite (same as OPERA_PRECOMPUTE_DB_PATH)")
    parser.add_argument("--chunk-size", "--batch-size", type=int, default=20, dest="chunk_size", help="CAS per OPERA batch")
    parser.add_argument("--max-cas", type=int, default=None, help="Cap total CAS when not using --sample-from-toxval")
    parser.add_argument("--no-tqdm", action="store_true", help="Disable progress bar")
    parser.add_argument(
        "--sample-from-toxval",
        action="store_true",
        help="Sample CAS using opera_to_toxval_mapping.json and ToxVal types",
    )
    parser.add_argument(
        "--mapping-json",
        type=Path,
        default=Path(config.DATA_DIR) / "opera_to_toxval_mapping.json",
        help="Endpoint → toxval_types mapping",
    )
    parser.add_argument(
        "--endpoints",
        nargs="+",
        default=None,
        help="OPERA endpoint names or shorthands (e.g. LogP_pred LogP LD50)",
    )
    parser.add_argument("--n-per-endpoint", type=int, default=100, help="Distinct CAS per endpoint when sampling")
    parser.add_argument(
        "--no-shuffle-sample",
        action="store_true",
        help="Use deterministic CAS order for ToxVal sampling (faster on large DBs)",
    )
    parser.add_argument("--parallel", action="store_true", help="Run multiple batches in parallel processes")
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel worker count")
    parser.add_argument(
        "--exclude-dot-smiles",
        action="store_true",
        help="Skip SMILES containing '.' (salts/mixtures), which often fail OPERA/CDK",
    )
    parser.add_argument(
        "--parallel-opera-unsafe",
        action="store_true",
        help="On Windows, allow multiple OPERA processes at once (often unstable; default is sequential OPERA)",
    )
    args = parser.parse_args()

    if args.parallel and sys.platform == "win32" and not args.parallel_opera_unsafe:
        print(
            "Windows: multi-process OPERA is disabled by default (MATLAB/OPERA.exe conflicts cause batch failures). "
            "Running sequential OPERA batches. Re-run with --parallel-opera-unsafe if you accept the risk, "
            "or omit --parallel for the same behavior without this message."
        )
        args.parallel = False

    init_db(args.cache_db)

    if args.sample_from_toxval:
        if not args.endpoints:
            print("When using --sample-from-toxval, provide --endpoints …")
            return 1
        mapping = load_mapping_dict(args.mapping_json)
        cas_set = sample_cas_for_endpoints(
            args.chem_db,
            mapping,
            list(args.endpoints),
            n_per_endpoint=int(args.n_per_endpoint),
            shuffle=not args.no_shuffle_sample,
        )
        cas_list = sorted(cas_set)
        if args.max_cas is not None:
            cas_list = cas_list[: int(args.max_cas)]
        src = f"sample-from-toxval:{args.chem_db}"
    elif args.cas_file:
        cas_list = _unique_cas_from_file(args.cas_file, args.max_cas)
        src = f"file:{args.cas_file}"
    else:
        cas_list = _unique_cas_from_toxval(args.chem_db, args.max_cas)
        src = f"toxval:{args.chem_db}"

    if not cas_list:
        print(f"No CAS loaded from {src}")
        return 1

    smiles_map = opera_batch.smiles_from_cas_batch(cas_list)
    pending: list[tuple[str, str]] = []
    skipped_dot = 0
    for cas in cas_list:
        sm = smiles_map.get(cas) or ""
        if not sm:
            continue
        if args.exclude_dot_smiles and "." in sm:
            skipped_dot += 1
            continue
        if is_cas_cached(args.cache_db, cas, sm):
            continue
        pending.append((cas, sm))

    print(f"Source: {src} | with SMILES: {sum(1 for c in cas_list if smiles_map.get(c))} | to_compute: {len(pending)}")
    if skipped_dot:
        print(f"Skipped {skipped_dot} CAS with dot-disconnected SMILES (--exclude-dot-smiles)")
    if not pending:
        print("Nothing to compute (all cached).")
        print("Finished precompute run. All requested CAS were already cached.")
        return 0

    use_tqdm = not args.no_tqdm
    if args.parallel:
        written, errs = _run_batches_parallel(
            pending,
            args.cache_db,
            args.chunk_size,
            args.max_workers,
            use_tqdm,
        )
    else:
        written, errs = _run_batches_sequential(pending, args.cache_db, args.chunk_size, use_tqdm)

    if errs:
        print("Warnings/errors (first 10):", errs[:10])
    print(f"Done. Rows written/updated (CAS rows): {written}. Cache: {args.cache_db}")
    print("Finished precompute run. All batches processed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
