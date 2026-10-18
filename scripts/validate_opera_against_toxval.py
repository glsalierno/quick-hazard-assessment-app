"""
Validate OPERA predictions against local ToxVal numeric records.

By default reads OPERA values from the precompute SQLite cache (no live OPERA).
Populate the cache first::

    python scripts/precompute_opera_for_cas_list.py --max-cas 200 --chunk-size 20

Then::

    python scripts/validate_opera_against_toxval.py --sample-size 200 --max-cas 500

Use ``--allow-live-opera`` only for small tests (each OPERA run is slow).
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from utils import opera_batch
from utils.opera_mapper import load_mapping
from utils.opera_precompute_cache import default_precompute_db_path, get_opera_value, init_db, put_batch_dataframe


def _r2_rmse(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float]:
    if len(y_true) < 2:
        return float("nan"), float("nan")
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else float("nan")
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    return r2, rmse


def _fraction_within_factor(y_true: np.ndarray, y_pred: np.ndarray, factor: float) -> float:
    if len(y_true) == 0:
        return float("nan")
    yt = np.clip(y_true, 1e-12, None)
    yp = np.clip(y_pred, 1e-12, None)
    ratio = np.maximum(yp / yt, yt / yp)
    return float(np.mean(ratio <= factor))


def _query_toxval_rows(db_path: Path, toxval_types: list[str], sample_size: int, *, shuffle: bool) -> pd.DataFrame:
    if not db_path.is_file() or not toxval_types:
        return pd.DataFrame()
    con = sqlite3.connect(db_path)
    try:
        marks = ",".join("?" for _ in toxval_types)
        order = "ORDER BY RANDOM()" if shuffle else "ORDER BY casrn, rowid"
        q = f"""
            SELECT casrn, study_type, toxval_numeric
            FROM toxvaldb
            WHERE LOWER(TRIM(study_type)) IN ({marks})
              AND toxval_numeric IS NOT NULL
              AND CAST(toxval_numeric AS TEXT) != ''
              AND casrn IS NOT NULL
              AND LENGTH(TRIM(CAST(casrn AS TEXT))) > 0
            {order}
            LIMIT ?
        """
        args = [t.lower() for t in toxval_types] + [int(sample_size)]
        return pd.read_sql_query(q, con, params=args)
    finally:
        con.close()


def _ensure_smiles_map(cas_list: list[str], smiles_cache: dict[str, str | None]) -> None:
    missing = [c for c in cas_list if c not in smiles_cache]
    if not missing:
        return
    resolved = opera_batch.smiles_from_cas_batch(missing)
    for c in missing:
        smiles_cache[c] = resolved.get(c)


def _fill_missing_from_opera(
    cas_list: list[str],
    smiles_map: dict[str, str | None],
    cache_path: Path,
    endpoint: str,
    *,
    allow_live: bool,
) -> None:
    """Write missing CAS into precompute cache using one batch_predict (if allow_live)."""
    need: list[tuple[str, str]] = []
    for cas in cas_list:
        sm = (smiles_map.get(cas) or "").strip()
        if not sm:
            continue
        if get_opera_value(cache_path, cas, endpoint) is not None:
            continue
        need.append((cas, sm))
    if not need or not allow_live:
        return
    cas_chunk = [c for c, _ in need]
    sm_chunk = [s for _, s in need]
    mids = [f"v_{c.replace('-', '_')}" for c in cas_chunk]
    br = opera_batch.batch_predict(
        sm_chunk,
        molecule_ids=mids,
        cas_for_cache=cas_chunk,
        sqlite_cache_path=None,
        disk_cache=True,
        fallback_single_on_error=True,
    )
    if not br.df.empty:
        put_batch_dataframe(cache_path, br.df)


def _run_endpoint(
    endpoint: str,
    tox_types: list[str],
    chem_db: Path,
    sample_size: int,
    cache_path: Path,
    smiles_cache: dict[str, str | None],
    *,
    max_cas: int | None,
    allow_live_opera: bool,
    shuffle_toxval: bool,
) -> tuple[pd.DataFrame, dict]:
    raw = _query_toxval_rows(chem_db, tox_types, sample_size, shuffle=shuffle_toxval)
    if raw.empty:
        return pd.DataFrame(), {}
    raw["toxval_numeric"] = pd.to_numeric(raw["toxval_numeric"], errors="coerce")
    raw = raw.dropna(subset=["toxval_numeric"])
    if raw.empty:
        return pd.DataFrame(), {}
    tmp = raw.assign(CAS=raw["casrn"].astype(str).str.strip())
    grouped = tmp.groupby("CAS", as_index=False)["toxval_numeric"].median()
    grouped = grouped.rename(columns={"toxval_numeric": "toxval_median"})
    if max_cas is not None and len(grouped) > max_cas:
        grouped = grouped.sample(n=int(max_cas), random_state=42).reset_index(drop=True)

    cas_list = grouped["CAS"].astype(str).tolist()
    _ensure_smiles_map(cas_list, smiles_cache)
    grouped["SMILES"] = grouped["CAS"].astype(str).map(lambda c: smiles_cache.get(c))
    grouped = grouped[grouped["SMILES"].notna()]
    if grouped.empty:
        return pd.DataFrame(), {}

    init_db(cache_path)
    _fill_missing_from_opera(
        grouped["CAS"].astype(str).tolist(),
        smiles_cache,
        cache_path,
        endpoint,
        allow_live=allow_live_opera,
    )

    opera_vals: list[float] = []
    for cas in grouped["CAS"].astype(str):
        s = get_opera_value(cache_path, cas.strip(), endpoint)
        opera_vals.append(float(pd.to_numeric(s, errors="coerce")) if s is not None else float("nan"))
    grouped["opera_value"] = opera_vals
    joined = grouped.dropna(subset=["opera_value"])
    if joined.empty:
        return pd.DataFrame(), {}

    y_true = joined["toxval_median"].to_numpy(dtype=float)
    y_pred = joined["opera_value"].to_numpy(dtype=float)
    r2, rmse = _r2_rmse(y_true, y_pred)
    metrics = {
        "endpoint": endpoint,
        "n_compounds": int(len(joined)),
        "r2": r2,
        "rmse": rmse,
        "agreement_2x": _fraction_within_factor(y_true, y_pred, 2.0),
        "agreement_5x": _fraction_within_factor(y_true, y_pred, 5.0),
        "agreement_10x": _fraction_within_factor(y_true, y_pred, 10.0),
    }
    return joined, metrics


def _plot_scatter(df: pd.DataFrame, endpoint: str, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(6, 5))
    x = np.clip(df["toxval_median"].to_numpy(dtype=float), 1e-12, None)
    y = np.clip(df["opera_value"].to_numpy(dtype=float), 1e-12, None)
    ax.scatter(x, y, alpha=0.7, s=20)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("ToxVal median (experimental)")
    ax.set_ylabel("OPERA prediction (cache)")
    ax.set_title(endpoint)
    lo = min(float(np.min(x)), float(np.min(y)))
    hi = max(float(np.max(x)), float(np.max(y)))
    ax.plot([lo, hi], [lo, hi], linestyle="--")
    fig.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in endpoint)[:80]
    fig.savefig(out_dir / f"{safe}_scatter.png", dpi=150)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample-size",
        type=int,
        default=500,
        help="Max ToxVal rows to pull per endpoint (see --shuffle-toxval)",
    )
    parser.add_argument("--max-endpoints", type=int, default=10)
    parser.add_argument("--max-cas", type=int, default=None, help="Cap distinct CAS after median aggregation")
    parser.add_argument("--mapping-json", type=Path, default=Path(config.DATA_DIR) / "opera_to_toxval_mapping.json")
    parser.add_argument("--chem-db", type=Path, default=Path(config.CHEMICAL_DB_PATH))
    parser.add_argument("--cache-db", type=Path, default=default_precompute_db_path(), help="OPERA precompute SQLite")
    parser.add_argument(
        "--allow-live-opera",
        action="store_true",
        help="Run OPERA for CAS missing from cache (slow; small samples only)",
    )
    parser.add_argument(
        "--shuffle-toxval",
        action="store_true",
        help="Sample ToxVal with ORDER BY RANDOM() (slow on large databases)",
    )
    parser.add_argument("--out-csv", type=Path, default=Path(config.DATA_DIR) / "opera_toxval_validation_summary.csv")
    parser.add_argument("--plots-dir", type=Path, default=Path(config.DATA_DIR) / "opera_toxval_plots")
    parser.add_argument("--no-tqdm", action="store_true")
    args = parser.parse_args()

    mapping = load_mapping(args.mapping_json).get("mapping", {})
    rows: list[dict] = []
    plotted = 0
    smiles_cache: dict[str, str | None] = {}

    ep_items = list(mapping.items())
    if not args.no_tqdm:
        try:
            from tqdm import tqdm

            ep_items = tqdm(ep_items, desc="Endpoints", unit="ep")
        except Exception:
            pass

    for endpoint, payload in ep_items:
        tox_types = [t for t in payload.get("toxval_types", []) if t]
        if not tox_types:
            continue
        joined, metrics = _run_endpoint(
            endpoint,
            tox_types,
            args.chem_db,
            args.sample_size,
            args.cache_db,
            smiles_cache,
            max_cas=args.max_cas,
            allow_live_opera=args.allow_live_opera,
            shuffle_toxval=args.shuffle_toxval,
        )
        if not metrics:
            continue
        rows.append(metrics)
        if plotted < args.max_endpoints and len(joined) >= 8:
            _plot_scatter(joined, endpoint, args.plots_dir)
            plotted += 1

    out_df = pd.DataFrame(rows).sort_values("n_compounds", ascending=False) if rows else pd.DataFrame()
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False)
    print(f"Wrote {len(out_df)} endpoint summary rows to {args.out_csv}")
    print(f"Scatter plots directory: {args.plots_dir}")
    print(f"OPERA cache DB: {args.cache_db} (use scripts/precompute_opera_for_cas_list.py to populate)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
