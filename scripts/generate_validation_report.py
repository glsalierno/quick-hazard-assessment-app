"""
Build OPERA vs ToxVal validation summary, plots, and a Markdown report.

Reads precomputed OPERA values from the SQLite cache (``OPERA_PRECOMPUTE_DB_PATH`` /
``data/opera_precompute.sqlite`` by default) and ToxVal from ``CHEMICAL_DB_PATH`` SQLite
or a CSV with CAS + type + numeric columns.

Examples::

    python scripts/generate_validation_report.py --min-pairs 10
    python scripts/generate_validation_report.py --endpoints CATMoS_LD50_pred FUB_pred --output-dir data
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from utils.opera_mapper import load_mapping
from utils.opera_precompute_cache import default_precompute_db_path, get_opera_value


def _r2_linear(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if len(y_true) < 2:
        return float("nan")
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    return 1.0 - (ss_res / ss_tot) if ss_tot > 0 else float("nan")


def _rmse(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) == 0:
        return float("nan")
    return float(np.sqrt(np.mean((a - b) ** 2)))


def _pct_within_factor(y_true: np.ndarray, y_pred: np.ndarray, factor: float) -> float:
    if len(y_true) == 0:
        return float("nan")
    yt = np.clip(np.abs(y_true), 1e-15, None)
    yp = np.clip(np.abs(y_pred), 1e-15, None)
    ratio = np.maximum(yp / yt, yt / yp)
    return float(np.mean(ratio <= factor))


def _toxval_cas_medians_sqlite(chem_db: Path, study_types_lower: list[str]) -> pd.DataFrame:
    import sqlite3

    if not chem_db.is_file() or not study_types_lower:
        return pd.DataFrame(columns=["CAS", "toxval_median"])
    marks = ",".join("?" for _ in study_types_lower)
    q = f"""
        SELECT TRIM(CAST(casrn AS TEXT)) AS CAS,
               AVG(CAST(toxval_numeric AS REAL)) AS toxval_median
        FROM toxvaldb
        WHERE LOWER(TRIM(study_type)) IN ({marks})
          AND toxval_numeric IS NOT NULL
          AND CAST(toxval_numeric AS TEXT) != ''
          AND casrn IS NOT NULL
        GROUP BY TRIM(CAST(casrn AS TEXT))
    """
    con = sqlite3.connect(chem_db)
    try:
        df = pd.read_sql_query(q, con, params=list(study_types_lower))
    finally:
        con.close()
    df["CAS"] = df["CAS"].astype(str).str.strip()
    df["toxval_median"] = pd.to_numeric(df["toxval_median"], errors="coerce")
    return df.dropna(subset=["toxval_median"])


def _toxval_cas_medians_csv(csv_path: Path, study_types_lower: list[str]) -> pd.DataFrame:
    if not csv_path.is_file():
        return pd.DataFrame(columns=["CAS", "toxval_median"])
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    cols_l = {c.lower(): c for c in df.columns}
    cas_col = cols_l.get("casrn") or cols_l.get("cas") or cols_l.get("cas_number")
    type_col = cols_l.get("study_type") or cols_l.get("toxval_type") or cols_l.get("endpoint")
    num_col = cols_l.get("toxval_numeric") or cols_l.get("value")
    if not cas_col or not type_col or not num_col:
        return pd.DataFrame(columns=["CAS", "toxval_median"])
    tmp = df[[cas_col, type_col, num_col]].copy()
    tmp["_t"] = tmp[type_col].astype(str).str.strip().str.lower()
    tmp = tmp[tmp["_t"].isin(study_types_lower)]
    tmp["_v"] = pd.to_numeric(tmp[num_col], errors="coerce")
    tmp = tmp.dropna(subset=["_v"])
    tmp["CAS"] = tmp[cas_col].astype(str).str.strip()
    g = tmp.groupby("CAS", as_index=False)["_v"].median().rename(columns={"_v": "toxval_median"})
    return g


def _load_toxval_medians(chem_path: Path, study_types_lower: list[str]) -> pd.DataFrame:
    suf = chem_path.suffix.lower()
    if suf in (".sqlite", ".db"):
        return _toxval_cas_medians_sqlite(chem_path, study_types_lower)
    return _toxval_cas_medians_csv(chem_path, study_types_lower)


def _use_log_space(endpoint: str) -> bool:
    ep = endpoint.lower()
    if "log" in ep and "logd" not in ep:
        return True
    if "logws" in ep or "logvp" in ep or "logbcf" in ep or "logkoa" in ep or "loghl" in ep:
        return True
    return False


def _plot_scatter(path_png: Path, x: np.ndarray, y: np.ndarray, title: str, log_axes: bool) -> None:
    path_png.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.scatter(x, y, alpha=0.65, s=22, edgecolors="none")
    if log_axes:
        ax.set_xscale("log")
        ax.set_yscale("log")
    ax.set_xlabel("ToxVal (median)")
    ax.set_ylabel("OPERA (precomputed)")
    ax.set_title(title[:120])
    lo = max(float(np.min(np.abs(x))), 1e-12)
    hi = max(float(np.max(np.abs(x))), float(np.max(np.abs(y))), 1e-12)
    if log_axes:
        ax.plot([lo, hi], [lo, hi], "k--", alpha=0.35, linewidth=1)
    fig.tight_layout()
    fig.savefig(path_png, dpi=150)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate OPERA vs ToxVal validation report.")
    parser.add_argument(
        "--toxval-db",
        type=Path,
        default=Path(config.CHEMICAL_DB_PATH),
        help="ToxVal SQLite (toxvaldb) or CSV with cas + type + numeric columns",
    )
    parser.add_argument(
        "--opera-cache",
        type=Path,
        default=default_precompute_db_path(),
        help="Precomputed OPERA SQLite (opera_cas_result / opera_predictions)",
    )
    parser.add_argument("--mapping-json", type=Path, default=Path(config.DATA_DIR) / "opera_to_toxval_mapping.json")
    parser.add_argument("--min-pairs", type=int, default=10)
    parser.add_argument("--endpoints", nargs="*", default=None, help="Subset of OPERA column names (optional)")
    parser.add_argument("--output-dir", type=Path, default=Path(config.DATA_DIR))
    parser.add_argument("--max-plots", type=int, default=6, help="Number of scatter PNGs for top endpoints by n")
    args = parser.parse_args()

    try:
        from scipy.stats import pearsonr, spearmanr
    except ImportError as e:
        print("scipy is required: pip install scipy", e)
        return 1

    out_dir = Path(args.output_dir)
    plots_dir = out_dir / "validation_plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    mapping_all = load_mapping(args.mapping_json).get("mapping", {})
    ep_list = list(args.endpoints) if args.endpoints else list(mapping_all.keys())

    rows: list[dict] = []
    plot_candidates: list[tuple[str, int, np.ndarray, np.ndarray, bool]] = []

    for endpoint in ep_list:
        payload = mapping_all.get(endpoint) or {}
        types = [str(t).strip().lower() for t in (payload.get("toxval_types") or []) if str(t).strip()]
        if not types:
            continue
        tv = _load_toxval_medians(Path(args.toxval_db), types)
        if tv.empty:
            continue
        xs: list[float] = []
        ys: list[float] = []
        for _, r in tv.iterrows():
            cas = str(r["CAS"]).strip()
            raw = get_opera_value(args.opera_cache, cas, endpoint)
            if raw is None:
                continue
            op = float(pd.to_numeric(raw, errors="coerce"))
            tvv = float(r["toxval_median"])
            if math.isnan(op) or math.isnan(tvv):
                continue
            xs.append(tvv)
            ys.append(op)
        if len(xs) < int(args.min_pairs):
            continue
        x = np.array(xs, dtype=float)
        y = np.array(ys, dtype=float)
        try:
            pr, pp = pearsonr(x, y)
        except Exception:
            pr, pp = float("nan"), float("nan")
        try:
            sr, sp = spearmanr(x, y)
        except Exception:
            sr, sp = float("nan"), float("nan")
        r2 = _r2_linear(x, y)
        rmse_lin = _rmse(x, y)
        log_axes = _use_log_space(endpoint)
        rmse_log = float("nan")
        if log_axes and np.all(x > 0) and np.all(y > 0):
            rmse_log = _rmse(np.log10(x), np.log10(y))
        rows.append(
            {
                "endpoint": endpoint,
                "n_pairs": len(x),
                "pearson_r": float(pr),
                "pearson_p": float(pp),
                "spearman_r": float(sr),
                "spearman_p": float(sp),
                "r2_linear": float(r2),
                "rmse_linear": float(rmse_lin),
                "rmse_log10": float(rmse_log),
                "pct_within_2x": _pct_within_factor(x, y, 2.0),
                "pct_within_5x": _pct_within_factor(x, y, 5.0),
                "pct_within_10x": _pct_within_factor(x, y, 10.0),
            }
        )
        plot_candidates.append((endpoint, len(x), x, y, log_axes))

    summary = pd.DataFrame(rows)
    summary_path = out_dir / "validation_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Wrote {len(summary)} rows to {summary_path}")

    plot_candidates.sort(key=lambda t: t[1], reverse=True)
    md_lines = [
        "# OPERA vs ToxVal validation report",
        "",
        f"- ToxVal source: `{args.toxval_db}`",
        f"- OPERA cache: `{args.opera_cache}`",
        f"- Mapping: `{args.mapping_json}`",
        f"- Minimum pairs per endpoint: {args.min_pairs}",
        "",
        "## Summary table",
        "",
    ]
    if summary.empty:
        md_lines.append("No endpoint met the minimum pair count. Precompute OPERA or lower --min-pairs.")
    else:
        try:
            md_lines.append(summary.to_markdown(index=False))
        except Exception:
            md_lines.append("```")
            md_lines.append(summary.to_string(index=False))
            md_lines.append("```")
        md_lines.append("")

    nplots = min(int(args.max_plots), len(plot_candidates))
    md_lines.append("## Scatter plots (top endpoints by n_pairs)")
    md_lines.append("")
    for i in range(nplots):
        ep, n, x, y, log_axes = plot_candidates[i]
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in ep)[:80]
        rel = f"validation_plots/{safe}.png"
        png_path = plots_dir / f"{safe}.png"
        _plot_scatter(png_path, x, y, f"{ep} (n={n})", log_axes)
        md_lines.append(f"### {ep} (n={n})")
        md_lines.append(f"![{ep}]({rel})")
        md_lines.append("")

    report_path = out_dir / "validation_report.md"
    report_path.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"Wrote {report_path}")
    print(f"Plots: {plots_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
