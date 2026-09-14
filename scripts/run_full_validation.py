#!/usr/bin/env python3
"""
Run P2OASys validation for a CAS list.

**Default (``--source expert``):** offline reference — ``allP2OASys`` database exports plus
``fastP2OASys`` expert category matrix (via ``lookup_p2oasys_by_cas.get_best_match``). No browser.

Use **`--check`** to verify retrieval only (no app scoring). Add **`--strict`** to fail the process if any CAS lacks expert categories.

**Optional (``--source scraped``):** Playwright scraper + compare-raw-data CSV (requires Chromium).

From the **quick-hazard-assessment-app** repo root::

  python scripts/run_full_validation.py --cas-file cas_list.txt \\
    --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys -o validation_report.csv

  python scripts/run_full_validation.py --check --cas 67-63-0 \\
    --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys

Or set ``P2OASYS_ARCHIVE_DIR`` and ``FAST_P2OASYS_DIR`` and omit those flags.

Set ``P2OASYS_LOOKUP_SCRIPT`` if ``lookup_p2oasys_by_cas.py`` is not at
``../sds examples/scripts/lookup_p2oasys_by_cas.py`` under ``GHhaz4``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_APP_ROOT = _SCRIPT_DIR.parent
_VALIDATE = _SCRIPT_DIR / "validate_p2oasys_vs_fast_reference.py"


def _build_summary_lines(comparison_csv: Path) -> list[str]:
    import pandas as pd

    df = pd.read_csv(comparison_csv, dtype=str)
    lines: list[str] = [f"Comparison file: {comparison_csv.resolve()}", f"Row count: {len(df)}", ""]

    num = df.copy()
    for col in ("computed_score", "reference_score", "absolute_error"):
        if col in num.columns:
            num[col] = pd.to_numeric(num[col], errors="coerce")
    pair = num.dropna(subset=["computed_score", "reference_score"])
    if not pair.empty:
        ae = (pair["computed_score"] - pair["reference_score"]).abs()
        lines.append(f"Overall MAE (|computed - reference|): {float(ae.mean()):.4f} (n={len(pair)})")
        if "category" in pair.columns:
            by_cat = pair.assign(_ae=ae).groupby("category", dropna=False)["_ae"].mean().sort_values()
            lines.append("")
            lines.append("MAE by category:")
            lines.append(by_cat.to_string())
    else:
        lines.append("No numeric pairs for MAE (check reference and mapping).")

    if "source_of_lowest_value" in df.columns:
        mask = df["source_of_lowest_value"].astype(str).str.contains("IUCLID", case=False, na=False)
        n_iuclid = int(mask.sum())
        lines.append("")
        lines.append(f"Rows mentioning IUCLID in source_of_lowest_value: {n_iuclid}")

    if "CAS" in df.columns:
        lines.append("")
        lines.append(f"Distinct CAS in output: {df['CAS'].nunique()}")

    return lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run P2OASys validation (default: offline expert/CAS reference; optional: scraped web CSV)."
    )
    parser.add_argument(
        "--source",
        choices=("expert", "scraped"),
        default="expert",
        help="expert=offline archive+fastP2OASys (default); scraped=Playwright compare-raw-data CSV.",
    )
    parser.add_argument("--cas-file", type=Path, default=None, help="One CAS per line.")
    parser.add_argument("--cas", nargs="*", default=[], metavar="CAS", help="CAS on the command line.")
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=None,
        help="allP2OASys-style folder (expert mode). Default: P2OASYS_ARCHIVE_DIR.",
    )
    parser.add_argument(
        "--fastp2oasys-dir",
        type=Path,
        default=None,
        help="fastP2OASys folder (expert mode). Default: FAST_P2OASYS_DIR or app resolver.",
    )
    parser.add_argument(
        "--category-csv",
        type=str,
        default="P2OASys_Category_Scores_Data_March_18_2026.csv",
        help="Expert category matrix filename inside --fastp2oasys-dir (expert mode).",
    )
    parser.add_argument(
        "--database-glob",
        type=str,
        default="P2OASys_Database_Results*.csv",
        help="Database export glob under archive-dir (expert mode).",
    )
    parser.add_argument(
        "--reference-csv",
        type=Path,
        default=None,
        help="Scraped wide reference CSV (scraped mode only).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("validation_report.csv"),
        help="Output comparison CSV (default: validation_report.csv in cwd).",
    )
    parser.add_argument(
        "--auto-fetch",
        action="store_true",
        help="Scraped mode only: if reference CSV is missing, run the Playwright scraper.",
    )
    parser.add_argument(
        "--scraper-args",
        type=str,
        default="",
        help="Scraped mode only: extra arguments for the scraper when auto-fetch runs.",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=None,
        help="If set, write MAE-by-category text summary to this path (full validation only, not with --check).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Pass --check-retrieval to the validator: verify archive/name lookup only (expert mode).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="With --check: pass through to validator; exit 1 if any CAS has no expert categories.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="With --check: pass --json to the validator (retrieval report as JSON).",
    )
    args = parser.parse_args()

    if not args.cas and args.cas_file is None:
        parser.error("Provide --cas-file and/or --cas.")
    if args.check and args.source != "expert":
        parser.error("--check is only supported with --source expert (the default).")

    out = args.output.resolve()

    cmd: list[str] = [
        sys.executable,
        str(_VALIDATE.resolve()),
        "--source",
        args.source,
        "-o",
        str(out),
    ]

    if args.source == "expert":
        if args.archive_dir is not None:
            cmd.extend(["--archive-dir", str(args.archive_dir.resolve())])
        if args.fastp2oasys_dir is not None:
            cmd.extend(["--fastp2oasys-dir", str(args.fastp2oasys_dir.resolve())])
        cmd.extend(["--category-csv", args.category_csv, "--database-glob", args.database_glob])
    else:
        ref = args.reference_csv
        if ref is not None:
            cmd.extend(["--reference-csv", str(ref.resolve())])
        if args.auto_fetch:
            cmd.append("--auto-fetch")
        if args.scraper_args.strip():
            cmd.extend(["--scraper-args", args.scraper_args])

    if args.cas_file is not None:
        cmd.extend(["--cas-file", str(args.cas_file.resolve())])
    for c in args.cas:
        cmd.extend(["--cas", c])

    if args.check:
        cmd.append("--check-retrieval")
    if args.strict:
        cmd.append("--strict")
    if args.json:
        cmd.append("--json")

    proc = subprocess.run(cmd, cwd=str(_APP_ROOT))
    if proc.returncode != 0:
        return int(proc.returncode)

    if out.is_file() and not args.check:
        lines = _build_summary_lines(out)
        text = "\n".join(lines) + "\n"
        if args.summary is not None:
            args.summary.parent.mkdir(parents=True, exist_ok=True)
            args.summary.write_text(text, encoding="utf-8")
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
