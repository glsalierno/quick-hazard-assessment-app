#!/usr/bin/env python3
"""
Compare Quick Hazard Assessment P2OASys scores to reference data.

**Mode ``fast`` (default):** expert CSVs in ``fastP2OASys`` — first column ``Category``,
remaining columns are **chemical names**; compares top-level category max scores.

**Mode ``scraped``:** wide CSV from ``scripts/fetch_p2oasys_category_scores.py`` (first
column ``CAS``, one column per P2OASys table row / endpoint). Compares computed
matrix unit scores (and optionally category max) to scraped values.

**Mode ``expert`` (recommended for CAS lists):** offline lookup — ``allP2OASys`` database
exports (by CAS) plus ``fastP2OASys`` category matrix (by matched chemical name). Uses
``get_best_match`` from ``../sds examples/scripts/lookup_p2oasys_by_cas.py``. Compares
expert **top-level category** scores to the app’s ``_category_max`` per category.

**Retrieval check:** ``--source expert --check-retrieval`` (alias ``--dry-run``) runs only
``get_best_match`` per CAS and prints a table or ``--json``; no scoring or comparison CSV.
Use ``--strict`` to exit with code 1 if any CAS has zero expert categories.

Run from the **quick-hazard-assessment-app** directory (repo root for imports):

  cd quick-hazard-assessment-app
  python scripts/validate_p2oasys_vs_fast_reference.py --source fast --limit 20
  python scripts/validate_p2oasys_vs_fast_reference.py --source expert --cas 67-63-0 \\
      --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys
  python scripts/validate_p2oasys_vs_fast_reference.py --source scraped --cas-file cas_list.txt \\
      --reference-csv data/p2oasys_category_scores.csv -o data/scraped_compare.csv

Environment (same as Streamlit): ``OFFLINE_LOCAL_ARCHIVE``, ``P2OASYS_MATRIX_PATH``,
``P2OASYS_DISABLE_AUTO_PLACEHOLDER``, ``IARC_DIR``, ``ATMO_DIR``, optional ToxVal/DSSTox DB paths.

Does **not** require ``streamlit run``; uses ``ChemicalAssessmentService()`` directly and
``OfflineDataContext`` for IUCLID (no ``st.cache_resource``).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

# -----------------------------------------------------------------------------
# Repo root on sys.path
# -----------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_APP_ROOT = _SCRIPT_DIR.parent
_GHHAZ4_ROOT = _APP_ROOT.parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))
os.chdir(_APP_ROOT)

import pandas as pd

import config
from services.chemical_assessment import ChemicalAssessmentService
from unified_hazard_report.unified_lookup import unified_lookup
from utils import hazard_for_p2oasys, p2oasys_matrix_placeholder, p2oasys_scorer
from utils.p2oasys_extras_merge import get_offline_ctx, merge_extra_sources_for_cas

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Map scraped CSV column headers -> computed score lookup
# -----------------------------------------------------------------------------
# Values are either:
#   - A **matrix unit name** as parsed by ``p2oasys_scorer`` (matched against
#     ``flatten_computed_unit_scores`` keys, including "Category|Subcategory|unit").
#   - ``category:<TopCategoryName>`` to compare against the app's top-level
#     ``_category_max`` for that sheet category (e.g. ``category:Ecological Hazards``).
# Use a **list** to try several keys (first hit wins).
#
# After you generate ``p2oasys_category_scores.csv``, open it and align headers with
# unit rows in the Hazard Matrix Excel / ``print_p2oasys_summary`` output.
# -----------------------------------------------------------------------------
SCRAPED_COLUMN_TO_COMPUTED_KEY: dict[str, str | list[str]] = {
    # Examples (uncomment and adjust after inspecting your scraped CSV):
    # "Acute Fish LC50 (mg/l)": "LC50 mg/L fish",
    # "Ecological aggregate (scraped)": "category:Ecological Hazards",
}


def _default_fastp2oasys_dir() -> Path:
    """``fastP2OASys`` is usually sibling to ``GHhaz4`` under ``hazquery``."""
    for base in (_APP_ROOT.parent.parent, _APP_ROOT.parent):
        p = base / "fastP2OASys"
        if p.is_dir():
            return p
    return _APP_ROOT.parent.parent / "fastP2OASys"


DEFAULT_REFERENCE_DIR = _default_fastp2oasys_dir()
DEFAULT_CATEGORY_CSV = "P2OASys_Category_Scores_Data_March_18_2026.csv"
DEFAULT_SCRAPED_REFERENCE_CSV = _APP_ROOT / "data" / "p2oasys_category_scores.csv"

# Category rows to align with ``compare_fast_p2oasys_to_reference.py`` / app scorer keys
SKIP_CATEGORY_ROWS = {"", "Weighted Average"}


def _parse_float(s: Any) -> Optional[float]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).strip().replace(",", ".")
    if not t or t.lower() in ("nan", ""):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def load_reference_category(csv_path: Path) -> tuple[list[str], dict[str, dict[str, float]]]:
    """
    Load category-level expert reference.
    Returns (chemical_names, expert[chemical_name][category] = score).
    """
    df = pd.read_csv(csv_path, dtype=str)
    chemicals = [c.strip() for c in df.columns[1:] if c and str(c).strip()]
    out: dict[str, dict[str, float]] = {ch: {} for ch in chemicals}
    for _, row in df.iterrows():
        cat = (row.iloc[0] or "").strip()
        if not cat or cat in SKIP_CATEGORY_ROWS:
            continue
        for j, ch in enumerate(chemicals):
            idx = j + 1
            if idx < len(row):
                v = _parse_float(row.iloc[idx])
                if v is not None:
                    out[ch][cat] = v
    return chemicals, out


def flatten_computed_category_scores(scores: dict[str, Any]) -> dict[str, float]:
    """``category -> _category_max`` from ``compute_p2oasys_scores`` output."""
    out: dict[str, float] = {}
    for cat, data in scores.items():
        if cat.startswith("_"):
            continue
        if isinstance(data, dict):
            cmax = data.get("_category_max")
            if cmax is not None:
                out[cat] = float(cmax)
    return out


def flatten_computed_unit_scores(scores: dict[str, Any]) -> dict[str, float]:
    """
    Flatten ``compute_p2oasys_scores`` output to lookup keys.

    Each scored matrix unit is stored under its bare ``unit_name`` (first wins if
    duplicated across categories) and under ``Category|Subcategory|unit_name``.
    """
    flat: dict[str, float] = {}
    for cat, data in scores.items():
        if cat.startswith("_") or not isinstance(data, dict):
            continue
        for subcat, bundle in data.items():
            if subcat.startswith("_") or not isinstance(bundle, dict):
                continue
            for unit_name, val in bundle.items():
                if unit_name.startswith("_") or not isinstance(val, (int, float)):
                    continue
                composite = f"{cat}|{subcat}|{unit_name}"
                flat[composite] = float(val)
                flat.setdefault(unit_name, float(val))
    return flat


def _acute_value_source_note(hazard_data: dict[str, Any]) -> str:
    """Rough provenance for acute human inputs (IUCLID vs other)."""
    parts: list[str] = []
    tox = hazard_data.get("toxicities") or []
    has_iuclid = any(
        "iuclid" in str((t.get("species_route") or "")).lower() or "iuclid" in str(t.get("value", "")).lower()
        for t in tox
        if isinstance(t, dict)
    )
    if has_iuclid:
        parts.append("IUCLID")
    if tox:
        parts.append("toxicities")
    ghs = (hazard_data.get("ghs") or {}).get("h_codes") or []
    if ghs:
        parts.append("GHS")
    return "+".join(parts) if parts else "none"


def assess_and_score_chemical(
    query: str,
    matrix: dict[str, Any],
    svc: ChemicalAssessmentService,
) -> tuple[dict[str, float] | None, dict[str, float] | None, str | None, str, str]:
    """
    Returns (category_scores, unit_scores, error_message, resolved_cas, pipeline_note).

    ``pipeline_note`` summarizes data sources (PubChem-only, IUCLID, lookups, etc.)
    and is written to ``source_of_lowest_value`` in scraped validation output.
    """
    try:
        ar = svc.assess(query)
    except Exception as exc:
        return None, None, str(exc), "", ""
    if ar.fetch_error or not ar.pubchem_data:
        return None, None, ar.fetch_error or "No PubChem data", ar.identity.cas, ""
    cas = str(ar.identity.cas or "").strip()
    extra, pipe_note = merge_extra_sources_for_cas(cas)
    hazard_data = hazard_for_p2oasys.build_hazard_data(
        ar.pubchem_data,
        toxval_data=ar.toxval_data,
        carc_potency_data=ar.carc_potency_data,
        extra_sources=extra,
    )
    scores = p2oasys_scorer.compute_p2oasys_scores(hazard_data, matrix)
    cat = flatten_computed_category_scores(scores)
    units = flatten_computed_unit_scores(scores)
    acute_note = _acute_value_source_note(hazard_data)
    return cat, units, None, cas, f"{pipe_note}; acute_inputs={acute_note}"


def _normalize_cas(s: str) -> str:
    return str(s).strip()


def _load_cas_list(cas_file: Path | None, cas_cli: list[str]) -> list[str]:
    out: list[str] = []
    if cas_file is not None:
        text = cas_file.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    for c in cas_cli:
        s = c.strip()
        if s:
            out.append(s)
    seen: set[str] = set()
    uniq: list[str] = []
    for c in out:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def _import_lookup_p2oasys_module() -> Any:
    """Load ``lookup_p2oasys_by_cas`` from the sibling ``sds examples`` repo (stdlib-only module)."""
    candidates: list[Path] = []
    env = (os.environ.get("P2OASYS_LOOKUP_SCRIPT") or "").strip()
    if env:
        candidates.append(Path(env))
    candidates.append(_GHHAZ4_ROOT / "sds examples" / "scripts" / "lookup_p2oasys_by_cas.py")
    for path in candidates:
        if path.is_file():
            spec = importlib.util.spec_from_file_location("lookup_p2oasys_by_cas", str(path.resolve()))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod
    return None


def _resolve_scraper_script() -> Path | None:
    env = (os.environ.get("P2OASYS_SCRAPER_SCRIPT") or "").strip()
    if env:
        p = Path(env)
        if p.is_file():
            return p.resolve()
    candidate = _GHHAZ4_ROOT / "sds examples" / "scripts" / "fetch_p2oasys_category_scores.py"
    if candidate.is_file():
        return candidate.resolve()
    alt = _APP_ROOT / "scripts" / "fetch_p2oasys_category_scores.py"
    if alt.is_file():
        return alt.resolve()
    return None


def _invoke_scraper_subprocess(
    scraper_script: Path,
    cas_file: Path | None,
    cas_cli: list[str],
    output_csv: Path,
    scraper_args: str,
) -> None:
    cmd = [sys.executable, str(scraper_script)]
    if cas_file is not None:
        cmd.extend(["--cas-file", str(cas_file.resolve())])
    for c in cas_cli:
        cmd.extend(["--cas", c])
    cmd.extend(["-o", str(output_csv.resolve())])
    extra = shlex.split(scraper_args or "", posix=os.name != "nt")
    cmd.extend(extra)
    logger.info("Running Playwright scraper: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _mapping_candidates(scraped_col: str) -> list[str]:
    raw = SCRAPED_COLUMN_TO_COMPUTED_KEY.get(scraped_col, scraped_col)
    if isinstance(raw, list):
        return list(raw)
    return [raw]


def _lookup_computed_value(
    category_flat: dict[str, float],
    unit_flat: dict[str, float],
    mapping_target: str,
) -> Optional[float]:
    if mapping_target.startswith("category:"):
        cname = mapping_target.split(":", 1)[1].strip()
        v = category_flat.get(cname)
        return float(v) if v is not None else None
    if mapping_target in unit_flat:
        return float(unit_flat[mapping_target])
    for ukey, val in unit_flat.items():
        if ukey == mapping_target or ukey.endswith("|" + mapping_target):
            return float(val)
    return None


def _lookup_computed_for_scraped_column(
    category_flat: dict[str, float],
    unit_flat: dict[str, float],
    scraped_col: str,
) -> Optional[float]:
    for target in _mapping_candidates(scraped_col):
        hit = _lookup_computed_value(category_flat, unit_flat, target)
        if hit is not None:
            return hit
    return None


def _ensure_cas_column(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    if "cas" in cols:
        key = cols["cas"]
        out = df.rename(columns={key: "CAS"})
        return out
    raise ValueError("Reference CSV must contain a CAS column (header 'CAS').")


def main_fast_reference(args: argparse.Namespace) -> int:
    ref_dir = (args.reference_dir or DEFAULT_REFERENCE_DIR).resolve()
    cat_path = ref_dir / args.category_csv
    if not cat_path.is_file():
        logger.error("Reference CSV not found: %s", cat_path)
        return 1

    matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH),
        Path(config.DATA_DIR),
    )
    if matrix_kind == "missing" or not matrix_path.is_file():
        logger.error("P2OASys matrix not available at %s (and placeholder disabled or failed)", config.P2OASYS_MATRIX_PATH)
        return 1
    if matrix_kind == "placeholder":
        logger.warning("Using dev placeholder matrix at %s — scores are not TURI-official.", matrix_path)

    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    chemicals, expert = load_reference_category(cat_path)
    if args.limit is not None and args.limit > 0:
        chemicals = chemicals[: args.limit]

    svc = ChemicalAssessmentService()
    rows: list[dict[str, Any]] = []

    for i, name in enumerate(chemicals):
        if args.verbose:
            logger.info("[%s/%s] %s", i + 1, len(chemicals), name[:60])
        comp, _units, err, cas, pipeline = assess_and_score_chemical(name, matrix, svc)
        if err:
            rows.append(
                {
                    "reference_name": name,
                    "resolved_cas": cas or "",
                    "category": "",
                    "computed_score": "",
                    "reference_score": "",
                    "absolute_error": "",
                    "diff": "",
                    "pipeline_note": pipeline,
                    "matrix_kind": matrix_kind,
                    "error": err,
                }
            )
            continue

        if args.iuclid_audit_dir and cas and cas != "MIXTURE":
            ctx = get_offline_ctx()
            if ctx is not None:
                try:
                    from ingest.crosswalk import normalize_cas

                    cas_n = normalize_cas(cas) or cas
                    if ctx.uuids_for_cas(cas_n):
                        args.iuclid_audit_dir.mkdir(parents=True, exist_ok=True)
                        ul = unified_lookup(cas_n, ctx)
                        nd = ul.get("iuclid_endpoints_normalized") or []
                        if nd:
                            safe = cas_n.replace("-", "_")
                            pd.DataFrame(nd).to_csv(
                                args.iuclid_audit_dir / f"{safe}_iuclid_normalized.csv",
                                index=False,
                            )
                except Exception as exc:
                    logger.debug("IUCLID audit dump skipped: %s", exc)

        for cat, ref_val in (expert.get(name) or {}).items():
            comp_val = comp.get(cat) if comp else None
            diff = (float(comp_val) - float(ref_val)) if (comp_val is not None and ref_val is not None) else None
            abs_err = abs(diff) if diff is not None else None
            rows.append(
                {
                    "reference_name": name,
                    "resolved_cas": cas,
                    "category": cat,
                    "computed_score": comp_val if comp_val is not None else "",
                    "reference_score": ref_val,
                    "absolute_error": abs_err if abs_err is not None else "",
                    "diff": diff if diff is not None else "",
                    "pipeline_note": pipeline,
                    "matrix_kind": matrix_kind,
                    "error": "",
                }
            )

    out = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    logger.info("Wrote %s rows to %s", len(out), args.output.resolve())

    both = out.copy()
    both["computed_score"] = pd.to_numeric(both["computed_score"], errors="coerce")
    both["reference_score"] = pd.to_numeric(both["reference_score"], errors="coerce")
    pair = both.dropna(subset=["computed_score", "reference_score"])
    if not pair.empty:
        err = (pair["computed_score"] - pair["reference_score"]).abs()
        logger.info(
            "MAE (|computed - reference|) over pairs with both scores: %.4f (n=%s)",
            float(err.mean()),
            len(pair),
        )
    return 0


def main_scraped_reference(args: argparse.Namespace) -> int:
    ref_csv = (args.reference_csv or DEFAULT_SCRAPED_REFERENCE_CSV).resolve()
    cas_list = _load_cas_list(args.cas_file, args.cas)
    if not cas_list:
        logger.error("Scraped validation requires --cas-file and/or --cas with at least one CAS.")
        return 2

    if not ref_csv.is_file():
        if not args.auto_fetch:
            logger.error(
                "Reference CSV not found: %s\n"
                "Run the Playwright scraper first, or re-run with --auto-fetch.",
                ref_csv,
            )
            return 1
        script = _resolve_scraper_script()
        if not script:
            logger.error(
                "Could not locate fetch_p2oasys_category_scores.py. "
                "Set P2OASYS_SCRAPER_SCRIPT or place the repo so '%s' exists.",
                _GHHAZ4_ROOT / "sds examples" / "scripts" / "fetch_p2oasys_category_scores.py",
            )
            return 1
        ref_csv.parent.mkdir(parents=True, exist_ok=True)
        _invoke_scraper_subprocess(script, args.cas_file, args.cas, ref_csv, args.scraper_args)
        if not ref_csv.is_file():
            logger.error("Scraper finished but reference CSV is still missing: %s", ref_csv)
            return 1

    matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH),
        Path(config.DATA_DIR),
    )
    if matrix_kind == "missing" or not matrix_path.is_file():
        logger.error("P2OASys matrix not available at %s (and placeholder disabled or failed)", config.P2OASYS_MATRIX_PATH)
        return 1
    if matrix_kind == "placeholder":
        logger.warning("Using dev placeholder matrix at %s — scores are not TURI-official.", matrix_path)

    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    svc = ChemicalAssessmentService()

    ref_df = pd.read_csv(ref_csv, dtype=str)
    ref_df = _ensure_cas_column(ref_df)
    ref_df["CAS"] = ref_df["CAS"].map(_normalize_cas)
    ref_df = ref_df.drop_duplicates(subset=["CAS"], keep="first")
    ref_by_cas = ref_df.set_index("CAS", drop=False)

    score_columns = [c for c in ref_df.columns if c != "CAS"]
    rows: list[dict[str, Any]] = []

    for i, query_cas in enumerate(cas_list):
        qc = _normalize_cas(query_cas)
        if args.verbose:
            logger.info("[%s/%s] CAS %s", i + 1, len(cas_list), qc)

        if qc not in ref_by_cas.index:
            logger.warning("Reference CSV has no row for CAS %s — skipping.", qc)
            continue

        cat_flat, unit_flat, err, resolved_cas, pipeline = assess_and_score_chemical(qc, matrix, svc)
        if err:
            rows.append(
                {
                    "CAS": qc,
                    "category": "",
                    "computed_score": "",
                    "reference_score": "",
                    "absolute_error": "",
                    "source_of_lowest_value": "",
                    "matrix_kind": matrix_kind,
                    "error": err,
                }
            )
            continue

        ref_row = ref_by_cas.loc[qc]
        if isinstance(ref_row, pd.DataFrame):
            ref_row = ref_row.iloc[0]

        for col in score_columns:
            ref_raw = ref_row.get(col)
            ref_val = _parse_float(ref_raw)
            if ref_val is None and (ref_raw is None or str(ref_raw).strip() == ""):
                continue

            comp_val = _lookup_computed_for_scraped_column(cat_flat or {}, unit_flat or {}, col)
            abs_err: Optional[float] = None
            if comp_val is not None and ref_val is not None:
                abs_err = abs(float(comp_val) - float(ref_val))
            elif comp_val is None:
                logger.debug("No computed value for CAS %s column %r", qc, col)

            err_cell = ""
            if comp_val is None and ref_val is not None:
                err_cell = "missing_computed"
            elif comp_val is not None and ref_val is None:
                err_cell = "missing_reference"

            rows.append(
                {
                    "CAS": resolved_cas or qc,
                    "category": col,
                    "computed_score": "" if comp_val is None else comp_val,
                    "reference_score": "" if ref_val is None else ref_val,
                    "absolute_error": "" if abs_err is None else abs_err,
                    "source_of_lowest_value": pipeline,
                    "matrix_kind": matrix_kind,
                    "error": err_cell,
                }
            )

    out = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    logger.info("Wrote %s rows to %s", len(out), args.output.resolve())

    if not out.empty and "absolute_error" in out.columns:
        both = out.copy()
        both["computed_score"] = pd.to_numeric(both["computed_score"], errors="coerce")
        both["reference_score"] = pd.to_numeric(both["reference_score"], errors="coerce")
        both["absolute_error"] = pd.to_numeric(both["absolute_error"], errors="coerce")
        pair = both.dropna(subset=["computed_score", "reference_score"])
        if not pair.empty:
            abs_err_series = (pair["computed_score"] - pair["reference_score"]).abs()
            logger.info(
                "MAE (|computed - reference|) over pairs with both scores: %.4f (n=%s)",
                float(abs_err_series.mean()),
                len(pair),
            )
            if "category" in pair.columns:
                mae_by_cat = pair.assign(_ae=abs_err_series).groupby("category", dropna=False)["_ae"].mean()
                logger.info("MAE by category (top 15):\n%s", mae_by_cat.sort_values().head(15).to_string())
        iuclid_rows = both[both["source_of_lowest_value"].astype(str).str.contains("IUCLID", case=False, na=False)]
        logger.info("Rows with IUCLID in source_of_lowest_value: %s", len(iuclid_rows))
    return 0


def _prepare_expert_cas_run(args: argparse.Namespace) -> tuple[Any, Path, Path, list[str]] | None:
    """Resolve archive + fastP2OASys paths, import lookup module, load CAS list. Returns None on failure."""
    lookup_mod = _import_lookup_p2oasys_module()
    if lookup_mod is None or not hasattr(lookup_mod, "get_best_match"):
        logger.error(
            "Could not load lookup_p2oasys_by_cas.py (need get_best_match). "
            "Set P2OASYS_LOOKUP_SCRIPT or place file at: %s",
            _GHHAZ4_ROOT / "sds examples" / "scripts" / "lookup_p2oasys_by_cas.py",
        )
        return None

    archive_dir = args.archive_dir
    if archive_dir is None:
        env_a = (os.environ.get("P2OASYS_ARCHIVE_DIR") or "").strip()
        if not env_a:
            logger.error("Expert mode requires --archive-dir or environment variable P2OASYS_ARCHIVE_DIR.")
            return None
        archive_dir = Path(env_a)

    fast_dir = args.fastp2oasys_dir
    if fast_dir is None:
        env_f = (os.environ.get("FAST_P2OASYS_DIR") or "").strip()
        if env_f:
            fast_dir = Path(env_f)
        else:
            fast_dir = DEFAULT_REFERENCE_DIR

    cas_list = _load_cas_list(args.cas_file, args.cas)
    if not cas_list:
        logger.error("Expert mode requires --cas-file and/or --cas with at least one CAS.")
        return None

    return lookup_mod, archive_dir.resolve(), fast_dir.resolve(), cas_list


def _computed_category_for_expert(cat_flat: dict[str, float], category: str) -> float | None:
    """Match expert CSV category row label to app top-level category max."""
    if not cat_flat:
        return None
    c = category.strip()
    if c in cat_flat:
        return float(cat_flat[c])
    cl = c.lower()
    for ck, cv in cat_flat.items():
        if str(ck).strip().lower() == cl:
            return float(cv)
    return None


def main_check_retrieval(args: argparse.Namespace) -> int:
    """
    For each CAS, run get_best_match only (no app scoring, no comparison CSV).
    Use before full ``--source expert`` validation to verify archive paths and name matching.
    """
    prep = _prepare_expert_cas_run(args)
    if prep is None:
        return 1
    lookup_mod, archive_dir, fast_dir, cas_list = prep

    records: list[dict[str, Any]] = []
    for query_cas in cas_list:
        qc = _normalize_cas(query_cas)
        gm = lookup_mod.get_best_match(
            qc,
            archive_dir,
            fast_dir,
            database_glob=args.database_glob,
            category_csv=args.category_csv,
        )
        expert = gm.get("expert_categories") or {}
        rec: dict[str, Any] = {
            "CAS": qc,
            "matched_name": gm.get("matched_name"),
            "matched_expert_column": gm.get("matched_expert_column"),
            "overall_evaluation": gm.get("overall_evaluation"),
            "n_expert_categories": len(expert),
            "archive_hits_count": gm.get("archive_hits_count"),
            "archive_pick_note": gm.get("archive_pick_note"),
            "warning": gm.get("warning"),
        }
        if args.verbose:
            rec["expert_categories"] = expert
        records.append(rec)

    if args.json:
        text = json.dumps(records, indent=2)
    else:
        lines = [
            "CAS".ljust(14)
            + "matched_name".ljust(28)
            + "eval".ljust(8)
            + "n_cat".ljust(6)
            + "hits".ljust(6)
            + "pick_note / warning",
        ]
        lines.append("-" * 120)
        for rec in records:
            mn = (rec["matched_name"] or "")[:26]
            ev = "" if rec["overall_evaluation"] is None else str(rec["overall_evaluation"])
            note = rec.get("archive_pick_note") or ""
            warn = rec.get("warning") or ""
            tail = (note + (" | " if note and warn else "") + warn)[:200]
            lines.append(
                f"{str(rec['CAS'])[:12]:<14}"
                f"{mn:<28}"
                f"{ev:<8}"
                f"{rec['n_expert_categories']!s:<6}"
                f"{str(rec.get('archive_hits_count', '')):<6}"
                f"{tail}"
            )
            if args.verbose and rec.get("expert_categories"):
                lines.append("  expert_categories: " + json.dumps(rec["expert_categories"], sort_keys=True))
        text = "\n".join(lines) + "\n"

    sys.stdout.write(text)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
        logger.info("Wrote retrieval check report to %s", args.output.resolve())

    failures = sum(1 for r in records if r["n_expert_categories"] == 0)
    if failures:
        logger.warning("%s of %s CAS had no expert categories retrieved.", failures, len(records))
    if getattr(args, "strict", False) and failures:
        return 1
    return 0


def main_expert_reference(args: argparse.Namespace) -> int:
    prep = _prepare_expert_cas_run(args)
    if prep is None:
        return 1
    lookup_mod, archive_dir, fast_dir, cas_list = prep

    matrix_path, matrix_kind = p2oasys_matrix_placeholder.resolve_p2oasys_matrix_path(
        Path(config.P2OASYS_MATRIX_PATH),
        Path(config.DATA_DIR),
    )
    if matrix_kind == "missing" or not matrix_path.is_file():
        logger.error("P2OASys matrix not available at %s (and placeholder disabled or failed)", config.P2OASYS_MATRIX_PATH)
        return 1
    if matrix_kind == "placeholder":
        logger.warning("Using dev placeholder matrix at %s — scores are not TURI-official.", matrix_path)

    matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
    svc = ChemicalAssessmentService()
    rows: list[dict[str, Any]] = []

    for i, query_cas in enumerate(cas_list):
        qc = _normalize_cas(query_cas)
        if args.verbose:
            logger.info("[%s/%s] CAS %s", i + 1, len(cas_list), qc)

        gm = lookup_mod.get_best_match(
            qc,
            archive_dir,
            fast_dir,
            database_glob=args.database_glob,
            category_csv=args.category_csv,
        )
        expert: dict[str, str] = gm.get("expert_categories") or {}
        if not expert:
            logger.warning(
                "CAS %s: skipped — no expert categories (%s)",
                qc,
                gm.get("warning") or "empty expert_categories",
            )
            continue

        cat_flat, _units, err, resolved_cas, pipeline = assess_and_score_chemical(qc, matrix, svc)
        matched = gm.get("matched_name") or ""
        expert_col = gm.get("matched_expert_column") or ""
        eval_ov = gm.get("overall_evaluation")
        prov = f"{pipeline}; expert_column={expert_col!r}; matched_name={matched!r}"
        if eval_ov is not None:
            prov += f"; archive_evaluation={eval_ov}"
        if gm.get("archive_pick_note"):
            prov += f"; note={gm['archive_pick_note']}"

        if err:
            rows.append(
                {
                    "CAS": resolved_cas or qc,
                    "category": "",
                    "computed_score": "",
                    "reference_score": "",
                    "absolute_error": "",
                    "source_of_lowest_value": prov,
                    "matrix_kind": matrix_kind,
                    "error": err,
                }
            )
            continue

        for cat, ref_str in sorted(expert.items()):
            ref_val = _parse_float(ref_str)
            if ref_val is None and (not str(ref_str).strip()):
                continue
            comp_val = _computed_category_for_expert(cat_flat or {}, cat)
            abs_err: Optional[float] = None
            if comp_val is not None and ref_val is not None:
                abs_err = abs(float(comp_val) - float(ref_val))
            err_cell = ""
            if comp_val is None and ref_val is not None:
                err_cell = "missing_computed"
            elif comp_val is not None and ref_val is None:
                err_cell = "missing_reference"

            rows.append(
                {
                    "CAS": resolved_cas or qc,
                    "category": cat,
                    "computed_score": "" if comp_val is None else comp_val,
                    "reference_score": "" if ref_val is None else ref_val,
                    "absolute_error": "" if abs_err is None else abs_err,
                    "source_of_lowest_value": prov,
                    "matrix_kind": matrix_kind,
                    "error": err_cell,
                }
            )

    out = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    logger.info("Wrote %s rows to %s", len(out), args.output.resolve())

    if not out.empty and "absolute_error" in out.columns:
        both = out.copy()
        both["computed_score"] = pd.to_numeric(both["computed_score"], errors="coerce")
        both["reference_score"] = pd.to_numeric(both["reference_score"], errors="coerce")
        pair = both.dropna(subset=["computed_score", "reference_score"])
        if not pair.empty:
            abs_err_series = (pair["computed_score"] - pair["reference_score"]).abs()
            logger.info(
                "MAE (|computed - reference|) over pairs with both scores: %.4f (n=%s)",
                float(abs_err_series.mean()),
                len(pair),
            )
            if "category" in pair.columns:
                mae_by_cat = pair.assign(_ae=abs_err_series).groupby("category", dropna=False)["_ae"].mean()
                logger.info("MAE by category (top 15):\n%s", mae_by_cat.sort_values().head(15).to_string())
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate P2OASys scores: fast (name columns), expert (CAS+offline archive), or scraped (Playwright CSV)."
    )
    parser.add_argument(
        "--source",
        choices=("fast", "scraped", "expert"),
        default="fast",
        help="Reference: fast=name-matrix CSV; expert=CAS+allP2OASys+fastP2OASys (offline); scraped=compare-raw-data CSV.",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=None,
        help="allP2OASys-style folder with P2OASys_Database_Results*.csv (expert mode). Default: P2OASYS_ARCHIVE_DIR.",
    )
    parser.add_argument(
        "--fastp2oasys-dir",
        type=Path,
        default=None,
        help="Folder with P2OASys_Category_Scores_Data_*.csv (expert mode). Default: FAST_P2OASYS_DIR or sibling fastP2OASys.",
    )
    parser.add_argument(
        "--database-glob",
        type=str,
        default="P2OASys_Database_Results*.csv",
        help="Glob under archive-dir for database exports (expert mode).",
    )
    parser.add_argument(
        "--reference-dir",
        type=Path,
        default=None,
        help=f"Folder containing category CSV for --source fast (default: sibling {DEFAULT_REFERENCE_DIR})",
    )
    parser.add_argument(
        "--category-csv",
        type=str,
        default=DEFAULT_CATEGORY_CSV,
        help="Category matrix filename: inside --reference-dir (fast) or --fastp2oasys-dir (expert).",
    )
    parser.add_argument(
        "--reference-csv",
        type=Path,
        default=None,
        help="Scraped wide CSV (CAS + per-endpoint columns). Default: data/p2oasys_category_scores.csv under app root.",
    )
    parser.add_argument(
        "--cas-file",
        type=Path,
        default=None,
        help="Text file with one CAS per line (# comments allowed). Used by scraped and expert modes.",
    )
    parser.add_argument(
        "--cas",
        nargs="*",
        default=[],
        metavar="CAS",
        help="CAS numbers on the command line (scraped and expert modes).",
    )
    parser.add_argument(
        "--auto-fetch",
        action="store_true",
        help="If --reference-csv is missing, run the Playwright scraper to create it (scraped mode only).",
    )
    parser.add_argument(
        "--scraper-args",
        type=str,
        default="",
        help='Extra arguments for the scraper subprocess (quoted), e.g. \'--timeout-ms 120000 --verbose\'.',
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output comparison CSV path (defaults depend on --source).",
    )
    parser.add_argument(
        "--limit",
        "--cas-limit",
        type=int,
        default=None,
        dest="limit",
        metavar="N",
        help="Process only the first N chemicals (column order in reference CSV; fast mode only).",
    )
    parser.add_argument(
        "--iuclid-audit-dir",
        type=Path,
        default=None,
        help="If set, write ``{{cas}}_iuclid_normalized.csv`` per compound when dossiers exist (for heuristic tuning; fast mode).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="More detail (expert check: dump expert_categories per CAS; expert run: log each CAS).",
    )
    parser.add_argument(
        "--check-retrieval",
        "--dry-run",
        action="store_true",
        dest="check_retrieval",
        help="With --source expert: only run get_best_match per CAS; print table or JSON; no scoring or CSV.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="With --check-retrieval: emit JSON instead of a text table.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="With --check-retrieval: exit with code 1 if any CAS has zero expert categories.",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")

    if args.json and not args.check_retrieval:
        parser.error("--json is only supported together with --check-retrieval.")
    if args.strict and not args.check_retrieval:
        parser.error("--strict is only supported together with --check-retrieval.")
    if args.check_retrieval:
        if args.source != "expert":
            parser.error("--check-retrieval/--dry-run requires --source expert.")
        if not args.cas and args.cas_file is None:
            parser.error("--check-retrieval requires --cas-file and/or --cas.")
        return main_check_retrieval(args)

    if args.output is None:
        if args.source == "fast":
            args.output = _APP_ROOT / "data" / "p2oasys_validation_vs_fast_reference.csv"
        elif args.source == "scraped":
            args.output = _APP_ROOT / "data" / "p2oasys_scraped_validation_comparison.csv"
        else:
            args.output = _APP_ROOT / "data" / "p2oasys_expert_validation_comparison.csv"

    if args.source == "scraped":
        if not args.cas and args.cas_file is None:
            parser.error("Scraped mode requires --cas-file and/or --cas.")
        return main_scraped_reference(args)
    if args.source == "expert":
        if not args.cas and args.cas_file is None:
            parser.error("Expert mode requires --cas-file and/or --cas.")
        return main_expert_reference(args)
    return main_fast_reference(args)


if __name__ == "__main__":
    raise SystemExit(main())
