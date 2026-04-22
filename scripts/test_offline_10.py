r"""v1.5 — Offline IUCLID lookup smoke test for **10 target CAS** (ChemDB / REACH dossiers).

**Interpretation**
- Rows come from ``offline_echa_loader`` snapshots (``OFFLINE_CACHE_DIR``) or a one-time build from
  ``OFFLINE_LOCAL_ARCHIVE``. Substance **UUID** is the ``.i6z`` stem; GHS rows are parsed from
  ``Document.i6d`` where present (not from live ECHA unless ``OFFLINE_SCRAPE_CL=true``).
  For REACH Study Results bundles, CAS/EC/name often come from ECHA’s companion
  ``reach_study_results-dossier_info_*.xlsx`` (auto-detected beside the ZIP or via ``OFFLINE_DOSSIER_INFO_XLSX``).
- Missing CAS in the archive simply means that dossier was not in your downloaded bundle (normal).

**Run** (from ``quick-hazard-assessment-app``).

**Do not paste the prompt.** In PowerShell, your line should start with ``$env:`` or ``python`` — not with
``C:\Users\...>`` or ``PS C:\...>``. If you paste ``...app>$env:PYTHONPATH``, PowerShell tries to run the path
before ``$`` as a command and errors with *term 'c:\Users\...' is not recognized*.

*PowerShell* (type or paste **only** these lines; replace the archive path with your real ZIP)::

    $env:PYTHONPATH = "."
    $env:OFFLINE_LOCAL_ARCHIVE = "C:/path/to/REACH_Study_Results_2023-06-12.zip"
    $env:OFFLINE_SCRAPE_CL = "false"
    python scripts/test_offline_10.py

*Command Prompt (cmd.exe)* — use ``set``, not ``$env:`` (``$env`` is PowerShell-only)::

    set PYTHONPATH=.
    set "OFFLINE_LOCAL_ARCHIVE=C:/path/to/REACH_Study_Results_2023-06-12.zip"
    set OFFLINE_SCRAPE_CL=false
    python scripts\test_offline_10.py

First run may take a long time while the full archive is extracted and parsed; later runs use CSV
cache under ``OFFLINE_CACHE_DIR`` (fast) unless you set ``OFFLINE_FORCE_REBUILD=true``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

# Ten reference substances (CAS, display name)
TARGET_CHEMICALS: list[tuple[str, str]] = [
    ("50-00-0", "Formaldehyde"),
    ("67-64-1", "Acetone"),
    ("71-43-2", "Benzene"),
    ("75-07-0", "Acetaldehyde"),
    ("108-88-3", "Toluene"),
    ("79-01-6", "Trichloroethylene"),
    ("127-18-4", "Tetrachloroethylene"),
    ("107-13-1", "Acrylonitrile"),
    ("75-09-2", "Dichloromethane"),
    ("64-17-5", "Ethanol"),
]


def get_substance_by_cas(substances_df: pd.DataFrame, cas: str) -> pd.Series | None:
    """Return the first substance row whose CAS matches ``cas`` (normalized), or ``None``."""
    from ingest.crosswalk import normalize_cas

    want = normalize_cas(cas.strip()) or cas.strip()
    if not want or substances_df.empty or "cas_number" not in substances_df.columns:
        return None
    cas_col = substances_df["cas_number"].map(
        lambda x: normalize_cas(str(x)) if x is not None and str(x).strip() and str(x).lower() != "nan" else ""
    )
    hit = substances_df[cas_col == want]
    if hit.empty:
        return None
    return hit.iloc[0]


def _norm_series_cas(s: pd.Series) -> str:
    from ingest.crosswalk import normalize_cas

    v = s.get("cas_number")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return normalize_cas(str(v)) or ""


def cl_rows_for_substance(cl_df: pd.DataFrame, sub_row: pd.Series) -> pd.DataFrame:
    """Classification rows for one substance (UUID match, then CAS fallback)."""
    from ingest.crosswalk import normalize_cas

    if cl_df.empty:
        return cl_df
    uid = str(sub_row.get("uuid") or "").strip()
    cas_n = _norm_series_cas(sub_row)
    mask = pd.Series(False, index=cl_df.index)
    if "substance_uuid" in cl_df.columns and uid:
        mask = mask | (cl_df["substance_uuid"].astype(str) == uid)
    if "cas_number" in cl_df.columns and cas_n:
        cc = cl_df["cas_number"].map(
            lambda x: normalize_cas(str(x))
            if x is not None and str(x).strip() and str(x).lower() != "nan"
            else ""
        )
        mask = mask | (cc == cas_n)
    return cl_df[mask]


def _cl_has_signal(cl_sub: pd.DataFrame) -> bool:
    for _, r in cl_sub.iterrows():
        if str(r.get("h_statement_code") or "").strip():
            return True
        if str(r.get("h_statement_text") or "").strip():
            return True
        if str(r.get("hazard_class") or "").strip():
            return True
    return False


def _format_ghs_lines(cl_sub: pd.DataFrame) -> str:
    if cl_sub.empty:
        return "(none)"
    parts: list[str] = []
    for _, r in cl_sub.iterrows():
        code = r.get("h_statement_code")
        text = (r.get("h_statement_text") or "").strip() if pd.notna(r.get("h_statement_text")) else ""
        hc = (r.get("hazard_class") or "").strip() if pd.notna(r.get("hazard_class")) else ""
        if code and text:
            parts.append(f"{code} ({text[:100]}{'…' if len(text) > 100 else ''})")
        elif code:
            parts.append(str(code))
        elif text:
            parts.append(text[:120])
        elif hc:
            parts.append(hc[:80])
    return "; ".join(parts) if parts else "(none)"


def _archive_path_looks_like_doc_placeholder(la: str) -> bool:
    s = la.replace("\\", "/").lower()
    return (
        "path/to/" in s
        or "/path/to" in s
        or "your/actual" in s
        or "your\\actual" in la.lower()
        or "....zip" in s
    )


def main() -> None:
    from ingest import offline_echa_loader as ob
    from ingest.crosswalk import normalize_cas

    la = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
    p_sub, p_cl = ob._snapshot_paths()
    snap_ok = p_sub.is_file() and p_cl.is_file()

    if not la and not snap_ok:
        print(
            "Set OFFLINE_LOCAL_ARCHIVE to your REACH .zip/.7z (or folder of .i6z) for the first build, "
            "or build snapshots once then rerun without it.",
            file=sys.stderr,
        )
        sys.exit(2)

    la_path = Path(os.path.expandvars(la)).expanduser() if la else None
    if la and la_path is not None and not la_path.exists():
        if _archive_path_looks_like_doc_placeholder(la):
            print(
                "OFFLINE_LOCAL_ARCHIVE looks like the tutorial placeholder (path/to, your/actual, or ....zip). "
                "Set it to the real ZIP or folder on your machine.",
                file=sys.stderr,
            )
        if not snap_ok:
            print(
                f"OFFLINE_LOCAL_ARCHIVE does not exist: {la_path}\n"
                "Fix the path, or unset OFFLINE_LOCAL_ARCHIVE if you already have CSV snapshots from a prior build.",
                file=sys.stderr,
            )
            sys.exit(2)
        print(
            f"Warning: OFFLINE_LOCAL_ARCHIVE does not exist ({la_path}); loading cached snapshots instead.",
            file=sys.stderr,
        )

    force_rebuild = os.getenv("OFFLINE_FORCE_REBUILD", "").strip().lower() in ("1", "true", "yes", "on")
    print("=== Testing offline IUCLID lookup for 10 chemicals (v1.5) ===")
    if la:
        print("Local archive / folder:", la)
    else:
        print("Using cached CSV snapshots under:", ob.OFFLINE_CACHE_DIR)

    substances_df, cl_hazards_df = ob.load_echa_from_offline(
        use_cache=True,
        force_rebuild=force_rebuild,
        force_download=force_rebuild,
        max_substances_for_cl=None,
    )

    print(f"Substances loaded: {len(substances_df)} rows. Filtering to {len(TARGET_CHEMICALS)} target CAS…")

    if substances_df.empty:
        print("No substances in snapshot/build; nothing to filter.")
        sys.exit(1)

    cas_norm_series = substances_df["cas_number"].map(
        lambda x: normalize_cas(str(x)) if x is not None and str(x).strip() and str(x).lower() != "nan" else ""
    )
    want_cas = {normalize_cas(c) or c for c, _ in TARGET_CHEMICALS}
    filtered = substances_df[cas_norm_series.isin(want_cas)].copy()

    # tqdm over target list (lookup / join)
    found: list[tuple[str, str, pd.Series | None]] = []
    for cas, label in tqdm(TARGET_CHEMICALS, desc="Resolve 10 targets"):
        row = get_substance_by_cas(substances_df, cas)
        found.append((cas, label, row))

    uuids = set(filtered["uuid"].astype(str)) if not filtered.empty else set()
    cl_filtered = (
        cl_hazards_df[cl_hazards_df["substance_uuid"].astype(str).isin(uuids)].copy()
        if not cl_hazards_df.empty and uuids
        else cl_hazards_df.iloc[0:0].copy()
    )
    # Include CL rows matched by CAS for targets (UUID mismatch)
    for _, sr in filtered.iterrows():
        extra = cl_rows_for_substance(cl_hazards_df, sr)
        if not extra.empty:
            cl_filtered = pd.concat([cl_filtered, extra], ignore_index=True)
    if not cl_filtered.empty:
        cl_filtered = cl_filtered.drop_duplicates()

    out_dir = Path("data") / "test_offline_10"
    out_dir.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(out_dir / "substances_10_targets.csv", index=False)
    cl_filtered.to_csv(out_dir / "cl_hazards_10_targets.csv", index=False)

    with_ghs = 0
    for i, (cas, label, row) in enumerate(found, 1):
        if row is None:
            print(f"\n[{i}] CAS: {cas} ({label}) — **not found** in archive snapshot.")
            continue
        ec = str(row.get("ec_number") or "").strip() or "—"
        name = str(row.get("substance_name") or "").strip() or label
        uid = str(row.get("uuid") or "").strip() or "—"
        cl_sub = cl_rows_for_substance(cl_hazards_df, row)
        ghs = _format_ghs_lines(cl_sub)
        if not cl_sub.empty and _cl_has_signal(cl_sub):
            with_ghs += 1
        print(f"\n[{i}] CAS: {cas}, EC: {ec}, Name: {name}, UUID: {uid}")
        print(f"    GHS: {ghs}")

    n_found = sum(1 for _, _, r in found if r is not None)
    print(f"\nSummary: {n_found} of {len(TARGET_CHEMICALS)} substances found. {with_ghs} have GHS classification rows.")
    print(f"CSV files saved to {out_dir.resolve()}")


if __name__ == "__main__":
    main()
