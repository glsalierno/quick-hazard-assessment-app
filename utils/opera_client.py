"""
Call the local NIEHS OPERA 2.9 command-line executable (MATLAB compiled).

OPERA does not expose ``opera predict --smiles … --format json``. Batch mode is:

    OPERA.exe -s input.smi -o output.csv -a -v 1

See https://github.com/kmansouri/OPERA/releases (OPERA2.9_CL). Prefer ``OPERA.exe``
(non-parallel); ``OPERA_P.exe`` can fail at the CDK descriptor step on some structures.

Environment:
    HAZQUERY_OPERA_EXE — full path to ``OPERA.exe`` or ``OPERA_P.exe``
    OPERA_JAVA_HOME — optional; if set, ``<home>/bin`` is prepended to PATH (PaDEL/CDK)
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

_LOG = logging.getLogger(__name__)

OPERA_CLIENT_VERSION = "opera_client_v6"

# Display key -> OPERA CSV column (prediction) — always read by name, never position
DISPLAY_COLUMNS: dict[str, str] = {
    "LogP (log Kow)": "LogP_pred",
    "Melting point (°C)": "MP_pred",
    "Boiling point (°C)": "BP_pred",
    "Water solubility (log mol/L)": "LogWS_pred",
    "BCF (log)": "LogBCF_pred",
    "Acute oral LD50 (mg/kg, CATMoS)": "CATMoS_LD50_pred",
    "pKa acid (pKa_a)": "pKa_a_pred",
    "pKa base BH+ (pKa_b)": "pKa_b_pred",
    "AD pKa": "AD_pKa",
    "Conf index pKa": "Conf_index_pKa",
}

# Common OPERA ID column names (column-name based matching)
_MOLECULE_ID_COLS = ("MoleculeID", "MolID", "molecule_id", "CAS", "cas", "ID", "id")
_SMILES_COLS = ("Canonical_SMILES", "SMILES", "smiles", "Input_SMILES", "original_smiles")

# Aliases for predict_opera(..., endpoint=...)
_ENDPOINT_ALIASES: dict[str, str] = {
    "logp": "LogP_pred",
    "melting_point": "MP_pred",
    "boiling_point": "BP_pred",
    "water_solubility": "LogWS_pred",
    "bcf": "LogBCF_pred",
    "ld50": "CATMoS_LD50_pred",
    "acute_oral_toxicity": "CATMoS_LD50_pred",
    "pka": "pKa_a_pred",
    "pka_a": "pKa_a_pred",
    "pka_b": "pKa_b_pred",
}


def _default_exe_candidates() -> list[Path]:
    return [
        Path(r"C:\Program Files\OPERA\application\OPERA.exe"),
        Path(r"C:\Program Files\OPERA\application\OPERA_P.exe"),
        Path(r"C:\Program Files\OPERA2.9_CL\application\OPERA.exe"),
        Path(r"C:\Program Files (x86)\OPERA\application\OPERA.exe"),
    ]


def find_opera_executable() -> Path | None:
    """Resolve OPERA executable: env, ``config.HAZQUERY_OPERA_EXE``, then common install paths."""
    try:
        import config as _cfg

        cfg_exe = (getattr(_cfg, "HAZQUERY_OPERA_EXE", None) or "").strip()
        if cfg_exe:
            p = Path(cfg_exe)
            if p.is_file():
                return p
    except Exception:
        pass
    for key in ("HAZQUERY_OPERA_EXE", "OPERA_EXE"):
        raw = (os.environ.get(key) or "").strip()
        if raw:
            p = Path(raw)
            if p.is_file():
                return p
            _LOG.debug("OPERA env %s=%s is not a file", key, raw)
    for p in _default_exe_candidates():
        if p.is_file():
            return p
    return None


def is_opera_available() -> bool:
    return find_opera_executable() is not None


def _opera_bundle_root(exe_path: Path) -> Path:
    """OPERA install root (parent of ``application/``)."""
    return exe_path.resolve().parent.parent


def _bundled_java_home_from_opera_exe(exe_path: Path) -> str | None:
    """
    OPERA ships PaDEL/CDK jars but needs a JRE on PATH.

    The GUI/CL Windows installer bundles a KNIME JRE under
    ``application/knime_*/plugins/org.knime.binary.jre.*/jre``.
    """
    app_dir = exe_path.resolve().parent
    try:
        for knime_dir in sorted(app_dir.glob("knime_*")):
            plugins = knime_dir / "plugins"
            if not plugins.is_dir():
                continue
            for jre_plugin in sorted(plugins.glob("org.knime.binary.jre.*")):
                jre_home = jre_plugin / "jre"
                java_bin = jre_home / "bin"
                java_name = "java.exe" if os.name == "nt" else "java"
                if (java_bin / java_name).is_file():
                    return str(jre_home)
    except OSError:
        pass
    return None


def _resolve_java_home(exe_path: Path | None, java_home: str | None) -> str:
    home = (java_home or "").strip()
    if not home:
        try:
            import config as _cfg

            home = (getattr(_cfg, "OPERA_JAVA_HOME", None) or "").strip()
        except Exception:
            home = ""
    if not home:
        home = (os.environ.get("OPERA_JAVA_HOME") or os.environ.get("JAVA_HOME") or "").strip()
    if not home and exe_path is not None:
        bundled = _bundled_java_home_from_opera_exe(exe_path)
        if bundled:
            home = bundled
    return home


def _java_enriched_env(java_home: str | None, exe_path: Path | None = None) -> dict[str, str]:
    env = os.environ.copy()
    home = _resolve_java_home(exe_path, java_home)
    if home:
        env["JAVA_HOME"] = home
        java_bin = str(Path(home) / "bin")
        if Path(java_bin).exists():
            env["PATH"] = java_bin + os.pathsep + env.get("PATH", "")
    return env


def _timeout_seconds() -> int:
    raw = (os.environ.get("OPERA_TIMEOUT_SECONDS") or "").strip()
    if raw.isdigit():
        return max(30, int(raw))
    try:
        import config as _cfg

        v = getattr(_cfg, "OPERA_TIMEOUT_SECONDS", 600)
        return max(30, int(v))
    except Exception:
        return 600


def _read_first_csv_row(path: Path) -> dict[str, str]:
    rows = _read_all_csv_rows(path)
    return rows[0] if rows else {}


def _read_all_csv_rows(path: Path) -> list[dict[str, str]]:
    """Parse OPERA CSV by column name (DictReader); never by fixed position."""
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        rows: list[dict[str, str]] = []
        for row in reader:
            # Drop unnamed / None keys from ragged CSV rows
            clean = {
                str(k).strip(): (v if v is not None else "")
                for k, v in row.items()
                if k is not None and str(k).strip()
            }
            rows.append(clean)
        return rows


def select_opera_row_for_input(
    rows: list[dict[str, str]],
    *,
    molecule_id: str,
    smiles: str = "",
) -> tuple[dict[str, str], list[str]]:
    """
    Pick the OPERA output row matching the batch MoleculeID / SMILES.
    Returns (row, warnings).
    """
    warnings: list[str] = []
    if not rows:
        return {}, ["opera_empty_rows"]
    mid = (molecule_id or "").strip()
    smi = (smiles or "").strip()

    for row in rows:
        for col in _MOLECULE_ID_COLS:
            if col in row and mid and str(row[col]).strip() == mid:
                return row, warnings
        for col in _SMILES_COLS:
            if col in row and smi and str(row[col]).strip() == smi:
                return row, warnings

    # Fallback: first row, but flag potential misalignment
    warnings.append(
        f"opera_row_id_mismatch: expected MoleculeID={mid!r}; "
        f"using first CSV row (columns={list(rows[0].keys())[:8]})"
    )
    return rows[0], warnings


def check_opera_pubchem_logp(
    opera_row: dict[str, str],
    pubchem_xlogp: Any = None,
    *,
    threshold: float = 2.0,
) -> list[str]:
    """Flag large OPERA LogP vs PubChem XLogP disagreement (alignment / applicability)."""
    warnings: list[str] = []
    if pubchem_xlogp is None or pubchem_xlogp == "":
        return warnings
    raw = opera_row.get("LogP_pred") or opera_row.get("logP_pred") or ""
    if raw in ("", "NA", "nan", "NaN"):
        return warnings
    try:
        opera_lp = float(str(raw).strip())
        pc_lp = float(str(pubchem_xlogp).strip())
    except (TypeError, ValueError):
        return warnings
    if abs(opera_lp - pc_lp) > threshold:
        warnings.append(
            "OPERA/PubChem logP disagreement; verify parser alignment or model applicability "
            f"(OPERA LogP={opera_lp}, PubChem XLogP={pc_lp})."
        )
    return warnings


def run_opera_on_paths(
    smi_path: Path,
    out_csv_path: Path,
    *,
    opera_exe: Path | str | None = None,
    java_home: str | None = None,
    timeout_seconds: int | None = None,
    cwd: str | None = None,
    endpoints: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run OPERA on an existing ``.smi`` file and write ``out_csv_path``.

    Returns ``{"ok", "error", "rows", "exe", "stdout", "stderr"}`` where ``rows`` is
    a list of dicts (one per molecule), empty on failure.
    """
    exe_path: Path | None
    if opera_exe:
        exe_path = Path(opera_exe)
        if not exe_path.is_file():
            return {
                "ok": False,
                "error": f"OPERA executable not found: {exe_path}",
                "rows": [],
                "exe": str(exe_path),
                "stdout": "",
                "stderr": "",
            }
    else:
        exe_path = find_opera_executable()
        if not exe_path:
            return {
                "ok": False,
                "error": "opera_not_found",
                "rows": [],
                "exe": None,
                "stdout": "",
                "stderr": "",
            }

    timeout = timeout_seconds if timeout_seconds is not None else _timeout_seconds()
    env = _java_enriched_env(java_home, exe_path)
    # PaDEL/CDK expect OPERA's install layout; run from bundle root (not %TEMP%).
    workdir = cwd or str(_opera_bundle_root(exe_path))
    cmd = [
        str(exe_path),
        "-s",
        str(smi_path.resolve()),
        "-o",
        str(out_csv_path.resolve()),
    ]
    eps = [str(e).strip() for e in (endpoints or []) if str(e).strip()]
    if eps:
        cmd.append("-e")
        cmd.extend(eps)
    else:
        cmd.append("-a")
    cmd.extend(["-v", "1"])
    try:
        proc = subprocess.run(
            cmd,
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        if proc.returncode != 0 or not out_csv_path.is_file():
            err = stderr.strip() or stdout.strip() or f"exit_code_{proc.returncode}"
            if "PaDEL descriptors failed" in stderr or "CDK descriptors failed" in stderr:
                if not _resolve_java_home(exe_path, java_home):
                    err += (
                        " — Java not found for PaDEL/CDK. Install a JDK or set OPERA_JAVA_HOME "
                        "to the JRE bundled with OPERA (…\\knime_*\\plugins\\org.knime.binary.jre.*\\jre)."
                    )
                if exe_path.name.lower() == "opera_p.exe":
                    err += (
                        " — try non-parallel OPERA.exe or set HAZQUERY_OPERA_EXE to …\\OPERA\\application\\OPERA.exe"
                    )
            return {
                "ok": False,
                "error": err[:2000],
                "rows": [],
                "exe": str(exe_path),
                "stdout": stdout[:4000],
                "stderr": stderr[:4000],
            }
        rows = _read_all_csv_rows(out_csv_path)
        if not rows:
            return {
                "ok": False,
                "error": "empty_opera_output",
                "rows": [],
                "exe": str(exe_path),
                "stdout": stdout[:4000],
                "stderr": stderr[:4000],
            }
        return {
            "ok": True,
            "error": None,
            "rows": rows,
            "exe": str(exe_path),
            "stdout": stdout[:4000],
            "stderr": stderr[:4000],
        }
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error": f"timeout_after_{timeout}s",
            "rows": [],
            "exe": str(exe_path),
            "stdout": "",
            "stderr": "",
        }
    except OSError as e:
        return {
            "ok": False,
            "error": str(e),
            "rows": [],
            "exe": str(exe_path),
            "stdout": "",
            "stderr": "",
        }


def run_opera_for_smiles(
    smiles: str,
    molecule_id: str = "query",
    *,
    opera_exe: Path | str | None = None,
    java_home: str | None = None,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """
    Run OPERA on a single SMILES and return the first output row as a flat dict.

    Returns:
        ``{"ok": bool, "error": str | None, "row": dict[str, str], "exe": str | None,
            "stdout": str, "stderr": str}``
    """
    smi = (smiles or "").strip()
    if not smi:
        return {"ok": False, "error": "empty_smiles", "row": {}, "exe": None, "stdout": "", "stderr": ""}

    tmp = tempfile.mkdtemp(prefix="hazquery_opera_")
    try:
        smi_path = Path(tmp) / "one.smi"
        out_path = Path(tmp) / "opera_out.csv"
        mid = (molecule_id or "query").strip() or "query"
        smi_path.write_text(f"{smi}\t{mid}\n", encoding="utf-8")
        batch = run_opera_on_paths(
            smi_path,
            out_path,
            opera_exe=opera_exe,
            java_home=java_home,
            timeout_seconds=timeout_seconds,
        )
        rows = batch.get("rows") or []
        row0, id_warnings = select_opera_row_for_input(rows, molecule_id=mid, smiles=smi)
        return {
            "ok": batch["ok"],
            "error": batch.get("error"),
            "row": row0,
            "exe": batch.get("exe"),
            "stdout": batch.get("stdout") or "",
            "stderr": batch.get("stderr") or "",
            "warnings": id_warnings,
            "parser_version": OPERA_CLIENT_VERSION,
        }
    finally:
        try:
            shutil.rmtree(tmp, ignore_errors=True)
        except Exception:
            pass


def _endpoint_to_column(endpoint: str) -> str | None:
    ep = (endpoint or "").strip().lower().replace(" ", "_").replace("-", "_")
    if ep in _ENDPOINT_ALIASES:
        return _ENDPOINT_ALIASES[ep]
    raw = endpoint.strip()
    if raw.endswith("_pred"):
        return raw
    return None


def _applicability_domain_key(pred_col: str) -> str | None:
    if not pred_col.endswith("_pred"):
        return None
    stem = pred_col[: -len("_pred")]
    if pred_col.startswith("CATMoS_"):
        return "AD_CATMoS"
    return f"AD_{stem}"


def predict_opera(smiles: str | list[str], endpoint: str) -> dict[str, Any] | dict[str, dict[str, Any]]:
    """
    Run OPERA for one *endpoint*.

    - **Single SMILES:** same as before — returns one dict with ``value``, ``column``, etc.
    - **List of SMILES:** one OPERA batch run; returns ``{smiles_str: {…same keys as single…}}``.
    """
    if isinstance(smiles, list):
        from utils import opera_batch

        return opera_batch.predict_opera_batch(smiles, endpoint)

    ep_raw = (endpoint or "").strip()
    if not ep_raw:
        return {"ok": False, "error": "empty_endpoint", "endpoint": endpoint, "value": None}
    col = _endpoint_to_column(ep_raw)
    if not col:
        return {"ok": False, "error": f"unknown_endpoint:{ep_raw}", "endpoint": ep_raw, "value": None}

    out = run_opera_for_smiles(smiles)
    if not out["ok"]:
        return {
            "ok": False,
            "error": out.get("error"),
            "endpoint": ep_raw,
            "column": col,
            "value": None,
        }
    row = out.get("row") or {}
    val = row.get(col, "")
    ad_key = _applicability_domain_key(col)
    return {
        "ok": True,
        "error": None,
        "endpoint": ep_raw,
        "column": col,
        "value": val,
        "applicability_domain": row.get(ad_key) if ad_key else None,
    }


def cas_to_smiles(cas_list: list[str]) -> dict[str, str | None]:
    """Map CAS → Canonical SMILES via PubChem PUG REST (sequential; see ``utils.opera_batch``)."""
    from utils import opera_batch

    return opera_batch.cas_to_smiles(cas_list)


def smiles_from_cas_batch(cas_list: list[str]) -> dict[str, str | None]:
    """Map CAS → SMILES using PubChem CID batch property fetch where possible."""
    from utils import opera_batch

    return opera_batch.smiles_from_cas_batch(cas_list)


def extract_opera_pka_from_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Parse OPERA pKa_a / pKa_b (+ AD/confidence) from a flat CSV row."""
    if not row:
        return None

    def _num(x: Any) -> float | None:
        try:
            if x is None or x == "" or str(x).lower() in ("nan", "na", "nd", "none"):
                return None
            return float(str(x).strip())
        except (TypeError, ValueError):
            return None

    pka_a = _num(row.get("pKa_a_pred") or row.get("pKa_a"))
    pka_b = _num(row.get("pKa_b_pred") or row.get("pKa_b"))
    if pka_a is None and pka_b is None:
        return None
    return {
        "pka_a": pka_a,
        "pka_b": pka_b,
        "ad_pka": row.get("AD_pKa"),
        "ad_index_pka": row.get("AD_index_pKa"),
        "conf_index_pka": row.get("Conf_index_pKa"),
        "ionization": row.get("ionization") or row.get("Ionization"),
        "predicted": True,
        "source": "OPERA",
    }


def get_opera_predictions(
    smiles: str,
    cas: str | None = None,
    *,
    pubchem_xlogp: Any = None,
) -> dict[str, Any] | None:
    """
    If OPERA is installed, run it and return a compact summary; otherwise ``None``.

    Summary shape:
        ``{"ok", "error", "exe", "display", "row", "pka", "warnings", "parser_version"}``
    """
    if not is_opera_available():
        return None
    mid = (cas or "").strip() or "query"
    out = run_opera_for_smiles(smiles, molecule_id=mid)
    row = out.get("row") or {}
    display: dict[str, str] = {}
    for label, col in DISPLAY_COLUMNS.items():
        # Always by column name
        v = row.get(col, "")
        if v != "" and v != "NA" and str(v).lower() != "nan":
            display[label] = str(v).strip()
    warnings = list(out.get("warnings") or [])
    warnings.extend(check_opera_pubchem_logp(row, pubchem_xlogp))
    pka = extract_opera_pka_from_row(row)
    # Seed precompute SQLite so P2OASys HITL can gap-fill without a cold CLI run.
    if out.get("ok") and row and mid and mid != "query":
        try:
            from utils import opera_precompute_cache

            opera_precompute_cache.put_cas_row(
                opera_precompute_cache.default_precompute_db_path(),
                mid,
                smiles,
                row,
            )
        except Exception as exc:
            _LOG.debug("opera precompute write skipped: %s", exc)
    return {
        "ok": out["ok"],
        "error": out.get("error"),
        "exe": out.get("exe"),
        "display": display,
        "row": row,
        "pka": pka,
        "warnings": warnings,
        "parser_version": OPERA_CLIENT_VERSION,
    }
