"""
CAS → PubChem SMILES → HSPiP CLI → append ``hsp_by_cas.csv``.

Wraps the workflow from https://github.com/glsalierno/cas-to-HSPiP_data
(``HSPiP_CLI_v7.py``) for interactive / batch database expansion.

Requires a licensed HSPiP install with CLI enabled. User must point
``HSPIP_INSTALL_DIR`` (or the Streamlit settings field) at the folder
containing ``HSPiP.exe``.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import config
from utils.lookup_tables import normalize_cas_for_lookup
from v7.settings_store import get_hspip_install_dir

logger = logging.getLogger(__name__)

_OUT_COLUMNS = [
    "HSPiP_SMILES",
    "Formula",
    "D",
    "P",
    "H",
    "HDon",
    "HAcc",
    "MWt",
    "Density",
    "MVol",
    "RER",
]


@dataclass
class HspExpandResult:
    cas: str
    status: str  # found_cache | computed | skipped | error
    delta_d: float | None = None
    delta_p: float | None = None
    delta_h: float | None = None
    rer: float | None = None
    smiles: str | None = None
    message: str = ""
    source_path: str | None = None


@dataclass
class HspExpandSummary:
    results: list[HspExpandResult] = field(default_factory=list)
    cache_path: str = ""

    @property
    def computed(self) -> int:
        return sum(1 for r in self.results if r.status == "computed")

    @property
    def cached(self) -> int:
        return sum(1 for r in self.results if r.status == "found_cache")

    @property
    def errors(self) -> int:
        return sum(1 for r in self.results if r.status == "error")


def hsp_cache_path() -> Path:
    return Path(getattr(config, "HSPIP_CACHE_CSV_PATH", Path("data") / "hsp_by_cas.csv"))


def _cas_display(digits: str) -> str:
    d = "".join(c for c in str(digits) if c.isdigit())
    if len(d) < 5:
        return str(digits)
    return f"{d[:-3]}-{d[-3:-1]}-{d[-1]}"


def bootstrap_hsp_from_doss(*, preserve_hspip_cli: bool = True) -> int:
    """
    Seed ``hsp_by_cas.csv`` from DoSS Hansen parameters.

    Rows already marked ``source=hspip_cli`` are kept when *preserve_hspip_cli*
    is True. Returns total row count written.
    """
    from utils.solvent_reference_lookup import resolve_lookup_tables

    tables = resolve_lookup_tables(
        p2oasys_csv=config.P2OASYS_EXPERT_CSV_PATH,
        chem21_csv=config.CHEM21_GUIDE_CSV_PATH,
        doss_xlsx=config.DOSS_XLSX_PATH,
        hspip_csv=None,
    )
    doss = tables.get("doss_hsp") or {}
    out = hsp_cache_path()
    existing: dict[str, dict[str, Any]] = {}
    if out.is_file():
        with open(out, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                key = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                if key:
                    existing[key] = dict(row)
    for key, hit in doss.items():
        if (
            preserve_hspip_cli
            and key in existing
            and str(existing[key].get("source") or "") == "hspip_cli"
        ):
            continue
        existing[key] = {
            "cas": _cas_display(key),
            "D": hit.get("delta_d"),
            "P": hit.get("delta_p"),
            "H": hit.get("delta_h"),
            "RER": "",
            "smiles": "",
            "source": "doss_bootstrap",
        }
    out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["cas", "D", "P", "H", "RER", "smiles", "source"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for k in sorted(existing, key=lambda x: (len(x), x)):
            writer.writerow({fn: existing[k].get(fn, "") for fn in fieldnames})
    return len(existing)


def resolve_hspip_dir(explicit: str | None = None) -> Path:
    raw = (explicit or get_hspip_install_dir() or "").strip().strip('"')
    if not raw:
        try:
            import config as _cfg
            raw = str(getattr(_cfg, "HSPIP_INSTALL_DIR", "") or "").strip().strip('"')
        except Exception:
            raw = ""
    if not raw:
        raise FileNotFoundError(
            "HSPiP install directory is not set. Enter the folder that contains "
            "HSPiP.exe (CLI license required), e.g. "
            r"C:\Program Files\Hansen-Solubility\HSPiP"
        )
    path = Path(raw)
    if not path.is_dir():
        raise FileNotFoundError(f"HSPiP directory not found: {path}")
    if not (path / "HSPiP.exe").is_file():
        raise FileNotFoundError(f"HSPiP.exe not found under {path}")
    return path


def hspip_workdir_path() -> Path:
    """Writable mirror used when Program Files install cannot write Out.dat."""
    data_dir = Path(getattr(config, "DATA_DIR", "data"))
    return data_dir / "hspip_workdir"


def _dir_is_writable(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / f"_ghaz7_write_probe_{os.getpid()}.tmp"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        try:
            probe.unlink(missing_ok=True)
        except OSError:
            pass
        return False


def ensure_writable_hspip_dir(source: Path | str | None = None) -> Path:
    """
    Return an HSPiP directory that can write ``Out.dat``.

    Program Files installs are read-only for normal users; in that case copy
    (or refresh) the install into ``data/hspip_workdir``.
    """
    src = resolve_hspip_dir(str(source) if source else None)
    if _dir_is_writable(src):
        return src
    dest = hspip_workdir_path()
    dest.mkdir(parents=True, exist_ok=True)
    if not (dest / "HSPiP.exe").is_file():
        logger.info("Copying HSPiP install to writable workdir %s", dest)
        shutil.copytree(src, dest, dirs_exist_ok=True)
    elif not _dir_is_writable(dest):
        raise PermissionError(
            f"Neither {src} nor {dest} is writable. "
            "Copy HSPiP to a user folder or run once as Administrator."
        )
    if not (dest / "HSPiP.exe").is_file():
        raise FileNotFoundError(f"HSPiP.exe missing after copy to {dest}")
    return dest


def smiles_for_cas(cas: str) -> str | None:
    """PubChem canonical/isomeric SMILES for a CAS (uses existing client)."""
    from utils.pubchem_client import get_compound_data

    data = get_compound_data(cas) or {}
    smiles = data.get("smiles") or data.get("canonical_smiles") or data.get("isomeric_smiles")
    if not smiles:
        return None
    try:
        from rdkit import Chem

        mol = Chem.MolFromSmiles(str(smiles))
        if mol is None:
            return str(smiles).strip()
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return str(smiles).strip()


def _read_cache(path: Path) -> dict[str, dict[str, Any]]:
    from utils.solvent_reference_lookup import load_hsp_csv_by_cas

    return load_hsp_csv_by_cas(path) if path.is_file() else {}


def _append_cache_row(
    path: Path,
    *,
    cas: str,
    delta_d: float,
    delta_p: float,
    delta_h: float,
    rer: float | None = None,
    smiles: str | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.is_file() or path.stat().st_size == 0
    # Rewrite merging so duplicates update
    rows: dict[str, dict[str, Any]] = {}
    if path.is_file():
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                key = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                if key:
                    rows[key] = dict(row)
    key = normalize_cas_for_lookup(cas)
    rows[key] = {
        "cas": cas,
        "D": delta_d,
        "P": delta_p,
        "H": delta_h,
        "RER": rer if rer is not None else "",
        "smiles": smiles or "",
        "source": "hspip_cli",
    }
    fieldnames = ["cas", "D", "P", "H", "RER", "smiles", "source"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _, row in sorted(rows.items()):
            writer.writerow({k: row.get(k, "") for k in fieldnames})


# Fixed Y-MBSX Out.dat column order (cas-to-HSPiP_data / HSPiP_CLI_v7.py).
# Prefer this over header-label matching — HSPiP may emit ΔHv@BPt / mojibake
# headers that previously confused single-letter "H" / "BPt" lookups.
_YMBSX_OUT_COLUMNS: list[str] = [
    "HSPiP_SMILES", "Formula", "D", "P", "H", "HDon", "HAcc", "MWt", "Density", "MVol",
    "Area", "Ovality", "BPt", "MPt", "Tc", "Pc", "Vc", "Zc", "AntA", "AntB",
    "AntC", "Ant1T", "LogKow", "LogS", "Henry", "LogOHR", "RI", "Hfus", "HvBPt",
    "Trouton", "RER", "Abra", "Abrb", "EdmiW", "Parachor", "RD", "Cp", "log",
    "Cond", "SurfTen", "HeavyAtom", "C", "H1", "Br", "Cl", "F", "I", "N", "O",
    "P1", "S", "Si", "B", "MaxPc", "MinMc", "Sym", "MCI", "Hcomb", "Hform",
    "Gform", "FGList",
]


def _safe_float(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none", "null", "-", "—"}:
        return None
    # Never coerce header labels (e.g. ΔHv@BPt) to floats.
    if any(ch.isalpha() for ch in s.replace("E", "").replace("e", "").replace("+", "").replace("-", "")):
        # allow scientific notation only; reject labels with letters like Hv, BPt
        if re.search(r"[A-Za-zΔδ]", s) and not re.fullmatch(r"[+-]?\d+(\.\d+)?([eE][+-]?\d+)?", s):
            return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _parse_out_dat(out_file: Path) -> dict[str, Any] | None:
    if not out_file.is_file():
        return None
    raw_text = out_file.read_text(encoding="utf-8", errors="replace")
    if "SMILES Error" in raw_text:
        return None
    lines = [ln for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return None

    def _row_parts(ln: str) -> list[str]:
        if "\t" in ln:
            return [p.strip() for p in ln.split("\t")]
        # rare comma-separated fallback
        return [p.strip() for p in ln.split(",")]

    # Pick the first row where fixed D/P/H positions are numeric (skip title/header lines).
    parts: list[str] | None = None
    for ln in lines:
        cand = _row_parts(ln)
        if len(cand) < 5:
            continue
        d_try = _safe_float(cand[2] if len(cand) > 2 else None)
        p_try = _safe_float(cand[3] if len(cand) > 3 else None)
        h_try = _safe_float(cand[4] if len(cand) > 4 else None)
        if d_try is not None and p_try is not None and h_try is not None:
            parts = cand
            break
    if parts is None:
        return None

    def _at(name: str) -> float | None:
        try:
            idx = _YMBSX_OUT_COLUMNS.index(name)
        except ValueError:
            return None
        if idx >= len(parts):
            return None
        return _safe_float(parts[idx])

    d, p, h = _at("D"), _at("P"), _at("H")
    if d is None or p is None or h is None:
        return None

    out: dict[str, Any] = {
        "D": d,
        "P": p,
        "H": h,
        "RER": _at("RER"),
        "HSPiP_SMILES": parts[0] if parts else "",
    }
    bpt = _at("BPt")
    # Official CLI name is HvBPt; some builds label the header ΔHv@BPt but same index.
    dhv = _at("HvBPt")
    anta, antb, antc = _at("AntA"), _at("AntB"), _at("AntC")
    if bpt is not None:
        out["BPt"] = bpt
    if dhv is not None:
        out["dHv_bpt"] = dhv
    if anta is not None:
        out["AntA"] = anta
    if antb is not None:
        out["AntB"] = antb
    if antc is not None:
        out["AntC"] = antc
    if len(parts) > 1 and parts[1]:
        out["Formula"] = parts[1]
    n_count = _at("N")
    s_count = _at("S")
    if n_count is not None:
        out["N#"] = n_count
    if s_count is not None:
        out["S#"] = s_count
    return out


def _clear_windows_clipboard() -> None:
    """HSPiP CLI uses the clipboard; clear it to reduce Interop failures."""
    try:
        import ctypes

        user32 = ctypes.windll.user32  # type: ignore[attr-defined]
        if user32.OpenClipboard(0):
            try:
                user32.EmptyClipboard()
            finally:
                user32.CloseClipboard()
    except Exception:
        pass


def run_hspip_for_smiles(
    smiles: str,
    *,
    hspip_dir: Path,
    timeout_s: float = 120.0,
    max_attempts: int = 3,
) -> dict[str, Any]:
    """
    Run ``HSPiP.exe Y-MBSX "<smiles>"`` in a writable HSPiP directory; parse Out.dat.

    The launcher ``.bat`` is written under ``%TEMP%`` (never under Program Files).
    Retries on clipboard / empty-output failures (same pattern as cas-to-HSPiP_data).
    """
    work = ensure_writable_hspip_dir(hspip_dir)
    out_file = work / "Out.dat"
    smi_q = smiles.replace('"', "")
    bat = Path(tempfile.gettempdir()) / f"_ghaz7_hspip_{os.getpid()}.bat"
    bat.write_text(
        f'@echo off\r\ncd /d "{work}"\r\nHSPiP.exe Y-MBSX "{smi_q}"\r\nexit\r\n',
        encoding="utf-8",
    )
    last_err = ""
    try:
        for attempt in range(1, max_attempts + 1):
            if out_file.exists():
                try:
                    out_file.unlink()
                except OSError as exc:
                    raise PermissionError(
                        f"Cannot clear {out_file} ({exc}). Use a writable HSPiP folder "
                        f"(Builder can copy Program Files -> data/hspip_workdir)."
                    ) from exc
            _clear_windows_clipboard()
            time.sleep(0.4)
            proc = subprocess.run(
                ["cmd", "/c", str(bat)],
                cwd=str(work),
                capture_output=True,
                text=True,
                timeout=timeout_s,
                check=False,
            )
            time.sleep(0.8)
            parsed = _parse_out_dat(out_file)
            if parsed is not None:
                return parsed
            last_err = (
                f"exit={proc.returncode}; stderr={(proc.stderr or '')[:300]}"
            )
            logger.warning(
                "HSPiP attempt %s/%s failed for %r (%s)",
                attempt,
                max_attempts,
                smiles,
                last_err,
            )
            _clear_windows_clipboard()
            time.sleep(1.5)
        raise RuntimeError(
            f"HSPiP produced no usable Out.dat for SMILES {smiles!r} ({last_err})"
        )
    finally:
        try:
            bat.unlink(missing_ok=True)
        except OSError:
            pass
        _clear_windows_clipboard()



def predicted_vp_mmhg_from_smiles(
    smiles: str,
    *,
    hspip_dir: Path | str | None = None,
    timeout_s: float = 120.0,
    cas: str | None = None,
) -> dict:
    """
    Run Y-MBSX and derive predicted VP (mmHg @ 25 °C / 298.15 K).

    Precedence (see `utils.hspip_vapor_pressure`): curated cas-to-HSPiP
    Antoine @298K > Clausius–Clapeyron > Y-MBSX Antoine.
    """
    from utils.hspip_vapor_pressure import (
        upsert_curated_antoine_row,
        vapor_pressure_from_ymb_row,
    )

    install = ensure_writable_hspip_dir(hspip_dir)
    parsed = run_hspip_for_smiles(smiles, hspip_dir=install, timeout_s=timeout_s)
    if (
        cas
        and parsed.get("AntA") is not None
        and parsed.get("AntB") is not None
        and parsed.get("AntC") is not None
    ):
        upsert_curated_antoine_row(
            cas,
            parsed["AntA"],
            parsed["AntB"],
            parsed["AntC"],
            source="hspip_ymb_sx_cached",
            notes="Cached from HSPiP Y-MBSX via cas-to-HSPiP_data workflow",
        )
    info = vapor_pressure_from_ymb_row(parsed, cas=cas)
    if info is None:
        raise RuntimeError(
            "HSPiP Y-MBSX returned HSP but no usable BPt/ΔHv/Antoine for VP"
        )
    info["ymb"] = {
        k: parsed.get(k)
        for k in (
            "D", "P", "H", "BPt", "dHv_bpt", "AntA", "AntB", "AntC",
            "Formula", "N#", "S#",
        )
    }
    return info


def hspip_vp_extra_for_cas(
    cas: str,
    *,
    smiles: str | None = None,
    hspip_dir: str | None = None,
    use_cache_only: bool = False,
) -> dict | None:
    """
    Build P2OASys extra_sources for predicted vapor pressure.

    Prefers curated cas-to-HSPiP Antoine @298K when present; otherwise runs
    Y-MBSX for C–C / Antoine. Does not invent values when nothing usable.
    """
    from utils.hspip_vapor_pressure import (
        get_curated_antoine,
        upsert_curated_antoine_row,
        vapor_pressure_from_ymb_row,
        vapor_pressure_predicted,
        vp_to_extra_sources,
    )

    curated = get_curated_antoine(cas)
    if curated is not None:
        src = str(curated.get("source") or "")
        if "ymb" not in src.lower():
            info = vapor_pressure_predicted(cas=cas, ymb_row={}, curated=curated)
            xs = vp_to_extra_sources(info)
            if xs:
                return xs

    if use_cache_only:
        return vp_to_extra_sources(vapor_pressure_predicted(cas=cas, ymb_row={}))

    smi = (smiles or "").strip() or None
    if not smi:
        smi = smiles_for_cas(cas)
    if not smi:
        return vp_to_extra_sources(vapor_pressure_predicted(cas=cas, ymb_row={}))

    try:
        install = ensure_writable_hspip_dir(hspip_dir)
        parsed = run_hspip_for_smiles(smi, hspip_dir=install)
    except Exception:
        return vp_to_extra_sources(vapor_pressure_predicted(cas=cas, ymb_row={}))

    if (
        parsed.get("AntA") is not None
        and parsed.get("AntB") is not None
        and parsed.get("AntC") is not None
    ):
        upsert_curated_antoine_row(
            cas,
            parsed["AntA"],
            parsed["AntB"],
            parsed["AntC"],
            source="hspip_ymb_sx_cached",
            notes="Cached from HSPiP Y-MBSX via cas-to-HSPiP_data workflow",
        )
    return vp_to_extra_sources(vapor_pressure_from_ymb_row(parsed, cas=cas))



def expand_hsp_for_cas_list(
    cas_list: list[str],
    *,
    hspip_dir: str | None = None,
    cache_csv: Path | str | None = None,
    skip_existing: bool = True,
    overwrite_non_cli: bool = False,
    progress: Callable[[int, int, HspExpandResult], None] | None = None,
) -> HspExpandSummary:
    """
    For each CAS: use cache if present, else PubChem SMILES → HSPiP → append cache.

    This is the automatic HSPiP database expansion path (cas-to-HSPiP_data workflow).

    When *overwrite_non_cli* is True, DoSS-bootstrap / non-``hspip_cli`` cache rows
    are recomputed so licensed HSPiP overrides DoSS.
    """
    install = ensure_writable_hspip_dir(hspip_dir)
    cache = Path(cache_csv) if cache_csv else hsp_cache_path()
    existing = _read_cache(cache)
    summary = HspExpandSummary(cache_path=str(cache))
    total = len(cas_list)

    for idx, raw in enumerate(cas_list, start=1):
        cas = (raw or "").strip()
        if not cas:
            continue
        key = normalize_cas_for_lookup(cas)
        hit = existing.get(key)
        src = str((hit or {}).get("source") or "").lower()
        is_cli = src.startswith("hspip_cli") or src in {"hspip_cli", "cli", "computed"}
        if skip_existing and hit is not None and (is_cli or not overwrite_non_cli):
            result = HspExpandResult(
                cas=cas,
                status="found_cache",
                delta_d=hit.get("delta_d"),
                delta_p=hit.get("delta_p"),
                delta_h=hit.get("delta_h"),
                message="Already in HSPiP cache" + ("" if is_cli else f" ({src or 'unknown'})"),
                source_path=str(cache),
            )
            summary.results.append(result)
            if progress:
                progress(idx, total, result)
            continue

        try:
            smiles = smiles_for_cas(cas)
            if not smiles:
                result = HspExpandResult(
                    cas=cas, status="error", message="No PubChem SMILES for CAS"
                )
                summary.results.append(result)
                if progress:
                    progress(idx, total, result)
                continue
            parsed = run_hspip_for_smiles(smiles, hspip_dir=install)
            d, p, h = float(parsed["D"]), float(parsed["P"]), float(parsed["H"])
            rer = parsed.get("RER")
            _append_cache_row(
                cache, cas=cas, delta_d=d, delta_p=p, delta_h=h, rer=rer, smiles=smiles
            )
            existing[key] = {"delta_d": d, "delta_p": p, "delta_h": h}
            result = HspExpandResult(
                cas=cas,
                status="computed",
                delta_d=d,
                delta_p=p,
                delta_h=h,
                rer=rer if isinstance(rer, float) else None,
                smiles=smiles,
                message="HSPiP CLI OK",
                source_path=str(cache),
            )
        except Exception as exc:
            logger.exception("HSPiP expand failed for %s", cas)
            result = HspExpandResult(cas=cas, status="error", message=str(exc))
        summary.results.append(result)
        if progress:
            progress(idx, total, result)

    return summary
