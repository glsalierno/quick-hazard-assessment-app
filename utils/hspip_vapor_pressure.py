"""
Derive predicted vapor pressure (mmHg @ 25 °C / 298.15 K) for P2OASys Physical gap-fill.

Sources and precedence (predicted path only; experimental PubChem/SDS always wins upstream):

1. **cas-to-HSPiP curated Antoine @ 298.15 K** — coefficients from
   ``data/cas_hspip_antoine.csv`` (offline table filled from the
   https://github.com/glsalierno/cas-to-HSPiP_data workflow / HSPiP dataset
   exports). Prefer these over live Y-MBSX Antoine when present and when they
   differ from (or are tagged as higher-quality than) Y-MBSX estimates.
2. **Clausius–Clapeyron** from HSPiP Y-MBSX BPt + ΔHv@BPt (P(Tb)=1 atm).
3. **Live / cached Y-MBSX Antoine** — same equation form as curated, but
   structure-estimated coeffs (methanol Y-MBSX historically over-predicts vs
   literature ~127 mmHg).

Antoine form used by HSPiP Y-MBSX ``Out.dat`` / Pirika-style (documented from
cas-to-HSPiP_data + local Out.dat):

    log10(P_mmHg) = A - B / (T_C + C)

with T in °C and P in mmHg. Curated rows may declare alternate forms
(log10|ln, T in C|K, P in mmHg|bar); the calculator converts to mmHg @ 25 °C.

All predicted values are tagged ``tier=predicted`` with a cited ``source``.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any, Optional

# HSPiP / cas-to-HSPiP default Antoine equation metadata
ANTOINE_FORM_HSPIP = {
    "log_base": "log10",  # log10 vs ln
    "t_unit": "C",        # C vs K
    "p_unit": "mmHg",     # mmHg vs bar
    "equation": "log10(P_mmHg) = A - B/(T_C + C)",
}

T_REF_C = 25.0
T_REF_K = 298.15


def _normalize_cas(cas: str | None) -> str:
    if not cas:
        return ""
    return "".join(c for c in str(cas) if c.isdigit())


def antoine_csv_path(explicit: Path | str | None = None) -> Path:
    if explicit:
        return Path(explicit)
    try:
        import config as _cfg

        data = Path(getattr(_cfg, "DATA_DIR", "data"))
        override = (getattr(_cfg, "CAS_HSPIP_ANTOINE_CSV", None) or "").strip()
        if override:
            return Path(override)
        return data / "cas_hspip_antoine.csv"
    except Exception:
        return Path("data") / "cas_hspip_antoine.csv"


def load_curated_antoine_by_cas(
    path: Path | str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Load curated CAS → Antoine coefficients.

    CSV columns (header required):
      cas, AntA, AntB, AntC [, log_base, t_unit, p_unit, source, notes]
    """
    p = antoine_csv_path(path)
    if not p.is_file():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with open(p, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            key = _normalize_cas(row.get("cas") or row.get("CAS"))
            if not key:
                continue
            try:
                a = float(row["AntA"])
                b = float(row["AntB"])
                c = float(row["AntC"])
            except (KeyError, TypeError, ValueError):
                continue
            out[key] = {
                "AntA": a,
                "AntB": b,
                "AntC": c,
                "log_base": (row.get("log_base") or "log10").strip() or "log10",
                "t_unit": (row.get("t_unit") or "C").strip() or "C",
                "p_unit": (row.get("p_unit") or "mmHg").strip() or "mmHg",
                "source": (row.get("source") or "cas_hspip_antoine").strip(),
                "notes": (row.get("notes") or "").strip(),
                "cas": row.get("cas") or row.get("CAS") or key,
            }
    return out


def get_curated_antoine(cas: str, path: Path | str | None = None) -> dict[str, Any] | None:
    hit = load_curated_antoine_by_cas(path).get(_normalize_cas(cas))
    return dict(hit) if hit else None


def upsert_curated_antoine_row(
    cas: str,
    ant_a: float,
    ant_b: float,
    ant_c: float,
    *,
    path: Path | str | None = None,
    source: str = "hspip_ymb_sx_cached",
    log_base: str = "log10",
    t_unit: str = "C",
    p_unit: str = "mmHg",
    notes: str = "",
    overwrite_ymb_only: bool = True,
) -> bool:
    """
    Append/update a row in the curated Antoine CSV.

    When *overwrite_ymb_only* is True, do not replace rows whose source is not
    a Y-MBSX cache tag (protects literature / dataset overrides).
    """
    p = antoine_csv_path(path)
    existing = load_curated_antoine_by_cas(p)
    key = _normalize_cas(cas)
    if not key:
        return False
    prev = existing.get(key)
    if prev and overwrite_ymb_only:
        prev_src = str(prev.get("source") or "")
        if prev_src and "ymb" not in prev_src.lower() and prev_src != "hspip_ymb_sx_cached":
            return False
    display = str(cas).strip() or key
    if len(key) >= 5 and "-" not in display:
        display = f"{key[:-3]}-{key[-3:-1]}-{key[-1]}"
    existing[key] = {
        "cas": display,
        "AntA": float(ant_a),
        "AntB": float(ant_b),
        "AntC": float(ant_c),
        "log_base": log_base,
        "t_unit": t_unit,
        "p_unit": p_unit,
        "source": source,
        "notes": notes,
    }
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["cas", "AntA", "AntB", "AntC", "log_base", "t_unit", "p_unit", "source", "notes"]
    with open(p, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for k in sorted(existing, key=lambda x: (len(x), x)):
            writer.writerow({fn: existing[k].get(fn, "") for fn in fieldnames})
    return True


def vp_mmhg_clausius_clapeyron(
    bpt_c: float,
    dhv_kj_per_mol: float,
    *,
    t_c: float = T_REF_C,
) -> Optional[float]:
    """P(mmHg) at t_c from BPt (°C) and ΔHv (kJ/mol), assuming P(Tb)=760 mmHg."""
    try:
        tb_k = float(bpt_c) + 273.15
        t_k = float(t_c) + 273.15
        dh = float(dhv_kj_per_mol) * 1000.0  # J/mol
    except (TypeError, ValueError):
        return None
    if tb_k <= 0 or t_k <= 0 or dh <= 0:
        return None
    r = 8.314462618
    ln_ratio = -(dh / r) * (1.0 / t_k - 1.0 / tb_k)
    try:
        p = 760.0 * math.exp(ln_ratio)
    except OverflowError:
        return None
    if not math.isfinite(p) or p <= 0:
        return None
    return float(p)


def vp_mmhg_antoine(
    ant_a: float,
    ant_b: float,
    ant_c: float,
    *,
    t_c: float = T_REF_C,
    log_base: str = "log10",
    t_unit: str = "C",
    p_unit: str = "mmHg",
) -> Optional[float]:
    """
    Evaluate Antoine and return P in mmHg at *t_c* (°C).

    Default form (HSPiP Y-MBSX / cas-to-HSPiP):
      log10(P_mmHg) = A - B/(T_C + C)
    """
    try:
        a, b, c = float(ant_a), float(ant_b), float(ant_c)
        t_c = float(t_c)
    except (TypeError, ValueError):
        return None
    t_unit_n = (t_unit or "C").strip().upper()
    if t_unit_n in ("K", "KELVIN"):
        t_eval = t_c + 273.15
    else:
        t_eval = t_c
    denom = t_eval + c
    if abs(denom) < 1e-9:
        return None
    try:
        base = (log_base or "log10").strip().lower()
        if base in ("ln", "log_e", "loge", "natural"):
            p = math.exp(a - b / denom)
        else:
            p = 10.0 ** (a - b / denom)
    except OverflowError:
        return None
    if not math.isfinite(p) or p <= 0:
        return None
    p_unit_n = (p_unit or "mmHg").strip().lower()
    if p_unit_n in ("bar", "bars"):
        p = p * 750.0616827  # bar → mmHg
    elif p_unit_n in ("pa", "pascal"):
        p = p / 133.322368
    elif p_unit_n in ("kpa",):
        p = p * 7.500616827
    elif p_unit_n in ("torr", "mmhg", "mm_hg", "mm hg"):
        pass
    else:
        # assume already mmHg
        pass
    if not math.isfinite(p) or p <= 0:
        return None
    return float(p)


def _antoine_from_row(row: dict[str, Any]) -> Optional[float]:
    if row.get("AntA") is None or row.get("AntB") is None or row.get("AntC") is None:
        return None
    return vp_mmhg_antoine(
        row["AntA"],
        row["AntB"],
        row["AntC"],
        log_base=str(row.get("log_base") or "log10"),
        t_unit=str(row.get("t_unit") or "C"),
        p_unit=str(row.get("p_unit") or "mmHg"),
    )


def _is_ymb_source(source: str | None) -> bool:
    s = (source or "").lower()
    return "ymb" in s or s in {"hspip_ymb_sx", "hspip_ymb_sx_cached"}


def vapor_pressure_predicted(
    *,
    cas: str | None = None,
    ymb_row: dict[str, Any] | None = None,
    curated: dict[str, Any] | None = None,
    curated_path: Path | str | None = None,
) -> dict[str, Any] | None:
    """
    Build predicted VP meta with precedence:

      cas-to-HSPiP curated Antoine @298K (non-YMB / distinct override)
        > Y-MBSX Clausius–Clapeyron (BPt+ΔHv)
        > Y-MBSX Antoine (live or cached-as-ymb)

    Curated rows tagged as Y-MBSX cache do **not** leapfrog C–C (methanol
    Y-MBSX Antoine over-predicts); literature/dataset overrides do.
    """
    ymb_row = ymb_row or {}
    cur = curated
    if cur is None and cas:
        cur = get_curated_antoine(cas, curated_path)

    cc = None
    bpt = ymb_row.get("BPt")
    dhv = ymb_row.get("dHv_bpt")
    if dhv is None:
        dhv = ymb_row.get("delta_hv_bpt")
    if dhv is None:
        dhv = ymb_row.get("Hv_bpt")
    if bpt is not None and dhv is not None:
        try:
            cc = vp_mmhg_clausius_clapeyron(float(bpt), float(dhv))
        except (TypeError, ValueError):
            cc = None

    ymb_ant = None
    if (
        ymb_row.get("AntA") is not None
        and ymb_row.get("AntB") is not None
        and ymb_row.get("AntC") is not None
    ):
        ymb_ant = vp_mmhg_antoine(ymb_row["AntA"], ymb_row["AntB"], ymb_row["AntC"])

    cur_ant = _antoine_from_row(cur) if cur else None
    cur_src = str((cur or {}).get("source") or "cas_hspip_antoine")
    curated_outranks = bool(cur_ant is not None and not _is_ymb_source(cur_src))

    if curated_outranks:
        primary = cur_ant
        method = "cas_hspip_antoine_298k"
        source = cur_src
    elif cc is not None:
        primary = cc
        method = "clausius_clapeyron_bpt_dhv"
        source = "hspip_ymb_sx"
    elif cur_ant is not None:
        primary = cur_ant
        method = "cas_hspip_antoine_298k_ymb_cached"
        source = cur_src
    elif ymb_ant is not None:
        primary = ymb_ant
        method = "antoine_ymb_sx"
        source = "hspip_ymb_sx"
    else:
        return None

    return {
        "vp_mmhg_25c": primary,
        "vp_mmhg_298k": primary,
        "method": method,
        "vp_mmhg_25c_clausius": cc,
        "vp_mmhg_25c_antoine_ymb": ymb_ant,
        "vp_mmhg_25c_antoine_curated": cur_ant,
        "bpt_c": bpt,
        "dhv_kj_mol": dhv,
        "antoine_form": dict(ANTOINE_FORM_HSPIP),
        "curated_antoine": (
            {
                "AntA": cur.get("AntA"),
                "AntB": cur.get("AntB"),
                "AntC": cur.get("AntC"),
                "log_base": cur.get("log_base"),
                "t_unit": cur.get("t_unit"),
                "p_unit": cur.get("p_unit"),
                "source": cur_src,
            }
            if cur
            else None
        ),
        "source": source,
        "tier": "predicted",
        "t_ref_k": T_REF_K,
    }


def vapor_pressure_from_ymb_row(
    row: dict[str, Any],
    *,
    cas: str | None = None,
    curated_path: Path | str | None = None,
) -> dict[str, Any] | None:
    """
    From a parsed Y-MBSX row (+ optional CAS for curated Antoine), return meta +
    primary mmHg @ 25 °C / 298.15 K.
    """
    if not row and not cas:
        return None
    return vapor_pressure_predicted(cas=cas, ymb_row=row or {}, curated_path=curated_path)


def vp_to_extra_sources(vp_info: dict[str, Any] | None) -> dict[str, Any] | None:
    """Map VP info into P2OASys ``extra_sources`` (other_designations mmHg string)."""
    if not vp_info or vp_info.get("vp_mmhg_25c") is None:
        return None
    v = float(vp_info["vp_mmhg_25c"])
    src = str(vp_info.get("source") or "hspip")
    method = str(vp_info.get("method") or "")
    if "antoine" in method and "cas_hspip" in method:
        label = f"{v:.6g} mmHg (cas-to-HSPiP Antoine predicted @298.15K; {src})"
    elif "clausius" in method:
        label = f"{v:.6g} mmHg (HSPiP Y-MBSX Clausius-Clapeyron predicted @25C)"
    else:
        label = f"{v:.6g} mmHg (HSPiP Y-MBSX Antoine predicted @25C; {src})"
    return {
        "hazard_metrics": {
            "other_designations": [label],
            "vapor_pressure_mmhg": [v],
        },
        "vp_meta": dict(vp_info),
        "_pipeline_notes": [
            f"Vapor pressure {v:.6g} mmHg @298.15K via {method} (source={src}; "
            f"form={ANTOINE_FORM_HSPIP['equation']})"
        ],
    }
