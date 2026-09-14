"""
Load IPCC GWP 100-year values from the atmo folder (parquet from Federal LCA Commons).
Used by P2OASys for Atmospheric Hazard when atmo/IPCC parquet is available.
Prefer AR6-100; fallback AR5-100 then AR4-100.

Also implements the Atmospheric GWP rule:
  - non-gas (liquid/solid/aerosol liquid at STP) → GWP = 0 and ODP = 0
    (heuristic_non_gas_gwp0 / heuristic_non_gas_odp0)
  - gas → lookup GWP100 / ODP from local IPCC ATMO parquet and/or odp_gwp_by_cas.csv
  - gas and not in tables → leave missing (do not invent)

Future refresh sources (prefer local files at runtime; URLs for documentation only):
  - EPA GHG inventory / GWP: https://www.epa.gov/ghgemissions/understanding-global-warming-potentials
  - EPA ODS class lists: https://www.epa.gov/ozone-layer-protection/ozone-depleting-substances
  - Federal LCA Commons IPCC factors (parquet under fastP2OASys/atmo)
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional

# EPA / ODS documentation URLs (not scraped at runtime).
EPA_GWP_DOC_URL = "https://www.epa.gov/ghgemissions/understanding-global-warming-potentials"
EPA_ODS_DOC_URL = "https://www.epa.gov/ozone-layer-protection/ozone-depleting-substances"


def _normalize_cas(cas: str) -> str:
    """Normalize CAS to digits-only for lookup."""
    if not cas or not isinstance(cas, str):
        return ""
    s = str(cas).strip()
    if s in ("(no data)", "", "nan"):
        return ""
    return "".join(c for c in s if c.isdigit())


def resolve_atmo_dir(
    explicit: Path | str | None = None,
    *,
    repo_root: Path | str | None = None,
    fastp2oasys_dir: Path | str | None = None,
) -> Optional[Path]:
    """
    Locate IPCC atmo folder. Preference:
      1) explicit / ATMO_DIR env if it exists
      2) <repo>/data/atmo
      3) <fastP2OASys>/atmo (sibling hazquery tree)
    """
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    env = (os.environ.get("ATMO_DIR") or "").strip()
    if env:
        candidates.append(Path(env))
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent
    candidates.append(root / "data" / "atmo")
    if fastp2oasys_dir:
        candidates.append(Path(fastp2oasys_dir) / "atmo")
    else:
        try:
            import config as _cfg

            fp = getattr(_cfg, "FASTP2OASYS_DIR", None)
            if fp:
                candidates.append(Path(fp) / "atmo")
        except Exception:
            pass
        # hazquery/fastP2OASys/atmo from GHaz7 app root (.../GHhaz6/GHaz7/app → ../../../fastP2OASys)
        candidates.append(root.parent.parent.parent / "fastP2OASys" / "atmo")

    seen: set[str] = set()
    for c in candidates:
        try:
            key = str(c.resolve())
        except Exception:
            key = str(c)
        if key in seen:
            continue
        seen.add(key)
        if c.is_dir() and (any(c.glob("IPCC_*.parquet")) or any(c.glob("*.parquet"))):
            return c
    return None


def load_ipcc_gwp_100_from_atmo(atmo_dir: Path | str) -> dict[str, float]:
    """
    Load GWP 100-year (kg CO2e/kg) by CAS from atmo folder.
    Looks for IPCC_v*.parquet; uses Indicator AR6-100, then AR5-100, then AR4-100.
    Returns dict normalized_cas -> GWP (float).
    """
    atmo_dir = Path(atmo_dir)
    if not atmo_dir.is_dir():
        return {}

    parquet_files = sorted(atmo_dir.glob("IPCC_*.parquet"), reverse=True)
    if not parquet_files:
        return {}

    try:
        import pandas as pd
    except ImportError:
        return {}

    try:
        df = pd.read_parquet(parquet_files[0], columns=["Indicator", "CAS No", "Characterization Factor"])
    except Exception:
        return {}

    order = ["AR6-100", "AR5-100", "AR4-100"]
    out: dict[str, float] = {}
    for ind in order:
        sub = df[df["Indicator"] == ind]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            cas_raw = row.get("CAS No")
            cas_norm = _normalize_cas(str(cas_raw) if pd.notna(cas_raw) else "")
            if not cas_norm:
                continue
            try:
                gwp = float(row["Characterization Factor"])
            except (TypeError, ValueError):
                continue
            if cas_norm not in out:
                out[cas_norm] = gwp
    return out


_GAS_RE = re.compile(
    r"\b("
    r"gas|gaseous|compressed\s+gas|liquefied\s+gas|liquified\s+gas|"
    r"pressurized\s+gas|dissolved\s+gas|refrigerant\s+gas"
    r")\b",
    re.I,
)
_NON_GAS_RE = re.compile(
    r"\b("
    r"liquid|solid|powder|crystal|crystalline|pellet|flake|paste|"
    r"solution|aqueous|suspension|emulsion|wax|gel|slurry|"
    r"aerosol\s*\(?liquid\)?|liquid\s+aerosol"
    r")\b",
    re.I,
)


def classify_physical_state(*texts: Any) -> str:
    """
    Return ``gas``, ``non_gas``, or ``unknown`` from SDS §9 / PubChem phrases.

    Treat gas / compressed gas / liquefied gas as gas.
    Liquids, solids, solutions, and liquid aerosols → non_gas.
    If both gas and liquid cues appear (e.g. "liquefied gas"), gas wins.
    """
    blob = " ".join(str(t) for t in texts if t not in (None, "")).strip()
    if not blob:
        return "unknown"
    # SDS heading boilerplate is not a physical-state cue
    blob = re.sub(r"(?i)flammability\s*\(\s*solid\s*,\s*gas\s*\)", " ", blob)
    # Explicit liquefied/compressed gas before bare "liquid"
    if _GAS_RE.search(blob):
        return "gas"
    if _NON_GAS_RE.search(blob):
        return "non_gas"
    return "unknown"


def infer_physical_state_from_sources(
    *,
    sds_fields: dict[str, Any] | None = None,
    structured_sds: dict[str, Any] | None = None,
    pubchem: dict[str, Any] | None = None,
    hazard_data: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """
    Resolve physical state + short provenance tag.
    Returns (state, source_tag) where state is gas|non_gas|unknown.
    """
    chunks: list[str] = []
    source = "unknown"

    if sds_fields:
        for k in ("physical_state", "form", "appearance", "physical_form"):
            v = sds_fields.get(k)
            if v:
                chunks.append(str(v))
                source = "sds_fields"
    if structured_sds:
        sec9 = structured_sds.get("section_9") or {}
        for k in ("physical_state", "form", "appearance"):
            v = sec9.get(k)
            if v:
                chunks.append(str(v))
                source = "sds_section_9"
    if pubchem:
        for k in (
            "physical_description",
            "physical_state",
            "form",
            "appearance",
            "record_title",
        ):
            v = pubchem.get(k)
            if v:
                chunks.append(str(v))
                if source == "unknown":
                    source = "pubchem"
    if hazard_data:
        ps = (hazard_data.get("physical_state") or hazard_data.get("physical_description"))
        if ps:
            chunks.append(str(ps))
            if source == "unknown":
                source = "hazard_data"
        hm = hazard_data.get("hazard_metrics") or {}
        for item in hm.get("other_designations") or []:
            s = str(item)
            if re.search(r"physical\s*state|form\s*:|appearance", s, re.I):
                chunks.append(s)
                if source == "unknown":
                    source = "hazard_metrics"

    state = classify_physical_state(*chunks)
    return state, (source if state != "unknown" else "unknown")


def apply_atmospheric_gwp_rule(
    extra_sources: dict[str, Any] | None,
    *,
    physical_state: str,
    state_source: str = "unknown",
) -> dict[str, Any]:
    """
    Apply non-gas → GWP=0 and ODP=0 heuristics; leave gas lookups untouched.

    Tags GWP=0 with ``heuristic_non_gas_gwp0`` / predicted-or-heuristic tier for HITL.
    """
    out: dict[str, Any] = dict(extra_sources) if extra_sources else {}
    hm = dict(out.get("hazard_metrics") or {})
    designations = list(hm.get("other_designations") or [])
    notes = list(out.get("_pipeline_notes") or [])

    if physical_state == "non_gas":
        # Remove prior GWP/ODP designations; set numeric 0 for both.
        designations = [
            d
            for d in designations
            if not re.match(r"^\s*(GWP|ODP)\b", str(d), re.I)
        ]
        designations.append("GWP 0")
        designations.append("ODP 0")
        hm["gwp100"] = [0.0]
        hm["odp"] = [0.0]
        hm["other_designations"] = designations
        out["hazard_metrics"] = hm
        out["gwp_meta"] = {
            "gwp100": 0.0,
            "source": "heuristic_non_gas_gwp0",
            "tier": "predicted_or_heuristic",
            "physical_state": physical_state,
            "state_source": state_source,
        }
        out["odp_meta"] = {
            "odp": 0.0,
            "source": "heuristic_non_gas_odp0",
            "tier": "predicted_or_heuristic",
            "physical_state": physical_state,
            "state_source": state_source,
        }
        note = (
            f"Atmospheric GWP=0/ODP=0 via non-gas heuristic "
            f"(state={physical_state} from {state_source})"
        )
        if note not in notes:
            notes.append(note)
        out["_pipeline_notes"] = notes
        return out

    # gas or unknown: do not invent; keep whatever lookups already placed
    if physical_state == "gas":
        out.setdefault("gwp_meta", {})
        meta = dict(out.get("gwp_meta") or {})
        meta["physical_state"] = "gas"
        meta["state_source"] = state_source
        if hm.get("gwp100"):
            meta.setdefault("source", "lookup_table")
            try:
                meta.setdefault("gwp100", float(hm["gwp100"][0]))
            except Exception:
                pass
        else:
            meta.setdefault("source", "missing")
            meta.setdefault("gwp100", None)
        out["gwp_meta"] = meta

        odp_meta = dict(out.get("odp_meta") or {})
        odp_meta["physical_state"] = "gas"
        odp_meta["state_source"] = state_source
        if hm.get("odp"):
            odp_meta.setdefault("source", "lookup_table")
            try:
                odp_meta.setdefault("odp", float(hm["odp"][0]))
            except Exception:
                pass
        else:
            odp_meta.setdefault("source", "missing")
            odp_meta.setdefault("odp", None)
        out["odp_meta"] = odp_meta

        if not hm.get("gwp100"):
            note = "Atmospheric GWP missing for gas (not in ATMO/CSV tables)"
            if note not in notes:
                notes.append(note)
        if not hm.get("odp"):
            note2 = "Atmospheric ODP missing for gas (not in ODP CSV/list)"
            if note2 not in notes:
                notes.append(note2)
        out["_pipeline_notes"] = notes
    return out


def merge_gwp_into_hazard_data(hazard_data: dict[str, Any], extra_sources: dict[str, Any] | None) -> dict[str, Any]:
    """Ensure gwp_meta / gwp100 land on hazard_data for scorers and HITL."""
    if not hazard_data:
        hazard_data = {}
    if not extra_sources:
        return hazard_data
    meta = extra_sources.get("gwp_meta")
    if meta:
        hazard_data["gwp_meta"] = meta
    odp_meta = extra_sources.get("odp_meta")
    if odp_meta:
        hazard_data["odp_meta"] = odp_meta
    hm_extra = extra_sources.get("hazard_metrics") or {}
    if hm_extra.get("gwp100") is not None:
        hazard_data.setdefault("hazard_metrics", {}).setdefault("gwp100", [])
        if not hazard_data["hazard_metrics"]["gwp100"]:
            hazard_data["hazard_metrics"]["gwp100"] = list(hm_extra["gwp100"])
    if hm_extra.get("odp") is not None:
        hazard_data.setdefault("hazard_metrics", {}).setdefault("odp", [])
        if not hazard_data["hazard_metrics"]["odp"]:
            hazard_data["hazard_metrics"]["odp"] = list(hm_extra["odp"])
    return hazard_data


# ---------------------------------------------------------------------------
# Acid Rain Formation (combustion SOx/NOx structural heuristic)
# ---------------------------------------------------------------------------
# P2OASys Atmospheric "Acid Rain Formation" Key Phrases (matrix):
#   - Does not contain S or N
#   - Contain S or N but does not form SOx or NOx
#   - Product may form SOx or NOx upon combustion
#   - Produces SOx and NOx
#
# We only claim the structural presence/absence of S/N (element counts / SMILES /
# formula). Presence → "may form SOx or NOx upon combustion" — honest heuristic,
# not measured acid-rain potential. Absence → "Does not contain S or N".

_ACID_RAIN_NO_SN = "Does not contain S or N"
_ACID_RAIN_MAY_FORM = "Product may form SOx or NOx upon combustion"


def molecule_has_s_or_n(
    *,
    smiles: str | None = None,
    formula: str | None = None,
    element_counts: dict[str, Any] | None = None,
    hspip_row: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """
    Detect sulfur and/or nitrogen for Acid Rain combustion heuristic.

    Returns (has_s_or_n, detail) where detail includes has_s, has_n, evidence.
    """
    has_s = False
    has_n = False
    evidence: list[str] = []

    counts = dict(element_counts or {})
    if hspip_row:
        for k_src, k_dst in (("S#", "S"), ("N#", "N"), ("S", "S"), ("N", "N")):
            if hspip_row.get(k_src) is not None and k_dst not in counts:
                try:
                    counts[k_dst] = float(hspip_row[k_src])
                except (TypeError, ValueError):
                    pass
        if hspip_row.get("Formula") and not formula:
            formula = str(hspip_row.get("Formula"))

    for el, flag_name in (("S", "has_s"), ("N", "has_n")):
        raw = counts.get(el)
        try:
            n = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            n = 0.0
        if n > 0:
            if el == "S":
                has_s = True
            else:
                has_n = True
            evidence.append(f"element_counts:{el}={n:g}")

    if formula:
        # Molecular formula tokens like CH4N2O, H2SO4 — element symbols.
        import re as _re

        f = str(formula).strip()
        if _re.search(r"(?<![a-z])S(?![a-z])", f):
            has_s = True
            evidence.append(f"formula:S:{f}")
        if _re.search(r"(?<![a-z])N(?![a-z])", f):
            has_n = True
            evidence.append(f"formula:N:{f}")

    if smiles:
        s = str(smiles)
        # Organic SMILES: S/s = sulfur, N/n = nitrogen (ignore Cl, Br, Si, Na, ...)
        # Strip two-letter elements that contain N/S as second letter is rare in SMILES;
        # use RDKit atom query when available.
        counted = False
        try:
            from rdkit import Chem

            mol = Chem.MolFromSmiles(s)
            if mol is not None:
                for atom in mol.GetAtoms():
                    z = atom.GetAtomicNum()
                    if z == 16:
                        has_s = True
                    elif z == 7:
                        has_n = True
                counted = True
                evidence.append("smiles:rdkit")
        except Exception:
            counted = False
        if not counted:
            import re as _re

            # Remove bracket atoms' isotope/charge noise; rough fallback
            if _re.search(r"(?<![A-Z])S(?![a-z])", s) or "[S" in s or "s" in s:
                # 's' aromatic sulfur; avoid matching inside other tokens
                if "S" in s or "s" in s:
                    has_s = True
                    evidence.append("smiles:regex:S")
            if _re.search(r"(?<![A-Z])N(?![a-z])", s) or "[N" in s or "n" in s:
                if "N" in s or "n" in s:
                    has_n = True
                    evidence.append("smiles:regex:N")

    return (has_s or has_n), {
        "has_s": has_s,
        "has_n": has_n,
        "evidence": evidence,
    }


def acid_rain_phrase_for_structure(
    *,
    smiles: str | None = None,
    formula: str | None = None,
    element_counts: dict[str, Any] | None = None,
    hspip_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return phrase + meta for Acid Rain Formation extras."""
    has_sn, detail = molecule_has_s_or_n(
        smiles=smiles,
        formula=formula,
        element_counts=element_counts,
        hspip_row=hspip_row,
    )
    if has_sn:
        phrase = _ACID_RAIN_MAY_FORM
        support = "has_S_or_N"
    else:
        phrase = _ACID_RAIN_NO_SN
        support = "no_S_or_N"
    return {
        "phrase": phrase,
        "support": support,
        "tier": "predicted_or_heuristic",
        "source": "structural_combustion_sox_nox_heuristic",
        "honesty": (
            "Structural heuristic for combustion SOx/NOx potential from S/N "
            "presence; not measured acid-rain potential."
        ),
        **detail,
    }


def apply_acid_rain_combustion_heuristic(
    extra_sources: dict[str, Any] | None,
    *,
    smiles: str | None = None,
    formula: str | None = None,
    element_counts: dict[str, Any] | None = None,
    hspip_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Feed Atmospheric Acid Rain Formation key phrase into other_designations.

    Safe to call with empty structure: leaves extras unchanged when no cues.
    """
    out: dict[str, Any] = dict(extra_sources) if extra_sources else {}
    if not any([smiles, formula, element_counts, hspip_row]):
        return out

    info = acid_rain_phrase_for_structure(
        smiles=smiles,
        formula=formula,
        element_counts=element_counts,
        hspip_row=hspip_row,
    )
    hm = dict(out.get("hazard_metrics") or {})
    designations = list(hm.get("other_designations") or [])
    # Drop prior acid-rain heuristic phrases to avoid conflicting first-match scores.
    drop_frags = (
        "does not contain s or n",
        "contain s or n but does not form",
        "may form sox or nox",
        "produces sox and nox",
        "acid rain",
    )
    designations = [
        d
        for d in designations
        if not any(f in str(d).lower() for f in drop_frags)
    ]
    designations.append(info["phrase"])
    hm["other_designations"] = designations
    out["hazard_metrics"] = hm
    out["acid_rain_meta"] = info
    notes = list(out.get("_pipeline_notes") or [])
    note = (
        f"Atmospheric Acid Rain Formation: '{info['phrase']}' "
        f"({info['honesty']} support={info['support']})"
    )
    if note not in notes:
        notes.append(note)
    out["_pipeline_notes"] = notes
    return out


def merge_acid_rain_into_hazard_data(
    hazard_data: dict[str, Any], extra_sources: dict[str, Any] | None
) -> dict[str, Any]:
    if not hazard_data:
        hazard_data = {}
    if not extra_sources:
        return hazard_data
    meta = extra_sources.get("acid_rain_meta")
    if meta:
        hazard_data["acid_rain_meta"] = meta
    return hazard_data
