"""
Load IPCC GWP 100-year values from the atmo folder (parquet from Federal LCA Commons).
Used by P2OASys for Atmospheric Hazard when atmo/IPCC parquet is available.
Prefer AR6-100; fallback AR5-100 then AR4-100.

Also implements the Atmospheric GWP/ODP rule:
  - non-gas (liquid/solid/aerosol liquid at STP) → GWP = 0 and ODP = 0
    (heuristic_non_gas_gwp0 / heuristic_non_gas_odp0)
  - gas → lookup GWP100 / ODP from local IPCC ATMO parquet and/or odp_gwp_by_cas.csv
  - gas/unknown and not in authoritative tables → GWP = 0 and ODP = 0
    (default_not_on_authoritative_list) — closed Montreal ODP set + IPCC GHG metrics

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


def _set_gwp_odp_zero(
    out: dict[str, Any],
    *,
    gwp_source: str,
    odp_source: str,
    physical_state: str,
    state_source: str,
    note: str,
) -> dict[str, Any]:
    hm = dict(out.get("hazard_metrics") or {})
    designations = [
        d
        for d in list(hm.get("other_designations") or [])
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
        "source": gwp_source,
        "tier": "predicted_or_heuristic",
        "physical_state": physical_state,
        "state_source": state_source,
    }
    out["odp_meta"] = {
        "odp": 0.0,
        "source": odp_source,
        "tier": "predicted_or_heuristic",
        "physical_state": physical_state,
        "state_source": state_source,
    }
    notes = list(out.get("_pipeline_notes") or [])
    if note not in notes:
        notes.append(note)
    out["_pipeline_notes"] = notes
    return out


def apply_atmospheric_gwp_rule(
    extra_sources: dict[str, Any] | None,
    *,
    physical_state: str,
    state_source: str = "unknown",
    default_zero_if_unlisted: bool = True,
) -> dict[str, Any]:
    """
    Apply atmospheric GWP/ODP fill rules.

    - non-gas → GWP=0 / ODP=0 (``heuristic_non_gas_*``)
    - gas/unknown with lookup hit → keep table values
    - gas/unknown without lookup → GWP=0 / ODP=0 when ``default_zero_if_unlisted``
      (``default_not_on_authoritative_list``)
    """
    out: dict[str, Any] = dict(extra_sources) if extra_sources else {}
    hm = dict(out.get("hazard_metrics") or {})
    notes = list(out.get("_pipeline_notes") or [])

    if physical_state == "non_gas":
        return _set_gwp_odp_zero(
            out,
            gwp_source="heuristic_non_gas_gwp0",
            odp_source="heuristic_non_gas_odp0",
            physical_state=physical_state,
            state_source=state_source,
            note=(
                f"Atmospheric GWP=0/ODP=0 via non-gas heuristic "
                f"(state={physical_state} from {state_source})"
            ),
        )

    # gas or unknown: keep lookups; optionally default missing to 0
    state_label = physical_state if physical_state in ("gas", "unknown") else "unknown"
    gwp_meta = dict(out.get("gwp_meta") or {})
    gwp_meta["physical_state"] = state_label
    gwp_meta["state_source"] = state_source
    odp_meta = dict(out.get("odp_meta") or {})
    odp_meta["physical_state"] = state_label
    odp_meta["state_source"] = state_source

    has_gwp = bool(hm.get("gwp100"))
    has_odp = bool(hm.get("odp"))

    if has_gwp:
        gwp_meta.setdefault("source", "lookup_table")
        try:
            gwp_meta.setdefault("gwp100", float(hm["gwp100"][0]))
        except Exception:
            pass
    if has_odp:
        odp_meta.setdefault("source", "lookup_table")
        try:
            odp_meta.setdefault("odp", float(hm["odp"][0]))
        except Exception:
            pass

    if default_zero_if_unlisted and (not has_gwp or not has_odp):
        designations = list(hm.get("other_designations") or [])
        if not has_gwp:
            designations = [d for d in designations if not re.match(r"^\s*GWP\b", str(d), re.I)]
            designations.append("GWP 0")
            hm["gwp100"] = [0.0]
            gwp_meta.update(
                {
                    "gwp100": 0.0,
                    "source": "default_not_on_authoritative_list",
                    "tier": "predicted_or_heuristic",
                }
            )
            note = (
                f"Atmospheric GWP=0 default (not on IPCC/EPA GHG tables; "
                f"state={state_label} from {state_source})"
            )
            if note not in notes:
                notes.append(note)
        if not has_odp:
            designations = [d for d in designations if not re.match(r"^\s*ODP\b", str(d), re.I)]
            designations.append("ODP 0")
            hm["odp"] = [0.0]
            odp_meta.update(
                {
                    "odp": 0.0,
                    "source": "default_not_on_authoritative_list",
                    "tier": "predicted_or_heuristic",
                }
            )
            note2 = (
                f"Atmospheric ODP=0 default (not on Montreal/EPA ODS list; "
                f"state={state_label} from {state_source})"
            )
            if note2 not in notes:
                notes.append(note2)
        hm["other_designations"] = designations
        out["hazard_metrics"] = hm
    elif not has_gwp or not has_odp:
        if not has_gwp:
            gwp_meta.setdefault("source", "missing")
            gwp_meta.setdefault("gwp100", None)
            note = "Atmospheric GWP missing (not in ATMO/CSV tables; default_zero disabled)"
            if note not in notes:
                notes.append(note)
        if not has_odp:
            odp_meta.setdefault("source", "missing")
            odp_meta.setdefault("odp", None)
            note2 = "Atmospheric ODP missing (not in ODP CSV/list; default_zero disabled)"
            if note2 not in notes:
                notes.append(note2)

    out["gwp_meta"] = gwp_meta
    out["odp_meta"] = odp_meta
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
# We only claim the structural presence/absence of S/N. Presence →
# "may form SOx or NOx upon combustion" — honest heuristic, not measured
# acid-rain potential. Absence → "Does not contain S or N".
#
# Structure source precedence (keep current):
#   1) PubChem molecular formula element symbols + atom counts (primary)
#      — N/S only; never Ni, Si, Na, Sn, …
#   2) HSPiP Y-MBSX row Formula / N# / S# when already computed (optional)
#   3) SMILES via RDKit only when formula is missing (fallback)
#   4) HSPiP sofx SMILES as last gap-fill when PubChem structure is missing
# Never treat sofx boolean field "S" (TRUE/FALSE) as sulfur count.
# Hansen D/P/H/RER and predicted VP stay HSPiP (Teams-licensed install).

_ACID_RAIN_NO_SN = "Does not contain S or N"
_ACID_RAIN_MAY_FORM = "Product may form SOx or NOx upon combustion"


def resolve_acid_rain_structure_inputs(
    *,
    pubchem: dict[str, Any] | None = None,
    formula: str | None = None,
    smiles: str | None = None,
    hspip_ymb_row: dict[str, Any] | None = None,
    sofx_smiles: str | None = None,
) -> dict[str, Any]:
    """
    Build formula / smiles / hspip_row for Acid Rain Formation.

    Prefers PubChem; HSPiP Y-MBSX N#/S#/Formula is optional enrichment;
    sofx SMILES is gap-fill only.
    """
    pc = pubchem or {}
    formula_out = (
        formula
        or pc.get("molecular_formula")
        or pc.get("formula")
        or None
    )
    if formula_out is not None:
        formula_out = str(formula_out).strip() or None

    smiles_out = smiles or pc.get("smiles") or None
    if smiles_out is not None:
        smiles_out = str(smiles_out).strip() or None

    structure_sources: list[str] = []
    if formula_out:
        structure_sources.append("pubchem_formula" if (pc.get("formula") or pc.get("molecular_formula")) else "formula")
    if smiles_out and (pc.get("smiles") and str(pc.get("smiles")).strip() == smiles_out):
        structure_sources.append("pubchem_smiles")
    elif smiles_out and smiles:
        structure_sources.append("smiles")

    hspip_row: dict[str, Any] | None = None
    if hspip_ymb_row:
        # Only numeric element counts + Formula from Y-MBSX Out.dat — never sofx "S".
        row: dict[str, Any] = {}
        for k in ("N#", "S#", "Formula"):
            if hspip_ymb_row.get(k) is not None:
                row[k] = hspip_ymb_row.get(k)
        # Y-MBSX parse may leave counts under N/S — copy only if numeric.
        for k_src, k_dst in (("N", "N#"), ("S", "S#")):
            if k_dst not in row and hspip_ymb_row.get(k_src) is not None:
                try:
                    row[k_dst] = float(hspip_ymb_row[k_src])
                except (TypeError, ValueError):
                    pass
        if row:
            hspip_row = row
            structure_sources.append("hspip_ymb")
            if not formula_out and row.get("Formula"):
                formula_out = str(row["Formula"]).strip() or None

    if not smiles_out and sofx_smiles:
        smiles_out = str(sofx_smiles).strip() or None
        if smiles_out:
            structure_sources.append("hspip_sofx_smiles")

    return {
        "formula": formula_out,
        "smiles": smiles_out,
        "hspip_row": hspip_row,
        "structure_sources": structure_sources,
    }


def _formula_element_counts(formula: str) -> dict[str, float]:
    """
    Parse a molecular formula into element → atom-count (Hill tokens).

    Uses element *symbols* and their stoichiometric counts (e.g. N₂ → N:2),
    not atomic numbers. Two-letter symbols (Ni, Si, Na, Sn, …) are kept
    distinct from N / S so they do not false-trigger acid-rain.
    """
    f = str(formula or "").strip()
    if not f:
        return {}
    # Strip charge / hydrate suffixes commonly seen in PubChem formulas.
    f = re.split(r"[·•.]", f, maxsplit=1)[0]
    f = re.sub(r"[\[\]()]", "", f)
    f = re.sub(r"[+\-]\d*$", "", f)
    out: dict[str, float] = {}
    for m in re.finditer(r"([A-Z][a-z]?)(\d*)", f):
        el = m.group(1)
        raw = m.group(2)
        n = float(raw) if raw else 1.0
        out[el] = out.get(el, 0.0) + n
    return out


def molecule_has_s_or_n(
    *,
    smiles: str | None = None,
    formula: str | None = None,
    element_counts: dict[str, Any] | None = None,
    hspip_row: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """
    Detect sulfur and/or nitrogen for Acid Rain combustion heuristic.

    Prefer molecular-formula element symbols + atom counts (PubChem formula).
    SMILES/RDKit is a fallback when formula is missing — not a substitute for
    reading N/S out of the formula. Never confuse Ni/Si/Na/Sn with N or S.

    Returns (has_s_or_n, detail) where detail includes has_s, has_n, evidence.
    ``hspip_row`` may supply ``N#`` / ``S#`` / ``Formula`` from Y-MBSX only.
    """
    has_s = False
    has_n = False
    evidence: list[str] = []

    counts = dict(element_counts or {})
    if hspip_row:
        # Prefer S#/N# only — bare "S" collides with HSPiP sofx boolean flags.
        for k_src, k_dst in (("S#", "S"), ("N#", "N")):
            if hspip_row.get(k_src) is not None and k_dst not in counts:
                try:
                    counts[k_dst] = float(hspip_row[k_src])
                except (TypeError, ValueError):
                    pass
        if hspip_row.get("Formula") and not formula:
            formula = str(hspip_row.get("Formula"))

    if formula:
        parsed = _formula_element_counts(str(formula))
        for el in ("S", "N"):
            if el not in counts and parsed.get(el, 0) > 0:
                counts[el] = parsed[el]

    for el in ("S", "N"):
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
            evidence.append(f"formula_atoms:{el}={n:g}")

    # SMILES/RDKit only when no usable formula — formula atom counts are authoritative.
    formula_ok = bool(formula and _formula_element_counts(str(formula)))
    if smiles and not formula_ok:
        s = str(smiles)
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
                evidence.append("smiles:rdkit_fallback")
        except Exception:
            counted = False
        if not counted:
            # Last resort: only lone S/N tokens (not Si/Sn/Ni/Na).
            if re.search(r"(?<![A-Z])S(?![a-z])", s) or "[S" in s:
                has_s = True
                evidence.append("smiles:regex:S")
            if re.search(r"(?<![A-Z])N(?![a-z])", s) or "[N" in s:
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
    structure_sources: list[str] | None = None,
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
        "structure_sources": list(structure_sources or []),
        "honesty": (
            "Structural heuristic for combustion SOx/NOx from formula element "
            "symbols N/S (atom counts; Ni/Si/Na/Sn ignored). PubChem formula "
            "primary; RDKit SMILES only if formula missing; HSPiP Y-MBSX N#/S# "
            "optional. Not measured acid-rain potential."
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
    pubchem: dict[str, Any] | None = None,
    hspip_ymb_row: dict[str, Any] | None = None,
    sofx_smiles: str | None = None,
    structure_sources: list[str] | None = None,
) -> dict[str, Any]:
    """
    Feed Atmospheric Acid Rain Formation key phrase into other_designations.

    Prefer PubChem formula/SMILES via ``pubchem=``; HSPiP Y-MBSX / sofx are optional.
    Safe to call with empty structure: leaves extras unchanged when no cues.
    """
    out: dict[str, Any] = dict(extra_sources) if extra_sources else {}
    resolved = resolve_acid_rain_structure_inputs(
        pubchem=pubchem,
        formula=formula,
        smiles=smiles,
        hspip_ymb_row=hspip_ymb_row or hspip_row,
        sofx_smiles=sofx_smiles,
    )
    formula = resolved["formula"]
    smiles = resolved["smiles"]
    hspip_row = resolved["hspip_row"]
    sources = list(structure_sources or []) + list(resolved.get("structure_sources") or [])
    deduped: list[str] = []
    for s in sources:
        if s and s not in deduped:
            deduped.append(s)
    sources = deduped

    if not any([smiles, formula, element_counts, hspip_row]):
        return out

    info = acid_rain_phrase_for_structure(
        smiles=smiles,
        formula=formula,
        element_counts=element_counts,
        hspip_row=hspip_row,
        structure_sources=sources,
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
    src_note = ",".join(sources) if sources else "structure"
    note = (
        f"Atmospheric Acid Rain Formation: '{info['phrase']}' "
        f"({info['honesty']} support={info['support']}; via={src_note})"
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
