"""
P2OASys Physical Properties → pH (band-level heuristic).

Cascade (stop at first usable value)
------------------------------------
1. PubChem / HSDB experimental pH of a 1% (or dilute aqueous) solution
2. Experimental pKa, else OPERA pKa_a / pKa_b → compute pH of a 1% w/v solution
3. LAST RESORT: RDKit SMARTS functional-group pKa table → same 1% formula

Chemistry
---------
* 1% w/v → C = 10 / MW mol/L
* Monoprotic charge-balance; strong electrolytes ≈ fully dissociated
* Amphoteric → pH farther from 7 (conservative)
* Ignore alcohols / amides / carbon acids; exclude esters-as-acids and amides-as-amines
* Success criterion = correct P2OASys band, not high-precision pKa

Do not use Excel numeric thresholds for pH (U-shaped hazard).
"""

from __future__ import annotations

import math
import re
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Bands
# ---------------------------------------------------------------------------


def score_ph_units(ph: float) -> tuple[int, str]:
    """Map pH → (score, Key Phrase). Exact boundaries → lower (less hazardous) score."""
    try:
        x = float(ph)
    except (TypeError, ValueError):
        return 2, "Neutral/Not Applicable"
    if x < 2:
        return 10, "Highly acidic"
    if x < 3:
        return 8, "Strong acid"
    if x < 4:
        return 6, "Acidic"
    if x < 6.5:
        return 4, "Mildly acidic"
    if x <= 7.5:
        return 2, "Neutral/Not Applicable"
    if x <= 10:
        return 4, "Mildly alkaline"
    if x <= 11.5:
        return 6, "Alkaline"
    if x <= 12:
        return 8, "Caustic"
    return 10, "Highly caustic"


# ---------------------------------------------------------------------------
# 1% monoprotic math
# ---------------------------------------------------------------------------

_KW = 1.0e-14


def _h_from_ka_c(ka: float, c: float) -> float:
    ka = max(float(ka), 1e-30)
    c = max(float(c), 0.0)
    if ka >= 0.1 or (c > 0 and ka / max(c, 1e-30) > 50):
        h = min(c, 1.0) if c > 0 else math.sqrt(_KW)
    else:
        disc = ka * ka + 4.0 * ka * c
        h = 0.5 * (-ka + math.sqrt(max(disc, 0.0)))
        h = max(h, 0.0)
    if h < 1e-6:
        disc_w = ka * ka + 4.0 * (ka * c + _KW)
        h = 0.5 * (-ka + math.sqrt(max(disc_w, 0.0)))
    return max(min(h, 1.0), 1e-15)


def ph_from_acid_pka(pka: float, conc_m: float) -> float:
    h = _h_from_ka_c(10.0 ** (-float(pka)), conc_m)
    return max(0.0, min(14.0, -math.log10(h)))


def ph_from_base_pka_bh(pka_bh: float, conc_m: float) -> float:
    kb = 10.0 ** (-(14.0 - float(pka_bh)))
    oh = _h_from_ka_c(kb, conc_m)
    return max(0.0, min(14.0, 14.0 - (-math.log10(max(oh, 1e-15)))))


def _farther_from_seven(ph_a: float, ph_b: float) -> float:
    return ph_a if abs(ph_a - 7.0) >= abs(ph_b - 7.0) else ph_b


def _f(x: Any) -> Optional[float]:
    try:
        if x is None or x == "" or str(x).lower() in ("nan", "na", "nd", "none"):
            return None
        return float(str(x).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Inorganic fallbacks
# ---------------------------------------------------------------------------

_INORGANIC: dict[str, dict[str, Any]] = {
    "HCL": {"role": "strong_acid", "pka": -6.3},
    "HBR": {"role": "strong_acid", "pka": -9.0},
    "HI": {"role": "strong_acid", "pka": -10.0},
    "HNO3": {"role": "strong_acid", "pka": -1.4},
    "H2SO4": {"role": "strong_acid", "pka": -3.0},
    "H3PO4": {"role": "acid", "pka": 2.1},
    "NAOH": {"role": "strong_base", "pka_bh": 15.7},
    "KOH": {"role": "strong_base", "pka_bh": 15.7},
    "CA(OH)2": {"role": "strong_base", "pka_bh": 15.7},
    "NH3": {"role": "base", "pka_bh": 9.25},
    "NH4OH": {"role": "base", "pka_bh": 9.25},
}


def _norm_formula(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").upper())


def _inorganic_match(formula: str | None, smiles: str | None) -> dict[str, Any] | None:
    keys: list[str] = []
    if formula:
        keys.append(_norm_formula(formula))
    if smiles:
        s = re.sub(r"\s+", "", str(smiles).upper())
        keys.append(s)
        keys.append(re.sub(r"[\[\]]", "", s))
    for k in keys:
        if k in _INORGANIC:
            return dict(_INORGANIC[k], key=k)
    return None


# ---------------------------------------------------------------------------
# SMARTS table (Priority 3 only)
# ---------------------------------------------------------------------------

_ACID_SMARTS: list[tuple[str, str, float]] = [
    ("sulfonic_acid", "[SX4](=O)(=O)([OH,O-])", -2.0),
    ("phosphonic_phosphoric", "[PX4](=O)([OH,O-])", 2.0),
    ("tfa_like", "C(F)(F)(F)[CX3](=O)[OX2H1]", 0.5),
    ("carboxylic_acid", "[CX3](=O)[OX2H1]", 4.5),  # excludes esters (needs OX2H1)
    ("tetrazole", "c1nnn[nH]1", 5.0),
    ("phenol", "[OX2H][c]", 10.0),
    ("thiol", "[SX2H]", 10.0),
    ("sulfonamide_nh", "[SX4](=O)(=O)[NX3H1,NX3H2]", 10.0),
]

_BASE_SMARTS: list[tuple[str, str, float]] = [
    ("guanidine", "[NX3H2][CX3](=[NX2])[NX3H2]", 13.0),
    ("amidine", "[NX3][CX3]=[NX2]", 13.0),
    ("aliphatic_amine", "[NX3;H2,H1,H0;!$(NC=O);!$(n);!$(N~c)]", 10.0),
    ("imidazole", "c1ncc[nH]1", 7.0),
    ("pyridine", "n1ccccc1", 5.0),
    ("aniline", "[NX3;H2,H1;$(Nc)]", 4.6),
]


def _mol_from_smiles(smiles: str):
    try:
        from rdkit import Chem

        return Chem.MolFromSmiles(str(smiles).strip())
    except Exception:
        return None


def _mw(mol) -> Optional[float]:
    try:
        from rdkit.Chem import Descriptors

        return float(Descriptors.MolWt(mol))
    except Exception:
        return None


def detect_groups_from_smiles(smiles: str) -> dict[str, Any]:
    mol = _mol_from_smiles(smiles)
    if mol is None:
        return {"acids": [], "bases": [], "mw": None, "ok": False}
    acids: list[dict[str, Any]] = []
    bases: list[dict[str, Any]] = []
    try:
        from rdkit import Chem

        for name, smarts, pka in _ACID_SMARTS:
            pat = Chem.MolFromSmarts(smarts)
            if pat is not None and mol.HasSubstructMatch(pat):
                acids.append({"name": name, "pka": pka, "smarts": smarts})
        for name, smarts, pka_bh in _BASE_SMARTS:
            pat = Chem.MolFromSmarts(smarts)
            if pat is not None and mol.HasSubstructMatch(pat):
                bases.append({"name": name, "pka_bh": pka_bh, "smarts": smarts})
    except Exception:
        return {"acids": [], "bases": [], "mw": _mw(mol), "ok": False}
    return {"acids": acids, "bases": bases, "mw": _mw(mol), "ok": True}


def ph_from_groups(
    *,
    acids: list[dict[str, Any]],
    bases: list[dict[str, Any]],
    mw: float | None,
    inorganic: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if mw is None or mw <= 0:
        mw = 100.0
    conc = 10.0 / float(mw)

    if inorganic:
        role = inorganic.get("role")
        if role == "strong_acid":
            ph = min(ph_from_acid_pka(float(inorganic["pka"]), conc), -math.log10(min(conc, 1.0)))
            sc, phrase = score_ph_units(ph)
            return {
                "ph": ph, "ph_est": ph, "score": sc, "phrase": phrase,
                "conc_M": conc, "mw": mw, "pka_used": {"acid": inorganic.get("pka")},
                "groups": {"inorganic": inorganic},
            }
        if role in ("strong_base", "base"):
            ph = ph_from_base_pka_bh(float(inorganic["pka_bh"]), conc)
            if role == "strong_base":
                ph = max(ph, 14.0 + math.log10(min(conc, 1.0)))
            sc, phrase = score_ph_units(ph)
            return {
                "ph": ph, "ph_est": ph, "score": sc, "phrase": phrase,
                "conc_M": conc, "mw": mw, "pka_used": {"base_bh": inorganic.get("pka_bh")},
                "groups": {"inorganic": inorganic},
            }
        if role == "acid":
            ph = ph_from_acid_pka(float(inorganic["pka"]), conc)
            sc, phrase = score_ph_units(ph)
            return {
                "ph": ph, "ph_est": ph, "score": sc, "phrase": phrase,
                "conc_M": conc, "mw": mw, "pka_used": {"acid": inorganic.get("pka")},
                "groups": {"inorganic": inorganic},
            }

    if not acids and not bases:
        sc, phrase = score_ph_units(7.0)
        return {
            "ph": 7.0, "ph_est": 7.0, "score": sc, "phrase": phrase,
            "conc_M": conc, "mw": mw, "pka_used": {},
            "groups": {"acids": [], "bases": []}, "neutral_default": True,
        }

    ph_acid = ph_base = None
    pka_used: dict[str, Any] = {}
    if acids:
        strongest = min(acids, key=lambda g: float(g["pka"]))
        pka_used["acid"] = strongest["pka"]
        pka_used["acid_group"] = strongest.get("name")
        ph_acid = ph_from_acid_pka(float(strongest["pka"]), conc)
    if bases:
        strongest_b = max(bases, key=lambda g: float(g["pka_bh"]))
        pka_used["base_bh"] = strongest_b["pka_bh"]
        pka_used["base_group"] = strongest_b.get("name")
        ph_base = ph_from_base_pka_bh(float(strongest_b["pka_bh"]), conc)

    if ph_acid is not None and ph_base is not None:
        ph = _farther_from_seven(ph_acid, ph_base)
    elif ph_acid is not None:
        ph = ph_acid
    else:
        ph = ph_base if ph_base is not None else 7.0

    sc, phrase = score_ph_units(ph)
    return {
        "ph": ph, "ph_est": ph, "score": sc, "phrase": phrase,
        "conc_M": conc, "mw": mw, "pka_used": pka_used,
        "groups": {"acids": acids, "bases": bases},
    }


def estimate_ph_fg_smarts(
    smiles: str,
    *,
    formula: str | None = None,
    mw: float | None = None,
) -> dict[str, Any] | None:
    inorganic = _inorganic_match(formula, smiles)
    groups = detect_groups_from_smiles(smiles) if smiles else {"acids": [], "bases": [], "mw": None, "ok": False}
    if not groups.get("ok") and not inorganic:
        if not inorganic:
            return None
        out = ph_from_groups(acids=[], bases=[], mw=mw or 36.5, inorganic=inorganic)
        if out:
            out["source"] = "fg_pka_1pct_inorganic"
            out["smiles"] = smiles
        return out
    out = ph_from_groups(
        acids=list(groups.get("acids") or []),
        bases=list(groups.get("bases") or []),
        mw=mw or groups.get("mw"),
        inorganic=inorganic,
    )
    if not out:
        return None
    out["source"] = "fg_pka_1pct"
    out["smiles"] = smiles
    return out


# ---------------------------------------------------------------------------
# Text / OPERA extractors
# ---------------------------------------------------------------------------

_ONE_PCT_PH_RE = re.compile(
    r"(?is)"
    r"(?:"
    r"pH\s*(?:of\s+)?(?:a\s+)?1\s*%[^\n|;]{0,60}?(?:aq(?:ueous)?\.?\s*)?(?:sol(?:ution|n)\.?)?"
    r"|"
    r"1\s*%\s*(?:w/v\s+|wt\s+)?(?:aq(?:ueous)?\.?\s*)?(?:sol(?:ution|n)\.?)[^\n|;]{0,40}?pH"
    r"|"
    r"pH\s*(?:of\s+)?(?:a\s+)?(?:dilute|diluted)\s+(?:aq(?:ueous)?\s+)?(?:sol(?:ution|n)\.?)"
    r")"
    r"[^\d\-<>~≈]{0,25}"
    r"([<>~≈]?\s*\d+(?:\.\d+)?(?:\s*[-–to]+\s*\d+(?:\.\d+)?)?)",
)
_GENERIC_PH_RE = re.compile(
    r"(?i)\bpH\b[^0-9\-<>~≈]{0,20}([<>~≈]?\s*\d+(?:\.\d+)?(?:\s*[-–to]+\s*\d+(?:\.\d+)?)?)"
)
_PRODUCT_PH_SKIP = re.compile(
    r"(?i)\b(buffer|tablet|shampoo|soap|detergent|cosmetic|formulation|product pH)\b"
)
_PKA_EXP_RE = re.compile(
    r"(?i)\b(?:pKa|pK_a|dissociation\s+constant(?:\s+pKa)?)\b"
    r"[^\d\-<>~≈]{0,30}"
    r"([<>~≈]?\s*-?\d+(?:\.\d+)?)"
)


def _parse_ph_number_token(raw: str) -> float | None:
    nums = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", raw or "") if 0 <= float(n) <= 14]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _collect_text_blobs(hazard_data: dict[str, Any]) -> list[str]:
    hd = hazard_data or {}
    chunks: list[str] = []
    hm = hd.get("hazard_metrics") or {}
    for key in ("other_designations", "physical_properties", "experimental_properties", "ph", "pH", "hsdb"):
        for item in hm.get(key) or []:
            chunks.append(str(item))
    for t in hd.get("toxicities") or []:
        chunks.append(str(t.get("value") or ""))
    for key in (
        "physical_description", "ph", "pH", "experimental_properties",
        "experimental_ph", "hsdb_ph", "pugview_experimental",
    ):
        v = hd.get(key)
        if isinstance(v, list):
            chunks.extend(str(x) for x in v)
        elif v:
            chunks.append(str(v))
    pub = hd.get("pubchem") if isinstance(hd.get("pubchem"), dict) else {}
    for key in ("experimental_properties", "other_designations", "physical_description"):
        v = pub.get(key)
        if isinstance(v, list):
            chunks.extend(str(x) for x in v)
        elif v:
            chunks.append(str(v))
    return [c for c in chunks if c and str(c).strip()]


def extract_pubchem_experimental_ph(hazard_data: dict[str, Any] | None) -> dict[str, Any] | None:
    """Priority 1: measured 1% / dilute aqueous pH from PubChem/HSDB-style text."""
    chunks = _collect_text_blobs(hazard_data or {})
    one_pct: list[tuple[float, str]] = []
    generic: list[tuple[float, str]] = []
    for chunk in chunks:
        if _PRODUCT_PH_SKIP.search(chunk) and not re.search(r"(?i)1\s*%", chunk):
            continue
        for m in _ONE_PCT_PH_RE.finditer(chunk):
            ph_v = _parse_ph_number_token(m.group(1))
            if ph_v is not None:
                one_pct.append((ph_v, m.group(0).strip()[:160]))
        if one_pct:
            continue
        if not re.search(r"(?i)\b(experimental|HSDB|solution|aqueous|aq\.?\s*sol|1\s*%)\b|\bpH\b", chunk):
            continue
        for m in _GENERIC_PH_RE.finditer(chunk):
            ctx = chunk[max(0, m.start() - 20): m.end() + 20]
            if re.search(r"(?i)pH\s*units|score|matrix", ctx):
                continue
            ph_v = _parse_ph_number_token(m.group(1))
            if ph_v is not None:
                generic.append((ph_v, m.group(0).strip()[:160]))

    if one_pct:
        ph_v, raw = one_pct[0]
        source = "pubchem_experimental_1pct_ph"
    elif generic:
        ph_v, raw = generic[0]
        source = "pubchem_experimental_ph"
    else:
        return None
    sc, phrase = score_ph_units(ph_v)
    return {
        "ph": ph_v, "ph_est": ph_v, "score": sc, "phrase": phrase,
        "source": source, "raw": raw, "pka_used": {}, "groups": {},
        "smiles": (hazard_data or {}).get("smiles"),
    }


def extract_experimental_pka(hazard_data: dict[str, Any] | None) -> dict[str, Any] | None:
    hd = hazard_data or {}
    hm = hd.get("hazard_metrics") or {}
    if hm.get("experimental_pka"):
        meta = hm["experimental_pka"]
        if isinstance(meta, list) and meta:
            meta = meta[0]
        if isinstance(meta, dict):
            pka_a = _f(meta.get("pka_a") or meta.get("pka"))
            pka_b = _f(meta.get("pka_b"))
            if pka_a is not None or pka_b is not None:
                return {"pka_a": pka_a, "pka_b": pka_b, "row_source": "hazard_metrics.experimental_pka"}

    acids: list[float] = []
    bases: list[float] = []
    for chunk in _collect_text_blobs(hd):
        for m in _PKA_EXP_RE.finditer(chunk):
            val = _f(re.sub(r"[<>~≈]", "", m.group(1)))
            if val is None or not (-10 <= val <= 20):
                continue
            ctx = chunk[max(0, m.start() - 40): m.end() + 40].lower()
            if "conjugat" in ctx or "bh+" in ctx or "pka_b" in ctx or re.search(r"\bbase\b", ctx):
                bases.append(val)
            else:
                acids.append(val)
    if not acids and not bases:
        return None
    return {
        "pka_a": min(acids) if acids else None,
        "pka_b": max(bases) if bases else None,
        "row_source": "pubchem_experimental_pka_text",
    }


def extract_opera_pka_pair(hazard_data: dict[str, Any] | None) -> dict[str, Any] | None:
    hd = hazard_data or {}
    row: dict[str, Any] = {}
    src = ""

    pka_block = hd.get("opera_pka")
    if isinstance(pka_block, dict) and (pka_block.get("pka_a") is not None or pka_block.get("pka_b") is not None):
        return {
            "pka_a": _f(pka_block.get("pka_a")),
            "pka_b": _f(pka_block.get("pka_b")),
            "ad_pka": pka_block.get("ad_pka"),
            "conf_index_pka": pka_block.get("conf_index_pka"),
            "row_source": "hazard_data.opera_pka",
        }

    for key in ("opera_row", "opera", "opera_panel"):
        blob = hd.get(key)
        if isinstance(blob, dict):
            if blob.get("row"):
                row = dict(blob["row"])
                src = key
                break
            if any(str(k).startswith("pKa") for k in blob):
                row = dict(blob)
                src = key
                break

    hm = hd.get("hazard_metrics") or {}
    if not row and hm.get("opera_pka"):
        meta = hm["opera_pka"]
        if isinstance(meta, list) and meta:
            meta = meta[0]
        if isinstance(meta, dict):
            return {
                "pka_a": _f(meta.get("pka_a")),
                "pka_b": _f(meta.get("pka_b")),
                "ad_pka": meta.get("ad_pka"),
                "conf_index_pka": meta.get("conf_index_pka"),
                "row_source": "hazard_metrics.opera_pka",
            }

    if not row:
        cas = str(hd.get("cas") or hd.get("CAS") or "").strip()
        smiles = str(hd.get("smiles") or "").strip()
        if cas:
            try:
                try:
                    from utils.opera_precompute_cache import default_precompute_db_path, get_cas_row
                except ImportError:
                    from opera_precompute_cache import default_precompute_db_path, get_cas_row  # type: ignore
                cached = get_cas_row(default_precompute_db_path(), cas, smiles)
                if cached and isinstance(cached.get("row"), dict):
                    row = cached["row"]
                    src = "opera_precompute"
                elif cached:
                    pka_a = _f(cached.get("pKa_a_pred"))
                    pka_b = _f(cached.get("pKa_b_pred"))
                    if pka_a is not None or pka_b is not None:
                        return {
                            "pka_a": pka_a, "pka_b": pka_b,
                            "ad_pka": cached.get("AD_pKa"),
                            "conf_index_pka": cached.get("Conf_index_pKa"),
                            "row_source": "opera_precompute",
                        }
            except Exception:
                pass

    if not row:
        return None
    pka_a = _f(row.get("pKa_a_pred") or row.get("pKa_a"))
    pka_b = _f(row.get("pKa_b_pred") or row.get("pKa_b"))
    if pka_a is None and pka_b is None:
        return None
    return {
        "pka_a": pka_a,
        "pka_b": pka_b,
        "ad_pka": row.get("AD_pKa"),
        "conf_index_pka": row.get("Conf_index_pKa"),
        "row_source": src or "opera_row",
    }


def _ph_from_pka_pair(
    pair: dict[str, Any],
    *,
    smiles: str | None,
    mw: float | None,
    hazard_data: dict[str, Any],
    source: str,
) -> dict[str, Any] | None:
    use_mw = mw
    if use_mw is None and smiles:
        mol = _mol_from_smiles(smiles)
        use_mw = _mw(mol) if mol is not None else None
    if use_mw is None:
        use_mw = _f(hazard_data.get("molecular_weight")) or 100.0
    acids = [{"name": "pKa_a", "pka": pair["pka_a"]}] if pair.get("pka_a") is not None else []
    bases = [{"name": "pKa_b", "pka_bh": pair["pka_b"]}] if pair.get("pka_b") is not None else []
    formula = hazard_data.get("formula") or hazard_data.get("molecular_formula")
    inorganic = _inorganic_match(str(formula) if formula else None, smiles)
    if not acids and not bases and not inorganic:
        return None
    out = ph_from_groups(
        acids=acids,
        bases=bases,
        mw=use_mw,
        inorganic=inorganic if not acids and not bases else None,
    )
    if not out:
        return None
    out["source"] = source
    out["smiles"] = smiles
    out["pka_pair"] = pair
    out["opera"] = pair if source.startswith("opera") else None
    return out


def estimate_ph_from_experimental_pka(
    hazard_data: dict[str, Any], *, smiles: str | None = None, mw: float | None = None
) -> dict[str, Any] | None:
    pair = extract_experimental_pka(hazard_data)
    if not pair:
        return None
    return _ph_from_pka_pair(
        pair, smiles=smiles, mw=mw, hazard_data=hazard_data, source="experimental_pka_1pct"
    )


def estimate_ph_from_opera_pka(
    hazard_data: dict[str, Any], *, smiles: str | None = None, mw: float | None = None
) -> dict[str, Any] | None:
    pair = extract_opera_pka_pair(hazard_data)
    if not pair:
        return None
    return _ph_from_pka_pair(
        pair, smiles=smiles, mw=mw, hazard_data=hazard_data, source="opera_pka_1pct"
    )


def resolve_smiles_for_ph(hazard_data: dict[str, Any] | None) -> str | None:
    hd = hazard_data or {}
    for key in ("smiles", "canonical_smiles", "connectivity_smiles"):
        s = str(hd.get(key) or "").strip()
        if s:
            return s
    pub = hd.get("pubchem") if isinstance(hd.get("pubchem"), dict) else {}
    for key in ("smiles", "canonical_smiles", "connectivity_smiles"):
        s = str(pub.get(key) or "").strip()
        if s:
            return s
    cid = hd.get("cid") or pub.get("cid")
    if not cid:
        return None
    for importer in (
        lambda: __import__("utils.opera_client", fromlist=["opera_client"]),
        lambda: __import__("opera_client"),
    ):
        try:
            oc = importer()
            if hasattr(oc, "get_smiles_from_cid"):
                s = oc.get_smiles_from_cid(int(cid))
                if s:
                    return str(s).strip()
        except Exception:
            pass
    try:
        import pubchempy as pcp

        c = pcp.Compound.from_cid(int(cid))
        s = getattr(c, "connectivity_smiles", None) or getattr(c, "canonical_smiles", None)
        if s:
            return str(s).strip()
    except Exception:
        pass
    return None


def estimate_ph_for_hazard(hazard_data: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Priority cascade:
      1) experimental 1% / dilute aq pH
      2) experimental pKa → 1% pH, else OPERA pKa → 1% pH
      3) SMARTS FG last resort
    """
    hd = hazard_data if isinstance(hazard_data, dict) else {}
    smiles = resolve_smiles_for_ph(hd)
    formula = (
        hd.get("formula")
        or hd.get("molecular_formula")
        or (hd.get("pubchem") or {}).get("formula")
        or (hd.get("pubchem") or {}).get("molecular_formula")
    )
    mw = _f(hd.get("molecular_weight"))

    def _store(out: dict[str, Any]) -> dict[str, Any]:
        out.setdefault("ph", out.get("ph_est"))
        hd["ph_estimate"] = out
        hd["ph_heuristic"] = out
        return out

    try:
        out = extract_pubchem_experimental_ph(hd)
        if out and out.get("score") is not None:
            return _store(out)
    except Exception:
        pass

    try:
        out = estimate_ph_from_experimental_pka(hd, smiles=smiles, mw=mw)
        if out and out.get("score") is not None:
            return _store(out)
    except Exception:
        pass

    try:
        out = estimate_ph_from_opera_pka(hd, smiles=smiles, mw=mw)
        if out and out.get("score") is not None:
            return _store(out)
    except Exception:
        pass

    if not smiles and not formula:
        return None
    try:
        out = estimate_ph_fg_smarts(smiles or "", formula=str(formula) if formula else None, mw=mw)
        if out:
            return _store(out)
    except Exception:
        return None
    return None


def apply_ph_rule(
    *,
    subcat: str,
    unit_name: str,
    ph_info: dict[str, Any] | None,
) -> tuple[Any, Any, bool]:
    """Special-case subcategory pH. Returns (score, input_value, handled)."""
    if not ph_info:
        return None, None, False
    if str(subcat or "").strip().lower() != "ph":
        return None, None, False
    u = str(unit_name or "").strip().lower()
    if u in ("ph units", "ph unit", "ph"):
        return ph_info.get("score"), ph_info.get("ph_est") if ph_info.get("ph_est") is not None else ph_info.get("ph"), True
    if "key phrase" in u or "phrase" in u:
        return ph_info.get("score"), ph_info.get("phrase"), True
    return None, None, True
