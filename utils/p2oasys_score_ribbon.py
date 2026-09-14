"""Streamlit helpers: P2OASys expert/auto score ribbons + optional TCI SDS enrich.

Color bins match the TURI flask legend:
  green  2 ≤ score < 4
  yellow 4 ≤ score < 6
  orange 6 ≤ score < 8
  red    8 ≤ score ≤ 10
  grey   missing / not scored
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import streamlit as st

from utils import p2oasys_score_lookup

logger = logging.getLogger(__name__)

def maybe_show_bin_legend_image() -> None:
    """Show TURI flask score-bin image when packaged under data/ or static/."""
    candidates = [
        Path(__file__).resolve().parent.parent / "static" / "p2oasys_score_bins.png",
        Path(__file__).resolve().parent.parent / "data" / "p2oasys_score_bins.png",
    ]
    for path in candidates:
        if path.is_file():
            st.image(str(path), width=320, caption="P2OASys score color bins")
            return



# Flask-legend colors (approx).
COLOR_GREEN = "#43a047"
COLOR_YELLOW = "#fdd835"
COLOR_ORANGE = "#fb8c00"
COLOR_RED = "#e53935"
COLOR_GREY = "#9e9e9e"
COLOR_TEXT_ON_LIGHT = "#212121"

COLOR_TEXT_ON_DARK = "#ffffff"

# ---------------------------------------------------------------------------
# Subcategory matrix ribbon (Option A/B)
# ---------------------------------------------------------------------------

# (matrix category key, short row label, auto-scorable)
MATRIX_ROWS: tuple[tuple[str, str, bool], ...] = (
    ("Acute Human Effects", "Acute", True),
    ("Chronic Human Effects", "Chronic", True),
    ("Ecological Hazards", "Ecological", True),
    ("Environmental Fate & Transport", "Fate", True),
    ("Atmospheric Hazard", "Atmospheric", True),
    ("Physical Properties", "Physical", True),
    ("Process Factors", "Process", False),
    ("Life Cycle Factors", "Life Cycle", False),
)

# Scorable subcategories only (skip duplicate category-header rows from the Excel parser).
# (full matrix subcategory name, acronym)
SUBCAT_DEFS: dict[str, tuple[tuple[str, str], ...]] = {
    "Acute Human Effects": (
        ("Inhalation Toxicity", "Inh"),
        ("Oral Toxicity", "Oral"),
        ("Dermal Toxicity", "Derm"),
        ("Respiratory Irritation", "RIr"),
        ("Dermal Irritation", "DIr"),
        ("Eye Irritation", "Eye"),
        ("Specific Target Organ Toxicity - STOT Single Exposure", "STOT"),
        ("Specific Target Organ Toxicity - STOT Other Organ Effects", "STOT2"),
        ("Exposure Limits", "OEL"),
        ("IDLH", "IDLH"),
        ("Health", "Hlth"),
    ),
    "Chronic Human Effects": (
        ("Carcinogen", "Carc"),
        ("Mutagen/ Teratogen", "Mut"),
        ("Reproductive/ Developmental", "Repro"),
        ("Neurotoxicity", "Neuro"),
        ("Respiratory Sensitivity/ Disease", "RSens"),
        ("Endocrine System Effects", "Endo"),
        ("Other Chronic Organ Effects", "Org"),
    ),
    "Ecological Hazards": (
        ("Acute Aquatic Toxicity", "AqA"),
        ("Chronic Aquatic Toxicity (fish, crustacea or algae)", "AqC"),
    ),
    "Environmental Fate & Transport": (
        ("Persistence", "Pers"),
        ("Rapid Degradability", "Deg"),
        ("Bioconcentration/ Bioaccumulation", "BCF"),
    ),
    "Atmospheric Hazard": (
        ("Greenhouse Gas", "GHG"),
        ("Ozone Depletor", "ODP"),
        ("Acid Rain Formation", "Acid"),
        ("NESHAP", "NESH"),
    ),
    "Physical Properties": (
        ("Vapor Pressure", "VP"),
        ("Flammability: Liquid", "FlamL"),
        ("Flammability: Gas", "FlamG"),
        ("Reactivity", "React"),
        ("Corrosivity to Materials", "Corr"),
        ("pH", "pH"),
        ("Odor", "Odor"),
        ("Volatile Organic Compound", "VOC"),
        ("Reportable Quantity", "RQ"),
    ),
    "Process Factors": (
        ("Heat", "Heat"),
        ("Cold", "Cold"),
        ("Noise Generation", "Noise"),
        ("Vibration", "Vib"),
        ("Ergonomic Hazard", "Ergo"),
        ("Psychosocial Hazard", "Psych"),
        ("High/Low Pressure System", "Press"),
        ("High/Low Temperature System", "Temp"),
        ("Water Use", "H2O"),
        ("Energy Use", "Ener"),
        ("Exposure Potential", "ExpP"),
    ),
    "Life Cycle Factors": (
        ("Upstream Effects", "Up"),
        ("Consumer Hazard", "Cons"),
        ("Disposal Hazard (landfill, incineration)", "Disp"),
        ("Recycling", "Recyc"),
        ("Renewable to Nonrenewable Resource", "Ren"),
    ),
}


# Alternate harvest / site-export subcategory labels → canonical SUBCAT_DEFS keys.
SUBCAT_NAME_ALIASES: dict[str, str] = {
    "High/Low Temperature - System": "High/Low Temperature System",
    "High/Low Temperature-System": "High/Low Temperature System",
    "Upstream Processing and Manufacturing": "Upstream Effects",
    "Upstream effects": "Upstream Effects",
    "Usage and Retail": "Consumer Hazard",
    "Consumer hazard": "Consumer Hazard",
    "End of life": "Disposal Hazard (landfill, incineration)",
    "End of Life": "Disposal Hazard (landfill, incineration)",
    "Disposal Hazard": "Disposal Hazard (landfill, incineration)",
    "Persistence in air, water and/or soil": "Persistence",
    "Persistence in Air, Water and/or Soil": "Persistence",
    "Persistence in air": "Persistence",
    "Persistence (air, water, soil)": "Persistence",
    "Bioconcentration / Bioaccumulation": "Bioconcentration/ Bioaccumulation",
    "Bioconcentration/Bioaccumulation": "Bioconcentration/ Bioaccumulation",
    "Mutagen/Teratogen": "Mutagen/ Teratogen",
    "Reproductive/Developmental": "Reproductive/ Developmental",
    "Respiratory Sensitivity / Disease": "Respiratory Sensitivity/ Disease",
    "Chronic Aquatic Toxicity": "Chronic Aquatic Toxicity (fish, crustacea or algae)",
    "Greenhouse Gases": "Greenhouse Gas",
    "GWP / Greenhouse Gas": "Greenhouse Gas",
}

_EXPERT_SUBCAT_CACHE: dict[str, Any] | None = None
_EXPERT_SUBCAT_CACHE_MTIME: float | None = None
_EXPERT_SUBCAT_CACHE_PATH: Path | None = None


def expert_subcat_cache_candidates() -> list[Path]:
    root = Path(__file__).resolve().parent.parent / "data"
    return [
        root / "expert_subcat_by_cas_priority62.json",
        root / "expert_subcat_by_cas.json",
    ]


def _cas_lookup_keys(cas: str | None) -> list[str]:
    raw = (cas or "").strip()
    if not raw:
        return []
    keys = [raw]
    try:
        from utils.lookup_tables import normalize_cas_for_lookup

        digits = normalize_cas_for_lookup(raw)
    except Exception:
        digits = "".join(c for c in raw if c.isdigit())
    if digits:
        keys.append(digits)
        if len(digits) >= 5:
            keys.append(f"{digits[:-3]}-{digits[-3:-1]}-{digits[-1]}")
        try:
            keys.append(p2oasys_score_lookup.format_cas_display(digits))
        except Exception:
            pass
    # unique preserve order
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _load_expert_subcat_cache() -> dict[str, Any]:
    """Load CAS → live_scores-shaped expert subcategory bundles (cached in-process)."""
    global _EXPERT_SUBCAT_CACHE, _EXPERT_SUBCAT_CACHE_MTIME, _EXPERT_SUBCAT_CACHE_PATH
    path: Path | None = None
    for cand in expert_subcat_cache_candidates():
        if cand.is_file():
            path = cand
            break
    if path is None:
        _EXPERT_SUBCAT_CACHE = {}
        _EXPERT_SUBCAT_CACHE_MTIME = None
        _EXPERT_SUBCAT_CACHE_PATH = None
        return _EXPERT_SUBCAT_CACHE
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = None
    if (
        _EXPERT_SUBCAT_CACHE is not None
        and _EXPERT_SUBCAT_CACHE_PATH == path
        and _EXPERT_SUBCAT_CACHE_MTIME == mtime
    ):
        return _EXPERT_SUBCAT_CACHE
    import json

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to load expert subcategory cache %s: %s", path, exc)
        data = {}
    if not isinstance(data, dict):
        data = {}
    # Index by hyphenated + digits-only for fast CAS lookup.
    indexed: dict[str, Any] = {}
    for cas_key, bundle in data.items():
        if not isinstance(bundle, dict):
            continue
        for k in _cas_lookup_keys(str(cas_key)):
            indexed[k] = bundle
            digits = "".join(c for c in k if c.isdigit())
            if digits:
                indexed[digits] = bundle
    _EXPERT_SUBCAT_CACHE = indexed
    _EXPERT_SUBCAT_CACHE_MTIME = mtime
    _EXPERT_SUBCAT_CACHE_PATH = path
    logger.info(
        "Loaded expert subcategory cache from %s (%d CAS keys indexed)",
        path,
        len(data),
    )
    return _EXPERT_SUBCAT_CACHE


def load_expert_subcat_scores(cas: str | None) -> dict[str, Any] | None:
    """Return live-scores-shaped expert subcategory dict for CAS, or None.

    Prefers ``p2oasys_score_lookup.sqlite`` (seeded from the harvest JSON).
    Falls back to the JSON cache if sqlite has no row for this CAS.
    """
    try:
        p2oasys_score_lookup.ensure_expert_subcat_seeded()
        bundle = p2oasys_score_lookup.load_subcat_bundle(cas, "expert")
        if isinstance(bundle, dict) and bundle:
            return normalize_expert_score_bundle(bundle)
    except Exception:
        logger.debug("Expert subcategory sqlite lookup failed", exc_info=True)
    cache = _load_expert_subcat_cache()
    if not cache:
        return None
    for k in _cas_lookup_keys(cas):
        bundle = cache.get(k)
        if isinstance(bundle, dict) and bundle:
            return normalize_expert_score_bundle(bundle)
        digits = "".join(c for c in k if c.isdigit())
        if digits and isinstance(cache.get(digits), dict) and cache[digits]:
            return normalize_expert_score_bundle(cache[digits])
    return None


def load_auto_subcat_scores(cas: str | None) -> dict[str, Any] | None:
    """Cached auto subcategory pills from a previous Generate draft."""
    try:
        bundle = p2oasys_score_lookup.load_subcat_bundle(cas, "auto")
        if isinstance(bundle, dict) and bundle:
            return normalize_expert_score_bundle(bundle)
    except Exception:
        logger.debug("Auto subcategory sqlite lookup failed", exc_info=True)
    return None


def normalize_expert_score_bundle(scores: dict[str, Any] | None) -> dict[str, Any]:
    """Remap subcategory keys to SUBCAT_DEFS canonical names; keep _category_max."""
    if not isinstance(scores, dict):
        return {}
    out: dict[str, Any] = {}
    for cat, bundle in scores.items():
        cat_key = str(cat).strip()
        if cat_key == "Environmental Fate and Transport":
            cat_key = "Environmental Fate & Transport"
        if not isinstance(bundle, dict):
            out[cat_key] = bundle
            continue
        new_b: dict[str, Any] = {}
        for sub, cell in bundle.items():
            if str(sub).startswith("_"):
                new_b[sub] = cell
                continue
            canon = SUBCAT_NAME_ALIASES.get(str(sub).strip(), str(sub).strip())
            # Prefer first-seen non-null; do not invent scores.
            if canon in new_b and isinstance(new_b[canon], dict) and isinstance(cell, dict):
                prev = new_b[canon].get("_max")
                cur = cell.get("_max")
                if prev is None and cur is not None:
                    new_b[canon] = cell
            else:
                new_b[canon] = cell
        out[cat_key] = new_b
    return out


# Expert/auto lookup column → matrix category
_EXPERT_COL_TO_CAT = {
    "expert_acute": "Acute Human Effects",
    "expert_chronic": "Chronic Human Effects",
    "expert_ecological": "Ecological Hazards",
    "expert_fate": "Environmental Fate & Transport",
    "expert_atmospheric": "Atmospheric Hazard",
    "expert_physical": "Physical Properties",
    "expert_process": "Process Factors",
    "expert_life_cycle": "Life Cycle Factors",
}
_AUTO_COL_TO_CAT = {
    "auto_acute": "Acute Human Effects",
    "auto_chronic": "Chronic Human Effects",
    "auto_ecological": "Ecological Hazards",
    "auto_fate": "Environmental Fate & Transport",
    "auto_atmospheric": "Atmospheric Hazard",
    "auto_physical": "Physical Properties",
}


def _esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# Eight slots shared by expert + auto ribbons (auto Process/LC stay empty/grey).
RIBBON_SLOTS_EXPERT = (
    ("expert_acute", "Acute"),
    ("expert_chronic", "Chronic"),
    ("expert_ecological", "Ecological"),
    ("expert_fate", "Fate & Transport"),
    ("expert_atmospheric", "Atmospheric"),
    ("expert_physical", "Physical"),
    ("expert_process", "Process"),
    ("expert_life_cycle", "Life Cycle"),
)

RIBBON_SLOTS_AUTO = (
    ("auto_acute", "Acute"),
    ("auto_chronic", "Chronic"),
    ("auto_ecological", "Ecological"),
    ("auto_fate", "Fate & Transport"),
    ("auto_atmospheric", "Atmospheric"),
    ("auto_physical", "Physical"),
    (None, "Process"),
    (None, "Life Cycle"),
)

_CATEGORY_TO_AUTO_COL = {
    "Acute Human Effects": "auto_acute",
    "Chronic Human Effects": "auto_chronic",
    "Ecological Hazards": "auto_ecological",
    "Environmental Fate & Transport": "auto_fate",
    "Environmental Fate and Transport": "auto_fate",
    "Atmospheric Hazard": "auto_atmospheric",
    "Physical Properties": "auto_physical",
}


def score_chip_color(score: float | None) -> str:
    """Return flask-bin background color for a P2OASys score."""
    if score is None:
        return COLOR_GREY
    try:
        s = float(score)
    except (TypeError, ValueError):
        return COLOR_GREY
    if s != s:  # NaN
        return COLOR_GREY
    if 2 <= s < 4:
        return COLOR_GREEN
    if 4 <= s < 6:
        return COLOR_YELLOW
    if 6 <= s < 8:
        return COLOR_ORANGE
    if 8 <= s <= 10:
        return COLOR_RED
    return COLOR_GREY


def _chip_text_color(bg: str) -> str:
    if bg in (COLOR_YELLOW,):
        return COLOR_TEXT_ON_LIGHT
    return COLOR_TEXT_ON_DARK


def format_p2oasys_score(score: float | None) -> str:
    """Format a P2OASys score with two significant digits (e.g. 6.0, 7.6, 10)."""
    if score is None:
        return "—"
    try:
        v = float(score)
    except (TypeError, ValueError):
        return "—"
    if v != v:  # NaN
        return "—"
    # Two significant digits on the 2–10 scale; keep one decimal for single-digit values.
    s = f"{v:.2g}"
    if abs(v) < 9.95 and "." not in s:
        s = f"{v:.1f}"
    return s


def chip_html(label: str, score: float | None, *, empty: bool = False) -> str:
    """Colored score box. empty=True forces grey with em-dash (Process/LC on auto)."""
    if empty:
        score = None
    bg = score_chip_color(score)
    fg = _chip_text_color(bg)
    if score is None:
        val = "—"
    else:
        try:
            val = format_p2oasys_score(float(score))
        except (TypeError, ValueError):
            val = "—"
            bg = COLOR_GREY
            fg = COLOR_TEXT_ON_DARK
    return (
        f'<span style="display:inline-block;margin:2px 4px;padding:6px 10px;'
        f'border-radius:8px;background:{bg};color:{fg};font-size:0.85rem;'
        f'font-weight:600;min-width:3.6rem;text-align:center;'
        f'white-space:nowrap;box-shadow:0 1px 2px rgba(0,0,0,.12);">'
        f'{label}<br/><span style="font-size:1.05rem;">{val}</span></span>'
    )



def ensure_compact_ribbon_css() -> None:
    """Tighten ribbon vertical footprint for landscape monitors."""
    import streamlit as st

    st.markdown(
        """<style id="p2o-ribbon-compact">
        .p2o-chip, .p2o-pill { margin: 1px 2px !important; padding: 1px 6px !important; font-size: 0.78rem !important; }
        .p2o-matrix-table { width: max-content !important; max-width: 100% !important; table-layout: auto !important; }
        .p2o-matrix-table td, .p2o-matrix-table th { padding: 2px 4px !important; line-height: 1.15 !important; }
        .p2o-matrix-table td:nth-child(1), .p2o-matrix-table th:nth-child(1),
        .p2o-matrix-table td:nth-child(3), .p2o-matrix-table th:nth-child(3) { width: 1% !important; white-space: nowrap !important; }
        .p2o-matrix-table td:nth-child(2) { width: auto !important; }
        .p2o-ribbon-row { margin: 0.15rem 0 !important; }
        </style>""",
        unsafe_allow_html=True,
    )

def color_legend_html() -> str:
    """Compact flask-bin legend."""
    items = [
        (COLOR_GREEN, COLOR_TEXT_ON_DARK, "2–4"),
        (COLOR_YELLOW, COLOR_TEXT_ON_LIGHT, "4–6"),
        (COLOR_ORANGE, COLOR_TEXT_ON_DARK, "6–8"),
        (COLOR_RED, COLOR_TEXT_ON_DARK, "8–10"),
        (COLOR_GREY, COLOR_TEXT_ON_DARK, "no score"),
    ]
    parts = []
    for bg, fg, lab in items:
        parts.append(
            f'<span style="display:inline-block;margin:0 6px 0 0;padding:2px 8px;'
            f'border-radius:6px;background:{bg};color:{fg};font-size:0.75rem;">{lab}</span>'
        )
    return (
        '<div style="margin:2px 0 8px 0;font-size:0.8rem;color:#555;">'
        "<b>Score bins</b> " + "".join(parts) + "</div>"
    )


def _chips_from_slots(row: dict[str, Any], slots: tuple, *, force_empty_none_keys: bool = True) -> str:
    chips = []
    for key, lab in slots:
        if key is None:
            chips.append(chip_html(lab, None, empty=True))
        else:
            chips.append(chip_html(lab, row.get(key)))
    return "".join(chips)


def scores_dict_to_auto_row(scores: dict[str, Any] | None) -> dict[str, Any]:
    """Map live scorer category bundles into auto_* fields for ribbon display."""
    out: dict[str, Any] = {
        "auto_acute": None,
        "auto_chronic": None,
        "auto_ecological": None,
        "auto_fate": None,
        "auto_atmospheric": None,
        "auto_physical": None,
        "has_auto": 0,
    }
    if not isinstance(scores, dict):
        return out
    for cat, bundle in scores.items():
        col = _CATEGORY_TO_AUTO_COL.get(str(cat).strip())
        if not col:
            continue
        val = None
        if isinstance(bundle, dict):
            raw = bundle.get("_category_max")
            if isinstance(raw, (int, float)) and raw == raw:
                val = float(raw)
        elif isinstance(bundle, (int, float)) and bundle == bundle:
            val = float(bundle)
        out[col] = val
    out["has_auto"] = int(any(out[c] is not None for c in (
        "auto_acute", "auto_chronic", "auto_ecological",
        "auto_fate", "auto_atmospheric", "auto_physical",
    )))
    try:
        from utils import p2oasys_aggregate

        ov = p2oasys_aggregate.aggregate_category_scores(scores, "max")
        if isinstance(ov, (int, float)) and ov == ov:
            out["auto_overall"] = float(ov)
    except Exception:
        nums = [out[c] for c in (
            "auto_acute", "auto_chronic", "auto_ecological",
            "auto_fate", "auto_atmospheric", "auto_physical",
        ) if isinstance(out.get(c), (int, float))]
        out["auto_overall"] = max(nums) if nums else None
    return out



def subcat_chip_html(
    acronym: str,
    full_name: str,
    score: float | None,
    *,
    empty: bool = False,
) -> str:
    """Compact single-line chip: acronym + score; hover = full name."""
    if empty:
        score = None
    if score is None:
        title = _esc(f"{full_name} — no score")
        return (
            f'<span title="{title}" style="display:inline-flex;align-items:center;gap:4px;'
            f'margin:1px 2px;padding:2px 7px;border-radius:999px;background:#f3f4f6;'
            f'color:#9ca3af;border:1px dashed #d1d5db;font-size:0.7rem;font-weight:600;'
            f'white-space:nowrap;cursor:default;line-height:1.3;">'
            f'{_esc(acronym)}<span style="opacity:.7;">—</span></span>'
        )
    bg = score_chip_color(score)
    fg = _chip_text_color(bg)
    try:
        val = format_p2oasys_score(float(score))
    except (TypeError, ValueError):
        val = "—"
        bg = COLOR_GREY
        fg = COLOR_TEXT_ON_DARK
    title = _esc(f"{full_name} — score {val}")
    return (
        f'<span title="{title}" style="display:inline-flex;align-items:center;gap:4px;'
        f'margin:1px 2px;padding:2px 7px;border-radius:999px;background:{bg};color:{fg};'
        f'font-size:0.7rem;font-weight:700;white-space:nowrap;cursor:default;'
        f'line-height:1.3;box-shadow:0 1px 1px rgba(0,0,0,.10);">'
        f'{_esc(acronym)}<span style="font-weight:600;opacity:.95;">{val}</span></span>'
    )


def _subcat_score_from_bundle(bundle: Any, subcat: str) -> float | None:
    if not isinstance(bundle, dict):
        return None

    def _cell_score(cell: Any) -> float | None:
        if isinstance(cell, dict):
            raw = cell.get("_max")
            if isinstance(raw, (int, float)) and raw == raw:
                return float(raw)
            return None
        if isinstance(cell, (int, float)) and cell == cell:
            return float(cell)
        return None

    # Direct canonical key
    sc = _cell_score(bundle.get(subcat))
    if sc is not None:
        return sc
    # Aliases that map TO this canonical subcategory
    for alt, canon in SUBCAT_NAME_ALIASES.items():
        if canon == subcat:
            sc = _cell_score(bundle.get(alt))
            if sc is not None:
                return sc
    # Case-insensitive fallback
    target = subcat.strip().lower()
    for key, cell in bundle.items():
        if str(key).startswith("_"):
            continue
        if str(key).strip().lower() == target:
            return _cell_score(cell)
        if SUBCAT_NAME_ALIASES.get(str(key).strip(), "").lower() == target:
            return _cell_score(cell)
    return None


def _category_max_from_bundle(bundle: Any) -> float | None:
    if isinstance(bundle, dict):
        raw = bundle.get("_category_max")
        if isinstance(raw, (int, float)) and raw == raw:
            return float(raw)
        return None
    if isinstance(bundle, (int, float)) and bundle == bundle:
        return float(bundle)
    return None


def category_scores_from_lookup_row(
    row: dict[str, Any] | None,
    *,
    prefix: str = "expert",
) -> dict[str, float | None]:
    """Map lookup DB columns → matrix category → category score (no subcats)."""
    row = row or {}
    mapping = _EXPERT_COL_TO_CAT if prefix == "expert" else _AUTO_COL_TO_CAT
    out: dict[str, float | None] = {}
    for col, cat in mapping.items():
        raw = row.get(col)
        if isinstance(raw, (int, float)) and raw == raw:
            out[cat] = float(raw)
        else:
            out[cat] = None
    if prefix == "expert":
        for col in ("expert_process", "expert_life_cycle"):
            cat = _EXPERT_COL_TO_CAT[col]
            raw = row.get(col)
            if isinstance(raw, (int, float)) and raw == raw:
                out[cat] = float(raw)
    return out


def matrix_ribbon_html(
    scores: dict[str, Any] | None,
    *,
    category_totals: dict[str, float | None] | None = None,
    force_empty_human: bool = False,
    prefer_category_totals: bool = False,
    title: str = "",
    subtitle: str = "",
) -> str:
    """HTML matrix: category rows × pill chips + Cat column."""
    scores = scores or {}
    category_totals = dict(category_totals or {})

    rows_html: list[str] = []
    for i, (cat_key, short, auto_ok) in enumerate(MATRIX_ROWS):
        bundle = scores.get(cat_key) if isinstance(scores.get(cat_key), dict) else None
        if bundle is None and cat_key == "Environmental Fate & Transport":
            alt = scores.get("Environmental Fate and Transport")
            if isinstance(alt, dict):
                bundle = alt

        human_empty = force_empty_human and not auto_ok
        if human_empty:
            cat_score = None
        elif prefer_category_totals:
            cat_score = category_totals.get(cat_key)
            if cat_score is None:
                cat_score = _category_max_from_bundle(bundle)
        else:
            cat_score = _category_max_from_bundle(bundle)
            if cat_score is None:
                cat_score = category_totals.get(cat_key)

        chips: list[str] = []
        filled = 0
        total = len(SUBCAT_DEFS.get(cat_key, ()))
        for full, acr in SUBCAT_DEFS.get(cat_key, ()):
            if human_empty:
                chips.append(subcat_chip_html(acr, full, None, empty=True))
            elif bundle is not None:
                sc = _subcat_score_from_bundle(bundle, full)
                if sc is not None:
                    filled += 1
                chips.append(subcat_chip_html(acr, full, sc))
            else:
                chips.append(subcat_chip_html(acr, full, None, empty=True))

        cat_bg = score_chip_color(None if human_empty else cat_score)
        cat_fg = _chip_text_color(cat_bg)
        if human_empty or cat_score is None:
            cat_val = "—"
            cat_tip = f"{short} — no category score"
            if human_empty:
                cat_tip += " (human-only on auto)"
        else:
            cat_val = format_p2oasys_score(float(cat_score))
            if prefer_category_totals:
                cat_tip = f"{short} category {cat_val} (expert GT category score)"
            else:
                cat_tip = f"{short} category {cat_val} (mean of top-two subcategories)"

        # Left accent bar matches category bin color
        accent = cat_bg if cat_score is not None and not human_empty else "#e5e7eb"
        row_bg = "#fafbfc" if i % 2 else "#ffffff"

        if human_empty:
            cov_html = '<span style="color:#bbb;font-size:0.65rem;margin-left:4px;">human</span>'
        elif bundle is not None:
            cov_html = (
                f'<span style="color:#9ca3af;font-size:0.65rem;margin-left:4px;">'
                f"{filled}/{total}</span>"
            )
        else:
            cov_html = ""

        cat_cell = (
            f'<span title="{_esc(cat_tip)}" style="display:inline-block;padding:4px 10px;'
            f'border-radius:8px;background:{cat_bg};color:{cat_fg};font-weight:700;'
            f'font-size:0.85rem;min-width:2.6rem;text-align:center;">{_esc(cat_val)}</span>'
        )

        rows_html.append(
            f'<tr style="background:{row_bg};">'
            f'<td style="padding:6px 10px 6px 0;white-space:nowrap;vertical-align:middle;'
            f'border-left:4px solid {accent};padding-left:8px;">'
            f'<div style="font-weight:700;font-size:0.78rem;color:#374151;" title="{_esc(cat_key)}">'
            f'{_esc(short)}{cov_html}</div></td>'
            f'<td style="padding:4px 4px;vertical-align:middle;">{"".join(chips)}</td>'
            f'<td style="padding:2px 0 2px 8px;vertical-align:middle;text-align:right;'
            f'white-space:nowrap;width:1%;">{cat_cell}</td>'
            "</tr>"
        )

    head = (
        '<tr style="font-size:0.68rem;color:#6b7280;text-transform:uppercase;letter-spacing:.02em;">'
        '<th style="text-align:left;font-weight:600;padding:0 10px 6px 8px;border-bottom:1px solid #e5e7eb;">Category</th>'
        '<th style="text-align:left;font-weight:600;padding:0 0 6px 4px;border-bottom:1px solid #e5e7eb;">'
        "Subcategories <span style=\"font-weight:400;text-transform:none;letter-spacing:0;\">(hover for name)</span></th>"
        '<th style="text-align:right;font-weight:600;padding:0 0 6px 10px;border-bottom:1px solid #e5e7eb;">Cat</th>'
        "</tr>"
    )
    title_html = ""
    if title:
        title_html = (
            f'<div style="display:flex;align-items:baseline;gap:10px;margin:0 0 8px 0;">'
            f'<div style="font-weight:700;font-size:0.88rem;color:#111827;">{_esc(title)}</div>'
        )
        if subtitle:
            title_html += (
                f'<div style="font-size:0.72rem;color:#6b7280;">{_esc(subtitle)}</div>'
            )
        title_html += "</div>"

    return (
        f'<div style="margin:4px 0 14px 0;padding:8px 10px;border:1px solid #e5e7eb;'
        f'border-radius:10px;background:#fff;overflow-x:auto;display:inline-block;'
        f'max-width:100%;">{title_html}'
        f'<table class="p2o-matrix-table" style="border-collapse:collapse;'
        f'width:max-content;max-width:100%;table-layout:auto;">'
        f"<thead>{head}</thead><tbody>{''.join(rows_html)}</tbody></table></div>"
    )


def render_matrix_ribbon(
    *,
    live_scores: dict[str, Any] | None = None,
    expert_scores: dict[str, Any] | None = None,
    lookup_row: dict[str, Any] | None = None,
    cas: str | None = None,
    show_expert: bool = True,
    show_auto: bool = True,
    default_mode: str = "always",
    widget_key: str = "p2oasys_matrix_ribbon_mode",
) -> None:
    """Option A expandable / Option B always-on subcategory matrix under the TLDR chips."""
    ensure_compact_ribbon_css()
    if widget_key not in st.session_state:
        st.session_state[widget_key] = default_mode == "always"

    always_on = st.toggle(
        "Always show subcategory matrix",
        key=widget_key,
        help="Off = Option A (expandable). On = Option B (always visible).",
    )
    st.caption(
        "Expert Cat column uses the scraped GT category score. "
        "Auto Cat column is the mean of the two highest subcategory scores. "
        "Pill chips: hover acronym for full name. Dashed grey = no score."
    )

    expert_totals = category_scores_from_lookup_row(lookup_row, prefix="expert") if lookup_row else {}
    auto_totals = category_scores_from_lookup_row(lookup_row, prefix="auto") if lookup_row else {}
    if expert_scores is None and cas:
        expert_scores = load_expert_subcat_scores(cas)
    if isinstance(expert_scores, dict) and expert_scores:
        expert_scores = normalize_expert_score_bundle(expert_scores)
    cached_auto_scores = None
    if not (isinstance(live_scores, dict) and live_scores) and cas:
        cached_auto_scores = load_auto_subcat_scores(cas)

    def _body() -> None:
        if show_expert and lookup_row and lookup_row.get("has_expert"):
            n_pills = 0
            if isinstance(expert_scores, dict):
                n_pills = sum(
                    1
                    for b in expert_scores.values()
                    if isinstance(b, dict)
                    for k in b
                    if not str(k).startswith("_")
                )
            expert_sub = (
                f"Subcategory pills from expert harvest ({n_pills} scored cells)"
                if n_pills
                else "Category scores from expert GT (no subcategory harvest for this CAS)"
            )
            st.markdown(
                matrix_ribbon_html(
                    expert_scores if isinstance(expert_scores, dict) else None,
                    category_totals=expert_totals,
                    force_empty_human=False,
                    prefer_category_totals=True,
                    title="Expert",
                    subtitle=expert_sub,
                ),
                unsafe_allow_html=True,
            )
        if show_auto:
            has_live = isinstance(live_scores, dict) and bool(live_scores)
            has_cached_auto = bool(lookup_row and lookup_row.get("has_auto"))
            has_cached_pills = isinstance(cached_auto_scores, dict) and bool(cached_auto_scores)
            if has_live:
                st.markdown(
                    matrix_ribbon_html(
                        live_scores,
                        category_totals=auto_totals,
                        force_empty_human=True,
                        title="Auto",
                        subtitle="Live draft — Process / Life Cycle stay human-only",
                    ),
                    unsafe_allow_html=True,
                )
            elif has_cached_pills:
                st.markdown(
                    matrix_ribbon_html(
                        cached_auto_scores,
                        category_totals=auto_totals,
                        force_empty_human=True,
                        title="Auto",
                        subtitle="Cached auto subcategory pills from last draft",
                    ),
                    unsafe_allow_html=True,
                )
            elif has_cached_auto:
                st.markdown(
                    matrix_ribbon_html(
                        None,
                        category_totals=auto_totals,
                        force_empty_human=True,
                        title="Auto",
                        subtitle="Cached category scores — run a draft to fill subcategory pills",
                    ),
                    unsafe_allow_html=True,
                )

    if always_on:
        _body()
    else:
        with st.expander("Subcategory matrix", expanded=False):
            _body()



def render_expert_ribbon_row(row: dict[str, Any], *, show_legend: bool = False) -> None:
    if show_legend:
        st.markdown(color_legend_html(), unsafe_allow_html=True)
    chips = _chips_from_slots(row, RIBBON_SLOTS_EXPERT)
    chips += chip_html("Overall", row.get("expert_overall"))
    st.markdown(
        '<div style="margin:4px 0 10px 0;"><span style="font-weight:700;margin-right:8px;'
        'vertical-align:top;display:inline-block;padding-top:8px;">Expert</span>'
        + chips + "</div>",
        unsafe_allow_html=True,
    )


def render_auto_ribbon_row(row: dict[str, Any], *, show_legend: bool = False, caption: bool = True) -> None:
    if show_legend:
        st.markdown(color_legend_html(), unsafe_allow_html=True)
    chips = _chips_from_slots(row, RIBBON_SLOTS_AUTO)
    if row.get("auto_overall") is not None:
        chips += chip_html("Overall", row.get("auto_overall"))
    st.markdown(
        '<div style="margin:4px 0 10px 0;"><span style="font-weight:700;margin-right:8px;'
        'vertical-align:top;display:inline-block;padding-top:8px;">Auto</span>'
        + chips + "</div>",
        unsafe_allow_html=True,
    )
    if caption:
        st.caption("Process / Life Cycle are human-only on auto (grey, no number).")


def render_score_ribbon(cas: str, *, auto_scores: dict[str, Any] | None = None) -> dict | None:
    """Show expert (lookup) + auto (live scores or lookup) ribbons. Return lookup row or None."""
    ensure_compact_ribbon_css()
    row = p2oasys_score_lookup.lookup(cas) or {}
    live_auto = scores_dict_to_auto_row(auto_scores) if auto_scores is not None else None

    has_expert = bool(row.get("has_expert"))
    has_auto = bool(
        (live_auto and live_auto.get("has_auto"))
        or row.get("has_auto")
    )
    if not has_expert and not has_auto:
        st.info(
            "No cached expert/auto P2OASys scores for this CAS — "
            "**Generate draft** will run the live assessment pipeline."
        )
        return None

    name = row.get("name_expert") or row.get("name_auto") or ""
    st.markdown(f"**P2OASys scores** for `{cas}`" + (f" — {name}" if name else ""))
    st.markdown(color_legend_html(), unsafe_allow_html=True)
    maybe_show_bin_legend_image()

    if has_expert:
        render_expert_ribbon_row(row)

    auto_row = dict(row)
    if live_auto and live_auto.get("has_auto"):
        auto_row.update(live_auto)
    if auto_row.get("has_auto"):
        render_auto_ribbon_row(auto_row)

    render_matrix_ribbon(
        live_scores=auto_scores if isinstance(auto_scores, dict) else None,
        expert_scores=load_expert_subcat_scores(cas) if has_expert else None,
        lookup_row=row,
        cas=cas,
        show_expert=has_expert,
        show_auto=bool(auto_row.get("has_auto")),
        default_mode="always",
        widget_key="p2oasys_matrix_mode_score_ribbon",
    )

    if p2oasys_score_lookup.should_skip_tci(cas) and auto_scores is None:
        st.success(
            "TCI auto-fetch will be **skipped** (cached expert/auto scores). "
            "Use **Fetch TCI SDS anyway** below if you need a live SDS."
        )
    return row or None


def render_expert_p2oasys_panel(cas: str) -> dict | None:
    """Expert category ribbon for the main-app Reference lookup (not DoSS / overall-only CSV)."""
    row = p2oasys_score_lookup.lookup(cas)
    st.markdown("**P2OASys (expert curated)**")
    if not row or not row.get("has_expert"):
        st.metric("Expert overall", "—")
        st.warning(
            "CAS not in the expert P2OASys category panel "
            "(score lookup DB from expert-vs-auto GT). "
            "Run a live assessment or rebuild the lookup."
        )
        return None
    name = row.get("name_expert") or ""
    if name:
        st.caption(name)
    st.markdown(color_legend_html(), unsafe_allow_html=True)
    maybe_show_bin_legend_image()
    render_expert_ribbon_row(row)
    render_matrix_ribbon(
        live_scores=None,
        expert_scores=load_expert_subcat_scores(cas),
        lookup_row=row,
        cas=cas,
        show_expert=True,
        show_auto=False,
        default_mode="always",
        widget_key="p2oasys_matrix_mode_expert_panel",
    )
    src = row.get("expert_source") or "expert P2OASys GT / score lookup"
    st.caption(f"Source: {src}")
    st.caption(
        "Expert subcategory pills: p2oasys.turi.org harvest (pages 1–101), "
        "clean single-CAS split only. Multi-CAS and empty-CAS names stay parked."
    )
    return row


def try_tci_sds_fields(cas: str) -> dict[str, Any]:
    """Best-effort TCI SDS → hazard fields (map/cache; live search off)."""
    try:
        from v7.sds_acquisition import find_vendor_hits, download_vendor_hit, structured_from_pdf
    except Exception:
        logger.debug("TCI SDS acquisition helpers unavailable", exc_info=True)
        return {}
    try:
        hits = find_vendor_hits(cas, "tci", enable_live_search=False)
        if not hits:
            logger.info("No local TCI map hit for %s", cas)
            return {}
        doc = download_vendor_hit(hits[0])
        pdf_bytes = getattr(doc, "pdf_bytes", None) or b""
        if not pdf_bytes:
            return {}
        structured = structured_from_pdf(pdf_bytes) or {}
        fields: dict[str, Any] = dict(structured) if isinstance(structured, dict) else {}
        if not fields:
            try:
                from utils.input_handler import get_input_handler
                from io import BytesIO

                class _Named(BytesIO):
                    def __init__(self, data: bytes, name: str) -> None:
                        super().__init__(data)
                        self.name = name

                staged = get_input_handler().process_sds_pdf(_Named(pdf_bytes, f"tci_{cas}.pdf"))
                fields = dict(getattr(staged, "hazard_fields", None) or {})
            except Exception:
                logger.debug("InputHandler SDS parse failed", exc_info=True)
        if fields:
            fields.setdefault("_tci_product", getattr(hits[0], "product_id", None))
            fields.setdefault("_tci_via", "opt_in_fetch")
        return fields
    except Exception as exc:
        logger.info("TCI SDS enrich skipped/failed for %s: %s", cas, exc)
        return {}
