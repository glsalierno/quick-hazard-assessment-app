"""
Human-in-the-loop P2OASys assessment form helpers.

Builds matrix-driven dropdown options (score 2/4/6/8/10 + cell label), maps
automatic scorer results into suggested subcategory selections, and converts
user picks into ``p2oasys_assessment.apply_overrides`` records.

Process Factors and Life Cycle Factors are **never** auto-filled (left blank /
unscored; never default to 2).
"""

from __future__ import annotations

import copy
from typing import Any, Optional

from utils import p2oasys_scorer

# Categories the automated pipeline may prefill.
AUTO_FILL_CATEGORIES: frozenset[str] = frozenset({
    "Acute Human Effects",
    "Chronic Human Effects",
    "Ecological Hazards",
    "Environmental Fate & Transport",
    "Atmospheric Hazard",
    "Physical Properties",
})

# Always left blank until the analyst selects a score.
MANUAL_ONLY_CATEGORIES: frozenset[str] = frozenset({
    "Process Factors",
    "Life Cycle Factors",
})

SELECT_SENTINEL = "— select —"
JUSTIFICATION_FORM = "Human-in-the-loop form selection"
JUSTIFICATION_AUTO_CONFIRM = "Confirmed automatic draft score"


def is_manual_only_category(category: str) -> bool:
    return category in MANUAL_ONLY_CATEGORIES


def _is_score_header_artifact(subcategory: str, unit_name: str, rule: dict[str, Any]) -> bool:
    """Detect sheet header rows mis-parsed as a numeric unit of 2/4/6/8/10."""
    if unit_name.strip() != subcategory.strip():
        return False
    if rule.get("type") != "numeric":
        return False
    thresholds = rule.get("thresholds") or []
    vals = [float(t) for t, _s in thresholds if t is not None]
    return vals == [2.0, 4.0, 6.0, 8.0, 10.0]


def cell_label_for_score(rule: dict[str, Any], score: int) -> str:
    """Best-effort matrix cell phrase/threshold text for a score bin."""
    labels = rule.get("cell_labels") or {}
    if score in labels:
        return str(labels[score]).strip()
    # Fallback from structured rule fields when cell_labels missing (older loads).
    rtype = rule.get("type")
    if rtype == "numeric":
        for thr, sc in rule.get("thresholds") or []:
            if int(sc) == int(score):
                return str(thr)
    elif rtype == "phrase":
        for phrase, sc in rule.get("phrases") or []:
            if int(sc) == int(score):
                return str(phrase)
    elif rtype in ("text", "ghs_h"):
        parts = [k for k, v in (rule.get("mapping") or {}).items() if int(v) == int(score)]
        if parts:
            return ", ".join(parts)
    return ""


def format_option_label(score: int, cell_label: str) -> str:
    label = (cell_label or "").strip()
    if not label:
        return str(score)
    # Keep dropdowns readable.
    if len(label) > 120:
        label = label[:117] + "…"
    # Collapse whitespace/newlines from matrix cells.
    label = " ".join(label.split())
    return f"{score} — {label}"


def _unit_priority(unit_name: str) -> int:
    u = unit_name.upper()
    if "KEY PHRASE" in u or "KEY WORD" in u:
        return 0
    if "GHS H" in u:
        return 1
    if "IARC" in u:
        return 2
    if "LD50" in u or "LC50" in u:
        return 3
    if "NFPA" in u or "FLASH" in u:
        return 4
    return 5


def _iter_subcategory_units(
    matrix: dict[str, Any],
) -> list[tuple[str, str, dict[str, dict[str, Any]]]]:
    """Yield (category, subcategory, units_dict) skipping header artifacts."""
    out: list[tuple[str, str, dict[str, dict[str, Any]]]] = []
    for category, subcats in matrix.items():
        if not isinstance(subcats, dict):
            continue
        for subcat, units in subcats.items():
            if subcat.startswith("_") or not isinstance(units, dict):
                continue
            cleaned: dict[str, dict[str, Any]] = {}
            for unit_name, rule in units.items():
                if not isinstance(rule, dict):
                    continue
                if _is_score_header_artifact(subcat, unit_name, rule):
                    continue
                cleaned[unit_name] = rule
            if not cleaned:
                continue
            # Skip subcategory that is only the category-name header artifact.
            if subcat.strip() == category.strip() and len(cleaned) == 0:
                continue
            out.append((category, subcat, cleaned))
    return out


def preferred_unit_for_labels(
    units: dict[str, dict[str, Any]],
    *,
    preferred_unit: str | None = None,
) -> str:
    """Pick the unit whose cell labels drive the subcategory dropdown."""
    if preferred_unit and preferred_unit in units:
        return preferred_unit
    ranked = sorted(units.keys(), key=lambda u: (_unit_priority(u), u.lower()))
    return ranked[0]


def build_score_options(
    rule: dict[str, Any],
    *,
    include_empty: bool = True,
) -> list[dict[str, Any]]:
    """
    Build selectbox options for one matrix unit.

    Each option: ``{value, label, score}`` where ``value`` is None for the empty
    sentinel or an int score.
    """
    options: list[dict[str, Any]] = []
    if include_empty:
        options.append({"value": None, "score": None, "label": SELECT_SENTINEL})
    for score in p2oasys_scorer.SCORE_COLS:
        cell = cell_label_for_score(rule, score)
        options.append({
            "value": score,
            "score": score,
            "label": format_option_label(score, cell),
        })
    return options


def build_form_fields(matrix: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Matrix → flat list of subcategory form fields.

    Each field::
        {
          category, subcategory, unit_names, primary_unit,
          options: [{value, score, label}, ...],
          allow_auto: bool,
        }
    """
    fields: list[dict[str, Any]] = []
    for category, subcat, units in _iter_subcategory_units(matrix):
        primary = preferred_unit_for_labels(units)
        fields.append({
            "category": category,
            "subcategory": subcat,
            "unit_names": list(units.keys()),
            "primary_unit": primary,
            "options": build_score_options(units[primary], include_empty=True),
            "allow_auto": category in AUTO_FILL_CATEGORIES,
            "rules": units,
        })
    return fields


def strip_manual_only_scores(scores: dict[str, Any] | None) -> dict[str, Any]:
    """
    Return a deep copy of scores with Process / Life Cycle cleared.

    Ensures those categories are never auto-presented (and never default to 2).
    """
    out = copy.deepcopy(scores or {})
    for cat in MANUAL_ONLY_CATEGORIES:
        if cat in out:
            out[cat] = {}
    return out


def _evidence_oneliner(
    category: str,
    subcategory: str,
    unit: str,
    score: int,
    trace: dict[str, Any] | None,
) -> str:
    scored = (trace or {}).get("scored") or []
    match = next(
        (
            s for s in scored
            if s.get("category") == category
            and s.get("subcategory") == subcategory
            and s.get("unit") == unit
            and s.get("score") == score
        ),
        None,
    )
    if not match:
        return f"Auto: {score} ({unit})"
    inp = match.get("input_value")
    status = match.get("status") or "Scored"
    pred = " predicted" if match.get("predicted") else ""
    if inp is not None and inp != "":
        return f"Auto: {score} from {unit} = {inp} ({status}{pred})"
    return f"Auto: {score} from {unit} ({status}{pred})"


def map_auto_suggestions(
    matrix: dict[str, Any],
    scores: dict[str, Any] | None,
    trace: dict[str, Any] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Map scorer output → suggested selection per subcategory.

    Key: ``(category, subcategory)``.
    Value::
        {
          score, unit, evidence, source: "auto"|"empty",
          options_unit,  # unit used for option labels
        }

    Manual-only categories always get ``source="empty"`` / score None.
    When multiple units scored in a subcategory, prefer the highest score;
    ties broken by unit priority (Key Phrases > GHS > …).
    """
    cleaned = strip_manual_only_scores(scores)
    suggestions: dict[tuple[str, str], dict[str, Any]] = {}

    for category, subcat, units in _iter_subcategory_units(matrix):
        key = (category, subcat)
        primary = preferred_unit_for_labels(units)
        base = {
            "score": None,
            "unit": primary,
            "options_unit": primary,
            "evidence": "Not auto-filled — please select",
            "source": "empty",
        }
        if category in MANUAL_ONLY_CATEGORIES:
            suggestions[key] = base
            continue

        cat_bundle = cleaned.get(category) or {}
        sub_bundle = cat_bundle.get(subcat) if isinstance(cat_bundle, dict) else None
        if not isinstance(sub_bundle, dict):
            suggestions[key] = base
            continue

        unit_scores: list[tuple[int, str]] = []
        for uname, val in sub_bundle.items():
            if uname.startswith("_") or uname not in units:
                continue
            if isinstance(val, (int, float)):
                unit_scores.append((int(val), uname))
        if not unit_scores:
            suggestions[key] = base
            continue

        # Prefer highest score; tie-break by unit priority.
        unit_scores.sort(key=lambda t: (-t[0], _unit_priority(t[1]), t[1].lower()))
        best_score, best_unit = unit_scores[0]
        label_unit = preferred_unit_for_labels(units, preferred_unit=best_unit)
        suggestions[key] = {
            "score": best_score,
            "unit": best_unit,
            "options_unit": label_unit,
            "evidence": _evidence_oneliner(category, subcat, best_unit, best_score, trace),
            "source": "auto",
        }
    return suggestions


def build_overrides_from_selections(
    selections: dict[tuple[str, str], Optional[int]],
    suggestions: dict[tuple[str, str], dict[str, Any]],
    matrix: dict[str, Any],
    *,
    analyst: str = "analyst",
) -> list[dict[str, Any]]:
    """
    Convert form selections into ``apply_overrides`` records.

    - Manual-only / previously empty: any selected score becomes an override.
    - Auto-prefilled: override when the user changes the score, or when they
      keep it (recorded as confirmation so the assessment package lists it).
    - ``None`` selection: no override (category stays unscored / auto-only).
    """
    # Index primary units from matrix for empty subs.
    primary_by_key: dict[tuple[str, str], str] = {}
    for category, subcat, units in _iter_subcategory_units(matrix):
        primary_by_key[(category, subcat)] = preferred_unit_for_labels(units)

    overrides: list[dict[str, Any]] = []
    for key, selected in selections.items():
        if selected is None:
            continue
        try:
            score = int(selected)
        except (TypeError, ValueError):
            continue
        if score not in p2oasys_scorer.SCORE_COLS:
            continue

        sug = suggestions.get(key) or {}
        unit = sug.get("unit") or primary_by_key.get(key)
        if not unit:
            continue
        category, subcategory = key
        auto_score = sug.get("score")
        if sug.get("source") == "auto" and auto_score == score:
            justification = JUSTIFICATION_AUTO_CONFIRM
        else:
            justification = JUSTIFICATION_FORM
        overrides.append({
            "category": category,
            "subcategory": subcategory,
            "unit": unit,
            "score": score,
            "justification": justification,
            "analyst": analyst,
        })

        # If other units in the subcategory scored higher than the selection,
        # pin them down so subcategory _max matches the analyst choice.
        # (apply_overrides only sets one unit; we add companion overrides.)
        # Companion units are discovered from suggestions' sibling scores via matrix.
        # We only know siblings from the original scores via suggestions map — pass
        # extras through selection metadata if needed. Handled in apply_form_selections.

    return overrides


def apply_form_selections(
    base_scores: dict[str, Any],
    selections: dict[tuple[str, str], Optional[int]],
    suggestions: dict[tuple[str, str], dict[str, Any]],
    matrix: dict[str, Any],
    *,
    analyst: str = "analyst",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Apply form selections so each chosen subcategory ``_max`` equals the pick,
    then recompute category rollups (mean of two highest subcategory maxima).

    Uses ``p2oasys_assessment.apply_overrides`` after normalizing multi-unit
    subcategory bundles so a single analyst pick is not overshadowed by a
    higher leftover unit score.
    """
    from utils import p2oasys_assessment

    working = strip_manual_only_scores(base_scores)

    # For each subcategory: None clears any auto score; a score collapses the
    # subcategory to that unit/value so _max matches the analyst pick.
    for (category, subcategory), selected in selections.items():
        if selected is None:
            cat_bundle = working.get(category)
            if isinstance(cat_bundle, dict) and subcategory in cat_bundle:
                del cat_bundle[subcategory]
            continue
        try:
            score = int(selected)
        except (TypeError, ValueError):
            continue
        sug = suggestions.get((category, subcategory)) or {}
        units_map = ((matrix.get(category) or {}).get(subcategory) or {})
        unit = sug.get("unit")
        if not unit or unit not in units_map:
            # Fall back to preferred label unit among real (non-artifact) units.
            cleaned = {
                u: r for u, r in units_map.items()
                if isinstance(r, dict) and not _is_score_header_artifact(subcategory, u, r)
            }
            if not cleaned:
                continue
            unit = preferred_unit_for_labels(cleaned, preferred_unit=sug.get("unit"))

        cat_bundle = working.setdefault(category, {})
        if not isinstance(cat_bundle, dict):
            continue
        cat_bundle[subcategory] = {unit: score, "_max": score}

    # Rebuild category rollups on the collapsed working copy first.
    for category, cat_bundle in list(working.items()):
        if category.startswith("_") or not isinstance(cat_bundle, dict):
            continue
        subcat_maxima: list[float] = []
        for sk, bundle in cat_bundle.items():
            if sk.startswith("_") or not isinstance(bundle, dict):
                continue
            sm = bundle.get("_max")
            if isinstance(sm, (int, float)):
                subcat_maxima.append(float(sm))
        agg = p2oasys_scorer._category_score_mean_top_two_subcategories(subcat_maxima)
        if agg is not None:
            cat_bundle["_category_max"] = agg
        elif "_category_max" in cat_bundle:
            del cat_bundle["_category_max"]

    overrides = build_overrides_from_selections(
        selections, suggestions, matrix, analyst=analyst
    )
    # apply_overrides on already-collapsed scores keeps audit trail; scores unchanged
    # if override matches collapsed values.
    final, applied = p2oasys_assessment.apply_overrides(working, overrides)
    return final, applied


def category_score_rows(scores: dict[str, Any], matrix: dict[str, Any]) -> list[dict[str, Any]]:
    """Summary rows for sidebar: category → score / status."""
    rows: list[dict[str, Any]] = []
    # Preserve matrix category order.
    for category in matrix.keys():
        bundle = scores.get(category) if isinstance(scores, dict) else None
        if not isinstance(bundle, dict):
            rows.append({
                "category": category,
                "score": None,
                "status": "Not assessed",
                "manual_only": category in MANUAL_ONLY_CATEGORIES,
            })
            continue
        val = bundle.get("_category_max")
        if isinstance(val, (int, float)):
            status = "Scored"
        else:
            status = "Not assessed"
        rows.append({
            "category": category,
            "score": val if isinstance(val, (int, float)) else None,
            "status": status,
            "manual_only": category in MANUAL_ONLY_CATEGORIES,
        })
    return rows


def option_index_for_score(options: list[dict[str, Any]], score: Optional[int]) -> int:
    """Index into options list for st.selectbox ``index=``."""
    if score is None:
        return 0
    for i, opt in enumerate(options):
        if opt.get("score") == score:
            return i
    return 0
