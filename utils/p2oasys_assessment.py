"""
Versioned P2OASys assessment package (v6 Phase B).

Builds an auditable draft assessment from hazard data + scorer traces:
executive summary, itemized scores, evidence appendix, missing-data statement,
conflicts, analyst overrides, and matrix/software fingerprints.

Source precedence (higher wins when selecting among competing experimental
vs predicted values for the *same* endpoint before scoring):
  1. curated_experimental
  2. experimental
  3. read_across
  4. qsar_in_domain
  5. qsar_out_of_domain / heuristic
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from utils import p2oasys_aggregate, p2oasys_scorer
from utils.evidence_normalizer import make_evidence_record

ASSESSMENT_SCHEMA_VERSION = "p2oasys_assessment_v6.2"

# Lower number = higher precedence.
SOURCE_PRECEDENCE: dict[str, int] = {
    "curated_experimental": 1,
    "experimental": 2,
    "read_across": 3,
    "qsar_in_domain": 4,
    "qsar_out_of_domain": 5,
    "heuristic": 6,
    "unknown": 9,
}


def classify_source_tier(source: str | None, *, predicted: bool = False, in_domain: bool | None = None) -> str:
    """Map a free-text source label to a precedence tier."""
    s = (source or "").strip().lower()
    if predicted or "opera" in s or "qsar" in s or "vega" in s or "catmos" in s:
        if in_domain is False:
            return "qsar_out_of_domain"
        return "qsar_in_domain"
    if "iarc" in s or "lookup" in s or "curat" in s:
        return "curated_experimental"
    if "pubchem" in s or "toxval" in s or "iuclid" in s or "sds" in s or "cpdb" in s:
        return "experimental"
    if "read" in s and "across" in s:
        return "read_across"
    if not s:
        return "unknown"
    return "experimental"


def prefer_evidence(candidates: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """
    Select the highest-precedence evidence record.

    Among equal precedence, prefer the more conservative numeric value when
    ``higher_is_safer`` is set on the records; otherwise keep the first.
    """
    if not candidates:
        return None
    ranked = sorted(
        candidates,
        key=lambda c: (
            SOURCE_PRECEDENCE.get(c.get("tier") or "unknown", 9),
            c.get("value") if c.get("higher_is_safer", True) else -(c.get("value") or 0),
        ),
    )
    return ranked[0]


def apply_overrides(
    scores: dict[str, Any],
    overrides: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Apply analyst overrides to a scores dict.

    Each override: ``{category, subcategory, unit, score, justification, analyst?}``.
    Justification is required; overrides without it are ignored.
    """
    applied: list[dict[str, Any]] = []
    if not overrides:
        return scores, applied

    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in scores.items()}
    for ov in overrides:
        just = (ov.get("justification") or "").strip()
        if not just:
            continue
        cat = ov.get("category")
        sub = ov.get("subcategory")
        unit = ov.get("unit")
        try:
            new_score = int(ov["score"])
        except (KeyError, TypeError, ValueError):
            continue
        if not cat or not sub or not unit:
            continue
        cat_bundle = out.setdefault(cat, {})
        if not isinstance(cat_bundle, dict):
            continue
        # Deep-copy subcategory dicts so we do not mutate the original scores.
        if sub in cat_bundle and isinstance(cat_bundle[sub], dict):
            cat_bundle[sub] = dict(cat_bundle[sub])
        sub_bundle = cat_bundle.setdefault(sub, {})
        if not isinstance(sub_bundle, dict):
            continue
        prev = sub_bundle.get(unit)
        sub_bundle[unit] = new_score
        unit_scores = [v for k, v in sub_bundle.items() if k != "_max" and isinstance(v, (int, float))]
        if unit_scores:
            sub_bundle["_max"] = max(unit_scores)
        subcat_maxima = []
        for sk, bundle in cat_bundle.items():
            if sk.startswith("_") or not isinstance(bundle, dict):
                continue
            sm = bundle.get("_max")
            if isinstance(sm, (int, float)):
                subcat_maxima.append(float(sm))
        agg = p2oasys_scorer._category_score_mean_top_two_subcategories(subcat_maxima)
        if agg is not None:
            cat_bundle["_category_max"] = agg
        applied.append({
            "category": cat,
            "subcategory": sub,
            "unit": unit,
            "previous_score": prev,
            "score": new_score,
            "justification": just,
            "analyst": ov.get("analyst") or "analyst",
            "applied_at": datetime.now(timezone.utc).isoformat(),
        })
    return out, applied


def build_assessment_package(
    *,
    cas: str,
    chemical_name: str | None,
    hazard_data: dict[str, Any],
    scores: dict[str, Any],
    trace: dict[str, Any],
    matrix_path: Path | str,
    matrix_kind: str,
    overrides: list[dict[str, Any]] | None = None,
    pipeline_notes: list[str] | None = None,
    sources_used: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build a versioned assessment JSON object suitable for export / audit.

    Does not mutate ``scores``; overrides are applied to a copy.
    """
    matrix_path = Path(matrix_path)
    fp = p2oasys_scorer.matrix_fingerprint(matrix_path)
    final_scores, applied_overrides = apply_overrides(scores, overrides)

    overall_max = p2oasys_aggregate.aggregate_category_scores(final_scores, "max")
    overall_mean = p2oasys_aggregate.aggregate_category_scores(final_scores, "mean")
    n_cat, cat_names = p2oasys_aggregate.count_scored_categories(final_scores)

    category_status = dict(trace.get("category_status") or {})
    itemized: list[dict[str, Any]] = []
    for category, data in final_scores.items():
        if category.startswith("_") or not isinstance(data, dict):
            continue
        cat_max = data.get("_category_max")
        for subcat, subdata in data.items():
            if subcat.startswith("_") or not isinstance(subdata, dict):
                continue
            for unit_name, score in subdata.items():
                if unit_name == "_max" or not isinstance(score, (int, float)):
                    continue
                match = next(
                    (
                        s for s in (trace.get("scored") or [])
                        if s.get("category") == category
                        and s.get("subcategory") == subcat
                        and s.get("unit") == unit_name
                    ),
                    None,
                )
                itemized.append({
                    "category": category,
                    "subcategory": subcat,
                    "endpoint": unit_name,
                    "score": score,
                    "category_score": cat_max,
                    "status": (match or {}).get("status") or p2oasys_scorer.STATUS_SCORED,
                    "input_value": (match or {}).get("input_value"),
                    "qualifier": (match or {}).get("qualifier"),
                    "predicted": bool((match or {}).get("predicted")),
                    "matrix_rule": (match or {}).get("matrix_rule"),
                })

    evidence_appendix: list[dict[str, Any]] = []
    for key, payload in (trace.get("evidence") or {}).items():
        if payload is None:
            continue
        if isinstance(payload, dict):
            evidence_appendix.append(
                make_evidence_record(
                    cas=cas,
                    source="p2oasys_selected",
                    evidence_type=key,
                    value=payload.get("value"),
                    unit=payload.get("unit"),
                    route=payload.get("route"),
                    raw_text=str(payload.get("raw") or payload),
                    flags=[f for f in [
                        f"qualifier:{payload['qualifier']}" if payload.get("qualifier") else None,
                        "predicted" if payload.get("predicted") else None,
                    ] if f],
                    parser_version=trace.get("scorer_version") or p2oasys_scorer.SCORER_VERSION,
                )
            )
        else:
            evidence_appendix.append(
                make_evidence_record(
                    cas=cas,
                    source="p2oasys_selected",
                    evidence_type=key,
                    value=payload,
                    parser_version=trace.get("scorer_version") or p2oasys_scorer.SCORER_VERSION,
                )
            )

    missing = list(trace.get("missing") or [])
    rejected = list(trace.get("rejected") or [])

    top_hazards = sorted(
        [
            {"category": c, "score": d.get("_category_max"), "status": category_status.get(c)}
            for c, d in final_scores.items()
            if isinstance(d, dict) and d.get("_category_max") is not None
        ],
        key=lambda x: -(x["score"] or 0),
    )[:5]

    is_draft_only = matrix_kind != "official"
    return {
        "schema_version": ASSESSMENT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "assessment_kind": "draft_screening" if is_draft_only else "draft_for_expert_review",
        "identity": {
            "cas": cas,
            "chemical_name": chemical_name,
            "cid": hazard_data.get("cid"),
            "molecular_weight": hazard_data.get("molecular_weight"),
        },
        "executive_summary": {
            "categories_scored": n_cat,
            "categories": cat_names,
            "overall_max": overall_max if overall_max == overall_max else None,
            "overall_mean": overall_mean if overall_mean == overall_mean else None,
            "top_hazards": top_hazards,
            "category_status": category_status,
            "matrix_kind": matrix_kind,
            "is_turi_calibrated": matrix_kind == "official",
            "needs_review": bool(rejected) or is_draft_only or bool(applied_overrides),
            "missing_data_count": len(missing),
            "rejected_evidence_count": len(rejected),
        },
        "p2oasys_table": itemized,
        "scores": final_scores,
        "evidence_appendix": evidence_appendix,
        "missing_data": missing,
        "rejected_evidence": rejected,
        "conflicts": [],
        "overrides": applied_overrides,
        "matrix": {**fp, "kind": matrix_kind},
        "software": {
            "scorer_version": trace.get("scorer_version") or p2oasys_scorer.SCORER_VERSION,
            "assessment_schema": ASSESSMENT_SCHEMA_VERSION,
        },
        "sources_used": list(sources_used or []),
        "pipeline_notes": list(pipeline_notes or []),
        "trace": {
            "scored": trace.get("scored") or [],
            "evidence": trace.get("evidence") or {},
        },
        "disclaimer": (
            "Draft automatic P2OASys assessment for expert review. "
            + (
                "Scores use a development placeholder matrix and are NOT TURI-calibrated. "
                if is_draft_only
                else "Scores use the configured TURI hazard matrix workbook. "
            )
            + "Every score should be checked against the evidence appendix before use."
        ),
    }


def assessment_to_html(package: dict[str, Any]) -> str:
    """
    Render a compact HTML executive summary from an assessment package.

    Same object as the JSON export — no second scoring path.
    """
    ident = package.get("identity") or {}
    summary = package.get("executive_summary") or {}
    top = summary.get("top_hazards") or []
    rows = package.get("p2oasys_table") or []
    disclaimer = package.get("disclaimer") or ""
    matrix = package.get("matrix") or {}
    software = package.get("software") or {}

    def _esc(x: Any) -> str:
        return (
            str(x if x is not None else "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    top_html = "".join(
        f"<li><strong>{_esc(t.get('category'))}</strong>: {_esc(t.get('score'))} "
        f"({_esc(t.get('status'))})</li>"
        for t in top
    )
    table_rows = "".join(
        "<tr>"
        f"<td>{_esc(r.get('category'))}</td>"
        f"<td>{_esc(r.get('subcategory'))}</td>"
        f"<td>{_esc(r.get('endpoint'))}</td>"
        f"<td>{_esc(r.get('score'))}</td>"
        f"<td>{_esc(r.get('status'))}</td>"
        f"<td>{_esc(r.get('input_value'))}</td>"
        "</tr>"
        for r in rows[:80]
    )
    missing_n = summary.get("missing_data_count")
    rejected_n = summary.get("rejected_evidence_count")
    override_n = len(package.get("overrides") or [])
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>P2OASys draft — {_esc(ident.get('cas'))}</title>
  <style>
    body {{ font-family: Georgia, serif; margin: 2rem; color: #1a1a1a; background: #faf8f5; }}
    h1 {{ font-size: 1.6rem; margin-bottom: 0.2rem; }}
    .meta {{ color: #555; margin-bottom: 1.5rem; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.9rem; }}
    th, td {{ border: 1px solid #ccc; padding: 0.35rem 0.5rem; text-align: left; }}
    th {{ background: #eee; }}
    .disclaimer {{ margin-top: 1.5rem; padding: 0.8rem; background: #fff3cd; border: 1px solid #e0c36a; }}
  </style>
</head>
<body>
  <h1>Draft P2OASys assessment</h1>
  <p class="meta">
    CAS {_esc(ident.get('cas'))}
    · {_esc(ident.get('chemical_name') or '—')}
    · generated {_esc(package.get('generated_at'))}
  </p>
  <p>
    Categories scored: <strong>{_esc(summary.get('categories_scored'))}</strong>
    · Overall (max): <strong>{_esc(summary.get('overall_max'))}</strong>
    · Matrix: {_esc(matrix.get('filename'))} ({_esc(matrix.get('kind'))})
    · Scorer: {_esc(software.get('scorer_version'))}
  </p>
  <h2>Top hazards</h2>
  <ul>{top_html or '<li>None scored</li>'}</ul>
  <p>Missing units: {_esc(missing_n)} · Rejected evidence: {_esc(rejected_n)} · Overrides: {_esc(override_n)}</p>
  <h2>Itemized scores</h2>
  <table>
    <thead><tr><th>Category</th><th>Subcategory</th><th>Endpoint</th><th>Score</th><th>Status</th><th>Input</th></tr></thead>
    <tbody>{table_rows or '<tr><td colspan="6">No scores</td></tr>'}</tbody>
  </table>
  <div class="disclaimer">{_esc(disclaimer)}</div>
</body>
</html>
"""


def assessment_is_exportable(matrix_kind: str, *, allow_placeholder: bool = False) -> bool:
    """Official assessments are always exportable; placeholder only if explicitly allowed."""
    if matrix_kind == "official":
        return True
    return bool(allow_placeholder)
