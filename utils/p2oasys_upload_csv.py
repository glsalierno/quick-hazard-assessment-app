"""Export a single-CAS P2OASys website 'Upload from file' CSV.

The live site expects a wide workbook: header block (name / CAS / SDS …)
then Category / Sub Category / Units rows, each with a paired Memo row.

Auto fills the six scored categories. Process Factors and Life Cycle Factors
stay blank (human-only). Unmatched template units stay blank.

Numeric matrix units get the raw input_value from the scoring trace
(e.g. LD50 mg/kg). Selection units get the 2–10 score.
"""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Any

AUTO_SKIP_CATEGORIES = {
    "process factors",
    "work environment process specific factors",
    "life cycle factors",
}

# Template string -> matrix string (category / subcategory / unit).
CAT_ALIASES = {
    "work environment process specific factors": "process factors",
}

SUB_ALIASES = {
    "heat of work space": "heat",
    "cold for work place": "cold",
    "high/low temperature - system": "high/low temperature system",
    "persistence in air, water, soil/sediment": "persistence",
    "degradability in water, soil, sediment": "rapid degradability",
    "upstream processing and manufacturing": "upstream effects",
    "usage and retail": "consumer hazard",
    "end of life": "disposal hazard (landfill, incineration)",
}

UNIT_ALIASES = {
    "pel/tlv ppm (gas or vapor)": "pel/tlv ppm",
    " pel/tlv (dusts/particles) mg/m3": "pel/tlv (dusts) mg/m3",
    "pel/tlv (dusts/particles) mg/m3": "pel/tlv (dusts) mg/m3",
    "mm hg at temp range 20-25c": "mm hg",
    "key phrases liquid": "key phrases",
    "key phrases gas": "key phrases",
    "air t1/2 life (days)": "air t1/2 days",
    "water t1/2 life (days)": "water t1/2 days",
    "soil/sediment t1/2 life (days)": "soil/sediment t1/2 days",
    "overall key phrases for air, water or soil/sediment": "key phrases",
    "28-day study: % breakdown dissolved organic carbon (doc or thdoc or thco2 or thod)":
        "28-day study: % breakdown dissolved organic carbon",
    "occurrence: near certain": "occurence: near certain",
    "occurrence: highly likely": "occurence: highly likely",
    "occurrence: likely": "occurence: likely",
    "occurrence: unlikely": "occurence: unlikely",
    "occurrence: remote": "occurence: remote",
    "gwp relative to co2": "gwp",
}

DEFAULT_TEMPLATE = Path(__file__).resolve().parent.parent / "data" / "p2oasys_upload_template.csv"


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("\ufeff", "")
    s = re.sub(r"\s+", " ", s)
    return s


def _alias_cat(cat: str) -> str:
    n = _norm(cat)
    return CAT_ALIASES.get(n, n)


def _alias_sub(sub: str) -> str:
    n = _norm(sub)
    return SUB_ALIASES.get(n, n)


def _alias_unit(unit: str) -> str:
    n = _norm(unit)
    return UNIT_ALIASES.get(n, n)


def _fmt_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value != value:  # NaN
            return ""
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
        return f"{value:.4g}"
    if isinstance(value, int):
        return str(value)
    s = str(value).strip()
    if s.lower() in {"none", "nan"}:
        return ""
    return s


def _index_scores(scores: dict[str, Any] | None) -> dict[tuple[str, str, str], Any]:
    out: dict[tuple[str, str, str], Any] = {}
    if not isinstance(scores, dict):
        return out
    for cat, cdat in scores.items():
        if str(cat).startswith("_") or not isinstance(cdat, dict):
            continue
        for sub, sdat in cdat.items():
            if str(sub).startswith("_") or not isinstance(sdat, dict):
                continue
            for unit, val in sdat.items():
                if str(unit).startswith("_"):
                    continue
                if isinstance(val, (int, float)) and val == val:
                    out[(_norm(cat), _norm(sub), _norm(unit))] = val
    return out


def _index_trace(score_trace: dict[str, Any] | None) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    if not isinstance(score_trace, dict):
        return out
    for item in score_trace.get("scored") or []:
        if not isinstance(item, dict):
            continue
        key = (
            _norm(str(item.get("category") or "")),
            _norm(str(item.get("subcategory") or "")),
            _norm(str(item.get("unit") or "")),
        )
        out[key] = item
    return out


def _lookup(
    cat: str,
    sub: str,
    unit: str,
    scores_ix: dict[tuple[str, str, str], Any],
    trace_ix: dict[tuple[str, str, str], dict[str, Any]],
) -> tuple[Any, dict[str, Any] | None]:
    """Return (cell_value, trace_item) for a template unit row."""
    cands = []
    c0, s0, u0 = _alias_cat(cat), _alias_sub(sub), _alias_unit(unit)
    cands.append((c0, s0, u0))
    cands.append((_norm(cat), _norm(sub), _norm(unit)))
    cands.append((c0, _norm(sub), u0))
    cands.append((c0, s0, _norm(unit)))
    seen = set()
    for key in cands:
        if key in seen:
            continue
        seen.add(key)
        tr = trace_ix.get(key)
        sc = scores_ix.get(key)
        if tr is None and sc is None:
            continue
        if tr:
            rtype = ""
            rule = tr.get("matrix_rule") or {}
            if isinstance(rule, dict):
                rtype = str(rule.get("type") or "")
            if rtype == "numeric" and tr.get("input_value") not in (None, ""):
                return tr.get("input_value"), tr
            if tr.get("score") not in (None, ""):
                return tr.get("score"), tr
        if sc is not None:
            return sc, tr
    return None, None


def _memo_text(trace_item: dict[str, Any] | None, extra_note: str = "") -> str:
    if not trace_item and not extra_note:
        return ""
    parts: list[str] = []
    if extra_note:
        parts.append(extra_note)
    if not trace_item:
        return " | ".join(parts)
    src = trace_item.get("qualifier")
    if src:
        parts.append(str(src))
    if trace_item.get("predicted"):
        parts.append("predicted")
    status = trace_item.get("status")
    if status and status not in ("Scored", "scored"):
        parts.append(str(status))
    inp = trace_item.get("input_value")
    unit = trace_item.get("unit")
    if inp not in (None, "") and unit:
        # keep memos short
        pass
    parts.append("auto-P2OASys")
    # unique preserve order
    seen = set()
    out = []
    for p in parts:
        p = str(p).strip()
        if p and p.lower() not in seen:
            seen.add(p.lower())
            out.append(p)
    return " | ".join(out)


def load_template_rows(template_path: str | Path | None = None) -> list[list[str]]:
    path = Path(template_path) if template_path else DEFAULT_TEMPLATE
    with path.open(encoding="utf-8", errors="replace", newline="") as f:
        return [list(r) for r in csv.reader(f)]


def build_single_cas_upload_csv(
    *,
    cas: str,
    name: str = "",
    scores: dict[str, Any] | None = None,
    score_trace: dict[str, Any] | None = None,
    sds_source: str = "",
    sds_url: str = "",
    sds_year: str = "",
    assessment_note: str = "",
    template_path: str | Path | None = None,
) -> str:
    """Return CSV text (utf-8) for one chemical in website upload layout."""
    rows = load_template_rows(template_path)
    if len(rows) < 9:
        raise ValueError("P2OASys upload template is missing header/data rows")

    chem = (name or cas or "chemical").strip() or cas
    scores_ix = _index_scores(scores)
    trace_ix = _index_trace(score_trace)

    note = assessment_note or (
        "Automatic P2OASys screening draft for expert review. "
        "Process / Life Cycle left blank (human-only). "
        "Numeric units are measured/predicted inputs; phrase/GHS/list units are 2-10 scores."
    )

    # Header block: 4 columns (Category, Sub Category, Units, chemical)
    header = [
        ["", "", "", chem],
        ["Cas #", "", "", cas],
        ["SDS Source", "", "", sds_source],
        ["SDS Source URL", "", "", sds_url],
        ["SDS Year", "", "", sds_year],
        ["Assessment Information", "", "", note],
        ["", "", "", ""],
        ["Category", "Sub Category", "Units", chem],
    ]

    filled = 0
    skipped_human = 0
    out_rows = list(header)
    last_value = ""
    last_trace: dict[str, Any] | None = None
    last_skip_human = False

    for raw in rows[8:]:
        cat = (raw[0] if len(raw) > 0 else "") or ""
        sub = (raw[1] if len(raw) > 1 else "") or ""
        unit = (raw[2] if len(raw) > 2 else "") or ""
        cat, sub, unit = cat.strip(), sub.strip(), unit.strip()
        if not cat and not sub and not unit:
            out_rows.append(["", "", "", ""])
            continue

        is_memo = unit.lower().startswith("memo for")
        cat_n = _alias_cat(cat)
        human_only = cat_n in AUTO_SKIP_CATEGORIES or _norm(cat) in AUTO_SKIP_CATEGORIES

        if cat.strip() == "Environmental Certification":
            out_rows.append([cat, sub, unit, "false"])
            continue

        if is_memo:
            if last_skip_human or human_only:
                skipped_human += 1
                out_rows.append([cat, sub, unit, ""])
            elif last_value == "":
                out_rows.append([cat, sub, unit, ""])
            else:
                out_rows.append([cat, sub, unit, _memo_text(last_trace)])
            continue

        last_skip_human = human_only
        last_value = ""
        last_trace = None
        if human_only:
            skipped_human += 1
            out_rows.append([cat, sub, unit, ""])
            continue

        val, tr = _lookup(cat, sub, unit, scores_ix, trace_ix)
        last_trace = tr
        last_value = _fmt_cell(val)
        if last_value:
            filled += 1
        out_rows.append([cat, sub, unit, last_value])

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_ALL)
    writer.writerows(out_rows)
    text = buf.getvalue()
    # coverage line is already in assessment note; stash stats on function attribute for UI
    build_single_cas_upload_csv.last_stats = {  # type: ignore[attr-defined]
        "filled_units": filled,
        "human_only_rows_blanked": skipped_human,
        "data_rows": len(out_rows) - 8,
        "name": chem,
        "cas": cas,
    }
    return text


def upload_csv_stats() -> dict[str, Any]:
    return dict(getattr(build_single_cas_upload_csv, "last_stats", {}) or {})
