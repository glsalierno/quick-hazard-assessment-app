"""
QA / evidence integrity helpers for GHhaz6 reports.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from utils import ghs_formatter


def _ghs_codes_from_result(result_data: dict[str, Any]) -> tuple[list[str], list[str]]:
    pub = result_data.get("pubchem") or {}
    ghs = pub.get("ghs") or {}
    return list(ghs.get("h_codes") or []), list(ghs.get("p_codes") or [])


def check_ghs_completeness(result_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Return list of QA issue dicts for GHS completeness."""
    h_codes, p_codes = _ghs_codes_from_result(result_data)
    acc = ghs_formatter.account_ghs_codes(h_codes, p_codes)
    issues: list[dict[str, Any]] = []
    if not acc["h_complete"]:
        issues.append(
            {
                "severity": "error",
                "code": "ghs_h_incomplete",
                "message": "Retrieved H-codes are not fully partitioned into displayed/unmapped/suppressed.",
                "detail": acc,
            }
        )
    if not acc["p_complete"]:
        issues.append(
            {
                "severity": "error",
                "code": "ghs_p_incomplete",
                "message": "Retrieved P-codes are not fully partitioned into displayed/unmapped/suppressed.",
            }
        )
    if acc["unmapped_h_codes"]:
        issues.append(
            {
                "severity": "warning",
                "code": "ghs_h_unmapped",
                "message": f"Unmapped H-codes (no lexicon phrase): {', '.join(acc['unmapped_h_codes'])}",
            }
        )
    if acc["unmapped_p_codes"]:
        issues.append(
            {
                "severity": "warning",
                "code": "ghs_p_unmapped",
                "message": f"Unmapped P-codes (no lexicon phrase): {', '.join(acc['unmapped_p_codes'])}",
            }
        )
    if acc["suppressed_or_subsumed_h_codes"]:
        labels = [
            f"{x['code']}⊂{x['primary']}" for x in acc["suppressed_or_subsumed_h_codes"]
        ]
        issues.append(
            {
                "severity": "info",
                "code": "ghs_h_subsumed",
                "message": f"Subsumed/suppressed H-codes (shown with reason): {', '.join(labels)}",
            }
        )
    return issues


def _codes_from_csv_payload(csv_payload: Any) -> set[str]:
    """Extract H-codes from CSV bytes/str or download helper output."""
    text = ""
    if isinstance(csv_payload, bytes):
        text = csv_payload.decode("utf-8", errors="replace")
    elif isinstance(csv_payload, str):
        text = csv_payload
    else:
        return set()
    codes: set[str] = set()
    # Header row often has GHS_H column with pipe-joined codes
    for m in re.finditer(r"\bH\d{3}(?:\+\d+)?\b", text):
        codes.add(m.group(0))
    return codes


def _codes_from_json_payload(json_payload: Any) -> set[str]:
    if not isinstance(json_payload, dict):
        return set()
    ghs = json_payload.get("ghs") or {}
    return {str(c).strip() for c in (ghs.get("h_codes") or []) if str(c).strip()}


def check_export_parity(
    result_data: dict[str, Any],
    csv_payload: Any = None,
    json_payload: Any = None,
    p2oasys_payload: Any = None,
) -> list[dict[str, Any]]:
    """Compare retrieved H-codes to export payloads when provided."""
    h_codes, _p = _ghs_codes_from_result(result_data)
    retrieved = set(h_codes)
    issues: list[dict[str, Any]] = []

    if csv_payload is not None:
        csv_codes = _codes_from_csv_payload(csv_payload)
        # CSV may only include codes that appear in the summary GHS_H field
        missing = retrieved - csv_codes
        if missing and retrieved:
            # Only warn if CSV has a GHS_H field at all
            text = (
                csv_payload.decode("utf-8", errors="replace")
                if isinstance(csv_payload, bytes)
                else str(csv_payload)
            )
            if "GHS_H" in text:
                issues.append(
                    {
                        "severity": "warning",
                        "code": "export_csv_ghs_mismatch",
                        "message": f"CSV GHS_H missing codes present in result_data: {sorted(missing)}",
                    }
                )

    if json_payload is not None:
        jcodes = _codes_from_json_payload(json_payload)
        if jcodes != retrieved:
            issues.append(
                {
                    "severity": "warning",
                    "code": "export_json_ghs_mismatch",
                    "message": (
                        f"JSON ghs.h_codes ({sorted(jcodes)}) ≠ retrieved "
                        f"({sorted(retrieved)})"
                    ),
                }
            )

    if p2oasys_payload is not None:
        p2_codes: set[str] = set()
        if isinstance(p2oasys_payload, dict):
            # hazard_data may nest GHS under various keys
            for key in ("ghs_h_codes", "h_codes", "hazard_codes"):
                vals = p2oasys_payload.get(key)
                if isinstance(vals, list):
                    p2_codes |= {str(c).strip() for c in vals if str(c).strip()}
            hm = p2oasys_payload.get("hazard_metrics") or {}
            if isinstance(hm, dict):
                for c in hm.get("ghs_h_codes") or hm.get("h_codes") or []:
                    if str(c).strip():
                        p2_codes.add(str(c).strip())
            tox = p2oasys_payload.get("toxicities") or []
            if isinstance(tox, list):
                for t in tox:
                    if isinstance(t, dict):
                        for c in re.findall(r"\bH\d{3}\b", str(t.get("value") or "")):
                            p2_codes.add(c)
        if p2_codes and retrieved and not retrieved.issubset(p2_codes | set()):
            missing = retrieved - p2_codes
            if missing:
                issues.append(
                    {
                        "severity": "info",
                        "code": "p2oasys_ghs_subset",
                        "message": (
                            f"P2OASys payload may not include all retrieved H-codes: "
                            f"{sorted(missing)}"
                        ),
                    }
                )

    return issues


def build_qa_status(
    result_data: dict[str, Any],
    *,
    csv_payload: Any = None,
    json_payload: Any = None,
    p2oasys_payload: Any = None,
) -> dict[str, Any]:
    """Aggregate GHS accounting + issues for Streamlit QA expander."""
    h_codes, p_codes = _ghs_codes_from_result(result_data)
    accounting = ghs_formatter.account_ghs_codes(h_codes, p_codes)
    issues = check_ghs_completeness(result_data)
    issues.extend(
        check_export_parity(
            result_data,
            csv_payload=csv_payload,
            json_payload=json_payload,
            p2oasys_payload=p2oasys_payload,
        )
    )

    # Ecotox contamination hints from already-parsed entries
    eco = (result_data.get("pubchem") or {}).get("ecotoxicity") or {}
    contaminated = []
    bad_markers = (
        "flammable liquid",
        "skin corrosion",
        "serious eye damage",
        "category 1",
        "category 4",
    )
    for e in eco.get("entries") or []:
        blob = f"{e.get('value') or ''} {e.get('conditions') or ''}".lower()
        if any(m in blob for m in bad_markers) and not e.get("value_num"):
            contaminated.append((e.get("value") or "")[:120])
    if contaminated:
        issues.append(
            {
                "severity": "warning",
                "code": "ecotox_contamination_suspect",
                "message": (
                    "Ecotoxicity entries still look like general GHS classification text; "
                    f"sample: {contaminated[0]!r}"
                ),
            }
        )

    p2_count: Optional[int] = None
    if isinstance(p2oasys_payload, dict):
        for key in ("ghs_h_codes", "h_codes"):
            vals = p2oasys_payload.get(key)
            if isinstance(vals, list):
                p2_count = len(vals)
                break

    warnings = [i for i in issues if i.get("severity") in ("warning", "error")]
    return {
        "accounting": accounting,
        "issues": issues,
        "retrieved_h_count": len(accounting["retrieved_h_codes"]),
        "displayed_h_count": len(accounting["displayed_h_codes"]),
        "unmapped_h_codes": accounting["unmapped_h_codes"],
        "suppressed_or_subsumed_h_codes": accounting["suppressed_or_subsumed_h_codes"],
        "retrieved_p_count": len(accounting["retrieved_p_codes"]),
        "displayed_p_count": len(accounting["displayed_p_codes"]),
        "p2oasys_input_code_count": p2_count,
        "warnings": warnings,
        "ok": not any(i.get("severity") == "error" for i in issues),
    }
