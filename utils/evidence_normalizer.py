"""
Normalized evidence records for QA / audit (GHhaz6).

Does not replace existing pipelines; consumers (qa_checks, UI) can build lists of
these dicts from ``result_data`` for evidence integrity summaries.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

PARSER_VERSION = "evidence_normalizer_v6"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_evidence_record(
    *,
    cas: Optional[str] = None,
    dtxsid: Optional[str] = None,
    cid: Optional[str | int] = None,
    source: str,
    source_section: Optional[str] = None,
    retrieved_at: Optional[str] = None,
    parser_version: str = PARSER_VERSION,
    evidence_type: str,
    raw_text: Optional[str] = None,
    normalized_code: Optional[str] = None,
    normalized_phrase: Optional[str] = None,
    value: float | str | None = None,
    unit: Optional[str] = None,
    species: Optional[str] = None,
    route: Optional[str] = None,
    flags: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Build one normalized evidence record with the v6 schema."""
    return {
        "cas": cas,
        "dtxsid": dtxsid,
        "cid": cid,
        "source": source,
        "source_section": source_section,
        "retrieved_at": retrieved_at or _now_iso(),
        "parser_version": parser_version,
        "evidence_type": evidence_type,
        "raw_text": raw_text,
        "normalized_code": normalized_code,
        "normalized_phrase": normalized_phrase,
        "value": value,
        "unit": unit,
        "species": species,
        "route": route,
        "flags": list(flags or []),
    }


def evidence_from_ghs_accounting(
    result_data: dict[str, Any],
    accounting: dict[str, Any],
) -> list[dict[str, Any]]:
    """Convert GHS accounting buckets into evidence records."""
    cas = result_data.get("clean_cas")
    dtxsid = result_data.get("dtxsid")
    pub = result_data.get("pubchem") or {}
    cid = pub.get("cid")
    retrieved_at = pub.get("retrieved_at")
    out: list[dict[str, Any]] = []
    for code, phrase in (accounting.get("displayed_h_phrases") or {}).items():
        out.append(
            make_evidence_record(
                cas=cas,
                dtxsid=dtxsid,
                cid=cid,
                source="PubChem",
                source_section="GHS",
                retrieved_at=retrieved_at,
                parser_version="ghs_formatter_v6",
                evidence_type="ghs_code",
                raw_text=code,
                normalized_code=code,
                normalized_phrase=phrase,
                flags=[],
            )
        )
    for code in accounting.get("unmapped_h_codes") or []:
        out.append(
            make_evidence_record(
                cas=cas,
                dtxsid=dtxsid,
                cid=cid,
                source="PubChem",
                source_section="GHS",
                retrieved_at=retrieved_at,
                parser_version="ghs_formatter_v6",
                evidence_type="ghs_code",
                raw_text=code,
                normalized_code=code,
                normalized_phrase=None,
                flags=["unmapped_phrase"],
            )
        )
    for item in accounting.get("suppressed_or_subsumed_h_codes") or []:
        code = item.get("code") if isinstance(item, dict) else str(item)
        reason = item.get("reason") if isinstance(item, dict) else "suppressed"
        out.append(
            make_evidence_record(
                cas=cas,
                dtxsid=dtxsid,
                cid=cid,
                source="PubChem",
                source_section="GHS",
                retrieved_at=retrieved_at,
                parser_version="ghs_formatter_v6",
                evidence_type="ghs_code",
                raw_text=code,
                normalized_code=code,
                normalized_phrase=item.get("phrase") if isinstance(item, dict) else None,
                flags=[f"subsumed:{reason}"],
            )
        )
    return out
