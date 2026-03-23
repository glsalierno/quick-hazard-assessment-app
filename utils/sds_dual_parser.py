"""
Dual SDS parser (A + B) with PubChem name-CAS validation.

- Parser A: SDSParser (pypdf + sds_regex_extractor + optional Docling)
- Parser B: extract_sds_for_llm (pypdf + pdfplumber fallback + regex)
- Name validation: PubChem synonyms vs SDS chemical_name when available
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

# Ensure we can import extract_sds_for_llm from sds examples (sibling of quick-hazard-assessment-app)
_APP_ROOT = Path(__file__).resolve().parent.parent
_SDS_EXAMPLES = Path(os.environ.get("SDS_EXAMPLES_DIR", str(_APP_ROOT / ".." / "sds examples"))).resolve()
if _SDS_EXAMPLES.exists() and str(_SDS_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_SDS_EXAMPLES))


def _run_parser_a(pdf_bytes: bytes) -> tuple[set[str], dict[str, dict[str, Any]]]:
    """Parser A: SDSParser. Returns (cas_set, cas_to_row)."""
    from utils.sds_parser import get_sds_parser

    parser = get_sds_parser()
    result = parser.parse_pdf(pdf_bytes)
    cas_set: set[str] = set()
    cas_to_row: dict[str, dict[str, Any]] = {}

    if not result or not result.cas_numbers:
        return cas_set, cas_to_row

    for ext in result.cas_numbers:
        c = (ext.cas or "").strip()
        if not c:
            continue
        cas_set.add(c)
        if c not in cas_to_row or (ext.chemical_name or ext.concentration):
            sections = ext.sections if ext.sections else ([ext.section] if ext.section is not None else [])
        cas_to_row[c] = {
                "cas": c,
                "chemical_name": (ext.chemical_name or "").strip() or None,
                "concentration": (ext.concentration or "").strip() or None,
                "section": ext.section,
                "sections": sections,
                "method": ext.method or "parser_a",
                "confidence": ext.confidence,
                "validated": ext.validated,
            }

    return cas_set, cas_to_row


def _run_parser_b(pdf_bytes: bytes) -> set[str]:
    """Parser B: extract_sds_for_llm. Returns cas_set (no per-CAS metadata from B)."""
    try:
        from extract_sds_for_llm import parse_sds
    except ImportError:
        return set()

    fd, path = tempfile.mkstemp(suffix=".pdf")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(pdf_bytes)
        r = parse_sds(path)
        cas_list = r.get("cas_numbers") or []
        return {c.strip() for c in cas_list if c and str(c).strip()}
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _is_valid_cas_format(cas: str) -> bool:
    """Quick format check for CAS."""
    from utils import cas_validator

    norm = cas_validator.normalize_cas_input(cas)
    return bool(norm and cas_validator.is_valid_cas_format(norm))


def _validate_cas_via_name(cas: str, sds_chemical_name: str) -> tuple[bool, Optional[str]]:
    """
    Cross-validate CAS against SDS chemical name via PubChem.
    Returns (match: bool, pubchem_name_or_none).
    """
    if not (cas and sds_chemical_name) or len(sds_chemical_name.strip()) < 2:
        return False, None

    try:
        from utils import pubchem_client
    except ImportError:
        return False, None

    comp_data = pubchem_client.get_compound_data(cas, input_type="cas")
    if not comp_data:
        return False, None

    iupac = (comp_data.get("iupac_name") or "").strip()
    names_to_check = [n for n in [iupac] if n]

    try:
        import pubchempy as pcp

        cid = comp_data.get("cid")
        if cid:
            comp = pcp.Compound.from_cid(cid)
            syns = getattr(comp, "synonyms", None) or []
            names_to_check.extend(str(s)[:200] for s in (syns or [])[:100])
    except Exception:
        pass

    sds_norm = re.sub(r"\s+", " ", sds_chemical_name.strip().lower())
    sds_tokens = set(re.split(r"[\s,;()\-]+", sds_norm)) - {""}

    for db_name in names_to_check:
        if not db_name or len(db_name) < 2:
            continue
        db_norm = re.sub(r"\s+", " ", db_name.strip().lower())
        if sds_norm in db_norm or db_norm in sds_norm:
            return True, db_name
        db_tokens = set(re.split(r"[\s,;()\-]+", db_norm)) - {""}
        overlap = len(sds_tokens & db_tokens) / max(len(sds_tokens), 1)
        if overlap >= 0.6:
            return True, db_name

    return False, (iupac or (names_to_check[0] if names_to_check else None))


def merge_and_cross_reference(
    pdf_bytes: bytes,
    *,
    use_name_validation: bool = True,
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Run both parsers, merge CAS, cross-reference with DB and optional name validation.
    Returns (cas_list_sorted, extraction_rows).
    """
    cas_a, rows_a = _run_parser_a(pdf_bytes)
    cas_b = _run_parser_b(pdf_bytes)

    all_cas = (cas_a | cas_b)
    all_cas = {c for c in all_cas if _is_valid_cas_format(c)}

    rows: list[dict[str, Any]] = []
    for cas in all_cas:
        in_a = cas in cas_a
        in_b = cas in cas_b
        source = "both" if (in_a and in_b) else ("parser_a" if in_a else "parser_b")

        base = rows_a.get(cas, {"cas": cas, "chemical_name": None, "concentration": None, "sections": []})
        chem_name = base.get("chemical_name") or ""
        concentration = base.get("concentration") or ""
        sections = base.get("sections") or []

        name_validated = False
        pubchem_name: Optional[str] = None
        if use_name_validation and chem_name:
            name_validated, pubchem_name = _validate_cas_via_name(cas, chem_name)

        rows.append({
            "cas": cas,
            "chemical_name": chem_name or "",
            "concentration": concentration or "",
            "section": base.get("section"),
            "sections": sections,
            "method": base.get("method", source),
            "confidence": base.get("confidence"),
            "validated": base.get("validated", False),
            "source": source,
            "name_validated": name_validated if chem_name else None,
            "pubchem_name": pubchem_name or "",
        })

    def sort_key(r: dict) -> tuple:
        name_ok = 0 if r.get("name_validated") else 1
        src = 0 if r.get("source") == "both" else (1 if "parser_a" in str(r.get("source", "")) else 2)
        return (name_ok, src, (r.get("cas") or ""))

    rows.sort(key=sort_key)
    cas_list = [r["cas"] for r in rows]

    return cas_list, rows
