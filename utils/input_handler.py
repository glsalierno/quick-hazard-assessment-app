"""
Unified chemical input: typed CAS/name or SDS PDF -> normalized identifiers for assessment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import streamlit as st

from utils import cas_validator


@dataclass
class ChemicalInput:
    """Result of parsing user input before database assessment."""

    input_type: str  # "cas" | "name" | "sds_single" | "sds_multi"
    primary: str  # string passed to existing pipeline (CAS or name)
    cas_numbers: list[str] = field(default_factory=list)
    source_label: Optional[str] = None
    extraction_rows: list[dict[str, Any]] = field(default_factory=list)

    def has_multiple_cas(self) -> bool:
        return len(self.cas_numbers) > 1


class UnifiedInputHandler:
    """Routes text or uploaded PDF to a ChemicalInput."""

    def __init__(self) -> None:
        from utils.sds_parser import get_sds_parser

        self._sds_parser = get_sds_parser()

    def process_text(self, text: str) -> Optional[ChemicalInput]:
        """Typed CAS or chemical name (same rules as main form)."""
        if not text or not str(text).strip():
            return None
        raw = str(text).strip()
        norm = cas_validator.normalize_cas_input(raw)
        if not norm:
            return None
        if cas_validator.is_valid_cas_format(norm):
            return ChemicalInput(
                input_type="cas",
                primary=norm,
                cas_numbers=[norm],
                source_label="typed",
            )
        return ChemicalInput(
            input_type="name",
            primary=norm,
            cas_numbers=[],
            source_label="typed_name",
        )

    def process_sds_pdf(self, uploaded_file: Any) -> Optional[ChemicalInput]:
        """
        Extract CAS list from SDS using unified parser.
        uploaded_file: Streamlit UploadedFile with .name and .getvalue().
        """
        if uploaded_file is None:
            return None
        pdf_bytes = uploaded_file.getvalue()
        if not pdf_bytes:
            return None
        parse_result = self._sds_parser.parse_pdf(pdf_bytes)
        if not parse_result or not parse_result.cas_numbers:
            return ChemicalInput(
                input_type="sds_single",
                primary="",
                cas_numbers=[],
                source_label=getattr(uploaded_file, "name", None) or "SDS.pdf",
                extraction_rows=[],
            )

        rows: list[dict[str, Any]] = []
        cas_list: list[str] = []
        seen: set[str] = set()
        best: tuple[float, str] = (-1.0, "")
        for ext in parse_result.cas_numbers:
            c = (ext.cas or "").strip()
            if not c or c in seen:
                continue
            seen.add(c)
            cas_list.append(c)
            conf = float(ext.confidence) if ext.confidence is not None else 0.0
            if conf > best[0]:
                best = (conf, c)
            rows.append(
                {
                    "cas": c,
                    "chemical_name": ext.chemical_name or "",
                    "concentration": ext.concentration or "",
                    "section": ext.section,
                    "method": ext.method,
                    "confidence": ext.confidence,
                    "validated": ext.validated,
                    "context": ext.context or "",
                    "warnings": ", ".join(ext.warnings) if ext.warnings else "",
                }
            )

        primary = best[1] if best[1] else cas_list[0]
        in_type = "sds_multi" if len(cas_list) > 1 else "sds_single"
        return ChemicalInput(
            input_type=in_type,
            primary=primary,
            cas_numbers=cas_list,
            source_label=getattr(uploaded_file, "name", None) or "SDS.pdf",
            extraction_rows=rows,
        )


@st.cache_resource
def get_input_handler() -> UnifiedInputHandler:
    return UnifiedInputHandler()
