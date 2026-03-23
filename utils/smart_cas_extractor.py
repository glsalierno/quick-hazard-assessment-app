"""
Smart CAS Number Extractor for Safety Data Sheets.
Combines rule-based extraction with optional local Hugging Face models.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils import sds_pdf_utils

logger = logging.getLogger(__name__)


@dataclass
class CASExtractionResult:
    """Structured result for CAS extraction."""

    cas: str
    chemical_name: Optional[str] = None
    concentration: Optional[str] = None
    source_section: Optional[int] = None
    extraction_method: str = "regex"
    confidence: str = "medium"
    context: Optional[str] = None
    validation_status: bool = False
    warnings: List[str] = field(default_factory=list)


def _get_hf_model_manager():
    """Lazy import to avoid loading HF when not used."""
    try:
        from utils.hf_model_manager import HFModelManager
        return HFModelManager()
    except ImportError:
        return None


class SmartCASExtractor:
    """
    Multi-method CAS extractor with progressive fallback.
    Tries increasingly sophisticated methods until success.
    """

    def __init__(self, use_llm: bool = True, model_manager: Any = None) -> None:
        self.use_llm = use_llm
        self.model_manager = model_manager if model_manager is not None else _get_hf_model_manager()
        if self.model_manager is None:
            self.use_llm = False
        self.cas_pattern = re.compile(r"\b(\d{1,7})-(\d{2})-(\d)\b")
        self.extraction_methods: List[Any] = [
            self.extract_from_section_3_table,
            self.extract_from_section_3_text,
            self.extract_from_section_1,
            self.extract_from_section_15,
            self.extract_from_any_section,
            self.extract_with_llm,
        ]

    def _extract_section(self, text: str, section_num: int) -> Optional[str]:
        """Extract specific section body from SDS text."""
        if not text:
            return None
        start_patterns = [
            rf"(?:^|\n)(?:\d+\.?\s*)?Section\s*{section_num}\b[^\n]*(?:\n(?!\s*(?:\d+\.?\s*)?Section\s*\d+\b).*)*",
            rf"(?:^|\n){section_num}\.\s*[^\n]+(?:\n(?!\s*\d+\.\s)[^\n]*)*",
        ]
        for pat in start_patterns:
            m = re.search(pat, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            if m:
                block = m.group(0).strip()
                next_sec = re.search(
                    rf"\n\s*(?:\d+\.?\s*)?Section\s*({section_num + 1}|\d+)\b",
                    text[m.end() : m.end() + 2000],
                    re.IGNORECASE,
                )
                if next_sec:
                    end = m.end() + next_sec.start()
                    block = text[m.start() : end].strip()
                return block
        start_re = re.compile(
            rf"(?:Section\s*{section_num}\b|{section_num}\.\s)", re.IGNORECASE
        )
        end_re = re.compile(
            rf"\bSection\s*{section_num + 1}\b|^\s*{section_num + 1}\.\s",
            re.IGNORECASE | re.MULTILINE,
        )
        m_start = start_re.search(text)
        if not m_start:
            return None
        start = m_start.end()
        m_end = end_re.search(text[start:])
        end = start + m_end.start() if m_end else len(text)
        return text[start:end].strip() or None

    def _clean_cas(self, cas_str: Optional[str]) -> Optional[str]:
        """Clean and normalize CAS string."""
        if not cas_str:
            return None
        s = str(cas_str).strip()
        for prefix in ("CAS", "CAS#", "CAS No", "CAS Number", "CASRN", "EC", "EINECS"):
            if s.upper().startswith(prefix.upper()):
                s = s[len(prefix) :].strip().lstrip(":#")
        m = re.search(r"(\d{1,7}-\d{2}-\d)", s)
        return m.group(1) if m else None

    def _validate_cas_checksum(self, cas: str) -> bool:
        """Validate CAS checksum digit (digits from right × 1,2,3... sum mod 10)."""
        if not cas or not re.match(r"^\d{1,7}-\d{2}-\d$", cas.strip()):
            return False
        try:
            parts = cas.strip().split("-")
            main = parts[0] + parts[1]
            check_digit = int(parts[2])
            total = sum(int(d) * (i + 1) for i, d in enumerate(reversed(main)))
            return total % 10 == check_digit
        except Exception:
            return False

    def _extract_concentration(self, row: List[Any]) -> Optional[str]:
        """Extract concentration from table row."""
        for cell in row:
            s = str(cell)
            m = re.search(r"(\d+(?:\.\d+)?)\s*[%％]", s)
            if m:
                return m.group(0)
            m2 = re.search(r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*[%％]?", s)
            if m2:
                return f"{m2.group(1)}-{m2.group(2)}%"
        return None

    def _extract_concentration_from_context(self, context: str) -> Optional[str]:
        """Extract concentration from surrounding text."""
        m = re.search(r"(\d+(?:\.\d+)?)\s*[%％]", context)
        if m:
            return m.group(0)
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*[%％]?", context)
        if m2:
            return f"{m2.group(1)}-{m2.group(2)}%"
        return None

    def _extract_chemical_from_context(self, context: str, cas: str) -> Optional[str]:
        """Try to extract chemical name from context."""
        before = context.split(cas)[0] if cas in context else ""
        m = re.search(r"([A-Z][A-Za-z0-9\s\-/]+?)\s*\(?\s*" + re.escape(cas), context)
        if m:
            return m.group(1).strip()
        words = before.split()
        if words:
            candidate = " ".join(words[-3:]).strip()
            if len(candidate) > 3 and not re.search(r"\d", candidate):
                return candidate
        return None

    def _identify_section_from_context(self, context: str, full_text: str) -> Optional[int]:
        """Try to determine which section the context came from."""
        for n in range(1, 17):
            if f"Section {n}" in context or (context[:50] and f"{n}." in context[:50]):
                return n
        pos = full_text.find(context[:50]) if len(context) >= 50 else -1
        if pos > 0:
            preceding = full_text[max(0, pos - 200) : pos]
            for n in range(1, 17):
                if f"Section {n}" in preceding or f"{n}." in preceding[-50:]:
                    return n
        return None

    def extract_from_section_3_table(self, text: str) -> List[CASExtractionResult]:
        """Extract CAS from Section 3 composition tables."""
        results: List[CASExtractionResult] = []
        section_3 = self._extract_section(text, 3)
        if not section_3:
            return results
        tables = sds_pdf_utils.extract_tables_from_text(section_3)
        for table in tables:
            if not table or len(table) < 2:
                continue
            header = table[0]
            cas_col_idx: Optional[int] = None
            chem_col_idx = 0
            for i, cell in enumerate(header):
                cell_lower = str(cell).lower()
                if any(t in cell_lower for t in ["cas", "cas no", "cas #", "registry"]):
                    cas_col_idx = i
                elif any(
                    t in cell_lower
                    for t in ["component", "chemical", "substance", "ingredient"]
                ):
                    chem_col_idx = i
            if cas_col_idx is None:
                continue
            for row in table[1:]:
                if len(row) <= max(cas_col_idx, chem_col_idx):
                    continue
                cas_cell = str(row[cas_col_idx]).strip()
                cas = self._clean_cas(cas_cell)
                if cas and self._validate_cas_checksum(cas):
                    chemical = (
                        str(row[chem_col_idx]).strip()
                        if chem_col_idx < len(row)
                        else None
                    )
                    concentration = self._extract_concentration(row)
                    results.append(
                        CASExtractionResult(
                            cas=cas,
                            chemical_name=chemical,
                            concentration=concentration,
                            source_section=3,
                            extraction_method="table",
                            confidence="high",
                            validation_status=True,
                            context="Table row: " + " | ".join(str(c) for c in row),
                        )
                    )
        return results

    def extract_from_section_3_text(self, text: str) -> List[CASExtractionResult]:
        """Extract CAS from Section 3 narrative text."""
        results: List[CASExtractionResult] = []
        section_3 = self._extract_section(text, 3)
        if not section_3:
            return results
        pattern1 = r"([A-Za-z][A-Za-z0-9\s\-/]+?)\s*\(?\s*CAS(?:\s*No\.?)?:?\s*(\d{1,7}-\d{2}-\d)\s*\)?"
        for m in re.finditer(pattern1, section_3, re.IGNORECASE):
            chemical = m.group(1).strip()
            cas = self._clean_cas(m.group(2))
            if cas and self._validate_cas_checksum(cas):
                start = max(0, m.start() - 50)
                end = min(len(section_3), m.end() + 50)
                context = section_3[start:end]
                concentration = self._extract_concentration_from_context(context)
                results.append(
                    CASExtractionResult(
                        cas=cas,
                        chemical_name=chemical,
                        concentration=concentration,
                        source_section=3,
                        extraction_method="regex_pattern",
                        confidence="high",
                        validation_status=True,
                        context=context,
                    )
                )
        pattern2 = r"([^,\n]+),\s*(\d{1,7}-\d{2}-\d),\s*(\d+(?:\.\d+)?)\s*[%％]"
        for m in re.finditer(pattern2, section_3):
            chemical = m.group(1).strip()
            cas = self._clean_cas(m.group(2))
            concentration = m.group(3) + "%"
            if cas and self._validate_cas_checksum(cas):
                results.append(
                    CASExtractionResult(
                        cas=cas,
                        chemical_name=chemical,
                        concentration=concentration,
                        source_section=3,
                        extraction_method="regex_pattern",
                        confidence="high",
                        validation_status=True,
                    )
                )
        return results

    def extract_from_section_1(self, text: str) -> List[CASExtractionResult]:
        """Extract CAS from Section 1 (Identification)."""
        results: List[CASExtractionResult] = []
        section_1 = self._extract_section(text, 1)
        if not section_1:
            return results
        patterns = [
            r"CAS(?:\s*No\.?|\s*Number)?:?\s*(\d{1,7}-\d{2}-\d)",
            r"CAS\s*#\s*(\d{1,7}-\d{2}-\d)",
            r"Registry\s*Number:?\s*(\d{1,7}-\d{2}-\d)",
            r"EINECS:?\s*(\d{1,7}-\d{2}-\d)",
        ]
        product_name: Optional[str] = None
        name_m = re.search(r"^([^\n]+)", section_1.strip())
        if name_m:
            product_name = name_m.group(1).strip()
        for pattern in patterns:
            for cas in re.findall(pattern, section_1, re.IGNORECASE):
                cas = self._clean_cas(cas)
                if cas and self._validate_cas_checksum(cas):
                    results.append(
                        CASExtractionResult(
                            cas=cas,
                            chemical_name=product_name,
                            source_section=1,
                            extraction_method="regex_section1",
                            confidence="medium",
                            validation_status=True,
                            context="Section 1: " + section_1[:200],
                        )
                    )
        return results

    def extract_from_section_15(self, text: str) -> List[CASExtractionResult]:
        """Extract CAS from Section 15 regulatory tables."""
        results: List[CASExtractionResult] = []
        section_15 = self._extract_section(text, 15)
        if not section_15:
            return results
        tables = sds_pdf_utils.extract_tables_from_text(section_15)
        keywords = ["TSCA", "DSL", "EINECS", "ELINCS", "IECSC", "KECL", "PICCS"]
        for table in tables:
            if not table or len(table) < 2:
                continue
            header_text = " ".join(str(c) for c in table[0]).upper()
            if not any(k in header_text for k in keywords):
                continue
            header = table[0]
            cas_col_idx = None
            chem_col_idx = 0
            for i, cell in enumerate(header):
                cstr = str(cell).upper()
                if "CAS" in cstr:
                    cas_col_idx = i
                elif "COMPONENT" in cstr or "SUBSTANCE" in cstr or "CHEMICAL" in cstr:
                    chem_col_idx = i
            if cas_col_idx is None:
                continue
            for row in table[1:]:
                if len(row) <= max(cas_col_idx, chem_col_idx):
                    continue
                cas_cell = str(row[cas_col_idx]).strip()
                cas = self._clean_cas(cas_cell)
                if cas and self._validate_cas_checksum(cas):
                    chemical = (
                        str(row[chem_col_idx]).strip()
                        if chem_col_idx < len(row)
                        else None
                    )
                    results.append(
                        CASExtractionResult(
                            cas=cas,
                            chemical_name=chemical,
                            source_section=15,
                            extraction_method="regulatory_table",
                            confidence="medium",
                            validation_status=True,
                            context="Regulatory table: "
                            + " | ".join(str(c) for c in row),
                        )
                    )
        return results

    def extract_from_any_section(self, text: str) -> List[CASExtractionResult]:
        """Fallback: find any CAS-like pattern in the document."""
        results: List[CASExtractionResult] = []
        seen: set[str] = set()
        for m in self.cas_pattern.finditer(text):
            cas = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            if cas in seen:
                continue
            seen.add(cas)
            valid = self._validate_cas_checksum(cas)
            ctx_pat = f".{{0,100}}{re.escape(cas)}.{{0,100}}"
            ctx_m = re.search(ctx_pat, text, re.DOTALL)
            context = ctx_m.group(0) if ctx_m else ""
            section = self._identify_section_from_context(context, text)
            chemical = self._extract_chemical_from_context(context, cas)
            results.append(
                CASExtractionResult(
                    cas=cas,
                    chemical_name=chemical,
                    source_section=section,
                    extraction_method="regex_fallback",
                    confidence="low" if not valid else "medium",
                    validation_status=valid,
                    context=context[:200],
                    warnings=(["Checksum validation failed"] if not valid else []),
                )
            )
        return results

    def extract_with_llm(self, text: str) -> List[CASExtractionResult]:
        """Use local HF model for ambiguous cases (only if use_llm and model available)."""
        if not self.use_llm or self.model_manager is None or not self.model_manager.is_available():
            return []
        results: List[CASExtractionResult] = []
        sections_to_check: List[str] = []
        for num in [3, 1, 15]:
            sec = self._extract_section(text, num)
            if sec:
                sections_to_check.append(f"Section {num}:\n{sec[:500]}")
        if not sections_to_check:
            return []
        context = "\n\n".join(sections_to_check)
        prompt = f"""Extract all Chemical Abstracts Service (CAS) registry numbers from this Safety Data Sheet.
CAS numbers are in format: [up to 7 digits]-[2 digits]-[1 digit] (e.g., 124-09-4)
For each CAS number, also identify: the chemical name, the section where it was found, any concentration information.
Return ONLY a JSON array with this exact format:
[
  {{"cas": "124-09-4", "chemical": "Hexamethylenediamine", "section": 3, "concentration": "60%"}},
  {{"cas": "7732-18-5", "chemical": "Water", "section": 3, "concentration": "40%"}}
]
Text to analyze:
{context}
JSON:"""
        try:
            response = self.model_manager.generate(prompt)
            json_m = re.search(r"\[.*\]", response, re.DOTALL)
            if json_m:
                items = json.loads(json_m.group())
                for item in items:
                    cas = self._clean_cas(item.get("cas", ""))
                    if not cas or not self._validate_cas_checksum(cas):
                        continue
                    results.append(
                        CASExtractionResult(
                            cas=cas,
                            chemical_name=item.get("chemical"),
                            concentration=item.get("concentration"),
                            source_section=item.get("section"),
                            extraction_method="llm",
                            confidence="medium",
                            validation_status=True,
                            context="LLM extracted from section "
                            + str(item.get("section", "")),
                        )
                    )
        except Exception as e:
            logger.error("LLM extraction failed: %s", e)
        return results

    def extract_all_cas(
        self, pdf_text: str, prefer_llm: bool = False
    ) -> Dict[str, Any]:
        """Main pipeline: try all methods, deduplicate by CAS, keep highest confidence."""
        all_results: List[CASExtractionResult] = []
        methods_used: set[str] = set()
        order = self.extraction_methods
        if prefer_llm and self.extract_with_llm in order:
            order = [self.extract_with_llm] + [m for m in order if m != self.extract_with_llm]
        for method in order:
            try:
                res = method(pdf_text)
                if res:
                    name = method.__name__.replace("extract_", "")
                    methods_used.add(name)
                    all_results.extend(res)
                    logger.info("Method %s found %s CAS numbers", name, len(res))
            except Exception as e:
                logger.warning("Method %s failed: %s", method.__name__, e)
        cas_map: Dict[str, CASExtractionResult] = {}
        conf_order = {"high": 3, "medium": 2, "low": 1}
        for r in all_results:
            if r.cas not in cas_map:
                cas_map[r.cas] = r
            else:
                ex = cas_map[r.cas]
                if conf_order.get(r.confidence, 0) > conf_order.get(ex.confidence, 0):
                    cas_map[r.cas] = r
        final = sorted(
            cas_map.values(),
            key=lambda x: (
                conf_order.get(x.confidence, 0),
                -(x.source_section or 99),
            ),
            reverse=True,
        )
        return {
            "cas_numbers": [r.cas for r in final],
            "details": [
                {
                    "cas": r.cas,
                    "chemical_name": r.chemical_name,
                    "concentration": r.concentration,
                    "source_section": r.source_section,
                    "extraction_method": r.extraction_method,
                    "confidence": r.confidence,
                    "context": r.context,
                    "validation_status": r.validation_status,
                    "warnings": r.warnings,
                }
                for r in final
            ],
            "methods_used": list(methods_used),
            "total_found": len(final),
            "high_confidence": sum(1 for r in final if r.confidence == "high"),
            "medium_confidence": sum(1 for r in final if r.confidence == "medium"),
            "low_confidence": sum(1 for r in final if r.confidence == "low"),
        }

    def render_smart_cas_extractor_ui(self) -> None:
        """Streamlit UI for smart CAS extraction (import st inside to avoid test dependency)."""
        import streamlit as st

        st.markdown("### 🔬 Smart CAS Number Extraction")
        st.caption(
            "Multi-method extraction with local AI assistance - 100% private, no data leaves your computer"
        )
        with st.sidebar:
            st.markdown("#### 🤖 Local AI Model")
            if self.model_manager and self.model_manager.is_available():
                st.success(f"✅ Model loaded: {self.model_manager.current_model}")
                memory = self.model_manager.get_memory_usage()
                st.caption(
                    f"RAM: {memory.get('ram_gb', 0):.1f}/{memory.get('ram_total_gb', 0):.1f} GB"
                )
                if "gpu_gb" in memory:
                    st.caption(
                        f"GPU: {memory['gpu_gb']:.1f}/{memory['gpu_total_gb']:.1f} GB"
                    )
                if st.button("🔄 Unload Model", key="smart_cas_unload"):
                    self.model_manager.model_loaded = False
                    st.rerun()
            else:
                st.warning("⚠️ No AI model loaded")
                size_label = st.selectbox(
                    "Select model size",
                    ["tiny (4GB RAM)", "small (6GB RAM)"],
                    key="smart_cas_model_size",
                )
                size_map = {
                    "tiny (4GB RAM)": "tiny",
                    "small (6GB RAM)": "small",
                }
                if st.button("🚀 Load Model", type="primary", key="smart_cas_load"):
                    with st.spinner("Loading model... (first download may take a few minutes)"):
                        if self.model_manager and self.model_manager.load_model(
                            size_map.get(size_label, "small")
                        ):
                            st.success("Model loaded!")
                            st.rerun()
                        else:
                            st.error("Failed to load model")
        uploaded = st.file_uploader(
            "Upload Safety Data Sheet (PDF)",
            type=["pdf"],
            key="smart_cas_upload",
            help="PDF processed locally - no data leaves your computer",
        )
        if uploaded:
            col1, col2 = st.columns([1, 3])
            with col1:
                use_llm = st.checkbox(
                    "Use AI assistance",
                    value=True,
                    key="smart_cas_use_llm",
                )
            if st.button(
                "🔍 Extract CAS Numbers", type="primary", key="smart_cas_extract_btn"
            ):
                # Auto-load a local model when AI assistance is enabled.
                # Try Phi-3 first, then SmolLM2 as fallback.
                if use_llm and self.model_manager and not self.model_manager.is_available():
                    with st.spinner("Auto-loading local AI model (Phi-3, then SmolLM2 fallback)..."):
                        loaded = self.model_manager.load_model("small")
                        if not loaded:
                            loaded = self.model_manager.load_model("tiny")
                    if not loaded:
                        st.warning(
                            "AI assistance is enabled but no model could be loaded. "
                            "Continuing with rule-based extraction only."
                        )
                        use_llm = False
                    else:
                        st.success(f"Model loaded: {self.model_manager.current_model}")
                with st.spinner("Processing PDF..."):
                    pdf_bytes = uploaded.getvalue()
                    raw = sds_pdf_utils.extract_text_from_pdf_bytes(pdf_bytes)
                    raw = sds_pdf_utils.normalize_whitespace(raw)
                self.use_llm = use_llm
                results = self.extract_all_cas(raw)
                st.session_state["smart_cas_results"] = results
                st.session_state["smart_cas_filename"] = uploaded.name
        if "smart_cas_results" in st.session_state:
            results = st.session_state["smart_cas_results"]
            st.markdown("### 📊 Extraction Results")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("Total CAS Found", results["total_found"])
            with c2:
                st.metric("High Confidence", results["high_confidence"])
            with c3:
                st.metric("Medium Confidence", results["medium_confidence"])
            with c4:
                st.metric("Methods Used", len(results["methods_used"]))
            if results["details"]:
                import pandas as pd

                df = pd.DataFrame(results["details"])
                display_cols = [
                    "cas",
                    "chemical_name",
                    "concentration",
                    "source_section",
                    "confidence",
                    "extraction_method",
                    "validation_status",
                ]
                avail = [c for c in display_cols if c in df.columns]
                st.dataframe(
                    df[avail].fillna(""),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "cas": "CAS Number",
                        "chemical_name": "Chemical Name",
                        "concentration": "Concentration",
                        "source_section": "Section",
                        "confidence": "Confidence",
                        "extraction_method": "Method",
                        "validation_status": st.column_config.CheckboxColumn("Valid"),
                    },
                )
                st.markdown("### 🎯 Use for Hazard Assessment")
                cas_options = [
                    f"{d['cas']} - {d.get('chemical_name') or 'Unknown'}"
                    for d in results["details"]
                ]
                selected = st.multiselect(
                    "Select CAS numbers to assess",
                    options=cas_options,
                    default=cas_options[:1] if cas_options else [],
                    key="smart_cas_multiselect",
                )
                if selected and st.button(
                    "📊 Assess Selected Chemicals", key="smart_cas_assess_btn"
                ):
                    selected_cas = [s.split(" - ")[0] for s in selected]
                    if len(selected_cas) == 1:
                        st.session_state["query"] = selected_cas[0]
                        st.session_state["result_for"] = None
                    else:
                        st.session_state["selected_cas_list"] = selected_cas
                        st.session_state["assessment_mode"] = "batch"
                    st.rerun()
