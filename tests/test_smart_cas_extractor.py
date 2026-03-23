"""Unit tests for Smart CAS Extractor."""

from __future__ import annotations

import sys
from pathlib import Path

# Run from repo root so utils is importable
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.smart_cas_extractor import CASExtractionResult, SmartCASExtractor


class TestSmartCASExtractor:
    def setup_method(self) -> None:
        self.extractor = SmartCASExtractor(use_llm=False)

    def test_clean_cas(self) -> None:
        assert self.extractor._clean_cas("CAS: 123-45-6") == "123-45-6"
        assert self.extractor._clean_cas("CAS No. 123-45-6") == "123-45-6"
        assert self.extractor._clean_cas("123-45-6") == "123-45-6"
        assert self.extractor._clean_cas("") is None
        assert self.extractor._clean_cas("  CAS# 67-64-1  ") == "67-64-1"

    def test_validate_cas_checksum(self) -> None:
        # Ethanol
        assert self.extractor._validate_cas_checksum("64-17-5") is True
        assert self.extractor._validate_cas_checksum("64-17-6") is False
        assert self.extractor._validate_cas_checksum("64-17") is False
        assert self.extractor._validate_cas_checksum("") is False
        # Hexamethylenediamine
        assert self.extractor._validate_cas_checksum("124-09-4") is True

    def test_extract_section_3_table(self) -> None:
        sample = """
Section 3: Composition

Component | CAS Number | Concentration
Hexamethylenediamine | 124-09-4 | 60%
Water | 7732-18-5 | 40%

Section 4: First Aid
"""
        results = self.extractor.extract_from_section_3_table(sample)
        assert len(results) >= 1
        cas_list = [r.cas for r in results]
        assert "124-09-4" in cas_list
        if len(results) >= 2:
            assert "7732-18-5" in cas_list
        r0 = results[0]
        assert r0.cas == "124-09-4"
        assert r0.source_section == 3
        assert r0.extraction_method == "table"
        assert r0.confidence == "high"
        assert r0.validation_status is True

    def test_extract_section_3_text(self) -> None:
        sample = """
Section 3: Composition/Information on Ingredients

Hexamethylenediamine (CAS: 124-09-4) 60-100%
Water (CAS 7732-18-5) 40%
"""
        results = self.extractor.extract_from_section_3_text(sample)
        assert len(results) >= 1
        assert any(r.cas == "124-09-4" for r in results)

    def test_extract_section_1(self) -> None:
        sample = """
Section 1: Identification

Product name: Acetone
CAS Number: 67-64-1
"""
        results = self.extractor.extract_from_section_1(sample)
        assert len(results) >= 1
        assert any(r.cas == "67-64-1" for r in results)

    def test_extract_from_any_section(self) -> None:
        sample = "Some text with CAS 64-17-5 (ethanol) and 7732-18-5 (water)."
        results = self.extractor.extract_from_any_section(sample)
        assert len(results) >= 1
        cas_set = {r.cas for r in results}
        assert "64-17-5" in cas_set or "7732-18-5" in cas_set

    def test_extract_all_cas_deduplicates(self) -> None:
        sample = """
Section 1: Identification
Product: Acetone. CAS No. 67-64-1.

Section 3: Composition
Acetone (CAS: 67-64-1) 99%
"""
        out = self.extractor.extract_all_cas(sample)
        assert "cas_numbers" in out
        assert "67-64-1" in out["cas_numbers"]
        assert out["cas_numbers"].count("67-64-1") == 1
        assert out["total_found"] >= 1
