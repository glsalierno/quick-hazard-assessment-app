from __future__ import annotations

import sys
from unittest.mock import patch


sys.path.insert(0, ".")

from utils.sds_parser import SDSParser  # noqa: E402
from utils.sds_parser_engine import SDSParserEngine  # noqa: E402


class TestSDSParser:
    def setup_method(self) -> None:
        self.parser = SDSParser()

    def test_parse_sample_sds(self) -> None:
        sample_text = """
        Section 3: Composition/Information on Ingredients
        Component | CAS Number | Concentration
        Hexamethylenediamine | 124-09-4 | 60%
        Water | 7732-18-5 | 40%

        Section 2: Hazard Identification
        Signal Word: Danger
        H314 H318
        P260 P280

        Section 9: Physical Properties
        Flash Point 116 C
        Boiling Point 204 C

        Section 12: Ecological Information
        EC50 = 23.4 mg/L (Daphnia magna)
        LC50 = 62 mg/L (Fish)
        """
        with patch("utils.sds_pdf_utils.extract_text_from_pdf_bytes") as mock_extract:
            mock_extract.return_value = sample_text
            result = self.parser.parse_pdf(b"fake-pdf")
            assert result is not None
            assert len(result.cas_numbers) >= 1
            assert any(x.cas == "124-09-4" for x in result.cas_numbers)
            assert len(result.ghs.h_codes) >= 1
            assert len(result.physical_properties) >= 1
            assert len(result.ecotoxicity) >= 1

    def test_cas_checksum_validation(self) -> None:
        assert self.parser.engine._validate_cas_checksum("64-17-5") is True
        assert self.parser.engine._validate_cas_checksum("64-17-6") is False
        assert self.parser.engine._validate_cas_checksum("64-17") is False

    def test_forane_style_whitespace_composition_table(self) -> None:
        """Multi-component refrigerant-style Section 3 (space-aligned columns)."""
        sample = """
3. COMPOSITION/INFORMATION ON INGREDIENTS

Chemical Name                 CAS-No.          Wt/Wt%       GHS Classification
Methane, chlorodifluoro-      75-45-6          >= 30 - < 60 %   H280, H420
Ethane, 1,1,1-trifluoro-      420-46-2         >= 30 - < 60 %   H220, H280
Ethane, pentafluoro-          354-33-6         >= 5 - < 10 %    H280
"""
        eng = SDSParserEngine()
        rows = eng._extract_composition_from_section3(sample)
        by_cas = {r.cas: r for r in rows}
        assert "75-45-6" in by_cas
        assert "420-46-2" in by_cas
        assert "354-33-6" in by_cas
        assert by_cas["75-45-6"].section == 3
        assert by_cas["75-45-6"].chemical_name and "chlorodifluoro" in by_cas["75-45-6"].chemical_name.lower()
        assert by_cas["75-45-6"].concentration and "30" in by_cas["75-45-6"].concentration.replace(" ", "")
        assert by_cas["354-33-6"].concentration and "5" in by_cas["354-33-6"].concentration.replace(" ", "")

    def test_orphan_cas_line_uses_pending_name(self) -> None:
        eng = SDSParserEngine()
        sample = """
3. Composition
Ethane, pentafluoro-
354-33-6
"""
        rows = eng._extract_composition_from_section3(sample)
        assert any(r.cas == "354-33-6" and r.chemical_name and "pentafluoro" in r.chemical_name.lower() for r in rows)

    def test_docling_module_reports_status_without_crash(self) -> None:
        from utils import docling_sds_parser

        msg = docling_sds_parser.docling_status_message()
        assert isinstance(msg, str) and len(msg) > 0

    def test_html_composition_table_optional(self) -> None:
        eng = SDSParserEngine()
        html = """
<table><tr><th>Chemical</th><th>CAS No.</th><th>Wt %</th></tr>
<tr><td>Water</td><td>7732-18-5</td><td>40 - 60%</td></tr>
</table>
"""
        rows = eng._parse_html_tables_for_cas(html)
        assert any(r.cas == "7732-18-5" for r in rows)
        assert any(r.chemical_name and "water" in r.chemical_name.lower() for r in rows)
