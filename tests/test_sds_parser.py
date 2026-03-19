from __future__ import annotations

import sys
from unittest.mock import patch


sys.path.insert(0, ".")

from utils.sds_parser import SDSParser  # noqa: E402


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
