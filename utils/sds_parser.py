"""
Unified SDS Parser public interface.
"""

from __future__ import annotations

from typing import Optional

import streamlit as st

from utils import sds_pdf_utils
from utils.sds_environment import EnvironmentDetector
from utils.sds_models import SDSParseResult
from utils.sds_parser_engine import SDSParserEngine


class SDSParser:
    def __init__(self) -> None:
        self.engine = SDSParserEngine()
        self.env = EnvironmentDetector.detect()

    def parse_pdf(self, pdf_bytes: bytes) -> Optional[SDSParseResult]:
        try:
            text = sds_pdf_utils.extract_text_from_pdf_bytes(pdf_bytes)
            text = sds_pdf_utils.normalize_whitespace(text)
            if not (text or "").strip():
                return None
            return self.engine.parse(text)
        except Exception as e:
            st.error(f"Parsing error: {e}")
            return None

    def get_capability_message(self) -> str:
        return EnvironmentDetector.get_capability_message()


@st.cache_resource
def get_sds_parser() -> SDSParser:
    return SDSParser()
