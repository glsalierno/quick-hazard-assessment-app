"""CAS extraction from SDS-style label variants (regex + checksum)."""

from __future__ import annotations

import pytest

from utils.cas_text_extract import (
    find_checksum_valid_cas_in_text,
    normalize_cas_labels_for_extraction,
)


@pytest.mark.parametrize(
    "snippet,expected_cas",
    [
        ("CAS 50-00-0", "50-00-0"),
        ("C.A.S. 50-00-0", "50-00-0"),
        ("CAS-No.: 50-00-0", "50-00-0"),
        ("CAS # 50-00-0", "50-00-0"),
        ("CAS Number: 50-00-0", "50-00-0"),
        ("RN 50-00-0", "50-00-0"),
        ("Registry Number 50-00-0", "50-00-0"),
        ("Index No. 50-00-0", "50-00-0"),
        ("INDEX NO. 50-00-0", "50-00-0"),
        ("CAS Registry Number 7732-18-5", "7732-18-5"),
        ("Just the number 67-64-1 here.", "67-64-1"),
    ],
)
def test_find_cas_after_label_variants(snippet: str, expected_cas: str) -> None:
    found = find_checksum_valid_cas_in_text(snippet)
    assert expected_cas in found


def test_order_and_dedupe() -> None:
    text = "RN 50-00-0 then CAS 67-64-1 and again 50-00-0"
    assert find_checksum_valid_cas_in_text(text) == ["50-00-0", "67-64-1"]


def test_normalize_rewrites_labels() -> None:
    t = normalize_cas_labels_for_extraction("C.A.S. 50-00-0 / Registry Number 67-64-1")
    assert "CAS" in t or "CAS:" in t
    assert "C.A.S." not in t
