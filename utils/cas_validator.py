"""
CAS number validation and normalization.
Validates format (digits-digits-digits) and optional checksum.
"""

from __future__ import annotations

import re
from typing import Tuple


# CAS format: up to 9 digits, hyphen, 2 digits, hyphen, single check digit
_CAS_PATTERN = re.compile(r"^(\d{1,9})-(\d{2})-(\d)$")


def is_valid_cas_format(cas: str) -> bool:
    """
    Check that string looks like a CAS number (N-N-N format).
    Does not verify checksum.
    """
    if not cas or not isinstance(cas, str):
        return False
    return _CAS_PATTERN.match(cas.strip()) is not None


def cas_checksum(digits: str) -> int:
    """Compute CAS check digit. digits is the full numeric string (no hyphens)."""
    n = len(digits)
    total = 0
    for i, d in enumerate(digits):
        total += int(d) * (n - i)
    return total % 10


def validate_cas(cas: str) -> Tuple[bool, str]:
    """
    Validate CAS format and check digit.
    Returns (is_valid, normalized_cas).
    Normalized form: standard hyphenation (e.g. 67-64-1).
    """
    if not cas or not isinstance(cas, str):
        return False, ""
    s = cas.strip()
    m = _CAS_PATTERN.match(s)
    if not m:
        return False, s
    first, second, check = m.group(1), m.group(2), m.group(3)
    digits = first + second + check
    expected_check = str(cas_checksum(digits))
    if check != expected_check:
        return False, f"{first}-{second}-{check}"
    return True, f"{first}-{second}-{check}"


def normalize_to_cas_format(part1: str, part2: str, part3: str) -> str:
    """Build N-N-N string from three digit parts (no hyphens)."""
    a, b, c = part1.strip(), part2.strip(), part3.strip()
    if a.isdigit() and b.isdigit() and c.isdigit() and len(b) == 2 and len(c) == 1:
        return f"{a}-{b}-{c}"
    return ""


def validate_cas_relaxed(cas: str) -> tuple[str, bool]:
    """
    Return (normalized_cas, check_digit_valid).
    If format is N-N-N but check digit is wrong, returns the same string with
    corrected check digit so the result is always checksum-valid when format is valid.
    Use this for SDS extraction so we maximize CAS retrieval for v1.3 lookup.
    """
    if not cas or not isinstance(cas, str):
        return "", False
    s = cas.strip()
    m = _CAS_PATTERN.match(s)
    if not m:
        return "", False
    first, second, check = m.group(1), m.group(2), m.group(3)
    digits = first + second + check
    expected_check = str(cas_checksum(digits))
    normalized = f"{first}-{second}-{check}"
    if check == expected_check:
        return normalized, True
    corrected = f"{first}-{second}-{expected_check}"
    return corrected, False


def normalize_cas_input(raw: str) -> str:
    """
    Extract CAS from input that may be "67-64-1" or "67-64-1 (Acetone)".
    Returns the first token that looks like a CAS, or the whole string stripped.
    """
    if not raw or not isinstance(raw, str):
        return ""
    s = raw.strip()
    # If it looks like "67-64-1 (Name)", take the first part
    parts = s.split()
    for p in parts:
        p = p.strip("(),")
        if _CAS_PATTERN.match(p):
            return p
    return s
