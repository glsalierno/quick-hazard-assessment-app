"""
Phase 1 SDS extraction using hybrid principles:
- Regex for format-stable tokens (CAS numbers, H/P codes, numeric properties)
- Heuristic parsing for quantitative values from SDS Section 2/3/11/12 text.

This file intentionally does NOT depend on any embedded LLM (no API key).
"""

from __future__ import annotations

import re
from typing import Any, TypedDict

from utils import cas_validator


class ParsedFlashPoint(TypedDict, total=False):
    value_c: float
    value: float
    unit: str
    operator: str
    raw_text: str


class ParsedVaporPressure(TypedDict, total=False):
    value: float
    unit: str
    temperature_c: float | None
    operator: str
    raw_text: str


class ParsedAquaticEndpoint(TypedDict, total=False):
    endpoint: str  # "LC50" or "EC50"
    value: float
    unit: str
    duration: str | None
    species: str | None
    operator: str
    raw_text: str


class ParsedSDSResult(TypedDict, total=False):
    cas_numbers: list[str]
    ghs: dict[str, Any]
    quantitative: dict[str, Any]
    meta: dict[str, Any]


# CAS: hyphen, en-dash, or (in context) space/dot between parts
_CAS_HYPHEN_RE = re.compile(r"\b(\d{1,9})-(\d{2})-(\d)\b")
# Same with en-dash U+2013 or em-dash U+2014
_CAS_DASH_RE = re.compile(r"\b(\d{1,9})[\u2010-\u2015\-](\d{2})[\u2010-\u2015\-](\d)\b")
# After "CAS" label (same line): CAS No. 67-64-1 or CAS Number: 67-64-1 or CAS# 67-64-1
_CAS_AFTER_LABEL_RE = re.compile(
    r"(?:CAS\s*(?:No\.?|Number|#|Registry\s*No\.?)?\s*[:\-]?\s*)(\d{1,9})[\-.\s\u2010-\u2015]+(\d{2})[\-.\s\u2010-\u2015]+(\d)\b",
    re.IGNORECASE,
)
# Registry No. / EC No. / EC-CAS (same line) — allow single-digit first part
_CAS_REGISTRY_EC_RE = re.compile(
    r"(?:Registry\s*No\.?|EC\s*No\.?|EC[-/]CAS|CAS\s*Registry)\s*[:\-]?\s*(\d{1,9})[\-.\s\u2010-\u2015]+(\d{2})[\-.\s\u2010-\u2015]+(\d)\b",
    re.IGNORECASE,
)
# Next line: line ending then digits-hyphen-digits-hyphen-digit (common in tables)
_CAS_NEXT_LINE_RE = re.compile(r"\n\s*(\d{1,9})[\-.\s\u2010-\u2015]+(\d{2})[\-.\s\u2010-\u2015]+(\d)\b")
# Same line contains "CAS" and later N-N-N (e.g. "Product CAS 75-45-3" or "CAS 75-45-3")
_CAS_SAME_LINE_RE = re.compile(
    r"CAS\b.{0,80}?(\d{1,9})[\-.\s\u2010-\u2015]+(\d{2})[\-.\s\u2010-\u2015]+(\d)\b",
    re.IGNORECASE | re.DOTALL,
)
# Spaces or dots as separators (no "CAS" required): 67 64 1 or 67.64.1
_CAS_SPACE_DOT_RE = re.compile(r"\b(\d{1,9})[\s.]+(\d{2})[\s.]+(\d)\b")

# GHS codes: capture combined forms like "P305+P351+P338" as a whole, then split later.
_H_CODE_COMBO_RE = re.compile(r"\bH\d{3}(?:\s*\+\s*H\d{3})*\b", re.I)
_P_CODE_COMBO_RE = re.compile(r"\bP\d{3}(?:\s*\+\s*P\d{3})*\b", re.I)

# A permissive single-code regex for splitting combined codes.
_H_CODE_SINGLE_RE = re.compile(r"\bH\d{3}\b", re.I)
_P_CODE_SINGLE_RE = re.compile(r"\bP\d{3}\b", re.I)

_SIGNAL_WORD_RE = re.compile(
    r"(?:Signal\s*word|GHS\s*signal\s*word)\s*[:\-]?\s*(Danger|Warning)\b",
    re.IGNORECASE,
)


def _split_codes(combos: list[str], single_re: re.Pattern[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for combo in combos:
        # Remove spaces and punctuation-like artifacts
        for code in single_re.findall(combo or ""):
            c = code.upper()
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def _normalize_text_for_cas(text: str) -> str:
    """Replace unicode dashes with hyphen so CAS patterns match."""
    if not text:
        return ""
    t = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2010", "-").replace("\u2011", "-")
    return t


def _extract_section3_block(text: str) -> str:
    """Extract Section 3 (Composition/identity) block where CAS often appears. Returns block or empty."""
    if not text:
        return ""
    t = _normalize_text_for_cas(text)
    start_re = re.compile(
        r"(?:Section\s*3|3\.\s|3\.1\s|Composition\s*/\s*identity|Identificat(?:ion|e))\s*[:\s]*",
        re.IGNORECASE,
    )
    end_re = re.compile(r"\bSection\s*4\b|^\s*4\.\s|\b(?:Section\s*5|5\.\s)\b", re.IGNORECASE | re.MULTILINE)
    m_start = start_re.search(t)
    if not m_start:
        return ""
    start = m_start.end()
    m_end = end_re.search(t[start:])
    end = start + m_end.start() if m_end else len(t)
    block = t[start:end]
    return block if len(block) > 50 else ""


def _cas_candidates_from_text(text: str) -> list[tuple[str, str, str]]:
    """Return list of (first, second, check) from all CAS patterns. Label context allows single-digit first part."""
    t = _normalize_text_for_cas(text)
    candidates: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    def add(matches: list[tuple[str, ...]], from_label_context: bool = False) -> None:
        for m in matches:
            if len(m) >= 3:
                key = (m[0].strip(), m[1].strip(), m[2].strip())
                if key not in seen and key[0].isdigit() and key[1].isdigit() and key[2].isdigit():
                    if len(key[1]) == 2 and len(key[2]) == 1:
                        if len(key[0]) >= 2 or from_label_context:
                            seen.add(key)
                            candidates.append(key)

    add(_CAS_AFTER_LABEL_RE.findall(t), from_label_context=True)
    add(_CAS_REGISTRY_EC_RE.findall(t), from_label_context=True)
    add(_CAS_NEXT_LINE_RE.findall(t), from_label_context=True)
    add(_CAS_SAME_LINE_RE.findall(t), from_label_context=True)
    add(_CAS_HYPHEN_RE.findall(t))
    add(_CAS_DASH_RE.findall(t))
    add(_CAS_SPACE_DOT_RE.findall(t))
    return candidates


def _try_ocr_normalize_cas_part(part: str) -> str:
    """Try common OCR substitutions (O->0, l->1) in a digit-like part."""
    if not part or not part.strip():
        return part
    s = part.strip()
    if s.isdigit():
        return s
    replaced = (
        s.replace("O", "0").replace("o", "0").replace("l", "1").replace("I", "1").replace("S", "5").replace("Z", "2")
    )
    return replaced if replaced.isdigit() else s


def _cas_candidates_with_ocr_fallback(text: str) -> list[tuple[str, str, str]]:
    """Get CAS candidates; add OCR-normalized versions of hyphen matches for misreads."""
    raw = _cas_candidates_from_text(text)
    out: list[tuple[str, str, str]] = list(raw)
    seen: set[tuple[str, str, str]] = set(raw)
    for m in _CAS_HYPHEN_RE.finditer(_normalize_text_for_cas(text or "")):
        if len(m.groups()) >= 3:
            a, b, c = m.group(1), m.group(2), m.group(3)
            a2 = _try_ocr_normalize_cas_part(a)
            b2 = _try_ocr_normalize_cas_part(b)
            c2 = _try_ocr_normalize_cas_part(c)
            if (a2, b2, c2) != (a, b, c) and len(b2) == 2 and len(c2) == 1 and (a2, b2, c2) not in seen:
                seen.add((a2, b2, c2))
                out.append((a2, b2, c2))
    return out


def _extract_cas_numbers(text: str, use_ocr_fallback: bool = True) -> list[str]:
    """Extract CAS numbers: Section 3 first, then full text; relaxed validation and check-digit correction."""
    if not text:
        return []
    # Prefer Section 3 block (Composition/identity) where CAS usually appears
    section3 = _extract_section3_block(text)
    get_candidates = _cas_candidates_with_ocr_fallback if use_ocr_fallback else _cas_candidates_from_text
    from_s3 = get_candidates(section3) if section3 else []
    from_full = get_candidates(text)
    # Merge: Section 3 candidates first, then rest from full text (no duplicates)
    seen_tuple: set[tuple[str, str, str]] = set()
    ordered: list[tuple[str, str, str]] = []
    for cand in from_s3 + from_full:
        if cand not in seen_tuple:
            seen_tuple.add(cand)
            ordered.append(cand)
    out: list[str] = []
    seen: set[str] = set()
    for first, second, check in ordered:
        normalized = cas_validator.normalize_to_cas_format(first, second, check)
        if not normalized:
            continue
        try:
            cas_result, _check_ok = cas_validator.validate_cas_relaxed(normalized)
        except Exception:
            continue
        if not cas_result:
            continue
        if cas_result not in seen:
            seen.add(cas_result)
            out.append(cas_result)
    return out


def _extract_ghs_codes(text: str) -> dict[str, Any]:
    h_combo = _H_CODE_COMBO_RE.findall(text or "")
    p_combo = _P_CODE_COMBO_RE.findall(text or "")
    h_codes = _split_codes(h_combo, _H_CODE_SINGLE_RE)
    p_codes = _split_codes(p_combo, _P_CODE_SINGLE_RE)
    signal_word = ""

    m = _SIGNAL_WORD_RE.search(text or "")
    if m:
        signal_word = m.group(1).title()

    return {
        "h_codes": h_codes,
        "p_codes": p_codes,
        "signal_word": signal_word,
        "source_notes": {
            "h_from": "regex:Hxxx (combined combos split)",
            "p_from": "regex:Pxxx (combined combos split)",
            "signal_from": "regex:Signal word/Danger/Warning",
        },
    }


def _parse_numeric(s: str) -> float | None:
    if not s:
        return None
    s2 = s.strip().replace(",", ".")
    try:
        return float(s2)
    except ValueError:
        return None


def _maybe_convert_f_to_c(value_f: float) -> float:
    return (value_f - 32.0) * 5.0 / 9.0


_FLASH_POINT_RE = re.compile(
    r"Flash\s*point\s*[:=]?\s*"
    r"(?P<operator>[<>~])?\s*"
    r"(?P<value>\d+(?:[.,]\d+)?)\s*"
    r"(?P<unit>°?\s*[CF]|deg\s*[CF]|Celsius|Fahrenheit)\b",
    re.IGNORECASE,
)


def _extract_flash_points(text: str) -> list[ParsedFlashPoint]:
    out: list[ParsedFlashPoint] = []
    for m in _FLASH_POINT_RE.finditer(text or ""):
        op = (m.group("operator") or "").strip()
        val = _parse_numeric(m.group("value") or "")
        unit_raw = (m.group("unit") or "").replace(" ", "").lower()
        if val is None:
            continue

        unit_raw_n = unit_raw.replace("°", "")
        if unit_raw_n.endswith("f") or "fahrenheit" in unit_raw_n:
            value_c = _maybe_convert_f_to_c(val)
            out.append(
                {
                    "value_c": value_c,
                    "value": val,
                    "unit": "F",
                    "operator": op,
                    "raw_text": m.group(0).strip(),
                }
            )
        else:
            out.append(
                {
                    "value_c": float(val),
                    "value": float(val),
                    "unit": "C",
                    "operator": op,
                    "raw_text": m.group(0).strip(),
                }
            )
    return out


_VAPOR_PRESSURE_RE = re.compile(
    r"Vapor\s*pressure\s*[:=]?\s*"
    r"(?P<operator>[<>~])?\s*"
    r"(?P<value>\d+(?:[.,]\d+)?)\s*"
    r"(?P<unit>mmHg|kPa|hPa|Pa|mbar|bar|Torr)\b"
    r"(?:[^\n]{0,40}?\bat\s*(?P<temp>\d+(?:[.,]\d+)?)\s*(?P<tempunit>°?\s*[CF]|deg\s*[CF])\b)?",
    re.IGNORECASE,
)


def _extract_vapor_pressures(text: str) -> list[ParsedVaporPressure]:
    out: list[ParsedVaporPressure] = []
    for m in _VAPOR_PRESSURE_RE.finditer(text or ""):
        op = (m.group("operator") or "").strip()
        val = _parse_numeric(m.group("value") or "")
        unit = (m.group("unit") or "").strip()
        if val is None or not unit:
            continue

        temp_c: float | None = None
        temp_raw = (m.group("temp") or "").strip()
        tempunit_raw = (m.group("tempunit") or "").replace(" ", "").lower()
        if temp_raw and tempunit_raw:
            temp_val = _parse_numeric(temp_raw)
            if temp_val is not None:
                if "f" in tempunit_raw or "fahrenheit" in tempunit_raw:
                    temp_c = _maybe_convert_f_to_c(temp_val)
                else:
                    temp_c = float(temp_val)

        out.append(
            {
                "value": float(val),
                "unit": unit,
                "temperature_c": temp_c,
                "operator": op,
                "raw_text": m.group(0).strip(),
            }
        )
    return out


_AQUATIC_ENDPOINT_RE = re.compile(r"\b(LC50|EC50)\b", re.IGNORECASE)
_ENDPOINT_VALUE_RE = re.compile(
    r"(?P<operator>[<>~])?\s*(?P<value>\d+(?:[.,]\d+)?)\s*(?P<unit>mg/L|µg/L|ug/L)\b",
    re.IGNORECASE,
)
_DURATION_RE = re.compile(r"\b(?P<dur>\d+(?:[.,]\d+)?)\s*(?P<unit>h|hr|hrs|hour|hours|d|day|days)\b", re.IGNORECASE)


def _infer_aquatic_species(segment: str) -> str | None:
    seg = (segment or "").lower()
    if "daphnia" in seg:
        return "Daphnia"
    if "algae" in seg:
        return "Algae"
    if "fish" in seg or "trout" in seg:
        return "Fish"
    if "aquatic" in seg:
        return "Aquatic"
    return None


def _extract_aquatic_endpoints(text: str) -> list[ParsedAquaticEndpoint]:
    out: list[ParsedAquaticEndpoint] = []
    if not text:
        return out

    for m in _AQUATIC_ENDPOINT_RE.finditer(text):
        start = max(0, m.start() - 10)
        end = min(len(text), m.start() + 250)
        segment = text[start:end]
        endpoint = m.group(1).upper()

        # Value/unit capture within the local segment.
        vm = _ENDPOINT_VALUE_RE.search(segment)
        if not vm:
            continue
        operator = (vm.group("operator") or "").strip()
        value = _parse_numeric(vm.group("value") or "")
        unit = (vm.group("unit") or "").strip()
        if value is None:
            continue

        # Duration and species are optional.
        dur_m = _DURATION_RE.search(segment)
        duration = None
        if dur_m:
            duration = f"{dur_m.group('dur')} {dur_m.group('unit')}"

        species = _infer_aquatic_species(segment)

        out.append(
            {
                "endpoint": endpoint,
                "value": float(value),
                "unit": unit,
                "duration": duration,
                "species": species,
                "operator": operator,
                "raw_text": segment.strip(),
            }
        )

    # De-dup exact duplicates (raw_text can differ; keep unique by endpoint/value/unit if close).
    dedup: list[ParsedAquaticEndpoint] = []
    seen_keys: set[tuple[str, str, float]] = set()
    for e in out:
        key = (e.get("endpoint", ""), e.get("unit", ""), round(float(e.get("value", 0.0)), 6))
        if key not in seen_keys:
            seen_keys.add(key)
            dedup.append(e)
    return dedup


def extract_sds_fields_from_text(text: str) -> ParsedSDSResult:
    """
    Main Phase 1 extractor:
    - CAS + GHS H/P + signal word
    - Flash point + vapor pressure
    - Aquatic LC50/EC50

    Fields that cannot be found are omitted to keep the result "meaningful-only".
    """
    cleaned: dict[str, Any] = {}
    cas_numbers = _extract_cas_numbers(text or "")
    ghs = _extract_ghs_codes(text or "")
    flash_points = _extract_flash_points(text or "")
    vapor_pressures = _extract_vapor_pressures(text or "")
    aquatic_endpoints = _extract_aquatic_endpoints(text or "")

    # Build "meaningful-only" output:
    if cas_numbers:
        cleaned["cas_numbers"] = cas_numbers
    if ghs:
        # drop empty codes/signal to make UI quieter
        ghs_cleaned = dict(ghs)
        if not ghs_cleaned.get("h_codes"):
            ghs_cleaned["h_codes"] = []
        if not ghs_cleaned.get("p_codes"):
            ghs_cleaned["p_codes"] = []
        if not (ghs_cleaned.get("signal_word") or "").strip():
            ghs_cleaned["signal_word"] = ""
        cleaned["ghs"] = ghs_cleaned
    quantitative: dict[str, Any] = {}
    if flash_points:
        quantitative["flash_point"] = flash_points
    if vapor_pressures:
        quantitative["vapor_pressure"] = vapor_pressures
    if aquatic_endpoints:
        quantitative["aquatic_toxicity"] = aquatic_endpoints
    if quantitative:
        cleaned["quantitative"] = quantitative

    cleaned["meta"] = {
        "extraction_method": "regex_only_phase1",
        "note": "Fields omitted when not found; caller can still ignore empty mandatory fields.",
    }
    return cleaned  # type: ignore[return-value]

