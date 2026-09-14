"""IUCLID phrase/picklist decoder for offline REACH dossier parsing."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Union
import xml.etree.ElementTree as ET

import pandas as pd

import config

logger = logging.getLogger(__name__)

_DOC_FALLBACKS: dict[str, str] = {
    "ENDPOINT_STUDY_RECORD": "Endpoint study record",
    "ENDPOINT_STUDY_RECORD.TOX": "Endpoint study record (toxicity)",
    "DOSSIER.R_COMPLETE": "REACH complete dossier",
    "DOCUMENT": "IUCLID document",
}


def _candidate_phrase_files(root: Path) -> list[Path]:
    patterns = (
        "*.properties",
        "*.xlsx",
        "*.xls",
        "*.csv",
        "*.xml",
    )
    hits: list[Path] = []
    for pat in patterns:
        hits.extend(root.rglob(pat))
    # Prefer likely phrase/picklist files first.
    hits.sort(
        key=lambda p: (
            0
            if any(k in p.name.lower() for k in ("picklist", "phrase", "all_fields", "properties", "dcr.xml"))
            else 1,
            len(str(p)),
        )
    )
    return hits


def _as_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _humanize_token(raw: str) -> str:
    s = _as_text(raw)
    if not s:
        return s
    s = s.replace("_", " ").replace(".", " ")
    # Break camel case boundaries.
    out_chars: list[str] = []
    prev = ""
    for ch in s:
        if prev and ch.isupper() and prev.islower():
            out_chars.append(" ")
        out_chars.append(ch)
        prev = ch
    h = " ".join("".join(out_chars).split())
    return h[:1].upper() + h[1:] if h else h


def _is_phrase_table(df: pd.DataFrame) -> bool:
    cols = {str(c).strip().lower() for c in df.columns}
    target = ("phrase", "picklist", "code", "value", "language", "text", "label")
    return any(any(t in col for t in target) for col in cols)


def _pick_col(df: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    for c in df.columns:
        lc = str(c).strip().lower()
        if any(n in lc for n in names):
            return str(c)
    return None


def _parse_table(df: pd.DataFrame, out: dict[str, str]) -> int:
    if df.empty or not _is_phrase_table(df):
        return 0
    code_col = _pick_col(df, ("phraseid", "code", "valueid", "picklist", "key"))
    label_col = _pick_col(df, ("phrasevalue", "label", "text", "description", "value"))
    lang_col = _pick_col(df, ("language", "lang"))
    group_col = _pick_col(df, ("phrasegroup", "group", "doctype", "documenttype", "category"))
    if not code_col or not label_col:
        return 0

    added = 0
    for _, row in df.iterrows():
        if lang_col:
            lang = _as_text(row.get(lang_col)).lower()
            if lang and lang not in ("en", "eng", "english"):
                continue
        code = _as_text(row.get(code_col))
        label = _as_text(row.get(label_col))
        if not code or not label:
            continue
        code_norm = code.strip()
        if code_norm and code_norm not in out:
            out[code_norm] = label
            added += 1
        # hierarchical helper: GROUP.CODE
        group = _as_text(row.get(group_col)) if group_col else ""
        if group:
            dotted = f"{group}.{code_norm}"
            if dotted not in out:
                out[dotted] = label
    return added


def _parse_dcr_xml(path: Path, out: dict[str, str]) -> int:
    """
    Parse IUCLID dcr.xml for definition/path tokens.

    Note: dcr.xml often references numeric phrase IDs without embedding phrase text.
    This parser still helps decode endpoint/document-type style dotted codes.
    """
    added = 0
    try:
        for _, elem in ET.iterparse(path, events=("end",)):
            tag = elem.tag.split("}")[-1] if elem.tag else ""
            if tag not in ("Definition", "Path"):
                continue
            txt = _as_text(elem.text)
            if not txt:
                continue
            if txt not in out:
                out[txt] = _humanize_token(txt)
                added += 1
            # Also map rightmost segment in dotted definitions.
            if "." in txt:
                tail = txt.split(".")[-1]
                if tail and tail not in out:
                    out[tail] = _humanize_token(tail)
                    added += 1
            elem.clear()
    except Exception as exc:
        logger.warning("IUCLID dcr.xml parse failed for %s: %s", path, exc)
    return added


def _unescape_properties_text(s: str) -> str:
    txt = s.replace("\\:", ":").replace("\\=", "=").replace("\\#", "#")
    txt = txt.replace("\\t", "\t").replace("\\n", " ").replace("\\r", " ")
    return " ".join(txt.split())


def _parse_phrase_properties(path: Path, out: dict[str, str]) -> int:
    added = 0
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = None
            import re

            m = re.match(r"^phrases\.(\d+)\.text=(.*)$", line)
            if not m:
                continue
            code = _as_text(m.group(1))
            label = _unescape_properties_text(_as_text(m.group(2)))
            if not code or not label:
                continue
            if code not in out:
                out[code] = label
                added += 1
    except Exception as exc:
        logger.warning("IUCLID properties parse failed for %s: %s", path, exc)
    return added


@lru_cache(maxsize=1)
def _load_phrase_map() -> dict[str, str]:
    out: dict[str, str] = dict(_DOC_FALLBACKS)
    raw_dir = (os.getenv("IUCLID_FORMAT_DIR") or "").strip() or str(getattr(config, "IUCLID_FORMAT_DIR", "") or "").strip()
    if not raw_dir:
        logger.info("IUCLID phrase mapper: IUCLID_FORMAT_DIR not set; picklist decoding uses fallbacks only.")
        return out
    root = Path(os.path.expandvars(raw_dir)).expanduser().resolve()
    if not root.is_dir():
        logger.warning("IUCLID phrase mapper disabled: IUCLID_FORMAT_DIR not found: %s", root)
        return out

    files = _candidate_phrase_files(root)
    if not files:
        # Many IUCLID format bundles ship dcr.xml only.
        dcr = root / "dcr.xml"
        if dcr.is_file():
            total = _parse_dcr_xml(dcr, out)
            logger.info("IUCLID phrase mapper loaded %s dcr.xml mappings from %s", total, dcr)
            return out
        logger.warning("IUCLID phrase mapper: no phrase/picklist files found under %s", root)
        return out

    total = 0
    # Pass 1: fast text-based sources (properties/xml) to avoid heavy workbook parsing when unnecessary.
    fast_files = [p for p in files if p.suffix.lower() in (".properties", ".xml")]
    slow_files = [p for p in files if p.suffix.lower() not in (".properties", ".xml")]

    for path in fast_files:
        try:
            if path.name.lower() == "dcr.xml":
                total += _parse_dcr_xml(path, out)
                continue
            if path.suffix.lower() == ".properties":
                total += _parse_phrase_properties(path, out)
                continue
        except Exception:
            continue

    # If fast sources already provide substantial phrase IDs, skip slow spreadsheet parse.
    if total > 1000:
        logger.info("IUCLID phrase mapper loaded %s mappings from fast sources under %s", total, root)
        return out

    for path in slow_files:
        try:
            if path.suffix.lower() == ".csv":
                df = pd.read_csv(path, low_memory=False)
                total += _parse_table(df, out)
            else:
                xl = pd.ExcelFile(path)
                for sheet in xl.sheet_names:
                    try:
                        df = pd.read_excel(path, sheet_name=sheet)
                    except Exception:
                        continue
                    total += _parse_table(df, out)
        except Exception:
            continue
    logger.info("IUCLID phrase mapper loaded %s mappings from %s", total, root)
    return out


def get_phrase_label(code: Union[str, int]) -> str:
    """Return human-readable label for an IUCLID code, or raw code if unknown."""
    raw = _as_text(code)
    if not raw:
        return raw
    mapping = _load_phrase_map()
    if raw in mapping:
        return mapping[raw]
    # Common hierarchical formats.
    upper = raw.upper()
    if upper in mapping:
        return mapping[upper]
    head = upper.split(".", 1)[0]
    if head in mapping:
        return mapping[head]
    # Humanize IUCLID-style keys when no canonical phrase text is available.
    if any(ch in raw for ch in ("_", ".")) or any(ch.isalpha() for ch in raw):
        return _humanize_token(raw)
    return raw


def has_phrase_mapping(code: Union[str, int]) -> bool:
    """Return True when a code resolves from explicit phrase-map entries."""
    raw = _as_text(code)
    if not raw:
        return False
    mapping = _load_phrase_map()
    if raw in mapping:
        return True
    upper = raw.upper()
    if upper in mapping:
        return True
    head = upper.split(".", 1)[0]
    return head in mapping
