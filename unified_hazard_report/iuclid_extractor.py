"""Best-effort extraction of study / endpoint signals from IUCLID ``Document.i6d`` inside an ``.i6z`` dossier."""

from __future__ import annotations

import re
import zipfile
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET


def _tag_local(tag: str) -> str:
    if not tag:
        return ""
    return tag.split("}")[-1]


def read_i6d_bytes_from_i6z(i6z_path: Path) -> bytes | None:
    """Return ``Document.i6d`` payload from a dossier zip, or ``None``."""
    try:
        with zipfile.ZipFile(i6z_path, "r") as zf:
            names = zf.namelist()
            doc = next((n for n in names if n.lower().endswith("document.i6d")), None)
            if not doc:
                doc = next((n for n in names if n.lower().endswith(".i6d")), None)
            if not doc:
                return None
            return zf.read(doc)
    except (OSError, zipfile.BadZipFile, KeyError):
        return None


def extract_endpoints_from_i6d(xml_bytes: bytes, *, max_rows: int = 150) -> list[dict[str, Any]]:
    """
    Heuristic scan of IUCLID XML for numeric / textual study results.

    Returns rows like::
        ``{'endpoint_name': '...', 'result': '...', 'units': ''}``
    """
    out: list[dict[str, Any]] = []
    if not xml_bytes:
        return out
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return out

    # Regex pass on decoded text (catches LD50 / NOAEL phrases not tied to a single element)
    try:
        text_blob = xml_bytes.decode("utf-8", errors="replace")
    except Exception:
        text_blob = ""
    for m in re.finditer(
        r"\b(LD50|LC50|LOAEL|NOAEL|EC50|IC50)\b[^<\n]{0,12}([0-9][0-9.,\s]*\s*(?:mg/kg|mg/L|ppm|μg/L|ug/L|g/kg)?)",
        text_blob,
        re.I,
    ):
        label, rest = m.group(1), (m.group(2) or "").strip()
        out.append({"endpoint_name": label, "result": rest[:500], "units": ""})
        if len(out) >= max_rows:
            return out

    interest = (
        "acute",
        "toxicity",
        "corrosion",
        "irritation",
        "sensiti",
        "mutagen",
        "reproduction",
        "aquatic",
        "chronic",
        "bioaccum",
        "pbt",
        "cmr",
        "dose",
        "endpoint",
        "conclusion",
        "result",
    )

    for el in root.iter():
        loc = _tag_local(el.tag).lower()
        if not any(k in loc for k in interest):
            continue
        blob = " ".join((el.text or "").split()) if el.text else ""
        if not blob:
            blob = " ".join("".join(el.itertext()).split())[:800]
        if len(blob) < 6 or len(blob) > 1200:
            continue
        if not any(c.isdigit() for c in blob):
            continue
        out.append({"endpoint_name": _tag_local(el.tag), "result": blob, "units": ""})
        if len(out) >= max_rows:
            break

    # Dedupe by (endpoint_name, result[:120])
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in out:
        key = (row["endpoint_name"], row["result"][:120])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def extract_endpoints_for_uuid(i6z_path: Path | None) -> list[dict[str, Any]]:
    if i6z_path is None or not i6z_path.is_file():
        return []
    raw = read_i6d_bytes_from_i6z(i6z_path)
    if not raw:
        return []
    return extract_endpoints_from_i6d(raw)
