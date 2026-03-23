"""
MarkItDown PDF → Markdown, table extraction, CAS via regex and optional DistilBERT on cells.
"""

from __future__ import annotations

import logging
import re
import tempfile
from io import StringIO
from pathlib import Path
from typing import Any, Optional

# Optional: pandas for pipe-separated table parsing
try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[misc, assignment]

from utils import cas_text_extract
from utils.cache_manager import ExtractionCacheManager

logger = logging.getLogger(__name__)

# Markdown pipe tables: header | sep | body rows
_MD_TABLE_SEP = re.compile(r"^\s*\|?[\s\-:|]+\|\s*$")


def _markdown_table_blocks(text: str) -> list[str]:
    """Split markdown into contiguous table-like blocks (| ... | lines)."""
    if not text:
        return []
    lines = text.replace("\r\n", "\n").split("\n")
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        line = line.rstrip()
        if "|" in line and line.strip().startswith("|"):
            current.append(line)
        else:
            if len(current) >= 2:
                blocks.append(current)
            current = []
    if len(current) >= 2:
        blocks.append(current)
    return ["\n".join(b) for b in blocks]


def _parse_markdown_table_block(block: str) -> Optional[Any]:
    """Return DataFrame of a pipe table block, or None."""
    if pd is None:
        return None
    lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
    if len(lines) < 2:
        return None
    # Skip separator line
    body: list[str] = []
    for i, ln in enumerate(lines):
        if _MD_TABLE_SEP.match(ln):
            continue
        body.append(ln)
    if not body:
        return None
    try:
        # Normalize: strip outer pipes for read_csv
        norm_lines: list[str] = []
        for ln in body:
            s = ln.strip()
            if s.startswith("|"):
                s = s[1:]
            if s.endswith("|"):
                s = s[:-1]
            norm_lines.append(s)
        buf = "\n".join(norm_lines)
        return pd.read_csv(StringIO(buf), sep="|")
    except Exception as e:
        logger.debug("markdown table parse failed: %s", e)
        return None


def extract_tables_from_markdown(markdown_text: str) -> list[Any]:
    """Return list of pandas DataFrames (may be empty if pandas missing)."""
    out: list[Any] = []
    if pd is None or not markdown_text:
        return out
    for block in _markdown_table_blocks(markdown_text):
        df = _parse_markdown_table_block(block)
        if df is not None and not df.empty:
            out.append(df)
    return out


def iter_cells_from_dataframes(dfs: list[Any]) -> list[str]:
    cells: list[str] = []
    for df in dfs:
        try:
            for col in df.columns:
                for val in df[col].dropna():
                    s = str(val).strip()
                    if len(s) >= 2:
                        cells.append(s)
        except Exception:
            continue
    return cells


class MarkItDownParser:
    """
    Convert PDF to Markdown via MarkItDown; extract CAS from full text + table cells.
    Optional DistilBERT on cells when ``use_bert`` and extractor is provided.
    """

    def __init__(
        self,
        *,
        use_bert: bool = False,
        bert_extractor: Any = None,
        cache: Optional[ExtractionCacheManager] = None,
    ) -> None:
        self.use_bert = bool(use_bert)
        self.bert_extractor = bert_extractor
        self.cache = cache
        self._md: Any = None
        self._last_fingerprint: str = ""

    def _get_markitdown(self) -> Any:
        if self._md is not None:
            return self._md
        from utils.markitdown_check import require_markitdown

        require_markitdown()
        from markitdown import MarkItDown
        self._md = MarkItDown(enable_plugins=False)
        return self._md

    def convert_pdf_to_markdown(
        self,
        pdf_bytes: bytes,
        fingerprint: str,
        *,
        force: bool = False,
    ) -> str:
        self._last_fingerprint = fingerprint
        if self.cache and not force:
            cached = self.cache.load_text(fingerprint, "markitdown.md")
            if cached is not None:
                return cached

        md = self._get_markitdown()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            path = Path(tmp.name)
        try:
            result = md.convert(str(path))
            text = (getattr(result, "text_content", None) or "") or ""
        finally:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass

        if self.cache:
            self.cache.save_text(fingerprint, "markitdown.md", text)
        return text

    def extract_cas_regex_from_text(self, text: str) -> list[str]:
        return cas_text_extract.find_checksum_valid_cas_in_text(text)

    def extract_cas_from_markdown(
        self,
        markdown_text: str,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """
        Returns (unique cas list, detail rows for debugging).
        """
        found: list[str] = []
        seen: set[str] = set()
        details: list[dict[str, Any]] = []

        # Full-document regex
        for cas in self.extract_cas_regex_from_text(markdown_text):
            if cas not in seen:
                seen.add(cas)
                found.append(cas)
                details.append({"cas": cas, "source": "regex_body", "confidence": 0.85})

        dfs = extract_tables_from_markdown(markdown_text)
        if self.cache and self._last_fingerprint:
            try:
                ser = []
                for df in dfs:
                    try:
                        ser.append(df.to_dict(orient="records"))
                    except Exception:
                        ser.append([])
                self.cache.save_json(self._last_fingerprint, "tables.json", ser)
            except Exception:
                pass

        cells = iter_cells_from_dataframes(dfs)
        for cell in cells:
            for cas in self.extract_cas_regex_from_text(cell):
                if cas not in seen:
                    seen.add(cas)
                    found.append(cas)
                    details.append({"cas": cas, "source": "regex_table_cell", "confidence": 0.88})

        # Optional BERT on cells that might contain CAS (short cells or regex miss)
        if self.use_bert and self.bert_extractor is not None:
            classify = getattr(self.bert_extractor, "classify_cell_text", None)
            if callable(classify):
                for cell in cells:
                    if len(cell) < 4:
                        continue
                    try:
                        res = classify(cell)
                    except Exception:
                        continue
                    if res is None:
                        continue
                    cas = (getattr(res, "cas", None) or "").strip()
                    if not cas or cas in seen:
                        continue
                    seen.add(cas)
                    found.append(cas)
                    conf = float(getattr(res, "confidence", 0.0) or 0.0)
                    details.append(
                        {
                            "cas": cas,
                            "source": "markitdown_bert_cell",
                            "confidence": conf,
                        }
                    )

        return found, details
