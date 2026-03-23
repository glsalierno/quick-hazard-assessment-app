"""
Pure Docling + DistilBERT CAS extraction (inference).

- Docling: PDF structure and table cells (reuse project converter).
- DistilBERT token classification: CAS vs non-CAS tokens; spans are decoded from labels.
- No ``re`` module / regex patterns in this file.

Install (if not already): ``torch``, ``transformers`` (see requirements.txt).
Optional fine-tuned weights: ``models/cas_bert`` or ``HAZQUERY_CAS_BERT_MODEL`` env path.
Untrained base checkpoints will rarely pass checksum validation and usually yield no CAS rows.
"""

from __future__ import annotations

import gc
import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from utils.hf_transformers_compat import dtype_kw_for_from_pretrained
from utils.docling_sds_parser import (
    DocumentStream,
    build_docling_converter,
    get_batch_low_memory_converter,
    get_cached_docling_converter,
    is_docling_available,
    is_docling_disabled,
)

# --- Optional torch / transformers (lazy) ---
_TORCH_IMPORT_ERROR: Optional[str] = None
try:
    import torch
    import torch.nn.functional as F
    from transformers import DistilBertForTokenClassification, DistilBertTokenizerFast
except ImportError as e:
    torch = None  # type: ignore[misc, assignment]
    F = None  # type: ignore[misc, assignment]
    DistilBertForTokenClassification = None  # type: ignore[misc, assignment]
    DistilBertTokenizerFast = None  # type: ignore[misc, assignment]
    _TORCH_IMPORT_ERROR = str(e)

_LABEL_O = 0
_LABEL_CAS = 1


@dataclass
class CASResult:
    cas: str
    chemical_name: Optional[str] = None
    concentration: Optional[str] = None
    confidence: float = 0.0
    source_page: Optional[int] = None
    source_table: Optional[int] = None


def _default_model_dir() -> Path:
    env = os.getenv("HAZQUERY_CAS_BERT_MODEL", "").strip()
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent / "models" / "cas_bert"


def _device():
    if torch is None:
        raise RuntimeError("torch not installed")
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _validate_cas_checksum_parts(part1: str, part2: str, check_digit: str) -> bool:
    """
    Standard CAS Registry check digit (no regex).
    Weights apply to the first block + second block only (last digit is the check).
    Note: ``cas_validator.cas_checksum`` uses a different weighting and must not be used here.
    """
    if not (part1.isdigit() and part2.isdigit() and check_digit.isdigit()):
        return False
    if len(part2) != 2 or len(check_digit) != 1:
        return False
    if not (1 <= len(part1) <= 9):
        return False
    main = part1 + part2
    check = int(check_digit)
    total = 0
    for i, d in enumerate(reversed(main), 1):
        total += int(d) * i
    return (total % 10) == check


def _normalize_cas_from_cell_fragment(fragment: str) -> Optional[str]:
    """
    Turn a labeled substring into N-N-N if it matches CAS shape and checksum.
    Allows extra spaces around hyphens.
    """
    if not fragment:
        return None
    # Keep digits and hyphens only (character loop, not regex)
    compact_chars: list[str] = []
    for ch in fragment:
        if ch.isdigit() or ch == "-":
            compact_chars.append(ch)
    compact = "".join(compact_chars)
    parts: list[str] = []
    buf: list[str] = []
    for ch in compact:
        if ch == "-":
            if buf:
                parts.append("".join(buf))
                buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    if len(parts) != 3:
        return None
    a, b, c = parts[0], parts[1], parts[2]
    if _validate_cas_checksum_parts(a, b, c):
        return f"{a}-{b}-{c}"
    return None


def _cell_confidence_from_logits(logits: Any, cas_mask: list[bool]) -> float:
    """Mean softmax probability of CAS label on tokens marked as part of span."""
    if torch is None or F is None:
        return 0.0
    probs = F.softmax(logits[0], dim=-1)  # [seq, num_labels]
    if cas_mask and len(cas_mask) == probs.shape[0]:
        idx = torch.tensor(cas_mask, device=probs.device, dtype=torch.bool)
        if bool(idx.any().item()):
            return float(probs[idx, 1].mean().item())
    return float(probs[:, 1].mean().item())


def _decode_labeled_span(
    text: str,
    offset_mapping: list[Any],
    predictions: list[int],
) -> tuple[Optional[str], list[bool], float]:
    """
    Build character mask from token labels and slice text.
    Returns (cas_or_none, token_cas_mask for confidence, raw_mean_cas_prob placeholder).
    """
    n = len(text)
    if n == 0:
        return None, [], 0.0
    char_cas = [False] * n
    cas_mask: list[bool] = []
    for i, off in enumerate(offset_mapping):
        if isinstance(off, (list, tuple)) and len(off) >= 2:
            start, end = off[0], off[1]
        else:
            start, end = None, None
        lab = predictions[i] if i < len(predictions) else _LABEL_O
        is_cas = lab == _LABEL_CAS
        cas_mask.append(is_cas)
        if start is None or end is None:
            continue
        if isinstance(start, int) and isinstance(end, int):
            for pos in range(max(0, start), min(n, end)):
                if is_cas:
                    char_cas[pos] = True
    # Expand to runs
    if not any(char_cas):
        return None, cas_mask, 0.0
    out_chars: list[str] = []
    for i, ch in enumerate(text):
        if char_cas[i]:
            out_chars.append(ch)
    fragment = "".join(out_chars).strip()
    cas = _normalize_cas_from_cell_fragment(fragment)
    return cas, cas_mask, 0.0


def _find_chemical_name_cell(cells: list[str], skip_idx: int) -> Optional[str]:
    """Longest mostly-alphabetic cell in row (structural heuristic, no regex)."""
    best: Optional[tuple[int, str]] = None
    for idx, raw in enumerate(cells):
        if idx == skip_idx:
            continue
        part = raw.strip()
        if len(part) < 3:
            continue
        letters = sum(1 for c in part if c.isalpha())
        digits = sum(1 for c in part if c.isdigit())
        if letters == 0:
            continue
        if digits > letters:
            continue
        score = len(part)
        if best is None or score > best[0]:
            best = (score, part)
    return best[1] if best else None


def _find_concentration_cell(cells: list[str]) -> Optional[str]:
    """Prefer cells containing % / ％ / wt (substring match, no regex)."""
    for raw in cells:
        s = raw.strip()
        low = s.lower()
        if "%" in s or "％" in s:
            return s
        if "wt" in low or "weight" in low or "conc" in low:
            return s
    return None


class PureCASExtractor:
    """
    Docling for layout/tables; DistilBERT token classification for CAS spans.
    No regex in this module.
    """

    def __init__(
        self,
        *,
        model_dir: Optional[Path] = None,
        confidence_threshold: float = 0.75,
        use_streamlit_converter_cache: bool = True,
        low_memory_docling: bool = False,
    ) -> None:
        self.model_dir = Path(model_dir) if model_dir else _default_model_dir()
        self.confidence_threshold = confidence_threshold
        self.use_streamlit_converter_cache = use_streamlit_converter_cache
        self.low_memory_docling = bool(low_memory_docling) or os.getenv(
            "HAZQUERY_DOCLING_LOW_MEMORY", ""
        ).strip().lower() in ("1", "true", "yes", "on")
        self._docling_converter: Optional[Any] = None
        self.device: Any = None
        self.tokenizer = None
        self.model = None
        self._load_model()

    def _load_model(self) -> None:
        if torch is None or DistilBertTokenizerFast is None or DistilBertForTokenClassification is None:
            return
        self.device = _device()
        base = "distilbert-base-uncased"
        has_ckpt = False
        if self.model_dir.is_dir():
            try:
                has_ckpt = any(self.model_dir.iterdir())
            except OSError:
                has_ckpt = False
        _dt = dtype_kw_for_from_pretrained(DistilBertForTokenClassification, torch.float32)
        if has_ckpt:
            self.tokenizer = DistilBertTokenizerFast.from_pretrained(str(self.model_dir))
            self.model = DistilBertForTokenClassification.from_pretrained(
                str(self.model_dir), **_dt
            )
        else:
            self.tokenizer = DistilBertTokenizerFast.from_pretrained(base)
            self.model = DistilBertForTokenClassification.from_pretrained(
                base,
                num_labels=2,
                **_dt,
            )
        self.model.to(self.device)
        self.model.eval()

    def _get_converter(self) -> Any:
        if self.use_streamlit_converter_cache:
            return get_cached_docling_converter()
        if self._docling_converter is not None:
            return self._docling_converter
        if self.low_memory_docling:
            self._docling_converter = get_batch_low_memory_converter()
        else:
            self._docling_converter = build_docling_converter(low_memory=False)
        return self._docling_converter

    def extract(self, pdf_bytes: bytes) -> list[CASResult]:
        results: list[CASResult] = []
        if not pdf_bytes:
            return results
        if is_docling_disabled() or not is_docling_available():
            return results
        if self.model is None or self.tokenizer is None:
            return results
        if DocumentStream is None:
            return results

        converter = self._get_converter()
        if converter is None:
            return results

        buf = BytesIO(pdf_bytes)
        source = DocumentStream(name="sds.pdf", stream=buf)
        try:
            conv_res = converter.convert(source)
        except Exception:
            return results

        doc = conv_res.document
        tables = getattr(doc, "tables", None) or []

        for table_idx, table in enumerate(tables):
            try:
                try:
                    table_df = table.export_to_dataframe(doc=doc)
                except TypeError:
                    table_df = table.export_to_dataframe()
            except Exception:
                continue

            page_no: Optional[int] = None
            try:
                prov = getattr(table, "prov", None)
                if prov and len(prov) > 0:
                    page_no = getattr(prov[0], "page_no", None)
            except Exception:
                page_no = None

            try:
                for row_idx in range(len(table_df)):
                    row = table_df.iloc[row_idx]
                    cells = [str(x).strip() if x is not None else "" for x in row.tolist()]
                    for cell_idx, cell_text in enumerate(cells):
                        if len(cell_text) < 3:
                            continue
                        cas_res = self._classify_cell(
                            cell_text=cell_text,
                            cells=cells,
                            cell_idx=cell_idx,
                            table_idx=table_idx,
                            page_no=page_no,
                        )
                        if cas_res:
                            results.append(cas_res)
            except Exception:
                continue

        return self._deduplicate(results)

    def classify_cell_text(self, cell_text: str) -> Optional[CASResult]:
        """
        Run DistilBERT token classification on a single text snippet (e.g. MarkItDown table cell).
        """
        if not cell_text or len(str(cell_text).strip()) < 3:
            return None
        if self.model is None or self.tokenizer is None:
            return None
        return self._classify_cell(
            cell_text=str(cell_text).strip(),
            cells=[str(cell_text).strip()],
            cell_idx=0,
            table_idx=-1,
            page_no=None,
        )

    def _classify_cell(
        self,
        *,
        cell_text: str,
        cells: list[str],
        cell_idx: int,
        table_idx: int,
        page_no: Optional[int],
    ) -> Optional[CASResult]:
        assert self.tokenizer is not None and self.model is not None and torch is not None
        enc = self.tokenizer(
            cell_text,
            return_tensors="pt",
            truncation=True,
            max_length=128,
            padding="max_length",
            return_offsets_mapping=True,
        )
        offset_mapping = enc.pop("offset_mapping")[0].tolist()
        enc = {k: v.to(self.device) for k, v in enc.items()}

        with torch.no_grad():
            outputs = self.model(**enc)

        logits = outputs.logits
        predictions = logits[0].argmax(dim=-1).tolist()
        cas, cas_mask, _ = _decode_labeled_span(cell_text, offset_mapping, predictions)
        if not cas:
            return None

        conf = _cell_confidence_from_logits(logits, cas_mask)
        if conf < self.confidence_threshold:
            return None

        name = _find_chemical_name_cell(cells, cell_idx)
        conc = _find_concentration_cell(cells)

        return CASResult(
            cas=cas,
            chemical_name=name,
            concentration=conc,
            confidence=conf,
            source_page=page_no,
            source_table=table_idx,
        )

    def _deduplicate(self, results: list[CASResult]) -> list[CASResult]:
        best: dict[str, CASResult] = {}
        for r in results:
            cur = best.get(r.cas)
            if cur is None or r.confidence > cur.confidence:
                best[r.cas] = r
        return list(best.values())


class MemoryOptimizedPureCASExtractor(PureCASExtractor):
    """Same pipeline with explicit GPU cache / GC (optional for Streamlit Cloud)."""

    def extract(self, pdf_bytes: bytes) -> list[CASResult]:
        try:
            return super().extract(pdf_bytes)
        finally:
            if torch and torch.cuda.is_available():
                torch.cuda.empty_cache()
            gc.collect()


def is_pure_cas_bert_available() -> str:
    if is_docling_disabled():
        return "Docling disabled (HAZQUERY_DISABLE_DOCLING)."
    if not is_docling_available():
        return "Docling not available."
    if _TORCH_IMPORT_ERROR:
        return f"torch/transformers missing ({_TORCH_IMPORT_ERROR[:120]})."
    return "Docling + DistilBERT CAS path available."


def get_cas_extractor(**kwargs: Any) -> PureCASExtractor:
    """Alias expected by app integration (single shared loaded model)."""
    return get_pure_cas_extractor(**kwargs)


def get_pure_cas_extractor(**kwargs: Any) -> PureCASExtractor:
    """Prefer memory-optimized wrapper on small instances; single shared model."""
    use_mem = os.getenv("HAZQUERY_CAS_BERT_GC", "1").strip().lower() in ("1", "true", "yes", "on")
    cls = MemoryOptimizedPureCASExtractor if use_mem else PureCASExtractor
    try:
        import streamlit as st

        @st.cache_resource(show_spinner=False)
        def _make() -> PureCASExtractor:
            return cls(**kwargs)

        return _make()
    except Exception:
        return cls(**kwargs)
