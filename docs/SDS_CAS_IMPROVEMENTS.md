# Improvement opportunities: guarantee CAS extraction

Goal: maximize the chance of extracting **at least one CAS** from every SDS (or nearly every one).

---

## 1. **Pattern and context**

| Opportunity | Description | Impact |
|-------------|-------------|--------|
| **Allow single-digit first part when after "CAS"** | Real CAS can be e.g. `9-20-2`. We currently skip first part length 1 to avoid ratios like `2-14-3`. Allow 1-digit first part only when the match is from "CAS No." / "CAS Number" / "Registry" (same or next line). | Recovers CAS in strict tables. |
| **More label patterns** | Add: `Registry No.`, `EC No.`, `EC/CAS`, `CAS Registry`, `Chemical Abstracts Service` followed by N-N-N on same or next line. | Handles EU/other SDS wording. |
| **Same-line "CAS" with number later** | Pattern: line containing "CAS" and elsewhere on the same line a N-N-N (e.g. "Product CAS 75-45-3"). Broaden `_CAS_AFTER_LABEL_RE` or add a pattern that allows text between "CAS" and the number. | Handles one-line layout. |

---

## 2. **Section 3 prioritization**

- Many SDS put CAS only in **Section 3 (Composition/identity)**.
- Extract the block between "Section 3" / "3. Composition" / "3.1" and the next "Section 4" / "4." (or next major section).
- Run CAS patterns **first** on this block; if any CAS found, use them (and optionally merge with CAS from full text to get mixtures).
- Ensures we don’t miss CAS that appear only in that section with different formatting.

---

## 3. **OCR and normalization**

| Opportunity | Description | Impact |
|-------------|-------------|--------|
| **Letter–digit normalization** | In potential CAS spans, try: `O`→`0`, `l`/`I`→`1`, `S`→`5`, `Z`→`2` (only in digit-like context) and re-validate. | Recovers CAS from OCR. |
| **Ensure OCR path runs** | When embedded text is short, OCR (Tesseract + EasyOCR) must run. Verify Poppler/Tesseract on PATH and that `extract_text_from_pdf_bytes` actually calls OCR when text length < threshold. | Recovers scanned-only SDS. |

---

## 4. **Validation and fallback**

| Opportunity | Description | Impact |
|-------------|-------------|--------|
| **Format-valid fallback** | If no checksum-valid CAS is found, optionally return format-valid N-N-N (with a `check_digit_valid: false` flag) so the app can still try PubChem lookup. | Last resort when check digit is wrong in PDF. |
| **Check-digit correction** | Already done: `validate_cas_relaxed` corrects the check digit when format is N-N-N. Keeps using it. | Already improves yield. |

---

## 5. **Order and deduplication**

- When multiple CAS are found, **rank** by: (1) appeared after "CAS" / "Registry" label, (2) in Section 3 block, (3) elsewhere.
- Return the list in that order so "first CAS" is most likely the main substance for v1.3 lookup.

---

## 6. **Implemented (this pass)**

- **Single-digit first part:** Allowed only when the match comes from label/context patterns: after "CAS", "Registry No.", "EC No.", next-line, or same-line CAS (avoids ratio false positives).
- **New patterns:** `_CAS_REGISTRY_EC_RE` (Registry No., EC No., EC-CAS, CAS Registry); `_CAS_SAME_LINE_RE` (CAS ... up to 80 chars ... N-N-N).
- **Section 3 block:** `_extract_section3_block()` finds text between Section 3/3.1/Composition and Section 4/5; CAS is extracted from this block first, then merged with full-text CAS (Section 3 first in order).
- **OCR-style normalization:** `_try_ocr_normalize_cas_part()` (O/o→0, l/I→1, S→5, Z→2) applied to hyphen-style candidates via `_cas_candidates_with_ocr_fallback()`; used by default in `_extract_cas_numbers(use_ocr_fallback=True)`.
- **Order/deduplication:** Section 3 candidates first, then full-text; merged without duplicates. Final list order favors Section 3 and label-sourced CAS.

**Status:** Code in `utils/sds_regex_extractor.py`. Run `python scripts/run_sds_examples.py --limit N` to verify on your SDS set.

---

## 7. **Focused CAS extraction (CAS recognition essential)**

To make CAS recognition **essential** and always prompt database lookup when a CAS-like value appears in the SDS:

| Option | Description | Status |
|--------|-------------|--------|
| **Multi-section blocks** | Run CAS patterns on Section 1, 2, 3, and 15 (not only Section 3), then full text. | Implemented: `_extract_section_block()`, `_ordered_cas_candidates_focused()`. |
| **Extra SDS-specific patterns** | "Chemical Abstracts Service", "CAS Registry Number", table-style "CAS" column header, "Product identifier" / "Substance" / "Trade name" + N-N-N, "Item No." / "Article No." / "Reach No." | Implemented: `_CAS_CHEMICAL_ABSTRACTS_RE`, `_CAS_COLUMN_HEADER_RE`, `_CAS_PRODUCT_IDENTIFIER_RE`, `_CAS_ITEM_ARTICLE_RE`; used in `_cas_candidates_from_text_focused()`. |
| **Format-valid fallback** | If no checksum-valid CAS is found, return format-valid N-N-N so the app still shows the CAS dropdown and allows database lookup. | Implemented: `extract_cas_focused(..., include_format_valid_fallback=True)`. |
| **Run when standard finds nothing** | After `_extract_cas_numbers()`, if the list is empty, call `extract_cas_focused()` and use its result so `legacy["cas_numbers"]` is set whenever any CAS-like string exists. | Implemented in `extract_sds_fields_from_text()`. |
| **UI hint for extended extraction** | When CAS came from focused extraction, show a caption so the user knows they can still run lookup and that check digit may be unverified. | Implemented: `legacy["meta"]["cas_from_focused_extraction"]` and app caption in Comparison tab. |

**Usage:** No API change. Upload an SDS PDF as before; if the standard extractor finds no CAS, the focused extractor runs automatically and the "CAS for PubChem comparison" dropdown appears when any CAS-like value is found. Run `python scripts/run_sds_examples.py --limit N` or use the app to verify.
