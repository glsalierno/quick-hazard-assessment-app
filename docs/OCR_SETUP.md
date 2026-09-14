# OCR setup for scanned SDS PDFs

The app and `scripts/run_sds_examples.py` extract text from PDFs in two steps:

1. **Embedded text** (pypdf) — used first.
2. **OCR** — used automatically when embedded text is short (< 250 characters), e.g. scanned PDFs.

OCR uses **Tesseract** and **pdf2image** (Poppler). **EasyOCR** runs as a fallback for pages where Tesseract returns very little text.

---

## 1. System dependencies

### Tesseract

- **Windows:** Install from [UB-Mannheim/tesseract](https://github.com/UB-Mannheim/tesseract/wiki) and add the `tesseract.exe` directory to `PATH` (e.g. `C:\Program Files\Tesseract-OCR`).
- **macOS:** `brew install tesseract`
- **Linux:** `sudo apt install tesseract-ocr` (or equivalent)

Check:

```bash
tesseract --version
```

### Poppler (for pdf2image)

pdf2image needs Poppler to render PDF pages to images. If you see **`Unable to get page count. Is poppler installed and in PATH?`**, Poppler is missing or not found.

- **Windows (recommended):**
  1. Download a release from [Poppler for Windows](https://github.com/oschwartz10612/poppler-windows/releases/) and extract it (e.g. to `C:\Tools\poppler`).
  2. Find the **`bin`** folder that contains `pdfinfo.exe` and `pdftoppm.exe` (often `...\poppler-xx\Library\bin`).
  3. Either:
     - Add that **`bin`** folder to your user **PATH**, **restart the terminal**, then run the app again, **or**
     - Set an environment variable (no PATH edit):  
       `HAZQUERY_POPPLER_PATH=C:\Tools\poppler\Library\bin`  
       (use your real path; must be the **`bin`** directory).
  4. Optional: in PowerShell for one session only:  
     `$env:HAZQUERY_POPPLER_PATH = "C:\Tools\poppler\Library\bin"`
- **Alternative:** `conda install -c conda-forge poppler` (then ensure Conda’s `bin` is on PATH when you run Python).
- **macOS:** `brew install poppler`
- **Linux:** `sudo apt install poppler-utils`

Check (after PATH or env is set):

```bash
pdfinfo -v
```

### Faster OCR (optional)

- Env **`HAZQUERY_OCR_DPI`** (default **200**): lower values (e.g. **120**–**150**) speed up `pdf2image` + Tesseract at the cost of slightly worse text for scans.
- Batch script: `python run_batch_extract.py --fast --ocr-dpi 150`

---

## 2. Python dependencies

From the repo root:

```bash
pip install pdf2image pytesseract easyocr
```

These are already in `requirements.txt`. If you skip OCR, the app still runs and uses only embedded text (scanned PDFs will show “no text” unless you install the above).

---

## 3. EasyOCR (optional fallback)

EasyOCR improves results on some pages when Tesseract returns very little text. Install with:

```bash
pip install easyocr
```

First run may download model data. On CPU (e.g. Core i7) it can be slow; it is only used as a fallback per page.

---

## 4. Optional: searchable PDFs (ocrmypdf)

To **add a text layer** to a PDF (so it becomes searchable and our embedded-text path can use it later):

```bash
pip install ocrmypdf
```

Then use the helper (e.g. from a script):

```python
from utils.sds_pdf_utils import make_searchable_pdf
make_searchable_pdf("input.pdf", "output_searchable.pdf", language="eng")
```

Or the command line:

```bash
ocrmypdf -l eng input.pdf output_searchable.pdf
```

Tesseract must be on `PATH` for ocrmypdf.

---

## 5. Flow summary

| Step | What runs |
|------|------------|
| 1 | Extract text with pypdf (embedded text). |
| 2 | If total length < 250 chars and `pdf2image` + `pytesseract` are available, render PDF to images and run Tesseract on each page. |
| 3 | For any page where Tesseract returns < 50 chars, run EasyOCR on that page if `easyocr` is installed. |
| 4 | Concatenate all page text and run the usual SDS regex extraction. |

No API keys are required; everything runs locally.
