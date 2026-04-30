"""
Configuration for Quick Hazard Assessment Streamlit app.
"""

from __future__ import annotations

import os

# Repo root (directory containing config.py / app.py) — use for DSS so it works on Streamlit Cloud
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
# DSSTox database lives in DSS/ (Git LFS–tracked when publishing to GitHub)
DSS_DIR = "DSS"
DSS_PATH = os.path.join(REPO_ROOT, DSS_DIR)
# Preferred mapping filename; loader also accepts any .csv or .xlsx in DSS/
DSSTOX_MAPPING_FILENAMES = ("cas_dtxsid_mapping.csv",)

# Local SQLite chemical DB (DSSTox + ToxValDB); built by scripts/setup_chemical_db.py
DATA_DIR = os.path.join(REPO_ROOT, "data")
CHEMICAL_DB_PATH = os.environ.get("CHEMICAL_DB_PATH", os.path.join(DATA_DIR, "chemical_db.sqlite"))

# OPERA precomputed batch rows (CAS → full CSV row JSON). Built by ``scripts/precompute_opera_for_cas_list.py``.
OPERA_PRECOMPUTE_DB_PATH = os.environ.get(
    "OPERA_PRECOMPUTE_DB_PATH", os.path.join(DATA_DIR, "opera_precompute.sqlite")
)

# P2OASys hazard scoring matrix (TURI). Place "Hazard Matrix Group Review 9-19-23.xlsx" in data/ or set path.
# See https://p2oasys.turi.org/chemical/hazard-score-matrix
P2OASYS_MATRIX_FILENAME = "Hazard Matrix Group Review 9-19-23.xlsx"
P2OASYS_MATRIX_PATH = os.environ.get("P2OASYS_MATRIX_PATH", os.path.join(DATA_DIR, P2OASYS_MATRIX_FILENAME))
# When the official matrix is missing, ``utils.p2oasys_matrix_placeholder`` writes this dev workbook under ``data/``.
# Set ``P2OASYS_DISABLE_AUTO_PLACEHOLDER=1`` to show "not available" instead (no auto file).
P2OASYS_PLACEHOLDER_MATRIX_FILENAME = os.environ.get(
    "P2OASYS_PLACEHOLDER_MATRIX_FILENAME", "p2oasys_matrix_dev_placeholder.xlsx"
)
# Optional lookup CSVs for P2OASys (see docs/P2OASYS_LOOKUP_SOURCES.md). Set to None to disable.
P2OASYS_IARC_CSV_PATH = os.environ.get("P2OASYS_IARC_CSV", os.path.join(DATA_DIR, "iarc_by_cas.csv"))
P2OASYS_ODP_GWP_CSV_PATH = os.environ.get("P2OASYS_ODP_GWP_CSV", os.path.join(DATA_DIR, "odp_gwp_by_cas.csv"))
# IPCC GWP 100-year (Federal LCA Commons parquet). Place files under ``data/atmo`` or set ``ATMO_DIR``.
ATMO_DIR = os.environ.get("ATMO_DIR", os.path.join(REPO_ROOT, "data", "atmo"))
# IARC classifications (CSV or Excel with CAS No. and Group). Place under ``data/iarc`` or set ``IARC_DIR``.
IARC_DIR = os.environ.get("IARC_DIR", os.path.join(REPO_ROOT, "data", "iarc"))

# COMPTOX public data folders (Excel and MySQL dump) — used by setup script
COMPTOX_EXCEL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data Excel Files Folder)", "Data Excel Files")
COMPTOX_MYSQL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data MySQL Dump File Folder)")

# App
APP_TITLE = "Quick Hazard Assessment"
ZENODO_DOI = "10.5281/zenodo.19056294"

# SDS example PDFs — place sample PDFs in ``sds_examples/`` at the repo root or set ``SDS_EXAMPLES_DIR``.
SDS_EXAMPLES_DIR = os.environ.get("SDS_EXAMPLES_DIR", os.path.join(REPO_ROOT, "sds_examples"))

# Local LLM (Ollama) — for SDS extraction/summarization when running locally (no API key).
# Not used on Streamlit Cloud. See docs/OLLAMA_SETUP.md.
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2:0.5b")  # alternative: phi3:mini, gemma2:2b

# Use local LLM (Ollama) as fallback when regex fails to find CAS, names, or concentrations.
# Set USE_LLM_CAS_EXTRACTION=1 to enable. Requires Ollama running with a small model (e.g. qwen2:0.5b).
USE_LLM_CAS_EXTRACTION = os.environ.get("USE_LLM_CAS_EXTRACTION", "").strip().lower() in (
    "1", "true", "yes", "on",
)

# Robust multi-stage CAS extractor (pdfplumber + optional Docling/OCR).
# Handles adversarial formatting: spaces around hyphens, Unicode dashes, split digits.
USE_ROBUST_CAS_EXTRACTOR = os.environ.get("USE_ROBUST_CAS_EXTRACTOR", "1").strip().lower() in (
    "1", "true", "yes", "on",
)
# Docling improves table extraction from complex SDS PDFs. Default on for reliable CAS extraction.
USE_DOCLING = os.environ.get("USE_DOCLING", "1").strip().lower() in ("1", "true", "yes", "on")

# PubChem cross-reference: validate CAS against PubChem.
USE_PUBCHEM_CAS_VALIDATION = os.environ.get("USE_PUBCHEM_CAS_VALIDATION", "1").strip().lower() in (
    "1", "true", "yes", "on",
)
# Only show CAS found in PubChem. Default 0 = show checksum-valid SDS extractions (MarkItDown/OCR)
# even when PubChem has no hit; set to 1 to hide unverified CAS.
SHOW_ONLY_PUBCHEM_VERIFIED = os.environ.get("SHOW_ONLY_PUBCHEM_VERIFIED", "0").strip().lower() in (
    "1", "true", "yes", "on",
)
# Minimum confidence (0–1) to show in SDS UI. Set to 0 to show all verified; 0.2 hides very low-confidence.
MIN_CAS_CONFIDENCE = float(os.environ.get("MIN_CAS_CONFIDENCE", "0.0"))

# Reconstructor: run ONLY when table/regex find zero CAS. Prevents "making up" CAS from random digit sequences.
USE_RECONSTRUCTOR_AS_FALLBACK_ONLY = os.environ.get("USE_RECONSTRUCTOR_AS_FALLBACK_ONLY", "1").strip().lower() in (
    "1", "true", "yes", "on",
)
RECONSTRUCTOR_MAX_GAP = int(os.environ.get("RECONSTRUCTOR_MAX_GAP", "10"))
# Require CAS-like context (composition, ingredient, CAS, etc.) when reconstructing — avoids fake CAS from unrelated digits.
RECONSTRUCTOR_USE_CONTEXT_FILTER = os.environ.get("RECONSTRUCTOR_USE_CONTEXT_FILTER", "1").strip().lower() in (
    "1", "true", "yes", "on",
)
USE_OCR = os.environ.get("USE_OCR", "").strip().lower() in ("1", "true", "yes", "on")

# SDS CAS upload (Streamlit): ``markitdown_fast``, ``hybrid_md_ocr``, or ``markdown_gliner_regex``
# (optional GLiNER2; see requirements-gliner2.txt). See docs/SDS_EXTRACTION_PIPELINES.md.
# Legacy values (e.g. ``default``) are normalized to hybrid at runtime.
DEFAULT_SDS_EXTRACTION_PIPELINE = (
    (os.environ.get("HAZQUERY_DEFAULT_SDS_PIPELINE") or "hybrid_md_ocr").strip() or "hybrid_md_ocr"
)

# EPA CompTox / CCTE APIs (ToxVal, chemical search). Same key as historical COMPTOX_API_KEY.
EPA_API_KEY = (os.environ.get("EPA_API_KEY") or os.environ.get("COMPTOX_API_KEY") or "").strip() or None

# Optional: path to a P2OASys-style CSV (CAS column) to offer “load CAS list” in cross-validation UI.
P2OASYS_COMPOUND_LIST_CSV = (os.environ.get("P2OASYS_COMPOUND_LIST_CSV") or "").strip() or None

# Local NIEHS OPERA 2.9 (command-line build). https://github.com/kmansouri/OPERA/releases
# Prefer …\\OPERA\\application\\OPERA.exe (non-parallel); OPERA_P.exe can fail CDK on some inputs.
HAZQUERY_OPERA_EXE = (os.environ.get("HAZQUERY_OPERA_EXE") or os.environ.get("OPERA_EXE") or "").strip() or None
OPERA_JAVA_HOME = (os.environ.get("OPERA_JAVA_HOME") or "").strip() or None
try:
    OPERA_TIMEOUT_SECONDS = max(30, int(os.environ.get("OPERA_TIMEOUT_SECONDS", "600")))
except (TypeError, ValueError):
    OPERA_TIMEOUT_SECONDS = 600

# QSAR Toolbox (OECD + VEGA/OPERA) — local WebSuite must be running. Windows only.
# Set port (e.g. 51946) or leave None to disable. See https://github.com/glsalierno/PyQSARToolbox
QSAR_TOOLBOX_PORT = os.environ.get("QSAR_TOOLBOX_PORT", None)
if QSAR_TOOLBOX_PORT is not None:
    try:
        QSAR_TOOLBOX_PORT = int(QSAR_TOOLBOX_PORT)
    except (TypeError, ValueError):
        QSAR_TOOLBOX_PORT = None

# Optional IUCLID format package folder for decoding phrase/picklist codes (extracted format bundle).
# Leave unset to skip phrase decoding beyond built-in fallbacks; set ``IUCLID_FORMAT_DIR`` to the extracted folder.
IUCLID_FORMAT_DIR = os.environ.get("IUCLID_FORMAT_DIR", "") or ""

# Example chemicals for quick buttons (CAS, label)
EXAMPLE_CHEMICALS = [
    ("67-64-1", "67-64-1 (Acetone)"),
    ("64-17-5", "64-17-5 (Ethanol)"),
    ("71-43-2", "71-43-2 (Benzene)"),
    ("50-00-0", "50-00-0 (Formaldehyde)"),
]

# REACH demo zip: hand-picked substances known to appear in ``data/reach_demo/reach_subset.zip`` (v2.0 shortcuts).
# Tuple format matches ``EXAMPLE_CHEMICALS``: (CAS, button label).
REACH_DEMO_CURATED_EXAMPLES = [
    ("75-77-4", "Chlorotrimethylsilane (CAS 75-77-4)"),
    ("7631-86-9", "Amorphous Silica (CAS 7631-86-9)"),
    ("544-63-8", "Myristic Acid (CAS 544-63-8)"),
    ("554-13-2", "Lithium Carbonate (CAS 554-13-2)"),
]
