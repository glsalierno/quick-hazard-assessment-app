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
CHEMICAL_DB_PATH = os.path.join(DATA_DIR, "chemical_db.sqlite")

# P2OASys hazard scoring matrix (TURI). Place "Hazard Matrix Group Review 9-19-23.xlsx" in data/ or set path.
# See https://p2oasys.turi.org/chemical/hazard-score-matrix
P2OASYS_MATRIX_FILENAME = "Hazard Matrix Group Review 9-19-23.xlsx"
P2OASYS_MATRIX_PATH = os.path.join(DATA_DIR, P2OASYS_MATRIX_FILENAME)
# Optional lookup CSVs for P2OASys (see docs/P2OASYS_LOOKUP_SOURCES.md). Set to None to disable.
P2OASYS_IARC_CSV_PATH = os.environ.get("P2OASYS_IARC_CSV", os.path.join(DATA_DIR, "iarc_by_cas.csv"))
P2OASYS_ODP_GWP_CSV_PATH = os.environ.get("P2OASYS_ODP_GWP_CSV", os.path.join(DATA_DIR, "odp_gwp_by_cas.csv"))
# IPCC GWP 100-year from atmo folder (Federal LCA Commons parquet). Default: fastP2OASys/atmo.
ATMO_DIR = os.environ.get("ATMO_DIR", os.path.join(REPO_ROOT, "..", "..", "fastP2OASys", "atmo"))
# IARC classifications from iarc folder (CSV or Excel with CAS No. and Group). Default: fastP2OASys/iarc.
IARC_DIR = os.environ.get("IARC_DIR", os.path.join(REPO_ROOT, "..", "..", "fastP2OASys", "iarc"))

# COMPTOX public data folders (Excel and MySQL dump) — used by setup script
COMPTOX_EXCEL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data Excel Files Folder)", "Data Excel Files")
COMPTOX_MYSQL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data MySQL Dump File Folder)")

# App
APP_TITLE = "Quick Hazard Assessment"
ZENODO_DOI = "10.5281/zenodo.19056294"

# SDS example PDFs (sibling folder when running from GHhaz4/quick-hazard-assessment-app)
SDS_EXAMPLES_DIR = os.environ.get("SDS_EXAMPLES_DIR", os.path.join(REPO_ROOT, "..", "sds examples"))

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
# Only show CAS found in PubChem. Default 1 = strict: no invalid or unverified CAS shown.
# Set to 0 to show all extracted (may include unverified when PubChem times out).
SHOW_ONLY_PUBCHEM_VERIFIED = os.environ.get("SHOW_ONLY_PUBCHEM_VERIFIED", "1").strip().lower() in (
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

# SDS CAS upload (Streamlit): only ``markitdown_fast`` or ``hybrid_md_ocr`` (see docs/SDS_EXTRACTION_PIPELINES.md).
# Legacy values (e.g. ``default``) are normalized to hybrid at runtime.
DEFAULT_SDS_EXTRACTION_PIPELINE = (
    (os.environ.get("HAZQUERY_DEFAULT_SDS_PIPELINE") or "hybrid_md_ocr").strip() or "hybrid_md_ocr"
)

# QSAR Toolbox (OECD + VEGA/OPERA) — local WebSuite must be running. Windows only.
# Set port (e.g. 51946) or leave None to disable. See https://github.com/glsalierno/PyQSARToolbox
QSAR_TOOLBOX_PORT = os.environ.get("QSAR_TOOLBOX_PORT", None)
if QSAR_TOOLBOX_PORT is not None:
    try:
        QSAR_TOOLBOX_PORT = int(QSAR_TOOLBOX_PORT)
    except (TypeError, ValueError):
        QSAR_TOOLBOX_PORT = None

# Example chemicals for quick buttons (CAS, label)
EXAMPLE_CHEMICALS = [
    ("67-64-1", "67-64-1 (Acetone)"),
    ("64-17-5", "64-17-5 (Ethanol)"),
    ("71-43-2", "71-43-2 (Benzene)"),
    ("50-00-0", "50-00-0 (Formaldehyde)"),
]
