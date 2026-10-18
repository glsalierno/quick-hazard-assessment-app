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

# ECOSAR (EPI Suite API) aquatic predictions cache. Filled on demand by ``utils.ecosar_client``.
ECOSAR_CACHE_DB_PATH = os.environ.get(
    "ECOSAR_CACHE_DB_PATH", os.path.join(DATA_DIR, "ecosar_cache.sqlite")
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
# Official-matrix gate: when True (default), assessment *export* of placeholder scores requires an
# explicit allow-placeholder acknowledgement. Scoring for layout testing still works with a warning.
P2OASYS_REQUIRE_OFFICIAL_MATRIX = os.environ.get(
    "P2OASYS_REQUIRE_OFFICIAL_MATRIX", "1"
).strip().lower() in ("1", "true", "yes", "on")
# Optional lookup CSVs for P2OASys (see docs/P2OASYS_LOOKUP_SOURCES.md). Set to None to disable.
P2OASYS_IARC_CSV_PATH = os.environ.get("P2OASYS_IARC_CSV", os.path.join(DATA_DIR, "iarc_by_cas.csv"))
P2OASYS_ODP_GWP_CSV_PATH = os.environ.get("P2OASYS_ODP_GWP_CSV", os.path.join(DATA_DIR, "odp_gwp_by_cas.csv"))
# CAA §112(b) HAP list for Atmospheric NESHAP (see scripts/build_caa112b_hap_csv.py).
P2OASYS_HAP_CSV_PATH = os.environ.get("P2OASYS_HAP_CSV", os.path.join(DATA_DIR, "caa112b_hap_by_cas.csv"))
# IPCC GWP 100-year (Federal LCA Commons parquet). Final path resolved after FASTP2OASYS_DIR.
ATMO_DIR = os.environ.get("ATMO_DIR", os.path.join(REPO_ROOT, "data", "atmo"))
# IARC classifications (CSV or Excel with CAS No. and Group). Place under ``data/iarc`` or set ``IARC_DIR``.
IARC_DIR = os.environ.get("IARC_DIR", os.path.join(REPO_ROOT, "data", "iarc"))

# COMPTOX public data folders (Excel and MySQL dump) — used by setup script
COMPTOX_EXCEL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data Excel Files Folder)", "Data Excel Files")
COMPTOX_MYSQL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data MySQL Dump File Folder)")

# App
APP_TITLE = "Quick Hazard Assessment v7"
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

# SDS CAS upload (Streamlit): only ``markitdown_fast`` or ``hybrid_md_ocr`` (see docs/SDS_EXTRACTION_PIPELINES.md).
# Legacy values (e.g. ``default``) are normalized to hybrid at runtime.
DEFAULT_SDS_EXTRACTION_PIPELINE = (
    (os.environ.get("HAZQUERY_DEFAULT_SDS_PIPELINE") or "hybrid_md_ocr").strip() or "hybrid_md_ocr"
)

# EPA CompTox / CCTE APIs (ToxVal, chemical search). Same key as historical COMPTOX_API_KEY.
EPA_API_KEY = (os.environ.get("EPA_API_KEY") or os.environ.get("COMPTOX_API_KEY") or "").strip() or None

# Optional: path to a P2OASys-style CSV (CAS column) to offer “load CAS list” in cross-validation UI.
P2OASYS_COMPOUND_LIST_CSV = (os.environ.get("P2OASYS_COMPOUND_LIST_CSV") or "").strip() or None

# GHaz7 app lives at GHhaz6/GHaz7/quick-hazard-assessment-app → hazquery is three levels up.
_HAZQUERY_ROOT = os.environ.get(
    "HAZQUERY_ROOT",
    os.path.abspath(os.path.join(REPO_ROOT, "..", "..", "..")),
)
# Expert P2OASys / CHEM21 lookups (sibling ``fastP2OASys``). Scores are looked up, not predicted.
FASTP2OASYS_DIR = os.environ.get("FASTP2OASYS_DIR", os.path.join(_HAZQUERY_ROOT, "fastP2OASys"))

# Prefer ATMO_DIR env, else data/atmo, else sibling fastP2OASys/atmo (IPCC parquet).
def _resolve_atmo_dir() -> str:
    env = (os.environ.get("ATMO_DIR") or "").strip()
    if env and os.path.isdir(env):
        return env
    local = os.path.join(DATA_DIR, "atmo")
    if os.path.isdir(local):
        try:
            if any(n.lower().endswith(".parquet") for n in os.listdir(local)):
                return local
        except OSError:
            pass
    sibling = os.path.join(FASTP2OASYS_DIR, "atmo")
    if os.path.isdir(sibling):
        return sibling
    return env or local

ATMO_DIR = _resolve_atmo_dir()

P2OASYS_EXPERT_CSV_PATH = os.environ.get(
    "P2OASYS_EXPERT_CSV",
    os.path.join(FASTP2OASYS_DIR, "data", "p2oasys_single_cas_clean.csv"),
)
# Precomputed expert+auto P2OASys category scores (CAS lookup ribbon / TCI skip).
P2OASYS_SCORE_LOOKUP_DB = os.environ.get(
    "P2OASYS_SCORE_LOOKUP_DB", os.path.join(DATA_DIR, "p2oasys_score_lookup.sqlite")
)
# Full p2oasys.turi.org harvest (pages 1–101), single-CAS split + parked multi/empty.
# Prefer the copy shipped in data/ (v7), then the sibling fastP2OASys tree.
_HARVEST_REPO = os.path.join(DATA_DIR, "p2oasys_harvest.sqlite")
_HARVEST_FAST = os.path.join(FASTP2OASYS_DIR, "p2oasys_harvest.sqlite")
P2OASYS_HARVEST_DB = os.environ.get(
    "P2OASYS_HARVEST_DB",
    _HARVEST_REPO if os.path.isfile(_HARVEST_REPO) else _HARVEST_FAST,
)
P2OASYS_EXPERT_VS_AUTO_CSV = os.environ.get(
    "P2OASYS_EXPERT_VS_AUTO_CSV",
    os.path.join(_HAZQUERY_ROOT, "GHhaz5", "report_expert_vs_auto_smiles_subset.csv"),
)

CHEM21_GUIDE_CSV_PATH = os.environ.get(
    "CHEM21_GUIDE_CSV",
    os.path.join(
        FASTP2OASYS_DIR,
        "CHEM21",
        "vendor",
        "solvent_flashcards",
        "src",
        "solvent_guide",
        "sources",
        "blueprints",
        "solvent_guide",
        "CHEM21_full.csv",
    ),
)
def _first_existing_path(*candidates: str) -> str | None:
    for c in candidates:
        c = (c or "").strip().strip('"')
        if not c:
            continue
        if os.path.isfile(c) or os.path.isdir(c):
            return c
    return None


_doss_env = (os.environ.get("DOSS_XLSX") or "").strip()
_DOSS_DEFAULT_XLSX = _doss_env or _first_existing_path(
    r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\AI\Retrieve\DoSS\DOSS.xlsx",
    r"C:\Users\Gabriel_Salierno\OneDrive - UMass Lowell\TURI\Research\AI\Retrieve\DoSS\DOSS.xlsx",
)
DOSS_XLSX_PATH = _DOSS_DEFAULT_XLSX if _DOSS_DEFAULT_XLSX and os.path.isfile(_DOSS_DEFAULT_XLSX) else None
HSPIP_CACHE_CSV_PATH = os.environ.get("HSPIP_CACHE_CSV", os.path.join(DATA_DIR, "hsp_by_cas.csv"))
CAS_HSPIP_ANTOINE_CSV = os.environ.get("CAS_HSPIP_ANTOINE_CSV", os.path.join(DATA_DIR, "cas_hspip_antoine.csv"))
# Local cas-to-HSPiP_data CLI (PubChem SMILES → HSPiP), not a GitHub link-only fallback.
_HSPIP_CLI_DEFAULT = _first_existing_path(
    os.path.join(_HAZQUERY_ROOT, "cas-to-HSPiP_data", "HSPiP_CLI_v7.py"),
    os.path.join(os.path.dirname(REPO_ROOT), "cas-to-HSPiP_data", "HSPiP_CLI_v7.py"),
)
HSPIP_CLI_SCRIPT = (os.environ.get("HSPIP_CLI_SCRIPT") or "").strip() or _HSPIP_CLI_DEFAULT
_HSPIP_DIR_DEFAULT = _first_existing_path(
    r"C:\Program Files\Hansen-Solubility\HSPiP",
    r"C:\Program Files\Hansen-Solubility-6\HSPiP",
)
HSPIP_INSTALL_DIR = (os.environ.get("HSPIP_INSTALL_DIR") or "").strip() or _HSPIP_DIR_DEFAULT
V7_SETTINGS_JSON = os.environ.get(
    "V7_SETTINGS_JSON", os.path.join(DATA_DIR, "ghaz7_settings.json")
)
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

# --- v7 knowledge / SDS providers (do not replace v6 scoring) ---
V7_CACHE_ROOT = os.environ.get("V7_CACHE_ROOT", os.path.join(REPO_ROOT, "cache"))
V7_SDS_CACHE_DIR = os.path.join(V7_CACHE_ROOT, "SDS")
V7_KNOWLEDGE_DB_PATH = os.environ.get(
    "V7_KNOWLEDGE_DB_PATH", os.path.join(DATA_DIR, "knowledge_v7.sqlite")
)
V7_MANUAL_OVERRIDES_CSV = os.environ.get(
    "V7_MANUAL_OVERRIDES_CSV", os.path.join(DATA_DIR, "manual_overrides_by_cas.csv")
)
GSD_XLSX_PATH = os.environ.get(
    "GSD_XLSX",
    os.path.join(
        os.path.dirname(_DOSS_DEFAULT_XLSX or ""),
        "Greener Solvent Database.xlsx",
    )
    if _DOSS_DEFAULT_XLSX
    else "",
)
# Unofficial/community OpenAPI claims this host; only used when SIGMA_API_KEY is set.
SIGMA_API_BASE = (os.environ.get("SIGMA_API_BASE") or "https://api.sigmaaldrich.com/v1").rstrip("/")
SIGMA_API_KEY = (os.environ.get("SIGMA_API_KEY") or "").strip() or None
SIGMA_PRODUCT_MAP_CSV = (os.environ.get("SIGMA_PRODUCT_MAP_CSV") or "").strip() or None
SIGMA_SDS_REGION = os.environ.get("SIGMA_SDS_REGION", "US/en")
SIGMA_HTTP_TIMEOUT_S = float(os.environ.get("SIGMA_HTTP_TIMEOUT_S", "30"))

# TCI SDS (HTTP only). CAS→product: seed map + learned map + optional live catalog search.
_TCI_MAP_DEFAULT = os.path.join(DATA_DIR, "tci_catalog_by_cas.csv")
TCI_PRODUCT_MAP_CSV = (os.environ.get("TCI_PRODUCT_MAP_CSV") or _TCI_MAP_DEFAULT).strip() or None
TCI_LEARNED_CSV = os.environ.get(
    "TCI_LEARNED_CSV", os.path.join(DATA_DIR, "tci_catalog_learned.csv")
)
TCI_RUNTIME_CSV = os.environ.get(
    "TCI_RUNTIME_CSV", os.path.join(DATA_DIR, "tci_catalog_runtime.csv")
)
TCI_SDS_REGION = os.environ.get("TCI_SDS_REGION", "US/en")
TCI_SDS_COUNTRY = os.environ.get("TCI_SDS_COUNTRY", "US")
TCI_SDS_LANGUAGE = os.environ.get("TCI_SDS_LANGUAGE", "EN")
TCI_HTTP_TIMEOUT_S = float(os.environ.get("TCI_HTTP_TIMEOUT_S", "30"))
TCI_MIN_INTERVAL_S = float(os.environ.get("TCI_MIN_INTERVAL_S", "5.0"))
# After HTTP 401/403 from Akamai, skip further TCI HTTP for this many seconds (process-wide).
TCI_COOLDOWN_S = float(os.environ.get("TCI_COOLDOWN_S", "720"))
# Optional single retry sleep after a 403 on the predictable SDS URL (only if cooldown allows).
TCI_403_RETRY_SLEEP_S = float(os.environ.get("TCI_403_RETRY_SLEEP_S", "8"))
# Live catalog search (CAS→product HTML). Off by default for batch; Compile UI enables it
# interactively. Often HTTP 403 (Akamai) from datacenter / non-browser clients.
TCI_ENABLE_LIVE_SEARCH = (os.environ.get("TCI_ENABLE_LIVE_SEARCH") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# documentSearch POSTs after a blocked PDF GET make Akamai angrier — off unless explicitly true.
TCI_ENABLE_DOCUMENT_SEARCH_FALLBACK = (
    os.environ.get("TCI_ENABLE_DOCUMENT_SEARCH_FALLBACK", "false")
).strip().lower() in {"1", "true", "yes", "on"}
# After a successful SDS fetch whose PDF CAS matches, append to TCI_LEARNED_CSV.
TCI_AUTO_REMEMBER = (os.environ.get("TCI_AUTO_REMEMBER", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# Example chemicals for quick buttons (CAS, label)
EXAMPLE_CHEMICALS = [
    ("67-64-1", "67-64-1 (Acetone)"),
    ("64-17-5", "64-17-5 (Ethanol)"),
    ("71-43-2", "71-43-2 (Benzene)"),
    ("50-00-0", "50-00-0 (Formaldehyde)"),
]
