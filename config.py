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

# Local SQLite chemical DB (DSSTox + ToxValDB + ECOTOX/ToxRefDB/CPDB); built by scripts/setup_chemical_db.py
DATA_DIR = os.path.join(REPO_ROOT, "data")
CHEMICAL_DB_PATH = os.path.join(DATA_DIR, "chemical_db.sqlite")
# Raw downloaded DB files (ECOTOX, ToxRefDB, CPDB) before loading into SQLite
RAW_DATABASES_DIR = os.path.join(DATA_DIR, "raw_databases")

# COMPTOX public data folders (Excel and MySQL dump) — used by setup script
COMPTOX_EXCEL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data Excel Files Folder)", "Data Excel Files")
COMPTOX_MYSQL_DIR = os.path.join(REPO_ROOT, "COMPTOX_Public (Data MySQL Dump File Folder)")

# App
APP_TITLE = "Quick Hazard Assessment"
ZENODO_DOI = "10.5281/zenodo.19056294"

# Example chemicals for quick buttons (CAS, label)
EXAMPLE_CHEMICALS = [
    ("67-64-1", "67-64-1 (Acetone)"),
    ("64-17-5", "64-17-5 (Ethanol)"),
    ("71-43-2", "71-43-2 (Benzene)"),
    ("50-00-0", "50-00-0 (Formaldehyde)"),
]
