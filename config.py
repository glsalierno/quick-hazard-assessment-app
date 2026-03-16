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

# App
APP_TITLE = "Quick Hazard Assessment"
ZENODO_DOI = "10.5281/zenodo.14636704"
OPERA_NOTE = "Enhanced predictions with OPERA QSAR available in the command-line version."

# Example chemicals for quick buttons (CAS, label)
EXAMPLE_CHEMICALS = [
    ("67-64-1", "67-64-1 (Acetone)"),
    ("64-17-5", "64-17-5 (Ethanol)"),
    ("71-43-2", "71-43-2 (Benzene)"),
    ("50-00-0", "50-00-0 (Formaldehyde)"),
]
