"""
Configuration for Quick Hazard Assessment Streamlit app.
"""

from __future__ import annotations

import os

# Paths (relative to project root when running streamlit run app.py)
# DSSTox database lives in DSS/ (Git LFS–tracked when publishing to GitHub)
DSS_DIR = "DSS"
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
