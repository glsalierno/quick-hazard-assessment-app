"""Process a local REACH archive or folder of .i6z files (set OFFLINE_LOCAL_ARCHIVE).

Example (PowerShell, from ``quick-hazard-assessment-app``)::

    $env:PYTHONPATH = '.'
    $env:OFFLINE_LOCAL_ARCHIVE = 'C:\\path\\to\\REACH_Study_Results_2023-06-12.zip'
    $env:OFFLINE_SCRAPE_CL = 'false'
    python scripts/test_offline_local.py

Quick test with one extracted dossier folder::

    $env:OFFLINE_LOCAL_ARCHIVE = 'C:\\path\\to\\one_substance_folder'
    python scripts/test_offline_local.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    la = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
    if not la:
        print("Set OFFLINE_LOCAL_ARCHIVE to your .zip, .7z, or directory of .i6z files.", file=sys.stderr)
        sys.exit(2)

    from ingest import offline_echa_loader as ob

    max_n = os.getenv("OFFLINE_MAX_CL", "").strip()
    max_cl = int(max_n) if max_n.isdigit() else None

    reg, cl = ob.build_offline_dataframes(
        force_download=False,
        use_cache=False,
        max_substances_for_cl=max_cl,
    )
    print("substances_df:", reg.shape)
    print("cl_hazards_df:", cl.shape)
    if not reg.empty:
        print(reg.head(5))
    if not cl.empty:
        print(cl.head(10))


if __name__ == "__main__":
    main()
