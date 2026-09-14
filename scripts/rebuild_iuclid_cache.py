"""Rebuild offline IUCLID snippet SQLite cache from .i6z dossiers."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from unified_hazard_report.iuclid_cache import cache_db_path, rebuild_iuclid_cache
from unified_hazard_report.iuclid_integration import sync_offline_secrets_from_st_secrets


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild offline IUCLID snippet cache.")
    ap.add_argument("--force-extract", action="store_true", help="Force re-extraction of archive before parsing .i6z files.")
    ap.add_argument("--debug-xml", action="store_true", help="Verbose XML debug logging during rebuild.")
    ap.add_argument("--debug-xml-dir", default="", help="Optional directory to save extracted Document.i6d XML files.")
    ap.add_argument("--uuid", action="append", default=[], help="Optional dossier UUID to limit parsing (repeatable).")
    ap.add_argument("--skip-existing-cache", action="store_true", default=False, help="Skip UUIDs already cached.")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug_xml else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    sync_offline_secrets_from_st_secrets()
    archive = (os.getenv("OFFLINE_LOCAL_ARCHIVE") or "").strip()
    if not archive:
        print(
            "OFFLINE_LOCAL_ARCHIVE is empty. Set it in PowerShell or .streamlit/secrets.toml before running.",
            file=sys.stderr,
        )
        return 2

    targets = [u.strip() for u in args.uuid if u and u.strip()]
    stats = rebuild_iuclid_cache(
        force_extract=args.force_extract,
        verbose_debug=args.debug_xml,
        target_uuids=targets or None,
        skip_existing_cache=args.skip_existing_cache,
        debug_dump_dir=args.debug_xml_dir or None,
    )
    print("Cache rebuild complete.")
    print(f"DB: {cache_db_path()}")
    print(
        f"i6z_total={stats['i6z_total']} parsed={stats['parsed']} parse_errors={stats['parse_errors']} "
        f"cl_rows={stats['cl_rows']} endpoint_rows={stats['endpoint_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
