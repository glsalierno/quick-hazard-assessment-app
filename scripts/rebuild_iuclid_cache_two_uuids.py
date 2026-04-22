"""One-time fast IUCLID cache rebuild for a tiny UUID subset."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from ingest.crosswalk import normalize_cas
from unified_hazard_report.data_context import OfflineDataContext
from unified_hazard_report.iuclid_cache import rebuild_iuclid_cache
from unified_hazard_report.iuclid_integration import sync_offline_secrets_from_st_secrets


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild IUCLID cache for two UUIDs only.")
    ap.add_argument("--cas", default="", help="CAS to resolve dossier UUIDs from offline index.")
    ap.add_argument("--uuid", action="append", default=[], help="Explicit dossier UUID (repeatable).")
    ap.add_argument("--debug-xml", action="store_true", help="Verbose XML logging.")
    ap.add_argument("--debug-xml-dir", default="", help="Optional directory to save extracted Document.i6d files.")
    ap.add_argument("--refresh", action="store_true", help="Re-parse even if UUIDs are already cached.")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug_xml else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    sync_offline_secrets_from_st_secrets()

    uuids = [u.strip() for u in args.uuid if u and u.strip()]
    if not uuids and args.cas.strip():
        ctx = OfflineDataContext()
        cas = normalize_cas(args.cas.strip()) or args.cas.strip()
        uuids = ctx.uuids_for_cas(cas)[:2]

    if not uuids:
        print(
            "No UUIDs provided/found. Use either:\n"
            "  python scripts/rebuild_iuclid_cache_two_uuids.py --uuid <uuid1> --uuid <uuid2>\n"
            "or:\n"
            "  python scripts/rebuild_iuclid_cache_two_uuids.py --cas <CAS_NUMBER>",
            file=sys.stderr,
        )
        return 2

    uuids = uuids[:2]
    print(f"Rebuilding IUCLID cache for UUIDs: {uuids}")
    stats = rebuild_iuclid_cache(
        force_extract=False,
        verbose_debug=args.debug_xml,
        target_uuids=uuids,
        skip_existing_cache=not args.refresh,
        debug_dump_dir=args.debug_xml_dir or None,
    )
    print(
        "Done. "
        f"parsed={stats['parsed']} total={stats['i6z_total']} skipped_cached={stats.get('skipped_cached', 0)} "
        f"cl_rows={stats['cl_rows']} endpoint_rows={stats['endpoint_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
