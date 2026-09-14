"""Smoke test: run unified lookup for a few CAS values and print source coverage."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from unified_hazard_report.data_context import OfflineDataContext
from unified_hazard_report.report_generator import flatten_unified
from unified_hazard_report.unified_lookup import unified_lookup


def main() -> None:
    ctx = OfflineDataContext()
    for cas in ("50-00-0", "67-64-1"):
        block = unified_lookup(cas, ctx)
        rows = flatten_unified(block["cas"], block)
        legacy = {r["source_name"] for r in rows if r["source_type"] == "legacy"}
        iuclid = {r["source_name"] for r in rows if r["source_type"] == "iuclid"}
        warn = sum(1 for r in rows if r["source_type"] == "warning")
        print(f"CAS {cas}: rows={len(rows)} legacy_sources={sorted(legacy)} iuclid_sources={sorted(iuclid)} warnings={warn}")
        print(f"  IUCLID UUIDs: {block.get('iuclid_uuids')}")


if __name__ == "__main__":
    main()
