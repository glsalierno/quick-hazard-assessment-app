#!/usr/bin/env python3
"""
Discover OPERA 2.9 CSV column names (endpoints) for batch / fastp2oasys integration.

Writes ``data/opera_endpoints.json`` with **all** CSV columns and short descriptions where known
(see ``utils.opera_batch.COLUMN_DESCRIPTIONS``). The app uses :func:`utils.opera_batch.get_all_opera_endpoints`.

Default: run a tiny OPERA job (aspirin + caffeine) and record the output header.
Use ``--offline`` to copy columns from the bundled sample header (no OPERA install).

Example::

    python scripts/discover_opera_endpoints.py --out data/opera_endpoints.json
    python scripts/discover_opera_endpoints.py --offline --out data/opera_endpoints.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.opera_batch import batch_predict, build_endpoints_json_from_columns, default_endpoints_json_path

SAMPLE_HEADER_PATH = ROOT / "utils" / "opera_2_9_sample_header.txt"
ASPIRIN = "CC(=O)OC1=CC=CC=C1C(=O)O"
CAFFEINE = "CN1C=NC2=C1C(=O)N(C(=O)N2C)C"


def main() -> int:
    p = argparse.ArgumentParser(description="Discover OPERA CSV endpoint columns.")
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output JSON path (default: data/opera_endpoints.json under app root)",
    )
    p.add_argument("--opera-exe", type=Path, default=None, help="Override OPERA executable")
    p.add_argument(
        "--offline",
        action="store_true",
        help="Use bundled OPERA 2.9 sample header (no OPERA run)",
    )
    args = p.parse_args()
    out_path = args.out or default_endpoints_json_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.offline:
        if not SAMPLE_HEADER_PATH.is_file():
            print(f"Missing sample header: {SAMPLE_HEADER_PATH}", file=sys.stderr)
            return 1
        header = SAMPLE_HEADER_PATH.read_text(encoding="utf-8").strip().splitlines()[0]
        columns = [c.strip() for c in header.split(",") if c.strip()]
        source = "bundled_header"
    else:
        br = batch_predict(
            [ASPIRIN, CAFFEINE],
            molecule_ids=["50-78-2", "58-08-2"],
            opera_exe=args.opera_exe,
            disk_cache=False,
            sqlite_cache_path=None,
            fallback_single_on_error=True,
        )
        if br.df.empty:
            print("OPERA run failed:", br.errors, file=sys.stderr)
            print("Tip: use --offline or install OPERA and set HAZQUERY_OPERA_EXE.", file=sys.stderr)
            return 1
        columns = list(br.df.columns)
        source = "live_opera"

    meta = build_endpoints_json_from_columns(columns)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "opera_version_hint": "2.9",
        "columns": meta,
        "column_names_only": columns,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(meta)} column entries to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
