#!/usr/bin/env python3
"""
Write ``data/opera_endpoints_reference.csv``: endpoint name + human clarification.

Uses :func:`utils.opera_batch.clarify_opera_endpoint` (pattern-based + curated glossary).

Example::

    python scripts/export_opera_endpoints_csv.py
    python scripts/export_opera_endpoints_csv.py --out data/opera_endpoints_reference.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.opera_batch import clarify_opera_endpoint, default_endpoints_json_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", type=Path, default=None, help="Source opera_endpoints.json")
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "opera_endpoints_reference.csv")
    args = ap.parse_args()

    src = args.json or default_endpoints_json_path()
    if not src.is_file():
        print(f"Missing {src}; run scripts/discover_opera_endpoints.py first.", file=sys.stderr)
        return 1

    data = json.loads(src.read_text(encoding="utf-8"))
    cols = data.get("columns") or []
    names = [c["name"] for c in cols if isinstance(c, dict) and c.get("name")]
    if not names and data.get("column_names_only"):
        names = list(data["column_names_only"])

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["endpoint", "clarification"])
        for name in names:
            w.writerow([name, clarify_opera_endpoint(name)])

    print(f"Wrote {len(names)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
