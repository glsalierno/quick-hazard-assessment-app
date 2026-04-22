"""Diagnostic inspector for IUCLID Document.i6d XML from CAS-matched UUIDs."""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from ingest.crosswalk import normalize_cas
from unified_hazard_report.data_context import OfflineDataContext
from unified_hazard_report.iuclid_extractor import read_i6d_bytes_from_i6z
from unified_hazard_report.iuclid_integration import sync_offline_secrets_from_st_secrets


def _local(tag: str) -> str:
    return tag.split("}")[-1] if tag else ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect Document.i6d XML for CAS-matched IUCLID dossiers.")
    ap.add_argument("--cas", required=True, help="CAS number to inspect (e.g., 71-43-2).")
    ap.add_argument("--max-uuids", type=int, default=2, help="How many matched UUIDs to inspect.")
    ap.add_argument("--out-dir", default="", help="Directory to save extracted Document.i6d files.")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    sync_offline_secrets_from_st_secrets()
    ctx = OfflineDataContext()
    cas = normalize_cas(args.cas.strip()) or args.cas.strip()
    uuids = ctx.uuids_for_cas(cas)[: max(1, args.max_uuids)]
    if not uuids:
        print(f"No UUIDs found for CAS {cas}")
        return 1

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir.strip() else Path(tempfile.gettempdir()) / "iuclid_inspect_xml"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Inspecting UUIDs: {uuids}")
    print(f"XML output directory: {out_dir}")

    for uid in uuids:
        i6z = ctx.i6z_path_for_uuid(uid)
        if i6z is None or not i6z.is_file():
            print(f"[{uid}] missing i6z path")
            continue
        raw = read_i6d_bytes_from_i6z(i6z)
        if not raw:
            print(f"[{uid}] Document.i6d not found")
            continue
        xml_path = out_dir / f"{uid}_Document.i6d.xml"
        xml_path.write_bytes(raw)
        print(f"\n[{uid}] i6z={i6z}")
        print(f"[{uid}] xml_saved={xml_path}")

        text = raw.decode("utf-8", errors="replace")
        lines = text.splitlines()
        print(f"[{uid}] --- first 100 XML lines ---")
        for ln in lines[:100]:
            print(ln)

        try:
            root = ET.fromstring(raw)
            counter: Counter[str] = Counter(_local(el.tag) for el in root.iter() if _local(el.tag))
            print(f"[{uid}] root={_local(root.tag)}")
            print(f"[{uid}] top_tags={dict(counter.most_common(40))}")
            print(
                f"[{uid}] xpath_counts="
                f"GHSClassification={len(root.findall('.//{{*}}GHSClassification'))}, "
                f"CLPClassification={len(root.findall('.//{{*}}CLPClassification'))}, "
                f"ClassificationAndLabelling={len(root.findall('.//{{*}}ClassificationAndLabelling'))}, "
                f"EndpointStudyRecord={len(root.findall('.//{{*}}EndpointStudyRecord'))}, "
                f"StudyResult={len(root.findall('.//{{*}}StudyResult'))}, "
                f"Result={len(root.findall('.//{{*}}Result'))}"
            )
        except ET.ParseError as exc:
            print(f"[{uid}] XML parse error: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
