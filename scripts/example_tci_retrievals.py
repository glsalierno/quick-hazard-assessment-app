"""
Example TCI SDS retrievals for common chemicals.

Uses the curated map in data/tci_catalog_by_cas.csv and the official SDS URL
template. Network calls are rate-limited; identical PDFs are cached under cache/SDS/.

Usage (from quick-hazard-assessment-app)::

    python scripts/example_tci_retrievals.py
    python scripts/example_tci_retrievals.py --cas 67-56-1
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from v7.providers.tci import TCIError, TCIProvider  # noqa: E402
from v7.sds_cache import SDSCache  # noqa: E402

DEFAULT_MAP = ROOT / "data" / "tci_catalog_by_cas.csv"
DEFAULT_CACHE = ROOT / "cache" / "SDS"

# Verified map entries that typically return a real PDF at the predictable URL.
EXAMPLE_CAS = [
    "67-56-1",  # methanol (M0097, M0628)
    "141-78-6",  # ethyl acetate (A0030)
    "67-68-5",  # DMSO (D0798)
    "109-99-9",  # THF (T0104)
    "68-12-2",  # DMF (D0722)
    "67-63-0",  # IPA (I0163)
]


def main() -> int:
    ap = argparse.ArgumentParser(description="Example TCI SDS HTTP retrievals")
    ap.add_argument("--cas", action="append", dest="cas_list", help="CAS to retrieve (repeatable)")
    ap.add_argument("--map", type=Path, default=DEFAULT_MAP, help="TCI product map CSV")
    ap.add_argument("--cache", type=Path, default=DEFAULT_CACHE, help="SDS cache root")
    ap.add_argument("--live-search", action="store_true", help="Enable Hybris catalog search")
    args = ap.parse_args()

    cas_list = args.cas_list or EXAMPLE_CAS
    provider = TCIProvider(
        product_map_csv=args.map,
        cache=SDSCache(args.cache),
        enable_live_search=args.live_search,
        enable_document_search_fallback=True,
        min_interval_s=1.0,
    )

    print(f"Map: {args.map}")
    print(f"Cache: {args.cache}")
    print()

    ok = 0
    for cas in cas_list:
        print(f"=== CAS {cas} ===")
        try:
            hits = provider.find_by_cas(cas)
        except TCIError as exc:
            print(f"  find error: {exc}")
            continue
        if not hits:
            print("  no mapped products")
            continue
        for hit in hits:
            print(f"  product {hit.product_id}: {hit.title or ''} -> {hit.sds_url}")
            try:
                doc = provider.download_sds(hit.product_id or "", cas=cas, hit=hit)
            except TCIError as exc:
                print(f"    download failed: {exc}")
                continue
            print(
                f"    OK bytes={len(doc.pdf_bytes)} sha256={doc.sha256[:12]}… "
                f"revision={doc.revision} reused={doc.reused} path={doc.cached_path}"
            )
            ok += 1
        print()

    print(f"Successful downloads: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
