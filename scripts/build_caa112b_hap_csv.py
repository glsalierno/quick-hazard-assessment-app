#!/usr/bin/env python3
"""Build data/caa112b_hap_by_cas.csv from Clean Air Act §112(b) (govinfo HTML).

EPA SRS JSON export requires interactive CDX login; the statutory table is the
authoritative HAP inventory used for P2OASys NESHAP list-membership scoring.
"""
from __future__ import annotations

import csv
import re
import urllib.request
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
OUT = APP / "data" / "caa112b_hap_by_cas.csv"
URL = (
    "https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/"
    "USCODE-2023-title42-chap85-subchapI-partA-sec7412.htm"
)


def format_cas(digits: str) -> str | None:
    d = "".join(c for c in digits if c.isdigit())
    if len(d) < 5:
        return None
    return f"{d[:-3]}-{d[-3:-1]}-{d[-1]}"


def main() -> None:
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0 hazquery-hap"})
    html = urllib.request.urlopen(req, timeout=90).read().decode("utf-8", "replace")
    pairs = re.findall(r"(\d{5,9})\s*</t[dh]>\s*<t[dh][^>]*>\s*([^<]+)", html, re.I)
    rows: list[tuple[str, str, str, str]] = []
    seen: set[str] = set()
    for cas_raw, name in pairs:
        cas = format_cas(cas_raw)
        if not cas or cas in seen:
            continue
        seen.add(cas)
        rows.append((cas, name.strip(), "CAA112(b)", cas_raw))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cas", "name", "list", "cas_raw", "source"])
        for cas, name, lst, raw in rows:
            w.writerow([cas, name, lst, raw, "USCODE-2023-title42-sec7412"])
    print(f"wrote {OUT} n={len(rows)}")
    for want in ("71-43-2", "75-07-0", "50-00-0", "107-13-1", "1332-21-4"):
        print(want, "OK" if want in {r[0] for r in rows} else "MISSING")


if __name__ == "__main__":
    main()
