#!/usr/bin/env python3
"""
Build data/reach_demo/reach_subset_examples.json from reach_subset.zip.

Study-result dossiers often omit CAS in the fields ``_parse_one_i6z`` reads; we scan
all ``.i6d`` XML in each ``.i6z`` for checksum-valid CAS tokens (same family of
identifiers the offline loader may surface after merges). Optional substance name
from the first ``Document.i6d``-style parse.

Usage (from repo root)::

    python scripts/refresh_reach_subset_examples.py
    python scripts/refresh_reach_subset_examples.py --zip path/to/other.zip --limit 12
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import zipfile
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _ensure_repo_on_path() -> None:
    root = str(_repo_root())
    if root not in sys.path:
        sys.path.insert(0, root)


_CAS_TOKEN = re.compile(r"\b(\d{2,10}-\d{2}-\d)\b")


def _first_valid_cas_from_i6z_xml(text: str) -> str | None:
    from utils.cas_validator import validate_cas

    seen: set[str] = set()
    for m in _CAS_TOKEN.finditer(text):
        tok = m.group(1)
        ok, norm = validate_cas(tok)
        if not ok or not norm or norm in seen:
            continue
        seen.add(norm)
        return norm
    return None


def _cas_and_name_from_i6z_blob(blob: bytes) -> tuple[str | None, str]:
    """Return (cas_or_none, substance_name)."""
    from ingest.offline_echa_loader import _parse_i6d_xml  # noqa: SLF001

    with zipfile.ZipFile(io.BytesIO(blob), "r") as iz:
        i6d_parts: list[tuple[str, bytes]] = []
        for n in iz.namelist():
            if not n.lower().endswith(".i6d"):
                continue
            try:
                i6d_parts.append((n, iz.read(n)))
            except (KeyError, OSError):
                continue
        if not i6d_parts:
            return None, ""
        # Prefer Document.i6d for name; concatenate all XML for CAS regex.
        i6d_parts.sort(key=lambda x: (0 if "document" in x[0].lower() else 1, len(x[0])))
        text = "".join(b.decode("utf-8", errors="ignore") for _, b in i6d_parts)
        meta, _ghs = _parse_i6d_xml(i6d_parts[0][1])
        name = str(meta.get("substance_name") or "").strip()
        cas = _first_valid_cas_from_i6z_xml(text)
        return cas, name


def main() -> None:
    root = _repo_root()
    p = argparse.ArgumentParser(description="Write reach_subset_examples.json from REACH demo zip")
    p.add_argument("--zip", type=Path, default=root / "data" / "reach_demo" / "reach_subset.zip")
    p.add_argument("--out", type=Path, default=root / "data" / "reach_demo" / "reach_subset_examples.json")
    p.add_argument("--limit", type=int, default=30, help="Max substances to write (deduped by CAS)")
    p.add_argument("--max-scan", type=int, default=500, help="Max .i6z entries to open from the zip")
    args = p.parse_args()

    zp = args.zip.resolve()
    if not zp.is_file():
        raise SystemExit(f"Zip not found: {zp}")

    _ensure_repo_on_path()

    entries: list[dict[str, str]] = []
    seen_cas: set[str] = set()

    with zipfile.ZipFile(zp, "r") as outer:
        names = sorted(n for n in outer.namelist() if n.lower().endswith(".i6z"))
        for i, member in enumerate(names):
            if len(entries) >= args.limit or i >= args.max_scan:
                break
            try:
                blob = outer.read(member)
            except (KeyError, OSError, zipfile.BadZipFile):
                continue
            cas, name = _cas_and_name_from_i6z_blob(blob)
            if not cas or cas in seen_cas:
                continue
            seen_cas.add(cas)
            uuid = Path(member).stem
            entries.append({"cas": cas, "name": name, "uuid": uuid})

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    print(f"Wrote {len(entries)} entries -> {args.out}")


if __name__ == "__main__":
    main()
