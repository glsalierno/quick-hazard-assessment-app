"""
TCI product catalog: seed CSV + learned mappings (grows as SDS fetches succeed).

The seed file ``tci_catalog_by_cas.csv`` is hand-verified. Successful interactive
lookups append to ``tci_catalog_learned.csv`` so any CAS can enrich the local
resolver over time — even when TCI's live catalog search is blocked (Akamai).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import config
from utils.lookup_tables import normalize_cas_for_lookup

_PRODUCT_RE_COLS = ("product_number", "product", "catalog_number", "productNumber")
_CAS_COLS = ("cas", "CAS")
_NAME_COLS = ("name", "title", "product_name")


def seed_catalog_path() -> Path | None:
    raw = getattr(config, "TCI_PRODUCT_MAP_CSV", None)
    if not raw:
        return None
    path = Path(str(raw))
    return path if path.is_file() else None


def learned_catalog_path() -> Path:
    raw = getattr(config, "TCI_LEARNED_CSV", None)
    if raw:
        return Path(str(raw))
    data_dir = Path(getattr(config, "DATA_DIR", "data"))
    return data_dir / "tci_catalog_learned.csv"


def runtime_catalog_path() -> Path:
    raw = getattr(config, "TCI_RUNTIME_CSV", None)
    if raw:
        return Path(str(raw))
    data_dir = Path(getattr(config, "DATA_DIR", "data"))
    return data_dir / "tci_catalog_runtime.csv"


def _row_key(cas: str, product: str) -> tuple[str, str]:
    return normalize_cas_for_lookup(cas), (product or "").strip().upper()


def _read_rows(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.is_file():
        return []
    out: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            cas = ""
            for col in _CAS_COLS:
                if row.get(col):
                    cas = str(row[col]).strip()
                    break
            prod = ""
            for col in _PRODUCT_RE_COLS:
                if row.get(col):
                    prod = str(row[col]).strip().upper()
                    break
            if not prod:
                continue
            name = ""
            for col in _NAME_COLS:
                if row.get(col):
                    name = str(row[col]).strip()
                    break
            notes = str(row.get("notes") or "").strip()
            out.append({"cas": cas, "product_number": prod, "name": name, "notes": notes})
    return out


def merge_catalog_rows(*sources: Path | None) -> list[dict[str, str]]:
    """Later sources override earlier ones for the same (cas, product) key."""
    merged: dict[tuple[str, str], dict[str, str]] = {}
    for path in sources:
        for row in _read_rows(path):
            key = _row_key(row["cas"], row["product_number"])
            if not key[1]:
                continue
            merged[key] = row
    return list(merged.values())


def write_runtime_catalog(rows: list[dict[str, str]] | None = None) -> Path:
    """Write merged seed+learned CSV for ``TCIProvider`` to load."""
    if rows is None:
        rows = merge_catalog_rows(seed_catalog_path(), learned_catalog_path())
    path = runtime_catalog_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["cas", "product_number", "name", "notes"])
        writer.writeheader()
        for row in sorted(rows, key=lambda r: (_row_key(r["cas"], r["product_number"]))):
            writer.writerow(
                {
                    "cas": r_cas_display(row.get("cas") or ""),
                    "product_number": row["product_number"],
                    "name": row.get("name") or "",
                    "notes": row.get("notes") or "",
                }
            )
    return path


def r_cas_display(cas: str) -> str:
    """Prefer hyphenated CAS when digits-only was stored."""
    s = (cas or "").strip()
    if "-" in s:
        return s
    digits = normalize_cas_for_lookup(s)
    if len(digits) < 5:
        return s
    # CAS: 2–7 + 2 + 1
    return f"{digits[:-3]}-{digits[-3:-1]}-{digits[-1]}"


def ensure_runtime_catalog() -> Path | None:
    """Ensure runtime CSV exists (seed ∪ learned). Returns path if any rows."""
    rows = merge_catalog_rows(seed_catalog_path(), learned_catalog_path())
    if not rows and not seed_catalog_path():
        return None
    return write_runtime_catalog(rows)


def remember_tci_mapping(
    cas: str,
    product_number: str,
    *,
    name: str | None = None,
    notes: str = "learned from successful SDS fetch",
) -> bool:
    """
    Append a verified CAS↔product row to the learned catalog.

    Returns True if a new row was written (False if already present).
    """
    cas_disp = (cas or "").strip()
    prod = (product_number or "").strip().upper()
    if not cas_disp or not prod:
        return False
    key = _row_key(cas_disp, prod)
    existing = {_row_key(r["cas"], r["product_number"]) for r in merge_catalog_rows(
        seed_catalog_path(), learned_catalog_path()
    )}
    if key in existing:
        write_runtime_catalog()  # keep runtime fresh
        return False

    path = learned_catalog_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.is_file() or path.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["cas", "product_number", "name", "notes"])
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "cas": r_cas_display(cas_disp),
                "product_number": prod,
                "name": (name or "").strip(),
                "notes": notes,
            }
        )
    write_runtime_catalog()
    return True


def catalog_stats() -> dict[str, Any]:
    seed = _read_rows(seed_catalog_path())
    learned = _read_rows(learned_catalog_path())
    merged = merge_catalog_rows(seed_catalog_path(), learned_catalog_path())
    return {
        "seed_rows": len(seed),
        "learned_rows": len(learned),
        "merged_rows": len(merged),
        "seed_path": str(seed_catalog_path() or ""),
        "learned_path": str(learned_catalog_path()),
        "runtime_path": str(runtime_catalog_path()),
    }


def tci_search_url(query: str, *, region: str | None = None) -> str:
    """Public TCI catalog search URL (for human browser fallback)."""
    from urllib.parse import quote

    region = (region or getattr(config, "TCI_SDS_REGION", "US/en")).strip("/") or "US/en"
    q = (query or "").strip()
    return f"https://www.tcichemicals.com/{region}/search/?text={quote(q)}&resulttype=product"
