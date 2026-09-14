"""Manual SDS / override provider (highest knowledge priority)."""

from __future__ import annotations

import csv
from pathlib import Path

from utils.lookup_tables import normalize_cas_for_lookup
from v7.providers.base import SDSDownload, SDSHit, SDSProvider
from v7.sds_cache import sha256_bytes


class ManualProvider(SDSProvider):
    """
    CSV columns: cas, pdf_path, manufacturer, revision, product_id, sds_url (optional).
    Local files only — no network.
    """

    name = "manual"

    def __init__(self, catalog_csv: Path | str | None = None) -> None:
        self.rows: dict[str, dict[str, str]] = {}
        path = Path(catalog_csv) if catalog_csv else None
        if path and path.is_file():
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    cas = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
                    if cas:
                        self.rows[cas] = {k: (v or "").strip() for k, v in row.items()}

    def find(self, cas: str) -> list[SDSHit]:
        row = self.rows.get(normalize_cas_for_lookup(cas))
        if not row:
            return []
        return [
            SDSHit(
                cas=cas,
                provider=self.name,
                manufacturer=row.get("manufacturer") or "manual",
                product_id=row.get("product_id") or None,
                title=row.get("title") or None,
                sds_url=row.get("sds_url") or None,
                revision=row.get("revision") or None,
                extra={"pdf_path": row.get("pdf_path")},
            )
        ]

    def download(self, hit: SDSHit) -> SDSDownload:
        path = Path(str(hit.extra.get("pdf_path") or ""))
        if not path.is_file():
            raise FileNotFoundError(f"Manual SDS PDF not found: {path}")
        data = path.read_bytes()
        return SDSDownload(hit=hit, pdf_bytes=data, sha256=sha256_bytes(data))
