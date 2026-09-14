"""DoSS SDS + curated solvent table provider."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from utils.lookup_tables import normalize_cas_for_lookup
from v7.providers.base import SDSDownload, SDSHit, SDSProvider
from v7.sds_cache import sha256_bytes


def load_doss_rows(xlsx_path: Path | str, sheet: str = "DoSS original datapoints") -> dict[str, dict[str, Any]]:
    path = Path(xlsx_path)
    if not path.is_file():
        return {}
    import pandas as pd

    df = pd.read_excel(path, sheet_name=sheet, header=1)
    if "CAS" not in df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        cas = normalize_cas_for_lookup(str(row.get("CAS") or ""))
        if not cas:
            continue
        out[cas] = {str(k): row.get(k) for k in df.columns}
    return out


def load_doss_needing_p2oasys(xlsx_path: Path | str) -> dict[str, dict[str, Any]]:
    path = Path(xlsx_path)
    if not path.is_file():
        return {}
    import pandas as pd

    df = pd.read_excel(path, sheet_name="DoSS list needing P2OASys", header=1)
    if "CAS" not in df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        cas = normalize_cas_for_lookup(str(row.get("CAS") or ""))
        if cas:
            out[cas] = {str(k): row.get(k) for k in df.columns}
    return out


class DoSSProvider(SDSProvider):
    name = "doss"

    def __init__(self, xlsx_path: Path | str | None, *, timeout_s: float = 30.0) -> None:
        self.xlsx_path = Path(xlsx_path) if xlsx_path else None
        self.timeout_s = timeout_s
        self.rows = load_doss_rows(self.xlsx_path) if self.xlsx_path else {}

    def find(self, cas: str) -> list[SDSHit]:
        key = normalize_cas_for_lookup(cas)
        row = self.rows.get(key)
        if not row:
            return []
        url = str(row.get("SDS Link") or "").strip()
        if not url or not url.lower().startswith("http"):
            return []
        return [
            SDSHit(
                cas=cas,
                provider=self.name,
                manufacturer="DoSS catalog",
                product_id=str(row.get("Solvent Name") or "") or None,
                title=str(row.get("Solvent Name") or None),
                sds_url=url,
                revision=None,
                extra={"source_xlsx": str(self.xlsx_path) if self.xlsx_path else None},
            )
        ]

    def download(self, hit: SDSHit) -> SDSDownload:
        if not hit.sds_url:
            raise ValueError("DoSS hit has no SDS URL")
        req = Request(hit.sds_url, headers={"User-Agent": "GHaz7-DoSSProvider/1.0 (research)"})
        with urlopen(req, timeout=self.timeout_s) as resp:
            data = resp.read()
        if not data:
            raise ValueError("Empty SDS download from DoSS URL")
        return SDSDownload(hit=hit, pdf_bytes=data, sha256=sha256_bytes(data))
