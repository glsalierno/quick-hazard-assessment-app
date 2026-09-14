"""
HSPiPProvider — curated Hansen / RER from cas-to-HSPiP_data cache and DoSS.

Never estimates Hansen parameters with QSAR when curated rows exist.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.lookup_tables import normalize_cas_for_lookup
from utils.solvent_reference_lookup import (
    HSPIP_REPO_URL,
    load_doss_hsp_by_cas,
    load_hsp_csv_by_cas,
    lookup_hsp,
)
from v7.evidence import EvidenceValue, evidence


class HSPiPProvider:
    name = "hspip"

    def __init__(
        self,
        *,
        hspip_cache_csv: Path | str | None = None,
        doss_xlsx: Path | str | None = None,
        doss_rer_by_cas: dict[str, float] | None = None,
    ) -> None:
        self.hspip_table = load_hsp_csv_by_cas(hspip_cache_csv) if hspip_cache_csv else {}
        if not self.hspip_table:
            self.hspip_table = None
        self.doss_hsp = load_doss_hsp_by_cas(doss_xlsx) if doss_xlsx else None
        self.doss_rer = doss_rer_by_cas or {}

    def lookup(self, cas: str) -> dict[str, EvidenceValue]:
        """Return curated Hansen / RER evidence. Empty dict if nothing curated."""
        out: dict[str, EvidenceValue] = {}
        hit = lookup_hsp(cas, doss_table=self.doss_hsp, hspip_table=self.hspip_table)
        if hit["status"] != "found":
            return out
        src = hit.get("source") or "hspip"
        provider = "hspip" if src == "hspip_cache" else "doss"
        citation = hit.get("source_path") or HSPIP_REPO_URL
        for key, field in (("hansen_d", "delta_d"), ("hansen_p", "delta_p"), ("hansen_h", "delta_h")):
            val = hit.get(field)
            if val is None:
                continue
            out[key] = evidence(
                val,
                source=str(src),
                provider=provider,
                confidence="curated",
                citation=citation,
            )
        cas_key = normalize_cas_for_lookup(cas)
        rer = self.doss_rer.get(cas_key)
        if rer is not None:
            out["rer"] = evidence(
                rer,
                source="DoSS",
                provider="doss",
                confidence="curated",
                citation=str(hit.get("source_path") or "DoSS"),
            )
        return out

    def extra_descriptors(self, cas: str) -> dict[str, Any]:
        """Pass-through of leftover HSPiP cache columns if present."""
        cas_key = normalize_cas_for_lookup(cas)
        if not self.hspip_table or cas_key not in self.hspip_table:
            return {}
        row = dict(self.hspip_table[cas_key])
        row.pop("source", None)
        row.pop("source_path", None)
        return row
