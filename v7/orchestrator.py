"""Assemble a KnowledgeRecord from curated tables + optional SDS + PubChem."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.lookup_tables import normalize_cas_for_lookup
from utils.solvent_reference_lookup import lookup_chem21, lookup_p2oasys_expert, resolve_lookup_tables
from v7.evidence import evidence
from v7.knowledge import (
    KnowledgeRecord,
    KnowledgeStore,
    compare_p2oasys,
    evidence_from_cameo,
    evidence_from_doss_row,
    evidence_from_pubchem,
    evidence_from_structured_sds,
    load_manual_overrides,
    merge_layers,
)
from v7.providers.base import SDSDocument, SDSHit
from v7.providers.doss import load_doss_rows
from v7.providers.hspip import HSPiPProvider
from v7.sds_acquisition import (
    SDSVendor,
    download_vendor_hit,
    find_vendor_hits,
    provenance_from_document,
    structured_from_pdf,
)
from v7.sds_cache import SDSCache


class KnowledgeOrchestrator:
    def __init__(self, config: Any) -> None:
        self.config = config
        self.tables = resolve_lookup_tables(
            p2oasys_csv=getattr(config, "P2OASYS_EXPERT_CSV_PATH", None),
            chem21_csv=getattr(config, "CHEM21_GUIDE_CSV_PATH", None),
            doss_xlsx=getattr(config, "DOSS_XLSX_PATH", None),
            hspip_csv=getattr(config, "HSPIP_CACHE_CSV_PATH", None),
        )
        doss_path = getattr(config, "DOSS_XLSX_PATH", None)
        self.doss_rows = load_doss_rows(doss_path) if doss_path else {}
        rer = {}
        for cas, row in self.doss_rows.items():
            try:
                rer[cas] = float(row.get("RER (HSP)"))
            except (TypeError, ValueError):
                continue
        self.hspip = HSPiPProvider(
            hspip_cache_csv=getattr(config, "HSPIP_CACHE_CSV_PATH", None),
            doss_xlsx=doss_path,
            doss_rer_by_cas=rer,
        )
        self.manual = load_manual_overrides(getattr(config, "V7_MANUAL_OVERRIDES_CSV", None))
        self.store = KnowledgeStore(getattr(config, "V7_KNOWLEDGE_DB_PATH", None))
        self.sds_cache = SDSCache(getattr(config, "V7_SDS_CACHE_DIR", Path("cache") / "SDS"))

    def find_sds(self, cas: str, vendor: SDSVendor) -> list[SDSHit]:
        """List SDS candidates for a CAS (no download)."""
        return find_vendor_hits(cas, vendor)

    def fetch_sds_document(self, hit: SDSHit) -> SDSDocument:
        """Download or reuse cache for a vendor hit → SDSDocument."""
        return download_vendor_hit(hit)

    def structured_from_sds_document(self, doc: SDSDocument) -> dict[str, Any] | None:
        return structured_from_pdf(doc.pdf_bytes)

    def assemble_with_sds_hit(
        self,
        cas: str,
        hit: SDSHit,
        *,
        pubchem: dict[str, Any] | None = None,
        computed_p2oasys: float | None = None,
    ) -> tuple[KnowledgeRecord, dict[str, Any]]:
        """
        Fetch vendor SDS, parse structured fields, merge into KnowledgeRecord.

        Returns ``(record, provenance_meta)``. Parsers stay vendor-agnostic.
        """
        doc = self.fetch_sds_document(hit)
        structured = self.structured_from_sds_document(doc)
        provider_key = f"{hit.provider}_sds" if hit.provider else "sds"
        citation = doc.source_url or f"{doc.manufacturer} SDS {doc.product_number or ''}".strip()
        record = self.assemble(
            cas,
            pubchem=pubchem,
            computed_p2oasys=computed_p2oasys,
            sds_structured=structured,
            sds_provider=provider_key,
            sds_citation=citation,
        )
        return record, provenance_from_document(doc)

    def assemble(
        self,
        cas: str,
        *,
        pubchem: dict[str, Any] | None = None,
        computed_p2oasys: float | None = None,
        sds_structured: dict[str, Any] | None = None,
        sds_provider: str = "sigma_sds",
        sds_citation: str = "SDS",
    ) -> KnowledgeRecord:
        cas_key = normalize_cas_for_lookup(cas)
        doss_row = self.doss_rows.get(cas_key) or {}
        chem21 = lookup_chem21(cas, self.tables.get("chem21"), source_path=self.tables.get("chem21_path"))
        chem21_layer = {}
        if chem21["status"] == "found":
            chem21_layer["chem21_ranking"] = evidence(
                chem21.get("ranking_default"),
                source="CHEM21 solvent guide",
                provider="gsd",
                confidence="curated",
                citation=chem21.get("source_path") or "CHEM21_full.csv",
            )
            if chem21.get("safety") is not None:
                chem21_layer["chem21_safety"] = evidence(
                    chem21["safety"],
                    source="CHEM21",
                    provider="gsd",
                    confidence="curated",
                    citation=str(chem21.get("source_path") or ""),
                )
            if chem21.get("health") is not None:
                chem21_layer["chem21_health"] = evidence(
                    chem21["health"],
                    source="CHEM21",
                    provider="gsd",
                    confidence="curated",
                    citation=str(chem21.get("source_path") or ""),
                )
            if chem21.get("env") is not None:
                chem21_layer["chem21_env"] = evidence(
                    chem21["env"],
                    source="CHEM21",
                    provider="gsd",
                    confidence="curated",
                    citation=str(chem21.get("source_path") or ""),
                )

        fields = merge_layers(
            evidence_from_pubchem(pubchem),
            evidence_from_cameo(cas),
            evidence_from_structured_sds(sds_structured, provider=sds_provider, citation=sds_citation),
            chem21_layer,
            evidence_from_doss_row(doss_row, xlsx=str(getattr(self.config, "DOSS_XLSX_PATH", None) or "DoSS")),
            self.hspip.lookup(cas),
            self.manual.get(cas_key) or {},
        )
        doss_p2 = None
        try:
            doss_p2 = float(doss_row.get("P2OASys")) if doss_row else None
        except (TypeError, ValueError):
            doss_p2 = None
        expert = lookup_p2oasys_expert(cas, self.tables.get("p2oasys"), source_path=self.tables.get("p2oasys_path"))
        if doss_p2 is None and expert["status"] == "found":
            doss_p2 = expert.get("score")
        p2 = compare_p2oasys(
            doss_score=doss_p2,
            computed_score=computed_p2oasys,
            doss_citation="DoSS original datapoints" if doss_row.get("P2OASys") is not None else expert.get("source_path"),
            computed_citation="experimental PubChem matrix scorer" if computed_p2oasys is not None else None,
        )
        record = KnowledgeRecord(cas=cas, fields=fields, p2oasys=p2)
        self.store.persist(record)
        return record
