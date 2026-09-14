"""
Compile flow helpers: resolve CAS (typed / SDS / TCI) then look up hazard +
expert P2OASys + Hansen (HSPiP/DoSS). No P2OASys prediction.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import config
from services.chemical_assessment import AssessmentResult, get_assessment_service
from utils import solvent_reference_lookup as srl
from v7.providers.base import SDSDocument, SDSHit
from v7.providers.tci import (
    TCICatalogEntry,
    TCIDownloadError,
    TCIError,
    TCINoSDSError,
    TCINotFoundError,
    TCIProvider,
)
from v7.sds_acquisition import build_tci_provider, provenance_from_document


@dataclass
class CompileLookups:
    p2oasys: dict[str, Any]
    hsp: dict[str, Any]
    chem21: dict[str, Any]
    tables_meta: dict[str, Any]


@dataclass
class CompileBundle:
    cas: str
    preferred_name: str
    source: str
    result_data: dict[str, Any]
    lookups: CompileLookups
    sds_meta: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


class TCIAmbiguousProductError(TCIError):
    """Multiple TCI catalog hits; caller must choose a product number."""

    def __init__(self, hits: list[SDSHit], message: str | None = None) -> None:
        self.hits = list(hits)
        products = ", ".join(h.product_id or "?" for h in self.hits[:8])
        super().__init__(
            message
            or (
                f"Multiple TCI products ({len(self.hits)}): {products}. "
                "Select a product number and retry."
            )
        )


class _NamedBytesIO(io.BytesIO):
    """File-like PDF buffer with a ``name`` attribute for SDS identify/assess."""

    def __init__(self, data: bytes, name: str = "tci_sds.pdf") -> None:
        super().__init__(data)
        self.name = name


def _lookup_tables() -> dict[str, Any]:
    return srl.resolve_lookup_tables(
        p2oasys_csv=config.P2OASYS_EXPERT_CSV_PATH,
        chem21_csv=config.CHEM21_GUIDE_CSV_PATH,
        doss_xlsx=config.DOSS_XLSX_PATH,
        hspip_csv=config.HSPIP_CACHE_CSV_PATH,
    )


def lookup_databases(cas: str) -> CompileLookups:
    tables = _lookup_tables()
    return CompileLookups(
        p2oasys=srl.lookup_p2oasys_expert(
            cas, tables["p2oasys"], source_path=tables.get("p2oasys_path")
        ),
        hsp=srl.lookup_hsp(
            cas, doss_table=tables.get("doss_hsp"), hspip_table=tables.get("hspip")
        ),
        chem21=srl.lookup_chem21(
            cas, tables["chem21"], source_path=tables.get("chem21_path")
        ),
        tables_meta={
            "p2oasys_path": tables.get("p2oasys_path"),
            "chem21_path": tables.get("chem21_path"),
            "doss_path": tables.get("doss_path"),
            "hspip_path": tables.get("hspip_path"),
            "hspip_rows": len(tables.get("hspip") or {}),
            "doss_hsp_rows": len(tables.get("doss_hsp") or {}),
            "p2oasys_rows": len(tables.get("p2oasys") or {}),
        },
    )


def get_tci_provider(*, enable_live_search: bool | None = None) -> TCIProvider:
    return build_tci_provider(enable_live_search=enable_live_search)


def _pubchem_name_hint(cas: str) -> str | None:
    """Best-effort preferred name for live TCI search when CAS map misses."""
    cas = (cas or "").strip()
    if not cas:
        return None
    try:
        from utils.pubchem_client import get_compound_data

        data = get_compound_data(cas) or {}
    except Exception:
        return None
    for key in ("iupac_name", "title", "molecular_formula"):
        val = data.get(key)
        if val and isinstance(val, str) and len(val.strip()) > 2:
            return val.strip()
    syns = data.get("synonyms") or []
    if syns and isinstance(syns[0], str):
        return syns[0].strip()
    return None


def find_tci_hits(
    *,
    cas: str = "",
    product_number: str = "",
    name: str = "",
    provider: TCIProvider | None = None,
    enable_live_search: bool | None = True,
) -> list[SDSHit]:
    """
    Resolve TCI catalog candidates without downloading.

    Resolution order:
      1. Explicit product number
      2. Local seed ∪ learned map (any CAS previously remembered)
      3. Live TCI catalog search by CAS (when enabled; may 403)
      4. Live search by chemical name / PubChem name hint
    """
    live = True if enable_live_search is None else bool(enable_live_search)
    provider = provider or get_tci_provider(enable_live_search=live)
    prod_raw = (product_number or "").strip()

    if prod_raw:
        prod = provider.resolver.normalize_product(prod_raw)
        entry = provider.resolver.by_product.get(prod)
        if entry is None:
            entry = TCICatalogEntry(
                cas=(cas or "").strip(),
                product_number=prod,
                name=(name or "").strip() or None,
            )
        return [
            provider._hit_from_entry(
                entry,
                cas=(cas or "").strip() or entry.cas,
                via="product_number",
            )
        ]

    query_cas = (cas or "").strip()
    query_name = (name or "").strip()
    if not query_cas and not query_name:
        raise ValueError("Enter a CAS, chemical name, or TCI product number.")

    live_error: Exception | None = None
    hits: list[SDSHit] = []

    if query_cas:
        try:
            hits = provider.find_by_cas(query_cas)
        except TCIError as exc:
            live_error = exc
            hits = []

    if not hits and query_name:
        try:
            hits = provider.find_by_name(query_name)
        except TCIError as exc:
            live_error = live_error or exc
            hits = []

    if not hits and query_cas and live:
        hint = query_name or _pubchem_name_hint(query_cas)
        if hint and hint.casefold() != query_cas.casefold():
            try:
                name_hits = provider.find_by_name(hint)
                # Keep only hits whose map/live CAS matches when known
                from utils.lookup_tables import normalize_cas_for_lookup

                want = normalize_cas_for_lookup(query_cas)
                filtered = [
                    h
                    for h in name_hits
                    if not h.cas or normalize_cas_for_lookup(h.cas) in {"", want}
                ]
                hits = filtered or name_hits
            except TCIError as exc:
                live_error = live_error or exc

    if hits:
        return hits

    from v7.tci_catalog import tci_search_url

    q = query_cas or query_name
    search = tci_search_url(q)
    if live_error is not None:
        raise TCINotFoundError(
            f"No local TCI mapping for {q!r}, and live catalog search failed: {live_error}\n"
            f"Open {search} in a browser, copy the product number (e.g. A0001), "
            "paste it in the Product number field, and fetch once — the app will "
            "remember the CAS↔product pair for next time."
        ) from live_error
    if live:
        raise TCINotFoundError(
            f"No TCI product found for {q!r} in the local map or live catalog.\n"
            f"TCI may not list this CAS, or search returned no products. "
            f"Try {search} manually, or enter a product number if you have one."
        )
    raise TCINotFoundError(
        f"No TCI product mapped for {q!r}. Enable live catalog discovery, "
        f"add a learned row, or enter a product number. Browser search: {search}"
    )

def _bundle_from_assessment(
    result: AssessmentResult,
    *,
    source: str,
    sds_meta: dict[str, Any] | None = None,
    extra_warnings: list[str] | None = None,
    assessment_service: Any | None = None,
) -> CompileBundle:
    svc = assessment_service or get_assessment_service()
    data = svc.to_result_data(result)
    cas = str(data.get("clean_cas") or result.identity.cas or "").strip()
    if not cas or cas == "MIXTURE":
        raise ValueError("Could not resolve a single CAS from the input.")
    warnings = list(result.identity.warnings or [])
    if extra_warnings:
        warnings.extend(extra_warnings)
    return CompileBundle(
        cas=cas,
        preferred_name=str(data.get("preferred_name") or result.identity.chemical_name or ""),
        source=source,
        result_data=data,
        lookups=lookup_databases(cas),
        sds_meta=sds_meta or {},
        warnings=warnings,
    )


def compile_from_cas(cas_or_name: str) -> CompileBundle:
    svc = get_assessment_service()
    result = svc.assess(cas_or_name)
    if not isinstance(result, AssessmentResult):
        raise ValueError("Expected a single chemical assessment result.")
    return _bundle_from_assessment(result, source="typed")


def compile_from_sds_upload(uploaded_file: Any) -> CompileBundle:
    svc = get_assessment_service()
    identities = svc.identify_from_sds(uploaded_file)
    if not identities:
        raise ValueError("No CAS numbers extracted from the SDS PDF.")
    identities = sorted(identities, key=lambda x: float(x.confidence or 0.0), reverse=True)
    primary = identities[0]
    result = svc.assess_identity(primary)
    extra: list[str] = []
    if len(identities) > 1:
        others = ", ".join(i.cas for i in identities[1:6])
        extra.append(f"Multiple CAS in SDS; using {primary.cas}. Also found: {others}")
    return _bundle_from_assessment(
        result,
        source="sds_upload",
        sds_meta={
            "filename": getattr(uploaded_file, "name", None),
            "cas_candidates": [i.cas for i in identities],
            "confidence": primary.confidence,
        },
        extra_warnings=extra,
    )


def _assess_from_tci_document(
    doc: SDSDocument,
    hit: SDSHit,
    *,
    map_path: str | None,
    extra_warnings: list[str] | None = None,
    assessment_service: Any | None = None,
) -> CompileBundle:
    warnings = list(extra_warnings or [])
    pdf = _NamedBytesIO(doc.pdf_bytes, name=f"tci_{hit.product_id or 'sds'}.pdf")
    svc = assessment_service or get_assessment_service()
    identities = svc.identify_from_sds(pdf)
    if identities:
        identities = sorted(identities, key=lambda x: float(x.confidence or 0.0), reverse=True)
        primary = identities[0]
        result = svc.assess_identity(primary)
    elif hit.cas:
        assessed = svc.assess(hit.cas)
        if not isinstance(assessed, AssessmentResult):
            raise ValueError("Expected a single chemical assessment result.")
        result = assessed
        warnings.append("CAS not extracted from TCI SDS text; used map/query CAS.")
    else:
        raise ValueError(
            "Downloaded TCI SDS but could not extract a CAS. "
            "Provide a CAS alongside the product number."
        )

    meta = provenance_from_document(doc)
    meta["map_path"] = map_path
    meta["via"] = (hit.extra or {}).get("via")
    if (hit.extra or {}).get("warning"):
        meta["warning"] = hit.extra["warning"]
    if (doc.extra or {}).get("warning"):
        meta["warning"] = doc.extra["warning"]
    if (doc.extra or {}).get("cas_in_pdf"):
        meta["cas_in_pdf"] = doc.extra["cas_in_pdf"]
    if doc.title:
        meta["title"] = doc.title

    bundle = _bundle_from_assessment(
        result,
        source="tci_sds",
        sds_meta=meta,
        extra_warnings=warnings,
        assessment_service=svc,
    )

    # Grow local map so any CAS can be resolved offline next time.
    if getattr(config, "TCI_AUTO_REMEMBER", True) and bundle.cas and hit.product_id:
        try:
            from utils.lookup_tables import normalize_cas_for_lookup
            from v7.tci_catalog import remember_tci_mapping

            pdf_cas = normalize_cas_for_lookup(
                str((doc.extra or {}).get("cas_in_pdf") or bundle.cas)
            )
            want = normalize_cas_for_lookup(bundle.cas)
            if pdf_cas and want and pdf_cas != want:
                warnings.append(
                    f"Did not auto-learn map: SDS CAS {pdf_cas} ≠ assessed {want}."
                )
            else:
                learned = remember_tci_mapping(
                    bundle.cas,
                    hit.product_id,
                    name=bundle.preferred_name or doc.title,
                    notes="learned from successful SDS fetch",
                )
                meta["learned_mapping"] = learned
                if learned:
                    warnings.append(
                        f"Saved TCI mapping {bundle.cas} → {hit.product_id} "
                        "to the learned catalog for future lookups."
                    )
        except Exception as exc:
            warnings.append(f"Could not persist TCI mapping: {exc}")
        bundle.warnings = list(dict.fromkeys([*bundle.warnings, *warnings]))
        bundle.sds_meta = meta

    return bundle


def compile_from_tci(
    *,
    cas: str = "",
    product_number: str = "",
    name: str = "",
    provider: TCIProvider | None = None,
    assessment_service: Any | None = None,
    allow_first_hit: bool = False,
    enable_live_search: bool | None = True,
    try_next_on_sds_fail: bool = True,
) -> CompileBundle:
    """
    Fetch TCI SDS → extract CAS → GHaz assessment + DB lookups.

    When the map returns multiple products for a CAS/name, pass ``product_number``
    (UI picker) or set ``allow_first_hit=True`` (tests / batch only).

    If the first product has no downloadable SDS, optionally try later hits
    (``try_next_on_sds_fail``) before raising.
    """
    provider = provider or get_tci_provider(enable_live_search=enable_live_search)
    from v7.tci_catalog import ensure_runtime_catalog

    runtime = ensure_runtime_catalog()

    hits = find_tci_hits(
        cas=cas,
        product_number=product_number,
        name=name,
        provider=provider,
        enable_live_search=enable_live_search,
    )
    if len(hits) > 1 and not (product_number or "").strip():
        if not allow_first_hit:
            raise TCIAmbiguousProductError(hits)

    last_err: Exception | None = None
    attempted: list[str] = []
    for hit in hits:
        prod = (hit.product_id or "").strip()
        if not prod:
            continue
        attempted.append(prod)
        try:
            doc = provider.download_sds(prod, cas=hit.cas or cas, hit=hit)
        except (TCINoSDSError, TCIDownloadError) as exc:
            last_err = exc
            if not try_next_on_sds_fail or (product_number or "").strip():
                raise
            continue
        except TCIError:
            raise

        extra_warnings: list[str] = []
        if len(attempted) > 1:
            skipped = ", ".join(attempted[:-1])
            extra_warnings.append(
                f"Earlier TCI product(s) had no SDS PDF ({skipped}); used {prod}."
            )
        warn = str((hit.extra or {}).get("warning") or "").strip()
        if warn:
            extra_warnings.append(warn)
        return _assess_from_tci_document(
            doc,
            hit,
            map_path=str(runtime) if runtime else None,
            assessment_service=assessment_service,
            extra_warnings=extra_warnings,
        )

    if last_err is not None:
        raise last_err
    raise TCINotFoundError(f"No TCI product hits for cas={cas!r} name={name!r}")


def compile_from_tci_hit(
    hit: SDSHit,
    *,
    provider: TCIProvider | None = None,
    assessment_service: Any | None = None,
) -> CompileBundle:
    """Download a previously selected ``SDSHit`` and compile."""
    provider = provider or get_tci_provider(enable_live_search=False)
    from v7.tci_catalog import ensure_runtime_catalog

    runtime = ensure_runtime_catalog()
    try:
        doc = provider.download_sds(hit.product_id or "", cas=hit.cas, hit=hit)
    except TCIError:
        raise
    return _assess_from_tci_document(
        doc,
        hit,
        map_path=str(runtime) if runtime else None,
        assessment_service=assessment_service,
    )


def source_status() -> dict[str, Any]:
    """UI-friendly availability of lookup tables and TCI map."""
    tables = _lookup_tables()
    p2_path = Path(config.P2OASYS_EXPERT_CSV_PATH) if config.P2OASYS_EXPERT_CSV_PATH else None
    hsp_path = Path(config.HSPIP_CACHE_CSV_PATH) if config.HSPIP_CACHE_CSV_PATH else None
    from v7.tci_catalog import catalog_stats

    tci_stats = catalog_stats()
    sigma_map = getattr(config, "SIGMA_PRODUCT_MAP_CSV", None)
    return {
        "p2oasys": {
            "ok": bool(tables.get("p2oasys")),
            "rows": len(tables.get("p2oasys") or {}),
            "path": str(tables.get("p2oasys_path") or p2_path or ""),
        },
        "doss_hsp": {
            "ok": bool(tables.get("doss_hsp")),
            "rows": len(tables.get("doss_hsp") or {}),
            "path": str(config.DOSS_XLSX_PATH or ""),
        },
        "hspip_cache": {
            "ok": bool(tables.get("hspip")),
            "rows": len(tables.get("hspip") or {}),
            "path": str(hsp_path or ""),
            "exists": bool(hsp_path and hsp_path.is_file()),
        },
        "chem21": {
            "ok": bool(tables.get("chem21")),
            "rows": len(tables.get("chem21") or {}),
            "path": str(tables.get("chem21_path") or ""),
        },
        "tci_map": {
            "ok": tci_stats["merged_rows"] > 0,
            "path": tci_stats["runtime_path"] or tci_stats["seed_path"],
            "live_search": config.TCI_ENABLE_LIVE_SEARCH,
            "seed_rows": tci_stats["seed_rows"],
            "learned_rows": tci_stats["learned_rows"],
            "merged_rows": tci_stats["merged_rows"],
        },
        "sigma": {
            "api_key": bool(getattr(config, "SIGMA_API_KEY", None)),
            "product_map": bool(sigma_map and Path(str(sigma_map)).is_file()),
        },
    }