"""
Build / expand local databases from a CAS list.

Targets:
  * HSPiP Hansen cache (``hsp_by_cas.csv``) via PubChem + HSPiP CLI
  * TCI SDS map + cache (map → live search when reachable → remember)
  * Optional MilliporeSigma SDS fallback when TCI cannot resolve CAS
  * Report expert P2OASys / CHEM21 / DoSS coverage gaps (lookup only)

End goal: grow caches from scratch by CAS query without hand-entering product codes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from v7.compile_service import compile_from_tci, lookup_databases
from v7.hspip_expand import HspExpandResult, expand_hsp_for_cas_list
from v7.providers.tci import TCIError
from v7.sds_acquisition import find_vendor_hits, user_facing_sds_error
from v7.tci_catalog import remember_tci_mapping

SdsVendorPref = Literal["tci", "sigma", "tci_then_sigma", "none"]


@dataclass
class CasBuildRow:
    cas: str
    hsp: HspExpandResult | None = None
    sds_vendor: str | None = None
    sds_product: str | None = None
    sds_status: str = "skipped"
    sds_message: str = ""
    p2oasys_status: str = ""
    p2oasys_score: float | None = None
    chem21_status: str = ""
    doss_hsp_status: str = ""


@dataclass
class CasBuildSummary:
    rows: list[CasBuildRow] = field(default_factory=list)
    hsp_cache_path: str = ""
    notes: list[str] = field(default_factory=list)


def _normalize_cas_list(raw: str | list[str]) -> list[str]:
    if isinstance(raw, list):
        items = raw
    else:
        items = []
        for line in str(raw).replace(",", "\n").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            # allow "CAS name" → first token
            items.append(s.split()[0])
    out: list[str] = []
    seen: set[str] = set()
    for cas in items:
        if cas not in seen:
            seen.add(cas)
            out.append(cas)
    return out


def build_from_cas_list(
    cas_input: str | list[str],
    *,
    expand_hsp: bool = True,
    expand_sds: SdsVendorPref = "tci_then_sigma",
    hspip_dir: str | None = None,
    skip_existing_hsp: bool = True,
    enable_tci_live_search: bool = True,
    progress: Callable[[str], None] | None = None,
) -> CasBuildSummary:
    """
    Expand local databases for each CAS.

    SDS path is best-effort: TCI website CAS search is often Akamai-blocked;
    when a product is resolved (map/live) or Sigma map/API hits, SDS is cached
    and TCI mappings are remembered for next time.
    """
    cas_list = _normalize_cas_list(cas_input)
    summary = CasBuildSummary()
    if not cas_list:
        summary.notes.append("No CAS numbers provided.")
        return summary

    hsp_by_cas: dict[str, HspExpandResult] = {}
    if expand_hsp:
        if progress:
            progress(f"Expanding HSPiP cache for {len(cas_list)} CAS…")
        try:
            hsp_sum = expand_hsp_for_cas_list(
                cas_list,
                hspip_dir=hspip_dir,
                skip_existing=skip_existing_hsp,
            )
            summary.hsp_cache_path = hsp_sum.cache_path
            for r in hsp_sum.results:
                hsp_by_cas[r.cas] = r
            summary.notes.append(
                f"HSPiP: {hsp_sum.computed} computed, {hsp_sum.cached} cached, "
                f"{hsp_sum.errors} errors → {hsp_sum.cache_path}"
            )
        except FileNotFoundError as exc:
            summary.notes.append(str(exc))
            expand_hsp = False

    for cas in cas_list:
        if progress:
            progress(f"Processing {cas}…")
        row = CasBuildRow(cas=cas, hsp=hsp_by_cas.get(cas))

        # Reference lookups (never predict P2OASys)
        try:
            lookups = lookup_databases(cas)
            row.p2oasys_status = str(lookups.p2oasys.get("status") or "")
            row.p2oasys_score = lookups.p2oasys.get("score")
            row.chem21_status = str(lookups.chem21.get("status") or "")
            row.doss_hsp_status = str(lookups.hsp.get("status") or "")
        except Exception as exc:
            row.p2oasys_status = f"error: {exc}"

        if expand_sds != "none":
            row.sds_status, row.sds_vendor, row.sds_product, row.sds_message = _expand_sds(
                cas,
                prefer=expand_sds,
                enable_tci_live_search=enable_tci_live_search,
            )

        summary.rows.append(row)

    missing_p2 = sum(1 for r in summary.rows if r.p2oasys_status == "not_found")
    if missing_p2:
        summary.notes.append(
            f"{missing_p2} CAS lack expert P2OASys scores (assessment required; not auto-predicted)."
        )
    return summary


def _expand_sds(
    cas: str,
    *,
    prefer: SdsVendorPref,
    enable_tci_live_search: bool,
) -> tuple[str, str | None, str | None, str]:
    order: list[str]
    if prefer == "tci":
        order = ["tci"]
    elif prefer == "sigma":
        order = ["sigma"]
    else:
        order = ["tci", "sigma"]

    errors: list[str] = []
    for vendor in order:
        try:
            if vendor == "tci":
                bundle = compile_from_tci(
                    cas=cas,
                    enable_live_search=enable_tci_live_search,
                    allow_first_hit=True,
                )
                prod = str(bundle.sds_meta.get("product_number") or "")
                if prod:
                    remember_tci_mapping(
                        bundle.cas or cas,
                        prod,
                        name=bundle.preferred_name,
                        notes="learned from CAS database builder",
                    )
                return (
                    "ok",
                    "tci",
                    prod or None,
                    f"SDS cached; reused={bundle.sds_meta.get('reused_cache')}",
                )
            hits = find_vendor_hits(cas, "sigma")
            if not hits:
                errors.append("sigma: no mapped products")
                continue
            from v7.sds_acquisition import download_vendor_hit

            doc = download_vendor_hit(hits[0])
            return (
                "ok",
                "sigma",
                doc.product_number,
                f"SDS cached; reused={doc.reused}",
            )
        except TCIError as exc:
            errors.append(f"tci: {user_facing_sds_error(exc)}")
        except Exception as exc:
            errors.append(f"{vendor}: {exc}")

    msg = " | ".join(errors) if errors else "no SDS vendor available"
    # Soften common Akamai case
    if "403" in msg or "Akamai" in msg or "blocked" in msg.lower():
        msg = (
            "TCI CAS->product search blocked (Akamai). SDS-by-product-number still works. "
            "HSPiP / DoSS expansion does not need TCI. " + msg
        )
    return "error", None, None, msg
