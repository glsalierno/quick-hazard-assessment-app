"""Merge legacy v1.4 assessment with offline REACH IUCLID rows for one CAS."""

from __future__ import annotations

from typing import Any

from unified_hazard_report.data_context import OfflineDataContext
from unified_hazard_report.iuclid_extractor import extract_endpoints_for_uuid
from unified_hazard_report.legacy_adapter import get_legacy_hazards
from ingest.crosswalk import normalize_cas


def unified_lookup(cas_number: str, ctx: OfflineDataContext) -> dict[str, Any]:
    """
    For one CAS:

    - ``legacy``: dict from ``get_legacy_hazards`` (PubChem / DSSTox / ToxVal / CPDB when configured).
    - ``iuclid_uuids``: dossier UUIDs from offline substances matching the CAS.
    - ``iuclid_substances``: one metadata dict per UUID (name, EC, UUID).
    - ``iuclid_endpoints``: flattened endpoint dicts, each including ``uuid``.
    - ``iuclid_cl_rows``: C&L / GHS-style rows from ``offline_cl_hazards`` for those UUIDs.
    """
    cas_raw = (cas_number or "").strip()
    cas_norm = normalize_cas(cas_raw) or cas_raw

    legacy = get_legacy_hazards(cas_norm)

    uuids = ctx.uuids_for_cas(cas_norm)
    substance_rows: list[dict[str, Any]] = []
    endpoint_rows: list[dict[str, Any]] = []
    cl_rows: list[dict[str, Any]] = []

    uid_set = set(uuids)
    if not ctx.substances_df.empty and uid_set:
        sub = ctx.substances_df[ctx.substances_df["uuid"].astype(str).isin(uid_set)]
        seen_meta: set[str] = set()
        for _, r in sub.iterrows():
            uid = str(r.get("uuid") or "")
            if not uid or uid in seen_meta:
                continue
            seen_meta.add(uid)
            substance_rows.append(
                {
                    "uuid": uid,
                    "cas_number": r.get("cas_number"),
                    "ec_number": r.get("ec_number"),
                    "substance_name": r.get("substance_name"),
                }
            )

    for uid in uuids:
        path = ctx.i6z_path_for_uuid(uid)
        for ep in extract_endpoints_for_uuid(path):
            row = dict(ep)
            row["uuid"] = uid
            endpoint_rows.append(row)

    if not ctx.cl_hazards_df.empty and uid_set and "substance_uuid" in ctx.cl_hazards_df.columns:
        cdf = ctx.cl_hazards_df[ctx.cl_hazards_df["substance_uuid"].astype(str).isin(uid_set)]
        cl_rows = cdf.to_dict(orient="records")

    return {
        "cas": cas_norm,
        "legacy": legacy,
        "iuclid_uuids": uuids,
        "iuclid_substances": substance_rows,
        "iuclid_endpoints": endpoint_rows,
        "iuclid_cl_rows": cl_rows,
    }
