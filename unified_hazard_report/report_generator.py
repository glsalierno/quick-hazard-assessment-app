"""Flatten ``unified_lookup`` results into one row per hazard line or IUCLID endpoint."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from unified_hazard_report.data_context import OfflineDataContext
from unified_hazard_report.unified_lookup import unified_lookup


def _append_row(
    rows: list[dict[str, Any]],
    *,
    cas: str,
    source_type: str,
    source_name: str,
    hazard_code: str,
    hazard_statement: str,
    endpoint_name: str,
    endpoint_value: str,
    units: str,
    uuid: str,
) -> None:
    rows.append(
        {
            "cas": cas,
            "source_type": source_type,
            "source_name": source_name,
            "hazard_code": hazard_code,
            "hazard_statement": hazard_statement,
            "endpoint_name": endpoint_name,
            "endpoint_value": endpoint_value,
            "units": units,
            "uuid": uuid,
        }
    )


def _legacy_rows(cas: str, legacy: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pub = legacy.get("pubchem") or {}
    if legacy.get("fetch_error"):
        _append_row(
            rows,
            cas=cas,
            source_type="legacy",
            source_name="system",
            hazard_code="",
            hazard_statement=f"fetch_note: {legacy.get('fetch_error')}",
            endpoint_name="",
            endpoint_value="",
            units="",
            uuid="",
        )

    ghs = pub.get("ghs") or {}
    for h in ghs.get("h_codes") or []:
        if not (h or "").strip():
            continue
        _append_row(
            rows,
            cas=cas,
            source_type="legacy",
            source_name="PubChem",
            hazard_code=str(h).strip(),
            hazard_statement="",
            endpoint_name="GHS",
            endpoint_value="",
            units="",
            uuid="",
        )
    for p in ghs.get("p_codes") or []:
        if not (p or "").strip():
            continue
        _append_row(
            rows,
            cas=cas,
            source_type="legacy",
            source_name="PubChem",
            hazard_code=str(p).strip(),
            hazard_statement="",
            endpoint_name="GHS_P",
            endpoint_value="",
            units="",
            uuid="",
        )

    for tox in pub.get("toxicities") or []:
        if not isinstance(tox, dict):
            continue
        val = tox.get("value") or tox.get("type") or ""
        stmt = "; ".join(
            str(x)
            for x in (
                tox.get("type"),
                tox.get("species_route"),
                tox.get("route"),
                tox.get("species"),
            )
            if x
        )
        _append_row(
            rows,
            cas=cas,
            source_type="legacy",
            source_name="PubChem",
            hazard_code="",
            hazard_statement=stmt[:2000],
            endpoint_name=str(tox.get("type") or "toxicity"),
            endpoint_value=str(val)[:2000],
            units=str(tox.get("unit") or ""),
            uuid="",
        )

    toxval = legacy.get("toxval_data")
    if isinstance(toxval, dict):
        for cat, recs in toxval.items():
            if not isinstance(recs, list):
                continue
            for rec in recs:
                if not isinstance(rec, dict):
                    continue
                parts = [f"{cat}", str(rec.get("study_type") or ""), str(rec.get("species") or "")]
                stmt = " | ".join(p for p in parts if p)
                _append_row(
                    rows,
                    cas=cas,
                    source_type="legacy",
                    source_name="ToxValDB",
                    hazard_code="",
                    hazard_statement=stmt[:2000],
                    endpoint_name=str(rec.get("study_type") or cat),
                    endpoint_value=str(rec.get("value") or ""),
                    units=str(rec.get("units") or ""),
                    uuid="",
                )

    carc = legacy.get("carc_potency_data")
    if isinstance(carc, dict) and carc.get("found"):
        for ex in carc.get("experiments") or []:
            if not isinstance(ex, dict):
                continue
            _append_row(
                rows,
                cas=cas,
                source_type="legacy",
                source_name="CPDB",
                hazard_code="",
                hazard_statement=str(ex.get("summary") or ex)[:2000],
                endpoint_name="carcinogenicity_potency",
                endpoint_value=str(ex.get("td50") or ex.get("value") or "")[:500],
                units=str(ex.get("units") or ""),
                uuid="",
            )

    return rows


def _iuclid_rows(cas: str, block: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for meta in block.get("iuclid_substances") or []:
        if not isinstance(meta, dict):
            continue
        uid = str(meta.get("uuid") or "")
        nm = str(meta.get("substance_name") or "").strip()
        ec = str(meta.get("ec_number") or "").strip()
        if not uid and not nm and not ec:
            continue
        stmt = " | ".join(x for x in (nm, f"EC {ec}" if ec else "") if x) or uid
        _append_row(
            rows,
            cas=cas,
            source_type="iuclid",
            source_name="REACH_dossier_meta",
            hazard_code="",
            hazard_statement=stmt[:2000],
            endpoint_name="substance_identity",
            endpoint_value=uid,
            units="",
            uuid=uid,
        )
    for ep in block.get("iuclid_endpoints") or []:
        if not isinstance(ep, dict):
            continue
        uid = str(ep.get("uuid") or "")
        _append_row(
            rows,
            cas=cas,
            source_type="iuclid",
            source_name="REACH_study_i6d",
            hazard_code="",
            hazard_statement="",
            endpoint_name=str(ep.get("endpoint_name") or ""),
            endpoint_value=str(ep.get("result") or ""),
            units=str(ep.get("units") or ""),
            uuid=uid,
        )
    for cr in block.get("iuclid_cl_rows") or []:
        if not isinstance(cr, dict):
            continue
        uid = str(cr.get("substance_uuid") or "")
        code = str(cr.get("h_statement_code") or "").strip()
        text = str(cr.get("h_statement_text") or "").strip()
        hc = str(cr.get("hazard_class") or "").strip()
        stmt = text or hc
        _append_row(
            rows,
            cas=cas,
            source_type="iuclid",
            source_name="REACH_i6d_CL",
            hazard_code=code,
            hazard_statement=stmt[:4000],
            endpoint_name="classification",
            endpoint_value="",
            units="",
            uuid=uid,
        )
    return rows


def flatten_unified(cas: str, block: dict[str, Any]) -> list[dict[str, Any]]:
    legacy = block.get("legacy") or {}
    legacy_part = _legacy_rows(cas, legacy)
    iuclid_part = _iuclid_rows(cas, block)
    combined = legacy_part + iuclid_part
    if not combined:
        combined = []
        _append_row(
            combined,
            cas=cas,
            source_type="warning",
            source_name="none",
            hazard_code="",
            hazard_statement="No legacy PubChem row and no IUCLID dossier rows for this CAS.",
            endpoint_name="",
            endpoint_value="",
            units="",
            uuid="",
        )
    return combined


def generate_report(
    cas_list: Iterable[str],
    ctx: OfflineDataContext,
    *,
    output_format: str = "csv",
    output_path: Path | str | None = None,
) -> pd.DataFrame:
    """
    Run ``unified_lookup`` for each CAS and return a long-form ``DataFrame``.

    If ``output_path`` is set, writes CSV / JSON / Excel depending on ``output_format``.
    """
    all_rows: list[dict[str, Any]] = []
    for cas in cas_list:
        c = (cas or "").strip()
        if not c:
            continue
        block = unified_lookup(c, ctx)
        all_rows.extend(flatten_unified(block.get("cas") or c, block))

    df = pd.DataFrame(all_rows)
    if output_path is None:
        return df

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = output_format.lower().strip()
    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "json":
        df.to_json(path, orient="records", indent=2, force_ascii=False)
    elif fmt in ("excel", "xlsx"):
        try:
            df.to_excel(path, index=False, engine="openpyxl")
        except ImportError as exc:
            raise RuntimeError("Excel output requires openpyxl: pip install openpyxl") from exc
    else:
        raise ValueError(f"Unsupported format: {output_format}")
    return df
