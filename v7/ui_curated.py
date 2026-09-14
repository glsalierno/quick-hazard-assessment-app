"""Streamlit Curated Knowledge panel with provenance badges + optional SDS fetch."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from v7.knowledge import KnowledgeRecord
from v7.orchestrator import KnowledgeOrchestrator
from v7.providers.base import SDSHit
from v7.sds_acquisition import user_facing_sds_error

_DISPLAY_FIELDS = (
    ("hansen_d", "Hansen D"),
    ("hansen_p", "Hansen P"),
    ("hansen_h", "Hansen H"),
    ("rer", "RER"),
    ("chem21_ranking", "CHEM21 ranking"),
    ("chem21_safety", "CHEM21 Safety"),
    ("chem21_health", "CHEM21 Health"),
    ("chem21_env", "CHEM21 Env"),
    ("nfpa_health", "NFPA Health"),
    ("nfpa_fire", "NFPA Fire"),
    ("gloves", "Gloves / PPE"),
    ("ppe", "PPE"),
    ("tlv", "TLV"),
    ("idlh", "IDLH"),
    ("flash_point", "Flash point"),
    ("flash_point_c", "Flash point (°C)"),
    ("vapor_pressure", "Vapor pressure"),
    ("signal_word", "Signal word"),
    ("ghs_h_codes", "GHS H-codes"),
    ("ghs_text", "GHS (DoSS text)"),
)

_SDS_PROVIDERS = frozenset({"sigma_sds", "sigma", "tci_sds", "tci", "doss"})


def _badge(ev: Any) -> str:
    return f"✓ {ev.source} ({ev.provider})"


def _hit_label(hit: SDSHit) -> str:
    title = (hit.title or "").strip()
    prod = hit.product_id or "?"
    if title:
        return f"{prod} — {title}"
    return str(prod)


def render_sds_provenance(meta: dict[str, Any] | None) -> None:
    if not meta:
        return
    st.markdown("**SDS provenance**")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Product #", meta.get("product_number") or "—")
    with c2:
        st.metric("Revision", meta.get("revision") or "—")
    with c3:
        reused = meta.get("reused_cache")
        st.metric("Cache", "reuse" if reused else ("fresh" if reused is False else "—"))
    with c4:
        sha = str(meta.get("sha256") or "")
        st.metric("SHA-256", f"{sha[:12]}…" if len(sha) > 12 else (sha or "—"))
    if meta.get("source_url"):
        st.caption(f"Source: {meta['source_url']}")
    if meta.get("cached_path"):
        st.caption(f"Cached: {meta['cached_path']}")
    if meta.get("manufacturer") or meta.get("provider"):
        st.caption(
            f"Manufacturer: {meta.get('manufacturer') or '—'} · "
            f"Provider: {meta.get('provider') or '—'}"
        )
    if meta.get("via"):
        st.caption(f"Resolved via: {meta['via']}")
    extra = meta.get("extra") if isinstance(meta.get("extra"), dict) else {}
    warn = str(meta.get("warning") or extra.get("warning") or "").strip()
    if warn:
        st.warning(warn)
    title = meta.get("title")
    if title:
        st.caption(f"Catalog title: {title}")


def auto_fetch_tci_sds(
    cas: str,
    orchestrator: KnowledgeOrchestrator,
    *,
    pubchem: dict[str, Any] | None = None,
    product_number: str | None = None,
    enable_live_search: bool = True,
    allow_first_hit: bool = False,
) -> tuple[KnowledgeRecord | None, dict[str, Any] | None, str | None, list[SDSHit]]:
    """
    Default SDS auto-search path: TCI only (no Sigma API).

    Returns ``(record, meta, error_message, ambiguous_hits)``.
    On multi-product CAS without ``product_number``, returns hits in the 4th slot.
    """
    from v7.compile_service import find_tci_hits
    from v7.tci_catalog import remember_tci_mapping

    cas = (cas or "").strip()
    if not cas:
        return None, None, "CAS is required for SDS auto search.", []

    try:
        if (product_number or "").strip():
            hits = find_tci_hits(
                cas=cas,
                product_number=product_number.strip(),
                enable_live_search=False,
            )
        else:
            hits = find_tci_hits(cas=cas, enable_live_search=enable_live_search)
    except Exception as exc:
        return None, None, user_facing_sds_error(exc), []

    if not hits:
        return (
            None,
            None,
            "No TCI product for this CAS. Paste a TCI product number below to teach the map.",
            [],
        )
    if len(hits) > 1 and not (product_number or "").strip() and not allow_first_hit:
        return None, None, None, hits

    last_err = ""
    for hit in hits:
        try:
            record, meta = orchestrator.assemble_with_sds_hit(cas, hit, pubchem=pubchem)
            prod = str((meta or {}).get("product_number") or hit.product_id or "")
            if prod:
                try:
                    remember_tci_mapping(cas, prod, notes="learned from SDS auto search")
                except Exception:
                    pass
            return record, meta, None, []
        except Exception as exc:
            last_err = user_facing_sds_error(exc)
            continue
    return None, None, last_err or "TCI SDS download failed.", []


def render_sds_fetch_panel(
    cas: str,
    orchestrator: KnowledgeOrchestrator,
    *,
    pubchem: dict[str, Any] | None = None,
    auto_run: bool = False,
) -> tuple[KnowledgeRecord | None, dict[str, Any] | None]:
    """
    TCI SDS auto-search (Sigma skipped — no API key).

    When ``auto_run`` is True, fetch immediately. Product picker / product # only
    when needed (ambiguous CAS or map miss).
    """
    meta_key = f"curated_sds_meta_{cas}"
    hits_key = f"curated_sds_hits_{cas}_tci"
    err_key = f"curated_sds_err_{cas}"

    if auto_run and not st.session_state.get(f"curated_sds_auto_done_{cas}"):
        with st.spinner("Finding TCI SDS…"):
            rec, meta, err, ambiguous = auto_fetch_tci_sds(
                cas, orchestrator, pubchem=pubchem, enable_live_search=True
            )
        st.session_state[f"curated_sds_auto_done_{cas}"] = True
        if ambiguous:
            st.session_state[hits_key] = ambiguous
        if meta:
            st.session_state[meta_key] = meta
            st.session_state[f"curated_sds_record_{cas}"] = rec
            st.session_state.pop(err_key, None)
            return rec, meta
        if err:
            st.session_state[err_key] = err

    err = st.session_state.get(err_key)
    if err:
        st.warning(str(err))

    hits: list[SDSHit] = list(st.session_state.get(hits_key) or [])
    if hits:
        labels = [_hit_label(h) for h in hits]
        idx = st.selectbox(
            "Multiple TCI products — choose grade",
            options=list(range(len(hits))),
            format_func=lambda i: labels[i],
            key=f"curated_sds_pick_{cas}",
        )
        if st.button("Download selected TCI SDS", type="primary", key=f"curated_sds_dl_pick_{cas}"):
            chosen = hits[int(idx)]
            with st.spinner("Downloading TCI SDS…"):
                try:
                    record, meta = orchestrator.assemble_with_sds_hit(
                        cas, chosen, pubchem=pubchem
                    )
                    st.session_state[meta_key] = meta
                    st.session_state[f"curated_sds_record_{cas}"] = record
                    st.session_state.pop(hits_key, None)
                    st.session_state.pop(err_key, None)
                    return record, meta
                except Exception as exc:
                    st.error(user_facing_sds_error(exc))

    teach = st.text_input(
        "TCI product # (if not in map)",
        placeholder="M0097",
        key=f"curated_tci_teach_{cas}",
    )
    if st.button("Fetch with product #", key=f"curated_tci_teach_go_{cas}"):
        if not (teach or "").strip():
            st.warning("Enter a TCI product number.")
        else:
            with st.spinner("Downloading TCI SDS…"):
                rec, meta, err2, _ = auto_fetch_tci_sds(
                    cas,
                    orchestrator,
                    pubchem=pubchem,
                    product_number=teach.strip(),
                    enable_live_search=False,
                )
            if meta:
                st.session_state[meta_key] = meta
                st.session_state[f"curated_sds_record_{cas}"] = rec
                st.session_state.pop(err_key, None)
                st.session_state.pop(hits_key, None)
                return rec, meta
            if err2:
                st.error(err2)

    meta = st.session_state.get(meta_key)
    cached_rec = st.session_state.get(f"curated_sds_record_{cas}")
    if isinstance(meta, dict):
        if isinstance(cached_rec, KnowledgeRecord):
            return cached_rec, meta
        return None, meta
    return None, None


def render_curated_knowledge(
    record: KnowledgeRecord | None,
    *,
    sds_meta: dict[str, Any] | None = None,
) -> None:
    if record is None:
        st.info("Enter a CAS, then click **SDS auto search**.")
        return

    if sds_meta:
        render_sds_provenance(sds_meta)

    p2 = record.p2oasys
    if p2:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("DoSS P2OASys", f"{p2.doss_score:.1f}" if p2.doss_score is not None else "—")
        with c2:
            st.metric("Computed P2OASys", f"{p2.computed_score:.1f}" if p2.computed_score is not None else "—")
        with c3:
            st.metric("Difference", f"{p2.difference:.1f}" if p2.difference is not None else "—")
        if p2.reason:
            st.caption(p2.reason)

    rows = []
    for key, label in _DISPLAY_FIELDS:
        ev = record.fields.get(key)
        if ev is None:
            continue
        rows.append(
            {
                "Field": label,
                "Value": ev.value if not isinstance(ev.value, list) else ", ".join(str(x) for x in ev.value),
                "Provenance": _badge(ev),
                "Confidence": ev.confidence,
                "Citation": ev.citation,
                "Retrieved": ev.retrieval_date,
            }
        )
    if rows:
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    else:
        st.warning("No curated fields for this CAS yet.")

    with st.expander("HSPiP / product SDS provenance", expanded=False):
        hsp = [k for k in ("hansen_d", "hansen_p", "hansen_h", "rer") if k in record.fields]
        if hsp:
            for k in hsp:
                ev = record.fields[k]
                st.write(f"**{k}:** {ev.value} — {_badge(ev)}")
        else:
            st.caption("No Hansen / RER row. Import an HSPiP cache CSV or DoSS workbook.")
        sds_fields = [
            k
            for k, ev in record.fields.items()
            if ev.provider in _SDS_PROVIDERS
            and ("sds" in (ev.source or "").lower() or ev.provider.endswith("_sds"))
        ]
        if sds_fields:
            for k in sds_fields:
                ev = record.fields[k]
                st.write(f"**{k}:** {ev.value} — {_badge(ev)}")
        else:
            st.caption("No product SDS fields merged yet — TCI fetch may have failed or the map has no product.")
