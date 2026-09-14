"""
Compile Hazard Lookup — v7 CAS→SDS bot + GHaz6 assessment + P2OASys / HSPiP.

v7 differentiator (vs v6): the user provides a **CAS**; the system finds the SDS
(TCI HTTP bot), reads it, and returns hazard + reference DB info. SDS upload and
typed CAS-only remain as fallbacks.

Run from the app root::

    streamlit run app.py
    # then open page "Compile Hazard Lookup"

Or as a single-page entry::

    streamlit run pages/01_Compile_Hazard_Lookup.py
"""

from __future__ import annotations

import json
from typing import Any

import streamlit as st

st.set_page_config(
    page_title="Compile Hazard Lookup",
    layout="wide",
    initial_sidebar_state="expanded",
)

import config
from v7.compile_service import (
    CompileBundle,
    TCIAmbiguousProductError,
    compile_from_cas,
    compile_from_sds_upload,
    compile_from_tci,
    compile_from_tci_hit,
    find_tci_hits,
    source_status,
)
from v7.providers.base import SDSHit
from v7.providers.tci import TCIError
from v7.sds_acquisition import user_facing_sds_error
from v7.tci_catalog import tci_search_url
from v7.ui_curated import render_sds_provenance


def _status_badge(ok: bool, label: str, detail: str) -> None:
    icon = "✓" if ok else "·"
    st.caption(f"{icon} **{label}** — {detail}")


def _hit_label(hit: SDSHit) -> str:
    title = (hit.title or "").strip()
    prod = hit.product_id or "?"
    if title:
        return f"{prod} — {title}"
    return str(prod)


def _render_source_panel() -> None:
    st.subheader("Data sources")
    status = source_status()
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        p2 = status["p2oasys"]
        _status_badge(
            p2["ok"],
            "P2OASys expert",
            f"{p2['rows']} CAS" if p2["ok"] else "CSV missing",
        )
    with c2:
        doss = status["doss_hsp"]
        _status_badge(
            doss["ok"],
            "Hansen (DoSS)",
            f"{doss['rows']} CAS" if doss["ok"] else "DoSS.xlsx missing",
        )
    with c3:
        hsp = status["hspip_cache"]
        _status_badge(
            hsp["ok"],
            "HSPiP cache",
            f"{hsp['rows']} CAS" if hsp["ok"] else "hsp_by_cas.csv optional",
        )
    with c4:
        tci = status["tci_map"]
        detail = (
            f"{tci.get('merged_rows', 0)} products"
            if tci.get("ok")
            else "map empty (product # / live search)"
        )
        if tci.get("learned_rows"):
            detail += f" · {tci['learned_rows']} learned"
        _status_badge(bool(tci.get("ok")), "TCI product map", detail)
    st.caption(
        "P2OASys / CHEM21 / Hansen are **database lookups only**. "
        "The TCI bot grows the product map when an SDS fetch succeeds."
    )


def _render_hazard_summary(bundle: CompileBundle) -> None:
    data = bundle.result_data
    pub = data.get("pubchem") or {}
    ghs = pub.get("ghs") or {}
    st.markdown(f"### {bundle.preferred_name or 'Chemical'} · `{bundle.cas}`")
    st.caption(f"Resolved via **{bundle.source}**")
    for w in bundle.warnings:
        st.warning(w)
    if data.get("fetch_error"):
        st.error(str(data["fetch_error"]))

    if bundle.sds_meta:
        render_sds_provenance(bundle.sds_meta)

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("CAS", bundle.cas)
    with m2:
        st.metric("Signal word", (ghs.get("signal_word") or "—") or "—")
    with m3:
        h_codes = ghs.get("h_codes") or []
        st.metric("H-codes", len(h_codes) if h_codes else "—")
    with m4:
        st.metric("DTXSID", data.get("dtxsid") or "—")

    if ghs.get("h_codes"):
        st.markdown("**GHS H-codes:** " + ", ".join(ghs["h_codes"][:24]))
        if len(ghs["h_codes"]) > 24:
            st.caption(f"…and {len(ghs['h_codes']) - 24} more")
    if ghs.get("p_codes"):
        with st.expander("P-codes"):
            st.write(", ".join(ghs["p_codes"]))

    props = []
    if pub.get("molecular_weight") is not None:
        props.append(f"MW {pub['molecular_weight']}")
    if pub.get("flash_point"):
        props.append(f"Flash point: {pub['flash_point']}")
    if pub.get("boiling_point"):
        props.append(f"BP: {pub['boiling_point']}")
    if pub.get("vapor_pressure"):
        props.append(f"VP: {pub['vapor_pressure']}")
    if props:
        st.caption(" · ".join(str(p) for p in props))

    if pub.get("iupac_name"):
        st.caption(f"IUPAC: {pub['iupac_name']}")
    if pub.get("canonical_smiles") or pub.get("smiles"):
        st.code(pub.get("canonical_smiles") or pub.get("smiles"), language=None)


def _render_lookups(bundle: CompileBundle) -> None:
    p2 = bundle.lookups.p2oasys
    hsp = bundle.lookups.hsp
    c21 = bundle.lookups.chem21

    st.subheader("Reference database lookup (not prediction)")
    col_p2, col_hsp, col_c21 = st.columns(3)

    with col_p2:
        st.markdown("**P2OASys (expert)**")
        if p2["status"] == "found":
            st.metric("Score", f"{p2['score']:.1f}" if p2.get("score") is not None else "—")
            st.caption(p2.get("name") or "")
            if p2.get("date_created"):
                st.caption(f"Record date: {p2['date_created']}")
            if p2.get("source_path"):
                st.caption(str(p2["source_path"]))
        elif p2["status"] == "not_found":
            st.metric("Score", "—")
            st.warning(p2.get("message") or "P2OASys assessment required")
        else:
            st.metric("Score", "—")
            st.info(p2.get("message") or "Source unavailable")

    with col_hsp:
        st.markdown("**Hansen (HSPiP / DoSS)**")
        if hsp["status"] == "found":
            st.metric(
                "δD / δP / δH",
                f"{hsp.get('delta_d')} / {hsp.get('delta_p')} / {hsp.get('delta_h')}",
            )
            st.caption(str(hsp.get("source") or ""))
            if hsp.get("source_path"):
                st.caption(str(hsp["source_path"]))
        else:
            st.metric("δD / δP / δH", "—")
            st.markdown(hsp.get("message") or "Not found")

    with col_c21:
        st.markdown("**CHEM21**")
        if c21["status"] == "found":
            st.metric("Ranking", c21.get("ranking_default") or "—")
            st.caption(
                f"S {c21.get('safety') if c21.get('safety') is not None else '—'} · "
                f"H {c21.get('health') if c21.get('health') is not None else '—'} · "
                f"E {c21.get('env') if c21.get('env') is not None else '—'}"
            )
            if c21.get("solvent"):
                st.caption(str(c21["solvent"]))
        else:
            st.metric("Ranking", "—")
            st.info(c21.get("message") or "Not in guide")


def _export_payload(bundle: CompileBundle) -> dict[str, Any]:
    return {
        "cas": bundle.cas,
        "preferred_name": bundle.preferred_name,
        "source": bundle.source,
        "warnings": bundle.warnings,
        "sds_meta": bundle.sds_meta,
        "p2oasys": bundle.lookups.p2oasys,
        "hansen": bundle.lookups.hsp,
        "chem21": bundle.lookups.chem21,
        "hazard": {
            "dtxsid": bundle.result_data.get("dtxsid"),
            "preferred_name": bundle.result_data.get("preferred_name"),
            "ghs": (bundle.result_data.get("pubchem") or {}).get("ghs"),
            "fetch_error": bundle.result_data.get("fetch_error"),
        },
    }


def _cas_in_local_tci_map(cas: str) -> bool:
    cas = (cas or "").strip()
    if not cas:
        return False
    try:
        hits = find_tci_hits(cas=cas, enable_live_search=False)
        return bool(hits)
    except Exception:
        return False


def _render_tci_mode() -> CompileBundle | None:
    st.markdown(
        "**v7 primary path:** enter a CAS — we resolve the TCI product, download the "
        "official SDS, parse it, then return GHaz6 hazard + P2OASys / HSPiP / CHEM21."
    )
    stats = source_status().get("tci_map") or {}
    st.caption(
        f"Local TCI map: {stats.get('merged_rows', 0)} products "
        f"(seed {stats.get('seed_rows', 0)} · learned {stats.get('learned_rows', 0)}). "
        "Successful fetches are remembered for next time."
    )

    live = st.checkbox(
        "Try TCI website search when CAS is not in the local map",
        value=True,
        key="compile_tci_live",
        help="Often blocked (Akamai) on campus/server networks. Map + product # still work.",
    )

    tci_cas = st.text_input("CAS number", placeholder="67-56-1", key="compile_tci_cas")
    cas_key = (tci_cas or "").strip()

    # Clear stale product picker when CAS changes
    prev = st.session_state.get("compile_tci_cas_prev")
    if prev != cas_key:
        st.session_state["compile_tci_cas_prev"] = cas_key
        st.session_state.pop("compile_tci_hits", None)

    if cas_key:
        in_map = _cas_in_local_tci_map(cas_key)
        if in_map:
            st.success("CAS is in the local TCI product map — SDS fetch should not need live search.")
        else:
            st.warning(
                "CAS is **not** in the local map yet. Live search may be blocked; "
                "paste a TCI product number once below to teach the map, or open the catalog link."
            )
        st.markdown(
            f"[Open TCI catalog search for `{cas_key}`]({tci_search_url(cas_key)}) "
            "(browser fallback)"
        )

    tci_prod = st.text_input(
        "TCI product number (optional — teach the map when live search fails)",
        placeholder="M0097",
        key="compile_tci_prod",
    )
    with st.expander("Optional name hint"):
        tci_name = st.text_input("Name hint", placeholder="methanol", key="compile_tci_name")

    hits: list[SDSHit] = list(st.session_state.get("compile_tci_hits") or [])
    selected_hit: SDSHit | None = None
    if hits:
        st.info(f"{len(hits)} TCI products match this CAS — choose the grade, then fetch.")
        labels = [_hit_label(h) for h in hits]
        idx = st.selectbox(
            "TCI product",
            options=list(range(len(hits))),
            format_func=lambda i: labels[i],
            key="compile_tci_pick",
        )
        selected_hit = hits[int(idx)]
        if selected_hit.sds_url:
            st.caption(selected_hit.sds_url)

    if st.button("Find SDS + compile", type="primary", key="compile_tci_go"):
        if not (tci_cas or tci_prod or tci_name):
            st.error("Enter a CAS number (product number is optional).")
            return None
        status = st.empty()
        try:
            if (tci_prod or "").strip():
                status.caption("Downloading SDS for product → assessing…")
                return compile_from_tci(
                    cas=tci_cas or "",
                    product_number=tci_prod.strip(),
                    name=tci_name or "",
                    enable_live_search=live,
                )
            if selected_hit is not None and hits:
                status.caption(
                    f"Downloading SDS for {selected_hit.product_id} → assessing…"
                )
                bundle = compile_from_tci_hit(selected_hit)
                st.session_state.pop("compile_tci_hits", None)
                return bundle

            status.caption("Resolving CAS → TCI product…")
            try:
                return compile_from_tci(
                    cas=tci_cas or "",
                    name=tci_name or "",
                    enable_live_search=live,
                    allow_first_hit=False,
                )
            except TCIAmbiguousProductError as exc:
                st.session_state["compile_tci_hits"] = exc.hits
                st.warning(str(exc))
                st.info("Select a product above, then click **Find SDS + compile** again.")
                st.rerun()
                return None
        except TCIError as exc:
            st.error(user_facing_sds_error(exc))
            st.info(
                "Workaround: open the TCI search link, note the product code "
                "(e.g. M0097), paste it above once — the map will remember it."
            )
            return None
        except Exception as exc:
            st.error(str(exc))
            return None
        finally:
            status.empty()
    return None


st.title("Compile Hazard Lookup")
st.markdown(
    "**/v7 vs v6:** instead of uploading an SDS, give a **CAS** and let the "
    "**TCI SDS bot** find and read the sheet, then compile GHaz6 + expert "
    "P2OASys + HSPiP/DoSS Hansen."
)

_render_source_panel()

mode = st.radio(
    "Input",
    [
        "TCI SDS bot (recommended)",
        "CAS / name (no SDS)",
        "SDS upload (v6-style fallback)",
    ],
    horizontal=True,
    help="Primary v7 path is TCI SDS bot: CAS → find SDS → read → compile.",
)

bundle: CompileBundle | None = None

if mode.startswith("TCI"):
    got = _render_tci_mode()
    if got is not None:
        bundle = got
        st.session_state["compile_bundle"] = bundle

elif mode.startswith("CAS"):
    examples = [label for _, label in config.EXAMPLE_CHEMICALS]
    cols = st.columns([3, 1])
    with cols[0]:
        query = st.text_input(
            "CAS number or chemical name",
            placeholder="e.g. 67-56-1 or methanol",
            key="compile_cas_query",
        )
    with cols[1]:
        pick = st.selectbox("Examples", ["—"] + examples, key="compile_cas_ex")
        if pick != "—":
            query = pick.split(" ", 1)[0]
    if st.button("Assess + look up", type="primary", key="compile_cas_go"):
        if not (query or "").strip():
            st.error("Enter a CAS or name.")
        else:
            with st.spinner("Assessing and looking up databases…"):
                try:
                    bundle = compile_from_cas(query.strip())
                    st.session_state["compile_bundle"] = bundle
                except Exception as exc:
                    st.error(str(exc))

else:
    uploaded = st.file_uploader("SDS PDF", type=["pdf"], key="compile_sds_upload")
    if st.button("Extract CAS + look up", type="primary", key="compile_sds_go"):
        if uploaded is None:
            st.error("Upload an SDS PDF first.")
        else:
            with st.spinner("Parsing SDS and looking up databases…"):
                try:
                    bundle = compile_from_sds_upload(uploaded)
                    st.session_state["compile_bundle"] = bundle
                except Exception as exc:
                    st.error(str(exc))

if bundle is None:
    bundle = st.session_state.get("compile_bundle")

if isinstance(bundle, CompileBundle):
    st.divider()
    tab_haz, tab_db, tab_json = st.tabs(
        ["Hazard (GHaz6)", "P2OASys / HSPiP / CHEM21", "Export JSON"]
    )
    with tab_haz:
        _render_hazard_summary(bundle)
        st.info(
            "Full GHS / ToxVal / OPERA / IUCLID panels remain on the main "
            "**Quick Hazard Assessment** page for this CAS."
        )
    with tab_db:
        _render_lookups(bundle)
    with tab_json:
        payload = _export_payload(bundle)
        st.download_button(
            "Download compile JSON",
            data=json.dumps(payload, indent=2, default=str),
            file_name=f"compile_{bundle.cas.replace('/', '-')}.json",
            mime="application/json",
        )
        st.json(payload)
else:
    st.info("Run the TCI SDS bot (or a fallback) above to see hazard + database lookups.")
