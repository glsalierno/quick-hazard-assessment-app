"""
Bridge SDS, OPERA, and ECOSAR outputs into P2OASys ``extra_sources`` (v6 Phase C).

SDS fields fill gaps with experimental (document) evidence.
OPERA predictions fill fate/physchem gaps when stronger experimental evidence is absent.
ECOSAR (EPI Suite API) fills aquatic LC50/EC50/ChV when measured aquatic evidence is absent.
Predictions are tagged ``predicted`` so the scorer/assessment can label them.
"""

from __future__ import annotations

from typing import Any, Optional


def ecosar_to_extra_sources(
    ecosar_result: dict[str, Any] | None,
    *,
    existing_hazard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Delegate to ``utils.ecosar_client.ecosar_to_extra_sources`` (aquatic gap-fill only)."""
    try:
        from utils.ecosar_client import ecosar_to_extra_sources as _bridge
    except ImportError:
        return {}
    return _bridge(ecosar_result, existing_hazard=existing_hazard)


def _f(x: Any) -> Optional[float]:
    try:
        if x is None or x == "" or str(x).lower() in ("nan", "na", "nd"):
            return None
        return float(str(x).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None


def opera_to_extra_sources(
    opera_summary: dict[str, Any] | None,
    *,
    existing_hazard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Convert ``opera_client.get_opera_predictions`` summary into gap-fill extras.

    Only adds endpoints that are missing from ``existing_hazard``. Predictions are
    tagged so scoring traces can mark them ``Predicted only``.

    Fate-relevant structured fields (when present on the OPERA row):
    - ``log_kow`` from ``LogP_pred``
    - ``bcf_l_kg`` from ``10 ** LogBCF_pred`` (also keeps phrase evidence)
    - ReadyBiodeg → Key-Phrase-friendly biodegradation strings (not Biowin_pred)
    - BioDeg_LogHalfLife → ``biodeg_half_life_days`` (hydrocarbon model; not water/soil t½)
    """
    if not opera_summary or not opera_summary.get("ok"):
        return {}
    row = opera_summary.get("row") or {}
    display = opera_summary.get("display") or {}
    existing = existing_hazard or {}
    existing_tox = " ".join(str(t.get("value") or "") for t in (existing.get("toxicities") or [])).lower()
    hm = existing.get("hazard_metrics") or {}
    has_fp = bool(hm.get("flash_point"))
    has_vp = any("mm" in str(x).lower() and "hg" in str(x).lower() for x in (hm.get("other_designations") or []))
    has_log_kow = existing.get("log_kow") is not None or bool(hm.get("log_kow"))
    has_bcf = existing.get("bcf_l_kg") is not None or bool(hm.get("bcf_l_kg"))

    toxicities: list[dict[str, Any]] = []
    hazard_metrics: dict[str, list] = {}
    notes: list[str] = []

    catmos = _f(row.get("CATMoS_LD50_pred") or row.get("CATMoS_LD50") or display.get("CATMoS_LD50"))
    if catmos is not None and "ld50" not in existing_tox:
        toxicities.append({
            "value": f"LD50 {catmos} mg/kg",
            "unit": "mg/kg",
            "species_route": ["oral", "predicted"],
            "source": "OPERA_CATMoS",
            "predicted": True,
        })
        notes.append("OPERA CATMoS LD50 used as oral gap-fill (predicted)")

    log_kow = _f(row.get("LogP_pred") or row.get("LogP") or display.get("LogP"))
    log_kow_out = None
    if log_kow is not None and not has_log_kow:
        log_kow_out = {"value": log_kow, "predicted": True, "source": "OPERA", "unit": "log10"}
        hazard_metrics.setdefault("log_kow", []).append(log_kow)
        toxicities.append({
            "value": f"OPERA LogP (log Kow) {log_kow}",
            "unit": "log10",
            "species_route": None,
            "source": "OPERA",
            "predicted": True,
        })
        notes.append(f"OPERA LogP_pred={log_kow} (predicted log Kow)")

    bcf = _f(row.get("LogBCF_pred") or row.get("LogBCF") or display.get("LogBCF"))
    bcf_out = None
    if bcf is not None:
        if bcf >= 3.0:
            band = "high bioaccumulation persistent"
            phrase = "Will bioaccumulate"
        elif bcf >= 2.0:
            band = "moderate bioaccumulation"
            phrase = "May bioaccumulate"
        else:
            band = "low bioaccumulation short"
            phrase = "Not likely/expected to bioaccumulate"
        toxicities.append({
            "value": f"OPERA LogBCF {bcf} ({band}); {phrase}",
            "unit": "log10(L/kg)",
            "species_route": None,
            "source": "OPERA",
            "predicted": True,
        })
        notes.append(f"OPERA LogBCF={bcf}")
        if not has_bcf:
            bcf_l_kg = 10.0 ** bcf
            bcf_out = {
                "value": bcf_l_kg,
                "log_bcf": bcf,
                "predicted": True,
                "source": "OPERA",
                "unit": "L/kg",
            }
            hazard_metrics.setdefault("bcf_l_kg", []).append(bcf_l_kg)

    # ReadyBiodeg (OPERA 2.9) — Biowin_pred is not emitted; keep as alias only.
    ready_raw = row.get("ReadyBiodeg_pred")
    if ready_raw in (None, "", "NA"):
        ready_raw = row.get("ReadyBiodeg") or row.get("RB_pred") or row.get("Biowin_pred")
    ready_f = _f(ready_raw)
    if ready_f is not None:
        if ready_f >= 0.5:
            tox_val = "OPERA ready biodegradation 1; Readily degradable; Biodegradable"
        else:
            tox_val = "OPERA ready biodegradation 0; Not likely to biodegrade; Not Degradable"
        toxicities.append({
            "value": tox_val,
            "unit": None,
            "species_route": None,
            "source": "OPERA",
            "predicted": True,
        })
        notes.append(f"OPERA ReadyBiodeg_pred={ready_raw}")

    # BioDeg log half-life (days) — hydrocarbon-oriented; NOT water/soil Persistence t½.
    biodeg_log = _f(
        row.get("BioDeg_LogHalfLife_pred")
        or row.get("BioDeg_LogHalfLife")
        or row.get("BioDeg_pred")
    )
    biodeg_hl_out = None
    if biodeg_log is not None:
        biodeg_days = 10.0 ** biodeg_log
        biodeg_hl_out = {
            "value": biodeg_days,
            "log_half_life": biodeg_log,
            "predicted": True,
            "source": "OPERA",
            "unit": "days",
            "note": "OPERA BioDeg (hydrocarbon training); not multimedia water/soil t1/2",
        }
        toxicities.append({
            "value": f"OPERA BioDeg half-life ~{biodeg_days:.4g} days (log10={biodeg_log})",
            "unit": "days",
            "species_route": None,
            "source": "OPERA",
            "predicted": True,
        })
        notes.append(f"OPERA BioDeg_LogHalfLife_pred={biodeg_log} (~{biodeg_days:.4g} d)")

    koc = row.get("LogKoc_pred")
    if koc not in (None, "", "NA"):
        toxicities.append({
            "value": f"OPERA LogKoc {koc}",
            "unit": None,
            "species_route": None,
            "source": "OPERA",
            "predicted": True,
        })

    vp = _f(row.get("LogVP_pred") or row.get("VP_pred"))
    if vp is not None and not has_vp:
        mmhg = 10 ** vp if -10 < vp < 5 else vp
        hazard_metrics.setdefault("other_designations", []).append(f"{mmhg} mmHg (OPERA predicted)")
        notes.append("OPERA vapor pressure used as gap-fill (predicted)")

    _ = has_fp  # reserved for future flash-point gap-fill
    mw = _f(row.get("MolWeight") or display.get("MolWeight"))

    # Expose OPERA pKa for Physical → pH cascade (Priority 2).
    try:
        from utils.opera_client import extract_opera_pka_from_row

        pka = extract_opera_pka_from_row(row)
    except Exception:
        pka = None
        pka_a = _f(row.get("pKa_a_pred") or row.get("pKa_a"))
        pka_b = _f(row.get("pKa_b_pred") or row.get("pKa_b"))
        if pka_a is not None or pka_b is not None:
            pka = {"pka_a": pka_a, "pka_b": pka_b, "predicted": True, "source": "OPERA"}
    if pka:
        hazard_metrics.setdefault("opera_pka", []).append(pka)
        notes.append(f"OPERA pKa_a={pka.get('pka_a')} pKa_b={pka.get('pka_b')} (for pH heuristic)")

    out: dict[str, Any] = {}
    if toxicities:
        out["toxicities"] = toxicities
    if hazard_metrics:
        out["hazard_metrics"] = hazard_metrics
    if mw is not None:
        out["molecular_weight"] = mw
    if log_kow_out is not None:
        out["log_kow"] = log_kow_out
    if bcf_out is not None:
        out["bcf_l_kg"] = bcf_out
    if biodeg_hl_out is not None:
        out["biodeg_half_life_days"] = biodeg_hl_out
    if pka:
        out["opera_pka"] = pka
    out["opera_row"] = dict(row)
    if notes:
        out["_pipeline_notes"] = notes
    return out



def structured_sds_to_extra_fields(structured: dict[str, Any] | None) -> dict[str, Any]:
    """
    Flatten ``v7.sds_structured.parse_structured_sds`` into the dict shape expected by
    :func:`sds_fields_to_extra_sources`.
    """
    if not structured:
        return {}
    out: dict[str, Any] = {}
    sec2 = structured.get("section_2") or {}
    sec5 = structured.get("section_5") or {}
    sec8 = structured.get("section_8") or {}
    sec9 = structured.get("section_9") or {}
    sec10 = structured.get("section_10") or {}
    sec11 = structured.get("section_11") or {}
    sec12 = structured.get("section_12") or {}

    h_codes = sec2.get("h_statements") or []
    if h_codes:
        out["ghs_h_codes"] = list(h_codes)
    if sec2.get("signal_word"):
        out["signal_word"] = sec2["signal_word"]

    ps = structured.get("physical_state") or sec9.get("physical_state") or sec9.get("form")
    if ps:
        out["physical_state"] = str(ps)
        out["form"] = str(ps)

    if sec9.get("flash_point"):
        out["flash_point"] = sec9["flash_point"]
    if sec9.get("vapor_pressure"):
        out["vapor_pressure"] = sec9["vapor_pressure"]

    if sec11.get("ld50_oral") is not None:
        unit = sec11.get("ld50_oral_unit") or "mg/kg"
        out["ld50_oral_mg_kg"] = sec11["ld50_oral"] if "mg" in str(unit).lower() else sec11["ld50_oral"]
        out["ld50_oral"] = f"LD50 {sec11['ld50_oral']} {unit}"
    if sec11.get("ld50_dermal") is not None:
        unit = sec11.get("ld50_dermal_unit") or "mg/kg"
        out["ld50_dermal_mg_kg"] = sec11["ld50_dermal"]
        out["ld50_dermal"] = f"LD50 {sec11['ld50_dermal']} {unit}"
    if sec11.get("lc50_inhalation") is not None:
        unit = sec11.get("lc50_inhalation_unit") or "ppm"
        out["lc50_inhalation"] = f"LC50 {sec11['lc50_inhalation']} {unit}"
        if "ppm" in str(unit).lower():
            out["lc50_inhalation_ppm"] = sec11["lc50_inhalation"]

    aq = sec12.get("aquatic_toxicity") or []
    if aq:
        first = aq[0]
        if isinstance(first, dict) and first.get("value") is not None:
            out["aquatic_toxicity"] = float(first["value"])
            out["lc50_aquatic_mg_l"] = float(first["value"])
        out["aquatic_toxicity_raw"] = first
    if sec12.get("phrase_cues"):
        out["eco_phrase_cues"] = list(sec12["phrase_cues"])
    if sec12.get("biodegradation"):
        out["biodegradation"] = sec12["biodegradation"]
    if sec12.get("persistence"):
        out["persistence"] = sec12["persistence"]
    if sec12.get("bcf") is not None:
        out["bcf"] = sec12["bcf"]
    if sec12.get("bioaccumulation"):
        out["bioaccumulation"] = sec12["bioaccumulation"]

    cues = list(sec5.get("combustion_phrase_cues") or []) + list(sec10.get("combustion_phrase_cues") or [])
    if cues:
        out["combustion_phrase_cues"] = list(dict.fromkeys(cues))

    # OELs as light evidence strings (no GESTIS scrape — SDS §8 only)
    oels = []
    for key in ("osha_pel", "niosh_rel", "tlv", "stel", "idlh"):
        item = sec8.get(key)
        if isinstance(item, dict) and item.get("value"):
            oels.append(f"{item.get('label', key)} {item['value']} {item.get('unit') or ''}".strip())
    if oels:
        out["exposure_limits"] = oels

    if structured.get("nfpa_health") is not None:
        out["nfpa_health"] = structured["nfpa_health"]
    if structured.get("nfpa_fire") is not None:
        out["nfpa_fire"] = structured["nfpa_fire"]

    return out


def sds_parsed_result_to_extra_fields(parsed: dict[str, Any] | None) -> dict[str, Any]:
    """
    Flatten ``extract_sds_fields_from_text`` output into the dict shape expected by
    :func:`sds_fields_to_extra_sources`.
    """
    if not parsed:
        return {}
    out: dict[str, Any] = {}
    ghs = parsed.get("ghs") or {}
    if isinstance(ghs, dict) and ghs.get("h_codes"):
        out["ghs_h_codes"] = list(ghs["h_codes"])
    quant = parsed.get("quantitative") or {}
    fps = quant.get("flash_point") or []
    if fps:
        first = fps[0]
        if isinstance(first, dict) and first.get("value_c") is not None:
            out["flash_point"] = f"{first['value_c']} °C"
        else:
            out["flash_point"] = str(first)
    vps = quant.get("vapor_pressure") or []
    if vps:
        first = vps[0]
        if isinstance(first, dict):
            val = first.get("value")
            unit = first.get("unit") or "mmHg"
            out["vapor_pressure"] = f"{val} {unit}" if val is not None else str(first)
        else:
            out["vapor_pressure"] = str(first)
    aqs = quant.get("aquatic_toxicity") or []
    if aqs:
        first = aqs[0]
        if isinstance(first, dict) and first.get("value") is not None:
            unit = first.get("unit") or "mg/L"
            out["aquatic_toxicity"] = (
                float(first["value"]) if unit.lower().startswith("mg") else f"{first['value']} {unit}"
            )
        else:
            out["aquatic_toxicity"] = str(first)
    # Optional richer keys if caller already attached them
    for k in (
        "physical_state",
        "form",
        "eco_phrase_cues",
        "biodegradation",
        "persistence",
        "bcf",
        "combustion_phrase_cues",
        "ld50_oral_mg_kg",
        "ld50_dermal_mg_kg",
        "lc50_inhalation_ppm",
    ):
        if parsed.get(k) is not None and k not in out:
            out[k] = parsed[k]
    return out


def sds_fields_to_extra_sources(sds_fields: dict[str, Any] | None) -> dict[str, Any]:
    """
    Convert regex/LLM/structured SDS field dicts into ``extra_sources``.

    Accepts either the LLM ``SDSHazardSchema``-like dict or a loose regex extract
    with keys such as ``ghs_h_codes``, ``flash_point``, ``vapor_pressure``,
    ``aquatic_toxicity``, ``physical_state``, eco/combustion phrase cues, OELs.
    """
    if not sds_fields:
        return {}

    # Prefer the dedicated LLM bridge when the richer schema is present.
    if any(k.startswith("ld50_") or k.startswith("lc50_") for k in sds_fields):
        try:
            from utils.sds_llm_extractor import sds_hazard_to_extra_sources

            llm_out = sds_hazard_to_extra_sources(sds_fields)  # type: ignore[arg-type]
            # Continue to merge physical_state / eco phrases below even when LLM path hits.
            if llm_out and not any(
                k in sds_fields
                for k in ("physical_state", "eco_phrase_cues", "combustion_phrase_cues", "bcf")
            ):
                return llm_out
            # Fall through to merge additional OR-pathway fields onto llm_out.
            base_llm = llm_out or {}
        except Exception:
            base_llm = {}
    else:
        base_llm = {}

    toxicities: list[dict[str, Any]] = list(base_llm.get("toxicities") or [])
    hazard_metrics: dict[str, list] = dict(base_llm.get("hazard_metrics") or {})
    ghs: dict[str, Any] = dict(base_llm.get("ghs") or {})
    notes: list[str] = list(base_llm.get("_pipeline_notes") or [])

    h_codes = sds_fields.get("ghs_h_codes") or sds_fields.get("h_codes") or ghs.get("h_codes") or []
    if isinstance(h_codes, str):
        h_codes = [c.strip() for c in h_codes.replace(";", ",").split(",") if c.strip()]
    if h_codes:
        ghs["h_codes"] = list(dict.fromkeys(list(ghs.get("h_codes") or []) + list(h_codes)))

    fp = sds_fields.get("flash_point") or sds_fields.get("flash_point_c")
    if fp is not None and fp != "" and not hazard_metrics.get("flash_point"):
        hazard_metrics["flash_point"] = [f"{fp} °C" if isinstance(fp, (int, float)) else str(fp)]

    vp = sds_fields.get("vapor_pressure") or sds_fields.get("vapor_pressure_mmhg")
    if vp is not None and vp != "":
        hazard_metrics.setdefault("other_designations", [])
        vp_s = f"{vp} mmHg" if isinstance(vp, (int, float)) else str(vp)
        if vp_s not in hazard_metrics["other_designations"]:
            hazard_metrics["other_designations"].append(vp_s)

    ps = sds_fields.get("physical_state") or sds_fields.get("form") or sds_fields.get("appearance")
    if ps:
        hazard_metrics.setdefault("other_designations", [])
        tag = f"Physical state: {ps}"
        if tag not in hazard_metrics["other_designations"]:
            hazard_metrics["other_designations"].append(tag)

    aq = sds_fields.get("aquatic_toxicity") or sds_fields.get("lc50_aquatic_mg_l")
    if aq is not None and aq != "":
        existing_aq = " ".join(str(t.get("value") or "") for t in toxicities).lower()
        if "lc50" not in existing_aq and "ec50" not in existing_aq:
            if isinstance(aq, (int, float)):
                toxicities.append({
                    "value": f"LC50 {aq} mg/L",
                    "unit": "mg/L",
                    "species_route": ["aquatic"],
                    "source": "SDS",
                })
            else:
                toxicities.append({
                    "value": str(aq),
                    "unit": None,
                    "species_route": ["aquatic"],
                    "source": "SDS",
                })

    # Oral / dermal / inhalation tox numbers when not already present via LLM bridge.
    existing_tox_blob = " ".join(str(t.get("value") or "") for t in toxicities).lower()
    if sds_fields.get("ld50_oral_mg_kg") is not None and "ld50" not in existing_tox_blob:
        v = sds_fields["ld50_oral_mg_kg"]
        toxicities.append({
            "value": f"LD50 {v} mg/kg",
            "unit": "mg/kg",
            "species_route": ["oral"],
            "source": "SDS",
        })
    if sds_fields.get("ld50_dermal_mg_kg") is not None and "dermal" not in existing_tox_blob:
        v = sds_fields["ld50_dermal_mg_kg"]
        toxicities.append({
            "value": f"LD50 {v} mg/kg",
            "unit": "mg/kg",
            "species_route": ["dermal"],
            "source": "SDS",
        })
    if sds_fields.get("lc50_inhalation_ppm") is not None and "lc50" not in existing_tox_blob:
        v = sds_fields["lc50_inhalation_ppm"]
        toxicities.append({
            "value": f"LC50 {v} ppm",
            "unit": "ppm",
            "species_route": ["inhalation"],
            "source": "SDS",
        })

    for cue in sds_fields.get("eco_phrase_cues") or []:
        toxicities.append({
            "value": str(cue),
            "unit": None,
            "species_route": ["ecological"],
            "source": "SDS_section_12",
        })
    for cue in sds_fields.get("combustion_phrase_cues") or []:
        toxicities.append({
            "value": str(cue),
            "unit": None,
            "species_route": ["atmospheric"],
            "source": "SDS_section_5_10",
        })
    for phrase_key, label in (
        ("biodegradation", "biodegradation"),
        ("persistence", "persistence"),
        ("bioaccumulation", "bioaccumulation"),
    ):
        v = sds_fields.get(phrase_key)
        if v:
            toxicities.append({
                "value": f"SDS {label}: {v}",
                "unit": None,
                "species_route": ["fate"],
                "source": "SDS_section_12",
            })
    if sds_fields.get("bcf") is not None:
        toxicities.append({
            "value": f"BCF {sds_fields['bcf']}",
            "unit": "L/kg",
            "species_route": ["fate"],
            "source": "SDS_section_12",
        })

    for oel in sds_fields.get("exposure_limits") or []:
        toxicities.append({
            "value": str(oel),
            "unit": None,
            "species_route": ["occupational"],
            "source": "SDS_section_8",
        })

    if sds_fields.get("nfpa_health") is not None:
        hazard_metrics.setdefault("nfpa", []).append(f"Health {sds_fields['nfpa_health']}")
    if sds_fields.get("nfpa_fire") is not None:
        hazard_metrics.setdefault("nfpa", []).append(f"Fire {sds_fields['nfpa_fire']}")

    out: dict[str, Any] = dict(base_llm) if base_llm else {}
    if toxicities:
        out["toxicities"] = toxicities
    if hazard_metrics:
        out["hazard_metrics"] = hazard_metrics
    if ghs:
        out["ghs"] = ghs
    if ps:
        out["physical_state"] = str(ps)
    if out:
        if "SDS fields merged into P2OASys evidence" not in notes:
            notes.append("SDS fields merged into P2OASys evidence")
        out["_pipeline_notes"] = notes
    return out


def merge_pipeline_notes(*extras: dict[str, Any] | None) -> list[str]:
    """Collect ``_pipeline_notes`` lists from extra_sources dicts."""
    notes: list[str] = []
    for ex in extras:
        if not ex:
            continue
        for n in ex.get("_pipeline_notes") or []:
            if n and n not in notes:
                notes.append(str(n))
    return notes
