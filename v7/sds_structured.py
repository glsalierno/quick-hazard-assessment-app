"""Expanded SDS section extraction → structured JSON (provider-agnostic)."""

from __future__ import annotations

import re
from typing import Any

_H_RE = re.compile(r"\bH\d{3}[A-Z]?\b", re.I)
_P_RE = re.compile(r"\bP\d{3}(?:\+P\d{3})*\b", re.I)
_UN_RE = re.compile(r"\bUN\s*(\d{3,5})\b", re.I)
_NFPA_RE = re.compile(
    r"NFPA[^\d]{0,20}(?:Health|H)?\s*[:=]?\s*([0-4]).{0,40}(?:Flamm|Fire|F)?\s*[:=]?\s*([0-4])?",
    re.I,
)
_LIMIT_LINE = re.compile(
    r"(?P<label>OSHA\s*PEL|NIOSH\s*REL|TLV|STEL|Ceiling|IDLH|TWA|WEEL)[^\n]{0,80}?"
    r"(?P<val>\d+(?:\.\d+)?)\s*(?P<unit>ppm|mg/m3|mg/m³)",
    re.I,
)
_LD50_RE = re.compile(
    r"\bLD\s*50\b[^\n]{0,80}?(\d[\d,]*(?:\.\d+)?)\s*(mg/kg|g/kg)",
    re.I,
)
_LC50_INH_RE = re.compile(
    r"\bLC\s*50\b[^\n]{0,100}?(\d[\d,]*(?:\.\d+)?)\s*(ppm|mg/m3|mg/m³|mg/L)",
    re.I,
)
_AQUATIC_RE = re.compile(
    r"\b(?:LC\s*50|EC\s*50)\b[^\n]{0,100}?(\d[\d,]*(?:\.\d+)?)\s*(mg/L|mg/l|µg/L|ug/L)",
    re.I,
)
_BCF_RE = re.compile(
    r"\b(?:BCF|bioconcentration\s*factor|bioaccumulation\s*factor)\b[^\n]{0,60}?"
    r"(\d[\d,]*(?:\.\d+)?)",
    re.I,
)
_LOGKOW_RE = re.compile(
    r"\b(?:log\s*K?\s*ow|log\s*P(?:ow)?|octanol[^\n]{0,20}partition)\b[^\n]{0,40}?"
    r"(-?\d+(?:\.\d+)?)",
    re.I,
)


def _clip(text: str | None, n: int = 2000) -> str | None:
    if not text:
        return None
    s = " ".join(text.split())
    return s[:n] if s else None


def _first_num(pattern: str, text: str) -> str | None:
    m = re.search(pattern, text, re.I)
    return m.group(1).strip() if m else None


def _section_after_label(text: str, label: str) -> str | None:
    if not text:
        return None
    m = re.search(rf"{re.escape(label)}[^\n]{{0,40}}\n?([^\n]{{8,400}})", text, re.I)
    return m.group(1).strip() if m else None


def _extract_pictograms(text: str) -> list[str]:
    names = []
    for label in (
        "Flame",
        "Exclamation",
        "Corrosion",
        "Skull",
        "Health hazard",
        "Environment",
        "Exploding",
        "Gas cylinder",
        "Oxidizer",
    ):
        if re.search(label, text, re.I):
            names.append(label)
    return names


def _kv_block(text: str, keys: tuple[str, ...]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for key in keys:
        pat = rf"{key}\s*[:\-]\s*([^\n]{{3,200}})"
        m = re.search(pat, text, re.I)
        out[key] = m.group(1).strip() if m else _clip(_section_after_label(text, key), 400)
    return out


def _tox_numbers(sec11: str) -> dict[str, Any]:
    """Pull oral/dermal LD50 and inhalation LC50 numbers from §11."""
    oral = dermal = inhalation = None
    oral_unit = dermal_unit = inh_unit = None
    for m in _LD50_RE.finditer(sec11 or ""):
        val = m.group(1).replace(",", "")
        unit = m.group(2)
        ctx = sec11[max(0, m.start() - 60) : m.end() + 40].lower()
        try:
            num = float(val)
        except ValueError:
            continue
        if "dermal" in ctx or "skin" in ctx:
            if dermal is None:
                dermal, dermal_unit = num, unit
        elif "oral" in ctx or "rat" in ctx or "mouse" in ctx or oral is None:
            if oral is None or "oral" in ctx:
                oral, oral_unit = num, unit
    for m in _LC50_INH_RE.finditer(sec11 or ""):
        val = m.group(1).replace(",", "")
        unit = m.group(2)
        ctx = sec11[max(0, m.start() - 60) : m.end() + 40].lower()
        if "fish" in ctx or "daphnia" in ctx or "algae" in ctx or "aquatic" in ctx:
            continue
        try:
            inhalation = float(val)
            inh_unit = unit
            break
        except ValueError:
            continue
    return {
        "ld50_oral": oral,
        "ld50_oral_unit": oral_unit,
        "ld50_dermal": dermal,
        "ld50_dermal_unit": dermal_unit,
        "lc50_inhalation": inhalation,
        "lc50_inhalation_unit": inh_unit,
    }


def _eco_fields(sec12: str) -> dict[str, Any]:
    """Aquatic / persistence / biodegradation / BCF from §12."""
    aquatic_hits: list[dict[str, Any]] = []
    for m in _AQUATIC_RE.finditer(sec12 or ""):
        try:
            aquatic_hits.append(
                {
                    "value": float(m.group(1).replace(",", "")),
                    "unit": m.group(2),
                    "raw": m.group(0)[:120],
                }
            )
        except ValueError:
            continue
    bcf = None
    bm = _BCF_RE.search(sec12 or "")
    if bm:
        try:
            bcf = float(bm.group(1).replace(",", ""))
        except ValueError:
            bcf = None
    logkow = None
    km = _LOGKOW_RE.search(sec12 or "")
    if km:
        try:
            logkow = float(km.group(1))
        except ValueError:
            logkow = None

    biodeg = _clip(
        _section_after_label(sec12, "Biodegradation")
        or _section_after_label(sec12, "Persistence and degradability")
        or _section_after_label(sec12, "Ready biodegradability"),
        500,
    )
    persistence = _clip(
        _section_after_label(sec12, "Persistence")
        or _section_after_label(sec12, "Persistence and degradability"),
        500,
    )
    bioaccum = _clip(
        _section_after_label(sec12, "Bioaccumul")
        or _section_after_label(sec12, "Bioaccumulation")
        or _section_after_label(sec12, "Bioconcentration"),
        500,
    )
    aquatic_phrase = _clip(
        _section_after_label(sec12, "Toxicity to fish")
        or _section_after_label(sec12, "Aquatic toxicity")
        or _section_after_label(sec12, "Ecotoxicity"),
        500,
    )
    # Phrase cues for KEY PHRASE matrix rows
    phrase_cues: list[str] = []
    blob = (sec12 or "").lower()
    if re.search(r"not\s+consider(?:ed)?\s+harmful\s+to\s+aquatic|not\s+harmful\s+to\s+aquatic", blob):
        phrase_cues.append("Not considered harmful to aquatic life")
    if re.search(r"readily\s+biodegrad", blob):
        phrase_cues.append("Readily degradable")
    elif re.search(r"\bbiodegrad", blob):
        phrase_cues.append("Biodegradable")
    if re.search(r"will\s+not\s+bioaccumul|not\s+(?:likely|expected)\s+to\s+bioaccumul", blob):
        phrase_cues.append("Will not bioaccumulate")
    if re.search(r"not\s+persistent|not\s+expected\s+to\s+be\s+persistent", blob):
        phrase_cues.append("Not persistent")

    return {
        "aquatic_toxicity": aquatic_hits,
        "aquatic_toxicity_text": aquatic_phrase,
        "persistence": persistence,
        "biodegradation": biodeg,
        "bioaccumulation": bioaccum,
        "bcf": bcf,
        "log_kow": logkow,
        "phrase_cues": phrase_cues,
        "raw_clip": _clip(sec12, 1200),
    }


def parse_structured_sds(sections: dict[int, str], *, full_text: str = "") -> dict[str, Any]:
    """Extract GHS / PPE / physchem / tox / eco / transport / regulatory blocks."""
    sec2 = sections.get(2) or ""
    sec5 = sections.get(5) or ""
    sec7 = sections.get(7) or ""
    sec8 = sections.get(8) or ""
    sec9 = sections.get(9) or ""
    sec10 = sections.get(10) or ""
    sec11 = sections.get(11) or ""
    sec12 = sections.get(12) or ""
    sec14 = sections.get(14) or ""
    sec15 = sections.get(15) or ""
    blob = full_text or "\n".join(sections.get(i, "") for i in range(1, 17))

    signal = None
    sm = re.search(r"\b(Danger|Warning)\b", sec2, re.I)
    if sm:
        signal = sm.group(1).capitalize()

    nfpa_h = nfpa_f = None
    nm = _NFPA_RE.search(blob)
    if nm:
        nfpa_h = int(nm.group(1))
        if nm.group(2) is not None and nm.group(2) != "":
            nfpa_f = int(nm.group(2))

    limits: list[dict[str, str]] = []
    for m in _LIMIT_LINE.finditer(sec8 or blob):
        limits.append(
            {
                "label": m.group("label").upper().replace("  ", " "),
                "value": m.group("val"),
                "unit": m.group("unit"),
            }
        )

    phys = _kv_block(
        sec9,
        (
            "Physical state",
            "Form",
            "Appearance",
            "Flash point",
            "Boiling point",
            "Melting point",
            "Vapor pressure",
            "Density",
            "Viscosity",
            "Water solubility",
            "Relative density",
            "Autoignition",
            "Explosive limits",
        ),
    )
    physical_state = (
        phys.pop("Physical state", None)
        or phys.pop("Form", None)
        or phys.pop("Appearance", None)
    )
    # Fallback: first line mentioning state keywords
    if not physical_state:
        m_state = re.search(
            r"(?:Physical\s*state|Form|Appearance)\s*[:\-]\s*([^\n]{2,80})",
            sec9,
            re.I,
        )
        if m_state:
            physical_state = m_state.group(1).strip()
    phys["physical_state"] = physical_state
    phys["form"] = physical_state
    phys["flash_point"] = phys.pop("Flash point", None) or _first_num(
        r"Flash\s*Point[:\s]*([<>]?\s*\d+(?:\.\d+)?\s*[°]?\s*[CF])", sec9
    )
    phys["boiling_point"] = phys.pop("Boiling point", None)
    phys["melting_point"] = phys.pop("Melting point", None)
    phys["vapor_pressure"] = phys.pop("Vapor pressure", None)
    phys["density"] = phys.pop("Density", None)
    phys["viscosity"] = phys.pop("Viscosity", None)
    phys["water_solubility"] = phys.pop("Water solubility", None)
    phys["relative_density"] = phys.pop("Relative density", None)
    phys["autoignition"] = phys.pop("Autoignition", None)
    phys["explosive_limits"] = phys.pop("Explosive limits", None)

    tox_nums = _tox_numbers(sec11)
    eco = _eco_fields(sec12)

    combustion_cues: list[str] = []
    fire_blob = f"{sec5}\n{sec10}".lower()
    if re.search(r"form\s+(?:sox|nox|so2|no2)|may\s+form\s+(?:sulfur|nitrogen)\s+oxide", fire_blob):
        combustion_cues.append("Product may form SOx or NOx upon combustion")
    elif re.search(r"\b(?:sox|nox|so2|no2)\b", fire_blob):
        combustion_cues.append("Product may form SOx or NOx upon combustion")

    return {
        "section_2": {
            "signal_word": signal,
            "ghs_classes": _clip(sec2, 800),
            "h_statements": sorted({c.upper() for c in _H_RE.findall(sec2)}),
            "p_statements": sorted({c.upper() for c in _P_RE.findall(sec2)}),
            "pictograms": _extract_pictograms(sec2),
        },
        "section_5": {
            "firefighting": _clip(sec5, 800),
            "hazardous_combustion": _clip(
                _section_after_label(sec5, "Hazardous combustion")
                or _section_after_label(sec5, "Combustion product"),
                500,
            ),
            "combustion_phrase_cues": combustion_cues,
        },
        "section_7": {
            "storage": _clip(_section_after_label(sec7, "Storage"), 800),
            "handling": _clip(_section_after_label(sec7, "Handling"), 800),
        },
        "section_8": {
            "gloves": _clip(
                _section_after_label(sec8, "Glove") or _section_after_label(sec8, "Hand protection"),
                500,
            ),
            "ppe": _clip(
                _section_after_label(sec8, "Personal protective") or _section_after_label(sec8, "PPE"),
                800,
            ),
            "respiratory_protection": _clip(_section_after_label(sec8, "Respiratory"), 500),
            "eye_protection": _clip(_section_after_label(sec8, "Eye"), 400),
            "skin_protection": _clip(
                _section_after_label(sec8, "Skin") or _section_after_label(sec8, "Body"), 400
            ),
            "engineering_controls": _clip(_section_after_label(sec8, "Engineering"), 500),
            "exposure_limits": limits,
            "osha_pel": next((x for x in limits if "OSHA" in x["label"] and "PEL" in x["label"]), None),
            "niosh_rel": next((x for x in limits if "NIOSH" in x["label"]), None),
            "tlv": next((x for x in limits if x["label"] in ("TLV", "TWA") or "TLV" in x["label"]), None),
            "stel": next((x for x in limits if "STEL" in x["label"]), None),
            "ceiling": next((x for x in limits if "CEILING" in x["label"]), None),
            "idlh": next((x for x in limits if "IDLH" in x["label"]), None),
        },
        "section_9": phys,
        "section_10": {
            "incompatible_materials": _clip(_section_after_label(sec10, "Incompatible"), 600),
            "hazardous_decomposition": _clip(_section_after_label(sec10, "Hazardous decomposition"), 600),
            "reactivity": _clip(_section_after_label(sec10, "Reactivity"), 400),
            "combustion_phrase_cues": combustion_cues,
        },
        "section_11": {
            "acute_toxicity": _clip(_section_after_label(sec11, "Acute"), 800),
            "sensitization": _clip(_section_after_label(sec11, "Sensitiz"), 400),
            "carcinogenicity": _clip(_section_after_label(sec11, "Carcino"), 400),
            "reproductive_toxicity": _clip(_section_after_label(sec11, "Reproduct"), 400),
            "stot": _clip(
                _section_after_label(sec11, "STOT") or _section_after_label(sec11, "specific target"),
                400,
            ),
            "aspiration_hazard": _clip(_section_after_label(sec11, "Aspiration"), 300),
            **tox_nums,
        },
        "section_12": eco,
        "section_14": {
            "un_number": (m.group(1) if (m := _UN_RE.search(sec14)) else None),
            "dot_class": _clip(
                _section_after_label(sec14, "Class") or _section_after_label(sec14, "DOT"), 200
            ),
            "packing_group": _first_num(r"Packing\s*Group\s*[:\-]?\s*(I{1,3}|IV|1|2|3)", sec14),
        },
        "section_15": {
            "regulatory_information": _clip(sec15, 1500),
        },
        "nfpa_health": nfpa_h,
        "nfpa_fire": nfpa_f,
        "physical_state": physical_state,
    }
