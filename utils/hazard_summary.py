"""
Build a short narrative hazard summary from extracted PubChem/GHS fields.

The summary is a lookup synthesis, not an independent toxicological review.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

from utils import ghs_formatter

CMR_PREFIXES = ("H340", "H341", "H350", "H351", "H360", "H361", "H362")
SERIOUS_HEALTH = {
    "H300",
    "H301",
    "H310",
    "H311",
    "H314",
    "H318",
    "H330",
    "H331",
    "H334",
    "H370",
    "H372",
}
HIGHLY_FLAMMABLE = {"H220", "H222", "H224", "H225"}
FLAMMABLE = HIGHLY_FLAMMABLE | {"H221", "H223", "H226", "H228"}


def _clean_codes(codes: Iterable[str] | None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in codes or []:
        code = (raw or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _phrase(code: str) -> str:
    phrase = (ghs_formatter.get_h_phrase(code) or "").strip()
    if not phrase or "(phrase not found)" in phrase.lower():
        return code
    if phrase.startswith(f"{code}:"):
        return phrase
    return f"{code}: {phrase.rstrip('.')}"


def _is_cmr(code: str) -> bool:
    return code.startswith(CMR_PREFIXES)


def _short(text: str, limit: int = 80) -> str:
    compact = " ".join((text or "").split())
    return compact if len(compact) <= limit else compact[: limit - 1] + "…"


def _first_text(values: Any) -> str:
    if isinstance(values, list):
        for item in values:
            text = str(item).strip()
            if text:
                return text
        return ""
    return str(values).strip() if values else ""


def _name_for_summary(preferred_name: Optional[str], iupac_name: Optional[str], query: str) -> str:
    return (preferred_name or iupac_name or query or "This compound").strip()


def _concern_level(h_codes: list[str], signal_word: str) -> str:
    if any(_is_cmr(code) or code in SERIOUS_HEALTH for code in h_codes):
        return "high"
    if any(code in HIGHLY_FLAMMABLE for code in h_codes) or signal_word.lower() == "danger":
        return "high"
    if h_codes or signal_word:
        return "moderate"
    return "unknown"


def _headline(name: str, h_codes: list[str], signal_word: str) -> str:
    bits: list[str] = []
    if signal_word and signal_word.lower() not in {"none", "n/a"}:
        bits.append(signal_word)

    if any(code in HIGHLY_FLAMMABLE for code in h_codes):
        bits.append("highly flammable liquid")
    elif any(code in FLAMMABLE for code in h_codes):
        bits.append("flammable liquid")

    if any(code.startswith(("H360", "H361")) for code in h_codes):
        bits.append("reproductive toxicity")
    elif any(code.startswith(("H350", "H351")) for code in h_codes):
        bits.append("carcinogenicity concern")
    elif any(code.startswith(("H340", "H341")) for code in h_codes):
        bits.append("germ-cell mutagenicity concern")
    elif "H318" in h_codes:
        bits.append("serious eye damage")
    elif "H319" in h_codes:
        bits.append("eye irritation")

    if len(bits) >= 2:
        return f"{bits[0]} — {'; '.join(bits[1:])}"
    if bits:
        return f"{bits[0]} — {name}"
    return f"{name} — limited GHS data in PubChem"


def _physical_sentence(h_codes: list[str], flash_point: str, vapor_pressure: str) -> str:
    physical = [code for code in h_codes if code.startswith("H2")]
    parts: list[str] = []
    if physical:
        parts.append("Physical hazards: " + "; ".join(_phrase(code) for code in physical) + ".")
    extras: list[str] = []
    if flash_point:
        extras.append(f"reported flash point {flash_point}")
    if vapor_pressure:
        extras.append(f"vapor pressure {vapor_pressure}")
    if extras:
        parts.append("Key physical data include " + " and ".join(extras) + ".")
    return " ".join(parts)


def _health_sentence(h_codes: list[str], iarc: Optional[str], prop65: Optional[str], exposure_bands: dict[str, Any] | None) -> str:
    health = [code for code in h_codes if code.startswith("H3")]
    parts: list[str] = []
    if health:
        cmr = [code for code in health if _is_cmr(code)]
        other = [code for code in health if not _is_cmr(code)]
        ordered = cmr + other
        parts.append("Health hazards: " + "; ".join(_phrase(code) for code in ordered) + ".")
    if iarc:
        parts.append(f"IARC: {iarc}.")
    if prop65:
        parts.append(f"Proposition 65: {prop65}.")

    band_bits: list[str] = []
    for route, label in (("oral", "oral"), ("dermal", "dermal"), ("inhalation", "inhalation")):
        band = (exposure_bands or {}).get(route) or {}
        if not band:
            continue
        value = band.get("value") or band.get("ld50_mg_kg") or band.get("lc50_mg_m3")
        metric = band.get("metric")
        unit = "mg/m3" if metric == "lc50_mg_m3" or band.get("lc50_mg_m3") else "mg/kg"
        band_no = band.get("band")
        if value is not None and band_no is not None:
            band_bits.append(f"{label} {value} {unit} (GHS-style band {band_no})")
    if band_bits:
        parts.append("Acute toxicity values used for screening bands: " + "; ".join(band_bits) + ".")
    return " ".join(parts)


def _environment_sentence(h_codes: list[str], ecotoxicity: dict[str, Any] | None) -> str:
    aquatic_codes = [code for code in h_codes if code.startswith("H4")]
    eco = ecotoxicity or {}
    aquatic_from_eco = [c for c in (eco.get("h_codes_aquatic") or eco.get("hCodesAquatic") or []) if c]
    codes = _clean_codes(aquatic_codes + aquatic_from_eco)
    if codes:
        return "Aquatic/environmental hazards: " + "; ".join(_phrase(code) for code in codes) + "."
    lc50 = eco.get("aquatic_lc50_mg_l") or eco.get("aquaticLc50MgL")
    if lc50 is not None:
        return f"No GHS aquatic H-codes were extracted; an aquatic LC50 of {lc50} mg/L was parsed from PubChem text."
    return "No GHS aquatic hazard statements were extracted from the current PubChem record."


def build_hazard_summary(
    *,
    query: str,
    preferred_name: Optional[str] = None,
    iupac_name: Optional[str] = None,
    formula: Optional[str] = None,
    ghs: Optional[dict[str, Any]] = None,
    flash_point: Any = None,
    vapor_pressure: Any = None,
    nfpa: Optional[str] = None,
    iarc: Optional[str] = None,
    prop65: Optional[str] = None,
    exposure_bands: Optional[dict[str, Any]] = None,
    ecotoxicity: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Return headline, paragraphs, highlights, and overall concern level."""
    ghs = ghs or {}
    h_codes = _clean_codes(ghs.get("h_codes") or ghs.get("hCodes"))
    signal_word = str(ghs.get("signal_word") or ghs.get("signalWord") or "").strip()
    name = _name_for_summary(preferred_name, iupac_name, query)
    identity_bits = [name]
    if formula:
        identity_bits.append(formula)
    if query and query.strip().lower() not in {name.lower(), (formula or "").lower()}:
        identity_bits.append(f"query {query}")

    flash = _first_text(flash_point)
    vapor = _first_text(vapor_pressure)
    concern = _concern_level(h_codes, signal_word)
    headline = _headline(name, h_codes, signal_word)

    intro = (
        f"{' / '.join(identity_bits)} has a PubChem GHS signal word of {signal_word}."
        if signal_word
        else f"{' / '.join(identity_bits)} has limited GHS classification text in the current PubChem record."
    )

    paragraphs = [p for p in (
        intro,
        _physical_sentence(h_codes, flash, vapor),
        _health_sentence(h_codes, iarc, prop65, exposure_bands),
        _environment_sentence(h_codes, ecotoxicity),
    ) if p]

    highlights: list[dict[str, str]] = []
    if signal_word:
        highlights.append({
            "label": "Signal word",
            "value": signal_word,
            "tone": "danger" if signal_word.lower() == "danger" else "warning",
        })
    highlights.append({
        "label": "Concern",
        "value": concern.capitalize(),
        "tone": "danger" if concern == "high" else "warning" if concern == "moderate" else "info",
    })
    if flash:
        highlights.append({"label": "Flash point", "value": _short(flash), "tone": "warning"})
    if nfpa:
        highlights.append({"label": "NFPA", "value": _short(nfpa), "tone": "info"})

    priority_codes = [code for code in h_codes if _is_cmr(code) or code in SERIOUS_HEALTH or code in HIGHLY_FLAMMABLE]
    remaining = [code for code in h_codes if code not in priority_codes]
    for code in (priority_codes + remaining)[:4]:
        highlights.append({
            "label": code,
            "value": _phrase(code).split(": ", 1)[-1],
            "tone": "danger" if (_is_cmr(code) or code in SERIOUS_HEALTH or code in HIGHLY_FLAMMABLE) else "warning",
        })

    return {
        "headline": headline,
        "paragraphs": paragraphs,
        "highlights": highlights,
        "concern_level": concern,
    }
