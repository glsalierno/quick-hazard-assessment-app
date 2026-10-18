"""
Low-dimensional LLM extraction from SDS text for P2OASys.
Outputs a fixed schema (flash_point_c, ghs_h_codes, ld50_oral_mg_kg, etc.) that merges
into hazard_data. Regex remains primary; this fills gaps when Ollama is available.
"""

from __future__ import annotations

import json
import re
from typing import Any, TypedDict

try:
    import requests
except ImportError:
    requests = None

# Schema for SDS → hazard_data (P2OASys). All fields optional.
SDS_HAZARD_SCHEMA_KEYS = (
    "flash_point_c",
    "vapor_pressure_mmhg",
    "ghs_h_codes",
    "ghs_p_codes",
    "signal_word",
    "ld50_oral_mg_kg",
    "lc50_inhalation_ppm",
    "lc50_aquatic_mg_l",
    "iarc",
    "epa_carcinogen",
    "nfpa_health",
    "nfpa_fire",
)


class SDSHazardSchema(TypedDict, total=False):
    flash_point_c: float
    vapor_pressure_mmhg: float
    ghs_h_codes: list[str]
    ghs_p_codes: list[str]
    signal_word: str
    ld50_oral_mg_kg: float
    lc50_inhalation_ppm: float
    lc50_aquatic_mg_l: float
    iarc: str
    epa_carcinogen: str
    nfpa_health: int
    nfpa_fire: int


_EXTRACT_PROMPT = """You are extracting hazard data from a Safety Data Sheet (SDS) for chemical scoring.
Extract ONLY the following fields from the text. Return a JSON object with these keys (use null for missing).
- flash_point_c: number (flash point in °C)
- vapor_pressure_mmhg: number (vapor pressure in mmHg)
- ghs_h_codes: list of strings (e.g. ["H225", "H302"])
- ghs_p_codes: list of strings (e.g. ["P210", "P301+P310"])
- signal_word: "Danger" or "Warning" or ""
- ld50_oral_mg_kg: number (oral LD50 in mg/kg)
- lc50_inhalation_ppm: number (inhalation LC50 in ppm)
- lc50_aquatic_mg_l: number (aquatic LC50 or EC50 in mg/L)
- iarc: string (e.g. "1", "2A", "2B", "3", "4")
- epa_carcinogen: string (e.g. "A", "B", "C", "D", "E")
- nfpa_health: number 0-4
- nfpa_fire: number 0-4

Return ONLY valid JSON, no markdown or explanation."""


def extract_hazard_from_sds_with_llm(
    text: str,
    host: str = "http://localhost:11434",
    model: str = "qwen2:0.5b",
    timeout: int = 60,
) -> SDSHazardSchema | None:
    """
    Send SDS text to Ollama and parse JSON into the fixed hazard schema.
    Returns None if Ollama is unreachable, model fails, or output is invalid.
    """
    if not text or not text.strip():
        return None
    if not requests:
        return None
    url = host.rstrip("/") + "/api/generate"
    payload = {
        "model": model,
        "prompt": _EXTRACT_PROMPT + "\n\nSDS text:\n" + (text[:12000] if len(text) > 12000 else text),
        "stream": False,
    }
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code != 200:
            return None
        data = r.json()
        response_text = data.get("response") or ""
    except Exception:
        return None
    # Parse JSON from response (may be wrapped in markdown code block)
    response_text = response_text.strip()
    m = re.search(r"\{[\s\S]*\}", response_text)
    if not m:
        return None
    try:
        parsed = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return _normalize_sds_hazard(parsed)


def _normalize_sds_hazard(raw: dict[str, Any]) -> SDSHazardSchema:
    """Ensure types match SDSHazardSchema; drop invalid keys."""
    out: dict[str, Any] = {}
    for k in SDS_HAZARD_SCHEMA_KEYS:
        v = raw.get(k)
        if v is None:
            continue
        if k in ("flash_point_c", "vapor_pressure_mmhg", "ld50_oral_mg_kg", "lc50_inhalation_ppm", "lc50_aquatic_mg_l"):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                pass
        elif k in ("nfpa_health", "nfpa_fire"):
            try:
                n = int(v)
                if 0 <= n <= 4:
                    out[k] = n
            except (TypeError, ValueError):
                pass
        elif k in ("ghs_h_codes", "ghs_p_codes"):
            if isinstance(v, list):
                out[k] = [str(x).strip() for x in v if str(x).strip()][:50]
            elif isinstance(v, str):
                out[k] = [x.strip() for x in v.replace(",", " ").split() if x.strip()][:50]
        elif k in ("signal_word", "iarc", "epa_carcinogen"):
            s = str(v).strip()
            if s and s.lower() != "null":
                out[k] = s
    return out


def sds_hazard_to_extra_sources(sds_hazard: SDSHazardSchema | None) -> dict[str, Any]:
    """
    Convert SDS LLM output to extra_sources for hazard_for_p2oasys.build_hazard_data().
    """
    if not sds_hazard:
        return {}
    toxicities: list[dict[str, Any]] = []
    if sds_hazard.get("ld50_oral_mg_kg") is not None:
        v = sds_hazard["ld50_oral_mg_kg"]
        toxicities.append({"value": f"LD50 {v} mg/kg", "unit": "mg/kg", "species_route": ["oral"]})
    if sds_hazard.get("lc50_inhalation_ppm") is not None:
        v = sds_hazard["lc50_inhalation_ppm"]
        toxicities.append({"value": f"LC50 {v} ppm", "unit": "ppm", "species_route": ["inhalation"]})
    if sds_hazard.get("lc50_aquatic_mg_l") is not None:
        v = sds_hazard["lc50_aquatic_mg_l"]
        toxicities.append({"value": f"LC50 {v} mg/L", "unit": "mg/L", "species_route": ["aquatic"]})
    if sds_hazard.get("iarc"):
        toxicities.append({
            "value": f"IARC Group {sds_hazard['iarc']}",
            "unit": None,
            "species_route": None,
        })
    if sds_hazard.get("epa_carcinogen"):
        toxicities.append({
            "value": f"EPA Carcinogen Group {sds_hazard['epa_carcinogen']}",
            "unit": None,
            "species_route": None,
        })
    hazard_metrics: dict[str, Any] = {}
    if sds_hazard.get("flash_point_c") is not None:
        hazard_metrics["flash_point"] = [f"{sds_hazard['flash_point_c']} °C"]
    if sds_hazard.get("vapor_pressure_mmhg") is not None:
        hazard_metrics["other_designations"] = [f"{sds_hazard['vapor_pressure_mmhg']} mmHg"]
    if sds_hazard.get("nfpa_health") is not None or sds_hazard.get("nfpa_fire") is not None:
        parts = []
        if sds_hazard.get("nfpa_health") is not None:
            parts.append(f"Health {sds_hazard['nfpa_health']}")
        if sds_hazard.get("nfpa_fire") is not None:
            parts.append(f"Fire {sds_hazard['nfpa_fire']}")
        hazard_metrics["nfpa"] = [" - ".join(parts)]
    ghs: dict[str, Any] = {}
    if sds_hazard.get("ghs_h_codes"):
        ghs["h_codes"] = sds_hazard["ghs_h_codes"]
    if sds_hazard.get("ghs_p_codes"):
        ghs["p_codes"] = sds_hazard["ghs_p_codes"]
    if sds_hazard.get("signal_word"):
        ghs["signal_word"] = sds_hazard["signal_word"]
    out: dict[str, Any] = {}
    if toxicities:
        out["toxicities"] = toxicities
    if hazard_metrics:
        out["hazard_metrics"] = hazard_metrics
    if ghs:
        out["ghs"] = ghs
    return out


def is_ollama_available(host: str = "http://localhost:11434", timeout: int = 2) -> bool:
    """Check if Ollama is reachable."""
    if not requests:
        return False
    try:
        r = requests.get(host.rstrip("/") + "/api/tags", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


# --- CAS / composition extraction (Section 3) ---

_CAS_EXTRACT_PROMPT = """You are a chemical safety expert. Extract all CAS numbers, chemical names, and concentrations from this Safety Data Sheet section. Return ONLY a JSON object with keys "cas_numbers", "chemical_names", "concentrations". Each list must have the same length; use empty string for missing values. Concentrations may include ranges like ">=30 - <60%".

Example:
{"cas_numbers": ["75-45-6", "420-46-2"], "chemical_names": ["Methane, chlorodifluoro-", "Ethane, 1,1,1-trifluoro-"], "concentrations": [">=30 - <60%", ">=30 - <60%"]}

Section text:
"""


def extract_cas_with_llm(
    section_text: str,
    host: str | None = None,
    model: str | None = None,
    timeout: int = 45,
) -> dict[str, Any]:
    """
    Use local LLM (Ollama) to extract CAS numbers, chemical names, and concentrations
    from SDS Section 3 (composition/ingredients) text.

    Returns dict with keys: cas_numbers, chemical_names, concentrations (lists of same length).
    Returns empty dict on failure (Ollama down, parse error, etc.).
    """
    if not section_text or not str(section_text).strip():
        return {}
    if not requests:
        return {}

    # Use config defaults when not provided
    if host is None or model is None:
        try:
            from config import OLLAMA_HOST, OLLAMA_MODEL
            host = host or OLLAMA_HOST
            model = model or OLLAMA_MODEL
        except ImportError:
            host = host or "http://localhost:11434"
            model = model or "qwen2:0.5b"

    url = host.rstrip("/") + "/api/generate"
    prompt = _CAS_EXTRACT_PROMPT + (section_text[:8000] if len(section_text) > 8000 else section_text)
    payload = {"model": model, "prompt": prompt, "stream": False, "temperature": 0.1}

    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code != 200:
            return {}
        data = r.json()
        response_text = (data.get("response") or "").strip()
    except Exception:
        return {}

    # Parse JSON from response (may be wrapped in markdown code block)
    m = re.search(r"\{[\s\S]*\}", response_text)
    if not m:
        return {}
    try:
        parsed = json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}

    cas_list = parsed.get("cas_numbers")
    names_list = parsed.get("chemical_names") or []
    conc_list = parsed.get("concentrations") or []
    if not isinstance(cas_list, list) or not cas_list:
        return {}

    # Validate and normalize CAS; align lengths
    from utils import cas_validator

    out_cas: list[str] = []
    out_names: list[str] = []
    out_conc: list[str] = []
    for i, raw_cas in enumerate(cas_list):
        cas_str = str(raw_cas).strip()
        if not cas_str:
            continue
        norm = cas_validator.normalize_cas_input(cas_str) or cas_str
        if not cas_validator.is_valid_cas_format(norm):
            continue
        try:
            validated, _ = cas_validator.validate_cas_relaxed(norm)
        except Exception:
            validated = norm
        if not validated:
            continue
        out_cas.append(validated)
        out_names.append(str(names_list[i]).strip() if i < len(names_list) else "")
        out_conc.append(str(conc_list[i]).strip() if i < len(conc_list) else "")

    return {"cas_numbers": out_cas, "chemical_names": out_names, "concentrations": out_conc}


# Alias per spec: extract_with_llm(section_text) -> dict
extract_with_llm = extract_cas_with_llm


def llm_cas_result_to_casextractions(result: dict[str, Any]) -> list[Any]:
    """
    Convert extract_cas_with_llm output to list of CASExtraction for merging into
    sds_parser_engine. Uses TYPE_CHECKING import to avoid circular dependency.
    """
    if not result or not result.get("cas_numbers"):
        return []
    from utils.sds_models import CASExtraction

    cas_list = result["cas_numbers"]
    names = result.get("chemical_names") or []
    concs = result.get("concentrations") or []
    out: list[CASExtraction] = []
    for i, cas in enumerate(cas_list):
        name = names[i].strip() if i < len(names) and names[i] else ""
        conc = concs[i].strip() if i < len(concs) and concs[i] else ""
        out.append(
            CASExtraction(
                cas=cas,
                chemical_name=name or None,
                concentration=conc or None,
                section=3,
                method="llm_ollama",
                confidence=0.9,
                validated=True,
            )
        )
    return out
