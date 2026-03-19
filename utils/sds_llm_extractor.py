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
