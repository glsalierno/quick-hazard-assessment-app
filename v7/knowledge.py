"""CAS-keyed knowledge merge. Curated providers win; live sources never overwrite them."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from utils.lookup_tables import normalize_cas_for_lookup
from v7.evidence import EvidenceValue, evidence, prefer


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS evidence (
    cas TEXT NOT NULL,
    field TEXT NOT NULL,
    provider TEXT NOT NULL,
    value_json TEXT,
    source TEXT,
    confidence TEXT,
    retrieval_date TEXT,
    citation TEXT,
    PRIMARY KEY (cas, field, provider)
);
"""


def _num(raw: Any) -> float | None:
    try:
        if raw is None or (isinstance(raw, float) and raw != raw):
            return None
        s = str(raw).strip()
        if not s or s.lower() in ("nan", "none", "-"):
            return None
        return float(s)
    except (TypeError, ValueError):
        return None


@dataclass
class P2OASysCompare:
    doss_score: float | None
    computed_score: float | None
    difference: float | None
    reason: str
    doss_citation: str | None = None
    computed_citation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "doss_p2oasys": self.doss_score,
            "computed_p2oasys": self.computed_score,
            "difference": self.difference,
            "reason_for_difference": self.reason,
            "doss_citation": self.doss_citation,
            "computed_citation": self.computed_citation,
        }


@dataclass
class KnowledgeRecord:
    cas: str
    fields: dict[str, EvidenceValue] = field(default_factory=dict)
    p2oasys: P2OASysCompare | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cas": self.cas,
            "fields": {k: v.to_dict() for k, v in self.fields.items()},
            "p2oasys": self.p2oasys.to_dict() if self.p2oasys else None,
        }


class KnowledgeStore:
    def __init__(self, sqlite_path: Path | str | None = None) -> None:
        self.path = Path(sqlite_path) if sqlite_path else None
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.path) as con:
                con.executescript(SCHEMA_SQL)

    def persist(self, record: KnowledgeRecord) -> None:
        if not self.path:
            return
        cas = normalize_cas_for_lookup(record.cas)
        rows = []
        for field_name, ev in record.fields.items():
            rows.append(
                (
                    cas,
                    field_name,
                    ev.provider,
                    json.dumps(ev.value, default=str),
                    ev.source,
                    ev.confidence,
                    ev.retrieval_date,
                    ev.citation,
                )
            )
        with sqlite3.connect(self.path) as con:
            con.executemany(
                """
                INSERT OR REPLACE INTO evidence
                (cas, field, provider, value_json, source, confidence, retrieval_date, citation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            con.commit()


def load_manual_overrides(csv_path: Path | str | None) -> dict[str, dict[str, EvidenceValue]]:
    path = Path(csv_path) if csv_path else None
    out: dict[str, dict[str, EvidenceValue]] = {}
    if not path or not path.is_file():
        return out
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            cas = normalize_cas_for_lookup(row.get("cas") or row.get("CAS"))
            field_name = (row.get("field") or "").strip()
            if not cas or not field_name:
                continue
            out.setdefault(cas, {})[field_name] = evidence(
                row.get("value"),
                source="manual_override",
                provider="manual",
                confidence="curated",
                citation=(row.get("citation") or str(path)),
            )
    return out


def merge_layers(*layers: dict[str, EvidenceValue]) -> dict[str, EvidenceValue]:
    """Apply layers in call order (later can only win if higher provider priority)."""
    merged: dict[str, EvidenceValue] = {}
    for layer in layers:
        for key, ev in (layer or {}).items():
            merged[key] = prefer(merged.get(key), ev)
    return merged


def compare_p2oasys(
    *,
    doss_score: float | None,
    computed_score: float | None,
    doss_citation: str | None = None,
    computed_citation: str | None = None,
) -> P2OASysCompare:
    if doss_score is None and computed_score is None:
        reason = "P2OASys assessment required"
        diff = None
    elif doss_score is None:
        reason = "Computed score only — CAS has no DoSS P2OASys value"
        diff = None
    elif computed_score is None:
        reason = "DoSS expert score only — computed P2OASys not requested"
        diff = None
    else:
        diff = round(float(computed_score) - float(doss_score), 2)
        if abs(diff) < 0.05:
            reason = "Computed score matches DoSS expert score"
        elif diff > 0:
            reason = f"Computed is {diff:.1f} higher (more hazardous) than DoSS expert"
        else:
            reason = f"Computed is {abs(diff):.1f} lower (less hazardous) than DoSS expert"
    return P2OASysCompare(
        doss_score=doss_score,
        computed_score=computed_score,
        difference=diff,
        reason=reason,
        doss_citation=doss_citation,
        computed_citation=computed_citation,
    )


def evidence_from_doss_row(row: dict[str, Any], *, xlsx: str | None) -> dict[str, EvidenceValue]:
    cite = xlsx or "DoSS"
    out: dict[str, EvidenceValue] = {}
    mapping = {
        "nfpa_health": "NFPA Health",
        "nfpa_fire": "NFPA Flame",
        "ghs_text": "GHS Hazards",
        "gloves": "Glove Type",
        "vapor_pressure": "Vapor Pressure (mmHg)",
        "flash_point_c": "Flash Point (C)",
        "melting_point_c": "Melting Point (C)",
        "boiling_point_c": "Boiling Point (C)",
        "density": "Density (g/L)",
        "viscosity": "Viscosity (cP)",
        "water_solubility": "Water Solubility (g/L)",
        "rer": "RER (HSP)",
        "hansen_d": "D",
        "hansen_p": "P",
        "hansen_h": "H",
    }
    for field_name, col in mapping.items():
        raw = row.get(col)
        if raw is None or str(raw).strip() in ("", "nan", "NaN"):
            continue
        val: Any = _num(raw)
        if val is None:
            val = str(raw).strip()
        out[field_name] = evidence(
            val, source="DoSS", provider="doss", confidence="curated", citation=cite
        )
    return out


def evidence_from_cameo(cas: str) -> dict[str, EvidenceValue]:
    """Substance-level NFPA 704 from local CAMEO Chemicals sqlite (not a scrape)."""
    try:
        from utils.cameo_lookup import lookup_cameo
    except ImportError:
        return {}
    hit = lookup_cameo(cas)
    if not hit:
        return {}
    cite = hit.get("datasheet_url") or "CAMEO Chemicals 3.1.0"
    src_note = hit.get("nfpa_source") or "CAMEO Chemicals"
    name = hit.get("name") or cas
    out: dict[str, EvidenceValue] = {}
    if hit.get("nfpa_health") is not None:
        out["nfpa_health"] = evidence(
            hit["nfpa_health"],
            source=f"CAMEO {name} {src_note}",
            provider="cameo",
            confidence="experimental",
            citation=cite,
        )
    if hit.get("nfpa_flame") is not None:
        out["nfpa_fire"] = evidence(
            hit["nfpa_flame"],
            source=f"CAMEO {name} {src_note}",
            provider="cameo",
            confidence="experimental",
            citation=cite,
        )
    return out


def evidence_from_pubchem(pubchem: dict[str, Any] | None) -> dict[str, EvidenceValue]:
    if not pubchem:
        return {}
    out: dict[str, EvidenceValue] = {}
    cite = "PubChem PUG View"
    ghs = pubchem.get("ghs") or {}
    if ghs.get("h_codes"):
        out["ghs_h_codes"] = evidence(
            ghs["h_codes"], source="PubChem", provider="pubchem", confidence="experimental", citation=cite
        )
    if ghs.get("signal_word"):
        out["signal_word"] = evidence(
            ghs["signal_word"], source="PubChem", provider="pubchem", confidence="experimental", citation=cite
        )
    fp = pubchem.get("flash_point")
    if fp:
        out["flash_point"] = evidence(
            fp, source="PubChem", provider="pubchem", confidence="experimental", citation=cite
        )
    vp = pubchem.get("vapor_pressure")
    if vp:
        out["vapor_pressure"] = evidence(
            vp, source="PubChem", provider="pubchem", confidence="experimental", citation=cite
        )
    nfpa = pubchem.get("nfpa")
    if nfpa:
        out["nfpa_text"] = evidence(
            nfpa, source="PubChem", provider="pubchem", confidence="experimental", citation=cite
        )
    return out


def evidence_from_structured_sds(
    structured: dict[str, Any] | None,
    *,
    provider: str,
    citation: str,
) -> dict[str, EvidenceValue]:
    if not structured:
        return {}
    out: dict[str, EvidenceValue] = {}
    sec2 = structured.get("section_2") or {}
    sec8 = structured.get("section_8") or {}
    sec9 = structured.get("section_9") or {}
    if sec2.get("signal_word"):
        out["signal_word"] = evidence(
            sec2["signal_word"], source="SDS section 2", provider=provider, confidence="document", citation=citation
        )
    if sec2.get("h_statements"):
        out["ghs_h_codes"] = evidence(
            sec2["h_statements"], source="SDS section 2", provider=provider, confidence="document", citation=citation
        )
    if sec8.get("gloves"):
        out["gloves"] = evidence(
            sec8["gloves"], source="SDS section 8", provider=provider, confidence="document", citation=citation
        )
    if sec8.get("ppe"):
        out["ppe"] = evidence(
            sec8["ppe"], source="SDS section 8", provider=provider, confidence="document", citation=citation
        )
    tlv = sec8.get("tlv")
    if tlv:
        out["tlv"] = evidence(tlv, source="SDS section 8", provider=provider, confidence="document", citation=citation)
    idlh = sec8.get("idlh")
    if idlh:
        out["idlh"] = evidence(idlh, source="SDS section 8", provider=provider, confidence="document", citation=citation)
    if structured.get("nfpa_health") is not None:
        out["nfpa_health"] = evidence(
            structured["nfpa_health"], source="SDS", provider=provider, confidence="document", citation=citation
        )
    if structured.get("nfpa_fire") is not None:
        out["nfpa_fire"] = evidence(
            structured["nfpa_fire"], source="SDS", provider=provider, confidence="document", citation=citation
        )
    if sec9.get("flash_point"):
        out["flash_point"] = evidence(
            sec9["flash_point"], source="SDS section 9", provider=provider, confidence="document", citation=citation
        )
    ps = structured.get("physical_state") or sec9.get("physical_state") or sec9.get("form")
    if ps:
        out["physical_state"] = evidence(
            ps, source="SDS section 9", provider=provider, confidence="document", citation=citation
        )
    sec11 = structured.get("section_11") or {}
    if sec11.get("ld50_oral") is not None:
        out["ld50_oral"] = evidence(
            f"{sec11['ld50_oral']} {sec11.get('ld50_oral_unit') or 'mg/kg'}",
            source="SDS section 11",
            provider=provider,
            confidence="document",
            citation=citation,
        )
    sec12 = structured.get("section_12") or {}
    if sec12.get("aquatic_toxicity"):
        out["aquatic_toxicity"] = evidence(
            sec12["aquatic_toxicity"],
            source="SDS section 12",
            provider=provider,
            confidence="document",
            citation=citation,
        )
    if sec12.get("bcf") is not None:
        out["bcf"] = evidence(
            sec12["bcf"], source="SDS section 12", provider=provider, confidence="document", citation=citation
        )
    return out
