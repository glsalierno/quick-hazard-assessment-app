"""
Data models for Safety Data Sheet structures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import pandas as pd


@dataclass
class CASExtraction:
    cas: str
    chemical_name: Optional[str] = None
    concentration: Optional[str] = None
    section: Optional[int] = None
    sections: list[int] = field(default_factory=list)  # sections where CAS appears (for multi-section evidence)
    method: str = "regex"
    confidence: float = 0.8
    context: Optional[str] = None
    validated: bool = False
    warnings: list[str] = field(default_factory=list)


@dataclass
class GHSExtraction:
    h_codes: list[str] = field(default_factory=list)
    p_codes: list[str] = field(default_factory=list)
    signal_word: Optional[str] = None
    pictograms: list[str] = field(default_factory=list)
    hazard_class: Optional[str] = None
    category: Optional[str] = None
    confidence: float = 0.8
    source_text: Optional[str] = None


@dataclass
class PhysicalProperty:
    property_name: str
    value: float
    unit: str
    method: Optional[str] = None
    conditions: Optional[str] = None
    confidence: float = 0.8
    raw_text: Optional[str] = None


@dataclass
class EcotoxValue:
    species: str
    endpoint: str
    value: float
    unit: str
    duration_h: Optional[float] = None
    confidence: float = 0.8
    raw_text: Optional[str] = None


@dataclass
class SDSParseResult:
    cas_numbers: list[CASExtraction] = field(default_factory=list)
    product_name: Optional[str] = None
    manufacturer: Optional[str] = None
    revision_date: Optional[datetime] = None
    ghs: GHSExtraction = field(default_factory=GHSExtraction)
    physical_properties: list[PhysicalProperty] = field(default_factory=list)
    ecotoxicity: list[EcotoxValue] = field(default_factory=list)
    parse_date: datetime = field(default_factory=datetime.now)
    environment: str = "unknown"
    methods_used: list[str] = field(default_factory=list)
    parse_time_ms: int = 0
    raw_sections: dict[int, str] = field(default_factory=dict)
    legacy: dict[str, Any] = field(default_factory=dict)
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)

    def to_dataframes(self) -> dict[str, pd.DataFrame]:
        out: dict[str, pd.DataFrame] = {}
        if self.cas_numbers:
            out["cas_numbers"] = pd.DataFrame(
                [
                    {
                        "CAS": c.cas,
                        "Chemical Name": c.chemical_name or "",
                        "Concentration": c.concentration or "",
                        "Section": c.section or "",
                        "Confidence": c.confidence,
                        "Method": c.method,
                        "Validated": c.validated,
                    }
                    for c in self.cas_numbers
                ]
            )
        if self.ghs.h_codes or self.ghs.p_codes:
            rows: list[dict[str, Any]] = []
            for h in self.ghs.h_codes:
                rows.append({"Type": "Hazard", "Code": h, "Signal Word": self.ghs.signal_word or ""})
            for p in self.ghs.p_codes:
                rows.append({"Type": "Precautionary", "Code": p, "Signal Word": ""})
            out["ghs"] = pd.DataFrame(rows)
        if self.physical_properties:
            out["physical_properties"] = pd.DataFrame(
                [
                    {
                        "Property": p.property_name,
                        "Value": p.value,
                        "Unit": p.unit,
                        "Method": p.method or "",
                        "Confidence": p.confidence,
                    }
                    for p in self.physical_properties
                ]
            )
        if self.ecotoxicity:
            out["ecotoxicity"] = pd.DataFrame(
                [
                    {
                        "Species": e.species,
                        "Endpoint": e.endpoint,
                        "Value": e.value,
                        "Unit": e.unit,
                        "Duration": (f"{e.duration_h}h" if e.duration_h is not None else ""),
                        "Confidence": e.confidence,
                    }
                    for e in self.ecotoxicity
                ]
            )
        return out
