"""
Unified SDS parsing engine with adaptive method selection.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from utils import cas_validator, sds_pdf_utils, sds_regex_extractor
from utils.sds_environment import EnvironmentDetector
from utils.sds_models import CASExtraction, EcotoxValue, GHSExtraction, PhysicalProperty, SDSParseResult


class SDSParserEngine:
    def __init__(self) -> None:
        self.env = EnvironmentDetector.detect()
        self.methods_used: list[str] = []
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        self.patterns = {
            "section_header": re.compile(r"(?:^|\n)\s*(?:Section\s*)?([1-9]|1[0-6])(?:[.:]|\s)", re.IGNORECASE),
            "cas": re.compile(r"\b(\d{1,7})-(\d{2})-(\d)\b"),
            "h_code": re.compile(r"\bH\d{3}[A-Z]?\b", re.IGNORECASE),
            "p_code": re.compile(r"\bP\d{3}[A-Z]?\b", re.IGNORECASE),
            "signal": re.compile(r"\b(Danger|Warning)\b", re.IGNORECASE),
            "flash_point": re.compile(r"Flash\s*Point[:\s]*([<>]?\s*\d+(?:\.\d+)?)\s*[°]?\s*([CF])", re.IGNORECASE),
            "boiling_point": re.compile(r"Boiling\s*Point[:\s]*([<>]?\s*\d+(?:\.\d+)?)\s*[°]?\s*([CF])", re.IGNORECASE),
            "ecotox": re.compile(r"\b(EC50|LC50)\b[^0-9]{0,20}(\d+(?:\.\d+)?)\s*(mg/L|µg/L|ug/L|ppm)\b", re.IGNORECASE),
            "duration_h": re.compile(r"\b(\d+(?:\.\d+)?)\s*(h|hr|hrs|hour|hours)\b", re.IGNORECASE),
            "species": re.compile(r"\(([^)]+)\)"),
        }

    def parse(self, pdf_text: str) -> SDSParseResult:
        start = time.time()
        result = SDSParseResult(environment=self.env["capability"])
        sections = self._extract_sections(pdf_text)
        result.raw_sections = sections

        # Leverage existing robust extractor for backward compatibility.
        structured = sds_regex_extractor.extract_sds_structured(pdf_text)
        result.tables = structured.get("tables") or {}
        result.legacy = structured.get("legacy") or {}
        self.methods_used.append("regex_structured")

        result.cas_numbers = self._extract_cas_numbers(sections, result.legacy)
        if 2 in sections:
            result.ghs = self._extract_ghs(sections[2], result.legacy)
        if 9 in sections:
            result.physical_properties = self._extract_physical_properties(sections[9], result.tables)
        if 12 in sections:
            result.ecotoxicity = self._extract_ecotoxicity(sections[12], result.tables)
        if 1 in sections:
            result.product_name = self._extract_product_name(sections[1])

        result.parse_time_ms = int((time.time() - start) * 1000)
        result.methods_used = sorted(set(self.methods_used))
        return result

    def _extract_sections(self, text: str) -> dict[int, str]:
        if not text:
            return {}
        matches = list(self.patterns["section_header"].finditer(text))
        sections: dict[int, str] = {}
        for i, m in enumerate(matches):
            sec = int(m.group(1))
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            sections[sec] = text[start:end].strip()
        return sections

    def _extract_cas_numbers(self, sections: dict[int, str], legacy: dict[str, Any]) -> list[CASExtraction]:
        out: list[CASExtraction] = []
        seen: set[str] = set()

        # Primary: existing focused extractor output.
        legacy_cas = legacy.get("cas_numbers") or []
        for cas in legacy_cas:
            if cas in seen:
                continue
            seen.add(cas)
            out.append(
                CASExtraction(
                    cas=cas,
                    section=3 if 3 in sections else None,
                    method="focused_regex",
                    confidence=0.95,
                    validated=cas_validator.is_valid_cas_format(cas),
                )
            )
        if legacy_cas:
            self.methods_used.append("focused_regex")

        # Secondary: table-style extraction on key sections.
        for sec in (3, 15, 1):
            txt = sections.get(sec, "")
            if not txt:
                continue
            for table in sds_pdf_utils.extract_tables_from_text(txt):
                for row in table:
                    for idx, cell in enumerate(row):
                        m = self.patterns["cas"].search(str(cell))
                        if not m:
                            continue
                        cas = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                        if cas in seen:
                            continue
                        seen.add(cas)
                        chem = row[0].strip() if row and idx != 0 else None
                        out.append(
                            CASExtraction(
                                cas=cas,
                                chemical_name=chem,
                                section=sec,
                                method="table_parsing",
                                confidence=0.9 if sec == 3 else 0.8,
                                context=" | ".join(str(x) for x in row)[:220],
                                validated=self._validate_cas_checksum(cas),
                            )
                        )
            self.methods_used.append("table_parsing")

        # Optional local AI enhancement via Ollama (cloud-safe auto-disable).
        if self.env.get("can_use_ai") and out:
            out = self._enhance_with_ollama(out, sections)

        # Final confidence penalty for failed checks.
        for item in out:
            if not item.validated:
                item.warnings.append("CAS checksum validation failed")
                item.confidence *= 0.5
        return out

    def _enhance_with_ollama(self, current: list[CASExtraction], sections: dict[int, str]) -> list[CASExtraction]:
        context = sections.get(3) or sections.get(1) or ""
        if not context:
            return current
        try:
            import requests

            prompt = (
                "Verify CAS numbers from SDS context. Return JSON array only: "
                '[{"cas":"64-17-5","confidence":0.95,"chemical":"Ethanol","concentration":"95%"}]\n'
                f'Current: {[x.cas for x in current]}\nContext:\n{context[:900]}'
            )
            r = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": "phi3:mini", "prompt": prompt, "stream": False, "temperature": 0.1},
                timeout=8,
            )
            if r.status_code != 200:
                return current
            data = r.json()
            raw = data.get("response") or ""
            m = re.search(r"\[.*\]", raw, re.DOTALL)
            if not m:
                return current
            parsed = json.loads(m.group(0))
            by_cas = {x.cas: x for x in current}
            for item in parsed:
                cas = str(item.get("cas") or "").strip()
                if cas not in by_cas:
                    continue
                existing = by_cas[cas]
                conf = item.get("confidence")
                if isinstance(conf, (int, float)):
                    existing.confidence = max(existing.confidence, float(conf))
                chem = item.get("chemical")
                if chem and not existing.chemical_name:
                    existing.chemical_name = str(chem)
                conc = item.get("concentration")
                if conc and not existing.concentration:
                    existing.concentration = str(conc)
            self.methods_used.append("ollama_ai")
        except Exception:
            # Enhancement only; never fail parsing.
            return current
        return current

    def _extract_ghs(self, text: str, legacy: dict[str, Any]) -> GHSExtraction:
        g = GHSExtraction()
        legacy_ghs = legacy.get("ghs") or {}
        g.h_codes = sorted(set([str(x).upper() for x in (legacy_ghs.get("h_codes") or [])]))
        g.p_codes = sorted(set([str(x).upper() for x in (legacy_ghs.get("p_codes") or [])]))
        g.signal_word = legacy_ghs.get("signal_word") or None
        if not g.signal_word:
            m = self.patterns["signal"].search(text or "")
            g.signal_word = m.group(1).title() if m else None
        g.confidence = 0.9 if (g.h_codes or g.p_codes or g.signal_word) else 0.5
        return g

    def _extract_physical_properties(self, section9: str, tables: dict[str, Any]) -> list[PhysicalProperty]:
        out: list[PhysicalProperty] = []
        df = tables.get("physical_properties")
        if df is not None and hasattr(df, "iterrows") and not df.empty:
            for _, row in df.iterrows():
                name = str(row.get("property", "")).strip()
                val = row.get("value")
                unit = str(row.get("unit", "")).strip()
                try:
                    fval = float(val)
                except Exception:
                    continue
                out.append(
                    PhysicalProperty(
                        property_name=name or "property",
                        value=fval,
                        unit=unit or "",
                        method=str(row.get("method", "") or ""),
                        confidence=0.9,
                        raw_text=str(row.get("raw_text", "") or ""),
                    )
                )
            if out:
                return out
        for key, label in (("flash_point", "Flash Point"), ("boiling_point", "Boiling Point")):
            m = self.patterns["flash_point" if key == "flash_point" else "boiling_point"].search(section9 or "")
            if not m:
                continue
            value = float(str(m.group(1)).replace("<", "").replace(">", "").strip())
            unit = f"°{m.group(2).upper()}"
            out.append(PhysicalProperty(property_name=label, value=value, unit=unit, confidence=0.8, raw_text=m.group(0)))
        return out

    def _extract_ecotoxicity(self, section12: str, tables: dict[str, Any]) -> list[EcotoxValue]:
        out: list[EcotoxValue] = []
        df = tables.get("ecotoxicity")
        if df is not None and hasattr(df, "iterrows") and not df.empty:
            for _, row in df.iterrows():
                endpoint = str(row.get("endpoint", "")).upper()
                val = row.get("value")
                unit = str(row.get("unit", "")).strip()
                try:
                    fval = float(val)
                except Exception:
                    continue
                out.append(
                    EcotoxValue(
                        species=str(row.get("species", "") or "Unknown"),
                        endpoint=endpoint or "EC50",
                        value=fval,
                        unit=unit or "",
                        confidence=0.85,
                        raw_text=str(row.get("raw_text", "") or ""),
                    )
                )
            if out:
                return out
        for m in self.patterns["ecotox"].finditer(section12 or ""):
            endpoint = m.group(1).upper()
            value = float(m.group(2))
            unit = m.group(3)
            local = section12[max(0, m.start() - 80) : m.end() + 80]
            sm = self.patterns["species"].search(local)
            species = (sm.group(1).strip() if sm else "Unknown")
            dm = self.patterns["duration_h"].search(local)
            dur = float(dm.group(1)) if dm else None
            out.append(EcotoxValue(species=species, endpoint=endpoint, value=value, unit=unit, duration_h=dur, confidence=0.8, raw_text=m.group(0)))
        return out

    def _extract_product_name(self, text: str) -> str | None:
        for line in (text or "").splitlines():
            s = line.strip()
            if not s:
                continue
            if re.search(r"section|identification|product identifier", s, re.IGNORECASE):
                continue
            if 3 <= len(s) <= 120:
                return s
        return None

    def _validate_cas_checksum(self, cas: str) -> bool:
        if not cas or not re.match(r"^\d{1,7}-\d{2}-\d$", cas):
            return False
        try:
            a, b, c = cas.split("-")
            main = a + b
            check = int(c)
            total = 0
            for i, d in enumerate(reversed(main), 1):
                total += int(d) * i
            return (total % 10) == check
        except Exception:
            return False
