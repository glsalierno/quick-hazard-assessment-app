"""
Unified SDS parsing engine with adaptive method selection.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Optional

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

    # --- Enhanced Section 3 composition / multi-column tables (e.g. FORANE® blends) ---

    def _clean_cas(self, text: str) -> Optional[str]:
        """First CAS-like token in *text*, optionally checksum-validated."""
        if not text:
            return None
        m = self.patterns["cas"].search(str(text))
        if not m:
            return None
        cas = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        norm = cas_validator.normalize_cas_input(cas)
        return norm or cas

    def _extract_concentration_from_text(self, text: str) -> Optional[str]:
        """Pull concentration / % range strings common on SDS composition tables."""
        if not text:
            return None
        t = text.strip()
        # ">= 30 - < 60 %" / ">=30 - <60%" / "30 - 60%"
        range_pct = re.search(
            r"(?:([<>]=?)\s*)?(\d+(?:\.\d+)?)\s*[-–]\s*(?:([<>]=?)\s*)?(\d+(?:\.\d+)?)\s*[%％]?",
            t,
            re.IGNORECASE,
        )
        if range_pct:
            lo, lv, ho, hv = range_pct.group(1) or "", range_pct.group(2), range_pct.group(3) or "", range_pct.group(4)
            left = f"{lo} {lv}".strip() if lo else lv
            right = f"{ho} {hv}".strip() if ho else hv
            return f"{left} - {right}%"
        simple = re.search(r"(\d+(?:\.\d+)?)\s*[%％]", t)
        if simple:
            return f"{simple.group(1)}%"
        return None

    def _richness(self, c: CASExtraction) -> int:
        n = 0
        if (c.chemical_name or "").strip():
            n += 3
        if (c.concentration or "").strip():
            n += 3
        if c.section == 3:
            n += 1
        return n

    def _merge_cas_extractions(self, a: CASExtraction, b: CASExtraction) -> CASExtraction:
        """Prefer richer row; fill missing name/conc/context; max confidence."""
        primary, secondary = (a, b) if self._richness(a) >= self._richness(b) else (b, a)
        out = CASExtraction(
            cas=primary.cas,
            chemical_name=(primary.chemical_name or secondary.chemical_name or None),
            concentration=(primary.concentration or secondary.concentration or None),
            section=primary.section if primary.section is not None else secondary.section,
            method=primary.method,
            confidence=max(primary.confidence, secondary.confidence),
            context=primary.context or secondary.context,
            validated=primary.validated or secondary.validated,
            warnings=list({*primary.warnings, *secondary.warnings}),
        )
        return out

    def _put_cas(
        self,
        store: dict[str, CASExtraction],
        order: list[str],
        item: CASExtraction,
    ) -> None:
        if item.cas not in store:
            store[item.cas] = item
            order.append(item.cas)
        else:
            store[item.cas] = self._merge_cas_extractions(store[item.cas], item)

    def _parse_html_tables_for_cas(self, html_text: str) -> list[CASExtraction]:
        results: list[CASExtraction] = []
        if not html_text or "<table" not in html_text.lower():
            return results
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_text, "html.parser")
            for table in soup.find_all("table"):
                rows = table.find_all("tr")
                if len(rows) < 2:
                    continue
                header_cells = rows[0].find_all(["td", "th"])
                headers = [cell.get_text().strip().lower() for cell in header_cells]
                cas_col = name_col = conc_col = None
                for i, header in enumerate(headers):
                    if any(x in header for x in ("cas", "cas-no", "cas no", "cas number", "registry")):
                        cas_col = i
                    elif any(x in header for x in ("chemical", "component", "ingredient", "substance", "name")):
                        name_col = i
                    elif any(x in header for x in ("wt", "weight", "concentration", "%", "percent", "amount")):
                        conc_col = i
                if cas_col is None:
                    continue
                _cols = [cas_col]
                if name_col is not None:
                    _cols.append(name_col)
                if conc_col is not None:
                    _cols.append(conc_col)
                max_idx = max(_cols)
                for row in rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) <= max_idx:
                        continue
                    cas_raw = cells[cas_col].get_text()
                    cas = self._clean_cas(cas_raw)
                    if not cas or not self._validate_cas_checksum(cas):
                        continue
                    chemical = cells[name_col].get_text().strip() if name_col is not None and name_col < len(cells) else None
                    conc_raw = cells[conc_col].get_text() if conc_col is not None and conc_col < len(cells) else ""
                    concentration = self._extract_concentration_from_text(conc_raw) or (conc_raw.strip() or None)
                    ctx = " | ".join(c.get_text().strip() for c in cells)[:280]
                    results.append(
                        CASExtraction(
                            cas=cas,
                            chemical_name=chemical or None,
                            concentration=concentration,
                            section=3,
                            method="html_table_parsing",
                            confidence=0.98,
                            context=ctx,
                            validated=True,
                        )
                    )
        except Exception:
            return results
        return results

    def _iter_pipe_table_blocks(self, text: str) -> list[list[str]]:
        lines = text.replace("\r", "\n").split("\n")
        blocks: list[list[str]] = []
        current: list[str] = []
        for line in lines:
            if "|" in line:
                current.append(line)
            elif current:
                blocks.append(current)
                current = []
        if current:
            blocks.append(current)
        return blocks

    def _process_pipe_table_lines(self, table_lines: list[str]) -> list[CASExtraction]:
        results: list[CASExtraction] = []
        if len(table_lines) < 2:
            return results
        header_line = table_lines[0]
        header_cells = [c.strip() for c in header_line.split("|")]
        while header_cells and header_cells[0] == "":
            header_cells.pop(0)
        while header_cells and header_cells[-1] == "":
            header_cells.pop()
        headers = [h.lower() for h in header_cells]
        cas_col = name_col = conc_col = None
        for i, header in enumerate(headers):
            if any(t in header for t in ("cas", "cas-no", "cas number", "registry")):
                cas_col = i
            elif any(t in header for t in ("chemical", "component", "ingredient", "substance", "name")):
                name_col = i
            elif any(t in header for t in ("wt", "weight", "concentration", "%", "percent")):
                conc_col = i
        if cas_col is None:
            return results
        for line in table_lines[1:]:
            if re.match(r"^[\s|\-_:]+$", line):
                continue
            raw_cells = [c.strip() for c in line.split("|")]
            while raw_cells and raw_cells[0] == "":
                raw_cells.pop(0)
            while raw_cells and raw_cells[-1] == "":
                raw_cells.pop()
            cells = raw_cells
            if len(cells) <= cas_col:
                continue
            cas = self._clean_cas(cells[cas_col])
            if not cas or not self._validate_cas_checksum(cas):
                continue
            chemical = cells[name_col] if name_col is not None and name_col < len(cells) else None
            conc_cell = cells[conc_col] if conc_col is not None and conc_col < len(cells) else ""
            concentration = self._extract_concentration_from_text(conc_cell) or (conc_cell.strip() or None)
            results.append(
                CASExtraction(
                    cas=cas,
                    chemical_name=(chemical.strip() if chemical else None) or None,
                    concentration=concentration,
                    section=3,
                    method="pipe_table_parsing",
                    confidence=0.95,
                    context=" | ".join(cells)[:280],
                    validated=True,
                )
            )
        return results

    def _table_rows_to_cas_extractions(self, table: list[list[str]], section: int) -> list[CASExtraction]:
        """Use first row as header when it looks like a composition header."""
        if not table or len(table) < 2:
            return []
        header_row = [str(x).lower() for x in table[0]]
        joined = " ".join(header_row)
        if "cas" not in joined:
            return []
        cas_col = name_col = conc_col = None
        for i, h in enumerate(header_row):
            h = h.strip()
            if any(t in h for t in ("cas", "cas-no", "registry")):
                cas_col = i
            elif any(t in h for t in ("chemical", "component", "ingredient", "substance", "name")):
                name_col = i
            elif any(t in h for t in ("wt", "weight", "concentration", "%", "percent")):
                conc_col = i
        if cas_col is None:
            return []
        out: list[CASExtraction] = []
        _cols = [cas_col]
        if name_col is not None:
            _cols.append(name_col)
        if conc_col is not None:
            _cols.append(conc_col)
        max_idx = max(_cols)
        for row in table[1:]:
            if len(row) <= max_idx:
                continue
            cas = self._clean_cas(str(row[cas_col]))
            if not cas or not self._validate_cas_checksum(cas):
                continue
            chem = str(row[name_col]).strip() if name_col is not None and name_col < len(row) else ""
            conc_raw = str(row[conc_col]) if conc_col is not None and conc_col < len(row) else ""
            concentration = self._extract_concentration_from_text(conc_raw) or (conc_raw.strip() or None)
            out.append(
                CASExtraction(
                    cas=cas,
                    chemical_name=chem or None,
                    concentration=concentration,
                    section=section,
                    method="delimiter_table_parsing",
                    confidence=0.93 if section == 3 else 0.85,
                    context=" | ".join(str(x) for x in row)[:280],
                    validated=True,
                )
            )
        return out

    def _parse_whitespace_composition_lines(self, section_text: str) -> list[CASExtraction]:
        """
        Space/tab-aligned composition rows and CAS-only continuation lines (PDF reflow).
        """
        results: list[CASExtraction] = []
        lines = section_text.replace("\r", "\n").split("\n")
        pending_name: Optional[str] = None
        only_cas_line = re.compile(r"^\s*(\d{1,7}-\d{2}-\d)\s*$")

        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            # Header / label lines — keep pending name unless it's a new subsection
            low = line.lower()
            if "chemical name" in low and "cas" in low:
                pending_name = None
                continue
            if only_cas_line.match(line):
                m = only_cas_line.match(line)
                if m and pending_name:
                    cas = self._clean_cas(m.group(1))
                    if cas and self._validate_cas_checksum(cas):
                        results.append(
                            CASExtraction(
                                cas=cas,
                                chemical_name=pending_name.strip() or None,
                                concentration=None,
                                section=3,
                                method="orphan_cas_line",
                                confidence=0.88,
                                context=line,
                                validated=True,
                            )
                        )
                continue

            m = self.patterns["cas"].search(line)
            if not m:
                # Possible name-only line before CAS on next line
                if len(line) > 2 and not re.match(r"^[\d\s.%<>,\-–]+$", line) and "section" not in low:
                    pending_name = line
                continue

            cas = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            norm = cas_validator.normalize_cas_input(cas) or cas
            if not self._validate_cas_checksum(norm):
                continue

            before = line[: m.start()].strip()
            name = before if len(before) > 2 else (pending_name.strip() if pending_name else None)
            pending_name = None

            rest = line[m.end() :].strip()
            conc_part = re.split(r"\s{2,}H\d", rest, maxsplit=1)[0].strip()
            if not conc_part:
                conc_part = re.split(r"\s{2,}(?:H\d|P\d)", rest, maxsplit=1)[0].strip()
            concentration = self._extract_concentration_from_text(conc_part) or (conc_part or None)
            if concentration and len(concentration) > 80:
                concentration = self._extract_concentration_from_text(concentration)

            results.append(
                CASExtraction(
                    cas=norm,
                    chemical_name=name,
                    concentration=concentration,
                    section=3,
                    method="line_composition_parsing",
                    confidence=0.9,
                    context=line[:300],
                    validated=True,
                )
            )
        return results

    def _extract_composition_from_section3(self, section_text: str) -> list[CASExtraction]:
        if not (section_text or "").strip():
            return []
        acc: list[CASExtraction] = []
        acc.extend(self._parse_html_tables_for_cas(section_text))
        for block in self._iter_pipe_table_blocks(section_text):
            acc.extend(self._process_pipe_table_lines(block))
        for table in sds_pdf_utils.extract_tables_from_text(section_text):
            acc.extend(self._table_rows_to_cas_extractions(table, section=3))
        acc.extend(self._parse_whitespace_composition_lines(section_text))
        if acc:
            self.methods_used.append("composition_section3")
        return acc

    def _extract_cas_numbers(self, sections: dict[int, str], legacy: dict[str, Any]) -> list[CASExtraction]:
        store: dict[str, CASExtraction] = {}
        order: list[str] = []

        # 1) Section 3 enhanced composition (names + concentrations + section id)
        sec3 = sections.get(3, "") or ""
        for item in self._extract_composition_from_section3(sec3):
            self._put_cas(store, order, item)

        # 2) Legacy regex CAS list (merge; fills CAS missed by table or vice versa)
        legacy_cas = legacy.get("cas_numbers") or []
        for cas in legacy_cas:
            cas = str(cas).strip()
            if not cas:
                continue
            norm = cas_validator.normalize_cas_input(cas) or cas
            item = CASExtraction(
                cas=norm,
                section=3 if 3 in sections else None,
                method="focused_regex",
                confidence=0.95,
                validated=cas_validator.is_valid_cas_format(norm),
            )
            self._put_cas(store, order, item)
        if legacy_cas:
            self.methods_used.append("focused_regex")

        # 3) Delimiter tables in sections 15 and 1 (skip 3 — already covered above)
        for sec in (15, 1):
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
                        norm = cas_validator.normalize_cas_input(cas) or cas
                        if not self._validate_cas_checksum(norm):
                            continue
                        chem = row[0].strip() if row and idx != 0 else None
                        self._put_cas(
                            store,
                            order,
                            CASExtraction(
                                cas=norm,
                                chemical_name=chem or None,
                                section=sec,
                                method="table_parsing",
                                confidence=0.82 if sec == 15 else 0.78,
                                context=" | ".join(str(x) for x in row)[:220],
                                validated=True,
                            ),
                        )
            self.methods_used.append("table_parsing")

        out = [store[k] for k in order]

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
