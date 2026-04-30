"""SQLite cache builder/reader for offline IUCLID .i6z dossier snippets."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import tempfile
from typing import Any
import xml.etree.ElementTree as ET

from ingest import offline_echa_loader as ob
from unified_hazard_report.iuclid_extractor import read_i6d_bytes_from_i6z
from tqdm import tqdm
from utils.iuclid_phrase_mapper import get_phrase_label

logger = logging.getLogger(__name__)
SKIP_EXISTING_CACHE = os.getenv("SKIP_EXISTING_CACHE", "true").strip().lower() in ("1", "true", "yes", "on")
I6Z_PARSE_TIMEOUT_S = int(os.getenv("OFFLINE_I6Z_PARSE_TIMEOUT_S", "20"))

_ENDPOINT_HINTS = (
    "endpoint",
    "result",
    "effectlevel",
    "effect_concentration",
    "effectconcentration",
    "dose",
    "exposure",
    "toxicity",
    "study",
    "species",
    "noael",
    "loael",
    "ld50",
    "lc50",
    "ec50",
    "ic50",
)


def _tag_local(tag: str) -> str:
    if not tag:
        return ""
    return tag.split("}")[-1]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def cache_db_path() -> Path:
    cache_dir = Path(os.getenv("OFFLINE_CACHE_DIR", str(ob.OFFLINE_CACHE_DIR)))
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "offline_snippets_cache.db"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS iuclid_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS iuclid_xml_debug (
            uuid TEXT PRIMARY KEY,
            i6z_path TEXT,
            document_name TEXT,
            root_tag TEXT,
            sample_tags_json TEXT,
            parse_error TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS iuclid_cl_rows (
            uuid TEXT NOT NULL,
            hazard_code TEXT,
            hazard_code_label TEXT,
            signal_word TEXT,
            hazard_statement TEXT,
            hazard_statement_label TEXT,
            hazard_class TEXT,
            hazard_class_label TEXT,
            hazard_category TEXT,
            hazard_category_label TEXT,
            source_tag TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS iuclid_endpoints (
            uuid TEXT NOT NULL,
            endpoint_name TEXT,
            endpoint_name_label TEXT,
            result_value TEXT,
            result_value_label TEXT,
            unit TEXT,
            unit_label TEXT,
            species TEXT,
            species_label TEXT,
            source_tag TEXT,
            raw_text TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS iuclid_endpoints_normalized (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT NOT NULL,
            endpoint_name TEXT,
            endpoint_name_label TEXT,
            study_result_type_code TEXT,
            study_result_type_label TEXT,
            purpose_flag_code TEXT,
            purpose_flag_label TEXT,
            reliability_code TEXT,
            reliability_label TEXT,
            species_code TEXT,
            species_label TEXT,
            strain_code TEXT,
            strain_label TEXT,
            sex_code TEXT,
            sex_label TEXT,
            administration_exposure_code TEXT,
            administration_exposure_label TEXT,
            duration_raw TEXT,
            duration_value REAL,
            duration_unit TEXT,
            effect_endpoint_code TEXT,
            effect_endpoint_label TEXT,
            effect_level_value REAL,
            effect_level_unit TEXT,
            based_on_code TEXT,
            based_on_label TEXT,
            key_result INTEGER,
            effect_kind TEXT,
            source_tag TEXT,
            updated_at TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_iuclid_cl_uuid ON iuclid_cl_rows(uuid);
        CREATE INDEX IF NOT EXISTS ix_iuclid_ep_uuid ON iuclid_endpoints(uuid);
        CREATE INDEX IF NOT EXISTS ix_iuclid_epn_uuid ON iuclid_endpoints_normalized(uuid);
        """
    )
    _ensure_table_columns(
        conn,
        "iuclid_cl_rows",
        {
            "hazard_code_label": "TEXT",
            "hazard_statement_label": "TEXT",
            "hazard_class_label": "TEXT",
            "hazard_category_label": "TEXT",
        },
    )
    _ensure_table_columns(
        conn,
        "iuclid_endpoints",
        {
            "endpoint_name_label": "TEXT",
            "result_value_label": "TEXT",
            "unit_label": "TEXT",
            "species_label": "TEXT",
        },
    )
    _ensure_table_columns(
        conn,
        "iuclid_endpoints_normalized",
        {
            "endpoint_name_label": "TEXT",
            "study_result_type_code": "TEXT",
            "study_result_type_label": "TEXT",
            "purpose_flag_code": "TEXT",
            "purpose_flag_label": "TEXT",
            "reliability_code": "TEXT",
            "reliability_label": "TEXT",
            "species_code": "TEXT",
            "species_label": "TEXT",
            "strain_code": "TEXT",
            "strain_label": "TEXT",
            "sex_code": "TEXT",
            "sex_label": "TEXT",
            "administration_exposure_code": "TEXT",
            "administration_exposure_label": "TEXT",
            "duration_raw": "TEXT",
            "duration_value": "REAL",
            "duration_unit": "TEXT",
            "effect_endpoint_code": "TEXT",
            "effect_endpoint_label": "TEXT",
            "effect_level_value": "REAL",
            "effect_level_unit": "TEXT",
            "based_on_code": "TEXT",
            "based_on_label": "TEXT",
            "key_result": "INTEGER",
            "effect_kind": "TEXT",
            "source_tag": "TEXT",
        },
    )
    conn.commit()


def _ensure_table_columns(conn: sqlite3.Connection, table: str, cols: dict[str, str]) -> None:
    existing = {str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, col_type in cols.items():
        if col in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def _resolve_archive_or_dir() -> Path | None:
    raw = (os.getenv("OFFLINE_LOCAL_ARCHIVE") or "").strip()
    if not raw:
        return None
    p = Path(os.path.expandvars(raw)).expanduser().resolve()
    return p if p.exists() else None


def _extract_archive_to_dir(archive: Path, extract_root: Path, *, force: bool) -> Path | None:
    if extract_root.exists() and force:
        for child in extract_root.glob("*"):
            if child.is_dir():
                import shutil

                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
    if not extract_root.exists():
        extract_root.mkdir(parents=True, exist_ok=True)
    if archive.suffix.lower() == ".zip":
        with zipfile.ZipFile(archive, "r") as zf:
            zf.extractall(extract_root)
        return extract_root
    if archive.suffix.lower() == ".7z":
        if not ob._extract_7z(archive, extract_root):  # noqa: SLF001 - reuse existing extractor
            return None
        return extract_root
    return None


def _resolve_i6z_roots(*, force_extract: bool) -> tuple[list[Path], str]:
    roots: list[Path] = []
    source = "none"
    ap = _resolve_archive_or_dir()
    if ap is None:
        return roots, source
    if ap.is_dir():
        roots.append(ap)
        source = "directory"
        return roots, source
    if ap.is_file():
        out_dir = ob._extract_dir_for_archive(ap, ob.OFFLINE_DATA_DIR)  # noqa: SLF001
        if force_extract or not out_dir.exists():
            extracted = _extract_archive_to_dir(ap, out_dir, force=force_extract)
            if extracted is None:
                return [], "extract_failed"
        roots.append(out_dir)
        source = "archive"
    return roots, source


def _scan_i6z_files(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for p in list(root.rglob("*.i6z")) + list(root.rglob("*.I6Z")):
            key = str(p.resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            files.append(p)
    files.sort(key=lambda x: str(x).lower())
    return files


def _extract_cl_rows_from_xml_text(text_blob: str, *, default_source: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for code in sorted(set(re.findall(r"\bH\d{3}[A-Z]?\b", text_blob, re.I))):
        rows.append(
            {
                "hazard_code": code.upper(),
                "signal_word": "",
                "hazard_statement": "",
                "hazard_class": "",
                "hazard_category": "",
                "source_tag": default_source,
            }
        )
    for m in re.finditer(r"\b(Danger|Warning)\b", text_blob, re.I):
        rows.append(
            {
                "hazard_code": "",
                "signal_word": m.group(1).title(),
                "hazard_statement": "",
                "hazard_class": "",
                "hazard_category": "",
                "source_tag": "regex_signal_word",
            }
        )
    for m in re.finditer(r"\b(H\d{3}[A-Z]?)\b\s*[:\-]?\s*([^\n]{4,220})", text_blob, re.I):
        code = _as_text(m.group(1)).upper()
        stmt = _as_text(m.group(2))
        if stmt:
            rows.append(
                {
                    "hazard_code": code,
                    "signal_word": "",
                    "hazard_statement": stmt,
                    "hazard_class": "",
                    "hazard_category": "",
                    "source_tag": "regex_h_statement",
                }
            )
    return rows


def _clean_small_text(s: str) -> str:
    t = _as_text(s)
    if not t:
        return ""
    if len(t) > 160:
        return ""
    if "/" in t and len(t) > 36:
        return ""
    return t


def _first_value_for_tags(root: ET.Element, tags: tuple[str, ...]) -> str:
    for el in root.iter():
        loc = _tag_local(el.tag).lower()
        if loc in tags:
            v = _clean_small_text(el.text or "")
            if v:
                return v
    return ""


def _decode_code(v: str) -> tuple[str, str]:
    raw = _as_text(v)
    if not raw:
        return "", ""
    label = get_phrase_label(raw)
    return raw, label


def xpath_value(element: ET.Element, path: str, namespaces: dict[str, str] | None = None) -> str | None:
    """Return first text value from XPath-like path, or None (namespace-safe with {*} paths)."""
    namespaces = namespaces or {}
    # If running on lxml element, prefer native xpath.
    if hasattr(element, "xpath"):
        try:
            els = element.xpath(path, namespaces=namespaces)  # type: ignore[attr-defined]
            if not els:
                return None
            first = els[0]
            if isinstance(first, str):
                return _as_text(first) or None
            txt = _as_text(getattr(first, "text", ""))
            return txt or None
        except Exception:
            pass
    try:
        els = element.findall(path)
    except Exception:
        return None
    if not els:
        return None
    txt = _as_text(els[0].text if hasattr(els[0], "text") else "")
    return txt or None


def _numeric_with_optional_unit(v: str) -> tuple[float | None, str]:
    txt = _as_text(v).replace(",", ".")
    if not txt:
        return None, ""
    m = re.match(r"^\s*([-+]?\d+(?:\.\d+)?)\s*([A-Za-z/%µu][A-Za-z0-9/%µu\-\s]*)?\s*$", txt)
    if not m:
        return None, ""
    try:
        num = float(m.group(1))
    except ValueError:
        return None, ""
    unit = _as_text(m.group(2) or "")
    return num, unit


def _content_endpoint_root(root: ET.Element) -> ET.Element | None:
    for el in root.iter():
        loc = _tag_local(el.tag)
        if loc.startswith("ENDPOINT_STUDY_RECORD."):
            return el
    return None


def _document_subtype(root: ET.Element, content_root: ET.Element | None) -> str:
    for el in root.iter():
        if _tag_local(el.tag) == "documentSubType":
            txt = _as_text(el.text or "")
            if txt:
                return txt
    if content_root is not None:
        loc = _tag_local(content_root.tag)
        if "." in loc:
            return loc.split(".", 1)[1]
    return ""


def _base_row(endpoint_template: str) -> dict[str, Any]:
    ec, el = _decode_code(endpoint_template)
    return {
        "endpoint_name": ec,
        "endpoint_name_label": el,
        "study_result_type_code": "",
        "study_result_type_label": "",
        "purpose_flag_code": "",
        "purpose_flag_label": "",
        "reliability_code": "",
        "reliability_label": "",
        "species_code": "",
        "species_label": "",
        "strain_code": "",
        "strain_label": "",
        "sex_code": "",
        "sex_label": "",
        "administration_exposure_code": "",
        "administration_exposure_label": "",
        "duration_raw": "",
        "duration_value": None,
        "duration_unit": "",
        "effect_endpoint_code": "",
        "effect_endpoint_label": "",
        "effect_level_value": None,
        "effect_level_unit": "",
        "based_on_code": "",
        "based_on_label": "",
        "key_result": None,
        "effect_kind": "",
        "source_tag": "endpoint_record_summary",
    }


def _extract_toxicity_to_aquatic_algae(content_root: ET.Element, endpoint_template: str) -> list[dict[str, Any]]:
    row = _base_row(endpoint_template)
    row["study_result_type_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}StudyResultType/{*}value") or ""
    row["purpose_flag_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}PurposeFlag/{*}value") or ""
    row["reliability_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}Reliability/{*}value") or ""
    for src, dst in (
        ("study_result_type_code", "study_result_type_label"),
        ("purpose_flag_code", "purpose_flag_label"),
        ("reliability_code", "reliability_label"),
    ):
        row[dst] = get_phrase_label(row[src]) if row[src] else ""
    return [row]


def _fill_common_admin_fields(row: dict[str, Any], content_root: ET.Element) -> None:
    row["study_result_type_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}StudyResultType/{*}value") or ""
    row["purpose_flag_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}PurposeFlag/{*}value") or ""
    row["reliability_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}Reliability/{*}value") or ""
    for src, dst in (
        ("study_result_type_code", "study_result_type_label"),
        ("purpose_flag_code", "purpose_flag_label"),
        ("reliability_code", "reliability_label"),
    ):
        row[dst] = get_phrase_label(row[src]) if row[src] else ""


def _fill_common_test_animals_and_exposure(row: dict[str, Any], content_root: ET.Element) -> None:
    row["species_code"] = xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}TestAnimals/{*}Species/{*}value") or ""
    row["strain_code"] = xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}TestAnimals/{*}Strain/{*}value") or ""
    row["sex_code"] = xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}TestAnimals/{*}Sex/{*}value") or ""
    row["administration_exposure_code"] = xpath_value(
        content_root,
        ".//{*}MaterialsAndMethods/{*}AdministrationExposure/{*}RouteOfAdministration/{*}value",
    ) or ""
    row["duration_raw"] = (
        xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}AdministrationExposure/{*}DurationOfTreatmentExposure")
        or xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}AdministrationExposure/{*}DurationOfExposure")
        or ""
    )
    row["duration_value"], row["duration_unit"] = _numeric_with_optional_unit(row["duration_raw"])
    for src, dst in (
        ("species_code", "species_label"),
        ("strain_code", "strain_label"),
        ("sex_code", "sex_label"),
        ("administration_exposure_code", "administration_exposure_label"),
    ):
        row[dst] = get_phrase_label(row[src]) if row[src] else ""


def _extract_effect_level_rows(base_row: dict[str, Any], content_root: ET.Element, *, source_tag: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    effect_entries = content_root.findall(".//{*}ResultsAndDiscussion/{*}EffectLevels/{*}Efflevel/{*}entry")
    for entry in effect_entries:
        rr = dict(base_row)
        rr["key_result"] = 1 if (_as_text(xpath_value(entry, ".//{*}KeyResult") or "").lower() == "true") else 0
        rr["effect_endpoint_code"] = xpath_value(entry, ".//{*}Endpoint/{*}value") or ""
        rr["effect_endpoint_label"] = get_phrase_label(rr["effect_endpoint_code"]) if rr["effect_endpoint_code"] else ""
        unit_code = xpath_value(entry, ".//{*}EffectLevel/{*}unitCode") or ""
        rr["effect_level_unit"] = get_phrase_label(unit_code) if unit_code else ""
        rr["effect_level_value"] = None
        lv = xpath_value(entry, ".//{*}EffectLevel/{*}lowerValue") or xpath_value(entry, ".//{*}EffectLevel/{*}value")
        if lv:
            try:
                rr["effect_level_value"] = float(_as_text(lv).replace(",", "."))
            except ValueError:
                rr["effect_level_value"] = None
        rr["based_on_code"] = xpath_value(entry, ".//{*}BasedOn/{*}value") or ""
        rr["based_on_label"] = get_phrase_label(rr["based_on_code"]) if rr["based_on_code"] else ""
        rr["effect_kind"] = rr["effect_endpoint_label"] or rr["effect_endpoint_code"] or "effect"
        rr["source_tag"] = source_tag
        rows.append(rr)
    return rows


def _extract_repeated_dose_toxicity_oral(content_root: ET.Element, endpoint_template: str) -> list[dict[str, Any]]:
    row = _base_row(endpoint_template)
    _fill_common_admin_fields(row, content_root)
    _fill_common_test_animals_and_exposure(row, content_root)
    rows = _extract_effect_level_rows(row, content_root, source_tag="template:RepeatedDoseToxicityOral")
    return rows or [row]


def _extract_acute_toxicity(content_root: ET.Element, endpoint_template: str, *, subtype: str) -> list[dict[str, Any]]:
    row = _base_row(endpoint_template)
    _fill_common_admin_fields(row, content_root)
    _fill_common_test_animals_and_exposure(row, content_root)
    rows = _extract_effect_level_rows(row, content_root, source_tag=f"template:{subtype}")
    return rows or [row]


def _extract_genetic_toxicity(content_root: ET.Element, endpoint_template: str, *, subtype: str) -> list[dict[str, Any]]:
    row = _base_row(endpoint_template)
    _fill_common_admin_fields(row, content_root)
    row["effect_endpoint_code"] = xpath_value(content_root, ".//{*}AdministrativeData/{*}Endpoint/{*}value") or ""
    row["effect_endpoint_label"] = get_phrase_label(row["effect_endpoint_code"]) if row["effect_endpoint_code"] else ""
    row["species_code"] = xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}TestAnimals/{*}Species/{*}value") or ""
    row["species_label"] = get_phrase_label(row["species_code"]) if row["species_code"] else (
        xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}SpeciesStrain/{*}value") or ""
    )
    row["strain_code"] = xpath_value(content_root, ".//{*}MaterialsAndMethods/{*}TestSystem/{*}CellType/{*}value") or ""
    row["strain_label"] = get_phrase_label(row["strain_code"]) if row["strain_code"] else ""
    row["based_on_code"] = (
        xpath_value(content_root, ".//{*}ResultsAndDiscussion/{*}Result/{*}value")
        or xpath_value(content_root, ".//{*}ResultsAndDiscussion/{*}ResultSummary/{*}value")
        or ""
    )
    row["based_on_label"] = get_phrase_label(row["based_on_code"]) if row["based_on_code"] else ""
    key_flag = xpath_value(content_root, ".//{*}ResultsAndDiscussion//{*}KeyResult")
    row["key_result"] = 1 if _as_text(key_flag).lower() == "true" else 0
    row["effect_kind"] = row["effect_endpoint_label"] or row["based_on_label"] or subtype
    row["source_tag"] = f"template:{subtype}"
    return [row]


def _extract_normalized_rows_from_xml(root: ET.Element) -> list[dict[str, Any]]:
    content_root = _content_endpoint_root(root)
    if content_root is None:
        return []
    endpoint_template = _tag_local(content_root.tag)
    subtype = _document_subtype(root, content_root)
    if subtype == "ToxicityToAquaticAlgae":
        return _extract_toxicity_to_aquatic_algae(content_root, endpoint_template)
    if subtype == "RepeatedDoseToxicityOral":
        return _extract_repeated_dose_toxicity_oral(content_root, endpoint_template)
    if subtype in ("AcuteToxicityOral", "AcuteToxicityDermal", "AcuteToxicityInhalation"):
        return _extract_acute_toxicity(content_root, endpoint_template, subtype=subtype)
    if subtype in ("GeneticToxicityVitro", "GeneticToxicityVivo"):
        return _extract_genetic_toxicity(content_root, endpoint_template, subtype=subtype)

    # Existing generic fallback for other endpoint templates.
    row = _base_row(endpoint_template)
    srt = _first_value_for_tags(root, ("studyresulttype",))
    sp = _first_value_for_tags(root, ("species", "testorganismsspecies"))
    adm = _first_value_for_tags(root, ("administrationexposure", "routeofexposure"))
    row["study_result_type_code"], row["study_result_type_label"] = _decode_code(srt)
    row["species_code"], row["species_label"] = _decode_code(sp)
    row["administration_exposure_code"], row["administration_exposure_label"] = _decode_code(adm)
    row["duration_raw"] = _first_value_for_tags(root, ("totalexposureduration", "durationoftreatmentexposure", "durationofexposure"))
    row["duration_value"], row["duration_unit"] = _numeric_with_optional_unit(row["duration_raw"])
    return [row]


def _extract_from_xml(root: ET.Element, xml_bytes: bytes) -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, Any]]:
    cl_rows: list[dict[str, str]] = []
    endpoint_rows: list[dict[str, str]] = []
    tag_counter: Counter[str] = Counter()

    for el in root.iter():
        loc = _tag_local(el.tag)
        if not loc:
            continue
        loc_l = loc.lower()
        tag_counter[loc] += 1
        text = _as_text(" ".join((el.text or "").split()))
        if not text and list(el):
            text = _as_text(" ".join("".join(el.itertext()).split()))

        if "hazardstatement" in loc_l or loc_l in ("hcode", "signalword", "hazardclass", "hazardcategory"):
            row = {
                "hazard_code": text if ("hcode" in loc_l or "hazardstatementcode" in loc_l) else "",
                "signal_word": text if "signalword" in loc_l else "",
                "hazard_statement": text if ("hazardstatement" in loc_l and "code" not in loc_l) else "",
                "hazard_class": text if ("hazardclass" in loc_l and "category" not in loc_l) else "",
                "hazard_category": text if "hazardcategory" in loc_l else "",
                "source_tag": f"tag:{loc}",
            }
            if any(row.values()):
                row["hazard_code_label"] = get_phrase_label(row["hazard_code"]) if row["hazard_code"] else ""
                row["hazard_statement_label"] = (
                    get_phrase_label(row["hazard_statement"]) if row["hazard_statement"] else ""
                )
                row["hazard_class_label"] = get_phrase_label(row["hazard_class"]) if row["hazard_class"] else ""
                row["hazard_category_label"] = (
                    get_phrase_label(row["hazard_category"]) if row["hazard_category"] else ""
                )
                cl_rows.append(row)

        if any(h in loc_l for h in _ENDPOINT_HINTS):
            if text and (any(ch.isdigit() for ch in text) or len(text) > 24):
                endpoint_rows.append(
                    {
                        "endpoint_name": loc,
                        "endpoint_name_label": get_phrase_label(loc),
                        "result_value": text[:1200],
                        "result_value_label": get_phrase_label(text[:1200]),
                        "unit": _as_text(el.attrib.get("unit") or ""),
                        "unit_label": get_phrase_label(_as_text(el.attrib.get("unit") or "")),
                        "species": _as_text(el.attrib.get("species") or ""),
                        "species_label": get_phrase_label(_as_text(el.attrib.get("species") or "")),
                        "source_tag": f"tag:{loc}",
                        "raw_text": text[:2000],
                    }
                )

    blob = xml_bytes.decode("utf-8", errors="replace")
    cl_rows.extend(_extract_cl_rows_from_xml_text(blob, default_source="regex_xml_blob"))

    endpoint_pattern = re.compile(
        r"\b(LD50|LC50|NOAEL|LOAEL|EC50|IC50)\b[^0-9\n]{0,30}([0-9][0-9.,\s]{0,40})(mg/kg|mg/L|ppm|ug/L|μg/L|g/kg)?",
        re.I,
    )
    for m in endpoint_pattern.finditer(blob):
        endpoint_rows.append(
            {
                "endpoint_name": _as_text(m.group(1)).upper(),
                "endpoint_name_label": get_phrase_label(_as_text(m.group(1)).upper()),
                "result_value": _as_text(m.group(2)),
                "result_value_label": get_phrase_label(_as_text(m.group(2))),
                "unit": _as_text(m.group(3)),
                "unit_label": get_phrase_label(_as_text(m.group(3))),
                "species": "",
                "species_label": "",
                "source_tag": "regex_endpoint",
                "raw_text": _as_text(m.group(0))[:2000],
            }
        )

    xpath_counts = {
        "ghs_classification_nodes": len(root.findall(".//{*}GHSClassification")),
        "clp_classification_nodes": len(root.findall(".//{*}CLPClassification")),
        "classification_labelling_nodes": len(root.findall(".//{*}ClassificationAndLabelling")),
        "hazard_statement_nodes": len(root.findall(".//{*}HazardStatement")),
        "hazard_code_nodes": len(root.findall(".//{*}HazardCode")),
        "endpoint_study_nodes": len(root.findall(".//{*}EndpointStudyRecord")),
        "study_result_nodes": len(root.findall(".//{*}StudyResult")),
        "result_nodes": len(root.findall(".//{*}Result")),
    }
    debug = {
        "root_tag": _tag_local(root.tag),
        "sample_tags": dict(tag_counter.most_common(80)),
        "xpath_counts": xpath_counts,
    }
    return _dedupe_cl(cl_rows), _dedupe_endpoints(endpoint_rows), debug


def _dedupe_cl(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for r in rows:
        key = (
            _as_text(r.get("hazard_code")).upper(),
            _as_text(r.get("signal_word")),
            _as_text(r.get("hazard_statement"))[:200],
            _as_text(r.get("hazard_class")),
            _as_text(r.get("hazard_category")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _dedupe_endpoints(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for r in rows:
        key = (
            _as_text(r.get("endpoint_name")),
            _as_text(r.get("result_value"))[:220],
            _as_text(r.get("unit")),
            _as_text(r.get("species")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _document_name_in_i6z(i6z_path: Path) -> str:
    try:
        with zipfile.ZipFile(i6z_path, "r") as zf:
            names = zf.namelist()
            doc = next((n for n in names if n.lower().endswith("document.i6d")), None)
            if doc:
                return doc
            any_i6d = next((n for n in names if n.lower().endswith(".i6d")), None)
            return any_i6d or ""
    except Exception:
        return ""


def _all_i6d_payloads_in_i6z(i6z_path: Path) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        with zipfile.ZipFile(i6z_path, "r") as zf:
            names = zf.namelist()
            i6d_names = [n for n in names if n.lower().endswith(".i6d")]
            if not i6d_names:
                raw = read_i6d_bytes_from_i6z(i6z_path)
                if raw:
                    out.append(("Document.i6d", raw))
                return out
            for name in i6d_names:
                try:
                    out.append((name, zf.read(name)))
                except Exception:
                    continue
    except Exception:
        raw = read_i6d_bytes_from_i6z(i6z_path)
        if raw:
            out.append(("Document.i6d", raw))
    return out


def _parse_xml_with_iterparse(xml_bytes: bytes) -> ET.Element:
    """Parse potentially large XML payloads with iterparse to reduce peak memory."""
    parser_stream = BytesIO(xml_bytes)
    context = ET.iterparse(parser_stream, events=("start", "end"))
    root: ET.Element | None = None
    for event, elem in context:
        if root is None and event == "start":
            root = elem
    if root is None:
        raise ET.ParseError("Empty XML document")
    return root


def _uuid_already_cached(conn: sqlite3.Connection, uuid: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM iuclid_xml_debug d
        WHERE d.uuid = ?
        AND EXISTS (SELECT 1 FROM iuclid_endpoints e WHERE e.uuid = d.uuid)
        LIMIT 1
        """,
        (uuid,),
    ).fetchone()
    if row is not None:
        return True
    row2 = conn.execute(
        "SELECT 1 FROM iuclid_cl_rows WHERE uuid = ? LIMIT 1",
        (uuid,),
    ).fetchone()
    return row2 is not None


def _parse_single_i6z(i6z: Path, *, debug_dump_dir: Path | None = None) -> dict[str, Any]:
    uuid = i6z.stem
    doc_name = _document_name_in_i6z(i6z)
    payloads = _all_i6d_payloads_in_i6z(i6z)
    if not payloads:
        return {
            "uuid": uuid,
            "i6z": str(i6z),
            "doc_name": doc_name,
            "parse_error": "Document.i6d not found/readable",
            "cl_rows": [],
            "eps": [],
            "debug": {"root_tag": "", "sample_tags": {}},
        }
    all_cl: list[dict[str, str]] = []
    all_ep: list[dict[str, str]] = []
    all_ep_norm: list[dict[str, Any]] = []
    merged_counts: Counter[str] = Counter()
    root_tag_first = ""
    parsed_docs = 0
    for i6d_name, raw in payloads:
        if debug_dump_dir is not None:
            try:
                debug_dump_dir.mkdir(parents=True, exist_ok=True)
                safe_name = i6d_name.replace("/", "_").replace("\\", "_")
                (debug_dump_dir / f"{uuid}_{safe_name}.xml").write_bytes(raw)
            except Exception as exc:
                logger.debug("Could not dump XML for %s/%s: %s", i6z, i6d_name, exc)
        try:
            root = _parse_xml_with_iterparse(raw)
            cl_rows, eps, debug = _extract_from_xml(root, raw)
        except ET.ParseError:
            continue
        parsed_docs += 1
        if not root_tag_first:
            root_tag_first = _as_text(debug.get("root_tag"))
        for k, v in (debug.get("xpath_counts") or {}).items():
            merged_counts[str(k)] += int(v or 0)
        all_cl.extend(cl_rows)
        all_ep.extend(eps)
        all_ep_norm.extend(_extract_normalized_rows_from_xml(root))

    debug = {
        "root_tag": root_tag_first,
        "sample_tags": {},
        "xpath_counts": dict(merged_counts),
        "parsed_i6d_docs": parsed_docs,
        "total_i6d_docs": len(payloads),
    }
    return {
        "uuid": uuid,
        "i6z": str(i6z),
        "doc_name": doc_name,
        "parse_error": "",
        "cl_rows": _dedupe_cl(all_cl),
        "eps": _dedupe_endpoints(all_ep),
        "eps_normalized": all_ep_norm,
        "debug": debug,
    }


def rebuild_iuclid_cache(
    *,
    force_extract: bool = False,
    verbose_debug: bool = False,
    target_uuids: list[str] | None = None,
    skip_existing_cache: bool = SKIP_EXISTING_CACHE,
    debug_dump_dir: str | None = None,
) -> dict[str, int]:
    roots, source = _resolve_i6z_roots(force_extract=force_extract)
    i6z_files = _scan_i6z_files(roots)
    target_set = {str(u).strip().lower() for u in (target_uuids or []) if str(u).strip()}
    if target_set:
        i6z_files = [p for p in i6z_files if p.stem.strip().lower() in target_set]
    db_path = cache_db_path()
    conn = _connect(db_path)
    _ensure_schema(conn)

    now = datetime.now(timezone.utc).isoformat()
    if not skip_existing_cache:
        if target_set:
            placeholders = ",".join(["?"] * len(target_set))
            values = list(target_set)
            conn.execute(f"DELETE FROM iuclid_cl_rows WHERE lower(uuid) IN ({placeholders})", values)
            conn.execute(f"DELETE FROM iuclid_endpoints WHERE lower(uuid) IN ({placeholders})", values)
            conn.execute(f"DELETE FROM iuclid_endpoints_normalized WHERE lower(uuid) IN ({placeholders})", values)
            conn.execute(f"DELETE FROM iuclid_xml_debug WHERE lower(uuid) IN ({placeholders})", values)
        else:
            conn.execute("DELETE FROM iuclid_cl_rows")
            conn.execute("DELETE FROM iuclid_endpoints")
            conn.execute("DELETE FROM iuclid_endpoints_normalized")
            conn.execute("DELETE FROM iuclid_xml_debug")

    parsed = 0
    parse_errors = 0
    cl_count = 0
    ep_count = 0
    skipped_cached = 0

    dump_dir_path: Path | None = None
    if verbose_debug:
        if debug_dump_dir and str(debug_dump_dir).strip():
            dump_dir_path = Path(debug_dump_dir).expanduser().resolve()
        else:
            dump_dir_path = Path(tempfile.gettempdir()) / "iuclid_xml_debug_dump"
        logger.info("XML debug dump directory: %s", dump_dir_path)

    logger.info(
        "IUCLID cache rebuild started: source=%s files=%s target_uuids=%s skip_existing=%s db=%s",
        source,
        len(i6z_files),
        len(target_set),
        skip_existing_cache,
        db_path,
    )
    progress = tqdm(i6z_files, desc="IUCLID snippet rebuild", unit="i6z")
    for idx, i6z in enumerate(progress, start=1):
        uuid = i6z.stem
        progress.set_postfix_str(uuid[:16])
        logger.info("Processing i6z %s/%s: %s", idx, len(i6z_files), i6z)
        if skip_existing_cache and _uuid_already_cached(conn, uuid):
            skipped_cached += 1
            continue
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_parse_single_i6z, i6z, debug_dump_dir=dump_dir_path)
                result = fut.result(timeout=max(1, I6Z_PARSE_TIMEOUT_S))
            doc_name = result["doc_name"]
            if result["parse_error"]:
                parse_errors += 1
                conn.execute(
                    "INSERT OR REPLACE INTO iuclid_xml_debug(uuid, i6z_path, document_name, root_tag, sample_tags_json, parse_error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (uuid, result["i6z"], doc_name, "", "{}", result["parse_error"], now),
                )
                continue
            cl_rows = result["cl_rows"]
            eps = result["eps"]
            eps_norm = result.get("eps_normalized") or []
            debug = result["debug"]
            conn.execute(
                "INSERT OR REPLACE INTO iuclid_xml_debug(uuid, i6z_path, document_name, root_tag, sample_tags_json, parse_error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    uuid,
                    result["i6z"],
                    doc_name,
                    _as_text(debug.get("root_tag")),
                    json.dumps(debug.get("sample_tags") or {}, ensure_ascii=True),
                    "",
                    now,
                ),
            )
            for row in cl_rows:
                conn.execute(
                    "INSERT INTO iuclid_cl_rows(uuid, hazard_code, hazard_code_label, signal_word, hazard_statement, hazard_statement_label, hazard_class, hazard_class_label, hazard_category, hazard_category_label, source_tag, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        uuid,
                        _as_text(row.get("hazard_code")).upper(),
                        _as_text(row.get("hazard_code_label")),
                        _as_text(row.get("signal_word")),
                        _as_text(row.get("hazard_statement")),
                        _as_text(row.get("hazard_statement_label")),
                        _as_text(row.get("hazard_class")),
                        _as_text(row.get("hazard_class_label")),
                        _as_text(row.get("hazard_category")),
                        _as_text(row.get("hazard_category_label")),
                        _as_text(row.get("source_tag")),
                        now,
                    ),
                )
            for row in eps:
                conn.execute(
                    "INSERT INTO iuclid_endpoints(uuid, endpoint_name, endpoint_name_label, result_value, result_value_label, unit, unit_label, species, species_label, source_tag, raw_text, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        uuid,
                        _as_text(row.get("endpoint_name")),
                        _as_text(row.get("endpoint_name_label")),
                        _as_text(row.get("result_value")),
                        _as_text(row.get("result_value_label")),
                        _as_text(row.get("unit")),
                        _as_text(row.get("unit_label")),
                        _as_text(row.get("species")),
                        _as_text(row.get("species_label")),
                        _as_text(row.get("source_tag")),
                        _as_text(row.get("raw_text")),
                        now,
                    ),
                )
            for row in eps_norm:
                conn.execute(
                    "INSERT INTO iuclid_endpoints_normalized(uuid, endpoint_name, endpoint_name_label, study_result_type_code, study_result_type_label, purpose_flag_code, purpose_flag_label, reliability_code, reliability_label, species_code, species_label, strain_code, strain_label, sex_code, sex_label, administration_exposure_code, administration_exposure_label, duration_raw, duration_value, duration_unit, effect_endpoint_code, effect_endpoint_label, effect_level_value, effect_level_unit, based_on_code, based_on_label, key_result, effect_kind, source_tag, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        uuid,
                        _as_text(row.get("endpoint_name")),
                        _as_text(row.get("endpoint_name_label")),
                        _as_text(row.get("study_result_type_code")),
                        _as_text(row.get("study_result_type_label")),
                        _as_text(row.get("purpose_flag_code")),
                        _as_text(row.get("purpose_flag_label")),
                        _as_text(row.get("reliability_code")),
                        _as_text(row.get("reliability_label")),
                        _as_text(row.get("species_code")),
                        _as_text(row.get("species_label")),
                        _as_text(row.get("strain_code")),
                        _as_text(row.get("strain_label")),
                        _as_text(row.get("sex_code")),
                        _as_text(row.get("sex_label")),
                        _as_text(row.get("administration_exposure_code")),
                        _as_text(row.get("administration_exposure_label")),
                        _as_text(row.get("duration_raw")),
                        row.get("duration_value"),
                        _as_text(row.get("duration_unit")),
                        _as_text(row.get("effect_endpoint_code")),
                        _as_text(row.get("effect_endpoint_label")),
                        row.get("effect_level_value"),
                        _as_text(row.get("effect_level_unit")),
                        _as_text(row.get("based_on_code")),
                        _as_text(row.get("based_on_label")),
                        row.get("key_result"),
                        _as_text(row.get("effect_kind")),
                        _as_text(row.get("source_tag")),
                        now,
                    ),
                )
            parsed += 1
            cl_count += len(cl_rows)
            ep_count += len(eps)
            if verbose_debug and (idx <= 5 or idx % 200 == 0):
                logger.info(
                    "Parsed %s/%s i6z=%s docs=%s/%s root=%s cl=%s endpoints=%s xpath=%s",
                    idx,
                    len(i6z_files),
                    i6z.name,
                    int((debug or {}).get("parsed_i6d_docs") or 0),
                    int((debug or {}).get("total_i6d_docs") or 0),
                    _as_text(debug.get("root_tag")),
                    len(cl_rows),
                    len(eps),
                    json.dumps(debug.get("xpath_counts") or {}, ensure_ascii=True),
                )
        except FutureTimeoutError:
            parse_errors += 1
            conn.execute(
                "INSERT OR REPLACE INTO iuclid_xml_debug(uuid, i6z_path, document_name, root_tag, sample_tags_json, parse_error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uuid, str(i6z), _document_name_in_i6z(i6z), "", "{}", f"Parse timeout after {I6Z_PARSE_TIMEOUT_S}s", now),
            )
            logger.warning("Timeout parsing %s after %ss; skipped.", i6z, I6Z_PARSE_TIMEOUT_S)
        except ET.ParseError as exc:
            parse_errors += 1
            conn.execute(
                "INSERT OR REPLACE INTO iuclid_xml_debug(uuid, i6z_path, document_name, root_tag, sample_tags_json, parse_error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uuid, str(i6z), doc_name, "", "{}", f"XML parse error: {exc}", now),
            )
            logger.debug("XML parse error for %s: %s", i6z, exc)
        except Exception as exc:
            parse_errors += 1
            conn.execute(
                "INSERT OR REPLACE INTO iuclid_xml_debug(uuid, i6z_path, document_name, root_tag, sample_tags_json, parse_error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uuid, str(i6z), doc_name, "", "{}", f"Unhandled parse error: {exc}", now),
            )
            logger.exception("Unhandled IUCLID parse failure for %s", i6z)

        if idx % 500 == 0:
            conn.commit()

    conn.execute("INSERT OR REPLACE INTO iuclid_meta(key, value) VALUES(?, ?)", ("rebuilt_at", now))
    conn.execute("INSERT OR REPLACE INTO iuclid_meta(key, value) VALUES(?, ?)", ("source_mode", source))
    conn.execute("INSERT OR REPLACE INTO iuclid_meta(key, value) VALUES(?, ?)", ("i6z_files_total", str(len(i6z_files))))
    conn.execute("INSERT OR REPLACE INTO iuclid_meta(key, value) VALUES(?, ?)", ("skip_existing_cache", str(skip_existing_cache).lower()))
    conn.commit()
    conn.close()
    logger.info(
        "IUCLID cache rebuild done: parsed=%s errors=%s skipped_cached=%s cl_rows=%s endpoint_rows=%s db=%s",
        parsed,
        parse_errors,
        skipped_cached,
        cl_count,
        ep_count,
        db_path,
    )
    return {
        "i6z_total": len(i6z_files),
        "parsed": parsed,
        "parse_errors": parse_errors,
        "skipped_cached": skipped_cached,
        "cl_rows": cl_count,
        "endpoint_rows": ep_count,
    }


def load_cached_snippets_for_uuids(uuids: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not uuids:
        return [], []
    db = cache_db_path()
    if not db.is_file():
        return [], []
    items = [str(u).strip() for u in uuids if str(u).strip()]
    if not items:
        return [], []
    placeholders = ",".join(["?"] * len(items))
    conn = _connect(db)
    try:
        cl_cur = conn.execute(
            f"SELECT uuid, hazard_code, hazard_code_label, signal_word, hazard_statement, hazard_statement_label, hazard_class, hazard_class_label, hazard_category, hazard_category_label, source_tag FROM iuclid_cl_rows WHERE uuid IN ({placeholders})",
            items,
        )
        ep_cur = conn.execute(
            f"SELECT uuid, endpoint_name, endpoint_name_label, result_value, result_value_label, unit, unit_label, species, species_label, source_tag, raw_text FROM iuclid_endpoints WHERE uuid IN ({placeholders})",
            items,
        )
        cl_rows = [dict(r) for r in cl_cur.fetchall()]
        ep_rows = [dict(r) for r in ep_cur.fetchall()]
        return cl_rows, ep_rows
    finally:
        conn.close()


def load_cached_normalized_endpoints_for_uuids(uuids: list[str]) -> list[dict[str, Any]]:
    if not uuids:
        return []
    db = cache_db_path()
    if not db.is_file():
        return []
    items = [str(u).strip() for u in uuids if str(u).strip()]
    if not items:
        return []
    placeholders = ",".join(["?"] * len(items))
    conn = _connect(db)
    try:
        cur = conn.execute(
            f"SELECT uuid, endpoint_name, endpoint_name_label, study_result_type_code, study_result_type_label, purpose_flag_code, purpose_flag_label, reliability_code, reliability_label, species_code, species_label, strain_code, strain_label, sex_code, sex_label, administration_exposure_code, administration_exposure_label, duration_raw, duration_value, duration_unit, effect_endpoint_code, effect_endpoint_label, effect_level_value, effect_level_unit, based_on_code, based_on_label, key_result, effect_kind, source_tag FROM iuclid_endpoints_normalized WHERE uuid IN ({placeholders})",
            items,
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
