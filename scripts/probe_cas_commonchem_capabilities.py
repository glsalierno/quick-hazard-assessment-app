"""Probe CAS Common Chemistry capabilities without changing the Streamlit app.

The output contains normalized identity/property metadata and capability
statistics only. API keys, structure images, and complete raw responses are
never written.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from utils.cas_commonchem_client import (  # noqa: E402
    CASCommonChemistryClient,
    CASCommonChemistryError,
    PARSER_VERSION,
)

DEFAULT_PANEL = [
    ("381-73-7", "Difluoroacetic acid"),
    ("67-56-1", "Methanol"),
    ("71-43-2", "Benzene"),
    ("7732-18-5", "Water"),
    ("7647-01-0", "Hydrochloric acid"),
    ("50-00-0", "Formaldehyde"),
    ("108-88-3", "Toluene"),
    ("1607-31-4", "CID-resolution edge case"),
    ("1912-24-9", "Atrazine property-rich reference"),
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_search_summary(payload: dict[str, Any], cas_rn: str) -> dict[str, Any]:
    results = payload.get("results") or []
    exact = [
        {
            "rn": item.get("rn"),
            "name": item.get("name"),
        }
        for item in results
        if isinstance(item, dict) and str(item.get("rn") or "") == cas_rn
    ]
    return {
        "count": payload.get("count"),
        "returned_count": len(results),
        "exact_matches": exact,
    }


def _record_for(
    client: CASCommonChemistryClient,
    cas_rn: str,
    panel_label: str,
    *,
    test_export: bool,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "cas_requested": cas_rn,
        "panel_label": panel_label,
        "search": None,
        "detail": None,
        "export_probe": None,
        "errors": [],
    }

    search_started = time.perf_counter()
    try:
        search = client.search(cas_rn, size=10)
        record["search"] = {
            **_safe_search_summary(search, cas_rn),
            "response_time_seconds": round(time.perf_counter() - search_started, 3),
        }
    except CASCommonChemistryError as exc:
        record["errors"].append(f"search: {exc}")

    detail_started = time.perf_counter()
    try:
        detail = client.get_normalized_detail(cas_rn)
        detail["total_detail_time_seconds"] = round(
            time.perf_counter() - detail_started, 3
        )
        record["detail"] = detail
    except CASCommonChemistryError as exc:
        record["errors"].append(f"detail: {exc}")
        return record

    if test_export and detail.get("has_molfile") and detail.get("uri"):
        export_started = time.perf_counter()
        try:
            molfile = client.export_molfile(str(detail["uri"]))
            record["export_probe"] = {
                "ok": bool(molfile.strip()),
                "character_count": len(molfile),
                "contains_mol_end_marker": "M  END" in molfile,
                "response_time_seconds": round(
                    time.perf_counter() - export_started, 3
                ),
            }
        except CASCommonChemistryError as exc:
            record["export_probe"] = {"ok": False, "error": str(exc)}
    elif detail.get("has_molfile"):
        record["export_probe"] = {"available": True, "tested": False}
    else:
        record["export_probe"] = {"available": False, "tested": False}

    return record


def _summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    found = [item for item in records if item.get("detail")]
    property_names = sorted(
        {
            prop.get("name")
            for item in found
            for prop in (item["detail"].get("experimental_properties") or [])
            if prop.get("name")
        }
    )
    toxicity_fields = sorted(
        {
            name
            for item in found
            for name in (
                item["detail"]
                .get("capability_observation", {})
                .get("toxicity_like_fields", [])
            )
        }
    )
    toxicity_properties = sorted(
        {
            name
            for item in found
            for name in (
                item["detail"]
                .get("capability_observation", {})
                .get("toxicity_like_property_names", [])
            )
        }
    )
    return {
        "panel_size": len(records),
        "detail_records_found": len(found),
        "detail_records_not_found_or_failed": len(records) - len(found),
        "records_with_experimental_properties": sum(
            bool(item["detail"].get("experimental_properties")) for item in found
        ),
        "total_experimental_property_rows": sum(
            len(item["detail"].get("experimental_properties") or []) for item in found
        ),
        "experimental_property_names": property_names,
        "toxicity_like_top_level_fields": toxicity_fields,
        "toxicity_like_property_names": toxicity_properties,
        "records_with_replaced_rns": sum(
            bool(item["detail"].get("replaced_rns")) for item in found
        ),
        "records_with_molfiles": sum(
            bool(item["detail"].get("has_molfile")) for item in found
        ),
    }


def _markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# CAS Common Chemistry capability probe",
        "",
        f"- Run at (UTC): `{report['run_at_utc']}`",
        f"- Parser: `{report['parser_version']}`",
        f"- Panel size: **{summary['panel_size']}**",
        f"- Detail records found: **{summary['detail_records_found']}**",
        f"- Records with experimental properties: **{summary['records_with_experimental_properties']}**",
        f"- Total experimental property rows: **{summary['total_experimental_property_rows']}**",
        f"- Toxicity-like top-level fields: `{summary['toxicity_like_top_level_fields']}`",
        f"- Toxicity-like property names: `{summary['toxicity_like_property_names']}`",
        "",
        "## Per-chemical results",
        "",
        "| CAS RN | CAS name | Properties | Property names | Molfile | Errors |",
        "|---|---|---:|---|---|---|",
    ]
    for item in report["records"]:
        detail = item.get("detail") or {}
        props = detail.get("experimental_properties") or []
        prop_names = ", ".join(
            sorted({str(prop.get("name")) for prop in props if prop.get("name")})
        )
        errors = "; ".join(item.get("errors") or [])
        lines.append(
            "| {cas} | {name} | {count} | {props} | {mol} | {errors} |".format(
                cas=item["cas_requested"],
                name=str(detail.get("name") or "not found").replace("|", "\\|"),
                count=len(props),
                props=prop_names.replace("|", "\\|"),
                mol="yes" if detail.get("has_molfile") else "no",
                errors=errors.replace("|", "\\|"),
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "CAS Common Chemistry is evaluated here as a curated identity, structure, "
            "citation, and experimental physical-property source. A lack of structured "
            "toxicity-like fields in this probe means toxicity retrieval must continue "
            "through dedicated sources such as ToxValDB and PubChem.",
            "",
            "The JSON companion preserves normalized property values and citations but "
            "does not contain the API key, SVG images, molfiles, or complete raw responses.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=APP_ROOT / "validation_snapshots" / "cas_commonchem",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Do not test molfile export for the first eligible panel record.",
    )
    args = parser.parse_args()

    client = CASCommonChemistryClient()
    records: list[dict[str, Any]] = []
    export_tested = False
    for index, (cas_rn, label) in enumerate(DEFAULT_PANEL, start=1):
        should_test_export = not args.skip_export and not export_tested
        print(f"[{index}/{len(DEFAULT_PANEL)}] CAS {cas_rn}", flush=True)
        record = _record_for(
            client,
            cas_rn,
            label,
            test_export=should_test_export,
        )
        if (record.get("export_probe") or {}).get("ok"):
            export_tested = True
        records.append(record)
        time.sleep(0.35)

    report = {
        "run_at_utc": _utc_now(),
        "purpose": "Academic/noncommercial GHhaz6 CAS retrieval capability test",
        "license_context": "CAS Common Chemistry CC BY-NC 4.0",
        "parser_version": PARSER_VERSION,
        "summary": _summarize(records),
        "records": records,
    }
    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.out_dir / "default_panel_capability.json"
    md_path = args.out_dir / "default_panel_capability.md"
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")

    print(json.dumps(report["summary"], indent=2))
    print(f"JSON: {json_path}")
    print(f"Markdown: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
