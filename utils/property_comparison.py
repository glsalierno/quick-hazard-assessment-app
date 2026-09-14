"""Cross-source physical-property comparison for CAS, PubChem, and OPERA."""

from __future__ import annotations

import re
from typing import Any

COMPARISON_VERSION = "property_comparison_v6_1"


def _float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)", str(value))
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None


def _display(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(str(item) for item in value if item is not None)
    return str(value).strip()


def _cas_property(
    cas_data: dict[str, Any],
    names: tuple[str, ...],
) -> dict[str, Any] | None:
    wanted = {name.casefold() for name in names}
    for item in cas_data.get("experimental_properties") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("name") or "").strip().casefold() in wanted:
            return item
    return None


def _cas_numeric(item: dict[str, Any] | None) -> float | None:
    if not item:
        return None
    if item.get("value") is not None:
        return _float(item["value"])
    low = _float(item.get("value_min"))
    high = _float(item.get("value_max"))
    if low is not None and high is not None:
        return (low + high) / 2
    values = item.get("numeric_values") or []
    return _float(values[0]) if values else None


def _opera_value(opera_result: dict[str, Any], column: str) -> Any:
    row = opera_result.get("row") or {}
    value = row.get(column)
    if value is None or str(value).strip().lower() in ("", "na", "nan", "none"):
        return None
    return value


def _comparison_status(
    values: list[tuple[str, float | None]],
    *,
    threshold: float | None,
    property_name: str,
    unit: str,
) -> tuple[str, list[str]]:
    available = [(source, value) for source, value in values if value is not None]
    if len(available) < 2:
        return "Single-source evidence", []
    numeric = [value for _, value in available]
    delta = max(numeric) - min(numeric)
    if threshold is None:
        return f"Compared; range {delta:g} {unit}".strip(), []
    if delta > threshold:
        sources = ", ".join(f"{source}={value:g}" for source, value in available)
        warning = (
            f"{property_name} disagreement across sources ({sources}; "
            f"range={delta:g} {unit}). CAS values are experimental/curated, "
            "PubChem values may aggregate literature, and OPERA values are predicted."
        )
        return "Review disagreement", [warning]
    return f"Agreement check passed (range {delta:g} {unit})".strip(), []


def build_property_comparison(
    cas_data: dict[str, Any] | None,
    pubchem_data: dict[str, Any] | None,
    opera_result: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return source-labeled comparison rows and non-blocking disagreement warnings."""
    cas = cas_data or {}
    pubchem = pubchem_data or {}
    opera = opera_result or {}
    warnings = list(opera.get("warnings") or [])
    rows: list[dict[str, Any]] = []

    definitions = [
        {
            "name": "Molecular weight",
            "unit": "g/mol",
            "cas_item": None,
            "cas_display": cas.get("molecular_mass"),
            "cas_numeric": _float(cas.get("molecular_mass")),
            "pubchem_display": pubchem.get("mw"),
            "pubchem_numeric": _float(pubchem.get("mw")),
            "opera_column": "MolWeight",
            "threshold": 1.0,
        },
        {
            "name": "LogP",
            "unit": "dimensionless",
            "cas_item": _cas_property(cas, ("LogP", "Partition Coefficient")),
            "pubchem_display": pubchem.get("xlogp"),
            "pubchem_numeric": _float(pubchem.get("xlogp")),
            "opera_column": "LogP_pred",
            "threshold": 2.0,
        },
        {
            "name": "Melting point",
            "unit": "°C",
            "cas_item": _cas_property(cas, ("Melting Point",)),
            "pubchem_display": pubchem.get("melting_point"),
            "pubchem_numeric": _float(pubchem.get("melting_point")),
            "opera_column": "MP_pred",
            "threshold": 50.0,
        },
        {
            "name": "Boiling point",
            "unit": "°C",
            "cas_item": _cas_property(cas, ("Boiling Point",)),
            "pubchem_display": pubchem.get("boiling_point"),
            "pubchem_numeric": _float(pubchem.get("boiling_point")),
            "opera_column": "BP_pred",
            "threshold": 50.0,
        },
        {
            "name": "Density",
            "unit": "g/cm3",
            "cas_item": _cas_property(cas, ("Density",)),
            "pubchem_display": pubchem.get("density"),
            "pubchem_numeric": _float(pubchem.get("density")),
            "opera_column": "",
            "threshold": 0.5,
        },
    ]

    for definition in definitions:
        cas_item = definition.get("cas_item")
        cas_display = definition.get("cas_display")
        cas_numeric = definition.get("cas_numeric")
        if cas_item:
            cas_display = cas_item.get("raw_value")
            cas_numeric = _cas_numeric(cas_item)
            cas_unit = str(cas_item.get("unit") or "")
            expected_unit = str(definition["unit"])
            if expected_unit == "°C" and cas_unit and cas_unit != "°C":
                cas_numeric = None
            if expected_unit == "g/cm3" and cas_unit and cas_unit.lower() != "g/cm3":
                cas_numeric = None

        opera_raw = (
            _opera_value(opera, str(definition["opera_column"]))
            if definition["opera_column"]
            else None
        )
        opera_numeric = _float(opera_raw)
        pubchem_display = definition.get("pubchem_display")
        pubchem_numeric = definition.get("pubchem_numeric")

        if not any(
            _display(value)
            for value in (cas_display, pubchem_display, opera_raw)
        ):
            continue

        status, row_warnings = _comparison_status(
            [
                ("CAS", cas_numeric),
                ("PubChem", pubchem_numeric),
                ("OPERA", opera_numeric),
            ],
            threshold=definition["threshold"],
            property_name=str(definition["name"]),
            unit=str(definition["unit"]),
        )
        if definition["name"] == "LogP" and any(
            "logp disagreement" in str(existing).casefold() for existing in warnings
        ):
            row_warnings = []
        warnings.extend(row_warnings)
        rows.append(
            {
                "Property": definition["name"],
                "CAS Common Chemistry (experimental)": _display(cas_display),
                "PubChem": _display(pubchem_display),
                "OPERA (predicted)": _display(opera_raw),
                "Comparison unit": definition["unit"],
                "QA status": status,
            }
        )

    # Preserve useful source-only properties without pretending units are comparable.
    for name, pubchem_key, unit in (
        ("Flash point", "flash_point", "as reported"),
        ("Vapor pressure", "vapor_pressure", "as reported"),
    ):
        value = pubchem.get(pubchem_key)
        if _display(value):
            rows.append(
                {
                    "Property": name,
                    "CAS Common Chemistry (experimental)": "",
                    "PubChem": _display(value),
                    "OPERA (predicted)": "",
                    "Comparison unit": unit,
                    "QA status": "Single-source evidence",
                }
            )

    # De-duplicate warnings already supplied by OPERA and generated here.
    unique_warnings: list[str] = []
    for warning in warnings:
        text = str(warning).strip()
        if text and text not in unique_warnings:
            unique_warnings.append(text)
    return {
        "version": COMPARISON_VERSION,
        "rows": rows,
        "warnings": unique_warnings,
    }
