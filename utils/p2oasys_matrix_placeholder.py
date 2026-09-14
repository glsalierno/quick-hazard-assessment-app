"""
Dev fallback when the official TURI P2OASys hazard matrix Excel is not installed.

Writes ``p2oasys_matrix_dev_placeholder.xlsx`` under ``data/`` with the minimal sheet
names and row shapes expected by ``p2oasys_scorer.load_p2oasys_matrix`` / ``compute_p2oasys_scores``.
Scores are **not** calibrated to the real TURI matrix — replace with the official workbook
from https://p2oasys.turi.org/chemical/hazard-score-matrix when available.

Set ``P2OASYS_DISABLE_AUTO_PLACEHOLDER=1`` to skip generation and keep the UI message
when the configured matrix path is missing.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Literal

import pandas as pd

logger = logging.getLogger(__name__)

MatrixSource = Literal["official", "placeholder", "missing"]


def _write_minimal_workbook(path: Path) -> None:
    """
    Build a multi-sheet xlsx matching ``SHEET_CATEGORIES`` keys in ``p2oasys_scorer``.

    Each sheet: column A = subcategory headers (rows without B–F values) or feature names;
    columns B–F = thresholds / GHS text for P2OASys score columns 2, 4, 6, 8, 10.
    """
    # Import locally to avoid circular imports at package load.
    from utils import p2oasys_scorer as _sc

    sheet_specs: list[tuple[str, list[list]]] = []

    # --- Acute (sheet name "Acute" or "Acute " both map to Acute Human Effects) ---
    acute_rows: list[list] = [
        ["Oral Toxicity", "", "", "", "", ""],
        ["LD50 (mg/kg)", 5000, 2000, 500, 50, 5],
        ["GHS H Phrases (Oral)", "H302", "H301", "H300", "", ""],
        ["Dermal Toxicity", "", "", "", "", ""],
        ["LD50 (mg/kg)", 5000, 2000, 500, 50, 5],
        ["Inhalation Toxicity", "", "", "", "", ""],
        ["LC50 (ppm)", 20000, 5000, 500, 100, 50],
    ]
    sheet_specs.append(("Acute", acute_rows))

    chronic_rows: list[list] = [
        ["Carcinogenicity", "", "", "", "", ""],
        ["IARC Category", "4", "3", "2B", "2A", "1"],
    ]
    sheet_specs.append(("Chronic", chronic_rows))

    eco_rows: list[list] = [
        ["Aquatic toxicity", "", "", "", "", ""],
        ["LC50 (mg/L)", 100, 10, 1, 0.1, 0.01],
    ]
    sheet_specs.append(("Ecological Hazards", eco_rows))

    fate_rows: list[list] = [
        ["Persistence (placeholder)", "", "", "", "", ""],
        ["KEY PHRASE half-life", "persistent", "moderate", "short", "", ""],
    ]
    sheet_specs.append(("Environmental Fate & Transport", fate_rows))

    atmo_rows: list[list] = [
        ["GWP placeholder", "", "", "", "", ""],
        ["KEY PHRASE GWP", "low", "medium", "high", "", ""],
    ]
    sheet_specs.append(("Atmospheric Hazard", atmo_rows))

    phys_rows: list[list] = [
        ["Flammability", "", "", "", "", ""],
        ["Flash point (deg C)", 93, 60, 37, 23, -20],
        ["Vapor pressure (mm Hg)", 0.1, 1, 10, 100, 760],
        ["Health and irritation", "", "", "", "", ""],
        ["NFPA Health (0-4)", 0, 1, 2, 3, 4],
        ["Fire / Flammability", "", "", "", "", ""],
        ["NFPA Fire (0-4)", 0, 1, 2, 3, 4],
    ]
    sheet_specs.append(("Physical Hazard", phys_rows))

    # Minimal non-empty sheets so ExcelFile has expected tabs (parser skips unknown structure).
    proc_rows: list[list] = [["Process (placeholder — see official matrix)", "", "", "", "", ""]]
    sheet_specs.append(("Process Factors", proc_rows))
    life_rows: list[list] = [["Life cycle (placeholder — see official matrix)", "", "", "", "", ""]]
    sheet_specs.append(("Life Cycle Factors", life_rows))

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, rows in sheet_specs:
            pd.DataFrame(rows).to_excel(writer, sheet_name=sheet_name[:31], header=False, index=False)

    # Smoke-parse so a bad layout fails at write time, not first score run.
    _ = _sc.load_p2oasys_matrix(path)
    logger.info("Wrote P2OASys dev placeholder matrix to %s", path)


def resolve_p2oasys_matrix_path(configured_path: Path, data_dir: Path) -> tuple[Path, MatrixSource]:
    """
    Return the matrix path to load and whether it is the official file or a generated placeholder.

    - **official**: ``configured_path`` exists (``P2OASYS_MATRIX_PATH`` or default under ``data/``).
    - **placeholder**: official missing, auto-placeholder allowed, wrote/used dev xlsx under ``data_dir``.
    - **missing**: official missing and placeholder disabled or write/parse failed.
    """
    try:
        configured_path = configured_path.expanduser().resolve()
    except OSError:
        pass
    if configured_path.is_file():
        return configured_path, "official"

    disable = os.environ.get("P2OASYS_DISABLE_AUTO_PLACEHOLDER", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if disable:
        return configured_path, "missing"

    try:
        import config as app_config

        name = getattr(app_config, "P2OASYS_PLACEHOLDER_MATRIX_FILENAME", "p2oasys_matrix_dev_placeholder.xlsx")
    except Exception:
        name = "p2oasys_matrix_dev_placeholder.xlsx"

    try:
        data_dir = data_dir.expanduser().resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        placeholder_path = (data_dir / name).resolve()
        if not placeholder_path.is_file():
            _write_minimal_workbook(placeholder_path)
        elif not placeholder_path.stat().st_size:
            _write_minimal_workbook(placeholder_path)
        return placeholder_path, "placeholder"
    except Exception as exc:
        logger.exception("Could not create P2OASys placeholder matrix: %s", exc)
        return configured_path, "missing"
