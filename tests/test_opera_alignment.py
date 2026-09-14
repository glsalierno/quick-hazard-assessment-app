"""OPERA column-name parsing and row identity checks (GHhaz6)."""

from __future__ import annotations

import csv
from pathlib import Path

from utils.opera_client import (
    check_opera_pubchem_logp,
    select_opera_row_for_input,
    _read_all_csv_rows,
)


def test_read_csv_by_column_name_not_position(tmp_path: Path):
    # Shuffled column order relative to a "canonical" layout
    p = tmp_path / "opera_out.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["LogP_pred", "MoleculeID", "MP_pred", "Canonical_SMILES"],
        )
        w.writeheader()
        w.writerow(
            {
                "LogP_pred": "1.23",
                "MoleculeID": "50-78-2",
                "MP_pred": "135",
                "Canonical_SMILES": "CC(=O)Oc1ccccc1C(=O)O",
            }
        )
    rows = _read_all_csv_rows(p)
    assert rows[0]["LogP_pred"] == "1.23"
    assert rows[0]["MoleculeID"] == "50-78-2"


def test_select_row_matches_molecule_id():
    rows = [
        {"MoleculeID": "other", "LogP_pred": "9.9"},
        {"MoleculeID": "381-73-7", "LogP_pred": "0.5"},
    ]
    row, warnings = select_opera_row_for_input(rows, molecule_id="381-73-7")
    assert row["LogP_pred"] == "0.5"
    assert not any("mismatch" in w for w in warnings)


def test_mismatch_flags_warning():
    rows = [{"MoleculeID": "wrong", "LogP_pred": "1.0"}]
    row, warnings = select_opera_row_for_input(rows, molecule_id="381-73-7")
    assert row["LogP_pred"] == "1.0"
    assert any("mismatch" in w for w in warnings)


def test_logp_disagreement_warning():
    warnings = check_opera_pubchem_logp({"LogP_pred": "5.0"}, pubchem_xlogp=1.0)
    assert warnings
    assert "disagreement" in warnings[0].lower()


def test_logp_agreement_no_warning():
    warnings = check_opera_pubchem_logp({"LogP_pred": "1.2"}, pubchem_xlogp=1.5)
    assert warnings == []
