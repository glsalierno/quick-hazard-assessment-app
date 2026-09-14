from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from utils.opera_mapper import OperaEndpointMapper


def test_mapper_builds_mapping_from_sqlite(tmp_path: Path) -> None:
    endpoints = {
        "column_names_only": ["LogP_pred", "CATMoS_LD50_pred", "MP_pred"]
    }
    endpoints_path = tmp_path / "opera_endpoints.json"
    endpoints_path.write_text(json.dumps(endpoints), encoding="utf-8")

    db_path = tmp_path / "chem.sqlite"
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE toxvaldb (study_type TEXT, toxval_numeric REAL, casrn TEXT)")
    con.executemany(
        "INSERT INTO toxvaldb (study_type, toxval_numeric, casrn) VALUES (?, ?, ?)",
        [
            ("LogP", 1.2, "50-00-0"),
            ("Oral LD50", 40.0, "50-00-0"),
            ("Melting Point", 110.0, "64-17-5"),
        ],
    )
    con.commit()
    con.close()

    mapper = OperaEndpointMapper(
        endpoints_json_path=endpoints_path,
        toxval_sqlite_path=db_path,
        mapping_output_path=tmp_path / "map.json",
    )
    payload = mapper.build_mapping()
    assert "mapping" in payload
    assert payload["mapping"]["LogP_pred"]["toxval_types"]
    assert payload["mapping"]["CATMoS_LD50_pred"]["toxval_types"]


def test_mapper_save_roundtrip(tmp_path: Path) -> None:
    endpoints = {"column_names_only": ["LogWS_pred"]}
    endpoints_path = tmp_path / "opera_endpoints.json"
    endpoints_path.write_text(json.dumps(endpoints), encoding="utf-8")
    csv_path = tmp_path / "tox.csv"
    csv_path.write_text("toxval_type,toxval_numeric,cas\nWater Solubility,1.0,64-17-5\n", encoding="utf-8")

    mapper = OperaEndpointMapper(
        endpoints_json_path=endpoints_path,
        toxval_csv_path=csv_path,
        mapping_output_path=tmp_path / "out.json",
    )
    out = mapper.build_and_save()
    assert out.is_file()
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert "LogWS_pred" in saved.get("mapping", {})
