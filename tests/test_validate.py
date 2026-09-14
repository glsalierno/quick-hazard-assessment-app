from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from utils import validate


def test_get_pubchem_experimental_mock(monkeypatch):
    monkeypatch.setattr(validate, "_disk_cache_get", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_disk_cache_set", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_cas_to_cid", lambda cas: 180 if cas == "64-17-5" else None)

    def fake_req(method, url, **kwargs):
        r = MagicMock()
        r.status_code = 200
        if "property/XLogP" in url:
            r.json = lambda: {"PropertyTable": {"Properties": [{"XLogP": -0.31}]}}
        else:
            r.json = lambda: {}
        return r

    monkeypatch.setattr(validate, "_request_with_backoff", fake_req)
    out = validate.get_pubchem_experimental("64-17-5", "LogP_pred")
    assert out["source"] == "PubChem"
    assert out["value"] in ("-0.31", "-0.3")


def test_get_epa_toxval_local_mock(monkeypatch):
    monkeypatch.setattr(validate, "_disk_cache_get", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_disk_cache_set", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_get_epa_api_key", lambda: None)

    fake_rows = [
        {
            "study_type": "acute_oral_ld50",
            "toxval_numeric": 5000.0,
            "toxval_units": "mg/kg",
            "reference": "test",
        }
    ]

    monkeypatch.setattr(
        "utils.chemical_db.get_toxicity_by_cas",
        lambda cas, numeric_only=True: fake_rows if cas == "50-78-2" else [],
    )
    out = validate.get_epa_toxval("50-78-2", "CATMoS_LD50_pred")
    assert "SQLite" in (out.get("source") or "")
    assert out.get("value") == "5000.0"


def test_get_epa_toxval_api_mock(monkeypatch):
    monkeypatch.setattr(validate, "_disk_cache_get", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_disk_cache_set", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_get_epa_api_key", lambda: "fake-key")
    monkeypatch.setattr("utils.chemical_db.get_toxicity_by_cas", lambda cas, numeric_only=True: [])

    monkeypatch.setattr(
        "utils.chemical_db.get_dsstox_by_cas",
        lambda cas: {"dtxsid": "DTXSID6026292"} if cas == "50-78-2" else None,
    )

    def fake_fetch(dtxsid, api_key=None):
        return {
            "acute_toxicity": [
                {
                    "study_type": "LD50 oral",
                    "toxval_numeric": 200.0,
                    "toxval_units": "mg/kg",
                    "reference": "r1",
                }
            ]
        }

    monkeypatch.setattr("utils.toxvaldb_client.fetch_toxval_data", fake_fetch)
    out = validate.get_epa_toxval("50-78-2", "LD50")
    assert "API" in (out.get("source") or "")
    assert out.get("value") == "200.0"


def test_robots_allowed_no_crash():
    assert validate._robots_allowed("https://example.com/some/path") in (True, False)


def test_load_cas_from_csv(tmp_path: Path):
    p = tmp_path / "t.csv"
    p.write_text("Name,CAS\nx,67-64-1\ny,50-78-2 and 7732-18-5\n", encoding="utf-8")
    cas = validate.load_cas_list_from_p2oasys_csv(p, limit=20)
    assert "67-64-1" in cas
    assert "50-78-2" in cas


def test_cross_validate_smoke(monkeypatch):
    monkeypatch.setattr(validate, "_disk_cache_get", lambda *a, **k: None)
    monkeypatch.setattr(validate, "_disk_cache_set", lambda *a, **k: None)
    monkeypatch.setattr(validate.opera_batch, "smiles_from_cas_batch", lambda cas: {c: "CC" for c in cas})

    def fake_pred(smiles, ep):
        lst = smiles if isinstance(smiles, list) else [smiles]
        return {s: {"ok": True, "value": "1.0", "column": ep} for s in lst}

    monkeypatch.setattr(validate.opera_client, "predict_opera", fake_pred)
    monkeypatch.setattr(
        validate,
        "get_pubchem_experimental",
        lambda c, e: {"source": "P", "value": "", "unit": "", "reference": ""},
    )
    monkeypatch.setattr(
        validate,
        "get_epa_toxval",
        lambda c, e: {"source": "", "value": "", "unit": "", "reference": "", "toxval_type": ""},
    )
    monkeypatch.setattr(
        validate,
        "get_echa_classification",
        lambda c, e: {"classification": "", "h_codes": "", "source_url": "", "note": ""},
    )

    df = validate.cross_validate_cas_list(["67-64-1"], "LogP_pred", use_disk_cache=False, sleep_between_cas=0.0)
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["CAS"] == "67-64-1"
    assert df.iloc[0]["OPERA_value"] == "1.0"
