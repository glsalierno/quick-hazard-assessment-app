from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from utils import opera_batch


def test_clarify_opera_endpoint():
    assert "atom" in opera_batch.clarify_opera_endpoint("nbAtoms").lower()
    assert "LogP_pred" in opera_batch.clarify_opera_endpoint("LogP_predRange")


def test_get_all_opera_endpoints():
    eps = opera_batch.get_all_opera_endpoints()
    assert isinstance(eps, list)
    assert any(e.get("name") == "LogP_pred" for e in eps)
    logp = next(e for e in eps if e.get("name") == "LogP_pred")
    assert logp.get("label")


def test_build_endpoints_json_from_columns():
    meta = opera_batch.build_endpoints_json_from_columns(["LogP_pred", "UnknownCol"])
    assert meta[0]["name"] == "LogP_pred"
    assert "log" in meta[0]["description"].lower() or "octanol" in meta[0]["description"].lower()
    assert meta[1]["description"] == "OPERA output column"


def test_smiles_file_hash_stable():
    a = opera_batch._smiles_file_hash([("CC", "1"), ("CO", "2")])
    b = opera_batch._smiles_file_hash([("CC", "1"), ("CO", "2")])
    assert a == b
    assert len(a) == 64


def test_cas_to_smiles_mock(monkeypatch):
    calls: list[str] = []

    def fake_get(url: str, timeout: float = 45.0, max_retries: int = 4):
        calls.append(url)
        r = MagicMock()
        if "67-64-1" in url:
            r.status_code = 200
            r.text = "CC(=O)C\n"
        else:
            r.status_code = 404
        return r

    monkeypatch.setattr(opera_batch, "_pubchem_get", fake_get)
    monkeypatch.setattr(opera_batch.time, "sleep", lambda _x: None)
    out = opera_batch.cas_to_smiles(["67-64-1", "99-99-9"])
    assert "67-64-1" in out
    assert out["67-64-1"] == "CC(=O)C"
    assert out.get("99-99-9") is None


def test_smiles_from_cas_batch_mock(monkeypatch):
    def fake_cid(cas: str):
        if cas == "67-64-1":
            return 180
        return None

    monkeypatch.setattr(opera_batch, "_cas_to_primary_cid", fake_cid)
    monkeypatch.setattr(opera_batch.time, "sleep", lambda _x: None)

    def fake_post(url, data=None, timeout=120, headers=None):
        r = MagicMock()
        r.ok = True
        r.text = "CID,CanonicalSMILES\n180,CC(=O)C\n"
        return r

    monkeypatch.setattr(opera_batch.requests, "post", fake_post)
    out = opera_batch.smiles_from_cas_batch(["67-64-1"])
    assert out["67-64-1"] == "CC(=O)C"


def test_batch_predict_mock_run(monkeypatch, tmp_path: Path):
    def fake_run(smi_path: Path, out_csv: Path, **kwargs):
        out_csv.write_text(
            "MoleculeID,LogP_pred\nm1,1.5\n",
            encoding="utf-8",
        )
        return {
            "ok": True,
            "error": None,
            "rows": [{"MoleculeID": "m1", "LogP_pred": "1.5"}],
            "exe": r"C:\fake\OPERA.exe",
            "stdout": "",
            "stderr": "",
        }

    monkeypatch.setattr(opera_batch.opera_client, "run_opera_on_paths", fake_run)
    br = opera_batch.batch_predict(
        ["CC"],
        molecule_ids=["m1"],
        disk_cache=False,
        sqlite_cache_path=None,
        fallback_single_on_error=False,
    )
    assert not br.df.empty
    assert br.df.iloc[0]["LogP_pred"] == "1.5"
    assert "CC" in br.by_smiles


def test_predict_opera_batch(monkeypatch):
    _df = pd.DataFrame([{"input_smiles": "CC", "molecule_id": "x", "LogP_pred": "2.0"}])

    class BR:
        df = _df
        by_smiles = {"CC": {"LogP_pred": "2.0", "input_smiles": "CC"}}
        errors: list[str] = []

    monkeypatch.setattr(opera_batch, "batch_predict", lambda *a, **k: BR())
    from utils import opera_client

    out = opera_client.predict_opera(["CC"], "logp")
    assert out["CC"]["ok"] is True
    assert out["CC"]["value"] == "2.0"


def test_opera_cas_cache_roundtrip(tmp_path: Path):
    db = tmp_path / "c.sqlite"
    c = opera_batch.OperaCasCache(db)
    payload = {"LogP_pred": "1.2", "MoleculeID": "50-78-2"}
    c.put_row("50-78-2", "CCO", payload)
    got = c.get_row("50-78-2", "CCO")
    assert got is not None
    assert got.get("LogP_pred") == "1.2"
