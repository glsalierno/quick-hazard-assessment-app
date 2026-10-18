from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from utils import opera_client


def test_endpoint_to_column_aliases():
    assert opera_client._endpoint_to_column("logp") == "LogP_pred"
    assert opera_client._endpoint_to_column("LD50") == "CATMoS_LD50_pred"
    assert opera_client._endpoint_to_column("LogP_pred") == "LogP_pred"
    assert opera_client._endpoint_to_column("not_a_real_endpoint") is None


def test_applicability_domain_key():
    assert opera_client._applicability_domain_key("LogP_pred") == "AD_LogP"
    assert opera_client._applicability_domain_key("CATMoS_LD50_pred") == "AD_CATMoS"
    assert opera_client._applicability_domain_key("nope") is None


def test_run_opera_empty_smiles():
    out = opera_client.run_opera_for_smiles("  ")
    assert out["ok"] is False
    assert out["error"] == "empty_smiles"


def test_predict_opera_unknown_endpoint():
    r = opera_client.predict_opera("CC", "fake_endpoint_xyz")
    assert r["ok"] is False
    assert "unknown_endpoint" in (r.get("error") or "")


def test_predict_opera_uses_run(monkeypatch):
    def fake_run(smiles: str, molecule_id: str = "query", **kwargs):
        return {
            "ok": True,
            "error": None,
            "row": {"LogP_pred": "1.23", "AD_LogP": "1"},
            "exe": r"C:\fake\OPERA.exe",
            "stdout": "",
            "stderr": "",
        }

    monkeypatch.setattr(opera_client, "run_opera_for_smiles", fake_run)
    r = opera_client.predict_opera("CCO", "logp")
    assert r["ok"] is True
    assert r["value"] == "1.23"
    assert r["applicability_domain"] == "1"


@pytest.mark.skipif(
    not Path(r"C:\Program Files\OPERA\application\OPERA.exe").is_file()
    and not Path(r"C:\Program Files\OPERA\application\OPERA_P.exe").is_file()
    and not (os.environ.get("HAZQUERY_OPERA_EXE") or os.environ.get("OPERA_EXE")),
    reason="OPERA executable not installed on this runner",
)
def test_run_opera_integration_aspirin():
    aspirin = "CC(=O)OC1=CC=CC=C1C(=O)O"
    out = opera_client.run_opera_for_smiles(aspirin, molecule_id="50-78-2", timeout_seconds=600)
    assert out["ok"] is True, out.get("error") or out.get("stderr")
    row = out.get("row") or {}
    assert row.get("LogP_pred") or row.get("MolWeight")
