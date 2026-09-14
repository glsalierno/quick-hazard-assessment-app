"""Unit tests for ECOSAR → P2OASys aquatic gap-fill (mocked API)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from utils import ecosar_client, p2oasys_scorer, p2oasys_source_bridges


@pytest.fixture()
def benzene_ecosar_raw() -> dict:
    return {
        "parameters": {"cas": "000071-43-2", "smiles": "c1ccccc1"},
        "ecosar": {
            "alerts": [],
            "modelResults": [
                {
                    "qsarClass": "Neutral Organics",
                    "organism": "Fish",
                    "duration": "96-hr",
                    "endpoint": "LC50",
                    "concentration": 49.04,
                    "maxLogKow": 5.0,
                    "flags": [],
                },
                {
                    "qsarClass": "Neutral Organics",
                    "organism": "Daphnid",
                    "duration": "48-hr",
                    "endpoint": "LC50",
                    "concentration": 28.18,
                    "maxLogKow": 5.0,
                    "flags": [],
                },
                {
                    "qsarClass": "Neutral Organics",
                    "organism": "Green Algae",
                    "duration": "96-hr",
                    "endpoint": "EC50",
                    "concentration": 22.06,
                    "maxLogKow": 5.0,
                    "flags": [],
                },
                {
                    "qsarClass": "Neutral Organics",
                    "organism": "Fish",
                    "duration": "ChV",
                    "endpoint": "ChV",
                    "concentration": 2.84,
                    "maxLogKow": 5.0,
                    "flags": [],
                },
                {
                    "qsarClass": "Neutral Organics",
                    "organism": "Earthworm",
                    "duration": "14-d",
                    "endpoint": "LC50",
                    "concentration": 0.01,
                    "maxLogKow": 5.0,
                    "flags": [],
                },
            ],
        },
    }


def test_package_conservative_min_excludes_earthworm(benzene_ecosar_raw):
    pkg = ecosar_client._package_result(benzene_ecosar_raw, cas="71-43-2")
    assert pkg["ok"]
    assert pkg["acute_mg_l"] == pytest.approx(22.06)
    assert pkg["chronic_chv_mg_l"] == pytest.approx(2.84)
    assert all(r["organism"] != "Earthworm" for r in pkg["rows"])


def test_gap_fill_skipped_when_aquatic_present(benzene_ecosar_raw):
    pkg = ecosar_client._package_result(benzene_ecosar_raw, cas="71-43-2")
    existing = {
        "toxicities": [{"value": "LC50 1.5 mg/L (fish)", "source": "IUCLID"}],
    }
    xs = ecosar_client.ecosar_to_extra_sources(pkg, existing_hazard=existing)
    assert xs == {}


def test_bridge_emits_lc50_for_scorer(benzene_ecosar_raw):
    pkg = ecosar_client._package_result(benzene_ecosar_raw, cas="71-43-2")
    xs = p2oasys_source_bridges.ecosar_to_extra_sources(pkg, existing_hazard={})
    assert xs.get("lc50_aquatic_mg_l") == pytest.approx(22.06)
    assert any("ECOSAR" in str(t.get("source")) for t in xs.get("toxicities") or [])
    hd = {
        "ghs": {"h_codes": []},
        "toxicities": list(xs["toxicities"]),
        "hazard_metrics": {},
        "lc50_aquatic_mg_l": xs["lc50_aquatic_mg_l"],
    }
    assert p2oasys_scorer._extract_lc50_aquatic(hd) == pytest.approx(22.06)


def test_submit_uses_cache(tmp_path, benzene_ecosar_raw, monkeypatch):
    db = tmp_path / "ecosar_cache.sqlite"
    monkeypatch.setenv("ECOSAR_CACHE_DB_PATH", str(db))
    monkeypatch.delenv("HAZQUERY_SKIP_ECOSAR", raising=False)
    # Avoid config.ECOSAR_CACHE_DB_PATH winning if env is ignored somehow.
    import config as _cfg

    monkeypatch.setattr(_cfg, "ECOSAR_CACHE_DB_PATH", str(db), raising=False)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = benzene_ecosar_raw

    with patch("utils.ecosar_client.requests.get", return_value=mock_resp) as get_mock:
        first = ecosar_client.submit_organics(cas="71-43-2", use_cache=True)
        assert first["ok"]
        assert db.is_file(), "cache db should exist after first fetch"
        second = ecosar_client.submit_organics(cas="71-43-2", use_cache=True)
    assert second["ok"]
    assert get_mock.call_count == 1
    assert second.get("from_cache") is True
    assert second.get("acute_mg_l") == pytest.approx(22.06)

def test_skip_env_disables(monkeypatch):
    monkeypatch.setenv("HAZQUERY_SKIP_ECOSAR", "1")
    assert ecosar_client.is_ecosar_enabled() is False
    assert ecosar_client.submit_organics(cas="71-43-2")["error"] == "ecosar_disabled"
