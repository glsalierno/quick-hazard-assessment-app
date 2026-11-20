"""Exercise PubChem and DSSTox paths without network or checked-in data files."""

from unittest.mock import Mock

import requests

from utils import diagnostics, dsstox_local, pubchem_client

LFS_CONTENT = "version https://git-lfs.github.com/spec/v1\noid sha256:example\nsize 42\n"


def test_pubchem_cas_lookup(monkeypatch):
    http = Mock()
    http.json.return_value = {"IdentifierList": {"CID": [180]}}
    get = Mock(return_value=http)
    monkeypatch.setattr(pubchem_client.requests, "get", get)
    monkeypatch.setattr(pubchem_client.time, "sleep", lambda _: None)

    assert pubchem_client.get_cid("67-64-1", "cas") == 180
    get.assert_called_once_with(
        f"{pubchem_client.PUBCHEM_BASE}/compound/xref/RegistryID/67-64-1/cids/JSON",
        timeout=30,
    )


def test_pubchem_transport_failure(monkeypatch, caplog):
    monkeypatch.setattr(pubchem_client.requests, "get", Mock(side_effect=requests.Timeout))
    monkeypatch.setattr(pubchem_client.time, "sleep", lambda _: None)

    assert pubchem_client.get_cid("67-64-1", "cas") is None
    assert "67-64-1" in caplog.text and "Timeout" in caplog.text


def test_dsstox_loads_fixture_and_skips_pointer(monkeypatch, tmp_path):
    csv_file = tmp_path / "mapping.csv"
    csv_file.write_text("CASRN,DTXSID,PREFERRED_NAME\n67-64-1,DTXSID8021482,Acetone\n")
    pointer = tmp_path / "not_downloaded.csv"
    pointer.write_text(LFS_CONTENT)
    monkeypatch.setattr(dsstox_local, "_find_mapping_files", lambda: [str(pointer), str(csv_file)])
    dsstox_local.load_dsstox_enhanced.clear()
    try:
        result = dsstox_local.load_dsstox_enhanced()
        assert dsstox_local.get_dtxsid("67-64-1", result) == "DTXSID8021482"
        assert len(result) == 1
    finally:
        dsstox_local.load_dsstox_enhanced.clear()


def test_artifact_status_for_present_missing_and_lfs_pointer(tmp_path):
    artifact = tmp_path / "chemical_db.sqlite"
    assert diagnostics.artifact_status(artifact) == "missing"
    artifact.write_text(LFS_CONTENT)
    assert diagnostics.artifact_status(artifact) == "Git LFS pointer (data not downloaded)"
    artifact.write_bytes(b"SQLite format 3\x00")
    assert diagnostics.artifact_status(artifact) == "present"
