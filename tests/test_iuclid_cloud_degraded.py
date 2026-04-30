"""REACH / IUCLID: archive status and unconfigured UI helpers (no Streamlit runtime required for status)."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def test_offline_reach_archive_status_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OFFLINE_LOCAL_ARCHIVE", raising=False)
    with patch("unified_hazard_report.iuclid_integration.sync_offline_secrets_from_st_secrets", lambda: None):
        from unified_hazard_report.iuclid_integration import offline_reach_archive_status

        ok, code = offline_reach_archive_status()
    assert ok is False
    assert code == "unset"


def test_offline_reach_archive_status_missing_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing = tmp_path / "no_such_reach_archive.zip"
    monkeypatch.setenv("OFFLINE_LOCAL_ARCHIVE", str(missing))
    with patch("unified_hazard_report.iuclid_integration.sync_offline_secrets_from_st_secrets", lambda: None):
        from unified_hazard_report.iuclid_integration import offline_reach_archive_status

        ok, code = offline_reach_archive_status()
    assert ok is False
    assert code == "missing"


def test_offline_reach_archive_status_ok(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    archive = tmp_path / "fake.zip"
    archive.write_bytes(b"PK\x05\x06" + b"\x00" * 18)  # minimal zip-like bytes; exists() is enough
    monkeypatch.setenv("OFFLINE_LOCAL_ARCHIVE", str(archive))
    with patch("unified_hazard_report.iuclid_integration.sync_offline_secrets_from_st_secrets", lambda: None):
        from unified_hazard_report.iuclid_integration import offline_reach_archive_status

        ok, code = offline_reach_archive_status()
    assert ok is True
    assert code == "ok"


def test_render_reach_iuclid_panel_unconfigured_unset_no_crash() -> None:
    """Smoke-test Streamlit calls used by the unconfigured panel (mocked)."""
    from unified_hazard_report import iuclid_integration as iu

    mock_st = MagicMock()
    mock_exp = MagicMock()
    mock_st.expander.return_value.__enter__ = MagicMock(return_value=None)
    mock_st.expander.return_value.__exit__ = MagicMock(return_value=None)

    with patch.object(iu, "st", mock_st):
        iu.render_reach_iuclid_panel_unconfigured("unset")

    mock_st.expander.assert_called_once()
    mock_st.info.assert_called_once()


def test_render_reach_iuclid_panel_unconfigured_missing_no_crash() -> None:
    from unified_hazard_report import iuclid_integration as iu

    mock_st = MagicMock()
    mock_st.expander.return_value.__enter__ = MagicMock(return_value=None)
    mock_st.expander.return_value.__exit__ = MagicMock(return_value=None)

    with patch.object(iu, "st", mock_st):
        iu.render_reach_iuclid_panel_unconfigured("missing")

    mock_st.warning.assert_called_once()


def test_apply_repo_iuclid_defaults_sets_env_on_cloud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import unified_hazard_report.iuclid_integration as iu

    monkeypatch.delenv("OFFLINE_LOCAL_ARCHIVE", raising=False)
    monkeypatch.delenv("IUCLID_FORMAT_DIR", raising=False)
    monkeypatch.delenv("HAZQUERY_DISABLE_REPO_IUCLID_DEFAULTS", raising=False)

    (tmp_path / "data" / "reach_demo").mkdir(parents=True)
    (tmp_path / "data" / "reach_demo" / "reach_subset.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    fmt = tmp_path / "data" / "iuclid_format" / "IUCLID_6_9_0_0_format"
    fmt.mkdir(parents=True)
    (fmt / "marker.txt").write_text("ok", encoding="utf-8")

    monkeypatch.setattr(iu, "_APP_ROOT", tmp_path)

    with patch("services.config.ServiceConfig.is_streamlit_cloud", return_value=True):
        iu.apply_repo_iuclid_defaults_for_streamlit_cloud()

    assert os.environ.get("OFFLINE_LOCAL_ARCHIVE") == str((tmp_path / "data" / "reach_demo" / "reach_subset.zip").resolve())
    assert os.environ.get("IUCLID_FORMAT_DIR") == str(fmt.resolve())


def test_apply_repo_iuclid_defaults_skips_when_not_cloud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import unified_hazard_report.iuclid_integration as iu

    monkeypatch.delenv("OFFLINE_LOCAL_ARCHIVE", raising=False)
    monkeypatch.delenv("IUCLID_FORMAT_DIR", raising=False)
    (tmp_path / "data" / "reach_demo" / "reach_subset.zip").parent.mkdir(parents=True)
    (tmp_path / "data" / "reach_demo" / "reach_subset.zip").write_bytes(b"x")
    monkeypatch.setattr(iu, "_APP_ROOT", tmp_path)

    with patch("services.config.ServiceConfig.is_streamlit_cloud", return_value=False):
        iu.apply_repo_iuclid_defaults_for_streamlit_cloud()

    assert os.environ.get("OFFLINE_LOCAL_ARCHIVE") in (None, "")


def test_using_committed_reach_demo_archive_true(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import unified_hazard_report.iuclid_integration as iu

    z = tmp_path / "data" / "reach_demo" / "reach_subset.zip"
    z.parent.mkdir(parents=True)
    z.write_bytes(b"x")
    monkeypatch.setattr(iu, "_APP_ROOT", tmp_path)
    monkeypatch.setenv("OFFLINE_LOCAL_ARCHIVE", str(z.resolve()))

    assert iu.using_committed_reach_demo_archive() is True
    assert iu.committed_reach_demo_zip_path() == z.resolve()


def test_using_committed_reach_demo_archive_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import unified_hazard_report.iuclid_integration as iu

    monkeypatch.setattr(iu, "_APP_ROOT", tmp_path)
    other = tmp_path / "other.zip"
    other.write_bytes(b"y")
    monkeypatch.setenv("OFFLINE_LOCAL_ARCHIVE", str(other))

    assert iu.using_committed_reach_demo_archive() is False
