"""Unit tests for HSPiP expand helpers (no real HSPiP.exe required)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from v7.hspip_expand import (
    _append_cache_row,
    expand_hsp_for_cas_list,
    hsp_cache_path,
    resolve_hspip_dir,
)


def test_resolve_hspip_dir_requires_exe(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        resolve_hspip_dir(str(tmp_path))
    (tmp_path / "HSPiP.exe").write_bytes(b"fake")
    assert resolve_hspip_dir(str(tmp_path)) == tmp_path


def test_append_and_expand_uses_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cache = tmp_path / "hsp_by_cas.csv"
    _append_cache_row(cache, cas="67-56-1", delta_d=15.1, delta_p=12.3, delta_h=22.3, smiles="CO")
    monkeypatch.setattr("v7.hspip_expand.hsp_cache_path", lambda: cache)
    (tmp_path / "HSPiP.exe").write_bytes(b"fake")

    with patch("v7.hspip_expand.run_hspip_for_smiles") as run:
        summary = expand_hsp_for_cas_list(
            ["67-56-1"],
            hspip_dir=str(tmp_path),
            cache_csv=cache,
            skip_existing=True,
        )
        run.assert_not_called()
    assert summary.cached == 1
    assert summary.results[0].delta_d == 15.1


def test_expand_computes_when_missing(tmp_path: Path):
    cache = tmp_path / "hsp_by_cas.csv"
    (tmp_path / "HSPiP.exe").write_bytes(b"fake")

    with patch("v7.hspip_expand.smiles_for_cas", return_value="CO"), patch(
        "v7.hspip_expand.run_hspip_for_smiles",
        return_value={"D": 15.0, "P": 12.0, "H": 22.0, "RER": 1.1},
    ):
        summary = expand_hsp_for_cas_list(
            ["67-56-1"],
            hspip_dir=str(tmp_path),
            cache_csv=cache,
            skip_existing=True,
        )
    assert summary.computed == 1
    assert cache.is_file()
    text = cache.read_text(encoding="utf-8")
    assert "67-56-1" in text and "15.0" in text
