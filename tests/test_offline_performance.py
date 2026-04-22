"""Offline ECHA i6z parsing: correctness of multiprocessing vs sequential; optional timing smoke."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _write_minimal_i6z(path: Path, stem: str) -> None:
    """Single-substance IUCLID-like ``Document.i6d`` inside a ``.i6z`` zip."""
    sample = f"""<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://iuclid6.echa.europa.eu/namespaces/platform-document/v1">
  <SubstanceName>TestChem-{stem}</SubstanceName>
  <ECNumber>200-753-7</ECNumber>
  <CASNumber>71-43-2</CASNumber>
  <GHSClassification><HazardStatementCode>H314</HazardStatementCode></GHSClassification>
</Root>
""".encode("utf-8")
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("Document.i6d", sample)


@pytest.fixture
def i6z_folder(tmp_path: Path) -> Path:
    d = tmp_path / "i6z_batch"
    d.mkdir()
    for i in range(8):
        stem = f"batch-{i:03d}"
        _write_minimal_i6z(d / f"{stem}.i6z", stem)
    return d


def test_extract_i6z_metadata_mp_matches_sequential(i6z_folder: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ingest import offline_echa_loader as ob

    data_dir = i6z_folder.parent / "offline_data"
    monkeypatch.setenv("OFFLINE_I6Z_MAX_WORKERS", "1")
    df1, cl1 = ob.extract_i6z_metadata(i6z_folder, data_dir)
    monkeypatch.setenv("OFFLINE_I6Z_MAX_WORKERS", "4")
    df2, cl2 = ob.extract_i6z_metadata(i6z_folder, data_dir)

    assert len(df1) == len(df2) == 8
    assert set(df1["uuid"].astype(str)) == set(df2["uuid"].astype(str))
    assert len(cl1) == len(cl2)


def test_collect_parsed_i6z_rows_mp_matches_sequential_many_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parallel ``_collect_parsed_i6z_rows`` must agree with sequential (spawn overhead dominates tiny zips)."""
    from ingest import offline_echa_loader as ob

    d = tmp_path / "many"
    d.mkdir()
    for i in range(16):
        _write_minimal_i6z(d / f"perf-{i:03d}.i6z", str(i))

    paths = sorted(d.glob("*.i6z"))
    monkeypatch.setenv("OFFLINE_I6Z_MAX_WORKERS", "1")
    r1, c1 = ob._collect_parsed_i6z_rows(paths, "seq")

    monkeypatch.setenv("OFFLINE_I6Z_MAX_WORKERS", "4")
    r2, c2 = ob._collect_parsed_i6z_rows(paths, "mp")

    u1 = {str(r.get("uuid")) for r in r1}
    u2 = {str(r.get("uuid")) for r in r2}
    assert u1 == u2
    assert len(r1) == len(r2) == 16
    assert len(c1) == len(c2)
