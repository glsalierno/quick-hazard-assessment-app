"""reach_subset_examples.json loader for main-page REACH demo buttons."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_load_reach_demo_example_chemicals_empty_when_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import utils.reach_demo_examples as rde

    monkeypatch.setenv("REACH_DEMO_EXAMPLES_JSON", str(tmp_path / "nope.json"))
    assert rde.load_reach_demo_example_chemicals(max_n=6) == []


def test_load_reach_demo_example_chemicals_reads_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import utils.reach_demo_examples as rde

    p = tmp_path / "reach_subset_examples.json"
    p.write_text(
        json.dumps(
            [
                {"cas": "67-64-1", "name": "Acetone"},
                {"cas": "64-17-5", "name": "Ethanol"},
                {"cas": "67-64-1", "name": "Dup"},
            ],
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("REACH_DEMO_EXAMPLES_JSON", str(p))
    out = rde.load_reach_demo_example_chemicals(max_n=6)
    assert len(out) == 2
    assert out[0][0] == "67-64-1"
    assert "Acetone" in out[0][1]
    assert out[1][0] == "64-17-5"
