from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_run_one_batch_mocked(tmp_path: Path, monkeypatch) -> None:
    from utils import opera_batch
    from utils.opera_precompute_worker import run_one_batch

    class FakeResult:
        df = pd.DataFrame(
            [
                {
                    "_opera_cache_cas": "99-99-9",
                    "input_smiles": "C",
                    "LogP_pred": "1.5",
                }
            ]
        )
        errors: list[str] = []

    monkeypatch.setattr(opera_batch, "batch_predict", lambda *a, **k: FakeResult())

    db = tmp_path / "cache.sqlite"
    res = run_one_batch((str(db), ["99-99-9"], ["C"], ["mid0"], "worker-test"))
    assert res["written"] == 1
    assert db.is_file()
