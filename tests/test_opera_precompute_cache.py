from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.opera_precompute_cache import clear_cache, get_opera_value, init_db, is_cas_cached, put_batch_dataframe, put_cas_row


def test_put_get_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "pc.sqlite"
    init_db(db)
    put_cas_row(
        db,
        "64-17-5",
        "CCO",
        {"LogP_pred": "0.1", "MP_pred": "10", "input_smiles": "CCO"},
    )
    assert is_cas_cached(db, "64-17-5", "CCO")
    assert get_opera_value(db, "64-17-5", "LogP_pred") == "0.1"


def test_clear_cache(tmp_path: Path) -> None:
    db = tmp_path / "del.sqlite"
    init_db(db)
    assert db.is_file()
    assert clear_cache(db) is True
    assert not db.is_file()
    assert clear_cache(db) is False


def test_put_batch_dataframe(tmp_path: Path) -> None:
    db = tmp_path / "pc2.sqlite"
    init_db(db)
    df = pd.DataFrame(
        [
            {"_opera_cache_cas": "50-00-0", "input_smiles": "CC", "LogP_pred": "1.23"},
        ]
    )
    n = put_batch_dataframe(db, df)
    assert n == 1
    assert get_opera_value(db, "50-00-0", "LogP_pred") == "1.23"
