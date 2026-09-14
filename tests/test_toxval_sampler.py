from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from utils.toxval_sampler import (
    get_common_cas_across_endpoints,
    load_mapping_dict,
    resolve_opera_endpoint_keys,
    sample_cas_for_endpoints,
)


def test_resolve_ld50_logp():
    m = {"CATMoS_LD50_pred": {"toxval_types": ["acute"]}, "LogP_pred": {"toxval_types": ["x"]}}
    assert "CATMoS_LD50_pred" in resolve_opera_endpoint_keys("LD50", m)
    assert "LogP_pred" in resolve_opera_endpoint_keys("LogP", m)


def test_sample_and_common(tmp_path: Path) -> None:
    db = tmp_path / "tv.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE toxvaldb (casrn TEXT, study_type TEXT, toxval_numeric REAL)")
    con.executemany(
        "INSERT INTO toxvaldb VALUES (?,?,?)",
        [
            ("50-00-0", "acute", 100.0),
            ("64-17-5", "acute", 200.0),
            ("50-00-0", "chronic", 1.0),
        ],
    )
    con.commit()
    con.close()
    mapping = {
        "CATMoS_LD50_pred": {"toxval_types": ["acute"]},
        "BioDeg_LogHalfLife_pred": {"toxval_types": ["chronic"]},
    }
    s = sample_cas_for_endpoints(db, mapping, ["LD50", "BioDeg_LogHalfLife_pred"], n_per_endpoint=10, shuffle=False)
    assert "50-00-0" in s and "64-17-5" in s
    common = get_common_cas_across_endpoints(db, mapping, ["LD50", "BioDeg_LogHalfLife_pred"], pool_limit=100, shuffle=False)
    assert "50-00-0" in common


def test_load_mapping_dict(tmp_path: Path) -> None:
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"mapping": {"LogP_pred": {"toxval_types": ["a"]}}}), encoding="utf-8")
    d = load_mapping_dict(p)
    assert "LogP_pred" in d
