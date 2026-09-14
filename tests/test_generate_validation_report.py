from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from utils.opera_precompute_cache import init_db, put_cas_row


def test_generate_validation_report_smoke(tmp_path: Path) -> None:
    chem = tmp_path / "chem.sqlite"
    con = sqlite3.connect(chem)
    con.execute("CREATE TABLE toxvaldb (casrn TEXT, study_type TEXT, toxval_numeric REAL)")
    cache = tmp_path / "opera.sqlite"
    init_db(cache)
    for i in range(15):
        cas = f"50-00-{i:02d}"
        con.execute("INSERT INTO toxvaldb VALUES (?,?,?)", (cas, "acute", float(50 + i)))
        put_cas_row(cache, cas, "C", {"LogP_pred": str(1.0 + i * 0.1), "input_smiles": "C"})
    con.commit()
    con.close()

    mapping = tmp_path / "map.json"
    mapping.write_text(
        json.dumps({"mapping": {"LogP_pred": {"toxval_types": ["acute"]}}}),
        encoding="utf-8",
    )
    outd = tmp_path / "out"
    outd.mkdir()
    script = Path(__file__).resolve().parents[1] / "scripts" / "generate_validation_report.py"
    r = subprocess.run(
        [
            sys.executable,
            str(script),
            "--toxval-db",
            str(chem),
            "--opera-cache",
            str(cache),
            "--mapping-json",
            str(mapping),
            "--endpoints",
            "LogP_pred",
            "--min-pairs",
            "10",
            "--output-dir",
            str(outd),
            "--max-plots",
            "1",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert r.returncode == 0, r.stdout + r.stderr
    assert (outd / "validation_summary.csv").is_file()
    assert (outd / "validation_report.md").is_file()
