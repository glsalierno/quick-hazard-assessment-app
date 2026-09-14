"""Exercise ``ingest.qsar_toolbox_loader`` (PyQSARToolbox path) with a small CAS seed list.

Testing checklist (manual):

- [ ] QSAR Toolbox WebSuite / Toolbox Server is running (console shows WebAPI / listening port, e.g. 8804).
- [ ] Dependencies: ``pip install`` PyQSARToolbox (see ``QSAR_PYQSARTOOLBOX_GIT`` / README).
- [ ] From ``quick-hazard-assessment-app``: ``python scripts/test_qsar_ingestion.py``
- [ ] Inspect printed shapes and ``data/qsar_ingestion_test_*.csv``; CAS 50-00-0 often has endpoint/GHS text if in your DB snapshot.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Repo root: quick-hazard-assessment-app
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.chdir(ROOT)

SEED_CAS = ["50-00-0", "67-64-1", "71-43-2"]


def main() -> None:
    from ingest import qsar_toolbox_loader as qb

    port = int(os.getenv("QSAR_TOOLBOX_PORT", "8804"))
    print("QSAR_TOOLBOX_PORT =", port)
    try:
        qb.install_dependencies()
        qs = qb.connect_to_websuite(port=port)
    except ConnectionError as exc:
        print(exc)
        print("Tip: match Toolbox Server **Listening on localhost:** port; try QSAR_SCHEME=https or http.")
        raise SystemExit(1) from exc
    reg, cl = qb.build_dataframes(qs, identifiers=SEED_CAS)

    print("substances_df:", reg.shape)
    print(reg.head())
    print("cl_hazards_df:", cl.shape)
    print(cl.head(10))

    out_dir = Path("data") / "qsar_ingestion_test"
    out_dir.mkdir(parents=True, exist_ok=True)
    reg_path = out_dir / "substances.csv"
    cl_path = out_dir / "cl_hazards.csv"
    reg.to_csv(reg_path, index=False)
    cl.to_csv(cl_path, index=False)
    print("Wrote:", reg_path.resolve())
    print("Wrote:", cl_path.resolve())


if __name__ == "__main__":
    main()
