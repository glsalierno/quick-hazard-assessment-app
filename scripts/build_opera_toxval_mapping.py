"""
Build OPERA endpoint -> ToxVal type mapping JSON.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from utils.opera_mapper import OperaEndpointMapper


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoints-json", type=Path, default=Path(config.DATA_DIR) / "opera_endpoints.json")
    parser.add_argument("--toxval-sqlite", type=Path, default=Path(config.CHEMICAL_DB_PATH))
    parser.add_argument("--toxval-csv", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=Path(config.DATA_DIR) / "opera_to_toxval_mapping.json")
    args = parser.parse_args()

    mapper = OperaEndpointMapper(
        endpoints_json_path=args.endpoints_json,
        toxval_sqlite_path=args.toxval_sqlite,
        toxval_csv_path=args.toxval_csv,
        mapping_output_path=args.out,
    )
    out = mapper.build_and_save()
    print(f"Saved mapping: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
