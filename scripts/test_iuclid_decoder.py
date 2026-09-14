"""Quick test for IUCLID phrase decoder."""

from __future__ import annotations

import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from unified_hazard_report.iuclid_integration import sync_offline_secrets_from_st_secrets
from utils.iuclid_phrase_mapper import get_phrase_label


def main() -> int:
    sync_offline_secrets_from_st_secrets()
    samples = [
        "61267",
        "1342",
        "ENDPOINT_STUDY_RECORD",
        "ENDPOINT_STUDY_RECORD.Tox",
        "DOSSIER.R_COMPLETE",
        "H315",
    ]
    print("IUCLID decoder smoke test:")
    for raw in samples:
        label = get_phrase_label(raw)
        print(f"  {raw} -> {label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
