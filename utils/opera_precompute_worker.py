"""
Top-level OPERA batch worker for ``ProcessPoolExecutor`` (Windows spawn-safe).

Imports are deferred inside ``run_one_batch`` so child processes resolve ``utils`` from repo root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def run_one_batch(payload: tuple[str, list[str], list[str], list[str], str]) -> dict[str, Any]:
    """
    Run ``batch_predict`` for one chunk and persist to the shared SQLite cache.

    ``payload`` is ``(cache_db_str, cas_chunk, sm_chunk, mids, worker_label)``.

    Returns ``{"written": int, "errors": list[str], "worker": str}``.
    """
    cache_db_str, cas_chunk, sm_chunk, mids, worker = payload
    cache_db = Path(cache_db_str)

    from utils import opera_batch
    from utils.opera_precompute_cache import init_db, put_batch_dataframe

    init_db(cache_db)
    br = opera_batch.batch_predict(
        sm_chunk,
        molecule_ids=mids,
        cas_for_cache=cas_chunk,
        sqlite_cache_path=None,
        disk_cache=True,
        fallback_single_on_error=True,
    )
    written = 0
    if not br.df.empty:
        try:
            written = put_batch_dataframe(cache_db, br.df)
        except Exception as e:
            return {"written": 0, "errors": [str(e)], "worker": worker}
    return {"written": int(written), "errors": list(br.errors or []), "worker": worker}
