"""Load offline IUCLID snapshots once and index ``.i6z`` paths by dossier UUID."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from ingest import offline_echa_loader as ob
from ingest.crosswalk import normalize_cas


class OfflineDataContext:
    """
    Holds ``substances_df`` / ``cl_hazards_df`` from ``load_echa_from_offline`` and a UUID → ``.i6z`` map.

    The map is built by scanning the extraction directory for the configured ``OFFLINE_LOCAL_ARCHIVE``
    (same layout as ``offline_echa_loader.extract_i6z_metadata``).
    """

    def __init__(self) -> None:
        self.substances_df, self.cl_hazards_df = ob.load_echa_from_offline(
            use_cache=True,
            force_rebuild=False,
        )
        self._i6z_by_uuid: dict[str, Path] = {}
        self._build_i6z_index()

    def _build_i6z_index(self) -> None:
        la = os.getenv("OFFLINE_LOCAL_ARCHIVE", "").strip()
        roots: list[Path] = []
        if la:
            ap = Path(os.path.expandvars(la)).expanduser().resolve()
            if ap.is_file():
                roots.append(ob._extract_dir_for_archive(ap, ob.OFFLINE_DATA_DIR))
            elif ap.is_dir():
                roots.append(ap)
        # Also scan default extracted children if archive env missing but data exists
        base_extracted = ob.OFFLINE_DATA_DIR / "extracted"
        if base_extracted.is_dir():
            roots.extend(sorted(base_extracted.glob("*")))

        seen: set[Path] = set()
        for root in roots:
            root = root.resolve()
            if not root.is_dir() or root in seen:
                continue
            seen.add(root)
            for pat in ("*.i6z", "*.I6Z"):
                for p in root.rglob(pat):
                    self._i6z_by_uuid.setdefault(p.stem.lower(), p)

    def i6z_path_for_uuid(self, uuid: str) -> Path | None:
        return self._i6z_by_uuid.get(str(uuid).strip().lower())

    def uuids_for_cas(self, cas: str) -> list[str]:
        want = normalize_cas(cas.strip()) or cas.strip()
        if not want or self.substances_df.empty or "cas_number" not in self.substances_df.columns:
            return []
        cc = self.substances_df["cas_number"].map(
            lambda x: normalize_cas(str(x))
            if x is not None and str(x).strip() and str(x).lower() != "nan"
            else ""
        )
        hit = self.substances_df[cc == want]
        return list(dict.fromkeys(hit["uuid"].astype(str).tolist()))
