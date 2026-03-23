"""
Unified data structures for hazard scrapers (ECHA, Danish QSAR, VEGA, ICE).
Outputs are standardized for P2OASys-style hazard scoring.
"""

from __future__ import annotations

import hashlib
import json
import pickle
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class HazardDataPoint:
    """Unified data structure for all hazard endpoints."""

    chemical_name: str
    cas: str | None = None
    smiles: str | None = None
    ec_number: str | None = None
    inchi_key: str | None = None

    source: str = ""  # "ECHA", "Danish_QSAR", "VEGA", "ICE"
    source_url: str | None = None
    retrieval_date: str = field(default_factory=lambda: datetime.now().isoformat())

    endpoint: str = ""
    value: Any = None
    unit: str | None = None
    value_type: str = "experimental"  # "experimental", "predicted", "curated"

    confidence: str | None = None
    reliability: str | None = None
    applicability_domain: str | None = None

    hazard_class: str | None = None
    hazard_statement: str | None = None
    hazard_codes: list[str] = field(default_factory=list)

    model_name: str | None = None
    protocol: str | None = None
    notes: str | None = None
    raw_data: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame / JSON; raw_data serialized as JSON string."""
        d = asdict(self)
        if d.get("raw_data") is not None:
            try:
                d["raw_data"] = json.dumps(d["raw_data"], default=str)
            except (TypeError, ValueError):
                d["raw_data"] = str(d["raw_data"])
        return d


class CacheManager:
    """Manages local caching to avoid repeated requests."""

    def __init__(self, cache_dir: str | Path = "hazard_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_key(self, source: str, identifier: str, endpoint: str | None = None) -> str:
        key_str = f"{source}_{identifier}_{endpoint or 'all'}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, source: str, identifier: str, endpoint: str | None = None) -> Any | None:
        cache_key = self._get_cache_key(source, identifier, endpoint)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        if not cache_file.exists():
            return None
        try:
            with open(cache_file, "rb") as f:
                cached = pickle.load(f)
            age = datetime.now() - cached["timestamp"]
            if age.days < 7:
                return cached["data"]
        except Exception:
            pass
        return None

    def set(self, source: str, identifier: str, data: Any, endpoint: str | None = None) -> None:
        cache_key = self._get_cache_key(source, identifier, endpoint)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        with open(cache_file, "wb") as f:
            pickle.dump({"timestamp": datetime.now(), "data": data}, f)
