"""Abstract base scraper and session utilities for hazard databases."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import requests

try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    HTTPAdapter = None
    Retry = None

if TYPE_CHECKING:
    import pandas as pd
    from .models import CacheManager, HazardDataPoint


class BaseHazardScraper(ABC):
    """Abstract base class for all hazard database scrapers."""

    def __init__(self, cache_manager: Any, rate_limit: float = 1.0):
        self.cache = cache_manager
        self.rate_limit = rate_limit
        self.session = self._create_session()
        self.results: list[HazardDataPoint] = []

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        if HTTPAdapter is not None and Retry is not None:
            retry = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; HazardScraper/1.0; +https://github.com/glsalierno/quick-hazard-assessment-app)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        return session

    def _respect_rate_limit(self) -> None:
        time.sleep(self.rate_limit)

    @abstractmethod
    def search_by_cas(self, cas: str) -> list[HazardDataPoint]:
        """Search database by CAS number."""
        pass

    @abstractmethod
    def search_by_smiles(self, smiles: str) -> list[HazardDataPoint]:
        """Search database by SMILES string."""
        pass

    @abstractmethod
    def batch_search(self, identifiers: list[str], id_type: str = "cas") -> Any:
        """Batch search multiple chemicals. Returns DataFrame of results."""
        pass
