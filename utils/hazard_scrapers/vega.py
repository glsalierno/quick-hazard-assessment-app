"""
Scraper for VEGA QSAR models (optional API).
Source: https://www.vegahub.eu
Adjust base_url and payload as per actual VEGA API docs.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import requests

from .base import BaseHazardScraper
from .models import HazardDataPoint

logger = logging.getLogger(__name__)


class VEGAQSARScraper(BaseHazardScraper):
    """Scraper for VEGA QSAR (API if available)."""

    def __init__(self, cache_manager: Any, api_key: str | None = None):
        super().__init__(cache_manager, rate_limit=0.5)
        self.base_url = "https://api.vegahub.eu/qsar"
        self.api_key = api_key

    def search_by_cas(self, cas: str) -> list[HazardDataPoint]:
        logger.warning("VEGA prefers SMILES; use search_by_smiles or resolve CAS to SMILES first.")
        return []

    def search_by_smiles(self, smiles: str) -> list[HazardDataPoint]:
        cache_key = hashlib.md5(("%s_vega" % smiles).encode()).hexdigest()
        cached = self.cache.get("VEGA", cache_key)
        if cached:
            return cached
        results: list[HazardDataPoint] = []
        try:
            headers = {}
            if self.api_key:
                headers["X-API-Key"] = self.api_key
            resp = self.session.post(
                self.base_url + "/predict",
                json={"smiles": smiles, "output_format": "json"},
                headers=headers,
                timeout=60,
            )
            if resp.status_code != 200:
                logger.warning("VEGA API returned %s", resp.status_code)
                self._respect_rate_limit()
                return []
            data = resp.json()
            pred = data.get("prediction") or data
            if isinstance(pred, dict):
                results.append(HazardDataPoint(
                    chemical_name=data.get("compound_name", "Unknown"),
                    smiles=smiles,
                    source="VEGA",
                    endpoint=data.get("endpoint", "prediction"),
                    value=pred.get("value"),
                    unit=pred.get("unit"),
                    value_type="predicted",
                    model_name=data.get("model_name"),
                    confidence=str(pred.get("confidence", "")),
                    applicability_domain=pred.get("applicability_domain"),
                    raw_data=data,
                ))
            self.cache.set("VEGA", cache_key, results)
        except Exception as e:
            logger.error("VEGA search failed: %s", e)
        self._respect_rate_limit()
        return results

    def batch_search(self, identifiers: list[str], id_type: str = "smiles") -> Any:
        import pandas as pd
        all_results: list[dict[str, Any]] = []
        for i, identifier in enumerate(identifiers):
            logger.info("VEGA processing %s/%s", i + 1, len(identifiers))
            if id_type == "smiles":
                points = self.search_by_smiles(identifier)
            else:
                points = self.search_by_cas(identifier)
            for p in points:
                all_results.append(p.to_dict())
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
