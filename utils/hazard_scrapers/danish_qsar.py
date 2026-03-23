"""
Scraper for Danish QSAR Database (DTU Food).
Source: http://qsar.food.dtu.dk
Provides predictions from multiple QSAR models (skin sensitization, mutagenicity, etc.).
Note: Site structure and form endpoints may change; adjust selectors as needed.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from .base import BaseHazardScraper
from .models import HazardDataPoint

logger = logging.getLogger(__name__)

MODELS_BY_ENDPOINT = {
    "skin_sensitization": ["Skin sensitization", "Derek", "MultiCASE", "Leadscope", "CAESAR"],
    "mutagenicity": ["Ames", "MultiCASE", "Leadscope", "CAESAR", "Sarah"],
    "acute_oral_toxicity": ["Acute oral toxicity", "MultiCASE", "Leadscope"],
    "developmental_toxicity": ["Developmental toxicity", "CAESAR", "MultiCASE"],
    "biodegradation": ["BIOWIN", "Ready biodegradability", "MultiCASE"],
    "bioaccumulation": ["BCF", "CAESAR", "MultiCASE"],
    "fish_toxicity": ["Fish acute", "MultiCASE", "Leadscope", "Fathead minnow"],
    "daphnia_toxicity": ["Daphnia", "MultiCASE"],
    "algae_toxicity": ["Algae", "MultiCASE"],
}


class DanishQSARScraper(BaseHazardScraper):
    """Scraper for Danish QSAR Database."""

    def __init__(self, cache_manager: Any):
        super().__init__(cache_manager, rate_limit=1.0)
        self.base_url = "http://qsar.food.dtu.dk"
        self.search_url = f"{self.base_url}/search.html"
        self.api_url = f"{self.base_url}/cgi-bin/search.pl"

    def search_by_cas(self, cas: str) -> list[HazardDataPoint]:
        cached = self.cache.get("Danish_QSAR", cas)
        if cached:
            return cached
        results: list[HazardDataPoint] = []
        try:
            resp = self.session.post(
                self.api_url,
                data={"query": cas, "searchtype": "cas", "database": "all"},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning("Danish QSAR returned %s for CAS %s", resp.status_code, cas)
                self._respect_rate_limit()
                return []
            soup = BeautifulSoup(resp.content, "html.parser")
            name = self._extract_label(soup, "Name")
            smiles = self._extract_label(soup, "SMILES")
            for endpoint, keywords in MODELS_BY_ENDPOINT.items():
                for row in soup.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    model_text = cells[0].get_text(strip=True)
                    if not any(k in model_text for k in keywords):
                        continue
                    pred_text = cells[1].get_text(strip=True)
                    value, unit = self._parse_prediction(pred_text)
                    applicability = cells[2].get_text(strip=True) if len(cells) > 2 else None
                    results.append(HazardDataPoint(
                        chemical_name=name or "Unknown",
                        cas=cas,
                        smiles=smiles,
                        source="Danish_QSAR",
                        source_url=self.search_url,
                        endpoint=endpoint,
                        value=value,
                        unit=unit,
                        value_type="predicted",
                        model_name=model_text,
                        applicability_domain=applicability,
                        confidence=self._confidence_from_applicability(applicability),
                    ))
            self.cache.set("Danish_QSAR", cas, results)
        except Exception as e:
            logger.error("Danish QSAR search failed for CAS %s: %s", cas, e)
        self._respect_rate_limit()
        return results

    def _extract_label(self, soup: BeautifulSoup, label: str) -> str | None:
        td = soup.find("td", string=re.compile(re.escape(label), re.I))
        if td:
            next_td = td.find_next_sibling("td")
            if next_td:
                return next_td.get_text(strip=True)
        return None

    def _parse_prediction(self, text: str) -> tuple[Any, str | None]:
        if "mg/L" in text:
            m = re.search(r"[\d.]+", text)
            return (float(m.group()) if m else None, "mg/L")
        if "mmol/L" in text:
            m = re.search(r"[\d.]+", text)
            return (float(m.group()) if m else None, "mmol/L")
        if "positive" in text.lower():
            return (True, None)
        if "negative" in text.lower():
            return (False, None)
        return (text, None)

    def _confidence_from_applicability(self, applicability: str | None) -> str:
        if not applicability:
            return "Low"
        if "within" in applicability.lower() or applicability.lower() == "yes":
            return "High"
        if "borderline" in applicability.lower():
            return "Moderate"
        return "Low"

    def search_by_smiles(self, smiles: str) -> list[HazardDataPoint]:
        cache_key = hashlib.md5(smiles.encode()).hexdigest()
        cached = self.cache.get("Danish_QSAR", cache_key)
        if cached:
            return cached
        results: list[HazardDataPoint] = []
        try:
            resp = self.session.post(
                self.api_url,
                data={"query": smiles, "searchtype": "smiles", "database": "all"},
                timeout=30,
            )
            if resp.status_code != 200:
                self._respect_rate_limit()
                return []
            soup = BeautifulSoup(resp.content, "html.parser")
            cas = self._extract_label(soup, "CAS")
            name = self._extract_label(soup, "Name")
            for endpoint, keywords in MODELS_BY_ENDPOINT.items():
                for row in soup.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2 or not any(k in cells[0].get_text() for k in keywords):
                        continue
                    pred_text = cells[1].get_text(strip=True)
                    value, unit = self._parse_prediction(pred_text)
                    applicability = cells[2].get_text(strip=True) if len(cells) > 2 else None
                    results.append(HazardDataPoint(
                        chemical_name=name or "Unknown",
                        cas=cas,
                        smiles=smiles,
                        source="Danish_QSAR",
                        endpoint=endpoint,
                        value=value,
                        unit=unit,
                        value_type="predicted",
                        model_name=cells[0].get_text(strip=True),
                        applicability_domain=applicability,
                        confidence=self._confidence_from_applicability(applicability),
                    ))
            self.cache.set("Danish_QSAR", cache_key, results)
        except Exception as e:
            logger.error("Danish QSAR search failed for SMILES: %s", e)
        self._respect_rate_limit()
        return results

    def batch_search(self, identifiers: list[str], id_type: str = "cas") -> Any:
        import pandas as pd
        all_results: list[dict[str, Any]] = []
        for i, identifier in enumerate(identifiers):
            logger.info("Danish QSAR processing %s/%s", i + 1, len(identifiers))
            points = self.search_by_cas(identifier) if id_type == "cas" else self.search_by_smiles(identifier)
            for p in points:
                all_results.append(p.to_dict())
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
