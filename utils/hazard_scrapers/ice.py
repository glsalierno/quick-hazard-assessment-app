"""
Scraper for NIH NICEATM Integrated Chemical Environment (ICE).
Source: https://ntp.niehs.nih.gov/go/niceatm-ice
Uses ICE REST API; API key may be required (request from ICE-support@niehs.nih.gov).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from .base import BaseHazardScraper
from .models import HazardDataPoint

logger = logging.getLogger(__name__)

ENDPOINT_KEYWORDS = {
    "acute_oral_toxicity": ["LD50", "OECD 425"],
    "skin_irritation": ["Draize", "OECD 404"],
    "eye_irritation": ["Draize", "OECD 405"],
    "skin_sensitization": ["LLNA", "OECD 406", "OECD 429"],
    "mutagenicity": ["Ames", "OECD 471"],
    "developmental_toxicity": ["OECD 414", "DevTox"],
    "reproductive_toxicity": ["OECD 421", "OECD 422"],
    "endocrine_disruption": ["ER", "AR", "H295R"],
}


class NICEATMICEScraper(BaseHazardScraper):
    """Scraper for NIH NICEATM ICE API."""

    def __init__(self, cache_manager: Any, api_key: str | None = None):
        super().__init__(cache_manager, rate_limit=1.0)
        self.base_url = "https://ice.ntp.niehs.nih.gov/api/v1"
        self.api_key = api_key or os.environ.get("ICE_API_KEY", "")

    def search_by_cas(self, cas: str) -> list[HazardDataPoint]:
        cached = self.cache.get("ICE", cas)
        if cached:
            return cached
        results: list[HazardDataPoint] = []
        try:
            headers = {}
            if self.api_key and self.api_key != "YOUR_API_KEY_HERE":
                headers["X-API-Key"] = self.api_key
            resp = self.session.get(
                f"{self.base_url}/chemical",
                params={"casrn": cas},
                headers=headers or None,
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning("ICE chemical lookup failed for %s: %s", cas, resp.status_code)
                self._respect_rate_limit()
                return []
            chem = resp.json()
            name = chem.get("name", "Unknown")
            smiles = chem.get("smiles")
            dtxsid = chem.get("dtxsid")
            if dtxsid:
                invivo = self._get_invivo(dtxsid, headers)
                for r in invivo:
                    r.chemical_name = name
                    r.cas = cas
                    r.smiles = smiles
                results.extend(invivo)
            else:
                results.append(HazardDataPoint(
                    chemical_name=name,
                    cas=cas,
                    smiles=smiles,
                    source="ICE",
                    endpoint="chemical_info",
                    value=chem,
                    value_type="curated",
                    raw_data=chem,
                ))
            self.cache.set("ICE", cas, results)
        except Exception as e:
            logger.error("ICE search failed for CAS %s: %s", cas, e)
        self._respect_rate_limit()
        return results

    def _get_invivo(self, dtxsid: str, headers: dict) -> list[HazardDataPoint]:
        out: list[HazardDataPoint] = []
        try:
            resp = self.session.get(
                f"{self.base_url}/invivo",
                params={"dtxsid": dtxsid},
                headers=headers or None,
                timeout=30,
            )
            if resp.status_code != 200:
                return out
            data = resp.json()
            for study in data.get("studies", []):
                study_type = study.get("study_type", "")
                endpoint = self._map_study_type(study_type)
                out.append(HazardDataPoint(
                    chemical_name="",
                    source="ICE_invivo",
                    endpoint=endpoint,
                    value=study.get("value"),
                    unit=study.get("unit"),
                    value_type="experimental",
                    protocol=study.get("guideline"),
                    reliability=study.get("reliability"),
                    notes=study.get("species"),
                    raw_data=study,
                ))
        except Exception as e:
            logger.warning("ICE invivo fetch failed: %s", e)
        return out

    def _map_study_type(self, study_type: str) -> str:
        s = study_type.lower()
        for endpoint, keywords in ENDPOINT_KEYWORDS.items():
            for k in keywords:
                if k.lower() in s:
                    return endpoint
        return s.replace(" ", "_")

    def search_by_smiles(self, smiles: str) -> list[HazardDataPoint]:
        logger.warning("ICE prefers CAS or DTXSID; use search_by_cas.")
        return []

    def batch_search(self, identifiers: list[str], id_type: str = "cas") -> Any:
        import pandas as pd
        all_results: list[dict[str, Any]] = []
        for i, identifier in enumerate(identifiers):
            logger.info("ICE processing %s/%s: %s", i + 1, len(identifiers), identifier)
            if id_type == "cas":
                points = self.search_by_cas(identifier)
            else:
                points = self.search_by_smiles(identifier)
            for p in points:
                all_results.append(p.to_dict())
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
