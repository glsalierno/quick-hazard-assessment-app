"""
Scraper for ECHA CHEM - C&L Inventory (harmonized and notified classifications).
Source: https://echa.europa.eu/information-on-chemicals/cl-inventory-database
Note: ECHA pages may use JavaScript; URLs and selectors may need updating.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from .base import BaseHazardScraper
from .models import HazardDataPoint

logger = logging.getLogger(__name__)

ENDPOINT_MAPPING = {
    "acute toxicity": "acute_human_toxicity",
    "skin corrosion/irritation": "dermal_irritation",
    "serious eye damage/eye irritation": "eye_irritation",
    "respiratory sensitisation": "respiratory_sensitization",
    "skin sensitisation": "skin_sensitization",
    "germ cell mutagenicity": "mutagenicity",
    "carcinogenicity": "carcinogenicity",
    "reproductive toxicity": "reproductive_toxicity",
    "specific target organ toxicity - single exposure": "stot_single",
    "specific target organ toxicity - repeated exposure": "stot_repeated",
    "hazardous to the aquatic environment": "aquatic_toxicity",
}


def _extract_h_codes(text: str) -> list[str]:
    return re.findall(r"H\d{3}[A-Z]?", text)


def _map_endpoint(hazard_class: str) -> str:
    h = hazard_class.lower()
    for key, value in ENDPOINT_MAPPING.items():
        if key in h:
            return value
    return h.replace(" ", "_")


def _map_endpoint_from_hcode(h_code: str) -> str:
    if h_code.startswith("H300") or h_code.startswith("H301"):
        return "acute_toxicity_oral"
    if h_code.startswith("H310") or h_code.startswith("H311"):
        return "acute_toxicity_dermal"
    if h_code.startswith("H315"):
        return "skin_irritation"
    if h_code.startswith("H317"):
        return "skin_sensitization"
    if h_code.startswith("H318"):
        return "eye_damage"
    if h_code.startswith("H319"):
        return "eye_irritation"
    if h_code.startswith("H330") or h_code.startswith("H331"):
        return "acute_toxicity_inhalation"
    if h_code.startswith("H334"):
        return "respiratory_sensitization"
    if h_code.startswith("H340") or h_code.startswith("H341"):
        return "mutagenicity"
    if h_code.startswith("H350"):
        return "carcinogenicity"
    if h_code.startswith("H351"):
        return "carcinogenicity"
    if h_code.startswith("H360") or h_code.startswith("H361"):
        return "reproductive_toxicity"
    if h_code.startswith("H370"):
        return "stot_single"
    if h_code.startswith("H372"):
        return "stot_repeated"
    if h_code.startswith("H400"):
        return "acute_aquatic_toxicity"
    if h_code.startswith("H410"):
        return "chronic_aquatic_toxicity"
    return f"h_code_{h_code}"


class ECHAChemScraper(BaseHazardScraper):
    """Scraper for ECHA C&L Inventory."""

    def __init__(self, cache_manager: Any):
        super().__init__(cache_manager, rate_limit=2.0)
        self.base_url = "https://echa.europa.eu"
        self.search_url = f"{self.base_url}/information-on-chemicals/cl-inventory-database"
        self.detail_url = f"{self.base_url}/information-on-chemicals/cl-inventory-database/-/cl-inventory/view-notification"

    def search_by_cas(self, cas: str) -> list[HazardDataPoint]:
        cached = self.cache.get("ECHA", cas)
        if cached:
            logger.info("Using cached ECHA data for %s", cas)
            return cached
        results: list[HazardDataPoint] = []
        try:
            # ECHA C&L search: exact URL/form may vary; try common pattern
            resp = self.session.get(
                self.search_url,
                params={"cas_number": cas.replace("-", "")},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning("ECHA returned %s for CAS %s", resp.status_code, cas)
                self._respect_rate_limit()
                return []
            soup = BeautifulSoup(resp.content, "html.parser")
            # Try to find substance link or result table
            link = soup.find("a", class_=re.compile(r"clInventory|substance|result", re.I))
            if not link:
                # No result; try extracting any table with hazard info on same page
                results = self._extract_tables_from_page(soup, cas)
            else:
                href = link.get("href", "")
                if href.startswith("/"):
                    detail_url = self.base_url + href
                else:
                    detail_url = href
                detail_resp = self.session.get(detail_url, timeout=30)
                if detail_resp.status_code == 200:
                    detail_soup = BeautifulSoup(detail_resp.content, "html.parser")
                    results = self._extract_classifications(detail_soup, cas)
            self.cache.set("ECHA", cas, results)
        except Exception as e:
            logger.error("ECHA search failed for CAS %s: %s", cas, e)
        self._respect_rate_limit()
        return results

    def _extract_tables_from_page(self, soup: BeautifulSoup, cas: str) -> list[HazardDataPoint]:
        results: list[HazardDataPoint] = []
        name_el = soup.find("h1", class_=re.compile("title|page", re.I))
        chemical_name = name_el.get_text(strip=True) if name_el else "Unknown"
        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    hazard_class = cells[0].get_text(strip=True)
                    text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    for h_code in _extract_h_codes(text):
                        results.append(HazardDataPoint(
                            chemical_name=chemical_name,
                            cas=cas,
                            source="ECHA",
                            source_url=self.search_url,
                            endpoint=_map_endpoint(hazard_class),
                            value=h_code,
                            value_type="experimental",
                            hazard_class=hazard_class,
                            hazard_statement=h_code,
                            hazard_codes=[h_code],
                        ))
        return results

    def _extract_classifications(self, soup: BeautifulSoup, cas: str) -> list[HazardDataPoint]:
        results: list[HazardDataPoint] = []
        name_el = soup.find("h1", class_=re.compile("title|page", re.I))
        chemical_name = name_el.get_text(strip=True) if name_el else "Unknown"
        table = soup.find("table", class_=re.compile(r"harmonised|classification", re.I))
        if not table:
            return self._extract_tables_from_page(soup, cas)
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) >= 2:
                hazard_class = cells[0].get_text(strip=True)
                text = cells[1].get_text(strip=True)
                for h_code in _extract_h_codes(text):
                    results.append(HazardDataPoint(
                        chemical_name=chemical_name,
                        cas=cas,
                        source="ECHA",
                        source_url=self.detail_url,
                        endpoint=_map_endpoint(hazard_class),
                        value=h_code,
                        value_type="experimental",
                        hazard_class=hazard_class,
                        hazard_statement=h_code,
                        hazard_codes=[h_code],
                    ))
        return results

    def search_by_smiles(self, smiles: str) -> list[HazardDataPoint]:
        logger.warning("ECHA does not support SMILES search; use CAS.")
        return []

    def batch_search(self, identifiers: list[str], id_type: str = "cas") -> Any:
        import pandas as pd
        all_results: list[dict[str, Any]] = []
        for i, identifier in enumerate(identifiers):
            logger.info("ECHA processing %s/%s: %s", i + 1, len(identifiers), identifier)
            if id_type == "cas":
                points = self.search_by_cas(identifier)
            else:
                points = self.search_by_smiles(identifier)
            for p in points:
                all_results.append(p.to_dict())
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
