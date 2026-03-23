"""
Unified aggregator for ECHA, Danish QSAR, VEGA, and ICE.
Combines results into P2OASys-ready format and extra_sources for build_hazard_data().
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from .danish_qsar import DanishQSARScraper
from .echa import ECHAChemScraper
from .ice import NICEATMICEScraper
from .models import CacheManager, HazardDataPoint
from .vega import VEGAQSARScraper

logger = logging.getLogger(__name__)

# P2OASys hazard categories and source endpoint mapping
P2OASYS_MAPPING = {
    "acute_human_toxicity": [
        "acute_toxicity_oral",
        "acute_toxicity_dermal",
        "acute_toxicity_inhalation",
        "acute_oral_toxicity",
    ],
    "carcinogenicity": ["carcinogenicity"],
    "mutagenicity": ["mutagenicity"],
    "reproductive_toxicity": ["reproductive_toxicity", "developmental_toxicity"],
    "skin_sensitization": ["skin_sensitization"],
    "respiratory_sensitization": ["respiratory_sensitization"],
    "skin_irritation": ["skin_irritation", "dermal_irritation"],
    "eye_irritation": ["eye_irritation", "eye_damage"],
    "stot_single": ["stot_single"],
    "stot_repeated": ["stot_repeated"],
    "acute_aquatic_toxicity": [
        "acute_aquatic_toxicity",
        "fish_toxicity",
        "daphnia_toxicity",
        "algae_toxicity",
    ],
    "chronic_aquatic_toxicity": ["chronic_aquatic_toxicity"],
    "bioaccumulation": ["bioconcentration_factor", "bioaccumulation"],
    "biodegradation": ["biodegradation"],
    "physical_properties": [
        "molecular_weight",
        "log_p",
        "water_solubility",
        "melting_point",
        "boiling_point",
        "vapor_pressure",
    ],
}


def scraper_results_to_extra_sources(
    chemical_data: dict[str, list[HazardDataPoint]],
) -> dict[str, Any]:
    """
    Convert aggregator search results into extra_sources for build_hazard_data().

    Merges toxicities and GHS h_codes from all sources. Does not overwrite
    hazard_metrics unless you extend this helper.

    Returns:
        extra_sources dict with keys: toxicities, ghs (h_codes, p_codes), hazard_metrics.
    """
    extra: dict[str, Any] = {
        "toxicities": [],
        "ghs": {"h_codes": [], "p_codes": []},
        "hazard_metrics": {},
    }
    seen_h: set[str] = set()

    for _source, points in chemical_data.items():
        for p in points:
            # Toxicities: value string + unit + optional species_route
            val = p.value
            if val is not None:
                value_str = str(val)
                if p.unit:
                    value_str += f" {p.unit}"
                extra["toxicities"].append({
                    "value": value_str,
                    "unit": p.unit,
                    "species_route": [p.source] if p.source else None,
                })
            # GHS H-codes from hazard_statement or hazard_codes
            for code in p.hazard_codes or ([] if not p.hazard_statement else [p.hazard_statement]):
                code = (code or "").strip()
                if code and code.upper().startswith("H") and code not in seen_h:
                    seen_h.add(code)
                    extra["ghs"]["h_codes"].append(code)
            if p.hazard_statement and p.hazard_statement not in seen_h:
                seen_h.add(p.hazard_statement)
                extra["ghs"]["h_codes"].append(p.hazard_statement)

    return extra


class HazardDataAggregator:
    """
    Aggregates ECHA, Danish QSAR, VEGA, and ICE into one interface.
    Use search_chemical() then aggregate_for_p2oasys() or scraper_results_to_extra_sources().
    """

    def __init__(
        self,
        cache_dir: str | Path = "hazard_cache",
        vega_api_key: str | None = None,
        ice_api_key: str | None = None,
    ):
        self.cache = CacheManager(cache_dir)
        self.scrapers: dict[str, Any] = {
            "ECHA": ECHAChemScraper(self.cache),
            "Danish_QSAR": DanishQSARScraper(self.cache),
            "VEGA": VEGAQSARScraper(self.cache, api_key=vega_api_key),
            "ICE": NICEATMICEScraper(self.cache, api_key=ice_api_key),
        }
        self.p2oasys_mapping = P2OASYS_MAPPING

    def search_chemical(
        self,
        identifier: str,
        id_type: str = "cas",
        sources: list[str] | None = None,
    ) -> dict[str, list[HazardDataPoint]]:
        """
        Query all (or selected) sources for one chemical.

        Args:
            identifier: CAS number or SMILES.
            id_type: 'cas' or 'smiles'.
            sources: Source names to use; default all.

        Returns:
            Dict source_name -> list of HazardDataPoint.
        """
        if sources is None:
            sources = list(self.scrapers.keys())
        results: dict[str, list[HazardDataPoint]] = {}

        for name in sources:
            scraper = self.scrapers.get(name)
            if not scraper:
                logger.warning("Unknown source: %s", name)
                continue
            try:
                if id_type == "cas":
                    data = scraper.search_by_cas(identifier)
                elif id_type == "smiles":
                    data = scraper.search_by_smiles(identifier)
                else:
                    logger.error("Unsupported id_type: %s", id_type)
                    continue
                results[name] = data or []
                logger.info("Found %s data points from %s", len(results[name]), name)
            except Exception as e:
                logger.error("Error searching %s: %s", name, e)
                results[name] = []
        return results

    def aggregate_for_p2oasys(
        self,
        chemical_data: dict[str, list[HazardDataPoint]],
    ) -> pd.DataFrame:
        """
        Flatten all points into a DataFrame with endpoint columns and optional confidence.
        """
        all_points: list[HazardDataPoint] = []
        for points in chemical_data.values():
            all_points.extend(points)
        if not all_points:
            return pd.DataFrame()
        df = pd.DataFrame([p.to_dict() for p in all_points])
        index_cols = ["chemical_name", "cas", "smiles"]
        for c in index_cols:
            if c not in df.columns:
                df[c] = None
        pivot = df.pivot_table(
            index=index_cols,
            columns="endpoint",
            values="value",
            aggfunc="first",
        ).reset_index()
        conf = df.pivot_table(
            index=index_cols,
            columns="endpoint",
            values="confidence",
            aggfunc="first",
        ).reset_index()
        conf.columns = [
            f"{col}_confidence" if col not in index_cols else col
            for col in conf.columns
        ]
        out = pivot.merge(conf, on=index_cols, how="left")
        for p2oasys_cat, source_endpoints in self.p2oasys_mapping.items():
            available = [c for c in out.columns if c in source_endpoints]
            if available:
                out[p2oasys_cat] = out[available[0]]
                out[f"{p2oasys_cat}_source"] = available[0]
        return out

    def batch_process(
        self,
        identifiers: list[str],
        id_type: str = "cas",
        sources: list[str] | None = None,
        output_file: str | Path | None = None,
        delay_between: float = 2.0,
    ) -> pd.DataFrame:
        """
        Run search_chemical + aggregate_for_p2oasys for each identifier and concat.
        """
        rows: list[pd.DataFrame] = []
        for i, identifier in enumerate(identifiers):
            logger.info("Processing %s/%s: %s", i + 1, len(identifiers), identifier)
            chemical_data = self.search_chemical(identifier, id_type=id_type, sources=sources)
            df = self.aggregate_for_p2oasys(chemical_data)
            if not df.empty:
                df["search_identifier"] = identifier
                rows.append(df)
            time.sleep(delay_between)
        if not rows:
            return pd.DataFrame()
        out = pd.concat(rows, ignore_index=True)
        if output_file:
            out.to_csv(output_file, index=False)
            logger.info("Saved to %s", output_file)
        return out
