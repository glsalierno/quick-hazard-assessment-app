"""
Unified hazard scrapers: ECHA CHEM, Danish QSAR, VEGA QSAR, NIH NICEATM ICE.
Outputs are standardized for P2OASys-style hazard scoring.
IARC remains a local DB lookup only (see utils.iarc_lookup / utils.lookup_tables).
"""

from .aggregator import HazardDataAggregator, scraper_results_to_extra_sources
from .base import BaseHazardScraper
from .danish_qsar import DanishQSARScraper
from .echa import ECHAChemScraper
from .ice import NICEATMICEScraper
from .models import CacheManager, HazardDataPoint
from .vega import VEGAQSARScraper

__all__ = [
    "BaseHazardScraper",
    "CacheManager",
    "DanishQSARScraper",
    "ECHAChemScraper",
    "HazardDataAggregator",
    "HazardDataPoint",
    "NICEATMICEScraper",
    "VEGAQSARScraper",
    "scraper_results_to_extra_sources",
]
