#!/usr/bin/env python3
"""
Quality test for quick-hazard-assessment-app before pushing to GitHub.
Checks imports, config paths, one-CAS flow, lookups, and optional hazard scrapers.
Run: python scripts/quality_test_pre_push.py [--skip-network]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

FAILED = []
PASSED = []


def ok(name: str) -> None:
    PASSED.append(name)


def fail(name: str, msg: str) -> None:
    FAILED.append(f"{name}: {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Quality test before push")
    parser.add_argument("--skip-network", action="store_true", help="Skip PubChem and scraper requests")
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # 1. Imports
    # -------------------------------------------------------------------------
    try:
        import config
        ok("import config")
    except Exception as e:
        fail("import config", str(e))
        print("FAIL: config import failed; cannot continue.")
        return 1

    try:
        from utils import cas_validator, pubchem_client, hazard_for_p2oasys, p2oasys_scorer, p2oasys_aggregate
        ok("import utils (cas_validator, pubchem_client, hazard_for_p2oasys, p2oasys_scorer, p2oasys_aggregate)")
    except Exception as e:
        fail("import core utils", str(e))

    try:
        from utils import lookup_tables, iarc_lookup, atmo_gwp
        ok("import utils (lookup_tables, iarc_lookup, atmo_gwp)")
    except Exception as e:
        fail("import lookup utils", str(e))

    try:
        from utils.hazard_scrapers import HazardDataAggregator, scraper_results_to_extra_sources
        ok("import hazard_scrapers")
    except Exception as e:
        fail("import hazard_scrapers", str(e))

    try:
        from utils import sds_regex_extractor
        ok("import sds_regex_extractor (optional)")
    except ImportError:
        ok("import sds_regex_extractor (optional, not installed)")

    try:
        from utils import carcinogenic_potency_client
        ok("import carcinogenic_potency_client (optional)")
    except ImportError:
        ok("import carcinogenic_potency_client (optional, not installed)")

    # -------------------------------------------------------------------------
    # 2. Config paths
    # -------------------------------------------------------------------------
    matrix_path = getattr(config, "P2OASYS_MATRIX_PATH", None)
    if matrix_path is None:
        matrix_path = getattr(p2oasys_scorer, "DEFAULT_MATRIX_PATH", None)
    if matrix_path:
        matrix_path = Path(matrix_path)
        if matrix_path.exists():
            ok("P2OASys matrix file exists")
        else:
            # Non-fatal: matrix is optional for repo push; app shows info message when missing
            ok("P2OASys matrix path configured (file missing - place matrix in data/ for P2OASys tab)")
    else:
        fail("P2OASys matrix", "no path configured")

    iarc_dir = getattr(config, "IARC_DIR", None)
    if iarc_dir and os.path.isdir(iarc_dir):
        ok("IARC_DIR exists")
    else:
        ok("IARC_DIR (optional) missing or not set")

    atmo_dir = getattr(config, "ATMO_DIR", None)
    if atmo_dir and os.path.isdir(atmo_dir):
        ok("ATMO_DIR exists")
    else:
        ok("ATMO_DIR (optional) missing or not set")

    # -------------------------------------------------------------------------
    # 3. One-CAS flow (minimal)
    # -------------------------------------------------------------------------
    clean_cas = cas_validator.normalize_cas_input("71-43-2")
    if clean_cas:
        ok("cas_validator.normalize_cas_input")
    else:
        fail("cas_validator", "normalize_cas_input returned falsy for 71-43-2")

    if not args.skip_network:
        try:
            pubchem_data = pubchem_client.get_compound_data("71-43-2", input_type="cas")
            if pubchem_data:
                ok("pubchem_client.get_compound_data")
            else:
                fail("pubchem_client", "get_compound_data returned None")
                pubchem_data = None
        except Exception as e:
            fail("pubchem_client", str(e))
            pubchem_data = None
    else:
        pubchem_data = {"ghs": {"h_codes": [], "p_codes": []}, "toxicities": [], "flash_point": None, "vapor_pressure": None}
        ok("pubchem (skipped, using mock)")

    if pubchem_data:
        try:
            hazard_data = hazard_for_p2oasys.build_hazard_data(pubchem_data)
            if hazard_data and "ghs" in hazard_data and "toxicities" in hazard_data:
                ok("hazard_for_p2oasys.build_hazard_data")
            else:
                fail("build_hazard_data", "unexpected structure")
        except Exception as e:
            fail("build_hazard_data", str(e))

    if matrix_path and matrix_path.exists() and pubchem_data:
        try:
            matrix = p2oasys_scorer.load_p2oasys_matrix(matrix_path)
            hazard_data = hazard_for_p2oasys.build_hazard_data(pubchem_data)
            scores = p2oasys_scorer.compute_p2oasys_scores(hazard_data, matrix)
            overall = p2oasys_aggregate.aggregate_category_scores(scores, "max")
            ok("p2oasys load + compute + aggregate")
        except Exception as e:
            fail("p2oasys flow", str(e))

    # -------------------------------------------------------------------------
    # 4. Lookups (when dirs/files exist)
    # -------------------------------------------------------------------------
    if iarc_dir and os.path.isdir(iarc_dir):
        try:
            iarc_by_cas = iarc_lookup.load_iarc_from_iarc_folder(iarc_dir)
            ok("iarc_lookup.load_iarc_from_iarc_folder")
        except Exception as e:
            fail("iarc_lookup", str(e))

    if atmo_dir and os.path.isdir(atmo_dir):
        try:
            ipcc = atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir)
            ok("atmo_gwp.load_ipcc_gwp_100_from_atmo")
        except Exception as e:
            fail("atmo_gwp", str(e))

    odp_path = getattr(config, "P2OASYS_ODP_GWP_CSV_PATH", None)
    if odp_path and os.path.isfile(odp_path):
        try:
            lookup_tables.load_odp_gwp_csv(odp_path)
            ok("lookup_tables.load_odp_gwp_csv")
        except Exception as e:
            fail("load_odp_gwp_csv", str(e))

    # -------------------------------------------------------------------------
    # 5. Hazard scrapers (optional; can skip network)
    # -------------------------------------------------------------------------
    if not args.skip_network:
        try:
            agg = HazardDataAggregator(cache_dir=str(APP_ROOT / "hazard_cache"))
            chemical_data = agg.search_chemical("71-43-2", id_type="cas", sources=[])
            extra = scraper_results_to_extra_sources(chemical_data)
            if "toxicities" in extra and "ghs" in extra:
                ok("HazardDataAggregator + scraper_results_to_extra_sources")
            else:
                fail("scraper_results_to_extra_sources", "missing keys")
        except Exception as e:
            fail("hazard_scrapers", str(e))
    else:
        try:
            agg = HazardDataAggregator(cache_dir=str(APP_ROOT / "hazard_cache"))
            extra = scraper_results_to_extra_sources({"ECHA": [], "Danish_QSAR": []})
            if "toxicities" in extra and "ghs" in extra:
                ok("scraper_results_to_extra_sources (no network)")
            else:
                fail("scraper_results_to_extra_sources", "missing keys")
        except Exception as e:
            fail("hazard_scrapers (no network)", str(e))

    # -------------------------------------------------------------------------
    # Report
    # -------------------------------------------------------------------------
    print("Passed:", len(PASSED))
    for p in PASSED:
        print("  OK", p)
    if FAILED:
        print("Failed:", len(FAILED))
        for f in FAILED:
            print("  FAIL", f)
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
