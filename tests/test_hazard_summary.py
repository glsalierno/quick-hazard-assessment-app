"""Unit tests for narrative hazard summary generation."""

from __future__ import annotations

import unittest

from utils.example_reports import load_dioxolane_example
from utils.hazard_summary import build_hazard_summary


class HazardSummaryTests(unittest.TestCase):
    def test_dioxolane_summary_prioritizes_flammability_and_repro_toxicity(self) -> None:
        summary = build_hazard_summary(
            query="646-06-0",
            preferred_name="1,3-Dioxolane",
            iupac_name="1,3-dioxolane",
            formula="C3H6O2",
            ghs={
                "h_codes": ["H225", "H319", "H360"],
                "p_codes": ["P210", "P280", "P305+P351+P338"],
                "signal_word": "Danger",
            },
            flash_point=["35 °F (2 °C) (Open cup)"],
            vapor_pressure=["79 mm Hg at 20 °C"],
            nfpa="Health 1; Fire 3; Instability 2",
            exposure_bands={
                "oral": {"ld50_mg_kg": 5200, "band": 5, "metric": "ld50_mg_kg"},
                "inhalation": {"lc50_mg_m3": 20650, "band": 5, "metric": "lc50_mg_m3"},
            },
            ecotoxicity={"aquatic_lc50_mg_l": 10000, "h_codes_aquatic": []},
        )

        self.assertEqual(summary["concern_level"], "high")
        self.assertIn("Danger", summary["headline"])
        self.assertIn("highly flammable", summary["headline"])
        self.assertIn("reproductive", summary["headline"])
        joined = " ".join(summary["paragraphs"])
        self.assertIn("H225", joined)
        self.assertIn("H360", joined)
        self.assertIn("H319", joined)
        self.assertIn("2 °C", joined)
        self.assertIn("10000 mg/L", joined)
        labels = [item["label"] for item in summary["highlights"]]
        self.assertIn("H360", labels)
        self.assertIn("H225", labels)

    def test_bundled_example_report_opens_as_high_concern(self) -> None:
        payload = load_dioxolane_example()
        self.assertEqual(payload["clean_cas"], "646-06-0")
        self.assertTrue(payload["is_example"])
        pubchem = payload["pubchem"]
        summary = build_hazard_summary(
            query=payload["clean_cas"],
            preferred_name=payload["preferred_name"],
            iupac_name=pubchem.get("iupac_name"),
            formula=pubchem.get("formula"),
            ghs=pubchem.get("ghs") or {},
            flash_point=pubchem.get("flash_point"),
            vapor_pressure=pubchem.get("vapor_pressure"),
            nfpa=pubchem.get("nfpa"),
            exposure_bands=pubchem.get("exposure_bands") or {},
            ecotoxicity=pubchem.get("ecotoxicity") or {},
        )
        self.assertEqual(summary["concern_level"], "high")
        self.assertIn("reproductive", summary["headline"])
        self.assertIn("H360", " ".join(summary["paragraphs"]))

    def test_unknown_when_no_ghs(self) -> None:
        summary = build_hazard_summary(query="unknown-chemical", ghs={"h_codes": [], "signal_word": ""})
        self.assertEqual(summary["concern_level"], "unknown")
        self.assertIn("limited GHS", summary["headline"])


if __name__ == "__main__":
    unittest.main()
