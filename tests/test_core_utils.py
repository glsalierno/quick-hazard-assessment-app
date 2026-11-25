"""Offline tests for identity validation and rendering utilities."""

import unittest

from utils import cas_validator, data_formatter, ghs_formatter


class CoreUtilityTests(unittest.TestCase):
    def test_cas_format_and_checksum(self):
        self.assertTrue(cas_validator.is_valid_cas_format("67-64-2"))
        self.assertEqual(cas_validator.validate_cas(" 67-64-1 "), (True, "67-64-1"))
        self.assertEqual(cas_validator.validate_cas("67-64-2"), (False, "67-64-2"))
        self.assertEqual(cas_validator.validate_cas("64-17-5"), (True, "64-17-5"))
        self.assertEqual(cas_validator.normalize_cas_input("67-64-1 (Acetone)"), "67-64-1")

    def test_ghs_known_and_missing_codes(self):
        self.assertEqual(
            ghs_formatter.expand_h_codes_with_phrases(["H225", ""]),
            ["H225: Highly flammable liquid and vapour."],
        )
        self.assertEqual(ghs_formatter.get_p_phrase("P000"), "P000: (phrase not found)")

    def test_toxicity_prioritization_preserves_source(self):
        result = data_formatter.prioritize_toxicity_data(
            {"toxicities": [{"type": "LD50", "value": ">100", "unit": "mg/kg"},
                            {"type": "Irritation", "value": "reported"}]},
            {"study": [{"study_type": "NOAEL", "value": 10, "units": "mg/kg"}]},
        )
        self.assertEqual([item["source"] for item in result["quantitative"]], ["PubChem", "ToxValDB"])
        self.assertEqual(result["categorical"][0]["endpoint"], "Irritation")
        self.assertEqual(len(data_formatter.build_toxicity_display_df(result)), 3)
