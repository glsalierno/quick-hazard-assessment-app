"""
PubChem-based CAS validation.

Uses existing pubchem_client to verify CAS existence in PubChem.
Filters out invalid/fake CAS that pass checksum but don't exist.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class PubChemValidator:
    """
    Validates CAS numbers against PubChem's database.
    Uses the existing pubchem_client (no new dependencies).
    """

    def __init__(self) -> None:
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.validation_stats = {
            "total_checked": 0,
            "found_in_pubchem": 0,
            "not_found": 0,
        }

    def validate(self, cas: str) -> Dict[str, Any]:
        """
        Check if CAS exists in PubChem.

        Returns:
            {
                'exists': bool | None,  # True=found, False=not found, None=lookup failed
                'cid': Optional[int],
                'name': Optional[str],
                'confidence_boost': float,  # 0.2 if exists, -0.3 if not, 0 if unknown
            }
        """
        if not cas or not str(cas).strip():
            return {"exists": False, "cid": None, "name": None, "confidence_boost": -0.3}

        cas = str(cas).strip()

        if cas in self.cache:
            return self.cache[cas]

        self.validation_stats["total_checked"] += 1

        try:
            from utils import pubchem_client

            data = pubchem_client.get_compound_data(cas, input_type="cas")
            exists = data is not None and data.get("cid") is not None

            result: Dict[str, Any] = {
                "exists": exists,
                "cid": data.get("cid") if exists else None,
                "name": (data.get("iupac_name") or data.get("formula")) if exists else None,
                "confidence_boost": 0.2 if exists else -0.3,
            }

            if exists:
                self.validation_stats["found_in_pubchem"] += 1
            else:
                self.validation_stats["not_found"] += 1

            self.cache[cas] = result
            return result

        except Exception as e:
            return {
                "exists": None,
                "cid": None,
                "name": None,
                "confidence_boost": 0.0,
                "error": str(e),
            }

    def validate_multiple(self, cas_list: List[str]) -> List[Dict[str, Any]]:
        """Validate multiple CAS numbers."""
        return [self.validate(cas) for cas in cas_list]

    def get_stats(self) -> Dict[str, int]:
        """Return validation statistics."""
        return self.validation_stats.copy()

    def reset_stats(self) -> None:
        """Reset statistics and cache."""
        self.validation_stats = {"total_checked": 0, "found_in_pubchem": 0, "not_found": 0}
        self.cache = {}


# Module-level singleton (avoids Streamlit import for script/CLI use)
_validator: Optional[PubChemValidator] = None


def get_pubchem_validator() -> PubChemValidator:
    """Get or create PubChem validator singleton."""
    global _validator
    if _validator is None:
        _validator = PubChemValidator()
    return _validator
