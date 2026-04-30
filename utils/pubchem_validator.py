"""
PubChem-based CAS validation.

Uses existing pubchem_client to verify CAS existence in PubChem.
Filters out invalid/fake CAS that pass checksum but don't exist.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _cid_synonyms_include_cas(cid: int, canonical_cas: str) -> bool:
    """
    Confirm the resolved PubChem record lists this CAS (xref alone can be ambiguous).
    """
    from utils import cas_validator

    canon = (cas_validator.normalize_cas_input(canonical_cas) or canonical_cas).strip()
    if not canon:
        return False
    canon_digits = "".join(c for c in canon if c.isdigit())
    try:
        import pubchempy as pcp

        comp = pcp.Compound.from_cid(int(cid))
    except Exception:
        return False
    for syn in getattr(comp, "synonyms", None) or []:
        if not isinstance(syn, str):
            continue
        s = syn.strip()
        if not s:
            continue
        n = cas_validator.normalize_cas_input(s)
        if n and n == canon:
            return True
        sd = "".join(c for c in s if c.isdigit())
        if len(sd) >= 5 and sd == canon_digits:
            return True
    return False


def gate_strict_cas_for_assessment(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Typed / manual CAS entry: require checksum-valid CAS, then PubChem confirmation when enabled.

    Returns:
        (canonical_cas, None) on success.
        (None, error_code) on failure — codes: ``checksum``, ``pubchem_not_found``.
        (None, ``not_cas_pattern``) if the string does not look like N-N-N (caller may treat as name).
    """
    from utils import cas_validator

    import config as _cfg

    if not raw or not str(raw).strip():
        return None, None
    clean = cas_validator.normalize_cas_input(raw)
    if not clean:
        return None, None
    if not cas_validator.is_valid_cas_format(clean):
        return None, "not_cas_pattern"
    ok, canon = cas_validator.validate_cas(clean)
    if not ok:
        return None, "checksum"
    if not getattr(_cfg, "USE_PUBCHEM_CAS_VALIDATION", True):
        return canon, None
    v = get_pubchem_validator().validate(canon)
    ex = v.get("exists")
    if ex is False:
        return None, "pubchem_not_found"
    if ex is None:
        # Network / service failure: still allow assessment attempt; caller may warn.
        return canon, "pubchem_unknown"
    return canon, None


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
            return {"exists": False, "cid": None, "name": None, "confidence_boost": 0.0}

        from utils import cas_validator

        canonical = cas_validator.normalize_cas_input(str(cas).strip()) or str(cas).strip()

        if canonical in self.cache:
            return self.cache[canonical]

        self.validation_stats["total_checked"] += 1

        try:
            from utils import pubchem_client

            data = pubchem_client.get_compound_data(canonical, input_type="cas")
            exists = data is not None and data.get("cid") is not None
            if exists and not _cid_synonyms_include_cas(int(data["cid"]), canonical):
                exists = False

            result: Dict[str, Any] = {
                "exists": exists,
                "cid": data.get("cid") if exists else None,
                "name": (data.get("iupac_name") or data.get("formula")) if exists else None,
                "confidence_boost": 0.2 if exists else 0.0,
            }

            if exists:
                self.validation_stats["found_in_pubchem"] += 1
            else:
                self.validation_stats["not_found"] += 1

            self.cache[canonical] = result
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
