"""
P2OASys Hazard Score Calculator (Quick Hazard Assessment app).

Maps hazard data to P2OASys scores using the TURI Hazard Matrix Excel file.
Used by the P2OASys scoring tab. https://p2oasys.turi.org/chemical/hazard-score-matrix
"""

import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# -----------------------------------------------------------------------------
# Configuration: use app config when available
# -----------------------------------------------------------------------------

def _default_matrix_path() -> Path:
    try:
        import config as _config
        return Path(_config.P2OASYS_MATRIX_PATH)
    except Exception:
        pass
    _root = Path(__file__).resolve().parent.parent
    return _root / "data" / "Hazard Matrix Group Review 9-19-23.xlsx"

DEFAULT_MATRIX_PATH = _default_matrix_path()
SCORE_COLS = [2, 4, 6, 8, 10]  # P2OASys score levels

# Sheet to category mapping (sheet names may have trailing space)
SHEET_CATEGORIES = {
    "Acute": "Acute Human Effects",
    "Acute ": "Acute Human Effects",  # Note: sheet name has trailing space
    "Chronic": "Chronic Human Effects",
    "Ecological Hazards": "Ecological Hazards",
    "Environmental Fate & Transport": "Environmental Fate & Transport",
    "Atmospheric Hazard": "Atmospheric Hazard",
    "Physical Hazard": "Physical Properties",
    "Process Factors": "Process Factors",
    "Life Cycle Factors": "Life Cycle Factors",
}


# -----------------------------------------------------------------------------
# Excel Matrix Loader
# -----------------------------------------------------------------------------


def _parse_numeric_threshold(val: Any) -> Optional[float]:
    """Parse numeric threshold from cell (handles '>100', '<50', etc.)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan" or s == ".":
        return None
    # Extract number from patterns like ">100", "<50", "5000", "0.05", "6-7"
    m = re.search(r"[<>]?\s*(\d+\.?\d*|\d*\.\d+)", s)
    if m:
        num_str = m.group(1)
        if num_str and num_str not in (".", ""):
            try:
                return float(num_str)
            except ValueError:
                pass
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def load_p2oasys_matrix(excel_path: Path) -> dict[str, Any]:
    """
    Load P2OASys scoring matrix from Excel.
    Returns dict: {category: {subcategory: {unit: {type, thresholds, mapping}}}}
    """
    if not excel_path.exists():
        raise FileNotFoundError(f"Matrix file not found: {excel_path}")

    xl = pd.ExcelFile(excel_path, engine="openpyxl")
    matrix: dict[str, Any] = {}

    for sheet_name in xl.sheet_names:
        if sheet_name == "Matrix notes":
            continue
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        category = SHEET_CATEGORIES.get(sheet_name.strip(), sheet_name)
        matrix[category] = _parse_sheet(df, category)

    return matrix


def _parse_sheet(df: pd.DataFrame, category: str) -> dict[str, Any]:
    """
    Parse one sheet following the P2OASys matrix structure:
    - Col A: Category name, subcategory name, or feature name
    - Cols B-F: Score levels 2, 4, 6, 8, 10 respectively
    - Hierarchy: Category (sheet) -> Subcategory (e.g. Inhalation Toxicity) -> Features (e.g. LC50 ppm, GHS H Phrases)
    - 'UNITS' is a section marker, not a feature
    - GHS H Phrases are subcategory-specific (Inhalation vs Oral vs Dermal, etc.)
    """
    rules: dict[str, Any] = {}
    current_sub = ""
    category_name = category  # e.g. "Acute Human Effects"

    for i in range(len(df)):
        row = df.iloc[i]
        c0 = _str(row.iloc[0])
        # Extract values from cols B-F (indices 1-5) = scores 2, 4, 6, 8, 10
        values = [row.iloc[j] if j < len(row) else None for j in range(1, 6)]

        # Skip empty rows
        if not c0 and all(v is None or (isinstance(v, float) and pd.isna(v)) for v in values):
            continue

        # Skip "UNITS" row - it's a section marker, not a subcategory or feature
        if c0.upper().strip() == "UNITS":
            continue

        # Check if this row has values in B-F (feature row)
        has_values = any(
            v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip()
            for v in values
        )

        if has_values:
            # Feature row: Col A = feature name, B-F = score thresholds/mappings
            rule = _build_rule(c0.strip(), values)
            if rule:
                sub = current_sub or category_name
                rules.setdefault(sub, {})[c0.strip()] = rule
        else:
            # Subcategory header: text in A, no values in B-F (e.g. "Inhalation Toxicity")
            # Don't overwrite with main category name if it's the sheet title
            if c0 and c0 != category_name:
                # Avoid treating numeric-only or very short labels as subcategories
                if len(c0) > 2 and not re.match(r"^[\d.\s]+$", c0):
                    current_sub = c0.strip()

    return rules


def _str(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def _is_numeric(s: str) -> bool:
    if not s:
        return False
    return bool(re.match(r"^[\d.]+$", str(s))) or s.startswith(("<", ">"))


def _is_unit_row(c0: str) -> bool:
    """Heuristic: unit rows often contain 'UNITS', specific units, or GHS."""
    u = c0.upper()
    return "UNITS" in u or "LD50" in u or "LC50" in u or "GHS" in u or "NFPA" in u or "FLASH" in u or "MM HG" in u or "KEY PHRASE" in u or "IARC" in u or "EPA" in u or "PROP 65" in u


def _looks_like_unit(c0: str) -> bool:
    if not c0 or len(c0) < 2:
        return False
    if c0.upper() == "UNITS":
        return False
    return True


def _build_rule(unit_name: str, values: list) -> Optional[dict]:
    """
    Build scoring rule from unit name and value row.
    Cols B-F (values[0:5]) map to scores 2, 4, 6, 8, 10.
    Check GHS H Phrases FIRST - otherwise "H333" gets parsed as numeric 333.
    """
    if not values:
        return None
    u = unit_name.upper()
    # GHS H phrases - subcategory-specific (Inhalation vs Oral vs Dermal vs Aquatic)
    # Must check BEFORE numeric - "H333" would otherwise parse as 333
    u = unit_name.upper()
    if "GHS H" in u or "GHS H PHRASE" in u:
        mapping = {}
        for i, v in enumerate(values):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            # Split "H332, H305" into individual codes, each gets same score
            for code in re.findall(r"H\d+(?:\+\d+)?", s):
                mapping[code.strip()] = SCORE_COLS[i]
        if mapping:
            return {"type": "ghs_h", "unit": unit_name, "mapping": mapping}
    # Key phrases - substring match for hazard descriptions
    if "KEY PHRASE" in u or "KEY WORD" in u:
        phrases = [str(v).strip() for v in values if v and not (isinstance(v, float) and pd.isna(v))]
        if phrases:
            return {"type": "phrase", "unit": unit_name, "phrases": list(zip(phrases, SCORE_COLS))}
    # IARC Category: "3", "2B", "1 or 2A" - split so 1 and 2A both map to same score
    if "IARC" in u:
        mapping = {}
        for i, v in enumerate(values):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            for part in re.split(r"\s+or\s+|\s*,\s*|\s*/\s*", s, flags=re.I):
                part = part.strip()
                if part and re.match(r"^[\dA-Za-z]+$", part):
                    mapping[part.upper()] = SCORE_COLS[i]
            if s:
                mapping[s.upper()] = SCORE_COLS[i]
        if mapping:
            return {"type": "text", "unit": unit_name, "mapping": mapping}
    # EPA / ACGIH / OSHA: extract Group X or key tokens for matching
    if "EPA" in u or "ACGIH" in u or "OSHA" in u or "PROP 65" in u:
        mapping = {}
        for i, v in enumerate(values):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            # Map "Group A" -> A, "Group B2" -> B2, etc.
            for m in re.finditer(r"Group\s*([A-Z0-9]+)", s, re.I):
                mapping[m.group(1).upper()] = SCORE_COLS[i]
            mapping[s[:80]] = SCORE_COLS[i]
        if mapping:
            return {"type": "text", "unit": unit_name, "mapping": mapping}
    # GHS Category level: "Not Classified", "Acute 1", "Acute 2", "Chronic 3", etc.
    if "GHS CATEGORY" in u and "H PHRASE" not in u:
        mapping = {}
        for i, v in enumerate(values):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if s:
                mapping[s.upper()] = SCORE_COLS[i]
        if mapping:
            return {"type": "text", "unit": unit_name, "mapping": mapping}
    # Numeric thresholds (LD50, LC50, Flash point, etc.) - after GHS H so "H333" is not parsed as 333
    nums = []
    for v in values:
        try:
            n = _parse_numeric_threshold(v)
            nums.append(n)
        except (ValueError, TypeError):
            nums.append(None)
    if any(n is not None for n in nums):
        return {
            "type": "numeric",
            "unit": unit_name,
            "thresholds": [(n, SCORE_COLS[i]) for i, n in enumerate(nums) if n is not None],
        }
    # Generic text mapping
    mapping = {}
    for i, v in enumerate(values):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s:
            mapping[s[:80]] = SCORE_COLS[i]
    if mapping:
        return {"type": "text", "unit": unit_name, "mapping": mapping}
    return None


def _dump_matrix(matrix: dict[str, Any]) -> None:
    """Print parsed matrix structure for verification. Cols B-F = scores 2,4,6,8,10."""
    print("\n" + "=" * 70)
    print("P2OASys MATRIX STRUCTURE (parsed from Excel)")
    print("Cols B-F = scores 2, 4, 6, 8, 10 | GHS H Phrases are subcategory-specific")
    print("=" * 70)
    for category, subcats in matrix.items():
        print(f"\n--- {category} ---")
        for subcat, units in subcats.items():
            print(f"  [{subcat}]")
            for unit_name, rule in units.items():
                rtype = rule.get("type", "?")
                if rtype == "numeric":
                    th = rule.get("thresholds", [])
                    print(f"    {unit_name}: numeric {[(t, s) for t, s in th]}")
                elif rtype == "ghs_h":
                    m = rule.get("mapping", {})
                    print(f"    {unit_name}: GHS H {list(m.items())[:8]}{'...' if len(m) > 8 else ''}")
                elif rtype == "phrase":
                    p = rule.get("phrases", [])
                    print(f"    {unit_name}: phrases ({len(p)} entries)")
                elif rtype == "text":
                    m = rule.get("mapping", {})
                    print(f"    {unit_name}: text {list(m.keys())[:5]}{'...' if len(m) > 5 else ''}")
    print("\n" + "=" * 70)


# -----------------------------------------------------------------------------
# Scoring Logic
# -----------------------------------------------------------------------------


def _score_numeric(rule: dict, value: float, higher_is_safer: bool = True) -> Optional[int]:
    """
    Score numeric value against thresholds.
    higher_is_safer: True for LD50 (higher = less toxic), False for flash point (lower = less flammable).
    """
    thresh = rule.get("thresholds", [])
    if not thresh:
        return None
    if higher_is_safer:
        # LD50: find highest threshold where value >= threshold
        for t, score in sorted(thresh, reverse=True):
            if value >= t:
                return score
        return thresh[-1][1]  # Most hazardous
    else:
        # Flash point: find lowest threshold where value <= threshold
        for t, score in sorted(thresh):
            if value <= t:
                return score
        return thresh[-1][1]


def _score_ghs_h(rule: dict, h_codes: list[str]) -> Optional[int]:
    """Score based on GHS H-codes. Return highest (most hazardous) match."""
    mapping = rule.get("mapping", {})
    scores = []
    for h in h_codes:
        base = re.sub(r"\s*\([^)]+\)", "", h).strip()
        if base in mapping:
            scores.append(mapping[base])
        for k, v in mapping.items():
            if base in k or k in base:
                scores.append(v)
    return max(scores) if scores else None


def _score_phrase(rule: dict, text: str) -> Optional[int]:
    """Score based on key phrase match (substring)."""
    text_lower = (text or "").lower()
    phrases = rule.get("phrases", [])
    for phrase, score in phrases:
        if phrase and phrase.lower() in text_lower:
            return score
    return None


def _score_text(rule: dict, text: str) -> Optional[int]:
    """Score based on exact or partial text match."""
    mapping = rule.get("mapping", {})
    text_lower = (text or "").lower()
    for k, v in mapping.items():
        if k.lower() in text_lower or text_lower in k.lower():
            return v
    return None


def _extract_ld50(hazard_data: dict) -> Optional[tuple[float, str]]:
    """Extract best LD50 (value, route) from hazard data."""
    tox = hazard_data.get("toxicities", [])
    best = None
    for t in tox:
        val = t.get("value", "")
        unit = t.get("unit")
        species = t.get("species_route") or []
        if "LD50" in val.upper() and ("oral" in str(species).lower() or "mg/kg" in val):
            m = re.search(r"(\d+(?:\.\d+)?)\s*(?:mg/kg|mg/kg bw)", val, re.I)
            if m:
                v = float(m.group(1))
                if best is None or v < best[0]:
                    best = (v, "oral")
    return best


def _extract_lc50_inhalation(hazard_data: dict) -> Optional[float]:
    """Extract LC50 inhalation in ppm from hazard data.
    In inhalation toxicity context, mg/m³ is treated as equivalent to ppm (same numeric value).
    """
    tox = hazard_data.get("toxicities", [])
    for t in tox:
        val = t.get("value", "")
        if "LC50" not in val.upper():
            continue
        if "inhalation" not in val.lower() and "ppm" not in val and "mg/m" not in val:
            continue
        # ppm
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*ppm", val, re.I)
        if m:
            return float(m.group(1).replace(",", "."))
        # mg/m³ or mg/m3 — treat as ppm (equal in inhalation context)
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*mg/m[³3]", val, re.I)
        if m:
            return float(m.group(1).replace(",", "."))
    return None


def _extract_flash_point_c(hazard_data: dict) -> Optional[float]:
    """Extract flash point in °C from hazard data."""
    hm = hazard_data.get("hazard_metrics", {})
    fp_list = hm.get("flash_point", [])
    for fp in fp_list:
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*°?C", str(fp), re.I)
        if m:
            return float(m.group(1))
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*°?F", str(fp), re.I)
        if m:
            return (float(m.group(1)) - 32) * 5 / 9  # Convert to C
    return None


def _extract_vapor_pressure_mmhg(hazard_data: dict) -> Optional[float]:
    """Extract vapor pressure in mmHg."""
    hm = hazard_data.get("hazard_metrics", {})
    other = hm.get("other_designations", [])
    for o in other:
        m = re.search(r"(\d+(?:\.\d+)?)\s*(?:mm\s*Hg|mmHg)", str(o), re.I)
        if m:
            return float(m.group(1))
    return None


def _extract_nfpa_health(hazard_data: dict) -> Optional[int]:
    """Extract NFPA health rating (0-4)."""
    hm = hazard_data.get("hazard_metrics", {})
    nfpa = hm.get("nfpa", [])
    for n in nfpa:
        if "health" in str(n).lower() or "irritation" in str(n).lower():
            m = re.search(r"^(\d)\s*[-–]", str(n))
            if m:
                return int(m.group(1))
    return None


def _extract_nfpa_fire(hazard_data: dict) -> Optional[int]:
    """Extract NFPA fire rating (0-4)."""
    hm = hazard_data.get("hazard_metrics", {})
    nfpa = hm.get("nfpa", [])
    for n in nfpa:
        if "fire" in str(n).lower() or "ignit" in str(n).lower():
            m = re.search(r"^(\d)\s*[-–]", str(n))
            if m:
                return int(m.group(1))
    return None


def _extract_iarc(hazard_data: dict) -> Optional[str]:
    """Extract IARC category (1, 2A, 2B, 3, 4)."""
    tox = hazard_data.get("toxicities", [])
    for t in tox:
        val = t.get("value", "")
        if "IARC" in val.upper():
            for cat in ["Group 1", "Group 2A", "Group 2B", "Group 3", "Group 4", "1", "2A", "2B", "3", "4"]:
                if cat in val:
                    return cat.replace("Group ", "")
    return None


def _extract_epa_carcinogen(hazard_data: dict) -> Optional[str]:
    """Extract EPA carcinogen class (A, B, C, D, E)."""
    tox = hazard_data.get("toxicities", [])
    for t in tox:
        val = t.get("value", "")
        if "EPA" in val and ("carcinogen" in val.lower() or "Group" in val):
            for g in ["Group A", "Group B", "Group C", "Group D", "Group E"]:
                if g in val:
                    return g[-1]
        if "Group D" in val or "Group E" in val:
            m = re.search(r"Group\s*([A-E])", val, re.I)
            if m:
                return m.group(1).upper()
    return None


def _extract_lc50_aquatic(hazard_data: dict) -> Optional[float]:
    """Extract acute aquatic LC50 (mg/L) for fish."""
    tox = hazard_data.get("toxicities", [])
    for t in tox:
        val = t.get("value", "")
        if ("LC50" in val or "EC50" in val) and ("mg/L" in val or "fish" in val.lower() or "trout" in val.lower()):
            m = re.search(r"(\d+(?:[.,]\d+)?)\s*mg/L", val, re.I)
            if m:
                return float(m.group(1).replace(",", "."))
    return None


def compute_p2oasys_scores(
    hazard_data: dict[str, Any],
    matrix: dict[str, Any],
) -> dict[str, Any]:
    """
    Compute P2OASys scores from hazard data using the loaded matrix.
    Returns {category: {subcategory: {unit: score, ...}, "max_score": N}, ...}
    """
    ghs = hazard_data.get("ghs", {})
    h_codes = ghs.get("h_codes", [])
    results: dict[str, Any] = {}

    # Extract values from hazard data (PubChem first)
    ld50 = _extract_ld50(hazard_data)
    lc50_inh = _extract_lc50_inhalation(hazard_data)
    flash_c = _extract_flash_point_c(hazard_data)
    vp = _extract_vapor_pressure_mmhg(hazard_data)
    nfpa_health = _extract_nfpa_health(hazard_data)
    nfpa_fire = _extract_nfpa_fire(hazard_data)
    iarc = _extract_iarc(hazard_data)
    epa_carc = _extract_epa_carcinogen(hazard_data)
    lc50_aq = _extract_lc50_aquatic(hazard_data)

    # Optionally supplement with local CompTox (ToxRefDB) POD data when present.
    # We use ToxRefDB POD values only as a fallback when PubChem is missing.
    comptox = hazard_data.get("comptox") or {}
    toxref = comptox.get("toxrefdb") or {}
    # toxref structure from hazard_query_toxrefdb.get_toxref_hazard_from_cas:
    # { "input_cas": ..., "dtxsid": ..., "n_records": N,
    #   "toxrefdb": { "records": [...], "summary": {"min_NOAEL": x, "min_LOAEL": y, "min_LEL": z} } }
    toxref_inner = toxref.get("toxrefdb") or {}
    tox_summary = toxref_inner.get("summary") or {}

    if ld50 is None and tox_summary:
        # Prefer LOAEL as a surrogate LD50, then NOAEL, then LEL.
        for key in ("min_LOAEL", "min_NOAEL", "min_LEL"):
            val = tox_summary.get(key)
            if isinstance(val, (int, float)):
                ld50 = (float(val), "oral_toxrefdb")
                break

    for category, subcats in matrix.items():
        results[category] = {}
        cat_max = 2
        for subcat, units in subcats.items():
            sub_scores = {}
            for unit_name, rule in units.items():
                score = None
                rtype = rule.get("type", "")

                if rtype == "numeric":
                    if "LD50" in unit_name and "Oral" in subcat:
                        if ld50:
                            score = _score_numeric(rule, ld50[0], higher_is_safer=True)
                    elif "LD50" in unit_name and "Dermal" in subcat:
                        if ld50:
                            score = _score_numeric(rule, ld50[0], higher_is_safer=True)
                    elif "LC50" in unit_name and "Inhalation" in subcat:
                        if lc50_inh is not None:
                            score = _score_numeric(rule, lc50_inh, higher_is_safer=True)
                    elif "Flash" in unit_name or "deg C" in unit_name:
                        if flash_c is not None:
                            score = _score_numeric(rule, flash_c, higher_is_safer=False)
                    elif "mm Hg" in unit_name or "Vapor" in unit_name:
                        if vp is not None:
                            score = _score_numeric(rule, vp, higher_is_safer=False)
                    elif "NFPA" in unit_name or "HMIS" in unit_name:
                        if "Health" in subcat or "health" in unit_name.lower():
                            if nfpa_health is not None:
                                for t, s in rule.get("thresholds", []):
                                    if t == nfpa_health:
                                        score = s
                                        break
                        elif "Fire" in subcat or "Flammability" in subcat:
                            if nfpa_fire is not None:
                                for t, s in rule.get("thresholds", []):
                                    if t == nfpa_fire:
                                        score = s
                                        break
                    elif "LC50" in unit_name and "Aquatic" in str(subcats):
                        if lc50_aq is not None:
                            score = _score_numeric(rule, lc50_aq, higher_is_safer=True)

                elif rtype == "ghs_h" and h_codes:
                    score = _score_ghs_h(rule, h_codes)

                elif rtype == "text":
                    if "IARC" in unit_name and iarc:
                        for k, v in rule.get("mapping", {}).items():
                            if iarc in k or k in str(iarc):
                                score = v
                                break
                    elif "EPA" in unit_name and epa_carc:
                        for k, v in rule.get("mapping", {}).items():
                            if epa_carc in k.upper():
                                score = v
                                break

                if score is not None:
                    sub_scores[unit_name] = score
                    cat_max = max(cat_max, score)

            if sub_scores:
                results[category][subcat] = {**sub_scores, "_max": max(sub_scores.values())}
        if results[category]:
            results[category]["_category_max"] = cat_max

    return results


# -----------------------------------------------------------------------------
# Output
# -----------------------------------------------------------------------------


def print_p2oasys_summary(identifier: str, scores: dict[str, Any]) -> None:
    """Print P2OASys score summary."""
    print("\n" + "=" * 70)
    print(f"P2OASys HAZARD SCORES: {identifier}")
    print("=" * 70)
    print("(Scores 2-10: higher = more hazardous; from TURI P2OASys matrix)\n")

    for category, data in scores.items():
        if category.startswith("_"):
            continue
        if isinstance(data, dict) and "_category_max" in data:
            cmax = data["_category_max"]
            print(f"--- {category} (category max: {cmax}) ---")
            for k, v in data.items():
                if k.startswith("_"):
                    continue
                if isinstance(v, dict):
                    submax = v.get("_max", "-")
                    print(f"  {k} (max: {submax}):")
                    for uk, uv in v.items():
                        if uk != "_max":
                            print(f"    {uk}: {uv}")
                else:
                    print(f"  {k}: {v}")
        else:
            print(f"--- {category} ---")
            print(f"  {data}")
    print("\n" + "=" * 70)


