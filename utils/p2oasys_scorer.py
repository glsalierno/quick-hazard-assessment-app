"""
P2OASys Hazard Score Calculator (Quick Hazard Assessment app).

Maps hazard data to P2OASys scores using the TURI Hazard Matrix Excel file.
Used by the P2OASys scoring tab. https://p2oasys.turi.org/chemical/hazard-score-matrix

**Category vs subcategory (TURI methodology):**
Matrix units are scored on the **2–10** band. Within each **subcategory**, the score is
the **maximum** among its units (most conservative). The **category** score stored under
``_category_max`` is the **mean of the two highest** subcategory scores (or the mean of
all scored subcategories if there are fewer than two), matching the official “top two
subcategories” rule—not the max across every unit in the sheet.
"""

import hashlib
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

# v6 hardening: bump when scoring semantics change so audit traces are comparable.
SCORER_VERSION = "p2oasys_scorer_v6.5_ph_cascade"

# Ideal-gas factor for mg/m³ → ppm at 25 °C, 1 atm (TURI / EPA convention).
_MGM3_TO_PPM_FACTOR = 24.45

# Explicit assessment status labels (prefer these over silent blanks).
STATUS_SCORED = "Scored"
STATUS_NO_DATA = "No data"
STATUS_NOT_ASSESSED = "Not assessed"
STATUS_PREDICTED_ONLY = "Predicted only"
STATUS_CONFLICTING = "Conflicting evidence"
STATUS_SOURCE_UNAVAILABLE = "Source unavailable"

# Routes that count as oral (or oral-equivalent) for Acute oral LD50.
# TURI / P2OASys practice: intraperitoneal (i.p. / ip) is treated as oral.
# Also accept common oral synonyms so true oral rows are not rejected.
_ORAL_ROUTE_RE = re.compile(
    r"(?:"
    r"\boral\b|\bperoral\b|per\s*os|\bp\.\s*o\.\b|\bpo\b|\bgavage\b|"
    r"drinking\s*water|\bfeed\b|\bdietary\b|"
    r"\bintraperitoneal\b|\bi\.\s*p\.\b|\bip\b"
    r")",
    re.I,
)

# Routes that disqualify a record from oral LD50 when no oral-equivalent token is present.
# Intraperitoneal / i.p. / ip are intentionally NOT listed (they are oral-equivalent).
# Word-bounded so short abbreviations (iv, sc, im) do not match inside other words.
_NON_ORAL_ROUTE_RE = re.compile(
    r"\b(?:dermal|skin|inhalation|intravenous|subcutaneous|"
    r"intramuscular|i\.?v\.?|s\.?c\.?|i\.?m\.?|parenteral)\b",
    re.I,
)


def _has_oral_equivalent_route(combined_lower: str) -> bool:
    """True when text/route mentions oral or oral-equivalent (incl. intraperitoneal)."""
    if not combined_lower:
        return False
    # Normalize common punctuated forms before regex.
    s = combined_lower.replace("i.p.", " ip ").replace("i.p", " ip ")
    s = s.replace("p.o.", " po ").replace("p.o", " po ")
    return bool(_ORAL_ROUTE_RE.search(s))

# Endpoints that are point-of-departure / repeated-dose values, NOT acute lethality.
# They must never be scored as LD50/LC50 (a core v6 contamination fix).
_NON_ACUTE_ENDPOINT_TOKENS = (
    "noael",
    "loael",
    "noel",
    "loel",
    "lel",
    "noec",
    "loec",
    "td50",
    "bmd",
    "bmdl",
    "reference dose",
    "rfd",
    "rfc",
    "tdi",
    "adi",
)


def _num(raw: Any) -> Optional[float]:
    """
    Parse a measured number, treating commas correctly.

    ``3,900`` is 3900 (thousands separator), not 3.9. ``3,9`` (comma + 1-2 digits)
    is treated as a decimal comma (3.9). Groupings like ``1,234,567.8`` are handled.
    Returns ``None`` when no finite number can be parsed.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            f = float(raw)
        except (TypeError, ValueError):
            return None
        return f if f == f else None
    s = str(raw).strip()
    if not s:
        return None
    # Grouped thousands (optionally with a decimal point): 3,900 / 1,234,567.8
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?", s):
        s = s.replace(",", "")
    # Decimal comma: 3,9 or 12,45 (comma followed by 1-2 digits, no grouping)
    elif re.fullmatch(r"\d+,\d{1,2}", s):
        s = s.replace(",", ".")
    else:
        # Any remaining commas are grouping separators.
        s = s.replace(",", "")
    try:
        f = float(s)
    except (ValueError, TypeError):
        return None
    return f if f == f else None


def parse_measured_value(
    raw: Any,
    *,
    higher_is_safer: bool = True,
) -> Optional[dict[str, Any]]:
    """
    Parse a measured value with optional qualifier / range.

    Returns ``{"value", "qualifier", "raw"}`` where ``qualifier`` is one of
    ``None``, ``"<"``, ``">"``, ``"range"``. For ranges, the more hazardous end
    is selected (min when higher_is_safer, max otherwise). Qualifier bounds are
    scored at the bound and flagged so the assessment remains transparent.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = _num(raw)
        return {"value": v, "qualifier": None, "raw": raw} if v is not None else None
    s = str(raw).strip()
    if not s:
        return None
    # Range: 100-200 or 100–200 (en dash)
    m_range = re.search(
        r"(\d[\d,]*(?:\.\d+)?)\s*[-–—]\s*(\d[\d,]*(?:\.\d+)?)",
        s,
    )
    if m_range:
        a = _num(m_range.group(1))
        b = _num(m_range.group(2))
        if a is None or b is None:
            return None
        lo, hi = (a, b) if a <= b else (b, a)
        chosen = lo if higher_is_safer else hi
        return {"value": chosen, "qualifier": "range", "raw": s, "range": [lo, hi]}
    m_q = re.search(r"([<>≤≥])\s*(\d[\d,]*(?:\.\d+)?)", s)
    if m_q:
        sym = m_q.group(1)
        v = _num(m_q.group(2))
        if v is None:
            return None
        if sym in ("<", "≤"):
            return {"value": v, "qualifier": "<", "raw": s}
        return {"value": v, "qualifier": ">", "raw": s}
    # Plain number (may sit beside units)
    m = re.search(r"(\d[\d,]*(?:\.\d+)?)", s)
    if m:
        v = _num(m.group(1))
        if v is not None:
            return {"value": v, "qualifier": None, "raw": s}
    return None


def mgm3_to_ppm(mg_m3: float, molecular_weight: float) -> Optional[float]:
    """Convert mg/m³ to ppm at 25 °C / 1 atm: ppm = mg/m³ × 24.45 / MW."""
    mw = _num(molecular_weight)
    conc = _num(mg_m3)
    if mw is None or conc is None or mw <= 0:
        return None
    return (conc * _MGM3_TO_PPM_FACTOR) / mw

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


def matrix_fingerprint(excel_path: Path) -> dict[str, Any]:
    """
    Return an audit fingerprint for the scoring matrix workbook.

    ``{"path", "filename", "exists", "sha256", "size_bytes", "mtime"}``. The sha256 lets
    an assessment record pin the exact matrix version used, so scores are reproducible
    and it is unambiguous whether the official TURI workbook or a dev placeholder ran.
    """
    p = Path(excel_path)
    info: dict[str, Any] = {
        "path": str(p),
        "filename": p.name,
        "exists": p.is_file(),
        "sha256": None,
        "size_bytes": None,
        "mtime": None,
    }
    if p.is_file():
        h = hashlib.sha256()
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        info["sha256"] = h.hexdigest()
        stat = p.stat()
        info["size_bytes"] = stat.st_size
        info["mtime"] = stat.st_mtime
    return info


def _parse_numeric_threshold(val: Any) -> Optional[float]:
    """Parse numeric threshold from cell (handles '>100', '<50', etc.)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan" or s == ".":
        return None
    # Extract number from patterns like ">100", "<50", "5,000", "0.05", "6-7".
    # Allow thousands separators so ">1,000" parses as 1000, not 1.
    m = re.search(r"[<>]?\s*(\d[\d,]*\.?\d*|\d*\.\d+)", s)
    if m:
        parsed = _num(m.group(1))
        if parsed is not None:
            return parsed
    return _num(s)


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
                # Allow short labels like "pH" (len==2); skip pure numerics.
                if len(c0) >= 2 and not re.match(r"^[\d.\s]+$", c0):
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

    Every returned rule includes ``cell_labels``: ``{score: original cell text}``
    for human-in-the-loop form dropdowns (see ``utils.p2oasys_form``).
    """
    if not values:
        return None
    cell_labels: dict[int, str] = {}
    for i, v in enumerate(values):
        if i >= len(SCORE_COLS):
            break
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s:
            cell_labels[SCORE_COLS[i]] = s
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
            return {"type": "ghs_h", "unit": unit_name, "mapping": mapping, "cell_labels": cell_labels}
    # Key phrases - substring match for hazard descriptions
    if "KEY PHRASE" in u or "KEY WORD" in u:
        phrases = [str(v).strip() for v in values if v and not (isinstance(v, float) and pd.isna(v))]
        if phrases:
            return {"type": "phrase", "unit": unit_name, "phrases": list(zip(phrases, SCORE_COLS)), "cell_labels": cell_labels}
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
            return {"type": "text", "unit": unit_name, "mapping": mapping, "cell_labels": cell_labels}
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
            return {"type": "text", "unit": unit_name, "mapping": mapping, "cell_labels": cell_labels}
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
            return {"type": "text", "unit": unit_name, "mapping": mapping, "cell_labels": cell_labels}
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
            "cell_labels": cell_labels,
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
        return {"type": "text", "unit": unit_name, "mapping": mapping, "cell_labels": cell_labels}
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


def _tox_text_and_route(entry: dict) -> tuple[str, str]:
    """Return (value_text, combined_route_species_lower) for a toxicity entry."""
    val = str(entry.get("value", ""))
    species = entry.get("species_route") or []
    if isinstance(species, (list, tuple)):
        species_s = " ".join(str(x) for x in species)
    else:
        species_s = str(species)
    route = str(entry.get("route") or "")
    exposure = str(entry.get("exposure_route") or entry.get("exposure") or "")
    sp = f"{species_s} {route} {exposure}".lower()
    return val, sp


def _is_non_acute_endpoint(val_lower: str) -> bool:
    """True if the text is a POD/repeated-dose endpoint (NOAEL/LOAEL/TD50/…), not acute lethality."""
    return any(tok in val_lower for tok in _NON_ACUTE_ENDPOINT_TOKENS)


def _reject(rejections: Optional[list], endpoint: str, value: Any, reason: str) -> None:
    if rejections is not None:
        rejections.append({"endpoint": endpoint, "value": str(value)[:160], "reason": reason})


def _extract_ld50_oral(
    hazard_data: dict, rejections: Optional[list] = None
) -> Optional[dict[str, Any]]:
    """Extract most conservative oral LD50 (lowest mg/kg).

    Accepts oral and oral-equivalent routes (gavage, p.o., intraperitoneal / i.p.).
    Rejects dermal / inhalation / iv / sc / im when no oral-equivalent token is present,
    and rejects non-acute POD endpoints (NOAEL/LOAEL/…). Returns
    ``{"value", "route", "qualifier", "raw"}`` or ``None``.

    If both oral-equivalent and a non-oral token appear (e.g. "oral/dermal"), the
    oral-equivalent wins so true oral rows are not falsely rejected.
    """
    tox = hazard_data.get("toxicities", [])
    best: Optional[dict[str, Any]] = None
    for t in tox:
        val, sp = _tox_text_and_route(t)
        vl = val.lower()
        if "ld50" not in vl:
            continue
        if _is_non_acute_endpoint(vl):
            _reject(rejections, "oral LD50", val, "non-acute endpoint (POD), not LD50")
            continue
        if "mg/kg" not in vl.replace(" ", ""):
            continue
        combined = vl + " " + sp
        if _has_oral_equivalent_route(combined):
            pass  # oral / IP / gavage / p.o. — keep
        elif _NON_ORAL_ROUTE_RE.search(combined):
            _reject(rejections, "oral LD50", val, "route is not oral")
            continue
        else:
            _reject(rejections, "oral LD50", val, "oral route not confirmed")
            continue
        m = re.search(r"([<>≤≥]?\s*\d[\d,]*(?:\.\d+)?(?:\s*[-–—]\s*\d[\d,]*(?:\.\d+)?)?)\s*(?:mg/kg|mg/kg bw)", val, re.I)
        if not m:
            continue
        parsed = parse_measured_value(m.group(1), higher_is_safer=True)
        if parsed is None:
            continue
        if best is None or parsed["value"] < best["value"]:
            best = {
                **parsed,
                "route": "oral",
                "unit": "mg/kg",
                "predicted": bool(t.get("predicted")),
            }
    return best


def _extract_ld50_dermal(
    hazard_data: dict, rejections: Optional[list] = None
) -> Optional[dict[str, Any]]:
    """Extract most conservative dermal LD50 (lowest mg/kg) when route is explicitly dermal.

    There is deliberately **no** fallback to oral LD50: an oral value is not a valid
    substitute for dermal acute toxicity.
    """
    tox = hazard_data.get("toxicities", [])
    best: Optional[dict[str, Any]] = None
    for t in tox:
        val, sp = _tox_text_and_route(t)
        vl = val.lower()
        if "ld50" not in vl:
            continue
        if _is_non_acute_endpoint(vl):
            _reject(rejections, "dermal LD50", val, "non-acute endpoint (POD), not LD50")
            continue
        if "mg/kg" not in vl.replace(" ", ""):
            continue
        combined = vl + " " + sp
        if "dermal" not in combined and "skin" not in combined:
            _reject(rejections, "dermal LD50", val, "dermal route not confirmed")
            continue
        m = re.search(r"([<>≤≥]?\s*\d[\d,]*(?:\.\d+)?(?:\s*[-–—]\s*\d[\d,]*(?:\.\d+)?)?)\s*(?:mg/kg|mg/kg bw)", val, re.I)
        if not m:
            continue
        parsed = parse_measured_value(m.group(1), higher_is_safer=True)
        if parsed is None:
            continue
        if best is None or parsed["value"] < best["value"]:
            best = {**parsed, "route": "dermal", "unit": "mg/kg"}
    return best


def _extract_lc50_inhalation(
    hazard_data: dict, rejections: Optional[list] = None
) -> Optional[dict[str, Any]]:
    """Extract most conservative inhalation LC50 in **ppm** (lowest ppm = highest hazard).

    ``mg/m³`` values are converted to ppm when ``hazard_data["molecular_weight"]`` is
    available (25 °C / 1 atm). Without MW they are rejected, not silently scored as ppm.
    """
    tox = hazard_data.get("toxicities", [])
    mw = _num(hazard_data.get("molecular_weight"))
    ppm_candidates: list[dict[str, Any]] = []
    for t in tox:
        val, _sp = _tox_text_and_route(t)
        vl = val.lower()
        if "lc50" not in vl:
            continue
        if _is_non_acute_endpoint(vl):
            _reject(rejections, "inhalation LC50", val, "non-acute endpoint, not LC50")
            continue
        if "inhalation" not in vl and "ppm" not in vl and "mg/m" not in vl:
            continue
        m = re.search(r"([<>≤≥]?\s*\d[\d,]*(?:\.\d+)?(?:\s*[-–—]\s*\d[\d,]*(?:\.\d+)?)?)\s*ppm", val, re.I)
        if m:
            parsed = parse_measured_value(m.group(1), higher_is_safer=True)
            if parsed is not None:
                ppm_candidates.append({**parsed, "unit": "ppm", "source_unit": "ppm"})
            continue
        m_mg = re.search(r"([<>≤≥]?\s*\d[\d,]*(?:\.\d+)?(?:\s*[-–—]\s*\d[\d,]*(?:\.\d+)?)?)\s*mg/m[³3]", val, re.I)
        if m_mg:
            parsed = parse_measured_value(m_mg.group(1), higher_is_safer=True)
            if parsed is None:
                continue
            if mw is None or mw <= 0:
                _reject(
                    rejections,
                    "inhalation LC50",
                    val,
                    "mg/m³ not converted to ppm (needs molecular weight); not scored",
                )
                continue
            ppm = mgm3_to_ppm(parsed["value"], mw)
            if ppm is None:
                _reject(rejections, "inhalation LC50", val, "mg/m³→ppm conversion failed")
                continue
            ppm_candidates.append({
                "value": ppm,
                "qualifier": parsed.get("qualifier"),
                "raw": val,
                "unit": "ppm",
                "source_unit": "mg/m3",
                "molecular_weight": mw,
                "mg_m3": parsed["value"],
            })
    if not ppm_candidates:
        return None
    return min(ppm_candidates, key=lambda c: c["value"])


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
    hm = hazard_data.get("hazard_metrics", {}) or {}
    for item in hm.get("vapor_pressure_mmhg") or []:
        try:
            return float(item)
        except (TypeError, ValueError):
            m = re.search(r"(\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)", str(item))
            if m:
                return float(m.group(1))
    meta = hazard_data.get("vp_meta") or {}
    if meta.get("vp_mmhg_25c") is not None:
        try:
            return float(meta["vp_mmhg_25c"])
        except (TypeError, ValueError):
            pass
    other = hm.get("other_designations", [])
    for o in other:
        m = re.search(r"(\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*(?:mm\s*Hg|mmHg)", str(o), re.I)
        if m:
            return float(m.group(1))
    return None


def _extract_nfpa_digit(text: str, *, kind: str) -> Optional[int]:
    """Parse a 0-4 NFPA digit from one hazard_metrics.nfpa string."""
    s = str(text)
    low = s.lower()
    if kind == "health" and not ("health" in low or "irritation" in low):
        return None
    if kind == "fire" and not ("fire" in low or "ignit" in low or "flamm" in low):
        return None
    m = re.search(r"^(\d)\s*[-–]", s)
    if m:
        return int(m.group(1))
    m = re.search(r"\b([0-4])\b", s)
    if m:
        return int(m.group(1))
    return None


def _extract_nfpa_health(hazard_data: dict) -> Optional[int]:
    """Extract NFPA health rating (0-4); precautionary max across sources."""
    hm = hazard_data.get("hazard_metrics", {})
    best: Optional[int] = None
    for n in hm.get("nfpa", []) or []:
        v = _extract_nfpa_digit(n, kind="health")
        if v is None:
            continue
        if best is None or v > best:
            best = v
    return best


def _extract_nfpa_fire(hazard_data: dict) -> Optional[int]:
    """Extract NFPA fire rating (0-4); precautionary max across sources."""
    hm = hazard_data.get("hazard_metrics", {})
    best: Optional[int] = None
    for n in hm.get("nfpa", []) or []:
        v = _extract_nfpa_digit(n, kind="fire")
        if v is None:
            continue
        if best is None or v > best:
            best = v
    return best


# Phrases that negate an IARC listing for the subject compound.
_IARC_NEGATION_RE = re.compile(
    r"\b(?:not\s+(?:listed|classified|evaluated|assign\w*|identif\w*)|"
    r"no(?:t)?\s+(?:evidence|data)|un(?:listed|classified)|"
    r"has\s+not\s+been\s+(?:classified|evaluated|listed))\b",
    re.I,
)
# Qualifiers suggesting the IARC group is attributed to a *different* substance,
# not the subject compound (e.g. "certain nitrosamines … Groups 2A and 2B").
_IARC_OTHER_SUBJECT_RE = re.compile(
    r"\b(?:certain|such\s+as|e\.?g\.?|nitrosamine|impurit|metabolite|"
    r"by-?product|contaminant|degradation\s+product|respectively|related\s+compound)\b",
    re.I,
)
_IARC_GROUP_RE = re.compile(r"\bGroups?\s*(1|2A|2B|3|4)\b", re.I)
# Hazard order: Group 1 is most hazardous, Group 4 least.
_IARC_RANK = {"1": 0, "2A": 1, "2B": 2, "3": 3, "4": 4}


def _extract_iarc(hazard_data: dict, rejections: Optional[list] = None) -> Optional[str]:
    """Extract IARC category (1, 2A, 2B, 3, 4) with negation- and subject-awareness.

    Only explicit ``Group N`` classifications count (never a bare digit found anywhere
    in the text). Sentences that negate listing (``Not listed by IARC``) or attribute a
    group to another substance (``certain nitrosamines … Groups 2A and 2B``) are
    skipped. Returns the most hazardous valid group across accepted sentences.
    """
    tox = hazard_data.get("toxicities", [])
    candidates: list[str] = []
    for t in tox:
        val = str(t.get("value", ""))
        if "iarc" not in val.lower():
            continue
        # Evaluate sentence-by-sentence so a negation in one clause does not
        # suppress a real classification in another, and vice versa.
        for sentence in re.split(r"(?<=[.;])\s+", val):
            if "iarc" not in sentence.lower() and not _IARC_GROUP_RE.search(sentence):
                continue
            if _IARC_NEGATION_RE.search(sentence):
                _reject(rejections, "IARC", sentence, "explicitly not listed/classified")
                continue
            if _IARC_OTHER_SUBJECT_RE.search(sentence):
                _reject(rejections, "IARC", sentence, "group attributed to another substance")
                continue
            m = _IARC_GROUP_RE.search(sentence)
            if m:
                candidates.append(m.group(1).upper())
    if not candidates:
        return None
    return min(candidates, key=lambda c: _IARC_RANK.get(c, 99))


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



def _unpack_numeric_evidence(raw: Any) -> tuple[Optional[float], bool]:
    """Return (value, predicted) from a float or ``{"value": ..., "predicted": ...}``."""
    if raw is None:
        return None, False
    if isinstance(raw, dict):
        v = _num(raw.get("value"))
        return v, bool(raw.get("predicted"))
    return _num(raw), False


def _extract_log_kow(hazard_data: dict) -> Optional[dict[str, Any]]:
    """
    Extract log Kow / Pow for Fate Bioaccumulation.

    Prefers structured ``log_kow`` (OPERA/extra), then hazard_metrics.log_kow,
    then PubChem-style XLogP / logP toxicities (experimental preferred when not predicted).
    """
    v, pred = _unpack_numeric_evidence(hazard_data.get("log_kow"))
    if v is not None:
        return {"value": v, "predicted": pred, "source": "log_kow"}
    hm = hazard_data.get("hazard_metrics") or {}
    for item in hm.get("log_kow") or []:
        vv, pp = _unpack_numeric_evidence(item)
        if vv is not None:
            return {"value": vv, "predicted": pp or True, "source": "hazard_metrics.log_kow"}
    for t in hazard_data.get("toxicities") or []:
        val = str(t.get("value") or "")
        low = val.lower()
        if "logp" in low or "log kow" in low or "xlogp" in low or "log pow" in low:
            import re
            m = re.search(r"(-?\d+(?:\.\d+)?)", val.replace(",", ""))
            if m:
                try:
                    num = float(m.group(1))
                except ValueError:
                    continue
                return {
                    "value": num,
                    "predicted": bool(t.get("predicted")),
                    "source": str(t.get("source") or "toxicity"),
                }
    return None


def _extract_bcf_l_kg(hazard_data: dict) -> Optional[dict[str, Any]]:
    """Extract BAF/BCF in L/kg (linear). Accepts structured bcf_l_kg or LogBCF→10**x."""
    v, pred = _unpack_numeric_evidence(hazard_data.get("bcf_l_kg"))
    if v is not None:
        src = hazard_data.get("bcf_l_kg") if isinstance(hazard_data.get("bcf_l_kg"), dict) else {}
        return {
            "value": v,
            "predicted": pred,
            "source": (src or {}).get("source") if isinstance(src, dict) else "bcf_l_kg",
            "log_bcf": (src or {}).get("log_bcf") if isinstance(src, dict) else None,
        }
    hm = hazard_data.get("hazard_metrics") or {}
    for item in hm.get("bcf_l_kg") or []:
        vv, pp = _unpack_numeric_evidence(item)
        if vv is not None:
            return {"value": vv, "predicted": pp or True, "source": "hazard_metrics.bcf_l_kg"}
    import re
    for t in hazard_data.get("toxicities") or []:
        val = str(t.get("value") or "")
        low = val.lower()
        if "logbcf" in low.replace(" ", "") or "log bcf" in low:
            m = re.search(r"(-?\d+(?:\.\d+)?)", val.replace(",", ""))
            if m:
                try:
                    log_bcf = float(m.group(1))
                except ValueError:
                    continue
                return {
                    "value": 10.0 ** log_bcf,
                    "predicted": bool(t.get("predicted")),
                    "source": str(t.get("source") or "toxicity"),
                    "log_bcf": log_bcf,
                }
        if "bcf" in low and "l/kg" in low.replace(" ", ""):
            m = re.search(r"(-?\d+(?:\.\d+)?)", val.replace(",", ""))
            if m:
                try:
                    num = float(m.group(1))
                except ValueError:
                    continue
                return {
                    "value": num,
                    "predicted": bool(t.get("predicted")),
                    "source": str(t.get("source") or "toxicity"),
                }
    return None


def _extract_biodeg_half_life_days(hazard_data: dict) -> Optional[dict[str, Any]]:
    """Optional BOD-style half-life days from OPERA BioDeg (not Persistence water/soil t½)."""
    v, pred = _unpack_numeric_evidence(hazard_data.get("biodeg_half_life_days"))
    if v is not None:
        return {"value": v, "predicted": pred, "source": "biodeg_half_life_days"}
    return None


def _extract_lc50_aquatic(hazard_data: dict) -> Optional[float]:
    """Extract most conservative acute aquatic LC50 / EC50 (lowest mg/L)."""
    candidates: list[float] = []
    for key in ("lc50_aquatic_mg_l", "aquatic_toxicity"):
        raw = hazard_data.get(key)
        if isinstance(raw, dict):
            raw = raw.get("value")
        v = _num(raw)
        if v is not None and v > 0:
            candidates.append(float(v))
    tox = hazard_data.get("toxicities", [])
    for t in tox:
        val = str(t.get("value", ""))
        if ("LC50" in val or "EC50" in val) and (
            "mg/L" in val
            or "fish" in val.lower()
            or "trout" in val.lower()
            or "aquatic" in val.lower()
            or "daphn" in val.lower()
            or "algae" in val.lower()
        ):
            m = re.search(r"(\d[\d,]*(?:\.\d+)?)\s*mg/L", val, re.I)
            if m:
                v = _num(m.group(1))
                if v is not None:
                    candidates.append(v)
    return min(candidates) if candidates else None



def _extract_gwp100(hazard_data: dict) -> Optional[float]:
    """Extract GWP100 (relative to CO2) from hazard_metrics or phrase designations."""
    hm = hazard_data.get("hazard_metrics") or {}
    for item in hm.get("gwp100") or []:
        v = _num(item)
        if v is not None:
            return float(v)
    meta = hazard_data.get("gwp_meta") or {}
    if meta.get("gwp100") is not None:
        v = _num(meta.get("gwp100"))
        if v is not None:
            return float(v)
    for item in hm.get("other_designations") or []:
        m = re.search(r"\bGWP\s*[:=]?\s*(-?\d[\d,]*(?:\.\d+)?)", str(item), re.I)
        if m:
            v = _num(m.group(1))
            if v is not None:
                return float(v)
    for t in hazard_data.get("toxicities") or []:
        m = re.search(r"\bGWP\s*[:=]?\s*(-?\d[\d,]*(?:\.\d+)?)", str(t.get("value") or ""), re.I)
        if m:
            v = _num(m.group(1))
            if v is not None:
                return float(v)
    return None


def _extract_odp(hazard_data: dict) -> Optional[float]:
    """Extract ozone depletion potential from hazard_metrics or phrase designations."""
    hm = hazard_data.get("hazard_metrics") or {}
    for item in hm.get("odp") or []:
        v = _num(item)
        if v is not None:
            return float(v)
    for item in hm.get("other_designations") or []:
        m = re.search(r"\bODP\s*[:=]?\s*(-?\d[\d,]*(?:\.\d+)?)", str(item), re.I)
        if m:
            v = _num(m.group(1))
            if v is not None:
                return float(v)
    for t in hazard_data.get("toxicities") or []:
        m = re.search(r"\bODP\s*[:=]?\s*(-?\d[\d,]*(?:\.\d+)?)", str(t.get("value") or ""), re.I)
        if m:
            v = _num(m.group(1))
            if v is not None:
                return float(v)
    return None


def _category_score_mean_top_two_subcategories(subcategory_maxima: list[float]) -> Optional[float]:
    """
    Official-style P2OASys **category** score from subcategory maxima.

    Each value is already the conservative score for one subcategory (max over its
    matrix units). TURI aggregates categories using the **mean of the two highest**
    subcategory scores; with only one scored subcategory, that single value is used.
    """
    if not subcategory_maxima:
        return None
    ranked = sorted(subcategory_maxima, reverse=True)
    k = min(2, len(ranked))
    return sum(ranked[:k]) / float(k)


def _rule_summary(rule: dict) -> dict[str, Any]:
    """Compact, JSON-friendly description of the matrix rule that produced a score."""
    rtype = rule.get("type", "")
    out: dict[str, Any] = {"type": rtype, "unit": rule.get("unit")}
    if rule.get("cell_labels"):
        out["cell_labels"] = {int(k): str(v) for k, v in rule["cell_labels"].items()}
    if rtype == "numeric":
        out["thresholds"] = [[t, s] for t, s in rule.get("thresholds", [])]
    elif rtype == "ghs_h":
        out["mapping"] = dict(rule.get("mapping", {}))
    elif rtype == "text":
        out["mapping"] = dict(rule.get("mapping", {}))
    elif rtype == "phrase":
        out["phrases"] = [[p, s] for p, s in rule.get("phrases", [])]
    return out


def compute_p2oasys_scores(
    hazard_data: dict[str, Any],
    matrix: dict[str, Any],
) -> dict[str, Any]:
    """
    Compute P2OASys scores from hazard data using the loaded matrix.

    Returns ``{category: {subcategory: {unit: score, ..., "_max": ...}, "_category_max": float}}``.
    ``_category_max`` is the **mean of the two highest subcategory ``_max`` values**
    (TURI category rule); the key name is kept for backward compatibility with the app.

    This is the stable public entry point; its return shape is unchanged. For an
    auditable decision trace use :func:`compute_p2oasys_scores_with_trace`.
    """
    scores, _trace = compute_p2oasys_scores_with_trace(hazard_data, matrix)
    return scores


def compute_p2oasys_scores_with_trace(
    hazard_data: dict[str, Any],
    matrix: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Compute P2OASys scores and a full decision trace.

    Returns ``(scores, trace)`` where ``scores`` has the exact same shape as
    :func:`compute_p2oasys_scores`, and ``trace`` is::

        {
          "scorer_version": str,
          "evidence": {endpoint: {...} | None},
          "rejected": [...],
          "scored": [...],
          "missing": [{"category", "subcategory", "unit", "status", "reason"}, ...],
          "category_status": {category: status},
        }

    The trace makes every non-null score explainable: the selected evidence, the
    normalized value, the matrix rule, and the resulting score, plus the candidates
    that were rejected and the matrix units that remain ``No data`` / ``Not assessed``.
    """
    ghs = hazard_data.get("ghs", {})
    h_codes = ghs.get("h_codes", [])
    results: dict[str, Any] = {}
    rejected: list[dict[str, Any]] = []
    scored: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []

    # Extract values from hazard data (PubChem first); collect rejection reasons.
    ld50_oral = _extract_ld50_oral(hazard_data, rejected)
    ld50_dermal = _extract_ld50_dermal(hazard_data, rejected)
    lc50_inh = _extract_lc50_inhalation(hazard_data, rejected)
    flash_c = _extract_flash_point_c(hazard_data)
    vp = _extract_vapor_pressure_mmhg(hazard_data)
    nfpa_health = _extract_nfpa_health(hazard_data)
    nfpa_fire = _extract_nfpa_fire(hazard_data)
    iarc = _extract_iarc(hazard_data, rejected)
    epa_carc = _extract_epa_carcinogen(hazard_data)
    lc50_aq = _extract_lc50_aquatic(hazard_data)
    gwp100 = _extract_gwp100(hazard_data)
    odp = _extract_odp(hazard_data)
    log_kow = _extract_log_kow(hazard_data)
    bcf_l_kg = _extract_bcf_l_kg(hazard_data)
    biodeg_hl = _extract_biodeg_half_life_days(hazard_data)

    # Physical Properties → pH: exp 1% pH → pKa (exp/OPERA) → FG SMARTS last resort.
    ph_info = None
    try:
        from utils.p2oasys_ph import estimate_ph_for_hazard

        ph_info = estimate_ph_for_hazard(hazard_data)
    except Exception:
        ph_info = None

    # Phrase corpus for KEY PHRASE / ODP / GWP style matrix rows.
    phrase_corpus_parts: list[str] = []
    for t in hazard_data.get("toxicities") or []:
        phrase_corpus_parts.append(str(t.get("value") or ""))
    hm = hazard_data.get("hazard_metrics") or {}
    for key in ("other_designations", "flash_point", "nfpa", "gwp100", "odp"):
        for item in hm.get(key) or []:
            phrase_corpus_parts.append(str(item) if key not in ("gwp100", "odp") else f"{'GWP' if key=='gwp100' else 'ODP'} {item}")
    phrase_corpus = " | ".join(p for p in phrase_corpus_parts if p)

    # NOTE: ToxRefDB / POD values (NOAEL, LOAEL, LEL, TD50) are intentionally NOT used
    # as surrogate LD50 values. They are repeated-dose points of departure, not acute
    # lethality, so treating them as LD50 produces false-high acute scores.

    evidence = {
        "oral_ld50": ld50_oral,
        "dermal_ld50": ld50_dermal,
        "inhalation_lc50": lc50_inh,
        "aquatic_lc50": ({"value": lc50_aq, "unit": "mg/L"} if lc50_aq is not None else None),
        "flash_point_c": ({"value": flash_c, "unit": "degC"} if flash_c is not None else None),
        "vapor_pressure_mmhg": ({"value": vp, "unit": "mmHg"} if vp is not None else None),
        "nfpa_health": nfpa_health,
        "nfpa_fire": nfpa_fire,
        "iarc": iarc,
        "epa_carcinogen": epa_carc,
        "gwp100": ({"value": gwp100, "unit": "CO2e"} if gwp100 is not None else None),
        "odp": ({"value": odp, "unit": "ODP"} if odp is not None else None),
        "log_kow": log_kow,
        "bcf_l_kg": bcf_l_kg,
        "biodeg_half_life_days": biodeg_hl,
        "molecular_weight": _num(hazard_data.get("molecular_weight")),
        "ph_estimate": ph_info,
        "ph_heuristic": ph_info,
    }

    def _record(
        category: str,
        subcat: str,
        unit_name: str,
        rule: dict,
        input_value: Any,
        score: Any,
        *,
        status: str = STATUS_SCORED,
        qualifier: Any = None,
        predicted: bool = False,
    ) -> None:
        scored.append({
            "category": category,
            "subcategory": subcat,
            "unit": unit_name,
            "input_value": input_value,
            "qualifier": qualifier,
            "matrix_rule": _rule_summary(rule),
            "score": score,
            "status": status,
            "predicted": predicted,
        })

    for category, subcats in matrix.items():
        results[category] = {}
        for subcat, units in subcats.items():
            sub_scores: dict[str, Any] = {}
            for unit_name, rule in units.items():
                score = None
                input_value: Any = None
                qualifier: Any = None
                predicted = False
                rtype = rule.get("type", "")
                miss_reason = "no applicable evidence"

                # pH is U-shaped — never use Excel numeric thresholds or generic phrase corpus.
                if str(subcat).strip().lower() == "ph":
                    try:
                        from utils.p2oasys_ph import apply_ph_rule

                        sc_ph, iv_ph, handled = apply_ph_rule(
                            subcat=subcat, unit_name=unit_name, ph_info=ph_info
                        )
                    except Exception:
                        sc_ph, iv_ph, handled = None, None, True
                    if handled:
                        if sc_ph is not None:
                            score = sc_ph
                            input_value = iv_ph
                            src = str((ph_info or {}).get("source") or "")
                            predicted = src.startswith(("fg_pka", "opera_pka"))
                        else:
                            miss_reason = "no pH estimate (need SMILES/CID or OPERA pKa / experimental pH)"
                        if score is not None:
                            status = STATUS_PREDICTED_ONLY if predicted else STATUS_SCORED
                            sub_scores[unit_name] = score
                            _record(
                                category, subcat, unit_name, rule, input_value, score,
                                status=status, qualifier=qualifier, predicted=predicted,
                            )
                        else:
                            missing.append({
                                "category": category,
                                "subcategory": subcat,
                                "unit": unit_name,
                                "status": STATUS_NO_DATA,
                                "reason": miss_reason,
                            })
                        continue

                if rtype == "numeric":
                    if "LD50" in unit_name and "Oral" in subcat:
                        if ld50_oral:
                            input_value = ld50_oral["value"]
                            qualifier = ld50_oral.get("qualifier")
                            score = _score_numeric(rule, ld50_oral["value"], higher_is_safer=True)
                        else:
                            miss_reason = "no oral LD50 with confirmed oral route"
                    elif "LD50" in unit_name and "Dermal" in subcat:
                        if ld50_dermal:
                            input_value = ld50_dermal["value"]
                            qualifier = ld50_dermal.get("qualifier")
                            score = _score_numeric(rule, ld50_dermal["value"], higher_is_safer=True)
                        else:
                            miss_reason = "no dermal LD50 (oral fallback disabled)"
                    elif "LC50" in unit_name and "Inhalation" in subcat:
                        if lc50_inh is not None:
                            input_value = lc50_inh["value"]
                            qualifier = lc50_inh.get("qualifier")
                            score = _score_numeric(rule, lc50_inh["value"], higher_is_safer=True)
                        else:
                            miss_reason = "no inhalation LC50 in ppm (or convertible mg/m³)"
                    elif "Flash" in unit_name or "deg C" in unit_name:
                        if flash_c is not None:
                            input_value = flash_c
                            score = _score_numeric(rule, flash_c, higher_is_safer=False)
                        else:
                            miss_reason = "no flash point"
                    elif "mm Hg" in unit_name or "Vapor" in unit_name:
                        if vp is not None:
                            input_value = vp
                            score = _score_numeric(rule, vp, higher_is_safer=False)
                        else:
                            miss_reason = "no vapor pressure"
                    elif "NFPA" in unit_name or "HMIS" in unit_name:
                        if "Health" in subcat or "health" in unit_name.lower():
                            if nfpa_health is not None:
                                input_value = nfpa_health
                                for t, s in rule.get("thresholds", []):
                                    if t == nfpa_health:
                                        score = s
                                        break
                            else:
                                miss_reason = "no NFPA health rating"
                        elif "Fire" in subcat or "Flammability" in subcat:
                            if nfpa_fire is not None:
                                input_value = nfpa_fire
                                for t, s in rule.get("thresholds", []):
                                    if t == nfpa_fire:
                                        score = s
                                        break
                            else:
                                miss_reason = "no NFPA fire rating"
                    elif "LC50" in unit_name and ("Aquatic" in subcat or "Aquatic" in str(subcats)):
                        if lc50_aq is not None:
                            input_value = lc50_aq
                            score = _score_numeric(rule, lc50_aq, higher_is_safer=True)
                        else:
                            miss_reason = "no aquatic LC50/EC50"
                    elif "GWP" in unit_name:
                        if gwp100 is not None:
                            input_value = gwp100
                            # Higher GWP = more hazardous
                            score = _score_numeric(rule, gwp100, higher_is_safer=False)
                        else:
                            miss_reason = "no GWP100 (non-gas heuristic or ATMO/CSV lookup)"
                    elif "ODP" in unit_name:
                        if odp is not None:
                            input_value = odp
                            score = _score_numeric(rule, odp, higher_is_safer=False)
                        else:
                            miss_reason = "no ODP value"
                    elif "Log Kow" in unit_name or "Log Kow / Pow" in unit_name or unit_name.strip() in ("Log Kow", "Log Pow", "Kow / Pow"):
                        if log_kow is not None:
                            input_value = log_kow
                            # Higher log Kow → more bioaccumulative / hazardous
                            score = _score_numeric(rule, float(log_kow["value"]), higher_is_safer=False)
                        else:
                            miss_reason = "no log Kow / Pow"
                    elif "BAF/BCF" in unit_name or unit_name.strip().startswith("BAF/BCF"):
                        if bcf_l_kg is not None:
                            input_value = bcf_l_kg
                            score = _score_numeric(rule, float(bcf_l_kg["value"]), higher_is_safer=False)
                        else:
                            miss_reason = "no BAF/BCF (L/kg)"
                    elif "BOD Half-life" in unit_name:
                        if biodeg_hl is not None:
                            input_value = biodeg_hl
                            # Longer half-life → more hazardous
                            score = _score_numeric(rule, float(biodeg_hl["value"]), higher_is_safer=False)
                        else:
                            miss_reason = "no BOD half-life (OPERA BioDeg optional)"
                    else:
                        miss_reason = "numeric unit not mapped to an extractor"

                elif rtype == "ghs_h":
                    if h_codes:
                        input_value = h_codes
                        score = _score_ghs_h(rule, h_codes)
                        if score is None:
                            miss_reason = "no matching GHS H-codes for this subcategory"
                    else:
                        miss_reason = "no GHS H-codes"

                elif rtype == "phrase":
                    if phrase_corpus.strip():
                        input_value = phrase_corpus[:240]
                        score = _score_phrase(rule, phrase_corpus)
                        if score is None:
                            miss_reason = "no matching key phrase"
                        else:
                            # Tag predicted when phrase evidence is entirely from predicted toxicities (e.g. OPERA ReadyBiodeg/LogBCF).
                            tox_list = hazard_data.get("toxicities") or []
                            if tox_list and all(bool(x.get("predicted")) for x in tox_list):
                                predicted = True
                    else:
                        miss_reason = "no phrase corpus"

                elif rtype == "text":
                    if "IARC" in unit_name:
                        if iarc:
                            input_value = iarc
                            for k, v in rule.get("mapping", {}).items():
                                if iarc in k or k in str(iarc):
                                    score = v
                                    break
                            if score is None:
                                miss_reason = f"IARC group {iarc} not in matrix mapping"
                        else:
                            miss_reason = "no IARC group for subject chemical"
                    elif "EPA" in unit_name:
                        if epa_carc:
                            input_value = epa_carc
                            for k, v in rule.get("mapping", {}).items():
                                if epa_carc in k.upper():
                                    score = v
                                    break
                            if score is None:
                                miss_reason = f"EPA class {epa_carc} not in matrix mapping"
                        else:
                            miss_reason = "no EPA carcinogen class"
                    else:
                        miss_reason = "text unit not mapped to an extractor"

                else:
                    miss_reason = f"unsupported rule type {rtype!r}"

                if score is not None:
                    # Predicted-only flag when evidence came from OPERA/QSAR gap-fill.
                    if isinstance(input_value, dict) and input_value.get("predicted"):
                        predicted = True
                    if ld50_oral and input_value == ld50_oral.get("value") and ld50_oral.get("predicted"):
                        predicted = True
                    if log_kow and input_value is log_kow and log_kow.get("predicted"):
                        predicted = True
                    if bcf_l_kg and input_value is bcf_l_kg and bcf_l_kg.get("predicted"):
                        predicted = True
                    if biodeg_hl and input_value is biodeg_hl and biodeg_hl.get("predicted"):
                        predicted = True
                    status = STATUS_PREDICTED_ONLY if predicted else STATUS_SCORED
                    sub_scores[unit_name] = score
                    _record(
                        category, subcat, unit_name, rule, input_value, score,
                        status=status, qualifier=qualifier, predicted=predicted,
                    )
                else:
                    missing.append({
                        "category": category,
                        "subcategory": subcat,
                        "unit": unit_name,
                        "status": STATUS_NO_DATA,
                        "reason": miss_reason,
                    })

            if sub_scores:
                results[category][subcat] = {**sub_scores, "_max": max(sub_scores.values())}

        subcat_maxima: list[float] = []
        for _sk, bundle in results[category].items():
            if _sk.startswith("_") or not isinstance(bundle, dict):
                continue
            sm = bundle.get("_max")
            if isinstance(sm, (int, float)):
                subcat_maxima.append(float(sm))
        cat_agg = _category_score_mean_top_two_subcategories(subcat_maxima)
        if cat_agg is not None:
            results[category]["_category_max"] = cat_agg

    category_status: dict[str, str] = {}
    for category in matrix:
        data = results.get(category) or {}
        if data.get("_category_max") is not None:
            # Predicted-only if every scored unit in this category is predicted.
            cat_scored = [s for s in scored if s["category"] == category]
            if cat_scored and all(s.get("predicted") for s in cat_scored):
                category_status[category] = STATUS_PREDICTED_ONLY
            else:
                category_status[category] = STATUS_SCORED
        else:
            category_status[category] = STATUS_NO_DATA

    trace = {
        "scorer_version": SCORER_VERSION,
        "evidence": evidence,
        "rejected": rejected,
        "scored": scored,
        "missing": missing,
        "category_status": category_status,
    }
    return results, trace


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
            print(f"--- {category} (category score, mean of top 2 subcats: {cmax}) ---")
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


