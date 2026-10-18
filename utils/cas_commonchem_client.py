"""Isolated client for the CAS Common Chemistry API.

This module is intentionally independent of Streamlit UI code. CAS Common
Chemistry is used as an identity and experimental physical-property source;
it is not treated as a toxicity or regulatory-classification source.
"""

from __future__ import annotations

import html
import os
import re
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://commonchemistry.cas.org/api"
PARSER_VERSION = "cas_commonchem_client_v6_1"
DEFAULT_TIMEOUT_SECONDS = 30

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NUMBER_RE = re.compile(r"(?<![\d.])[-+]?(?:\d+(?:\.\d*)?|\.\d+)")
_TOXICITY_TERM_RE = re.compile(
    r"(?:^|[^a-z])(toxic|toxicity|ld50|lc50|ec50|noael|loael|ghs|hazard)(?:[^a-z]|$)",
    re.IGNORECASE,
)


class CASCommonChemistryError(RuntimeError):
    """Raised when the CAS Common Chemistry API request fails."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean_html(value: Any) -> str:
    text = html.unescape(str(value or ""))
    return re.sub(r"\s+", " ", _HTML_TAG_RE.sub("", text)).strip()


def _secret_file_key(app_root: Path | None = None) -> str | None:
    """Read only the named CAS key from the gitignored Streamlit secrets file."""
    root = app_root or Path(__file__).resolve().parents[1]
    path = root / ".streamlit" / "secrets.toml"
    if not path.is_file():
        return None
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None
    value = data.get("CAS_COMMONCHEM_API_KEY")
    return str(value).strip() if value else None


def get_api_key(app_root: Path | None = None) -> str | None:
    """Resolve API key from environment first, then local Streamlit secrets."""
    return (
        (os.environ.get("CAS_COMMONCHEM_API_KEY") or "").strip()
        or _secret_file_key(app_root)
    ) or None


def _build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "GHhaz6-CAS-capability-test/1.0 (academic research)",
        }
    )
    return session


def _parse_property_value(raw_value: str) -> dict[str, Any]:
    """Best-effort parsing while retaining the authoritative original text."""
    clean = _clean_html(raw_value)
    numbers: list[float] = []
    # Do not treat unit exponents (for example the 3 in g/cm3) as measurements.
    numeric_text = re.sub(r"(?<=[A-Za-z/])\d+(?=\b)", "", clean)
    for token in _NUMBER_RE.findall(numeric_text):
        try:
            numbers.append(float(token))
        except ValueError:
            continue
    unit_match = re.search(
        r"(g\s*/\s*cm[3³]|g\s*/\s*mL|kg\s*/\s*m[3³]|°\s*[CFK]|"
        r"deg\s*[CF]|mmHg|kPa|MPa|Pa|bar|atm|mPa[·.\s]*s|cP|%)"
        r"(?=\s|@|$|\))",
        clean,
        re.IGNORECASE,
    )
    condition_match = re.search(
        r"(?:@|\bTemp(?:erature)?\s*:|\bPressure\s*:)(.+)$",
        clean,
        re.IGNORECASE,
    )
    unit = re.sub(r"\s+", "", unit_match.group(1)) if unit_match else None
    if unit:
        unit = unit.replace("³", "3")
    parsed: dict[str, Any] = {
        "raw_value": clean,
        "numeric_values": numbers,
        "unit": unit,
        "conditions": condition_match.group(1).strip() if condition_match else None,
    }
    if len(numbers) >= 2 and re.search(r"\d\s*(?:-|–|—|to)\s*\d", clean, re.IGNORECASE):
        parsed["value_min"] = numbers[0]
        parsed["value_max"] = numbers[1]
    elif numbers:
        # Additional numbers usually belong to conditions (for example 20 °C).
        parsed["value"] = numbers[0]
    return parsed


def normalize_detail(detail: dict[str, Any], *, retrieved_at: str | None = None) -> dict[str, Any]:
    """Normalize a `/detail` response without discarding raw property strings."""
    citations: dict[str, dict[str, Any]] = {}
    for citation in detail.get("propertyCitations") or []:
        if not isinstance(citation, dict):
            continue
        number = str(citation.get("sourceNumber") or "")
        citations[number] = {
            "source_number": citation.get("sourceNumber"),
            "source": _clean_html(citation.get("source")),
            "document_uri": citation.get("docUri") or None,
        }

    properties: list[dict[str, Any]] = []
    for prop in detail.get("experimentalProperties") or []:
        if not isinstance(prop, dict):
            continue
        source_number = prop.get("sourceNumber")
        parsed = _parse_property_value(str(prop.get("property") or ""))
        properties.append(
            {
                "name": _clean_html(prop.get("name")),
                **parsed,
                "source_number": source_number,
                "citation": citations.get(str(source_number)),
                "source": "CAS Common Chemistry",
                "parser_version": PARSER_VERSION,
            }
        )

    top_level_fields = sorted(str(key) for key in detail)
    toxicity_like_fields = [
        key for key in top_level_fields if _TOXICITY_TERM_RE.search(key)
    ]
    toxicity_like_properties = [
        item["name"] for item in properties if _TOXICITY_TERM_RE.search(item["name"])
    ]

    return {
        "source": "CAS Common Chemistry",
        "retrieved_at": retrieved_at or _utc_now(),
        "parser_version": PARSER_VERSION,
        "uri": detail.get("uri"),
        "cas_rn": detail.get("rn") or detail.get("cas_rn"),
        "name": _clean_html(detail.get("name")),
        "molecular_formula": _clean_html(detail.get("molecularFormula")),
        "molecular_mass": _clean_html(detail.get("molecularMass")),
        "inchi": detail.get("inchi"),
        "inchi_key": detail.get("inchiKey"),
        "smiles": detail.get("smile"),
        "canonical_smiles": detail.get("canonicalSmile"),
        "synonyms": [_clean_html(item) for item in detail.get("synonyms") or []],
        "replaced_rns": list(detail.get("replacedRns") or []),
        "has_molfile": bool(detail.get("hasMolfile")),
        "experimental_properties": properties,
        "property_citations": list(citations.values()),
        "capability_observation": {
            "top_level_fields": top_level_fields,
            "toxicity_like_fields": toxicity_like_fields,
            "toxicity_like_property_names": toxicity_like_properties,
        },
    }


class CASCommonChemistryClient:
    """Small authenticated client for search, detail, and molfile export."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = api_key or get_api_key()
        if not self._api_key:
            raise CASCommonChemistryError(
                "CAS_COMMONCHEM_API_KEY is not configured in the environment "
                "or .streamlit/secrets.toml"
            )
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = session or _build_session()

    def _get(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        accept: str = "application/json",
    ) -> requests.Response:
        headers = {"X-Api-Key": self._api_key, "Accept": accept}
        try:
            response = self.session.get(
                f"{self.base_url}/{endpoint.lstrip('/')}",
                params=params,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise CASCommonChemistryError(f"CAS API request failed: {exc}") from exc
        if response.status_code == 401:
            raise CASCommonChemistryError("CAS API authentication failed (HTTP 401)")
        if response.status_code == 403:
            raise CASCommonChemistryError("CAS API access denied (HTTP 403)")
        if response.status_code == 404:
            raise CASCommonChemistryError("CAS Common Chemistry record not found (HTTP 404)")
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise CASCommonChemistryError(
                f"CAS API returned HTTP {response.status_code}"
            ) from exc
        return response

    def search(self, query: str, *, offset: int = 0, size: int = 50) -> dict[str, Any]:
        """Search CAS Common Chemistry by RN, name, SMILES, InChI, or InChIKey."""
        response = self._get(
            "search",
            params={"q": query, "offset": max(0, offset), "size": min(max(1, size), 100)},
        )
        try:
            return response.json()
        except ValueError as exc:
            raise CASCommonChemistryError("CAS search returned invalid JSON") from exc

    def get_detail(self, cas_rn: str) -> dict[str, Any]:
        """Return the raw detail JSON for one CAS RN."""
        response = self._get("detail", params={"cas_rn": cas_rn})
        try:
            return response.json()
        except ValueError as exc:
            raise CASCommonChemistryError("CAS detail returned invalid JSON") from exc

    def get_normalized_detail(self, cas_rn: str) -> dict[str, Any]:
        """Retrieve and normalize one CAS Common Chemistry detail record."""
        started = time.perf_counter()
        raw = self.get_detail(cas_rn)
        normalized = normalize_detail(raw)
        normalized["response_time_seconds"] = round(time.perf_counter() - started, 3)
        return normalized

    def export_molfile(self, uri: str) -> str:
        """Retrieve a molfile using the substance URI from `/detail`."""
        response = self._get(
            "export",
            params={"uri": uri},
            accept="chemical/x-mdl-molfile, text/plain, */*",
        )
        return response.text
