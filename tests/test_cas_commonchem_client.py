"""Unit tests for the isolated CAS Common Chemistry client."""

from __future__ import annotations

from typing import Any

import pytest

from utils.cas_commonchem_client import (
    CASCommonChemistryClient,
    CASCommonChemistryError,
    normalize_detail,
)


class _FakeResponse:
    def __init__(
        self,
        *,
        payload: dict[str, Any] | None = None,
        text: str = "",
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.text = text
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        if self._payload is None:
            raise ValueError("not JSON")
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response


def _sample_detail() -> dict[str, Any]:
    return {
        "uri": "substance/pt/1912249",
        "rn": "1912-24-9",
        "name": "Atrazine",
        "inchi": "InChI=1S/example",
        "inchiKey": "InChIKey=EXAMPLE",
        "smile": "example",
        "canonicalSmile": "canonical",
        "molecularFormula": "C<sub>8</sub>H<sub>14</sub>ClN<sub>5</sub>",
        "molecularMass": "215.68",
        "experimentalProperties": [
            {
                "name": "Melting Point",
                "property": "175-177 &deg;C",
                "sourceNumber": 1,
            },
            {
                "name": "Density",
                "property": "1.23 g/cm<sup>3</sup> @ Temp: 22 &deg;C",
                "sourceNumber": 1,
            },
        ],
        "propertyCitations": [
            {
                "docUri": "",
                "sourceNumber": 1,
                "source": "Hazardous Substances Data Bank",
            }
        ],
        "synonyms": ["Atrazine", "<em>s</em>-Triazine"],
        "replacedRns": ["11121-31-6"],
        "hasMolfile": True,
    }


def test_normalize_detail_preserves_raw_value_and_citation() -> None:
    normalized = normalize_detail(_sample_detail(), retrieved_at="2026-07-16T00:00:00Z")
    assert normalized["cas_rn"] == "1912-24-9"
    assert normalized["molecular_formula"] == "C8H14ClN5"
    assert normalized["synonyms"][1] == "s-Triazine"
    assert normalized["replaced_rns"] == ["11121-31-6"]
    assert normalized["has_molfile"] is True

    melting = normalized["experimental_properties"][0]
    assert melting["raw_value"] == "175-177 °C"
    assert melting["value_min"] == 175.0
    assert melting["value_max"] == 177.0
    assert melting["unit"] == "°C"
    assert melting["citation"]["source"] == "Hazardous Substances Data Bank"

    density = normalized["experimental_properties"][1]
    assert density["numeric_values"] == [1.23, 22.0]
    assert density["unit"].lower() == "g/cm3"
    assert density["value"] == 1.23
    assert density["conditions"] == "Temp: 22 °C"


def test_normalize_detail_reports_no_toxicity_fields_for_normal_record() -> None:
    normalized = normalize_detail(_sample_detail())
    observed = normalized["capability_observation"]
    assert observed["toxicity_like_fields"] == []
    assert observed["toxicity_like_property_names"] == []


def test_detail_uses_api_key_header_without_returning_key() -> None:
    session = _FakeSession(_FakeResponse(payload=_sample_detail()))
    client = CASCommonChemistryClient(api_key="test-secret", session=session)
    result = client.get_detail("1912-24-9")
    assert result["rn"] == "1912-24-9"
    call = session.calls[0]
    assert call["url"].endswith("/detail")
    assert call["params"] == {"cas_rn": "1912-24-9"}
    assert call["headers"]["X-Api-Key"] == "test-secret"
    assert "test-secret" not in repr(result)


def test_search_caps_page_size_at_100() -> None:
    session = _FakeSession(_FakeResponse(payload={"count": 0, "results": []}))
    client = CASCommonChemistryClient(api_key="test-secret", session=session)
    client.search("benz*", size=500)
    assert session.calls[0]["params"]["size"] == 100


def test_export_uses_uri_and_returns_molfile() -> None:
    session = _FakeSession(_FakeResponse(text="Atrazine\n  CAS\nM  END\n"))
    client = CASCommonChemistryClient(api_key="test-secret", session=session)
    molfile = client.export_molfile("substance/pt/1912249")
    assert "M  END" in molfile
    assert session.calls[0]["params"] == {"uri": "substance/pt/1912249"}


def test_missing_key_fails_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAS_COMMONCHEM_API_KEY", raising=False)
    monkeypatch.setattr(
        "utils.cas_commonchem_client._secret_file_key",
        lambda app_root=None: None,
    )
    with pytest.raises(CASCommonChemistryError, match="not configured"):
        CASCommonChemistryClient()
