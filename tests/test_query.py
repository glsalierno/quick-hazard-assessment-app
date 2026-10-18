from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

import pandas as pd
import pytest

import chemdb.query as query_mod

pytest.importorskip("pytest_benchmark")


@dataclass
class DummySubstance:
    substance_id: uuid.UUID
    preferred_name: str
    cas_rn: str | None = None
    ec_number: str | None = None
    dtxsid: str | None = None


class DummyQuery:
    def __init__(self, substance: DummySubstance | None):
        self.substance = substance

    def filter(self, *_args, **_kwargs):
        return self

    def one_or_none(self):
        return self.substance


class DummyResult:
    def __init__(self, mapping_row=None, rows=None):
        self._mapping_row = mapping_row
        self._rows = rows or []

    def mappings(self):
        return self

    def first(self):
        return self._mapping_row

    def fetchall(self):
        return self._rows


class DummySession:
    def __init__(self, fuzzy_match=False):
        self.fuzzy_match = fuzzy_match

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False

    def query(self, _model):
        return DummyQuery(None)

    def get(self, _model, substance_id):
        return DummySubstance(substance_id=substance_id, preferred_name="Formaldehyde")

    def execute(self, stmt, _params=None):
        sql = str(stmt)
        if "JOIN substance_synonyms" in sql and self.fuzzy_match:
            return DummyResult(mapping_row={"substance_id": uuid.uuid4()})
        if "FROM matched m" in sql:
            return DummyResult(rows=[("50-00-0", uuid.uuid4(), "Formaldehyde", ["H301"])])
        return DummyResult()

    def commit(self):
        return None


def test_formaldehyde_fuzzy_match(monkeypatch):
    monkeypatch.setattr(query_mod, "SessionLocal", lambda: DummySession(fuzzy_match=True))
    found = query_mod.find_substance("formaldehyde")
    assert found is not None
    assert found.preferred_name == "Formaldehyde"


def test_lookup_many_returns_dataframe(monkeypatch):
    monkeypatch.setattr(query_mod, "SessionLocal", lambda: DummySession())
    df = query_mod.lookup_many(["50-00-0"])
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(df.columns) == {"input", "matched_id", "name", "ghs_codes"}


def test_batch_1k_under_2s(monkeypatch):
    monkeypatch.setattr(query_mod, "SessionLocal", lambda: DummySession())
    identifiers = ["50-00-0"] * 1000
    start = time.perf_counter()
    query_mod.lookup_many(identifiers)
    elapsed = time.perf_counter() - start
    assert elapsed < 2.0


def test_lookup_many_benchmark(benchmark, monkeypatch):
    monkeypatch.setattr(query_mod, "SessionLocal", lambda: DummySession())
    identifiers = ["50-00-0"] * 1000
    benchmark(query_mod.lookup_many, identifiers)
