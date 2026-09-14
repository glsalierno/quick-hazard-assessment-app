"""High-performance lookup/query layer for ChemDB."""

from __future__ import annotations

import uuid
from typing import Any

import pandas as pd
from sqlalchemy import text

from chemdb.config import SessionLocal
from chemdb.models import Substance
from ingest.crosswalk import detect_id_type


def find_substance(identifier: str, id_type: str = "auto") -> Substance | None:
    """Lookup by CAS, EC, DTXSID, InChIKey, then trigram synonym match.

    Expected plan on indexed database:
    - exact lookups use btree/partial indexes on `cas_rn`, `ec_number`, `dtxsid`, `inchikey14`
    - fallback name search uses `pg_trgm` GIN index on `substance_synonyms.synonym`
    - target latency: <50ms on ~1M rows
    """
    resolved = detect_id_type(identifier) if id_type == "auto" else id_type
    with SessionLocal() as session:
        if resolved == "cas":
            return session.query(Substance).filter(Substance.cas_rn == identifier).one_or_none()
        if resolved == "ec":
            return session.query(Substance).filter(Substance.ec_number == identifier).one_or_none()
        if resolved == "dtxsid":
            return session.query(Substance).filter(Substance.dtxsid == identifier.upper()).one_or_none()
        if resolved == "inchikey":
            return session.query(Substance).filter(Substance.inchikey14 == identifier.upper()[:14]).one_or_none()

        stmt = text(
            """
            SELECT s.*
            FROM substances s
            JOIN substance_synonyms ss ON ss.substance_id = s.substance_id
            WHERE lower(ss.synonym) % lower(:name)
            ORDER BY similarity(lower(ss.synonym), lower(:name)) DESC
            LIMIT 1
            """
        )
        row = session.execute(stmt, {"name": identifier}).mappings().first()
        if not row:
            return None
        return session.get(Substance, row["substance_id"])


def get_hazard_summary(substance_id: uuid.UUID) -> dict[str, Any]:
    """Return hazard summary from materialized view for a substance ID."""
    with SessionLocal() as session:
        stmt = text(
            """
            SELECT ghs_codes, is_pbt, contributing_sources
            FROM hazard_summary
            WHERE substance_id = :substance_id
            """
        )
        row = session.execute(stmt, {"substance_id": str(substance_id)}).mappings().first()
        if not row:
            return {"ghs_codes": [], "is_pbt": False, "sources": []}
        return {
            "ghs_codes": row["ghs_codes"] or [],
            "is_pbt": bool(row["is_pbt"]),
            "sources": row["contributing_sources"] or [],
        }


def lookup_many(identifiers: list[str]) -> pd.DataFrame:
    """Batch lookup using a single SQL statement with UNNEST + joins."""
    if not identifiers:
        return pd.DataFrame(columns=["input", "matched_id", "name", "ghs_codes"])

    with SessionLocal() as session:
        stmt = text(
            """
            WITH incoming AS (
              SELECT trim(x)::text AS input
              FROM unnest(:identifiers::text[]) AS x
            ),
            matched AS (
              SELECT
                i.input,
                s.substance_id,
                s.preferred_name
              FROM incoming i
              LEFT JOIN substances s
                ON s.cas_rn = i.input
                OR s.ec_number = i.input
                OR s.dtxsid = upper(i.input)
                OR s.inchikey14 = upper(substr(i.input, 1, 14))
            )
            SELECT
              m.input,
              m.substance_id AS matched_id,
              m.preferred_name AS name,
              hs.ghs_codes
            FROM matched m
            LEFT JOIN hazard_summary hs ON hs.substance_id = m.substance_id
            """
        )
        result = session.execute(stmt, {"identifiers": identifiers})
        return pd.DataFrame(result.fetchall(), columns=["input", "matched_id", "name", "ghs_codes"])


def refresh_hazard_summary() -> None:
    """Refresh the hazard summary materialized view concurrently."""
    with SessionLocal() as session:
        session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY hazard_summary;"))
        session.commit()
