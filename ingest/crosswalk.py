"""ID normalization and cross-source merge logic."""

from __future__ import annotations

import re

from sqlalchemy import text

from chemdb.config import SessionLocal

CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")
EC_RE = re.compile(r"^\d{3}-\d{3}-\d$")
DTXSID_RE = re.compile(r"^DTXSID\d{7,}$", re.IGNORECASE)
INCHIKEY_RE = re.compile(r"^[A-Z]{14}(?:-[A-Z]{10}-[A-Z])?$")


def normalize_cas(cas: str | None) -> str | None:
    """Normalize CAS and validate checksum.

    Returns canonical dashed CAS (e.g. 50000-0 -> 50-00-0), otherwise None.
    """
    if not cas:
        return None
    digits = "".join(ch for ch in str(cas) if ch.isdigit())
    if len(digits) < 3:
        return None

    checksum = int(digits[-1])
    body = digits[:-1]
    calc = sum(int(n) * (idx + 1) for idx, n in enumerate(reversed(body))) % 10
    if calc != checksum:
        return None

    return f"{digits[:-3]}-{digits[-3:-1]}-{digits[-1]}"


def detect_id_type(value: str) -> str:
    """Detect whether input is CAS, DTXSID, EC, InChIKey, or name."""
    raw = value.strip()
    normalized = normalize_cas(raw)
    if normalized and (CAS_RE.match(raw) or raw.replace("-", "").isdigit()):
        return "cas"
    if DTXSID_RE.match(raw):
        return "dtxsid"
    if EC_RE.match(raw):
        return "ec"
    if INCHIKEY_RE.match(raw.upper()):
        return "inchikey"
    return "name"


def is_uvcb_name(name: str | None) -> bool:
    """Best-effort UVCB marker detection from substance naming."""
    if not name:
        return False
    lowered = name.lower()
    markers = ("reaction mass", "uva", "unknown composition", "uvcb", "complex combination")
    return any(token in lowered for token in markers)


def merge_substance_records() -> int:
    """Merge duplicate substances where DTXSID/CAS/EC collisions indicate same identity."""
    merge_sql = text(
        """
        WITH ranked AS (
          SELECT
            substance_id,
            dtxsid,
            cas_rn,
            ec_number,
            coalesce(dtxsid, cas_rn, ec_number) AS merge_key,
            min(substance_id) OVER (PARTITION BY coalesce(dtxsid, cas_rn, ec_number)) AS canonical_id
          FROM substances
          WHERE coalesce(dtxsid, cas_rn, ec_number) IS NOT NULL
        ),
        moved_hazards AS (
          UPDATE hazard_endpoints h
          SET substance_id = r.canonical_id
          FROM ranked r
          WHERE h.substance_id = r.substance_id
            AND r.substance_id <> r.canonical_id
          RETURNING h.id
        ),
        moved_synonyms AS (
          UPDATE substance_synonyms ss
          SET substance_id = r.canonical_id
          FROM ranked r
          WHERE ss.substance_id = r.substance_id
            AND r.substance_id <> r.canonical_id
          RETURNING ss.id
        )
        DELETE FROM substances s
        USING ranked r
        WHERE s.substance_id = r.substance_id
          AND r.substance_id <> r.canonical_id
        """
    )
    with SessionLocal() as session:
        result = session.execute(merge_sql)
        session.commit()
        return result.rowcount if result.rowcount is not None else 0
